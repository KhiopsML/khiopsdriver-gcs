#include <gtest/gtest.h>
#include "google/cloud/storage/testing/mock_client.h"

#include <array>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <memory>

#include "../src/gcsplugin.h"

namespace gc = ::google::cloud;
namespace gcs = gc::storage;

using ::testing::Return;
using LOReturnType = gc::StatusOr<gcs::internal::ListObjectsResponse>;

constexpr int kSuccess{ 1 };
constexpr int kFailure{ 0 };

constexpr int kFalse{ 0 };
constexpr int kTrue{ 1 };


TEST(GCSDriverTest, GetDriverName)
{
    ASSERT_STREQ(driver_getDriverName(), "GCS driver");
}

TEST(GCSDriverTest, GetVersion)
{
    ASSERT_STREQ(driver_getVersion(), "0.1.0");
}

TEST(GCSDriverTest, GetScheme)
{
    ASSERT_STREQ(driver_getScheme(), "gs");
}

TEST(GCSDriverTest, IsReadOnly)
{
    ASSERT_EQ(driver_isReadOnly(), kFalse);
}

TEST(GCSDriverTest, Connect)
{
    //check connection state before call to connect
    ASSERT_EQ(driver_isConnected(), kFalse);

    //call connect and check connection
    ASSERT_EQ(driver_connect(), kSuccess);
    ASSERT_EQ(driver_isConnected(), kTrue);

    //call disconnect and check connection
    ASSERT_EQ(driver_disconnect(), kSuccess);
    ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(GCSDriverTest, Disconnect)
{
    ASSERT_EQ(driver_connect(), kSuccess);
    ASSERT_EQ(driver_disconnect(), kSuccess);
    ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(GCSDriverTest, GetFileSize)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(GCSDriverTest, GetMultipartFileSize)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/Adult-split-00000000000*.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(GCSDriverTest, GetSystemPreferredBufferSize)
{
	ASSERT_EQ(driver_getSystemPreferredBufferSize(), 4 * 1024 * 1024);
}

constexpr const char* test_dir_name = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/";

constexpr const char* test_single_file = "gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt";
constexpr const char* test_range_file_one_header = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult/Adult-split-0[0-5].txt";
constexpr const char* test_glob_file_header_each = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/*.txt";
constexpr const char* test_double_glob_header_each = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**/Adult-split-*.txt";

constexpr std::array<const char*, 4> test_files = {
    test_single_file,
    test_range_file_one_header,
    test_glob_file_header_each,
    test_double_glob_header_each
};


#define READ_MOCK_LAMBDA(read_sim) [&](gcs::internal::ReadObjectRangeRequest const& request) {                                                  \
                                        EXPECT_EQ(request.bucket_name(), "mock_bucket") << request;                                             \
                                        std::unique_ptr<gcs::testing::MockObjectReadSource> mock_source{new gcs::testing::MockObjectReadSource};\
                                        ::testing::InSequence seq;                                                                              \
                                        EXPECT_CALL(*mock_source, IsOpen()).WillRepeatedly(Return(true));                                       \
                                        EXPECT_CALL(*mock_source, Read).WillOnce((read_sim));                                                   \
                                        EXPECT_CALL(*mock_source, IsOpen()).WillRepeatedly(Return(false));                                      \
                                                                                                                                                \
                                        return gc::make_status_or<std::unique_ptr<gcs::internal::ObjectReadSource>>(std::move(mock_source));}   \


#define READ_MOCK_LAMBDA_FAILURE [](gcs::internal::ReadObjectRangeRequest const& request) {                                                              \
                                        EXPECT_EQ(request.bucket_name(), "mock_bucket") << request;                                                      \
                                        std::unique_ptr<gcs::testing::MockObjectReadSource> mock_source{new gcs::testing::MockObjectReadSource};         \
                                        ::testing::InSequence seq;                                                                                       \
                                        EXPECT_CALL(*mock_source, IsOpen).WillRepeatedly(Return(true));                                                  \
                                        EXPECT_CALL(*mock_source, Read)                                                                                  \
                                            .WillOnce(Return(google::cloud::Status(google::cloud::StatusCode::kInvalidArgument, "Invalid Argument")));   \
                                        EXPECT_CALL(*mock_source, IsOpen).WillRepeatedly(Return(false));                                                 \
                                                                                                                                                         \
                                        return google::cloud::make_status_or<std::unique_ptr<gcs::internal::ObjectReadSource>>(std::move(mock_source));} \




class GCSDriverTestFixture : public ::testing::Test
{
protected:
    void SetUp() override
    {
        mock_client = std::make_shared<gcs::testing::MockClient>();
        auto client = gcs::testing::UndecoratedClientFromMock(mock_client);
        test_setClient(std::move(client));
    }

    void TearDown() override
    {
        test_unsetClient();
    }

public:
    static void TearDownTestSuite()
    {
        ASSERT_EQ(driver_disconnect(), kSuccess);
    }

    std::shared_ptr<gcs::testing::MockClient> mock_client;

    template<typename Func, typename ReturnType>
    void CheckInvalidURIs(Func f, ReturnType expect)
    {
        // null pointer
        ASSERT_EQ(f(nullptr), expect);

        // name without "gs://" prefix
        ASSERT_EQ(f("noprefix"), expect);

        // name with correct prefix, but no clear bucket and object names
        ASSERT_EQ(f("gs://not_valid"), expect);

        // name with only bucket name
        ASSERT_EQ(f("gs://only_bucket_name/"), expect);

        // valid URI, but only object name and assuming global bucket name is not set
        ASSERT_EQ(f("gs:///no_bucket"), expect);
    }

    template<typename Func, typename Arg, typename ReturnType>
    void CheckInvalidURIs(Func f, Arg arg, ReturnType expect)
    {
        // null pointer
        ASSERT_EQ(f(nullptr, arg), expect);

        // name without "gs://" prefix
        ASSERT_EQ(f("noprefix", arg), expect);

        // name with correct prefix, but no clear bucket and object names
        ASSERT_EQ(f("gs://not_valid", arg), expect);

        // name with only bucket name
        ASSERT_EQ(f("gs://only_bucket_name/", arg), expect);

        // valid URI, but only object name and assuming global bucket name is not set
        ASSERT_EQ(f("gs:///no_bucket", arg), expect);
    }

    void PrepareListObjects(LOReturnType result)
    {
        EXPECT_CALL(*mock_client, ListObjects).WillOnce(Return<LOReturnType>(std::move(result)));
    }

    struct MultiPartFile
    {
        std::string bucketname_;
        std::string filename_;
        long long offset_{ 0 };
        // Added for multifile support
        long long commonHeaderLength_{ 0 };
        std::vector<std::string> filenames_;
        std::vector<long long int> cumulativeSize_;
        long long total_size_{ 0 };
    };

    enum class HandleType { kRead, kWrite, kAppend };

    using ReaderPtr = std::unique_ptr<MultiPartFile>;
    using WriterPtr = std::unique_ptr<gcs::ObjectWriteStream>;

    union ClientVariant
    {
        ReaderPtr reader;
        WriterPtr writer;

        //  no default ctor is allowed since member have non trivial ctors
        //  the chosen variant must be initialized by placement new
        explicit ClientVariant(HandleType type)
        {
            switch (type)
            {
            case HandleType::kWrite:
                new (&writer) WriterPtr;
                break;
            case HandleType::kAppend:
            case HandleType::kRead:
            default:
                new (&reader) ReaderPtr;
                break;
            }
        }

        ~ClientVariant() {}
    };

    struct Handle
    {
        HandleType type;
        ClientVariant var;

        Handle(HandleType p_type)
            : type{ p_type }
            , var(p_type)
        {}

        ~Handle()
        {
            switch (type)
            {
            case HandleType::kRead: var.reader.~ReaderPtr(); break;
            case HandleType::kWrite: var.writer.~WriterPtr(); break;
            case HandleType::kAppend:
            default: break;
            }
        }

        MultiPartFile& Reader() { return *(var.reader); }
        gcs::ObjectWriteStream& Writer() { return *(var.writer); }
    };

    using HandlePtr = std::unique_ptr<Handle>;

    //TODO would be nice to have a generic version, but cannot find the way in C++11
    // 
    // 

    HandlePtr MakeHandlePtrFromReader(ReaderPtr&& reader_ptr)
    {
        HandlePtr h{ new Handle(HandleType::kRead) };
        h->var.reader = std::move(reader_ptr);
        return h;
    }

    HandlePtr MakeHandlePtrFromWriter(WriterPtr&& writer_ptr)
    {
        std::unique_ptr<Handle> h{ new Handle(HandleType::kWrite) };
        h->var.writer = std::move(writer_ptr);
        return h;
    }

    void ResetReader(Handle& handle, ReaderPtr&& file)
    {
        handle.var.reader = std::move(file);// .reset(file);
    }

    void ResetWriter(Handle& handle, WriterPtr&& stream)
    {
        handle.var.writer = std::move(stream);// .reset(stream);
    }

    HandlePtr MakeReaderHandle(ReaderPtr&& file)
    {
        HandlePtr h_ptr(new Handle(HandleType::kRead));
        ResetReader(*h_ptr, std::move(file));
        return h_ptr;
    }

    HandlePtr MakeWriterHandle(WriterPtr&& stream)
    {
        HandlePtr h_ptr(new Handle(HandleType::kWrite));
        ResetWriter(*h_ptr, std::move(stream));
        return h_ptr;
    }

    struct ReadSimulatorParams
    {
        const char* content;
        size_t content_size;
        size_t* offset;
    };


    void* OpenReadOnly()
    {
        return driver_fopen("gs://mock_bucket/mock_file", 'r');
    }


    void OpenSuccess(const MultiPartFile& expected)
    {
        void* res = OpenReadOnly();
        ASSERT_NE(res, nullptr);
        Handle* res_cast{ reinterpret_cast<Handle*>(res) };
        ASSERT_EQ(res_cast->type, HandleType::kRead);
        ASSERT_EQ(*res_cast->var.reader, expected);
        ASSERT_EQ(driver_fclose(res), kSuccess);
    }


    void OpenFailure()
    {
        void* res = OpenReadOnly();
        EXPECT_EQ(res, nullptr);
        ASSERT_EQ(driver_fclose(res), kFailure);
    }


    // simulate the answer to a reading request
    gcs::internal::ReadSourceResult SimulateRead(void* buf, size_t n, ReadSimulatorParams& args)
    {
        size_t& offset = *args.offset;
        const size_t l = std::min(n, args.content_size - offset);
        std::memcpy(buf, args.content + offset, l);
        offset += l;
        return gcs::internal::ReadSourceResult{ l, gcs::internal::HttpResponse{200, {}, {}} };
    }


    // generate the read simulator lambda, parameterised by content, size and offset
    std::function<gcs::internal::ReadSourceResult(void* buf, size_t n)> GenerateReadSimulator(ReadSimulatorParams& args)
    {
        return [&](void* buf, size_t n) { return SimulateRead(buf, n, args); };
    }


    void TestMultifileOpenSuccess(LOReturnType arg, ReadSimulatorParams& mock_file_1, ReadSimulatorParams& mock_file_2, const MultiPartFile& expected)
    {
        PrepareListObjects(std::move(arg));
        EXPECT_CALL(*mock_client, ReadObject)
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_file_1)))
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_file_2)));
        OpenSuccess(expected);

        *mock_file_1.offset = 0;
        *mock_file_2.offset = 0;
    }

};

bool operator==(const GCSDriverTestFixture::MultiPartFile& op1, const GCSDriverTestFixture::MultiPartFile& op2)
{
    return (op1.bucketname_ == op2.bucketname_
        && op1.filename_ == op2.filename_
        && op1.offset_ == op2.offset_
        && op1.commonHeaderLength_ == op2.commonHeaderLength_
        && op1.filenames_ == op2.filenames_
        && op1.cumulativeSize_ == op2.cumulativeSize_
        && op1.total_size_ == op2.total_size_);
}


gcs::ObjectMetadata MakeObjectMetadata(const std::string& bucket_name, const std::string& name, int64_t generation, uint64_t size)
{
    gcs::ObjectMetadata res;
    res.set_bucket(bucket_name);
    res.set_name(name);
    res.set_generation(generation);
    res.set_size(size);
    std::ostringstream id_oss;
    id_oss << bucket_name << '/' << name << '/' << generation;
    res.set_id(id_oss.str());

    return res;
}

gcs::internal::ListObjectsResponse MakeLOR(const std::string& bucket_name, const std::vector<std::string>& names, std::vector<uint64_t> file_sizes)
{
    gcs::internal::ListObjectsResponse res;
    const size_t count{ names.size() };
    for (size_t i = 0; i < count; i++)
    {
        res.items.push_back(MakeObjectMetadata(bucket_name, names[i], 1, file_sizes[i]));
    }
    return res;
}


TEST_F(GCSDriverTestFixture, FileExists)
{
    CheckInvalidURIs(driver_fileExists, kFalse);

    EXPECT_CALL(*mock_client, ListObjects)
        .WillOnce(Return<LOReturnType>(MakeLOR("mock_bucket", { "mock_name" }, { 10 })))  // file exists
        .WillOnce(Return<LOReturnType>(gcs::internal::ListObjectsResponse{}))             // no file found
        .WillOnce(Return<LOReturnType>({}));                                              // return error

    ASSERT_EQ(driver_fileExists("gs://mock_bucket/mock_name"), kTrue);
    ASSERT_EQ(driver_fileExists("gs://mock_bucket/no_match"), kFalse);
    ASSERT_EQ(driver_fileExists("gs://mock_bucket/error"), kFalse);
}

TEST_F(GCSDriverTestFixture, DirExists)
{
    ASSERT_EQ(driver_dirExists(nullptr), kFalse);
    ASSERT_EQ(driver_dirExists("any_name"), kTrue);
}

TEST_F(GCSDriverTestFixture, GetFileSize)
{
    CheckInvalidURIs(driver_getFileSize, -1);

    auto prepare_list_objects = [&](LOReturnType&& result) {
        EXPECT_CALL(*mock_client, ListObjects).WillOnce(Return<LOReturnType>(std::move(result)));
        };

    // dir passed as argument, not a file. same behaviour as "no file found"
    prepare_list_objects(MakeLOR("mock_bucket", {}, {}));
    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/dir_name/"), -1);

    // valid URI, but ListObjects returns unusable data
    prepare_list_objects({});
    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/error"), -1);

    // single file
    constexpr uint64_t expected_size{ 10 };
    prepare_list_objects(MakeLOR("mock_bucket", { "mock_object" }, { expected_size }));
    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), static_cast<long long>(expected_size));


    // tests for multifile cases

    // lambda to simulate the answer to a reading request
    auto simulate_read = [](void* buf, size_t n, const char* content, size_t content_size, size_t& offset) {
        const size_t l = std::min(n, content_size - offset);
        std::memcpy(buf, content + offset, l);
        offset += l;
        return gcs::internal::ReadSourceResult{ l, gcs::internal::HttpResponse{200, {}, {}} };
        };

    // lambda to generate the read simulator lambda, parameterised by content, size and offset
    auto generate_simulator = [&](const char* content, size_t size, size_t& offset) {
        return [&, content, size](void* buf, size_t n) { return simulate_read(buf, n, content, size, offset); };
        };


    // multifile, 2 files, single header

    constexpr auto mock_content_1 = "mock_header\nmock_content_1";
    constexpr auto mock_content_2 = "mock_content_2";
    constexpr size_t mock_header_size{ 12 }; // std::strlen("mock_header\n")    includes end of line char
    constexpr size_t mock_content_1_size{ 26 }; //std::strlen(mock_content_1)
    constexpr size_t mock_content_2_size{ 14 }; //std::strlen(mock_content_2)
    constexpr size_t mock_content_total_size{ mock_content_1_size + mock_content_2_size };

    size_t offset_1{ 0 };
    size_t offset_2{ 0 };


    prepare_list_objects(MakeLOR("mock_bucket", { "mock_file_1", "mock_file_2" }, { mock_content_1_size, mock_content_2_size }));

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(generate_simulator(mock_content_1, mock_content_1_size, offset_1))) //simulate_read_file_1))
        .WillOnce(READ_MOCK_LAMBDA(generate_simulator(mock_content_2, mock_content_2_size, offset_2)));

    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), mock_content_total_size);


    // multifile, 2 files, same header

    constexpr const char* mock_content_3 = "mock_header\nmock_content_3_larger";
    constexpr size_t mock_content_3_size{ 33 };
    constexpr size_t expected_size_common_header{ mock_content_1_size + mock_content_3_size - mock_header_size };

    offset_1 = 0;
    size_t offset_3{ 0 };


    prepare_list_objects(MakeLOR("mock_bucket", { "mock_file_1", "mock_file_3" }, { mock_content_1_size, mock_content_3_size }));

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(generate_simulator(mock_content_1, mock_content_1_size, offset_1)))
        .WillOnce(READ_MOCK_LAMBDA(generate_simulator(mock_content_3, mock_content_3_size, offset_3)));

    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), expected_size_common_header);


    // multifile, with a read failure on first file

    offset_1 = 0;
    offset_3 = 0;


    prepare_list_objects(MakeLOR("mock_bucket", { "mock_file_1", "mock_file_3" }, { mock_content_1_size, mock_content_3_size }));

    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE);
    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), -1);


    //multi file, read failure on second file

    prepare_list_objects(MakeLOR("mock_bucket", { "mock_file_1", "mock_file_3" }, { mock_content_1_size, mock_content_3_size }));

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(generate_simulator(mock_content_1, mock_content_1_size, offset_1)))
        .WillOnce(READ_MOCK_LAMBDA_FAILURE);

    ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), -1);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_InvalidURIs)
{
    CheckInvalidURIs(driver_fopen, 'r', nullptr);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_OneFileSuccess)
{
    MultiPartFile expected_struct{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {10},
        10
    };

    PrepareListObjects(MakeLOR("mock_bucket", { "mock_file" }, { 10 }));
    OpenSuccess(expected_struct);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_OneFileFailure)
{
    PrepareListObjects({});
    OpenFailure();
}

TEST_F(GCSDriverTestFixture, CloseFileFailure)
{
    // try closing a handle not present
    Handle dummy_handle(HandleType::kRead);
    ASSERT_EQ(driver_fclose(&dummy_handle), kFailure);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_TwoFilesNoCommonHeaderSuccess)
{
    constexpr const char* mock_file_0_content = "mock_header\ncontent";
    constexpr size_t mock_file_0_size{ 19 };
    constexpr size_t mock_header_size{ 12 };
    size_t mock_file_0_offset{ 0 };
    ReadSimulatorParams mock_file_0{ mock_file_0_content, mock_file_0_size, &mock_file_0_offset };

    constexpr const char* mock_file_1_content = "content";
    constexpr size_t mock_file_1_size{ 7 };
    size_t mock_file_1_offset{ 0 };
    ReadSimulatorParams mock_file_1{ mock_file_1_content, mock_file_1_size, &mock_file_1_offset };

    LOReturnType file0_file1_response = MakeLOR("mock_bucket", { "mock_file_0", "mock_file_1" }, { mock_file_0_size, mock_file_1_size });


    constexpr size_t total_size{ mock_file_0_size + mock_file_1_size };

    MultiPartFile expected_struct{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file_0", "mock_file_1"},
        {static_cast<long long>(mock_file_0_size), static_cast<long long>(total_size)},
        static_cast<long long>(total_size)
    };


    TestMultifileOpenSuccess(file0_file1_response, mock_file_0, mock_file_1, expected_struct);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_TwoFilesCommonHeaderSuccess)
{
    constexpr const char* mock_file_0_content = "mock_header\ncontent";
    constexpr size_t mock_file_0_size{ 19 };
    constexpr size_t mock_header_size{ 12 };
    size_t mock_file_0_offset{ 0 };
    ReadSimulatorParams mock_file_0{ mock_file_0_content, mock_file_0_size, &mock_file_0_offset };

    constexpr const char* mock_file_1_content = "mock_header\ncontent";
    constexpr size_t mock_file_1_size{ 19 };
    size_t mock_file_1_offset{ 0 };
    ReadSimulatorParams mock_file_1{ mock_file_1_content, mock_file_1_size, &mock_file_1_offset };

    LOReturnType file0_file1_response = MakeLOR("mock_bucket", { "mock_file_0", "mock_file_1" }, { mock_file_0_size, mock_file_1_size });


    constexpr size_t total_size{ mock_file_0_size + mock_file_1_size - mock_header_size };

    MultiPartFile expected_struct{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file_0", "mock_file_1"},
        {static_cast<long long>(mock_file_0_size), static_cast<long long>(total_size)},
        static_cast<long long>(total_size)
    };


    TestMultifileOpenSuccess(file0_file1_response, mock_file_0, mock_file_1, expected_struct);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_TwoFilesNoCommonHeaderFailureOnFirstRead)
{
    constexpr size_t mock_file_0_size{ 19 };

    constexpr size_t mock_file_1_size{ 7 };

    LOReturnType file0_file1_response = MakeLOR("mock_bucket", { "mock_file_0", "mock_file_1" }, { mock_file_0_size, mock_file_1_size });


    PrepareListObjects(file0_file1_response);
    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE);
    OpenFailure();
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_TwoFilesNoCommonHeaderFailureOnSecondRead)
{
    constexpr const char* mock_file_0_content = "mock_header\ncontent";
    constexpr size_t mock_file_0_size{ 19 };
    constexpr size_t mock_header_size{ 12 };
    size_t mock_file_0_offset{ 0 };
    ReadSimulatorParams mock_file_0{ mock_file_0_content, mock_file_0_size, &mock_file_0_offset };

    constexpr size_t mock_file_1_size{ 7 };

    LOReturnType file0_file1_response = MakeLOR("mock_bucket", { "mock_file_0", "mock_file_1" }, { mock_file_0_size, mock_file_1_size });


    PrepareListObjects(file0_file1_response);
    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_file_0)))
        .WillOnce(READ_MOCK_LAMBDA_FAILURE);
    OpenFailure();
}

TEST_F(GCSDriverTestFixture, Seek_BadArgs)
{
    constexpr int seek_failure{ -1 };

    ReaderPtr one_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {0},
        0
        });

    /*auto one_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {0},
        0
    };*/

    HandlePtr handle_reader = MakeReaderHandle(std::move(one_file));

    Handle dummy_writer(HandleType::kWrite);

    ASSERT_EQ(driver_fseek(nullptr, 0, std::ios::beg), seek_failure);
    ASSERT_EQ(driver_fseek(handle_reader.get(), 0, -1), seek_failure);           // unrecognised whence
    ASSERT_EQ(driver_fseek(&dummy_writer, 0, std::ios::beg), seek_failure);      // attempted seek on writer 
}

TEST_F(GCSDriverTestFixture, SeekFromStart)
{
    struct TestParams
    {
        long long offset;
        int expected_result;
    };

    constexpr int seek_failure{ -1 };
    constexpr int seek_success{ 0 };

    auto test_func = [seek_failure](const std::vector<TestParams> vals, Handle& sample, long long sample_starting_offset) {
        for (const auto& v : vals)
        {
            int res{ 0 };
            ASSERT_NO_THROW(res = driver_fseek(&sample, v.offset, std::ios::beg));
            ASSERT_EQ(res, v.expected_result);
            ASSERT_EQ(sample.Reader().offset_, v.expected_result == seek_failure ? sample_starting_offset : v.offset);
            sample.Reader().offset_ = sample_starting_offset;
        }
        };


    constexpr long long filesize{ 10 };
    constexpr long long starting_offset{ 1 };

    ReaderPtr one_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        starting_offset,
        0,
        {"mock_file"},
        {filesize},
        filesize
        });

    /*auto one_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        starting_offset,
        0,
        {"mock_file"},
        {filesize},
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(one_file));

    std::vector<TestParams> test_values = {
        TestParams{0, seek_success},
        TestParams{5, seek_success},
        TestParams{filesize - 1, seek_success},
        TestParams{filesize, seek_success},
        TestParams{filesize + 1, seek_success},
        TestParams{-1, seek_failure},
        TestParams{std::numeric_limits<long long>::min(), seek_failure},
        TestParams{std::numeric_limits<long long>::max(), seek_success}
    };

    test_func(test_values, *test_reader, starting_offset);

    //special case

    // multifile

    ReaderPtr multi_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file_0", "mock_file_1", "mock_file_3"},
        {filesize, 2 * filesize, 3 * filesize},
        3 * filesize
        });

    /*auto multi_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file_0", "mock_file_1", "mock_file_3"},
        {filesize, 2 * filesize, 3 * filesize},
        3 * filesize
    };*/

    ResetReader(*test_reader, std::move(multi_file));

    std::vector<TestParams> test_values_multifile = {
        TestParams{0, seek_success},
        TestParams{2 * filesize, seek_success},
        TestParams{3 * filesize - 1, seek_success},
        TestParams{3 * filesize, seek_success},
        TestParams{3 * filesize + 1, seek_success},
        TestParams{-1, seek_failure},
        TestParams{std::numeric_limits<long long>::min(), seek_failure},
        TestParams{std::numeric_limits<long long>::max(), seek_success}
    };

    test_func(test_values_multifile, *test_reader, starting_offset);
}

TEST_F(GCSDriverTestFixture, SeekFromCurrentOffset)
{
    struct TestParams
    {
        long long offset;
        int expected_result;
    };

    constexpr int seek_failure{ -1 };
    constexpr int seek_success{ 0 };

    auto test_func = [seek_failure](const std::vector<TestParams> vals, Handle& sample, long long sample_starting_offset) {
        for (const auto& v : vals)
        {
            int res{ 0 };
            ASSERT_NO_THROW(res = driver_fseek(&sample, v.offset, std::ios::cur));
            ASSERT_EQ(res, v.expected_result);
            ASSERT_EQ(sample.Reader().offset_, v.expected_result == seek_failure ? sample_starting_offset : sample_starting_offset + v.offset);
            sample.Reader().offset_ = sample_starting_offset;
        }
        };


    constexpr long long filesize{ 10 };
    constexpr long long starting_offset{ 5 };
    constexpr long long gap_to_end{ filesize - starting_offset - 1 };

    ReaderPtr one_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        starting_offset,
        0,
        {"mock_file"},
        {filesize},
        filesize
        });

    /*auto one_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        starting_offset,
        0,
        {"mock_file"},
        {filesize},
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(one_file));

    std::vector<TestParams> test_values = {
        TestParams{0, seek_success},
        TestParams{-starting_offset , seek_success},
        TestParams{-(starting_offset - 1), seek_success},
        TestParams{gap_to_end - 1, seek_success},
        TestParams{gap_to_end, seek_success},
        TestParams{gap_to_end + 1, seek_success},
        TestParams{-(starting_offset + 1), seek_failure},
        TestParams{std::numeric_limits<long long>::min(), seek_failure},
        TestParams{std::numeric_limits<long long>::max(), seek_failure}
    };

    test_func(test_values, *test_reader, starting_offset);

    // special case: starting offset is 0

    ReaderPtr one_file_special_case(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {filesize},
        filesize
        });

    /*auto one_file_special_case = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {filesize},
        filesize
    };*/

    ResetReader(*test_reader, std::move(one_file_special_case));

    std::vector<TestParams> special_test_values = { TestParams{std::numeric_limits<long long>::max(), seek_success} };
    test_func(special_test_values, *test_reader, 0);
}

TEST_F(GCSDriverTestFixture, SeekFromEnd)
{
    struct TestParams
    {
        long long offset;
        int expected_result;
    };

    constexpr int seek_failure{ -1 };
    constexpr int seek_success{ 0 };

    auto test_func = [seek_failure](const std::vector<TestParams> vals, Handle& sample, long long sample_starting_offset) {
        for (const auto& v : vals)
        {
            int res{ 0 };
            ASSERT_NO_THROW(res = driver_fseek(&sample, v.offset, std::ios::end));
            ASSERT_EQ(res, v.expected_result);
            ASSERT_EQ(sample.Reader().offset_, v.expected_result == seek_failure ? sample_starting_offset : sample_starting_offset + v.offset);
            sample.Reader().offset_ = sample_starting_offset;
        }
        };


    constexpr long long filesize{ 10 };
    constexpr long long starting_offset{ filesize - 1 };

    ReaderPtr one_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        starting_offset,
        0,
        {"mock_file"},
        {filesize},
        filesize
        });

    /*auto one_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        starting_offset,
        0,
        {"mock_file"},
        {filesize},
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(one_file));

    std::vector<TestParams> test_values = {
        TestParams{0, seek_success},
        TestParams{-starting_offset , seek_success},
        TestParams{1, seek_success},
        TestParams{-(starting_offset + 1), seek_failure},
        TestParams{std::numeric_limits<long long>::min(), seek_failure},
        TestParams{std::numeric_limits<long long>::max(), seek_failure}
    };

    test_func(test_values, *test_reader, starting_offset);

    // special case: file of size 0, offset 0

    ReaderPtr one_file_special_case(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {0},
        0
        });

    /*auto one_file_special_case = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        {"mock_file"},
        {0},
        0
    };*/

    ResetReader(*test_reader, std::move(one_file_special_case));

    std::vector<TestParams> special_test_values = { TestParams{std::numeric_limits<long long>::max(), seek_success} };
    test_func(special_test_values, *test_reader, 0);
}

TEST_F(GCSDriverTestFixture, Read_BadArgs)
{
    constexpr long long max_pos{ std::numeric_limits<long long>::max() };

    HandlePtr dummy_handle = MakeReaderHandle(ReaderPtr(new MultiPartFile()));
    char dummy_buff[1];

    // null stream
    ASSERT_EQ(driver_fread(nullptr, 0, 0, nullptr), -1);

    // null buffer
    ASSERT_EQ(driver_fread(nullptr, 0, 0, dummy_handle.get()), -1);

    // size of 0
    ASSERT_EQ(driver_fread(dummy_buff, 0, 0, dummy_handle.get()), -1);

    // size and / or count too large
    constexpr size_t large_size{ static_cast<size_t>(std::numeric_limits<long long>::max()) };
    constexpr size_t count{ 4 };
    ASSERT_EQ(driver_fread(dummy_buff, large_size, count, dummy_handle.get()), -1);

    // size and count within numerical limits, but file position would go beyond numerical limit during read
    constexpr long long large_file_size{ max_pos - 1 };
    
    ReaderPtr dummy_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        large_file_size - 1,
        0,
        {"mock_file"},
        {large_file_size},
        large_file_size
        });
    
    /*auto dummy_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        large_file_size - 1,
        0,
        {"mock_file"},
        {large_file_size},
        large_file_size
    };*/

    ResetReader(*dummy_handle, std::move(dummy_file));

    constexpr size_t size{ 10 };

    ASSERT_EQ(driver_fread(dummy_buff, size, count, dummy_handle.get()), -1);

    // read attempt on a writer stream
    HandlePtr dummy_writer = MakeWriterHandle(WriterPtr(new gcs::ObjectWriteStream));
    ASSERT_EQ(driver_fread(dummy_buff, 1, 1, dummy_writer.get()), -1);
}

TEST_F(GCSDriverTestFixture, Read_OneFile)
{
    constexpr const char* mock_content{ "mock_content" };
    constexpr size_t mock_size{ 12 };
    size_t mock_offset{ 0 };
    ReadSimulatorParams mock_read_params{ mock_content, mock_size, &mock_offset };

    constexpr long long filesize{ static_cast<long long>(mock_size) };

    ReaderPtr mock_one_file(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file" },
        { filesize },
        filesize
        });

    /*auto mock_one_file = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file" },
        { filesize },
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(mock_one_file));
    char buff[16] = {};

    constexpr size_t size{ sizeof(uint8_t) };


    // special case: 0 bytes to read
    ASSERT_EQ(driver_fread(buff, size, 0, test_reader.get()), 0);


    //special case: offset > filesize, trying to read at least one byte
    test_reader->Reader().offset_ = filesize + 1;
    //mock_one_file->offset_ = filesize + 1;

    ASSERT_EQ(driver_fread(buff, size, 1, test_reader.get()), -1);


    //basic case: offset 0, 1 byte to read
    test_reader->Reader().offset_ = 0;
    //mock_one_file->offset_ = 0;

    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
    ASSERT_EQ(driver_fread(buff, size, 1, test_reader.get()), 1);
    ASSERT_EQ(buff[0], mock_read_params.content[0]);
    ASSERT_EQ(test_reader->Reader().offset_, 1);



    auto sync_offset = [&](long long off) {
        test_reader->Reader().offset_ = off;
        *mock_read_params.offset = static_cast<size_t>(test_reader->Reader().offset_);
        };

    //basic case: 0 < offset < filesize-1, 1 byte to read
    sync_offset(1);

    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
    ASSERT_EQ(driver_fread(buff, size, 1, test_reader.get()), 1);
    ASSERT_EQ(buff[0], mock_read_params.content[1]);
    ASSERT_EQ(test_reader->Reader().offset_, 2);


    //basic case: offset == filesize - 1, 1 byte to read
    sync_offset(filesize - 1);

    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
    ASSERT_EQ(driver_fread(buff, size, 1, test_reader.get()), 1);
    ASSERT_EQ(buff[0], mock_read_params.content[filesize - 1]);
    ASSERT_EQ(test_reader->Reader().offset_, filesize);


    auto test_one_file_read_n_bytes = [&](long long offset) {
        sync_offset(offset);
        const long long bytes_to_read = filesize - 1 - test_reader->Reader().offset_;
        const size_t cast_btr = static_cast<size_t>(bytes_to_read);

        EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
        ASSERT_EQ(driver_fread(buff, size, cast_btr, test_reader.get()), bytes_to_read);
        ASSERT_EQ(std::strncmp(buff, mock_content + offset, cast_btr), 0);
        ASSERT_EQ(test_reader->Reader().offset_, offset + cast_btr);
        };

    const std::vector<long long> one_file_read_n_bytes_test_values = {
        0,              // offset 0, offset < n < filesize bytes to read
        filesize / 2,   // 0 < offset < filesize-1, 2 < n < filesize - offset bytes to read
    };

    for (long long i : one_file_read_n_bytes_test_values)
    {
        test_one_file_read_n_bytes(i);
    }


    // try reading more bytes than available. must read exactly all remaining bytes from offset and leave offset at filesize
    auto test_try_read_more_bytes_than_available = [&](long long offset) {
        sync_offset(offset);
        const long long available{ filesize - offset };

        EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
        ASSERT_EQ(driver_fread(buff, size, static_cast<size_t>(2 * filesize), test_reader.get()), available);
        ASSERT_EQ(std::strncmp(buff, mock_content + offset, static_cast<size_t>(available)), 0);
        ASSERT_EQ(test_reader->Reader().offset_, filesize);
        };

    // offset tests
    const std::vector<long long> one_file_try_reading_more_than_possible_test_values = {
        0,
        filesize / 2,
        filesize - 1,
    };

    for (long long i : one_file_try_reading_more_than_possible_test_values)
    {
        test_try_read_more_bytes_than_available(i);
    }
}

TEST_F(GCSDriverTestFixture, Read_NFiles_NoCommonHeader)
{
    constexpr const char* mock_content_0{ "mock_header\nmock_content0" };
    constexpr const char* mock_content_1{ "mock_content1" };
    constexpr const char* mock_content_2{ "mock_content2" };

    const size_t mock_size_0{ std::strlen(mock_content_0) };
    const size_t mock_size_1{ std::strlen(mock_content_1) };
    const size_t mock_size_2{ std::strlen(mock_content_2) };

    const long long cast_mock_size_0 = static_cast<long long>(mock_size_0);
    const long long cast_mock_size_1 = static_cast<long long>(mock_size_1);
    const long long cast_mock_size_2 = static_cast<long long>(mock_size_2);

    const long long filesize{ cast_mock_size_0 + cast_mock_size_1 + cast_mock_size_2 };

    ReaderPtr mock_n_files_one_header(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file_0", "mock_file_1", "mock_file_2"},
        { cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1, filesize },
        filesize
        });

    /*auto mock_n_files_one_header = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file_0", "mock_file_1", "mock_file_2"},
        { cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1, filesize },
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(mock_n_files_one_header));

    size_t mock_offset_0{ 0 };
    size_t mock_offset_1{ 0 };
    size_t mock_offset_2{ 0 };

    ReadSimulatorParams mock_read_params_0{ mock_content_0, mock_size_0, &mock_offset_0 };
    ReadSimulatorParams mock_read_params_1{ mock_content_1, mock_size_1, &mock_offset_1 };
    ReadSimulatorParams mock_read_params_2{ mock_content_2, mock_size_2, &mock_offset_2 };

    char* buff = new char[2 * static_cast<size_t>(filesize)] {};

    constexpr size_t size{ sizeof(uint8_t) };

    // read 1 byte from start of an intermediate file
    test_reader->Reader().offset_ = cast_mock_size_0;

    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
    EXPECT_EQ(driver_fread(buff, size, 1, test_reader.get()), 1);
    EXPECT_EQ(test_reader->Reader().offset_, cast_mock_size_0 + 1);
    EXPECT_EQ(buff[0], mock_read_params_1.content[0]);

    // read a small amount of bytes overlapping two file fragments
    test_reader->Reader().offset_ = cast_mock_size_0 - 1;
    *mock_read_params_0.offset = mock_size_0 - 1;
    *mock_read_params_1.offset = 0;

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
    EXPECT_EQ(driver_fread(buff, size, 2, test_reader.get()), 2);
    EXPECT_EQ(test_reader->Reader().offset_, cast_mock_size_0 + 1);
    EXPECT_EQ(buff[0], mock_content_0[mock_size_0 - 1]);
    EXPECT_EQ(buff[1], mock_content_1[0]);

    // read more than mock_size_0 and less than mock_size_0 + mock_size_1 bytes from the start of the file
    test_reader->Reader().offset_ = 0;
    *mock_read_params_0.offset = 0;
    *mock_read_params_1.offset = 0;

    long long to_read = cast_mock_size_0 + cast_mock_size_1 - 1;

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
    EXPECT_EQ(driver_fread(buff, size, static_cast<size_t>(to_read), test_reader.get()), to_read);
    EXPECT_EQ(test_reader->Reader().offset_, to_read);
    EXPECT_EQ(std::strncmp(buff, mock_content_0, mock_size_0), 0);
    EXPECT_EQ(std::strncmp(buff + mock_size_0, mock_content_1, mock_size_1 - 1), 0);


    // tests reading the whole file

    auto test_whole_file = [&](long long bytes_to_read) {
        test_reader->Reader().offset_ = 0;
        *mock_read_params_0.offset = 0;
        *mock_read_params_1.offset = 0;
        *mock_read_params_2.offset = 0;

        EXPECT_CALL(*mock_client, ReadObject)
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)))
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_2)));
        EXPECT_EQ(driver_fread(buff, size, static_cast<size_t>(bytes_to_read), test_reader.get()), filesize);
        EXPECT_EQ(test_reader->Reader().offset_, filesize);
        EXPECT_EQ(std::strlen(buff), filesize);
        EXPECT_EQ(std::strncmp(buff, mock_content_0, mock_size_0), 0);
        EXPECT_EQ(std::strncmp(buff + mock_size_0, mock_content_1, mock_size_1), 0);
        EXPECT_EQ(std::strncmp(buff + mock_size_0 + mock_size_1, mock_content_2, mock_size_2), 0);
        };

    // read the whole file
    test_whole_file(filesize);

    // try to read more than the whole file
    test_whole_file(filesize + 1);

    delete[] buff;
}

TEST_F(GCSDriverTestFixture, Read_NFiles_CommonHeader)
{
    constexpr const char* mock_content_0{ "mock_header\nmock_content0" };
    constexpr const char* mock_content_1{ "mock_header\nmock_content1" };
    constexpr const char* mock_content_2{ "mock_header\nmock_content2" };

    const size_t hdr_size{ std::strlen("mock_header\n") };
    const size_t mock_size_0{ std::strlen(mock_content_0) };
    const size_t mock_size_1{ std::strlen(mock_content_1) };
    const size_t mock_size_2{ std::strlen(mock_content_2) };

    const long long cast_hdr_size = static_cast<long long>(hdr_size);
    const long long cast_mock_size_0 = static_cast<long long>(mock_size_0);
    const long long cast_mock_size_1 = static_cast<long long>(mock_size_1);
    const long long cast_mock_size_2 = static_cast<long long>(mock_size_2);

    const long long filesize{ cast_mock_size_0 + cast_mock_size_1 - cast_hdr_size + cast_mock_size_2 - cast_hdr_size };

    ReaderPtr mock_n_files_common_header(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file_0", "mock_file_1", "mock_file_2"},
        { cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1 - cast_hdr_size, filesize },
        filesize
        });

    /*auto mock_n_files_common_header = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file_0", "mock_file_1", "mock_file_2"},
        { cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1 - cast_hdr_size, filesize },
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(mock_n_files_common_header));

    size_t mock_offset_0{ 0 };
    size_t mock_offset_1{ hdr_size };
    size_t mock_offset_2{ hdr_size };

    ReadSimulatorParams mock_read_params_0{ mock_content_0, mock_size_0, &mock_offset_0 };
    ReadSimulatorParams mock_read_params_1{ mock_content_1, mock_size_1, &mock_offset_1 };
    ReadSimulatorParams mock_read_params_2{ mock_content_2, mock_size_2, &mock_offset_2 };

    char* buff = new char[2 * static_cast<size_t>(filesize)] {};

    constexpr size_t size{ sizeof(uint8_t) };

    // read 1 byte from start of an intermediate file
    test_reader->Reader().offset_ = cast_mock_size_0;

    EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
    EXPECT_EQ(driver_fread(buff, size, 1, test_reader.get()), 1);
    EXPECT_EQ(test_reader->Reader().offset_, cast_mock_size_0 + 1);
    EXPECT_EQ(buff[0], mock_read_params_1.content[hdr_size]);

    // read a small amount of bytes overlapping two file fragments
    test_reader->Reader().offset_ = cast_mock_size_0 - 1;
    *mock_read_params_0.offset = mock_size_0 - 1;
    *mock_read_params_1.offset = hdr_size;

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
    EXPECT_EQ(driver_fread(buff, size, 2, test_reader.get()), 2);
    EXPECT_EQ(test_reader->Reader().offset_, cast_mock_size_0 + 1);
    EXPECT_EQ(buff[0], mock_content_0[mock_size_0 - 1]);
    EXPECT_EQ(buff[1], mock_content_1[hdr_size]);

    // read more than mock_size_0 and less than mock_size_0 + mock_size_1 (excluding header size) bytes from the start of the file
    test_reader->Reader().offset_ = 0;
    *mock_read_params_0.offset = 0;
    *mock_read_params_1.offset = hdr_size;

    const size_t read_in_file1 = mock_size_1 - hdr_size - 1;
    const long long to_read = cast_mock_size_0 + static_cast<long long>(read_in_file1);

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
    EXPECT_EQ(driver_fread(buff, size, static_cast<size_t>(to_read), test_reader.get()), to_read);
    EXPECT_EQ(test_reader->Reader().offset_, to_read);
    EXPECT_EQ(std::strncmp(buff, mock_content_0, mock_size_0), 0);
    EXPECT_EQ(std::strncmp(buff + mock_size_0, mock_content_1 + hdr_size, read_in_file1), 0);


    // tests reading the whole file

    auto test_whole_file = [&](long long bytes_to_read) {
        test_reader->Reader().offset_ = 0;
        *mock_read_params_0.offset = 0;
        *mock_read_params_1.offset = hdr_size;
        *mock_read_params_2.offset = hdr_size;

        EXPECT_CALL(*mock_client, ReadObject)
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)))
            .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_2)));
        EXPECT_EQ(driver_fread(buff, size, static_cast<size_t>(bytes_to_read), test_reader.get()), filesize);
        EXPECT_EQ(test_reader->Reader().offset_, filesize);
        EXPECT_EQ(std::strlen(buff), filesize);
        EXPECT_EQ(std::strncmp(buff, mock_content_0, mock_size_0), 0);
        EXPECT_EQ(std::strncmp(buff + mock_size_0, mock_content_1 + hdr_size, mock_size_1 - hdr_size), 0);
        EXPECT_EQ(std::strncmp(buff + mock_size_0 + mock_size_1 - hdr_size, mock_content_2 + hdr_size, mock_size_2 - hdr_size), 0);
        };

    // read the whole file
    test_whole_file(filesize);

    // try to read more than the whole file
    test_whole_file(filesize + 1);

    delete[] buff;
}

TEST_F(GCSDriverTestFixture, Read_NFiles_ReadFailures)
{
    constexpr const char* mock_content_0{ "mock_header\nmock_content0" };
    constexpr const char* mock_content_1{ "mock_content1" };
    constexpr const char* mock_content_2{ "mock_content2" };

    const size_t mock_size_0{ std::strlen(mock_content_0) };
    const size_t mock_size_1{ std::strlen(mock_content_1) };
    const size_t mock_size_2{ std::strlen(mock_content_2) };

    const long long cast_mock_size_0 = static_cast<long long>(mock_size_0);
    const long long cast_mock_size_1 = static_cast<long long>(mock_size_1);
    const long long cast_mock_size_2 = static_cast<long long>(mock_size_2);

    const long long filesize{ cast_mock_size_0 + cast_mock_size_1 + cast_mock_size_2 };

    ReaderPtr mock_n_files_one_header(new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file_0", "mock_file_1", "mock_file_2"},
        { cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1, filesize },
        filesize
        });

    /*auto mock_n_files_one_header = new MultiPartFile{
        "mock_bucket",
        "mock_file",
        0,
        0,
        { "mock_file_0", "mock_file_1", "mock_file_2"},
        { cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1, filesize },
        filesize
    };*/

    HandlePtr test_reader = MakeReaderHandle(std::move(mock_n_files_one_header));

    size_t mock_offset_0{ 0 };
    size_t mock_offset_1{ 0 };
    size_t mock_offset_2{ 0 };

    ReadSimulatorParams mock_read_params_0{ mock_content_0, mock_size_0, &mock_offset_0 };
    ReadSimulatorParams mock_read_params_1{ mock_content_1, mock_size_1, &mock_offset_1 };
    ReadSimulatorParams mock_read_params_2{ mock_content_2, mock_size_2, &mock_offset_2 };

    char* buff = new char[2 * static_cast<size_t>(filesize)] {};

    constexpr size_t size{ sizeof(uint8_t) };

    /*auto sync_offset = [&](long long offset) {
        mock_n_files_one_header.offset_ = offset;
        *mock_read_params_0.offset = static_cast<size_t>(offset);
        };

    auto check_after_fail = [&](long long expected_offset) {
        EXPECT_EQ(mock_n_files_one_header.offset_, expected_offset);
        EXPECT_EQ(std::strlen(buff), 0);
        };*/

    auto test_func = [&](long long offset_before_read, size_t to_read, std::function<void()> set_mock_calls) {
        test_reader->Reader().offset_ = offset_before_read;
        *mock_read_params_0.offset = static_cast<size_t>(offset_before_read);

        set_mock_calls();

        EXPECT_EQ(driver_fread(buff, size, to_read, test_reader.get()), -1);
        EXPECT_EQ(test_reader->Reader().offset_, offset_before_read);
        };

    /*auto test_fail_first_read = [&](long long offset_before_read) {
        sync_offset(offset_before_read);

        EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE);
        EXPECT_EQ(driver_fread(buff, size, 1, &mock_n_files_one_header), -1);

        check_after_fail(offset_before_read);
        };*/

        //fail at first read
    const std::vector<long long> fail_first_read_test_values = { 0, cast_mock_size_0 - 1, cast_mock_size_0 };
    for (long long i : fail_first_read_test_values)
    {
        test_func(i, 1, [&]() { EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE); });
    }

    //fail at subsequent read
    const std::vector<long long> fail_other_read_test_values = { 0, cast_mock_size_0 - 1 };
    for (long long i : fail_first_read_test_values)
    {
        test_func(i, static_cast<size_t>(filesize), [&]() {
            EXPECT_CALL(*mock_client, ReadObject)
                .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
                .WillOnce(READ_MOCK_LAMBDA_FAILURE);
            });
    }

    delete[] buff;
}