#include <gtest/gtest.h>
#include "google/cloud/storage/testing/mock_client.h"

#include <array>
#include <iostream>
#include <sstream>
#include <memory>

#include "../src/gcsplugin.h"

namespace gc = ::google::cloud;
namespace gcs = gc::storage;

using ::testing::Return;
using LOReturnType = gc::StatusOr<gcs::internal::ListObjectsResponse>;

constexpr int kSuccess{ 0 };
constexpr int kFailure{ 1 };

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


class GCSDriverTestFixture : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        mock_client = std::make_shared<gcs::testing::MockClient>();
        auto client = gcs::testing::UndecoratedClientFromMock(mock_client);
        test_setClient(std::move(client));
    }

    static void TearDownTestSuite()
    {
        ASSERT_EQ(driver_disconnect(), kSuccess);
    }

    static std::shared_ptr<gcs::testing::MockClient> mock_client;

    template<typename Func>
    void CheckInvalidURIs(Func f, int expect)
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
};

std::shared_ptr<gcs::testing::MockClient> GCSDriverTestFixture::mock_client = nullptr;

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
    constexpr std::array<long long, 4> expected_sizes = {
        5585568,	// samples/Adult/Adult.txt
        5634411,	// split/Adult/Adult-split-0[0-5].txt
        5585576,	// bq_export/Adult/*.txt
        5634411		// split/Adult_subsplit/**/Adult-split-*.txt
    };

    for (size_t i = 0; i < test_files.size(); i++)
    {
        ASSERT_EQ(driver_getFileSize(test_files[i]), expected_sizes[i]);
    }

	//multifile _ header line at each file
	constexpr const char* test_multifile_glob = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/*.txt";
	constexpr long long expected_size_glob{ 5585576 };
	ASSERT_EQ(driver_getFileSize(test_multifile_glob), expected_size_glob);

	//mutifile _ one header _ double star glob pattern
	constexpr const char* test_doublestar_glob = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**/Adult-split-*.txt";
	//sample has same size as multifile _ one header line
	ASSERT_EQ(driver_getFileSize(test_doublestar_glob), expected_size_range);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose)
{
    for (const char* file : test_files)
    {
        void* stream = driver_fopen(file, 'r');
        ASSERT_NE(stream, nullptr);
        ASSERT_EQ(driver_fclose(stream), kSuccess);
    }

    //fail on dir
    ASSERT_EQ(driver_fopen(test_dir_name, 'r'), nullptr);

    //fail close on null ptr
    ASSERT_EQ(driver_fclose(nullptr), kFailure);
}