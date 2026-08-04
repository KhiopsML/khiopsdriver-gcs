#include "gcsplugin.h"
#include "gcsplugin_internal.h"

#include <array>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>

#include <boost/process/v1/environment.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "google/cloud/storage/testing/mock_client.h"
#include <gtest/gtest.h>

using namespace gcsplugin;

namespace gc = ::google::cloud;
namespace gcs = gc::storage;

using ::testing::Return;
using LOReturnType = gc::StatusOr<gcs::internal::ListObjectsResponse>;

// constexpr const char *test_single_file =
//     "gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt";
// constexpr const char *test_range_file_one_header =
//     "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult/"
//     "Adult-split-0[0-5].txt";
// constexpr const char *test_glob_file_header_each =
//     "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/*.txt";
// constexpr const char *test_double_glob_header_each =
//     "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**/"
//     "Adult-split-*.txt";

#define READ_MOCK_LAMBDA(read_sim)                                             \
  [&](gcs::internal::ReadObjectRangeRequest const &request) {                  \
    EXPECT_EQ(request.bucket_name(), "mock_bucket") << request;                \
    std::unique_ptr<gcs::testing::MockObjectReadSource> mock_source{           \
        new gcs::testing::MockObjectReadSource};                               \
    ::testing::InSequence seq;                                                 \
    EXPECT_CALL(*mock_source, IsOpen()).WillRepeatedly(Return(true));          \
    EXPECT_CALL(*mock_source, Read).WillOnce((read_sim));                      \
    EXPECT_CALL(*mock_source, IsOpen()).WillRepeatedly(Return(false));         \
                                                                               \
    return gc::make_status_or<                                                 \
        std::unique_ptr<gcs::internal::ObjectReadSource>>(                     \
        std::move(mock_source));                                               \
  }

#define READ_MOCK_LAMBDA_FAILURE                                               \
  [](gcs::internal::ReadObjectRangeRequest const &request) {                   \
    EXPECT_EQ(request.bucket_name(), "mock_bucket") << request;                \
    std::unique_ptr<gcs::testing::MockObjectReadSource> mock_source{           \
        new gcs::testing::MockObjectReadSource};                               \
    ::testing::InSequence seq;                                                 \
    EXPECT_CALL(*mock_source, IsOpen).WillRepeatedly(Return(true));            \
    EXPECT_CALL(*mock_source, Read)                                            \
        .WillOnce(Return(                                                      \
            google::cloud::Status(google::cloud::StatusCode::kInvalidArgument, \
                                  "Invalid Argument")));                       \
    EXPECT_CALL(*mock_source, IsOpen).WillRepeatedly(Return(false));           \
                                                                               \
    return google::cloud::make_status_or<                                      \
        std::unique_ptr<gcs::internal::ObjectReadSource>>(                     \
        std::move(mock_source));                                               \
  }

// Forward declaration
gcs::ObjectMetadata MakeObjectMetadata(const std::string &bucket_name,
                                       const std::string &name,
                                       int64_t generation, uint64_t size);

class GCSDriverTestFixture : public ::testing::Test {
protected:
  void SetUp() override {
    mock_client = std::make_shared<gcs::testing::MockClient>();

    ON_CALL(*mock_client, GetObjectMetadata)
        .WillByDefault([](gcs::internal::GetObjectMetadataRequest const &req) {
          return MakeObjectMetadata(req.bucket_name(), req.object_name(),
                                    /*generation*/ 1, /*size*/ 0);
        });

    auto client = gcs::testing::UndecoratedClientFromMock(mock_client);
    test_setClient(std::move(client));
  }

  void TearDown() override {
    GetHandles()->clear();
    test_unsetClient();
  }

public:
  static constexpr const char *mock_bucket = "mock_bucket";
  static constexpr const char *mock_object = "mock_object";
  static constexpr const char *mock_uri = "gs://mock_bucket/mock_object";

  static void TearDownTestSuite() {
    ASSERT_EQ(driver_disconnect(), kOtherSuccess);
  }

  std::shared_ptr<gcs::testing::MockClient> mock_client;

  template <typename Func, typename ReturnType>
  void CheckInvalidURIs(Func f, ReturnType expect) {
    // null pointer
    ASSERT_EQ(f(nullptr), expect);

    // name without "gs://" prefix
    ASSERT_EQ(f("noprefix"), expect);

    // name with correct prefix, but no clear bucket and object names
    ASSERT_EQ(f("gs://not_valid"), expect);

    // name with only bucket name
    ASSERT_EQ(f("gs://only_bucket_name/"), expect);

    // valid URI, but only object name and assuming global bucket name is not
    // set
    ASSERT_EQ(f("gs:///no_bucket"), expect);
  }

  template <typename Func, typename Arg, typename ReturnType>
  void CheckInvalidURIs(Func f, Arg arg, ReturnType expect) {
    // null pointer
    ASSERT_EQ(f(nullptr, arg), expect);

    // name without "gs://" prefix
    ASSERT_EQ(f("noprefix", arg), expect);

    // name with correct prefix, but no clear bucket and object names
    ASSERT_EQ(f("gs://not_valid", arg), expect);

    // name with only bucket name
    ASSERT_EQ(f("gs://only_bucket_name/", arg), expect);

    // valid URI, but only object name and assuming global bucket name is not
    // set
    ASSERT_EQ(f("gs:///no_bucket", arg), expect);
  }

  void PrepareListObjects(LOReturnType result) {
    EXPECT_CALL(*mock_client, ListObjects)
        .WillOnce(Return<LOReturnType>(std::move(result)));
  }

  HandleContainer *GetHandles() {
    return reinterpret_cast<HandleContainer *>(test_getActiveHandles());
  }

  void CheckHandlesEmpty() { ASSERT_TRUE(GetHandles()->empty()); }
  void CheckHandlesSize(size_t size) { ASSERT_EQ(GetHandles()->size(), size); }

  void *OpenReadOnly() {
    return driver_fopen("gs://mock_bucket/mock_file", 'r');
  }

  void *OpenWriteOnly() { return driver_fopen(mock_uri, 'w'); }

  void OpenSuccess(const Reader &expected) {
    void *res = OpenReadOnly();
    ASSERT_NE(res, nullptr);

    CheckHandlesSize(1);

    Handle *res_cast{reinterpret_cast<Handle *>(res)};
    ASSERT_EQ(res_cast->type, HandleType::kRead);
    ASSERT_EQ(res_cast->GetReader(), expected);
  }

  void OpenFailure() {
    void *res = OpenReadOnly();
    EXPECT_EQ(res, nullptr);
    CheckHandlesEmpty();
  }

  // simulate the answer to a reading request
  struct ReadSimulatorParams {
    const char *content;
    size_t content_size;
    size_t *offset;
  };

  gcs::internal::ReadSourceResult SimulateRead(void *buf, size_t n,
                                               ReadSimulatorParams &args) {
    size_t &offset = *args.offset;
    const size_t l = std::min(n, args.content_size - offset);
    std::memcpy(buf, args.content + offset, l);
    offset += l;
    return gcs::internal::ReadSourceResult{
        l, gcs::internal::HttpResponse{200, {}, {}}};
  }

  // generate the read simulator lambda, parameterised by content, size and
  // offset
  std::function<gcs::internal::ReadSourceResult(void *buf, size_t n)>
  GenerateReadSimulator(ReadSimulatorParams &args) {
    return [&](void *buf, size_t n) { return SimulateRead(buf, n, args); };
  }

  void TestMultifileOpenSuccess(LOReturnType arg,
                                ReadSimulatorParams &mock_file_1,
                                ReadSimulatorParams &mock_file_2,
                                const Reader &expected) {
    PrepareListObjects(std::move(arg));
    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_file_1)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_file_2)));
    OpenSuccess(expected);

    *mock_file_1.offset = 0;
    *mock_file_2.offset = 0;
  }

  using DriverState = std::vector<Handle *>;

  DriverState RecordDriverState() {
    const auto handles = GetHandles();
    DriverState res;
    res.reserve(handles->size());

    for (const auto &h : *handles) {
      res.push_back(h.get());
    }

    return res;
  }
};

gcs::ObjectMetadata MakeObjectMetadata(const std::string &bucket_name,
                                       const std::string &name,
                                       int64_t generation, uint64_t size) {
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

gcs::internal::ListObjectsResponse
MakeLOR(const std::string &bucket_name, const std::vector<std::string> &names,
        std::vector<uint64_t> file_sizes) {
  gcs::internal::ListObjectsResponse res;
  const size_t count{names.size()};
  for (size_t i = 0; i < count; i++) {
    res.items.push_back(
        MakeObjectMetadata(bucket_name, names[i], 1, file_sizes[i]));
  }
  return res;
}

TEST_F(GCSDriverTestFixture, FileExists) {
  CheckInvalidURIs(driver_fileExists, kFalse);

  ON_CALL(*mock_client, ListObjects)
      .WillByDefault(Return<LOReturnType>(
          MakeLOR("mock_bucket", {"mock_name"}, {10}))); // file exists
  ASSERT_EQ(driver_fileExists("gs://mock_bucket/mock_name"), kTrue);
  ON_CALL(*mock_client, ListObjects)
      .WillByDefault(Return<LOReturnType>(
          gcs::internal::ListObjectsResponse{})); // no file found
  ASSERT_EQ(driver_fileExists("gs://mock_bucket/no_match"), kFalse);
  ON_CALL(*mock_client, ListObjects)
      .WillByDefault(Return<LOReturnType>({})); // return error

  ASSERT_EQ(driver_fileExists("gs://mock_bucket/error"), kFalse);
}

TEST_F(GCSDriverTestFixture, DirExists) {
  ASSERT_EQ(driver_dirExists(nullptr), kFalse);
  ASSERT_EQ(driver_dirExists("any_name"), kFalse);

  ASSERT_EQ(driver_dirExists("gs://mock_bucket/dir/"), kTrue);

  EXPECT_CALL(*mock_client, GetObjectMetadata)
      .WillOnce(Return(gc::Status(gc::StatusCode::kNotFound, "not found")));
  EXPECT_CALL(*mock_client, ListObjects)
      .WillOnce(Return<LOReturnType>(gcs::internal::ListObjectsResponse{}));
  ASSERT_EQ(driver_dirExists("gs://mock_bucket/missing_dir/"), kFalse);
}

// lambda to simulate the answer to a reading request
auto simulate_read = [](void *buf, size_t n, const char *content,
                        size_t content_size, size_t &offset) {
  const size_t l = std::min(n, content_size - offset);
  std::memcpy(buf, content + offset, l);
  offset += l;
  return gcs::internal::ReadSourceResult{
      l, gcs::internal::HttpResponse{200, {}, {}}};
};

TEST_F(GCSDriverTestFixture, GetFileSize) {
  EXPECT_CALL(*mock_client, ListObjects).Times(2);

  CheckInvalidURIs(driver_getFileSize, -1);

  auto prepare_list_objects = [&](LOReturnType &&result) {
    EXPECT_CALL(*mock_client, ListObjects)
        .WillOnce(Return<LOReturnType>(std::move(result)));
  };

  // dir passed as argument, not a file. same behaviour as "no file found"
  prepare_list_objects(MakeLOR("mock_bucket", {}, {}));
  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/dir_name/"), -1);

  // valid URI, but ListObjects returns unusable data
  prepare_list_objects({});
  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/error"), -1);

  // single file
  constexpr uint64_t expected_size{10};
  prepare_list_objects(
      MakeLOR("mock_bucket", {"mock_object"}, {expected_size}));
  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"),
            static_cast<long long>(expected_size));
}

// tests for multifile cases
TEST_F(GCSDriverTestFixture, GetFileSizeMultifile) {

  // lambda to generate the read simulator lambda, parameterised by content,
  // size and offset
  auto generate_simulator = [&](const char *content, size_t size,
                                size_t &offset) {
    return [&, content, size](void *buf, size_t n) {
      return simulate_read(buf, n, content, size, offset);
    };
  };

  auto prepare_list_objects = [&](LOReturnType &&result) {
    EXPECT_CALL(*mock_client, ListObjects)
        .WillOnce(Return<LOReturnType>(std::move(result)));
  };

  // multifile, 2 files, single header

  constexpr auto mock_content_1 = "mock_header\nmock_content_1";
  constexpr auto mock_content_2 = "mock_content_2";
  constexpr size_t mock_header_size{
      12}; // std::strlen("mock_header\n")    includes end of line char
  constexpr size_t mock_content_1_size{26}; // std::strlen(mock_content_1)
  constexpr size_t mock_content_2_size{14}; // std::strlen(mock_content_2)
  constexpr size_t mock_content_total_size{mock_content_1_size +
                                           mock_content_2_size};

  size_t offset_1{0};
  size_t offset_2{0};

  prepare_list_objects(MakeLOR("mock_bucket", {"mock_file_1", "mock_file_2"},
                               {mock_content_1_size, mock_content_2_size}));

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(
          generate_simulator(mock_content_1, mock_content_1_size,
                             offset_1))) // simulate_read_file_1))
      .WillOnce(READ_MOCK_LAMBDA(
          generate_simulator(mock_content_2, mock_content_2_size, offset_2)));

  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"),
            mock_content_total_size);

  // multifile, 2 files, same header

  constexpr const char *mock_content_3 = "mock_header\nmock_content_3_larger";
  constexpr size_t mock_content_3_size{33};
  constexpr size_t expected_size_common_header{
      mock_content_1_size + mock_content_3_size - mock_header_size};

  offset_1 = 0;
  size_t offset_3{0};

  prepare_list_objects(MakeLOR("mock_bucket", {"mock_file_1", "mock_file_3"},
                               {mock_content_1_size, mock_content_3_size}));

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(
          generate_simulator(mock_content_1, mock_content_1_size, offset_1)))
      .WillOnce(READ_MOCK_LAMBDA(
          generate_simulator(mock_content_3, mock_content_3_size, offset_3)));

  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"),
            expected_size_common_header);

  // multifile, with a read failure on first file

  offset_1 = 0;
  offset_3 = 0;

  prepare_list_objects(MakeLOR("mock_bucket", {"mock_file_1", "mock_file_3"},
                               {mock_content_1_size, mock_content_3_size}));

  EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE);
  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), -1);

  // multi file, read failure on second file

  prepare_list_objects(MakeLOR("mock_bucket", {"mock_file_1", "mock_file_3"},
                               {mock_content_1_size, mock_content_3_size}));

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(
          generate_simulator(mock_content_1, mock_content_1_size, offset_1)))
      .WillOnce(READ_MOCK_LAMBDA_FAILURE);

  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"), -1);
}

TEST_F(GCSDriverTestFixture, GetFileSizeMultifileSampling) {

  auto generate_read_from_start_simulator = [&](const char *content,
                                                size_t size) {
    return [&, content, size](void *buf, size_t n) {
      size_t offset{0};
      return simulate_read(buf, n, content, size, offset);
    };
  };

  auto prepare_list_objects = [&](LOReturnType &&result) {
    EXPECT_CALL(*mock_client, ListObjects)
        .WillOnce(Return<LOReturnType>(std::move(result)));
  };

  // multifile, 30 files, same header

  constexpr auto mock_content_1 = "mock_header\nmock_content_1";
  constexpr size_t mock_header_size{
      12}; // std::strlen("mock_header\n")    includes end of line char
  constexpr size_t mock_content_1_size{26}; // std::strlen(mock_content_1)

  constexpr size_t expected_size_common_header_30{mock_content_1_size * 30 -
                                                  mock_header_size * 29};

  prepare_list_objects(
      MakeLOR("mock_bucket",
              {"mock_file_1",  "mock_file_2",  "mock_file_3",  "mock_file_4",
               "mock_file_5",  "mock_file_6",  "mock_file_7",  "mock_file_8",
               "mock_file_9",  "mock_file_10", "mock_file_11", "mock_file_12",
               "mock_file_13", "mock_file_14", "mock_file_15", "mock_file_16",
               "mock_file_17", "mock_file_18", "mock_file_19", "mock_file_20",
               "mock_file_21", "mock_file_22", "mock_file_23", "mock_file_24",
               "mock_file_25", "mock_file_26", "mock_file_27", "mock_file_28",
               "mock_file_29", "mock_file_30"},
              {
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
                  mock_content_1_size, mock_content_1_size, mock_content_1_size,
              }));

  // We expect the header detection routine to actually read data from 20 files
  // out of the full list
  EXPECT_CALL(*mock_client, ReadObject)
      .Times(20)
      .WillRepeatedly(READ_MOCK_LAMBDA(generate_read_from_start_simulator(
          mock_content_1, mock_content_1_size)));

  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"),
            expected_size_common_header_30);

  // multifile, 30 files, with zero length files causing header detection
  // algorithm to classify file as "no common header"

  constexpr size_t expected_size_different_lenghts_30{mock_content_1_size * 18};

  prepare_list_objects(
      MakeLOR("mock_bucket",
              {"mock_file_1",  "mock_file_2",  "mock_file_3",  "mock_file_4",
               "mock_file_5",  "mock_file_6",  "mock_file_7",  "mock_file_8",
               "mock_file_9",  "mock_file_10", "mock_file_11", "mock_file_12",
               "mock_file_13", "mock_file_14", "mock_file_15", "mock_file_16",
               "mock_file_17", "mock_file_18", "mock_file_19", "mock_file_20",
               "mock_file_21", "mock_file_22", "mock_file_23", "mock_file_24",
               "mock_file_25", "mock_file_26", "mock_file_27", "mock_file_28",
               "mock_file_29", "mock_file_30"},
              {
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  0,
                  0,
                  0,
                  0,
                  0,
                  0,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
                  mock_content_1_size,
              }));

  // We expect the header detection routine to actually read data from at least
  // the 5 first files out of the full list
  EXPECT_CALL(*mock_client, ReadObject)
      .Times(testing::AtLeast(5))
      .WillRepeatedly(READ_MOCK_LAMBDA(generate_read_from_start_simulator(
          mock_content_1, mock_content_1_size)));

  ASSERT_EQ(driver_getFileSize("gs://mock_bucket/mock_object"),
            expected_size_different_lenghts_30);
}

TEST_F(GCSDriverTestFixture, Open_InvalidURIs_AllModes) {
  constexpr char modes[3] = {'r', 'w', 'a'};
  for (char m : modes) {
    CheckInvalidURIs(driver_fopen, m, nullptr);
  }
}

TEST_F(GCSDriverTestFixture, Close) {
  void *read_h = test_addReaderHandle("mock_bucket", "mock_object", 0, 0,
                                      {"mock_name"}, {1}, 1);

  void *another_read_h = test_addReaderHandle("mock_bucket", "mock_object", 0,
                                              0, {"mock_name"}, {1}, 1);

  // to test close on a writing stream, a "valid" stream is required, obtained
  // below through the mocked call
  ON_CALL(*mock_client, CreateResumableUpload)
      .WillByDefault(Return(
          gcs::internal::CreateResumableUploadResponse{"mock_upload_id"}));

  void *write_h = test_addWriterHandle(false, true, mock_bucket, mock_object);

  Handle unknown(HandleType::kRead);

  auto check_handle = [&](void *h) {
    ASSERT_EQ(static_cast<void *>(GetHandles()->front().get()), h);
  };

  // null pointer
  CheckHandlesSize(3);
  ASSERT_EQ(driver_fclose(nullptr), kFailure);
  CheckHandlesSize(3);
  check_handle(read_h);

  // address unknown
  ASSERT_EQ(driver_fclose(&unknown), kFailure);
  CheckHandlesSize(3);
  check_handle(read_h);

  // close read_h
  // additional post-condition: write_h, that was the last handle, must have
  // been swapped to the front
  printf(" 1 =================== read_h = %p\n", read_h);
  ASSERT_EQ(driver_fclose(read_h), kSuccess);
  CheckHandlesSize(2);
  check_handle(write_h);

  // try to close read_h handle again
  ASSERT_EQ(driver_fclose(read_h), kFailure);
  printf(" 2 =================== read_h = %p\n", read_h);
  CheckHandlesSize(2);
  check_handle(write_h);

  // close write_h
  gcs::ObjectMetadata expected_metadata;
  EXPECT_CALL(*mock_client, UploadChunk)
      .WillOnce(Return(gcs::internal::QueryResumableUploadResponse{
          /*.committed_size=*/absl::nullopt,
          /*.object_metadata=*/expected_metadata}));

  ASSERT_EQ(driver_fclose(write_h), kSuccess);
  CheckHandlesSize(1);
  check_handle(another_read_h);

  // close last handle
  ASSERT_EQ(driver_fclose(another_read_h), kSuccess);
  CheckHandlesEmpty();

  // try closing a handle again while container is empty
  ASSERT_EQ(driver_fclose(read_h), kFailure);
  CheckHandlesEmpty();
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_OneFileSuccess) {
  MultiPartFile expected_struct{"mock_bucket", "mock_file", 0,  0,
                                {"mock_file"}, {10},        10, {1}};

  PrepareListObjects(MakeLOR("mock_bucket", {"mock_file"}, {10}));
  OpenSuccess(expected_struct);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_OneFileFailure) {
  PrepareListObjects({});
  OpenFailure();
}

TEST_F(GCSDriverTestFixture,
       OpenReadModeAndClose_TwoFilesNoCommonHeaderSuccess) {
  constexpr const char *mock_file_0_content = "mock_header\ncontent";
  constexpr size_t mock_file_0_size{19};
  size_t mock_file_0_offset{0};
  ReadSimulatorParams mock_file_0{mock_file_0_content, mock_file_0_size,
                                  &mock_file_0_offset};

  constexpr const char *mock_file_1_content = "content";
  constexpr size_t mock_file_1_size{7};
  size_t mock_file_1_offset{0};
  ReadSimulatorParams mock_file_1{mock_file_1_content, mock_file_1_size,
                                  &mock_file_1_offset};

  LOReturnType file0_file1_response =
      MakeLOR("mock_bucket", {"mock_file_0", "mock_file_1"},
              {mock_file_0_size, mock_file_1_size});

  constexpr size_t total_size{mock_file_0_size + mock_file_1_size};

  MultiPartFile expected_struct{"mock_bucket",
                                "mock_file",
                                0,
                                0,
                                {"mock_file_0", "mock_file_1"},
                                {static_cast<long long>(mock_file_0_size),
                                 static_cast<long long>(total_size)},
                                static_cast<long long>(total_size),
                                {1}};

  TestMultifileOpenSuccess(file0_file1_response, mock_file_0, mock_file_1,
                           expected_struct);
}

TEST_F(GCSDriverTestFixture, OpenReadModeAndClose_TwoFilesCommonHeaderSuccess) {
  constexpr const char *mock_file_0_content = "mock_header\ncontent";
  constexpr size_t mock_file_0_size{19};
  constexpr size_t mock_header_size{12};
  size_t mock_file_0_offset{0};
  ReadSimulatorParams mock_file_0{mock_file_0_content, mock_file_0_size,
                                  &mock_file_0_offset};

  constexpr const char *mock_file_1_content = "mock_header\ncontent";
  constexpr size_t mock_file_1_size{19};
  size_t mock_file_1_offset{0};
  ReadSimulatorParams mock_file_1{mock_file_1_content, mock_file_1_size,
                                  &mock_file_1_offset};

  LOReturnType file0_file1_response =
      MakeLOR("mock_bucket", {"mock_file_0", "mock_file_1"},
              {mock_file_0_size, mock_file_1_size});

  constexpr size_t total_size{mock_file_0_size + mock_file_1_size -
                              mock_header_size};

  MultiPartFile expected_struct{"mock_bucket",
                                "mock_file",
                                0,
                                static_cast<long long>(mock_header_size),
                                {"mock_file_0", "mock_file_1"},
                                {static_cast<long long>(mock_file_0_size),
                                 static_cast<long long>(total_size)},
                                static_cast<long long>(total_size),
                                {1}};

  TestMultifileOpenSuccess(file0_file1_response, mock_file_0, mock_file_1,
                           expected_struct);
}

TEST_F(GCSDriverTestFixture,
       OpenReadModeAndClose_TwoFilesNoCommonHeaderFailureOnFirstRead) {
  constexpr size_t mock_file_0_size{19};

  constexpr size_t mock_file_1_size{7};

  LOReturnType file0_file1_response =
      MakeLOR("mock_bucket", {"mock_file_0", "mock_file_1"},
              {mock_file_0_size, mock_file_1_size});

  PrepareListObjects(file0_file1_response);
  EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE);
  OpenFailure();
}

TEST_F(GCSDriverTestFixture,
       OpenReadModeAndClose_TwoFilesNoCommonHeaderFailureOnSecondRead) {
  constexpr const char *mock_file_0_content = "mock_header\ncontent";
  constexpr size_t mock_file_0_size{19};
  // constexpr size_t mock_header_size{12};
  size_t mock_file_0_offset{0};
  ReadSimulatorParams mock_file_0{mock_file_0_content, mock_file_0_size,
                                  &mock_file_0_offset};

  constexpr size_t mock_file_1_size{7};

  LOReturnType file0_file1_response =
      MakeLOR("mock_bucket", {"mock_file_0", "mock_file_1"},
              {mock_file_0_size, mock_file_1_size});

  PrepareListObjects(file0_file1_response);
  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_file_0)))
      .WillOnce(READ_MOCK_LAMBDA_FAILURE);
  OpenFailure();
}

TEST_F(GCSDriverTestFixture, Seek_BadArgs) {
  constexpr int seek_failure{-1};

  void *manual_handle_reader = test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0, {"mock_file"}, {0}, 0);

  void *manual_handle_writer = test_addWriterHandle();

  ASSERT_EQ(driver_fseek(nullptr, 0, std::ios::beg), seek_failure);
  ASSERT_EQ(driver_fseek(manual_handle_reader, 0, -1),
            seek_failure); // unrecognised whence
  ASSERT_EQ(driver_fseek(manual_handle_writer, 0, std::ios::beg),
            seek_failure); // attempted seek on writer
}

TEST_F(GCSDriverTestFixture, SeekFromStart) {
  struct TestParams {
    long long offset;
    int expected_result;
  };

  const int seek_failure{-1};
  constexpr int seek_success{0};

  auto test_func = [seek_failure](const std::vector<TestParams> vals,
                                  Handle &sample,
                                  long long sample_starting_offset) {
    for (const auto &v : vals) {
      int res{0};
      ASSERT_NO_THROW(res = driver_fseek(&sample, v.offset, std::ios::beg));
      ASSERT_EQ(res, v.expected_result);
      ASSERT_EQ(sample.GetReader().offset_, v.expected_result == seek_failure
                                                ? sample_starting_offset
                                                : v.offset);
      sample.GetReader().offset_ = sample_starting_offset;
    }
  };

  constexpr long long filesize{10};
  constexpr long long starting_offset{1};

  Handle *test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0, {"mock_file"}, {0}, 0));

  std::vector<TestParams> test_values = {
      TestParams{0, seek_success},
      TestParams{5, seek_success},
      TestParams{filesize - 1, seek_success},
      TestParams{filesize, seek_success},
      TestParams{filesize + 1, seek_success},
      TestParams{-1, seek_failure},
      TestParams{std::numeric_limits<long long>::min(), seek_failure},
      TestParams{std::numeric_limits<long long>::max(), seek_success}};

  test_func(test_values, *test_reader, starting_offset);

  // special case

  // multifile

  test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0,
      {"mock_file_0", "mock_file_1", "mock_file_3"},
      {filesize, 2 * filesize, 3 * filesize}, 3 * filesize));

  std::vector<TestParams> test_values_multifile = {
      TestParams{0, seek_success},
      TestParams{2 * filesize, seek_success},
      TestParams{3 * filesize - 1, seek_success},
      TestParams{3 * filesize, seek_success},
      TestParams{3 * filesize + 1, seek_success},
      TestParams{-1, seek_failure},
      TestParams{std::numeric_limits<long long>::min(), seek_failure},
      TestParams{std::numeric_limits<long long>::max(), seek_success}};

  test_func(test_values_multifile, *test_reader, starting_offset);
}

TEST_F(GCSDriverTestFixture, SeekFromCurrentOffset) {
  struct TestParams {
    long long offset;
    int expected_result;
  };

  constexpr int seek_failure{-1};
  constexpr int seek_success{0};

  auto test_func = [seek_failure](const std::vector<TestParams> vals,
                                  Handle &sample,
                                  long long sample_starting_offset) {
    for (const auto &v : vals) {
      int res{0};
      ASSERT_NO_THROW(res = driver_fseek(&sample, v.offset, std::ios::cur));
      ASSERT_EQ(res, v.expected_result);
      ASSERT_EQ(sample.GetReader().offset_,
                v.expected_result == seek_failure
                    ? sample_starting_offset
                    : sample_starting_offset + v.offset);
      sample.GetReader().offset_ = sample_starting_offset;
    }
  };

  constexpr long long filesize{10};
  constexpr long long starting_offset{5};
  constexpr long long gap_to_end{filesize - starting_offset - 1};

  Handle *test_reader = reinterpret_cast<Handle *>(
      test_addReaderHandle("mock_bucket", "mock_file", starting_offset, 0,
                           {"mock_file"}, {filesize}, filesize));

  std::vector<TestParams> test_values = {
      TestParams{0, seek_success},
      TestParams{-starting_offset, seek_success},
      TestParams{-(starting_offset - 1), seek_success},
      TestParams{gap_to_end - 1, seek_success},
      TestParams{gap_to_end, seek_success},
      TestParams{gap_to_end + 1, seek_success},
      TestParams{-(starting_offset + 1), seek_failure},
      TestParams{std::numeric_limits<long long>::min(), seek_failure},
      TestParams{std::numeric_limits<long long>::max(), seek_failure}};

  test_func(test_values, *test_reader, starting_offset);

  // special case: starting offset is 0

  test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0, {"mock_file"}, {filesize}, filesize));

  std::vector<TestParams> special_test_values = {
      TestParams{std::numeric_limits<long long>::max(), seek_success}};
  test_func(special_test_values, *test_reader, 0);
}

TEST_F(GCSDriverTestFixture, SeekFromEnd) {
  struct TestParams {
    long long offset;
    int expected_result;
  };

  constexpr int seek_failure{-1};
  constexpr int seek_success{0};

  auto test_func = [seek_failure](const std::vector<TestParams> vals,
                                  Handle &sample,
                                  long long sample_starting_offset,
                                  long long sample_filesize) {
    for (const auto &v : vals) {
      int res{0};
      ASSERT_NO_THROW(res = driver_fseek(&sample, v.offset, std::ios::end));
      ASSERT_EQ(res, v.expected_result);
      ASSERT_EQ(sample.GetReader().offset_, v.expected_result == seek_failure
                                                ? sample_starting_offset
                                                : sample_filesize + v.offset);
      sample.GetReader().offset_ = sample_starting_offset;
    }
  };

  constexpr long long filesize{10};
  constexpr long long starting_offset{filesize - 1};

  Handle *test_reader = reinterpret_cast<Handle *>(
      test_addReaderHandle("mock_bucket", "mock_file", starting_offset, 0,
                           {"mock_file"}, {filesize}, filesize));

  std::vector<TestParams> test_values = {
      TestParams{0, seek_success}, TestParams{-starting_offset, seek_success},
      TestParams{2, seek_success}, // Seek beyond end of file should succeed, as
                                   // per standard library behaviour
      TestParams{-(filesize + 1),
                 seek_failure}, // Seek before start of file should fail
      TestParams{std::numeric_limits<long long>::min(), seek_failure},
      TestParams{std::numeric_limits<long long>::max(), seek_failure}};

  test_func(test_values, *test_reader, starting_offset, filesize);

  // special case: file of size 0, offset 0

  test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0, {"mock_file"}, {0}, 0));

  std::vector<TestParams> special_test_values = {
      TestParams{std::numeric_limits<long long>::max(), seek_success}};
  test_func(special_test_values, *test_reader, 0, 0);

  // Read after successful seek beyond EOF should fail and not attempt I/O.
  Handle *read_after_seek_reader = reinterpret_cast<Handle *>(
      test_addReaderHandle("mock_bucket", "mock_file", 0, 0, {"mock_file"},
                           {filesize}, filesize));
  ASSERT_EQ(driver_fseek(read_after_seek_reader, 2, std::ios::end), 0);
  ASSERT_EQ(read_after_seek_reader->GetReader().offset_, filesize + 2);

  char buff[4] = {};
  ASSERT_EQ(driver_fread(buff, sizeof(uint8_t), 1, read_after_seek_reader), -1);
  ASSERT_EQ(read_after_seek_reader->GetReader().offset_, filesize + 2);
}

TEST_F(GCSDriverTestFixture, Read_BadArgs) {
  constexpr long long max_pos{std::numeric_limits<long long>::max()};

  Handle *dummy_handle = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0, {"mock_file"}, {1}, 1));

  char dummy_buff[1];

  // null stream
  ASSERT_EQ(driver_fread(nullptr, 0, 0, nullptr), -1);

  // null buffer
  ASSERT_EQ(driver_fread(nullptr, 0, 0, dummy_handle), -1);

  // size of 0
  ASSERT_EQ(driver_fread(dummy_buff, 0, 0, dummy_handle), -1);

  // size and / or count too large
  constexpr size_t large_size{
      static_cast<size_t>(std::numeric_limits<long long>::max())};
  constexpr size_t count{4};
  ASSERT_EQ(driver_fread(dummy_buff, large_size, count, dummy_handle), -1);

  // size and count within numerical limits, but file position would go beyond
  // numerical limit during read
  constexpr long long large_file_size{max_pos - 1};

  auto &reader_ref = dummy_handle->GetReader();
  reader_ref.offset_ = large_file_size - 1;
  reader_ref.cumulativeSize_ = {large_file_size};
  reader_ref.total_size_ = large_file_size;

  constexpr size_t size{10};

  ASSERT_EQ(driver_fread(dummy_buff, size, count, dummy_handle), -1);

  // read attempt on a writer stream
  Handle *dummy_writer_handle =
      reinterpret_cast<Handle *>(test_addWriterHandle());

  ASSERT_EQ(driver_fread(dummy_buff, 1, 1, dummy_writer_handle), -1);
}

TEST_F(GCSDriverTestFixture, Read_OneFile) {
  constexpr const char *mock_content{"mock_content"};
  constexpr size_t mock_size{12};
  size_t mock_offset{0};
  ReadSimulatorParams mock_read_params{mock_content, mock_size, &mock_offset};

  constexpr long long filesize{static_cast<long long>(mock_size)};

  Handle *test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0, {"mock_file"}, {filesize}, filesize));

  char buff[16] = {};

  constexpr size_t size{sizeof(uint8_t)};

  // special case: 0 bytes to read
  ASSERT_EQ(driver_fread(buff, size, 0, test_reader), 0);

  // special case: offset > filesize, trying to read at least one byte
  test_reader->GetReader().offset_ = filesize + 1;

  ASSERT_EQ(driver_fread(buff, size, 1, test_reader), -1);
  ASSERT_EQ(test_reader->GetReader().offset_, filesize + 1);

  // basic case: offset 0, 1 byte to read
  test_reader->GetReader().offset_ = 0;

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
  ASSERT_EQ(driver_fread(buff, size, 1, test_reader), 1);
  ASSERT_EQ(buff[0], mock_read_params.content[0]);
  ASSERT_EQ(test_reader->GetReader().offset_, 1);

  auto sync_offset = [&](long long off) {
    test_reader->GetReader().offset_ = off;
    *mock_read_params.offset =
        static_cast<size_t>(test_reader->GetReader().offset_);
  };

  // basic case: 0 < offset < filesize-1, 1 byte to read
  sync_offset(1);

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
  ASSERT_EQ(driver_fread(buff, size, 1, test_reader), 1);
  ASSERT_EQ(buff[0], mock_read_params.content[1]);
  ASSERT_EQ(test_reader->GetReader().offset_, 2);

  // basic case: offset == filesize - 1, 1 byte to read
  sync_offset(filesize - 1);

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
  ASSERT_EQ(driver_fread(buff, size, 1, test_reader), 1);
  ASSERT_EQ(buff[0], mock_read_params.content[filesize - 1]);
  ASSERT_EQ(test_reader->GetReader().offset_, filesize);

  auto test_one_file_read_n_bytes = [&](long long offset) {
    sync_offset(offset);
    const long long bytes_to_read =
        filesize - 1 - test_reader->GetReader().offset_;
    const size_t cast_btr = static_cast<size_t>(bytes_to_read);

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
    ASSERT_EQ(driver_fread(buff, size, cast_btr, test_reader), bytes_to_read);
    ASSERT_EQ(std::strncmp(buff, mock_content + offset, cast_btr), 0);
    ASSERT_EQ(test_reader->GetReader().offset_, offset + cast_btr);
  };

  const std::vector<long long> one_file_read_n_bytes_test_values = {
      0, // offset 0, offset < n < filesize bytes to read
      filesize /
          2, // 0 < offset < filesize-1, 2 < n < filesize - offset bytes to read
  };

  for (long long i : one_file_read_n_bytes_test_values) {
    test_one_file_read_n_bytes(i);
  }

  // try reading more bytes than available. must read exactly all remaining
  // bytes from offset and leave offset at filesize
  auto test_try_read_more_bytes_than_available = [&](long long offset) {
    sync_offset(offset);
    const long long available{filesize - offset};

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params)));
    ASSERT_EQ(driver_fread(buff, size, static_cast<size_t>(2 * filesize),
                           test_reader),
              available);
    ASSERT_EQ(std::strncmp(buff, mock_content + offset,
                           static_cast<size_t>(available)),
              0);
    ASSERT_EQ(test_reader->GetReader().offset_, filesize);
  };

  // offset tests
  const std::vector<long long>
      one_file_try_reading_more_than_possible_test_values = {
          0,
          filesize / 2,
          filesize - 1,
      };

  for (long long i : one_file_try_reading_more_than_possible_test_values) {
    test_try_read_more_bytes_than_available(i);
  }
}

TEST_F(GCSDriverTestFixture, Read_NFiles_OffsetBeyondEOF_FailsNoIO) {
  constexpr long long size_0{5};
  constexpr long long size_1{7};
  constexpr long long total_size{size_0 + size_1};

  Handle *test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", total_size + 3, 0,
      {"mock_file_0", "mock_file_1"}, {size_0, total_size}, total_size));

  char buff[8] = {};
  ASSERT_EQ(driver_fread(buff, sizeof(uint8_t), 1, test_reader), -1);
  ASSERT_EQ(test_reader->GetReader().offset_, total_size + 3);
}

TEST_F(GCSDriverTestFixture, Read_NFiles_NoCommonHeader) {
  constexpr const char *mock_content_0{"mock_header\nmock_content0"};
  constexpr const char *mock_content_1{"mock_content1"};
  constexpr const char *mock_content_2{"mock_content2"};

  const size_t mock_size_0{std::strlen(mock_content_0)};
  const size_t mock_size_1{std::strlen(mock_content_1)};
  const size_t mock_size_2{std::strlen(mock_content_2)};

  const long long cast_mock_size_0 = static_cast<long long>(mock_size_0);
  const long long cast_mock_size_1 = static_cast<long long>(mock_size_1);
  const long long cast_mock_size_2 = static_cast<long long>(mock_size_2);

  const long long filesize{cast_mock_size_0 + cast_mock_size_1 +
                           cast_mock_size_2};

  Handle *test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0,
      {"mock_file_0", "mock_file_1", "mock_file_2"},
      {cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1, filesize},
      filesize));

  size_t mock_offset_0{0};
  size_t mock_offset_1{0};
  size_t mock_offset_2{0};

  ReadSimulatorParams mock_read_params_0{mock_content_0, mock_size_0,
                                         &mock_offset_0};
  ReadSimulatorParams mock_read_params_1{mock_content_1, mock_size_1,
                                         &mock_offset_1};
  ReadSimulatorParams mock_read_params_2{mock_content_2, mock_size_2,
                                         &mock_offset_2};

  std::vector<char> buff(2 * static_cast<size_t>(filesize));
  char *buff_data = buff.data();

  constexpr size_t size{sizeof(uint8_t)};

  // read 1 byte from start of an intermediate file
  test_reader->GetReader().offset_ = cast_mock_size_0;

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
  EXPECT_EQ(driver_fread(buff_data, size, 1, test_reader), 1);
  EXPECT_EQ(test_reader->GetReader().offset_, cast_mock_size_0 + 1);
  EXPECT_EQ(buff[0], mock_read_params_1.content[0]);

  // read a small amount of bytes overlapping two file fragments
  test_reader->GetReader().offset_ = cast_mock_size_0 - 1;
  *mock_read_params_0.offset = mock_size_0 - 1;
  *mock_read_params_1.offset = 0;

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
  EXPECT_EQ(driver_fread(buff_data, size, 2, test_reader), 2);
  EXPECT_EQ(test_reader->GetReader().offset_, cast_mock_size_0 + 1);
  EXPECT_EQ(buff[0], mock_content_0[mock_size_0 - 1]);
  EXPECT_EQ(buff[1], mock_content_1[0]);

  // read more than mock_size_0 and less than mock_size_0 + mock_size_1 bytes
  // from the start of the file
  test_reader->GetReader().offset_ = 0;
  *mock_read_params_0.offset = 0;
  *mock_read_params_1.offset = 0;

  long long to_read = cast_mock_size_0 + cast_mock_size_1 - 1;

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
  EXPECT_EQ(
      driver_fread(buff_data, size, static_cast<size_t>(to_read), test_reader),
      to_read);
  EXPECT_EQ(test_reader->GetReader().offset_, to_read);
  EXPECT_EQ(std::strncmp(buff_data, mock_content_0, mock_size_0), 0);
  EXPECT_EQ(
      std::strncmp(buff_data + mock_size_0, mock_content_1, mock_size_1 - 1),
      0);

  // tests reading the whole file

  auto test_whole_file = [&](long long bytes_to_read) {
    test_reader->GetReader().offset_ = 0;
    *mock_read_params_0.offset = 0;
    *mock_read_params_1.offset = 0;
    *mock_read_params_2.offset = 0;

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_2)));
    EXPECT_EQ(driver_fread(buff_data, size, static_cast<size_t>(bytes_to_read),
                           test_reader),
              filesize);
    EXPECT_EQ(test_reader->GetReader().offset_, filesize);
    EXPECT_EQ(std::strlen(buff_data), filesize);
    EXPECT_EQ(std::strncmp(buff_data, mock_content_0, mock_size_0), 0);
    EXPECT_EQ(
        std::strncmp(buff_data + mock_size_0, mock_content_1, mock_size_1), 0);
    EXPECT_EQ(std::strncmp(buff_data + mock_size_0 + mock_size_1,
                           mock_content_2, mock_size_2),
              0);
  };

  // read the whole file
  test_whole_file(filesize);

  // try to read more than the whole file
  test_whole_file(filesize + 1);
}

TEST_F(GCSDriverTestFixture, Read_NFiles_CommonHeader) {
  constexpr const char *mock_content_0{"mock_header\nmock_content0"};
  constexpr const char *mock_content_1{"mock_header\nmock_content1"};
  constexpr const char *mock_content_2{"mock_header\nmock_content2"};

  const size_t hdr_size{std::strlen("mock_header\n")};
  const size_t mock_size_0{std::strlen(mock_content_0)};
  const size_t mock_size_1{std::strlen(mock_content_1)};
  const size_t mock_size_2{std::strlen(mock_content_2)};

  const long long cast_hdr_size = static_cast<long long>(hdr_size);
  const long long cast_mock_size_0 = static_cast<long long>(mock_size_0);
  const long long cast_mock_size_1 = static_cast<long long>(mock_size_1);
  const long long cast_mock_size_2 = static_cast<long long>(mock_size_2);

  const long long filesize{cast_mock_size_0 + cast_mock_size_1 - cast_hdr_size +
                           cast_mock_size_2 - cast_hdr_size};

  Handle *test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0,
      {"mock_file_0", "mock_file_1", "mock_file_2"},
      {cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1 - cast_hdr_size,
       filesize},
      filesize));

  size_t mock_offset_0{0};
  size_t mock_offset_1{hdr_size};
  size_t mock_offset_2{hdr_size};

  ReadSimulatorParams mock_read_params_0{mock_content_0, mock_size_0,
                                         &mock_offset_0};
  ReadSimulatorParams mock_read_params_1{mock_content_1, mock_size_1,
                                         &mock_offset_1};
  ReadSimulatorParams mock_read_params_2{mock_content_2, mock_size_2,
                                         &mock_offset_2};

  std::vector<char> buff(2 * static_cast<size_t>(filesize));
  char *buff_data = buff.data();

  constexpr size_t size{sizeof(uint8_t)};

  // read 1 byte from start of an intermediate file
  test_reader->GetReader().offset_ = cast_mock_size_0;

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
  EXPECT_EQ(driver_fread(buff_data, size, 1, test_reader), 1);
  EXPECT_EQ(test_reader->GetReader().offset_, cast_mock_size_0 + 1);
  EXPECT_EQ(buff[0], mock_read_params_1.content[hdr_size]);

  // read a small amount of bytes overlapping two file fragments
  test_reader->GetReader().offset_ = cast_mock_size_0 - 1;
  *mock_read_params_0.offset = mock_size_0 - 1;
  *mock_read_params_1.offset = hdr_size;

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
  EXPECT_EQ(driver_fread(buff_data, size, 2, test_reader), 2);
  EXPECT_EQ(test_reader->GetReader().offset_, cast_mock_size_0 + 1);
  EXPECT_EQ(buff[0], mock_content_0[mock_size_0 - 1]);
  EXPECT_EQ(buff[1], mock_content_1[hdr_size]);

  // read more than mock_size_0 and less than mock_size_0 + mock_size_1
  // (excluding header size) bytes from the start of the file
  test_reader->GetReader().offset_ = 0;
  *mock_read_params_0.offset = 0;
  *mock_read_params_1.offset = hdr_size;

  const size_t read_in_file1 = mock_size_1 - hdr_size - 1;
  const long long to_read =
      cast_mock_size_0 + static_cast<long long>(read_in_file1);

  EXPECT_CALL(*mock_client, ReadObject)
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
      .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)));
  EXPECT_EQ(
      driver_fread(buff_data, size, static_cast<size_t>(to_read), test_reader),
      to_read);
  EXPECT_EQ(test_reader->GetReader().offset_, to_read);
  EXPECT_EQ(std::strncmp(buff_data, mock_content_0, mock_size_0), 0);
  EXPECT_EQ(std::strncmp(buff_data + mock_size_0, mock_content_1 + hdr_size,
                         read_in_file1),
            0);

  // tests reading the whole file

  auto test_whole_file = [&](long long bytes_to_read) {
    test_reader->GetReader().offset_ = 0;
    *mock_read_params_0.offset = 0;
    *mock_read_params_1.offset = hdr_size;
    *mock_read_params_2.offset = hdr_size;

    EXPECT_CALL(*mock_client, ReadObject)
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_1)))
        .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_2)));
    EXPECT_EQ(driver_fread(buff_data, size, static_cast<size_t>(bytes_to_read),
                           test_reader),
              filesize);
    EXPECT_EQ(test_reader->GetReader().offset_, filesize);
    EXPECT_EQ(std::strlen(buff_data), filesize);
    EXPECT_EQ(std::strncmp(buff_data, mock_content_0, mock_size_0), 0);
    EXPECT_EQ(std::strncmp(buff_data + mock_size_0, mock_content_1 + hdr_size,
                           mock_size_1 - hdr_size),
              0);
    EXPECT_EQ(std::strncmp(buff_data + mock_size_0 + mock_size_1 - hdr_size,
                           mock_content_2 + hdr_size, mock_size_2 - hdr_size),
              0);
  };

  // read the whole file
  test_whole_file(filesize);

  // try to read more than the whole file
  test_whole_file(filesize + 1);
}

TEST_F(GCSDriverTestFixture, Read_NFiles_ReadFailures) {
  constexpr const char *mock_content_0{"mock_header\nmock_content0"};
  constexpr const char *mock_content_1{"mock_content1"};
  constexpr const char *mock_content_2{"mock_content2"};

  const size_t mock_size_0{std::strlen(mock_content_0)};
  const size_t mock_size_1{std::strlen(mock_content_1)};
  const size_t mock_size_2{std::strlen(mock_content_2)};

  const long long cast_mock_size_0 = static_cast<long long>(mock_size_0);
  const long long cast_mock_size_1 = static_cast<long long>(mock_size_1);
  const long long cast_mock_size_2 = static_cast<long long>(mock_size_2);

  const long long filesize{cast_mock_size_0 + cast_mock_size_1 +
                           cast_mock_size_2};

  Handle *test_reader = reinterpret_cast<Handle *>(test_addReaderHandle(
      "mock_bucket", "mock_file", 0, 0,
      {"mock_file_0", "mock_file_1", "mock_file_2"},
      {cast_mock_size_0, cast_mock_size_0 + cast_mock_size_1, filesize},
      filesize));

  size_t mock_offset_0{0};
  // size_t mock_offset_1{0};
  // size_t mock_offset_2{0};

  ReadSimulatorParams mock_read_params_0{mock_content_0, mock_size_0,
                                         &mock_offset_0};
  /*
  ReadSimulatorParams mock_read_params_1{mock_content_1, mock_size_1,
                                         &mock_offset_1};
  ReadSimulatorParams mock_read_params_2{mock_content_2, mock_size_2,
                                         &mock_offset_2};
  */
  std::vector<char> buff(2 * static_cast<size_t>(filesize));
  char *buff_data = buff.data();

  constexpr size_t size{sizeof(uint8_t)};

  auto test_func = [&](long long offset_before_read, size_t to_read,
                       std::function<void()> set_mock_calls) {
    test_reader->GetReader().offset_ = offset_before_read;
    *mock_read_params_0.offset = static_cast<size_t>(offset_before_read);

    set_mock_calls();

    EXPECT_EQ(driver_fread(buff_data, size, to_read, test_reader), -1);
    EXPECT_EQ(test_reader->GetReader().offset_, offset_before_read);
  };

  // fail at first read
  const std::vector<long long> fail_first_read_test_values = {
      0, cast_mock_size_0 - 1, cast_mock_size_0};
  for (long long i : fail_first_read_test_values) {
    test_func(i, 1, [&]() {
      EXPECT_CALL(*mock_client, ReadObject).WillOnce(READ_MOCK_LAMBDA_FAILURE);
    });
  }

  // fail at subsequent read
  const std::vector<long long> fail_other_read_test_values = {
      0, cast_mock_size_0 - 1};
  for (long long i : fail_other_read_test_values) {
    test_func(i, static_cast<size_t>(filesize), [&]() {
      EXPECT_CALL(*mock_client, ReadObject)
          .WillOnce(READ_MOCK_LAMBDA(GenerateReadSimulator(mock_read_params_0)))
          .WillOnce(READ_MOCK_LAMBDA_FAILURE);
    });
  }
}

TEST_F(GCSDriverTestFixture, OpenWriteMode_OK) {
  using gcs::internal::CreateResumableUploadResponse;

  constexpr const char *upload_id = "mock_upload_id";

  ON_CALL(*mock_client, CreateResumableUpload)
      .WillByDefault(Return(CreateResumableUploadResponse{upload_id}));

  gcsplugin::Writer expected;
  expected.bucketname_ = mock_bucket;
  expected.filename_ = mock_object;

  void *stream = OpenWriteOnly();
  ASSERT_NE(stream, nullptr);

  CheckHandlesSize(1);
  ASSERT_EQ(GetHandles()->front().get(), stream);

  const auto stream_cast = reinterpret_cast<Handle *>(stream);
  ASSERT_EQ(stream_cast->type, HandleType::kWrite);

  const auto &writer = stream_cast->GetWriter();
  ASSERT_EQ(writer, expected);

  const auto &sub_writer = writer.writer_;
  ASSERT_EQ(sub_writer.resumable_session_id(), upload_id);
  ASSERT_TRUE(sub_writer.last_status().ok());
}

TEST_F(GCSDriverTestFixture, OpenWriteMode_FailClientWriteObject) {
  using gcs::internal::CreateResumableUploadResponse;

  EXPECT_CALL(*mock_client, CreateResumableUpload)
      .WillOnce(Return(gc::Status(gc::StatusCode::kUnknown, "Mock failure")));

  ASSERT_EQ(OpenWriteOnly(), nullptr);
  CheckHandlesEmpty();
}

TEST_F(GCSDriverTestFixture, Write_BadArgs) {
  struct Params {
    void *ptr_;
    size_t size_;
    size_t count_;
    void *stream_;
  };

  Handle unknown_stream(HandleType::kRead);

  char dummy_buffer[8] = {};

  void *wrong_type = test_addReaderHandle(mock_bucket, mock_object, 0, 0,
                                          {"mock_file"}, {1}, 1);

  void *legit_stream = test_addWriterHandle();

  std::vector<Params> test_params = {
      // fails because
      Params{nullptr, 0, 0, nullptr},               // stream == nullptr
      Params{nullptr, 0, 0, &unknown_stream},       // ptr == nullptr
      Params{&dummy_buffer, 0, 0, &unknown_stream}, // size == 0
      Params{&dummy_buffer, 1, 0, &unknown_stream}, // stream unknown
      Params{&dummy_buffer, 1, 0, wrong_type},      // wrong stream variant
      Params{&dummy_buffer, 2, std::numeric_limits<size_t>::max(),
             legit_stream} // numbers would overflow
  };

  // state of the driver before calls to fwrite. these must not be changed by
  // the calls.
  const DriverState initial_state = RecordDriverState();

  // the test
  for (const Params &p : test_params) {
    ASSERT_EQ(driver_fwrite(p.ptr_, p.size_, p.count_, p.stream_), -1);
    ASSERT_EQ(initial_state, RecordDriverState());
  }
}

TEST_F(GCSDriverTestFixture, Write_FailOnWrite) {
  using gcs::internal::CreateResumableUploadResponse;

  // to test a bad writing, a "valid" stream is required, obtained below
  // through the mocked call to create such stream
  ON_CALL(*mock_client, CreateResumableUpload)
      .WillByDefault(Return(CreateResumableUploadResponse{"mock_upload_id"}));

  void *stream_write =
      test_addWriterHandle(false, true, mock_bucket, mock_object);
  ASSERT_NE(stream_write, nullptr);

  // driver state before test
  const DriverState initial_state = RecordDriverState();

  // the attempt to write needs to pass an amount of data
  // sufficiently large to exceed the maximum size of the put area
  // and trigger the request to upload to the server
  std::vector<char> dummy_buffer(1024 * 1024 * 8);

  // the test
  EXPECT_CALL(*mock_client, UploadChunk)
      .WillOnce(Return(google::cloud::Status{
          google::cloud::StatusCode::kUnknown, "Failing, just because."}));

  ASSERT_EQ(
      driver_fwrite(dummy_buffer.data(), 1, dummy_buffer.size(), stream_write),
      -1);
  ASSERT_EQ(initial_state, RecordDriverState());
}

TEST_F(GCSDriverTestFixture, Write_NoUpload_OK) {
  using gcs::internal::CreateResumableUploadResponse;

  // to test a good writing, a "valid" stream is required, obtained below
  // through the mocked call to create such stream
  ON_CALL(*mock_client, CreateResumableUpload)
      .WillByDefault(Return(CreateResumableUploadResponse{"mock_upload_id"}));

  void *stream_write =
      test_addWriterHandle(false, true, mock_bucket, mock_object);
  ASSERT_NE(stream_write, nullptr);

  void *stream_append =
      test_addWriterHandle(true, true, mock_bucket, mock_object);
  ASSERT_NE(stream_append, nullptr);

  std::vector<void *> stream_ptrs = {stream_write, stream_append};

  // driver state before test
  const DriverState initial_state = RecordDriverState();

  // this test checks that a stream remains good when its underlying put area
  // is not yet at capacity.
  constexpr size_t nb_bytes{8};
  char dummy_buffer[nb_bytes] = {};

  // the test
  for (void *stream : stream_ptrs) {
    ASSERT_EQ(driver_fwrite(&dummy_buffer, 1, nb_bytes, stream), nb_bytes);
    ASSERT_EQ(initial_state, RecordDriverState());
  }
}

TEST_F(GCSDriverTestFixture, Write_Upload_OK) {
  using gcs::internal::CreateResumableUploadResponse;

  ON_CALL(*mock_client, CreateResumableUpload)
      .WillByDefault(Return(CreateResumableUploadResponse{"mock_upload_id"}));

  // to test a good writing, a "valid" stream is required, obtained below
  // through the mocked call to create such stream
  void *stream = test_addWriterHandle(false, true, mock_bucket, mock_object);
  ASSERT_NE(stream, nullptr);

  // driver state before test
  const DriverState initial_state = RecordDriverState();

  // this test checks the handling of a succesful upload
  constexpr size_t nb_bytes{1024 * 1024 * 8};
  std::vector<char> dummy_buffer(nb_bytes);

  // the test
  using gcs::internal::QueryResumableUploadResponse;

  gcs::ObjectMetadata expected_metadata;

  EXPECT_CALL(*mock_client, UploadChunk)
      .WillOnce(Return(QueryResumableUploadResponse{
          /*.committed_size=*/absl::nullopt,
          /*.object_metadata=*/expected_metadata}));

  ASSERT_EQ(driver_fwrite(dummy_buffer.data(), 1, nb_bytes, stream), nb_bytes);
  ASSERT_EQ(initial_state, RecordDriverState());
}

TEST_F(GCSDriverTestFixture, Remove_NonExistingFile) {
  // ListObjects renvoie "NotFound" → driver_remove doit réussir sans supprimer
  EXPECT_CALL(*mock_client, ListObjects)
      .WillOnce(Return(gc::Status(gc::StatusCode::kNotFound, "not found")));
  EXPECT_CALL(*mock_client, DeleteObject).Times(0);

  ASSERT_EQ(driver_remove("gs://mock_bucket/missing.txt"), kOtherSuccess);
}

static void
ExpectDelete(gcs::testing::MockClient &client, const std::string &bucket,
             const std::string &object_name,
             gc::StatusOr<gc::storage::internal::EmptyResponse> status =
                 gc::storage::internal::EmptyResponse{}) {
  EXPECT_CALL(
      client,
      DeleteObject(::testing::Truly(
          [bucket, object_name](gcs::internal::DeleteObjectRequest const &req) {
            return req.bucket_name() == bucket &&
                   req.object_name() == object_name;
          })))
      .WillOnce(Return(status));
}

TEST_F(GCSDriverTestFixture, Remove_SingleFile) {
  EXPECT_CALL(*mock_client, ListObjects)
      .WillOnce(
          Return<LOReturnType>(MakeLOR("mock_bucket", {"file1.txt"}, {10})));

  ExpectDelete(*mock_client, "mock_bucket", "file1.txt");

  ASSERT_EQ(driver_remove("gs://mock_bucket/file1.txt"), kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, Remove_MultipleFilesByGlob) {
  EXPECT_CALL(*mock_client, ListObjects)
      .WillOnce(Return<LOReturnType>(
          MakeLOR("mock_bucket", {"file1.txt", "file2.txt", "file3.txt"},
                  {10, 20, 30})));

  ExpectDelete(*mock_client, "mock_bucket", "file1.txt");
  ExpectDelete(*mock_client, "mock_bucket", "file2.txt");
  ExpectDelete(*mock_client, "mock_bucket", "file3.txt");

  ASSERT_EQ(driver_remove("gs://mock_bucket/file*.txt"), kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, Remove_InvalidGlobbingPattern) {
  EXPECT_CALL(*mock_client, ListObjects).Times(0);
  EXPECT_CALL(*mock_client, DeleteObject).Times(0);

  ASSERT_EQ(driver_remove("gs://mock_bucket/file_*_*.txt"), kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Remove_InvalidGlobbingPatternFolder) {
  EXPECT_CALL(*mock_client, ListObjects).Times(0);
  EXPECT_CALL(*mock_client, DeleteObject).Times(0);

  ASSERT_EQ(driver_remove("gs://mock_bucket/folder/*"), kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_Success) {
  const char *sources[3] = {"gs://mock_bucket/input/file_a.txt",
                            "gs://mock_bucket/input/file_b.txt",
                            "gs://mock_bucket/input/file_c.txt"};

  const std::string bucket = "mock_bucket";
  const std::string dest_object = "output/concatenated.txt";

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);
  metadata.set_name(dest_object);

  // Expect ComposeObject call
  EXPECT_CALL(
      *mock_client,
      ComposeObject(
          ::testing::Truly([bucket, dest_object](
                               gcs::internal::ComposeObjectRequest const &req) {
            return req.bucket_name() == bucket &&
                   req.object_name() == dest_object &&
                   req.source_objects().size() == 3 &&
                   req.source_objects()[0].object_name == "input/file_a.txt" &&
                   req.source_objects()[1].object_name == "input/file_b.txt" &&
                   req.source_objects()[2].object_name == "input/file_c.txt";
          })))
      .WillOnce(Return(metadata));

  // Expect DeleteObject calls for each source file
  ExpectDelete(*mock_client, bucket, "input/file_a.txt");
  ExpectDelete(*mock_client, bucket, "input/file_b.txt");
  ExpectDelete(*mock_client, bucket, "input/file_c.txt");

  ASSERT_EQ(
      driver_concat("gs://mock_bucket/output/concatenated.txt", sources, 3),
      kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, Concat_ComposeFailure) {
  const char *sources[2] = {"gs://mock_bucket/file1.txt",
                            "gs://mock_bucket/file2.txt"};

  // ComposeObject fails
  EXPECT_CALL(*mock_client, ComposeObject)
      .WillOnce(Return(gc::Status(gc::StatusCode::kUnknown, "Compose failed")));

  // DeleteObject should NOT be called since compose failed
  EXPECT_CALL(*mock_client, DeleteObject).Times(0);

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 2),
            kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_DeleteFailureAfterCompose) {
  const char *sources[2] = {"gs://mock_bucket/file1.txt",
                            "gs://mock_bucket/file2.txt"};

  const std::string bucket = "mock_bucket";

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);

  // ComposeObject succeeds
  EXPECT_CALL(*mock_client, ComposeObject).WillOnce(Return(metadata));

  // First delete succeeds
  ExpectDelete(*mock_client, bucket, "file1.txt");

  // Second delete fails
  ExpectDelete(*mock_client, bucket, "file2.txt",
               gc::Status(gc::StatusCode::kUnknown, "Delete failed"));

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 2),
            kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_DeleteNotFoundIgnored) {
  const char *sources[2] = {"gs://mock_bucket/file1.txt",
                            "gs://mock_bucket/file2.txt"};

  const std::string bucket = "mock_bucket";

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);

  // ComposeObject succeeds
  EXPECT_CALL(*mock_client, ComposeObject).WillOnce(Return(metadata));

  // First delete succeeds
  ExpectDelete(*mock_client, bucket, "file1.txt");

  // Second delete returns NotFound (should be ignored)
  ExpectDelete(*mock_client, bucket, "file2.txt",
               gc::Status(gc::StatusCode::kNotFound, "Not found"));

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 2),
            kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, Concat_NullPointers) {
  const char *sources[1] = {"gs://bucket/file.txt"};

  ASSERT_EQ(driver_concat(nullptr, sources, 1), kOtherFailure);
  ASSERT_EQ(driver_concat("gs://bucket/output.txt", nullptr, 1), kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_InvalidDestinationURI) {
  const char *sources[1] = {"gs://bucket/file.txt"};

  // Invalid URI formats
  ASSERT_EQ(driver_concat("invalid_uri", sources, 1), kOtherFailure);
  ASSERT_EQ(driver_concat("gs://bucket_only/", sources, 1), kOtherFailure);
  ASSERT_EQ(driver_concat("gs:///no_bucket", sources, 1), kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_SourceDifferentBucket_Fails) {
  const char *sources[2] = {"gs://mock_bucket/valid.txt",
                            "gs://other_bucket/other.txt"};

  EXPECT_CALL(*mock_client, ComposeObject).Times(0);
  EXPECT_CALL(*mock_client, DeleteObject).Times(0);
  EXPECT_CALL(*mock_client, CopyObject).Times(0);

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 2),
            kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_SourceRelativePath_Fails) {
  const char *sources[2] = {"file1.txt", "gs://mock_bucket/file2.txt"};

  EXPECT_CALL(*mock_client, ComposeObject).Times(0);
  EXPECT_CALL(*mock_client, DeleteObject).Times(0);
  EXPECT_CALL(*mock_client, CopyObject).Times(0);

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 2),
            kOtherFailure);
}

TEST_F(GCSDriverTestFixture, Concat_SingleFile) {
  const char *sources[1] = {"gs://mock_bucket/single.txt"};

  const std::string bucket = "mock_bucket";

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);

  // ComposeObject with single source
  EXPECT_CALL(*mock_client,
              ComposeObject(::testing::Truly(
                  [bucket](gcs::internal::ComposeObjectRequest const &req) {
                    return req.bucket_name() == bucket &&
                           req.source_objects().size() == 1 &&
                           req.source_objects()[0].object_name == "single.txt";
                  })))
      .WillOnce(Return(metadata));

  // Expect delete of the single source
  ExpectDelete(*mock_client, bucket, "single.txt");

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 1),
            kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, Concat_ManyFiles) {
  const char *sources[5] = {
      "gs://mock_bucket/file1.txt", "gs://mock_bucket/file2.txt",
      "gs://mock_bucket/file3.txt", "gs://mock_bucket/file4.txt",
      "gs://mock_bucket/file5.txt"};

  const std::string bucket = "mock_bucket";

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);

  // ComposeObject with 5 sources
  EXPECT_CALL(*mock_client,
              ComposeObject(::testing::Truly(
                  [bucket](gcs::internal::ComposeObjectRequest const &req) {
                    return req.bucket_name() == bucket &&
                           req.source_objects().size() == 5;
                  })))
      .WillOnce(Return(metadata));

  // Expect delete of all 5 sources
  for (int i = 1; i <= 5; ++i) {
    std::string filename = "file" + std::to_string(i) + ".txt";
    ExpectDelete(*mock_client, bucket, filename);
  }

  ASSERT_EQ(driver_concat("gs://mock_bucket/output.txt", sources, 5),
            kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, Concat_ManyFiles_Batching) {
  // Test with 70 files
  // New strategy:
  // 1. First batch: files[0-31] → temp_000000
  // 2. Second batch: temp_000000 + files[32-62] → temp_000001
  // 3. Third batch: temp_000001 + files[63-69] → temp_000002
  // 4. Final: temp_000002 → output.txt (via CopyObject)

  constexpr size_t num_files = 70;
  std::vector<std::string> source_names;
  source_names.reserve(num_files);

  for (size_t i = 0; i < num_files; ++i) {
    source_names.push_back("file" + std::to_string(i) + ".txt");
  }

  std::vector<std::string> source_uris;
  source_uris.reserve(num_files);
  for (const auto &name : source_names) {
    source_uris.push_back("gs://mock_bucket/" + name);
  }

  // Build array of const char* AFTER all strings are in the vector
  std::vector<const char *> sources;
  sources.reserve(num_files);

  for (const auto &uri : source_uris) {
    sources.push_back(uri.c_str());
  }

  const std::string bucket = "mock_bucket";

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);

  // ===== First batch: files[0-31] → temp_000000 =====
  EXPECT_CALL(*mock_client,
              ComposeObject(::testing::Truly(
                  [bucket](gcs::internal::ComposeObjectRequest const &req) {
                    if (req.bucket_name() != bucket)
                      return false;
                    if (req.object_name() != ".tmp_concat_output.txt_000000")
                      return false;
                    if (req.source_objects().size() != 32)
                      return false;

                    // Verify first few source objects
                    for (size_t i = 0; i < 3; ++i) {
                      std::string expected =
                          "file" + std::to_string(i) + ".txt";
                      if (req.source_objects()[i].object_name != expected)
                        return false;
                    }
                    return true;
                  })))
      .WillOnce(Return(metadata));

  // Expect deletion of files[0-31]
  for (size_t i = 0; i < 32; ++i) {
    ExpectDelete(*mock_client, bucket, source_names[i]);
  }

  // ===== Second batch: temp_000000 + files[32-62] → temp_000001 =====
  EXPECT_CALL(*mock_client,
              ComposeObject(::testing::Truly(
                  [bucket](gcs::internal::ComposeObjectRequest const &req) {
                    if (req.bucket_name() != bucket)
                      return false;
                    if (req.object_name() != ".tmp_concat_output.txt_000001")
                      return false;
                    if (req.source_objects().size() != 32)
                      return false;

                    // First source should be the previous temp file
                    if (req.source_objects()[0].object_name !=
                        ".tmp_concat_output.txt_000000")
                      return false;

                    // Next 31 sources should be files[32-62]
                    for (size_t i = 1; i < 32; ++i) {
                      std::string expected =
                          "file" + std::to_string(31 + i) + ".txt";
                      if (req.source_objects()[i].object_name != expected)
                        return false;
                    }
                    return true;
                  })))
      .WillOnce(Return(metadata));

  // Expect deletion of temp_000000
  ExpectDelete(*mock_client, bucket, ".tmp_concat_output.txt_000000");

  // Expect deletion of files[32-62]
  for (size_t i = 32; i < 63; ++i) {
    ExpectDelete(*mock_client, bucket, source_names[i]);
  }

  // ===== Third batch: temp_000001 + files[63-69] → temp_000002 =====
  EXPECT_CALL(*mock_client,
              ComposeObject(::testing::Truly(
                  [bucket](gcs::internal::ComposeObjectRequest const &req) {
                    if (req.bucket_name() != bucket)
                      return false;
                    if (req.object_name() != ".tmp_concat_output.txt_000002")
                      return false;
                    if (req.source_objects().size() != 8)
                      return false; // 1 temp + 7 files

                    // First source should be the previous temp file
                    if (req.source_objects()[0].object_name !=
                        ".tmp_concat_output.txt_000001")
                      return false;

                    // Next 7 sources should be files[63-69]
                    for (size_t i = 1; i < 8; ++i) {
                      std::string expected =
                          "file" + std::to_string(62 + i) + ".txt";
                      if (req.source_objects()[i].object_name != expected)
                        return false;
                    }
                    return true;
                  })))
      .WillOnce(Return(metadata));

  // Expect deletion of temp_000001
  ExpectDelete(*mock_client, bucket, ".tmp_concat_output.txt_000001");

  // Expect deletion of files[63-69]
  for (size_t i = 63; i < 70; ++i) {
    ExpectDelete(*mock_client, bucket, source_names[i]);
  }

  // ===== Final step: CopyObject temp_000002 → output.txt =====
  EXPECT_CALL(*mock_client,
              CopyObject(::testing::Truly(
                  [bucket](gcs::internal::CopyObjectRequest const &req) {
                    return req.source_bucket() == bucket &&
                           req.source_object() ==
                               ".tmp_concat_output.txt_000002" &&
                           req.destination_bucket() == bucket &&
                           req.destination_object() == "output.txt";
                  })))
      .WillOnce(Return(metadata));

  // Expect deletion of final temp file
  ExpectDelete(*mock_client, bucket, ".tmp_concat_output.txt_000002");

  ASSERT_EQ(
      driver_concat("gs://mock_bucket/output.txt", sources.data(), num_files),
      kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, ComposeMultifile_Success) {
  const char *sources[3] = {"file_a.txt", "file_b.txt", "file_c.txt"};

  const std::string bucket = "mock_bucket";

  // Expected renamed files
  std::vector<std::string> expected_names = {"output/data_000000000000.txt",
                                             "output/data_000000000001.txt",
                                             "output/data_000000000002.txt"};

  gcs::ObjectMetadata metadata;
  metadata.set_bucket(bucket);

  for (size_t i = 0; i < 3; ++i) {
    const std::string source = sources[i];
    const std::string expected = expected_names[i];

    // Expect CopyObject call instead of ComposeObject
    EXPECT_CALL(*mock_client,
                CopyObject(::testing::Truly(
                    [bucket, source,
                     expected](gcs::internal::CopyObjectRequest const &req) {
                      return req.source_bucket() == bucket &&
                             req.source_object() == source &&
                             req.destination_bucket() == bucket &&
                             req.destination_object() == expected;
                    })))
        .WillOnce(Return(metadata));

    // Expect DeleteObject call for original file
    ExpectDelete(*mock_client, bucket, source);
  }

  ASSERT_EQ(
      driver_composeMultifile("gs://mock_bucket/output/data_*.txt", sources, 3),
      kOtherSuccess);
}

TEST_F(GCSDriverTestFixture, ComposeMultifile_InvalidPattern) {
  const char *sources[1] = {"file.txt"};

  // No '*' in pattern
  ASSERT_EQ(driver_composeMultifile("gs://mock_bucket/output.txt", sources, 1),
            kOtherFailure);

  // Multiple '*'
  ASSERT_EQ(
      driver_composeMultifile("gs://mock_bucket/output_*_*.txt", sources, 1),
      kOtherFailure);

  // Prefix ends with digit
  ASSERT_EQ(
      driver_composeMultifile("gs://mock_bucket/output1*.txt", sources, 1),
      kOtherFailure);

  // Suffix starts with digit
  ASSERT_EQ(
      driver_composeMultifile("gs://mock_bucket/output_*1.txt", sources, 1),
      kOtherFailure);

  // Slash-star is forbidden
  ASSERT_EQ(driver_composeMultifile("gs://mock_bucket/folder/*", sources, 1),
            kOtherFailure);
}

TEST_F(GCSDriverTestFixture, ComposeMultifile_NonRelativePath) {
  const char *sources[2] = {"gs://bucket/file.txt", // Absolute GCS path
                            "relative/file.txt"};

  ASSERT_EQ(
      driver_composeMultifile("gs://mock_bucket/output_*.txt", sources, 2),
      kOtherFailure);
}

TEST_F(GCSDriverTestFixture, ComposeMultifile_NullPointers) {
  const char *sources[1] = {"file.txt"};

  ASSERT_EQ(driver_composeMultifile(nullptr, sources, 1), kOtherFailure);
  ASSERT_EQ(driver_composeMultifile("gs://bucket/*", nullptr, 1),
            kOtherFailure);
}

TEST_F(GCSDriverTestFixture, ComposeMultifile_EmptyList) {
  const char *sources[1] = {"file.txt"};

  ASSERT_EQ(driver_composeMultifile("gs://bucket/*", sources, 0),
            kOtherFailure);
}
