#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "gcsplugin.h"
#include "gcsplugin_internal.h"

#include <algorithm>
#include <assert.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits.h>
#include <limits>
#include <memory>
#include <sstream>

#include "google/cloud/rest_options.h"
#include "google/cloud/storage/client.h"
#include <google/cloud/storage/object_write_stream.h>

// Use boost for generating unique object id while appending
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "spdlog/spdlog.h"

using namespace gcsplugin;

namespace gc = ::google::cloud;
namespace gcs = gc::storage;

constexpr const char *version = "0.1.0";
constexpr const char *driver_name = "GCS driver";
constexpr const char *driver_scheme = "gs";
// Buffer size might need some tuning
// ref https://github.com/googleapis/google-cloud-cpp/issues/2657
// Default value below can be overriden by setting GCS_PREFERRED_BUFFER_SIZE
constexpr long long preferred_buffer_size = 4 * 1024 * 1024;

bool bIsConnected = false;

gcs::Client client;
// Global bucket name
std::string globalBucketName;

// Last error
std::string lastError;

HandleContainer active_handles;

#define RETURN_STATUS(x) return std::move((x)).status();

#define RETURN_STATUS_ON_ERROR(x)                                              \
  if (!(x)) {                                                                  \
    RETURN_STATUS((x));                                                        \
  }

#define ERROR_ON_NULL_ARG(arg, msg, err_val)                                   \
  if (!(arg)) {                                                                \
    LogError((msg));                                                           \
    return (err_val);                                                          \
  }

void LogError(const std::string &msg) {
  lastError = msg;
  spdlog::error(lastError);
}

void LogBadStatus(const gc::Status &status, const std::string &msg) {
  std::ostringstream os;
  os << msg << ": " << status;
  LogError(os.str());
}

void InitHandle(Handle &h, ReaderPtr &&r_ptr) {
  h.var.reader = std::move(r_ptr);
}

void InitHandle(Handle &h, WriterPtr &&w_ptr) {
  h.var.writer = std::move(w_ptr);
}

template <typename VariantPtr, HandleType Type>
HandlePtr MakeHandleFromVariant(VariantPtr &&var_ptr) {
  HandlePtr h{new Handle(Type)};
  InitHandle(*h, std::move(var_ptr));
  return h;
}

template <typename VariantPtr, HandleType Type>
Handle *InsertHandle(VariantPtr &&var_ptr) {
  return active_handles
      .insert(active_handles.end(),
              MakeHandleFromVariant<VariantPtr, Type>(std::move(var_ptr)))
      ->get();
}

HandleIt FindHandle(void *handle) {
  return std::find_if(active_handles.begin(), active_handles.end(),
                      [handle](const HandlePtr &act_h_ptr) {
                        return handle == static_cast<void *>(act_h_ptr.get());
                      });
}

void EraseRemove(HandleIt pos) {
  *pos = std::move(active_handles.back());
  active_handles.pop_back();
}

// Definition of helper functions
gc::StatusOr<long long int>
DownloadFileRangeToBuffer(const std::string &bucket_name,
                          const std::string &object_name, char *buffer,
                          std::int64_t start_range, std::int64_t end_range) {
  auto reader = client.ReadObject(bucket_name, object_name,
                                  gcs::ReadRange(start_range, end_range));
  if (!reader) {
    auto &o_status = reader.status();
    return gc::Status{o_status.code(), "Error while creating reading stream; " +
                                           o_status.message()};
  }

  reader.read(buffer, end_range - start_range);
  if (reader.bad()) {
    auto &o_status = reader.status();
    return gc::Status{o_status.code(), "Error while creating reading stream; " +
                                           o_status.message()};
  }

  long long int num_read = static_cast<long long>(reader.gcount());
  spdlog::debug("read = {}", num_read);

  return num_read;
}

gc::StatusOr<long long> ReadBytesInFile(MultiPartFile &multifile, char *buffer,
                                        tOffset to_read) {
  // Start at first usable file chunk
  // Advance through file chunks, advancing buffer pointer
  // Until last requested byte was read
  // Or error occured

  tOffset bytes_read{0};

  // Lookup item containing initial bytes at requested offset
  const auto &cumul_sizes = multifile.cumulativeSize_;
  const tOffset common_header_length = multifile.commonHeaderLength_;
  const std::string &bucket_name = multifile.bucketname_;
  const auto &filenames = multifile.filenames_;
  char *buffer_pos = buffer;
  tOffset &offset = multifile.offset_;
  const tOffset offset_bak = offset; // in case of irrecoverable error, leave
                                     // the multifile in its starting state

  auto greater_than_offset_it =
      std::upper_bound(cumul_sizes.begin(), cumul_sizes.end(), offset);
  size_t idx = static_cast<size_t>(
      std::distance(cumul_sizes.begin(), greater_than_offset_it));

  spdlog::debug("Use item {} to read @ {} (end = {})", idx, offset,
                *greater_than_offset_it);

  auto read_range_and_update = [&](const std::string &filename, tOffset start,
                                   tOffset end) -> gc::Status {
    auto maybe_actual_read = DownloadFileRangeToBuffer(
        bucket_name, filename, buffer_pos, static_cast<int64_t>(start),
        static_cast<int64_t>(end));
    if (!maybe_actual_read) {
      offset = offset_bak;
      RETURN_STATUS(maybe_actual_read);
    }

    tOffset actual_read = *maybe_actual_read;

    bytes_read += actual_read;
    buffer_pos += actual_read;
    offset += actual_read;

    if (actual_read < (end - start) /*expected read*/) {
      spdlog::debug("End of file encountered");
      to_read = 0;
    } else {
      to_read -= actual_read;
    }

    return {};
  };

  // first file read

  const tOffset file_start =
      (idx == 0) ? offset
                 : offset - cumul_sizes[idx - 1] + common_header_length;
  const tOffset read_end =
      std::min(file_start + to_read, file_start + cumul_sizes[idx] - offset);

  gc::Status read_status =
      read_range_and_update(filenames[idx], file_start, read_end);

  // continue with the next files
  while (read_status.ok() && to_read) {
    // read the missing bytes in the next files as necessary
    idx++;
    const tOffset start = common_header_length;
    const tOffset end = std::min(start + to_read, start + cumul_sizes[idx] -
                                                      cumul_sizes[idx - 1]);

    read_status = read_range_and_update(filenames[idx], start, end);
  }

  return read_status.ok() ? bytes_read : gc::StatusOr<long long>{read_status};
}

struct ParseUriResult {
  std::string bucket;
  std::string object;
};

gc::StatusOr<ParseUriResult> ParseGcsUri(const std::string &gcs_uri) {
  char const *prefix = "gs://";
  const size_t prefix_size{std::strlen(prefix)};
  if (gcs_uri.compare(0, prefix_size, prefix) != 0) {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Invalid GCS URI: " + gcs_uri};
  }

  const size_t pos = gcs_uri.find('/', prefix_size);
  if (pos == std::string::npos) {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Invalid GCS URI, missing object name: " + gcs_uri};
  }

  return ParseUriResult{gcs_uri.substr(prefix_size, pos - prefix_size),
                        gcs_uri.substr(pos + 1)};
}

gc::StatusOr<ParseUriResult> GetBucketAndObjectNames(
    const char *sFilePathName) //, std::string &bucket, std::string &object)
{
  auto maybe_parse_res = ParseGcsUri(sFilePathName);
  if (!maybe_parse_res) {
    return maybe_parse_res;
  }

  // fallback to default bucket if bucket empty
  if (maybe_parse_res->bucket.empty()) {
    if (globalBucketName.empty()) {
      maybe_parse_res =
          gc::Status{gc::StatusCode::kInternal,
                     "No bucket specified and GCS_BUCKET_NAME is not set!"};
    } else {
      maybe_parse_res->bucket = globalBucketName;
    }
  }
  return maybe_parse_res;
}

std::string ToLower(const std::string &str) {
  std::string low{str};
  const size_t cnt = low.length();
  for (size_t i = 0; i < cnt; i++) {
    low[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(
        low[i]))); // see https://en.cppreference.com/w/cpp/string/byte/tolower
  }
  return low;
}

std::string GetEnvironmentVariableOrDefault(const std::string &variable_name,
                                            const std::string &default_value) {
#ifdef _WIN32
  size_t len;
  char value[2048];
  getenv_s(&len, value, 2048, name.c_str());
#else
  char *value = getenv(variable_name.c_str());
#endif

  if (value && std::strlen(value) > 0) {
    return value;
  }

  const std::string low_key = ToLower(variable_name);
  if (low_key.find("token") || low_key.find("password") ||
      low_key.find("key") || low_key.find("secret")) {
    spdlog::debug("No {} specified, using **REDACTED** as default.",
                  variable_name);
  } else {
    spdlog::debug("No {} specified, using '{}' as default.", variable_name,
                  default_value);
  }

  return default_value;
}

bool WillSizeCountProductOverflow(size_t size, size_t count) {
  constexpr size_t max_prod_usable{
      static_cast<size_t>(std::numeric_limits<tOffset>::max())};
  return (max_prod_usable / size < count || max_prod_usable / count < size);
}

gc::StatusOr<gcs::ListObjectsReader>
ListObjects(const std::string &bucket_name, const std::string &object_name) {
  auto list = client.ListObjects(bucket_name, gcs::MatchGlob{object_name});
  auto first = list.begin();
  if (first == list.end()) {
    return gc::Status{gc::StatusCode::kNotFound,
                      "Error while searching object : not found"};
  }
  if (!first->ok()) {
    return std::move(*first).status();
  }
  return list;
}

// pre condition: stream is of a writing type. do not call otherwise.
gc::Status CloseWriterStream(Handle &stream) {
  gc::StatusOr<gcs::ObjectMetadata> maybe_meta;
  std::ostringstream err_msg_os;

  // close the stream to flush all remaining bytes in the put area
  auto &writer = stream.GetWriter().writer_;
  writer.Close();
  maybe_meta = writer.metadata();
  if (!maybe_meta) {
    err_msg_os << "Error during upload";
  } else if (HandleType::kAppend == stream.type) {
    // the tmp file is valid and ready for composition with the source
    const auto &writer_h = stream.GetWriter();
    const std::string &bucket = writer_h.bucketname_;
    const std::string &append_source = writer_h.filename_;
    const std::string &dest = writer_h.append_target_;
    std::vector<gcs::ComposeSourceObject> source_objects = {
        {dest, {}, {}}, {append_source, {}, {}}};
    maybe_meta = client.ComposeObject(bucket, std::move(source_objects), dest);

    // whatever happened, delete the tmp file
    gc::Status delete_status = client.DeleteObject(bucket, append_source);

    // TODO: what to do with an error on Delete?
    (void)delete_status;

    // if composition failed, nothing is written, the source did not change.
    // signal it
    if (!maybe_meta) {
      err_msg_os << "Error while uploading the data to append";
    }
  }

  if (maybe_meta) {
    return {};
  }

  const gc::Status &status = maybe_meta.status();
  err_msg_os << ": " << maybe_meta.status().message();
  return gc::Status{status.code(), err_msg_os.str()};
}

// Implementation of driver functions
void test_setClient(::google::cloud::storage::Client &&mock_client) {
  client = std::move(mock_client);
  bIsConnected = kTrue;
}

void test_unsetClient() { client = ::google::cloud::storage::Client{}; }

void *test_getActiveHandles() { return &active_handles; }

void *test_addReaderHandle(const std::string &bucket, const std::string &object,
                           long long offset, long long commonHeaderLength,
                           const std::vector<std::string> &filenames,
                           const std::vector<long long int> &cumulativeSize,
                           long long total_size) {
  ReaderPtr reader_ptr{new MultiPartFile{bucket, object, offset,
                                         commonHeaderLength, filenames,
                                         cumulativeSize, total_size}};
  return InsertHandle<ReaderPtr, HandleType::kRead>(std::move(reader_ptr));
}

void *test_addWriterHandle(bool appendMode, bool create_with_mock_client,
                           std::string bucketname, std::string objectname) {
  if (!create_with_mock_client) {
    if (appendMode) {
      return InsertHandle<WriterPtr, HandleType::kAppend>(
          WriterPtr(new WriteFile));
    }
    return InsertHandle<WriterPtr, HandleType::kWrite>(
        WriterPtr(new WriteFile));
  }

  auto writer = client.WriteObject(bucketname, objectname);
  if (!writer) {
    return nullptr;
  }

  WriterPtr writer_struct{new WriteFile};
  writer_struct->bucketname_ = std::move(bucketname);
  writer_struct->filename_ = std::move(objectname);
  writer_struct->writer_ = std::move(writer);

  if (appendMode) {
    return InsertHandle<WriterPtr, HandleType::kAppend>(
        std::move(writer_struct));
  }
  return InsertHandle<WriterPtr, HandleType::kWrite>(std::move(writer_struct));
}

const char *driver_getDriverName() { return driver_name; }

const char *driver_getVersion() { return version; }

const char *driver_getScheme() { return driver_scheme; }

int driver_isReadOnly() { return kFalse; }

int driver_connect() {
  const std::string loglevel =
      GetEnvironmentVariableOrDefault("GCS_DRIVER_LOGLEVEL", "info");
  if (loglevel == "debug")
    spdlog::set_level(spdlog::level::debug);
  else if (loglevel == "trace")
    spdlog::set_level(spdlog::level::trace);
  else
    spdlog::set_level(spdlog::level::info);

  spdlog::debug("Connect {}", loglevel);

  // Initialize variables from environment
  globalBucketName = GetEnvironmentVariableOrDefault("GCS_BUCKET_NAME", "");

  gc::Options options{};

  // Add project ID if defined
  std::string project =
      GetEnvironmentVariableOrDefault("CLOUD_ML_PROJECT_ID", "");
  if (!project.empty()) {
    options.set<gc::UserProjectOption>(std::move(project));
  }

  std::string gcp_token_filename =
      GetEnvironmentVariableOrDefault("GCP_TOKEN", "");
  if (!gcp_token_filename.empty()) {
    // Initialize from token file
    std::ifstream t(gcp_token_filename);
    std::stringstream buffer;
    buffer << t.rdbuf();
    if (t.fail()) {
      LogError("Error initializing token from file");
      return kFailure;
    }
    std::shared_ptr<gc::Credentials> creds =
        gc::MakeServiceAccountCredentials(buffer.str());
    options.set<gc::UnifiedCredentialsOption>(std::move(creds));
  }

  // Create client with configured options
  client = gcs::Client{std::move(options)};

  bIsConnected = true;
  return kSuccess;
}

int driver_disconnect() {
  // loop on the still active handles to close as necessary and remove. clear()
  // on the container would do it but the procedures would fail silently.
  std::vector<gc::Status> failures;
  for (auto &h_ptr : active_handles) {
    // the writing streams need to be closed
    const HandleType type = h_ptr->type;
    if (HandleType::kRead != type) {
      gc::Status status = CloseWriterStream(*h_ptr);
      if (!status.ok()) {
        failures.push_back(std::move(status));
      }
    }
  }
  active_handles.clear();

  bIsConnected = false;

  if (failures.empty()) {
    return kSuccess;
  }

  std::ostringstream os;
  os << "Errors occured during disconnection:\n";
  for (const auto &status : failures) {
    os << status << '\n';
  }
  LogError(os.str());
  return kFailure;
}

int driver_isConnected() { return bIsConnected ? 1 : 0; }

long long int driver_getSystemPreferredBufferSize() {
  std::string configured_preferred_size = GetEnvironmentVariableOrDefault(
      "GCS_PREFERRED_BUFFER_SIZE", std::to_string(preferred_buffer_size));
  return std::stoi(configured_preferred_size);
}

int driver_exist(const char *filename) {
  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to exist", kFalse);

  spdlog::debug("exist {}", filename);

  std::string file_uri = filename;
  spdlog::debug("exist file_uri {}", file_uri);
  spdlog::debug("exist last char {}", file_uri.back());

  if (file_uri.back() == '/') {
    return driver_dirExists(filename);
  } else {
    return driver_fileExists(filename);
  }
}

#define RETURN_ON_ERROR(status_or, msg, err_val)                               \
  {                                                                            \
    if (!(status_or)) {                                                        \
      LogBadStatus((status_or).status(), (msg));                               \
      return (err_val);                                                        \
    }                                                                          \
  }

#define ERROR_ON_NAMES(status_or_names, err_val)                               \
  RETURN_ON_ERROR((status_or_names), "Error parsing URL", (err_val))

int driver_fileExists(const char *sFilePathName) {
  ERROR_ON_NULL_ARG(sFilePathName, "Error passing null pointer to fileExists.",
                    kFalse);

  spdlog::debug("fileExist {}", sFilePathName);

  auto maybe_parsed_names = GetBucketAndObjectNames(sFilePathName);
  ERROR_ON_NAMES(maybe_parsed_names, kFalse);

  auto maybe_list =
      ListObjects(maybe_parsed_names->bucket, maybe_parsed_names->object);
  if (!maybe_list) {
    if (maybe_list.status().code() != gc::StatusCode::kNotFound) {
      LogBadStatus((maybe_list).status(), ("Error checking if file exists"));
    }
    return kFalse;
  }

  spdlog::debug("file {} exists!", sFilePathName);
  return kTrue; // L'objet existe
}

int driver_dirExists(const char *sFilePathName) {
  ERROR_ON_NULL_ARG(sFilePathName, "Error passing null pointer to dirExists",
                    kFalse);

  spdlog::debug("dirExist {}", sFilePathName);
  return kTrue;
}

gc::StatusOr<std::string> ReadHeader(const std::string &bucket_name,
                                     const std::string &filename) {
  gcs::ObjectReadStream stream = client.ReadObject(bucket_name, filename);
  std::string line;
  std::getline(stream, line, '\n');
  if (stream.bad()) {
    return stream.status();
  }
  if (!stream.eof()) {
    line.push_back('\n');
  }
  if (line.empty()) {
    return gc::Status{gc::StatusCode::kInternal, "Got an empty header"};
  }
  return line;
}

gc::StatusOr<long long> GetFileSize(const std::string &bucket_name,
                                    const std::string &object_name) {
  auto maybe_list = ListObjects(bucket_name, object_name);
  RETURN_STATUS_ON_ERROR(maybe_list);

  auto list_it = maybe_list->begin();
  const auto list_end = maybe_list->end();

  const auto first_object_metadata = std::move(*list_it);
  long long total_size = static_cast<long long>(first_object_metadata->size());

  list_it++;
  if (list_end == list_it) {
    // unique file
    return total_size;
  }

  // multifile
  // check headers
  auto maybe_header = ReadHeader(bucket_name, first_object_metadata->name());
  RETURN_STATUS_ON_ERROR(maybe_header);

  const std::string &header = *maybe_header;
  const long long header_size = static_cast<long long>(header.size());
  int header_to_subtract{0};
  bool same_header{true};

  for (; list_it != list_end; list_it++) {
    RETURN_STATUS_ON_ERROR(*list_it);

    if (same_header) {
      auto maybe_curr_header = ReadHeader(bucket_name, (*list_it)->name());
      RETURN_STATUS_ON_ERROR(maybe_curr_header);

      same_header = (header == *maybe_curr_header);
      if (same_header) {
        header_to_subtract++;
      }
    }
    total_size += static_cast<long long>((*list_it)->size());
  }

  if (!same_header) {
    header_to_subtract = 0;
  }
  return total_size - header_to_subtract * header_size;
}

long long int driver_getFileSize(const char *filename) {
  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to getFileSize.", -1);

  spdlog::debug("getFileSize {}", filename);

  auto maybe_names = ParseGcsUri(filename);
  ERROR_ON_NAMES(maybe_names, -1);

  auto maybe_file_size = GetFileSize(maybe_names->bucket, maybe_names->object);
  RETURN_ON_ERROR(maybe_file_size, "Error getting file size", -1);

  return *maybe_file_size;
}

gc::StatusOr<ReaderPtr> MakeReaderPtr(std::string bucketname,
                                      std::string objectname) {
  std::vector<std::string> filenames;
  std::vector<long long> cumulative_sizes;

  auto maybe_list = ListObjects(bucketname, objectname);
  RETURN_STATUS_ON_ERROR(maybe_list);

  auto list_it = maybe_list->begin();
  const auto list_end = maybe_list->end();

  filenames.push_back((*list_it)->name());
  cumulative_sizes.push_back(static_cast<long long>((*list_it)->size()));
  long long common_header_size{0};

  list_it++;
  if (list_end != list_it) {
    // multifile
    // check headers
    auto maybe_header = ReadHeader(bucketname, filenames.front());
    RETURN_STATUS_ON_ERROR(maybe_header);

    const std::string &header = *maybe_header;
    const long long header_size = static_cast<long long>(header.size());
    bool same_header{true};

    for (; list_it != list_end; list_it++) {
      RETURN_STATUS_ON_ERROR(*list_it);

      filenames.push_back((*list_it)->name());
      cumulative_sizes.push_back(cumulative_sizes.back() +
                                 static_cast<long long>((*list_it)->size()));

      if (same_header) {
        auto maybe_curr_header = ReadHeader(bucketname, filenames.back());
        RETURN_STATUS_ON_ERROR(maybe_curr_header);
        same_header = (header == *maybe_curr_header);
      }
    }

    // if headers remained the same, adjust cumulative_sizes
    if (same_header) {
      common_header_size = header_size;
      for (size_t i = 0; i < cumulative_sizes.size(); i++) {
        cumulative_sizes[i] -= (i * common_header_size);
      }
    }
  }

  tOffset total_size = cumulative_sizes.back();
  return ReaderPtr(new MultiPartFile{
      std::move(bucketname), std::move(objectname), 0, common_header_size,
      std::move(filenames), std::move(cumulative_sizes), total_size});
}

gc::StatusOr<WriterPtr> MakeWriterPtr(std::string bucketname,
                                      std::string objectname) {
  auto writer = client.WriteObject(bucketname, objectname);
  if (!writer) {
    return writer.last_status();
  }
  WriterPtr writer_struct{new WriteFile};
  writer_struct->bucketname_ = std::move(bucketname);
  writer_struct->filename_ = std::move(objectname);
  writer_struct->writer_ = std::move(writer);
  return writer_struct;
}

template <typename StreamPtr, HandleType Type>
gc::StatusOr<Handle *>
RegisterStream(std::function<gc::StatusOr<StreamPtr>(std::string, std::string)>
                   MakeStreamPtr,
               std::string &&bucket, std::string &&object) {
  auto maybe_stream = MakeStreamPtr(std::move(bucket), std::move(object));
  RETURN_STATUS_ON_ERROR(maybe_stream);

  return InsertHandle<StreamPtr, Type>(std::move(maybe_stream).value());
}

gc::StatusOr<Handle *> RegisterReader(std::string &&bucket,
                                      std::string &&object) {
  return RegisterStream<ReaderPtr, HandleType::kRead>(
      MakeReaderPtr, std::move(bucket), std::move(object));
}

gc::StatusOr<Handle *> RegisterWriter(std::string &&bucket,
                                      std::string &&object) {
  return RegisterStream<WriterPtr, HandleType::kWrite>(
      MakeWriterPtr, std::move(bucket), std::move(object));
}

gc::StatusOr<Handle *> RegisterWriterForAppend(std::string &&bucket,
                                               std::string &&tmp,
                                               std::string append_target) {
  auto maybe_handle = RegisterStream<WriterPtr, HandleType::kAppend>(
      MakeWriterPtr, std::move(bucket), std::move(tmp));
  if (maybe_handle) {
    (*maybe_handle)->GetWriter().append_target_ = std::move(append_target);
  }
  return maybe_handle;
}

void *driver_fopen(const char *filename, char mode) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to fopen.", nullptr);

  spdlog::debug("fopen {} {}", filename, mode);

  auto maybe_names = GetBucketAndObjectNames(filename);
  ERROR_ON_NAMES(maybe_names, nullptr);

  auto &names = *maybe_names;

  gc::StatusOr<Handle *> maybe_handle;
  std::string err_msg;

  switch (mode) {
  case 'r': {
    maybe_handle =
        RegisterReader(std::move(names.bucket), std::move(names.object));
    err_msg = "Error while opening reader stream";
    break;
  }
  case 'w': {
    maybe_handle =
        RegisterWriter(std::move(names.bucket), std::move(names.object));
    err_msg = "Error while opening writer stream";
    break;
  }
  case 'a': {
    // GCS does not as yet provide a way to add data to existing files.
    // This will be the process to emulate an append:
    // - check existence of the target object
    //   - if file does not exist, fallback to write mode
    // - open a temporary write object to upload the new data
    // - compose, as defined by GCS, the source with the new temporary object
    //
    // The actual composition will happen on closing of the append stream

    auto maybe_list = ListObjects(names.bucket, names.object);
    if (!maybe_list) {
      auto &status = maybe_list.status();
      if (gc::StatusCode::kNotFound == status.code()) {
        // file doesn't exist, fallback to write mode
        maybe_handle =
            RegisterWriter(std::move(names.bucket), std::move(names.object));
      } else {
        // genuine error
        maybe_handle = status;
      }
      err_msg = "Error while opening writer stream";
      break;
    }

    // go to end of list to get the target file name
    auto list_it = maybe_list->begin();
    const auto list_end = maybe_list->end();
    auto to_last_item = list_it;
    list_it++;
    while (list_end != list_it) {
      to_last_item = list_it;
      list_it++;
    }

    if (!to_last_item->ok()) {
      // data is unusable
      maybe_handle = std::move(*to_last_item).status();
      err_msg = "Error opening file in append mode";
      break;
    }

    // get a writer handle
    maybe_handle = RegisterWriterForAppend(
        std::move(names.bucket),
        std::string("tmp_object_to_append_") +
            boost::uuids::to_string(boost::uuids::random_generator()()),
        to_last_item->value().name());
    err_msg = "Error opening file in append mode, cannot open tmp object";
    break;
  }
  default:
    LogError(std::string("Invalid open mode: ") + mode);
    return nullptr;
  }

  RETURN_ON_ERROR(maybe_handle, err_msg, nullptr);

  return *maybe_handle;
}

#define ERROR_NO_STREAM(handle_it, errval)                                     \
  if ((handle_it) == active_handles.end()) {                                   \
    LogError("Cannot identify stream");                                        \
    return (errval);                                                           \
  }

int driver_fclose(void *stream) {
  assert(driver_isConnected());

  ERROR_ON_NULL_ARG(stream, "Error passing null pointer to fclose", kCloseEOF);

  spdlog::debug("fclose {}", (void *)stream);

  auto stream_it = FindHandle(stream);
  ERROR_NO_STREAM(stream_it, kCloseEOF);
  auto &h_ptr = *stream_it;

  gc::Status status;

  if (HandleType::kRead != h_ptr->type) {
    status = CloseWriterStream(*h_ptr);
  }

  EraseRemove(stream_it);

  if (!status.ok()) {
    LogBadStatus(status, "Error while closing writer stream");
    return kFailure;
  }

  return kCloseSuccess;
}

int driver_fseek(void *stream, long long int offset, int whence) {
  constexpr long long max_val = std::numeric_limits<long long>::max();

  ERROR_ON_NULL_ARG(stream, "Error passing null pointer to fseek", -1);

  // confirm stream's presence
  auto to_stream = FindHandle(stream);
  ERROR_NO_STREAM(to_stream, -1);

  auto &stream_h = *to_stream;

  if (HandleType::kRead != stream_h->type) {
    LogError("Cannot seek on not reading stream");
    return -1;
  }

  spdlog::debug("fseek {} {} {}", stream, offset, whence);

  MultiPartFile &h = stream_h->GetReader();

  tOffset computed_offset{0};

  switch (whence) {
  case std::ios::beg:
    computed_offset = offset;
    break;
  case std::ios::cur:
    if (offset > max_val - h.offset_) {
      LogError("Signed overflow prevented");
      return -1;
    }
    computed_offset = h.offset_ + offset;
    break;
  case std::ios::end:
    if (h.total_size_ > 0) {
      long long minus1 = h.total_size_ - 1;
      if (offset > max_val - minus1) {
        LogError("Signed overflow prevented");
        return -1;
      }
    }
    if ((offset == std::numeric_limits<long long>::min()) &&
        (h.total_size_ == 0)) {
      LogError("Signed overflow prevented");
      return -1;
    }

    computed_offset =
        (h.total_size_ == 0) ? offset : h.total_size_ - 1 + offset;
    break;
  default:
    LogError("Invalid seek mode " + std::to_string(whence));
    return -1;
  }

  if (computed_offset < 0) {
    LogError("Invalid seek offset " + std::to_string(computed_offset));
    return -1;
  }
  h.offset_ = computed_offset;
  return 0;
}

const char *driver_getlasterror() {
  spdlog::debug("getlasterror");

  if (!lastError.empty()) {
    return lastError.c_str();
  }
  return NULL;
}

long long int driver_fread(void *ptr, size_t size, size_t count, void *stream) {
  ERROR_ON_NULL_ARG(stream, "Error passing null stream pointer to fread", -1);
  ERROR_ON_NULL_ARG(ptr, "Error passing null buffer pointer to fread", -1);

  if (0 == size) {
    LogError("Error passing size of 0");
    return -1;
  }

  // confirm stream's presence
  auto to_stream = FindHandle(stream);
  if (to_stream == active_handles.end()) {
    LogError("Cannot identify stream");
    return -1;
  }

  auto &stream_h = *to_stream;

  if (HandleType::kRead != stream_h->type) {
    LogError("Cannot read on not reading stream");
    return -1;
  }

  spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

  MultiPartFile &h = stream_h->GetReader();

  const tOffset offset = h.offset_;

  // fast exit for 0 read
  if (0 == count) {
    return 0;
  }

  // prevent overflow
  if (WillSizeCountProductOverflow(size, count)) {
    LogError("product size * count is too large, would overflow");
    return -1;
  }

  tOffset to_read{static_cast<tOffset>(size * count)};
  if (offset > std::numeric_limits<long long>::max() - to_read) {
    LogError("signed overflow prevented on reading attempt");
    return -1;
  }
  // end of overflow prevention

  // special case: if offset >= total_size, error if not 0 byte required. 0 byte
  // required is already done above
  const tOffset total_size = h.total_size_;
  if (offset >= total_size) {
    LogError("Error trying to read more bytes while already out of bounds");
    return -1;
  }

  // normal cases
  if (offset + to_read > total_size) {
    to_read = total_size - offset;
    spdlog::debug(
        "offset {}, req len {} exceeds file size ({}) -> reducing len to {}",
        offset, to_read, total_size, to_read);
  } else {
    spdlog::debug("offset = {} to_read = {}", offset, to_read);
  }

  auto maybe_read = ReadBytesInFile(h, reinterpret_cast<char *>(ptr), to_read);
  RETURN_ON_ERROR(maybe_read, "Error while reading from file", -1);

  return *maybe_read;
}

long long int driver_fwrite(const void *ptr, size_t size, size_t count,
                            void *stream) {
  ERROR_ON_NULL_ARG(stream, "Error passing null stream pointer to fwrite", -1);
  ERROR_ON_NULL_ARG(ptr, "Error passing null buffer pointer to fwrite", -1);

  if (0 == size) {
    LogError("Error passing size 0 to fwrite");
    return -1;
  }

  spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

  auto stream_it = FindHandle(stream);
  ERROR_NO_STREAM(stream_it, -1);
  Handle &stream_h = **stream_it;

  const HandleType type = stream_h.type;

  if (HandleType::kRead == type) {
    LogError("Cannot write on not writing stream");
    return -1;
  }

  // fast exit for 0
  if (0 == count) {
    return 0;
  }

  // prevent integer overflow
  if (WillSizeCountProductOverflow(size, count)) {
    LogError(
        "Error on write: product size * count is too large, would overflow");
    return -1;
  }

  const long long to_write = static_cast<long long>(size * count);

  gcs::ObjectWriteStream &writer = stream_h.GetWriter().writer_;
  writer.write(static_cast<const char *>(ptr), to_write);
  if (writer.bad()) {
    LogBadStatus(writer.last_status(), "Error during upload");
    return -1;
  }
  spdlog::debug("Write status after write: good {}, bad {}, fail {}",
                writer.good(), writer.bad(), writer.fail());

  return to_write;
}

int driver_fflush(void *stream) {
  ERROR_ON_NULL_ARG(stream, "Error passing null stream pointer to fflush", -1);

  auto stream_it = FindHandle(stream);
  ERROR_NO_STREAM(stream_it, -1);
  Handle &stream_h = **stream_it;

  if (HandleType::kWrite != stream_h.type) {
    LogError("Cannot flush on not writing stream");
    return -1;
  }

  auto &out_stream = stream_h.GetWriter().writer_;
  if (!out_stream.flush()) {
    LogBadStatus(out_stream.last_status(), "Error during upload");
    return -1;
  }

  return 0;
}

int driver_remove(const char *filename) {
  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to remove", kFailure);

  spdlog::debug("remove {}", filename);

  assert(driver_isConnected());

  auto maybe_names = GetBucketAndObjectNames(filename);
  ERROR_ON_NAMES(maybe_names, kFailure);
  auto &names = *maybe_names;

  const auto status = client.DeleteObject(names.bucket, names.object);
  if (!status.ok() && status.code() != gc::StatusCode::kNotFound) {
    LogBadStatus(status, "Error deleting object");
    return kFailure;
  }

  return kSuccess;
}

int driver_rmdir(const char *filename) {
  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to rmdir", kFailure);

  spdlog::debug("rmdir {}", filename);

  assert(driver_isConnected());
  spdlog::debug("Remove dir (does nothing...)");
  return kSuccess;
}

int driver_mkdir(const char *filename) {
  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to mkdir", kFailure);

  spdlog::debug("mkdir {}", filename);

  assert(driver_isConnected());
  return kSuccess;
}

long long int driver_diskFreeSpace(const char *filename) {
  ERROR_ON_NULL_ARG(filename, "Error passing null pointer to diskFreeSpace",
                    kFailure);

  spdlog::debug("diskFreeSpace {}", filename);

  assert(driver_isConnected());
  constexpr long long free_space{5LL * 1024LL * 1024LL * 1024LL * 1024LL};
  return free_space;
}

int driver_copyToLocal(const char *sSourceFilePathName,
                       const char *sDestFilePathName) {
  assert(driver_isConnected());

  if (!sSourceFilePathName || !sDestFilePathName) {
    LogError("Error passing null pointer to driver_copyToLocal");
    return kFailure;
  }

  spdlog::debug("copyToLocal {} {}", sSourceFilePathName, sDestFilePathName);

  auto maybe_names = GetBucketAndObjectNames(sSourceFilePathName);
  ERROR_ON_NAMES(maybe_names, kFailure);

  const std::string &bucket_name = maybe_names->bucket;
  const std::string &object_name = maybe_names->object;

  auto maybe_reader = MakeReaderPtr(bucket_name, object_name);
  RETURN_ON_ERROR(maybe_reader, "Error while opening Remote file", kFailure);

  ReaderPtr &reader = *maybe_reader;
  const size_t nb_files = reader->filenames_.size();

  // Open the local file
  std::ofstream file_stream(sDestFilePathName, std::ios::binary);
  if (!file_stream.is_open()) {
    std::ostringstream os;
    os << "Failed to open local file for writing: " << sDestFilePathName;
    LogError(os.str());
    return kFailure;
  }

  // Allocate a relay buffer
  constexpr size_t buf_size{1024 * 1024};
  std::vector<char> buffer(buf_size);
  char *buf_data = buffer.data();

  // create a waste buffer now, so the lambdas can reference it
  // memory allocation will occur later, before actual use
  std::vector<char> waste;

  auto read_and_write = [&](gcs::ObjectReadStream &from,
                            bool skip_header = false,
                            std::streamsize header_size = 0) {
    if (!from) {
      LogBadStatus(from.status(), "Error initializing download stream");
      return false;
    }

    if (skip_header) {
      // according to gcs sources, seekg is not implemented
      // waste a read on the first bytes
      if (!from.read(waste.data(), header_size)) {
        // check failure reasons to give feedback
        std::string err_msg;
        if (from.eof()) {
          err_msg = "Error reading header. Shorter header than expected";
        } else if (from.bad()) {
          err_msg = "Error reading header. Read failed";
        }
        LogBadStatus(from.status(), err_msg);
        return false;
      }
    }

    const std::streamsize buf_size_cast =
        static_cast<std::streamsize>(buf_size);
    while (from.read(buf_data, buf_size_cast) &&
           file_stream.write(buf_data, buf_size_cast)) {
    }
    // what made the process stop?
    if (!file_stream) {
      // something went wrong on write side, abort
      LogError("Error while writing data to local file");
      return false;
    } else if (from.eof()) {
      // short read, copy what remains, if any
      const std::streamsize rem = from.gcount();
      if (rem > 0 && !file_stream.write(buf_data, rem)) {
        // something went wrong on write side, abort
        LogError("Error while writing data to local file");
        return false;
      }
    } else if (from.bad()) {
      // something went wrong on read side
      LogBadStatus(from.status(), "Error while reading from cloud storage");
      return false;
    }

    return true;
  };

  auto operation = [&](gcs::ObjectReadStream &from, const std::string &filename,
                       bool skip_header = false, tOffset header_size = 0) {
    from = client.ReadObject(bucket_name, filename);
    bool res = read_and_write(from, skip_header, header_size);
    from.Close();
    return res;
  };

  auto &filenames = reader->filenames_;

  // Read the whole first file
  gcs::ObjectReadStream read_stream;
  if (!operation(read_stream, filenames.front())) {
    return kFailure;
  }

  // fast exit
  if (nb_files == 1) {
    return kSuccess;
  }

  // Read from the next files
  const tOffset header_size = reader->commonHeaderLength_;
  const bool skip_header = header_size > 0;
  waste.reserve(static_cast<size_t>(header_size));

  for (size_t i = 1; i < nb_files; i++) {
    if (!operation(read_stream, filenames[i], skip_header, header_size)) {
      return kFailure;
    }
  }

  // done copying
  spdlog::debug("Done copying");

  return kSuccess;
}

int driver_copyFromLocal(const char *sSourceFilePathName,
                         const char *sDestFilePathName) {
  if (!sSourceFilePathName || !sDestFilePathName) {
    LogError("Error passing null pointers as arguments to copyFromLocal");
    return kFailure;
  }

  spdlog::debug("copyFromLocal {} {}", sSourceFilePathName, sDestFilePathName);

  assert(driver_isConnected());

  auto maybe_names = GetBucketAndObjectNames(sDestFilePathName);
  ERROR_ON_NAMES(maybe_names, kFailure);

  // Open the local file
  std::ifstream file_stream(sSourceFilePathName, std::ios::binary);
  if (!file_stream.is_open()) {
    std::ostringstream os;
    os << "Failed to open local file: " << sSourceFilePathName;
    LogError(os.str());
    return kFailure;
  }

  // Create a WriteObject stream
  const auto &names = *maybe_names;
  auto writer = client.WriteObject(names.bucket, names.object);
  if (!writer || !writer.IsOpen()) {
    LogBadStatus(writer.metadata().status(),
                 "Error initializing upload stream to remote storage");
    return kFailure;
  }

  // Read from the local file and write to the GCS object
  constexpr size_t buf_size{1024};
  std::array<char, buf_size> buffer{};
  char *buf_data = buffer.data();

  while (file_stream.read(buf_data, buf_size) &&
         writer.write(buf_data, buf_size)) {
  }
  // what made the process stop?
  if (!writer) {
    LogBadStatus(writer.last_status(), "Error while copying to remote storage");
    return kFailure;
  } else if (file_stream.eof()) {
    // copy what remains in the buffer
    const auto rem = file_stream.gcount();
    if (rem > 0 && !writer.write(buf_data, rem)) {
      LogError("Error while copying to remote storage");
      return kFailure;
    }
  } else if (file_stream.bad()) {
    LogError("Error while reading on local storage");
    return kFailure;
  }

  // Close the GCS WriteObject stream to complete the upload
  writer.Close();

  auto &maybe_meta = writer.metadata();
  RETURN_ON_ERROR(maybe_meta, "Error during file upload to remote storage",
                  kFailure);

  return kSuccess;
}
