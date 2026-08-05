#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#define GCS_PLUGIN_EXPORT
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

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <curl/curl.h>

#include "oauth2_token_manager.h"
#include "khiops_driver_common/logging.hpp"
#include "khiops_driver_common/util.hpp"
#include "utils.h"

using namespace gcsplugin;

namespace gc = ::google::cloud;
namespace gcs = gc::storage;

using namespace khiops_driver_common;

namespace khiops_driver_common { spdlog::logger *GetLogger() { return GetLogger("gcsdriver", "GCS_DRIVER_LOGFILE", "GCS_DRIVER_LOGLEVEL"); } }

constexpr const char *version = DRIVER_VERSION;
constexpr const char *driver_name = "GCS driver";
constexpr const char *driver_scheme = "gs";
// Buffer size might need some tuning
// ref https://github.com/googleapis/google-cloud-cpp/issues/2657
// Default value below can be overriden by setting GCS_PREFERRED_BUFFER_SIZE
constexpr long long preferred_buffer_size = 4 * 1024 * 1024;
constexpr int failure_timeout = 30; // 30s

bool bIsConnected = false;

gcs::Client client;
// Global bucket name
std::string globalBucketName;

HandleContainer active_handles;

// Error strings
static const char *ERR_NOT_CONNECTED = "Driver is not connected";
static const char *ERR_NULL_ARG = "Error passing null pointer to {}";
static const char *ERR_URL_PARSING = "Error parsing URL";

void InitHandle(Handle &h, ReaderPtr &&r_ptr) {
  h.var.reader = std::move(r_ptr);
}

void InitHandle(Handle &h, WriterPtr &&w_ptr) {
  h.var.writer = std::move(w_ptr);
}

HandlePtr MakeHandleFromReaderPtr(ReaderPtr &&reader_ptr) {
  HandlePtr h{new Handle(HandleType::kRead)};
  InitHandle(*h, std::move(reader_ptr));
  return h;
}
HandlePtr MakeHandleFromWriterPtr(WriterPtr &&writer_ptr) {
  HandlePtr h{new Handle(HandleType::kWrite)};
  InitHandle(*h, std::move(writer_ptr));
  return h;
}
HandlePtr MakeHandleFromAppenderPtr(WriterPtr &&writer_ptr) {
  HandlePtr h{new Handle(HandleType::kAppend)};
  InitHandle(*h, std::move(writer_ptr));
  return h;
}

Handle *InsertReaderHandle(ReaderPtr &&reader_ptr) {
  return active_handles
      .insert(active_handles.end(),
              MakeHandleFromReaderPtr(std::move(reader_ptr)))
      ->get();
}
Handle *InsertWriterHandle(WriterPtr &&writer_ptr) {
  return active_handles
      .insert(active_handles.end(),
              MakeHandleFromWriterPtr(std::move(writer_ptr)))
      ->get();
}
Handle *InsertAppenderHandle(WriterPtr &&writer_ptr) {
  return active_handles
      .insert(active_handles.end(),
              MakeHandleFromAppenderPtr(std::move(writer_ptr)))
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

int GetGeneration(int64_t *generation, const std::string &bucket_name,
                  const std::string &filename) {
  auto metadata = client.GetObjectMetadata(bucket_name, filename);
  if (!metadata) {
    GetLogger()->error(
        "Failed to get generation of object (bucket: {}, filename: {}).",
        bucket_name, filename);
    return -1;
  }
  *generation = metadata->generation();
  return 0;
}

// Definition of helper functions
int DownloadFileRangeToBuffer(long long *sizeresult,
                              const std::string &bucket_name,
                              const std::string &object_name, char *buffer,
                              std::int64_t start_range, std::int64_t end_range,
                              int64_t generation) {
  auto reader = client.ReadObject(bucket_name, object_name,
                                  gcs::ReadRange(start_range, end_range),
                                  gcs::IfGenerationMatch(generation));

  if (start_range >= end_range) {
    GetLogger()->error("Cannot read after end of file.");
    return -1;
  }
  
  if (!reader) {
    auto &o_status = reader.status();
    if (o_status.code() == gc::StatusCode::kFailedPrecondition) {
      GetLogger()->error("The file has been updated while reading it.");
      return -1;
    }
    GetLogger()->error("Error while creating reading stream; {}",
                       o_status.message());
    return -1;
  }

  reader.read(buffer, end_range - start_range);
  if (reader.bad()) {
    auto &o_status = reader.status();
    GetLogger()->error("Error while creating reading stream; {}",
                       o_status.message());
    return -1;
  }

  long long int num_read = static_cast<long long>(reader.gcount());
  GetLogger()->debug("read = {}", num_read);

  *sizeresult = num_read;
  return 0;
}

struct OffsetChunkLookup {
  size_t initial_chunk_index;
  bool offset_at_or_past_last_chunk;
  tOffset range_end_for_log;
};

OffsetChunkLookup LookupInitialChunk(const std::vector<tOffset> &cumul_sizes,
                                     tOffset offset) {
  auto first_chunk_end_after_offset =
      std::upper_bound(cumul_sizes.begin(), cumul_sizes.end(), offset);

  const bool offset_at_or_past_last_chunk =
      (first_chunk_end_after_offset == cumul_sizes.end());

  size_t idx = static_cast<size_t>(
      std::distance(cumul_sizes.begin(), first_chunk_end_after_offset));

  // If offset is at/after the tracked end, route through the last file so
  // generation checks still run before signaling EOF/out-of-range.
  if (idx == cumul_sizes.size()) {
    idx = cumul_sizes.size() - 1;
  }

  const tOffset range_end_for_log = offset_at_or_past_last_chunk
                                        ? cumul_sizes.back()
                                        : *first_chunk_end_after_offset;

  return {idx, offset_at_or_past_last_chunk, range_end_for_log};
}

int ReadBytesInFile(long long *nread, MultiPartFile &multifile, char *buffer,
                    tOffset to_read) {
  // Start at first usable file chunk
  // Advance through file chunks, advancing buffer pointer
  // Until last requested byte was read
  // Or error occured

  tOffset bytes_read{0LL};

  // Lookup item containing initial bytes at requested offset
  const auto &cumul_sizes = multifile.cumulativeSize_;
  const tOffset common_header_length = multifile.commonHeaderLength_;
  const std::string &bucket_name = multifile.bucketname_;
  const auto &filenames = multifile.filenames_;
  const auto &generations = multifile.generations;
  tOffset &offset = multifile.offset_;

  if (filenames.empty() || cumul_sizes.empty()) {
    GetLogger()->error("Cannot read from an empty multipart file.");
    return -1;
  }

  char *buffer_pos = buffer;
  const tOffset offset_bak = offset; // in case of irrecoverable error, leave
                                     // the multifile in its starting state

  const OffsetChunkLookup chunk_lookup =
      LookupInitialChunk(cumul_sizes, offset);
  size_t idx = chunk_lookup.initial_chunk_index;

  if (idx >= cumul_sizes.size() || idx >= filenames.size() ||
      idx >= generations.size()) {
    GetLogger()->error("Cannot read after end of file.");
    return -1;
  }

  // Skip empty chunks (equal cumulative boundaries). They can legitimately
  // exist in multipart datasets and must not trigger invalid 0-length ranges.
  if (!chunk_lookup.offset_at_or_past_last_chunk) {
    while (idx < cumul_sizes.size()) {
      const tOffset prev_cumul = (idx == 0) ? 0 : cumul_sizes[idx - 1];
      if (cumul_sizes[idx] > prev_cumul && cumul_sizes[idx] > offset) {
        break;
      }
      ++idx;
    }
  }

  if (idx >= cumul_sizes.size() || idx >= filenames.size() ||
      idx >= generations.size()) {
    return bytes_read;
  }

  GetLogger()->debug("Use item {} to read @ {} (end = {})", idx, offset,
                     chunk_lookup.range_end_for_log);

  auto read_range_and_update = [&](const std::string &filename,
                                   int64_t generation, tOffset start,
                                   tOffset end) -> int {
    tOffset actual_read;
    if (DownloadFileRangeToBuffer(&actual_read, bucket_name, filename,
                                  buffer_pos, static_cast<int64_t>(start),
                                  static_cast<int64_t>(end), generation)) {
      offset = offset_bak;
      return -1;
    }

    bytes_read += actual_read;
    buffer_pos += actual_read;
    offset += actual_read;

    if (actual_read < (end - start) /*expected read*/) {
      GetLogger()->debug("End of file encountered");
      to_read = 0;
    } else {
      to_read -= actual_read;
    }

    return 0;
  };

  // first file read

  const tOffset file_start =
      (idx == 0) ? offset
                 : offset - cumul_sizes[idx - 1] + common_header_length;
  const tOffset read_end =
      std::min(file_start + to_read, file_start + cumul_sizes[idx] - offset);

  int read_code = read_range_and_update(filenames[idx], generations[idx],
                                        file_start, read_end);

  // continue with the next files
  while (!read_code && to_read && (idx + 1) < cumul_sizes.size() &&
         (idx + 1) < filenames.size() && (idx + 1) < generations.size()) {
    // read the missing bytes in the next files as necessary
    idx++;

    const tOffset chunk_capacity = cumul_sizes[idx] - cumul_sizes[idx - 1];
    if (chunk_capacity <= 0) {
      continue;
    }

    const tOffset start = common_header_length;
    const tOffset end = std::min(start + to_read, start + chunk_capacity);

    read_code =
        read_range_and_update(filenames[idx], generations[idx], start, end);
  }

  if (read_code) {
    return -1;
  }
  *nread = bytes_read;
  return 0;
}

struct ParseUriResult {
  std::string bucket;
  std::string object;
};

int ParseGcsUri(ParseUriResult *result, const std::string &gcs_uri) {
  char const *prefix = "gs://";
  const size_t prefix_size{std::strlen(prefix)};
  if (gcs_uri.compare(0, prefix_size, prefix) != 0) {

    GetLogger()->error("Invalid GCS URI: {}", gcs_uri);
    return -1;
  }

  const size_t pos = gcs_uri.find('/', prefix_size);
  if (pos == std::string::npos) {
    GetLogger()->error("Invalid GCS URI, missing object name: {}", gcs_uri);
    return -1;
  }

  *result = std::move(ParseUriResult{
      gcs_uri.substr(prefix_size, pos - prefix_size), gcs_uri.substr(pos + 1)});
  return 0;
}

int GetBucketAndObjectNames(ParseUriResult *result, const char *sFilePathName) {
  if (ParseGcsUri(result, sFilePathName)) {
    return -1;
  }

  // fallback to default bucket if bucket empty
  if (result->bucket.empty()) {
    if (globalBucketName.empty()) {
      GetLogger()->error("No bucket specified and GCS_BUCKET_NAME is not set!");
      return -1;
    } else {
      result->bucket = globalBucketName;
    }
  }
  return 0;
}

bool WillSizeCountProductOverflow(size_t size, size_t count) {
  constexpr size_t max_prod_usable{
      static_cast<size_t>(std::numeric_limits<tOffset>::max())};
  return (max_prod_usable / size < count || max_prod_usable / count < size);
}

int ListObjects(gcs::ListObjectsReader *result, const std::string &bucket_name,
                const std::string &object_name) {
  auto list = client.ListObjects(bucket_name, gcs::MatchGlob{object_name});
  auto first = list.begin();
  if (first == list.end()) {
    return -2;
  }
  if (!first->ok()) {
    if (first->status().code() == gc::StatusCode::kNotFound)
      return -2;
    return -1;
  }
  *result = std::move(list);
  return 0;
}

std::string NormalizeDirectoryObjectPath(std::string object_path) {
  if (object_path.empty() || object_path.back() != '/') {
    object_path.push_back('/');
  }
  return object_path;
}

bool HasDirectoryTrailingSlash(const char *sFilePathName) {
  return sFilePathName != nullptr && std::strlen(sFilePathName) > 0 &&
         sFilePathName[std::strlen(sFilePathName) - 1] == '/';
}

// pre condition: stream is of a writing type. do not call otherwise.
int CloseWriterStream(Handle &stream) {
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
    return 0;
  }

  err_msg_os << ": " << maybe_meta.status().message();
  GetLogger()->error(err_msg_os.str());
  return -1;
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
  std::vector<int64_t> generations(filenames.size(), 1);
  ReaderPtr reader_ptr{
      new MultiPartFile{bucket, object, offset, commonHeaderLength, filenames,
                        cumulativeSize, total_size, std::move(generations)}};
  return InsertReaderHandle(std::move(reader_ptr));
}

void *test_addWriterHandle(bool appendMode, bool create_with_mock_client,
                           std::string bucketname, std::string objectname) {
  if (!create_with_mock_client) {
    if (appendMode) {
      return InsertAppenderHandle(WriterPtr(new Writer));
    }
    return InsertWriterHandle(WriterPtr(new Writer));
  }

  auto writer = client.WriteObject(bucketname, objectname);
  if (!writer) {
    return nullptr;
  }

  WriterPtr writer_struct{new Writer};
  writer_struct->bucketname_ = std::move(bucketname);
  writer_struct->filename_ = std::move(objectname);
  writer_struct->writer_ = std::move(writer);

  if (appendMode) {
    return InsertAppenderHandle(std::move(writer_struct));
  }
  return InsertWriterHandle(std::move(writer_struct));
}

int test_copyObject(const std::string &source_bucket,
                    const std::string &source_object,
                    const std::string &dest_bucket,
                    const std::string &dest_object) {
  auto result =
      client.CopyObject(source_bucket, source_object, dest_bucket, dest_object);
  if (!result) {
    return -1;
  }
  return 0;
}

gcs::ListObjectsReader test_listObjects(const std::string &bucket,
                                        const std::string &glob_pattern) {
  return client.ListObjects(bucket, gcs::MatchGlob(glob_pattern));
}

const char *driver_getDriverName() { return driver_name; }

const char *driver_getVersion() { return version; }

const char *driver_getScheme() { return driver_scheme; }

int driver_isReadOnly() { return kFalse; }

int driver_connect() {
  if (bIsConnected) {
    GetLogger()->error("Driver is already connected");
    return kOtherFailure;
  }

  GetLogger()->debug("Connect driver {} version {}", driver_name, version);

  // Initialize CURL globally
  curl_global_init(CURL_GLOBAL_ALL);

  // Initialize variables from environment
  globalBucketName = GetEnvVarOrDefault("GCS_BUCKET_NAME", "");

#if defined(__linux__)
  // CA bundle path
  std::string certificate_path;
  if (FindCertificate(&certificate_path) != 0) return kOtherFailure;
#endif

  // Base options
  gc::Options options;
  options
      .set<gcs::RetryPolicyOption>(
          gcs::LimitedTimeRetryPolicy(std::chrono::seconds(1)).clone())
      .set<gcs::TransferStallTimeoutOption>(
          std::chrono::seconds(failure_timeout));
#if defined(__linux__)
  options.set<gc::CARootsFilePathOption>(certificate_path);
#endif

  // Optional project
  std::string project = GetEnvVarOrDefault("CLOUD_ML_PROJECT_ID", "");
  if (!project.empty()) {
    options.set<gc::UserProjectOption>(project);
  }

  std::shared_ptr<gc::Credentials> creds;

  // 1) Service account JSON key has priority
  std::string gcp_token_filename = GetEnvVarOrDefault("GCP_TOKEN", "");
  if (!gcp_token_filename.empty()) {
    std::ifstream t(gcp_token_filename);
    std::stringstream buffer;
    buffer << t.rdbuf();
    if (t.fail()) {
      GetLogger()->error("Error reading GCP_TOKEN file: {}", gcp_token_filename);
      return kOtherFailure;
    }

    // Pass options so credentials use the same CA config
    creds = gc::MakeServiceAccountCredentials(buffer.str(), options);
    if (!creds) {
      GetLogger()->error("MakeServiceAccountCredentials failed");
      return kOtherFailure;
    }
  }

  // 2) Optional custom OAuth token manager fallback
  if (!creds) {
    std::string gcp_oauth_token_filename =
        GetEnvVarOrDefault("GCP_OAUTH_TOKEN", "");
    if (!gcp_oauth_token_filename.empty()) {
      try {
#if defined(__linux__)
        OAuth2TokenManager token_manager(gcp_oauth_token_filename, certificate_path);
#else
        OAuth2TokenManager token_manager(gcp_oauth_token_filename);
#endif
        creds = token_manager.MakeCredentials();
      } catch (std::exception const &ex) {
        GetLogger()->error("OAuth2TokenManager init/credentials failed: {}",
                           ex.what());
        return kOtherFailure;
      }
    }
  }

  // 3) Default ADC fallback
  if (!creds) {
    creds = gc::MakeGoogleDefaultCredentials(options);
    if (!creds) {
      GetLogger()->error("MakeGoogleDefaultCredentials failed");
      return kOtherFailure;
    }
  }

  // Inject credentials into client options
  options.set<gc::UnifiedCredentialsOption>(std::move(creds));

  // Create client
  try {
    client = gcs::Client(std::move(options));
  } catch (std::exception const &ex) {
    GetLogger()->error("Failed to create GCS client: {}", ex.what());
    return kOtherFailure;
  }

  bIsConnected = true;
  return kOtherSuccess;
}

int driver_disconnect() {
  // loop on the still active handles to close as necessary and remove. clear()
  // on the container would do it but the procedures would fail silently.
  std::vector<int> failures;
  for (auto &h_ptr : active_handles) {
    // the writing streams need to be closed
    const HandleType type = h_ptr->type;
    if (HandleType::kRead != type) {
      int status;
      if ((status = CloseWriterStream(*h_ptr))) {
        failures.push_back(status);
      }
    }
  }
  active_handles.clear();

  // Clean up CURL
  curl_global_cleanup();

  bIsConnected = false;

  if (failures.empty()) {
    return kOtherSuccess;
  }

  std::ostringstream os;
  os << "Errors occured during disconnection:\n";
  for (const auto &status : failures) {
    os << status << '\n';
  }
  GetLogger()->error(os.str());
  return kOtherFailure;
}

int driver_isConnected() { return bIsConnected ? 1 : 0; }

long long int driver_getSystemPreferredBufferSize() {
  std::string configured_preferred_size = GetEnvVarOrDefault(
      "GCS_PREFERRED_BUFFER_SIZE", std::to_string(preferred_buffer_size));
  return std::stoi(configured_preferred_size);
}

int driver_fileExists(const char *sFilePathName) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kFalse;
  }

  if (!(sFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFalse);
  };

  GetLogger()->debug("fileExist {}", sFilePathName);

  ParseUriResult parsedUri;
  if (GetBucketAndObjectNames(&parsedUri, sFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kFalse;
  }

  google::cloud::storage::v2_37::ListObjectsReader objects;
  int code;
  if ((code = ListObjects(&objects, parsedUri.bucket, parsedUri.object))) {
    if (code == -2)
      return kFalse;
    GetLogger()->error("Error checking if file exists");
    return kFalse;
  }

  GetLogger()->debug("file {} exists!", sFilePathName);
  return kTrue; // L'objet existe
}

int driver_dirExists(const char *sFilePathName) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kFalse;
  }

  if (!(sFilePathName)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kFalse);
  };
  if (!HasDirectoryTrailingSlash(sFilePathName)) {
    GetLogger()->error("Directory URL must end with '/'");
    return kFalse;
  }

  GetLogger()->debug("dirExist {}", sFilePathName);

  ParseUriResult parsedUri;
  if (GetBucketAndObjectNames(&parsedUri, sFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kFalse;
  }

  const std::string dir_prefix = NormalizeDirectoryObjectPath(parsedUri.object);
  auto maybe_dir = client.GetObjectMetadata(parsedUri.bucket, dir_prefix);
  if (maybe_dir) {
    return kTrue;
  }
  if (maybe_dir.status().code() != gc::StatusCode::kNotFound) {
    GetLogger()->debug("Slash directory object lookup failed: {}",
                       maybe_dir.status().message());
  }

  auto objects =
      client.ListObjects(parsedUri.bucket, gcs::Prefix(dir_prefix),
                         gcs::MaxResults(1));

  auto it = objects.begin();
  if (it == objects.end()) {
    return kFalse;
  }

  if (!(*it)) {
    if (it->status().code() == gc::StatusCode::kNotFound) {
      return kFalse;
    }
    GetLogger()->error("Error checking if directory exists");
    return kFalse;
  }

  return kTrue;
}

// Khiops allows header length to be max 8MB
constexpr int KHIOPS_MAX_HEADERLENGTH = 8 * 1024 * 1024;

int ReadHeader(std::string *headerResult, const std::string &bucket_name,
               const std::string &filename,
               int64_t max_length = KHIOPS_MAX_HEADERLENGTH) {
  GetLogger()->debug("ReadHeader {} max_length {}", filename, max_length);
  gcs::ObjectReadStream stream =
      client.ReadObject(bucket_name, filename, gcs::ReadRange(0, max_length));
  std::string line;
  std::getline(stream, line, '\n');
  if (stream.bad()) {
    GetLogger()->error("Failed to read header; stream reported bad status.");
    return -1;
  }
  if (!stream.eof()) {
    line.push_back('\n');
  }
  if (line.empty()) {
    GetLogger()->error("Got an empty header");
    return -1;
  }
  *headerResult = line;
  return 0;
}

// Speed up common header detection by comparing the header only in the first
// and last few files plus some files randomly chosen from the middle of the
// complete list.
std::set<std::string>
SelectObjectsSubset(std::vector<std::string> const &all_objects) {
  size_t total = all_objects.size();
  if (total == 0)
    return {};

  size_t first_count = std::min<size_t>(5, total);
  size_t last_count = total < 10 ? std::max<size_t>(0, total - first_count) : 5;

  size_t used = first_count + last_count;
  size_t random_count = 10;
  if (total < 20) {
    if (total > used) {
      random_count = total - used;
    } else {
      random_count = 0;
    }
  }

  std::set<std::string> result;

  // Add first elements
  for (size_t i = 0; i < first_count; ++i) {
    result.insert(all_objects[i]);
  }

  // Add last elements
  if (last_count > 0) {
    for (size_t i = total - last_count; i < total; ++i) {
      result.insert(all_objects[i]);
    }
  }

  // Prepare remaining list for random selection
  std::vector<std::string> remaining;
  size_t start_random = first_count;
  size_t end_random = total - last_count;
  if (start_random < end_random) {
    remaining.insert(remaining.end(), all_objects.begin() + start_random,
                     all_objects.begin() + end_random);
  }

  // Random selection without duplicate
  if (random_count > 0 && !remaining.empty()) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(remaining.begin(), remaining.end(), gen);

    size_t to_take = std::min(random_count, remaining.size());
    for (size_t i = 0; i < to_take; ++i) {
      result.insert(remaining[i]);
    }
  }

  GetLogger()->debug("Selected objects for header detection");
  for (auto const &name : result) {
    GetLogger()->debug(" {}", name);
  }

  return result;
}

int GetFileSize(long long *sizeresult, const std::string &bucket_name,
                const std::string &object_name) {
  google::cloud::storage::v2_37::ListObjectsReader objects;
  if (ListObjects(&objects, bucket_name, object_name)) {
    return -1;
  }

  auto list_it = objects.begin();
  const auto list_end = objects.end();

  std::vector<std::string> filenames;
  std::vector<long long> filesizes;
  for (; list_it != list_end; list_it++) {
    if (!(*list_it)) {
      return -1;
    }
    filenames.push_back((*list_it)->name());
    filesizes.push_back(static_cast<long long>((*list_it)->size()));
  }
  // Create set of filenames considered for common header detection
  std::set<std::string> selected = SelectObjectsSubset(filenames);

  long long total_size = filesizes[0];
  if (filenames.size() == 1) {
    // unique file
    *sizeresult = total_size;
    return 0;
  }

  // multifile
  // check headers
  std::string header;
  if (ReadHeader(&header, bucket_name, filenames[0])) {
    return -1;
  }

  const long long header_size = static_cast<long long>(header.size());
  int header_to_subtract{0};
  bool same_header{true};

  for (unsigned long int i = 1; i < filenames.size(); i++) {

    if (same_header) {
      if (selected.find(filenames[i]) != selected.end()) {
        std::string curr_header;
        // Actually verify file contents
        if (ReadHeader(&curr_header, bucket_name, filenames[i], header_size)) {
          return -1;
        }

        same_header = (header == curr_header);
        if (same_header) {
          header_to_subtract++;
        }
      } else {
        // Only check filesize
        GetLogger()->debug("Skip header detect {} {} expect min {}",
                           filenames[i], filesizes[i], header_size);
        same_header = (header_size <= filesizes[i]);
        if (same_header) {
          header_to_subtract++;
        }
      }
    }
    total_size += filesizes[i];
  }

  if (!same_header) {
    header_to_subtract = 0;
  }
  *sizeresult = total_size - header_to_subtract * header_size;
  return 0;
}

long long int driver_getFileSize(const char *filename) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return -1;
  }

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return -1;
  };

  GetLogger()->debug("getFileSize {}", filename);

  ParseUriResult parsedUri;
  if (ParseGcsUri(&parsedUri, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kFailure;
  }

  long long size;
  if (GetFileSize(&size, parsedUri.bucket, parsedUri.object)) {
    GetLogger()->error("Error getting file size");
    return kFailure;
  }

  return size;
}

int MakeReaderPtr(ReaderPtr *result, std::string bucketname,
                  std::string objectname) {
  std::vector<std::string> filenames;
  std::vector<long long> cumulative_sizes;

  google::cloud::storage::v2_37::ListObjectsReader objects;
  if (ListObjects(&objects, bucketname, objectname)) {
    return -1;
  }

  const auto list_end = objects.end();

  std::vector<long long> filesizes;
  auto list_it = objects.begin();
  for (; list_it != list_end; list_it++) {
    if (!(*list_it)) {
      return -1;
    }
    filenames.push_back((*list_it)->name());
    filesizes.push_back(static_cast<long long>((*list_it)->size()));
  }
  // Create set of filenames considered for common header detection
  std::set<std::string> selected = SelectObjectsSubset(filenames);

  cumulative_sizes.push_back(filesizes[0]);
  long long common_header_size{0};

  // Allocate a generation vector big enough to hold the generations of all
  // parts. A single-part file will result in a vector of size 1.
  std::vector<int64_t> generations(filenames.size());
  // Get generation of first part.
  if (GetGeneration(&generations.data()[0], bucketname, filenames[0])) {
    return -1;
  }

  if (filenames.size() > 1) {
    // multifile
    // check headers
    std::string header;
    if (ReadHeader(&header, bucketname, filenames[0])) {
      return -1;
    }

    const long long header_size = static_cast<long long>(header.size());
    bool same_header{true};

    for (long unsigned int i = 1; i < filenames.size(); i++) {
      // Get generation of current part.
      if (GetGeneration(&generations.data()[i], bucketname, filenames[i])) {
        return -1;
      }

      cumulative_sizes.push_back(cumulative_sizes.back() +
                                 static_cast<long long>(filesizes[i]));

      if (same_header) {
        if (selected.find(filenames[i]) != selected.end()) {
          // Actually verify file contents
          std::string curr_header;
          if (ReadHeader(&curr_header, bucketname, filenames[i], header_size)) {
            return -1;
          }
          same_header = (header == curr_header);
        } else {
          // Only check filesize
          GetLogger()->debug("Skip header detect {} {} expect min {}",
                             filenames[i], filesizes[i], header_size);
          same_header = (header_size <= filesizes[i]);
        }
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
  *result = ReaderPtr(new MultiPartFile{
      std::move(bucketname), std::move(objectname), 0, common_header_size,
      std::move(filenames), std::move(cumulative_sizes), total_size,
      std::move(generations)});
  return 0;
}

int MakeWriterPtr(WriterPtr *result, std::string bucketname,
                  std::string objectname) {
  auto writer = client.WriteObject(bucketname, objectname);
  if (!writer) {
    GetLogger()->error("Failed to get object writer.");
    return -1;
  }
  WriterPtr writer_struct{new Writer};
  writer_struct->bucketname_ = std::move(bucketname);
  writer_struct->filename_ = std::move(objectname);
  writer_struct->writer_ = std::move(writer);
  *result = std::move(writer_struct);
  return 0;
}

int RegisterReaderStream(Handle **result, std::string &&bucket,
                         std::string &&object) {
  ReaderPtr readerPtr;
  if (MakeReaderPtr(&readerPtr, std::move(bucket), std::move(object))) {
    return -1;
  }

  *result = InsertReaderHandle(std::move(readerPtr));
  return 0;
}
int RegisterWriterStream(Handle **result, std::string &&bucket,
                         std::string &&object) {
  WriterPtr writerPtr;
  if (MakeWriterPtr(&writerPtr, std::move(bucket), std::move(object))) {
    return -1;
  }

  *result = InsertWriterHandle(std::move(writerPtr));
  return 0;
}
int RegisterAppenderStream(Handle **result, std::string &&bucket,
                           std::string &&object) {
  WriterPtr writerPtr;
  if (MakeWriterPtr(&writerPtr, std::move(bucket), std::move(object))) {
    return -1;
  }

  *result = InsertAppenderHandle(std::move(writerPtr));
  return 0;
}

int RegisterReader(Handle **result, std::string &&bucket,
                   std::string &&object) {
  return RegisterReaderStream(result, std::move(bucket), std::move(object));
}

int RegisterWriter(Handle **result, std::string &&bucket,
                   std::string &&object) {
  return RegisterWriterStream(result, std::move(bucket), std::move(object));
}

int RegisterWriterForAppend(Handle **result, std::string &&bucket,
                            std::string &&tmp, std::string append_target) {
  if (RegisterAppenderStream(result, std::move(bucket), std::move(tmp))) {
    return -1;
  }
  (*result)->GetWriter().append_target_ = std::move(append_target);
  return 0;
}

void *driver_fopen(const char *filename, char mode) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return nullptr;
  }

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (nullptr);
  };

  GetLogger()->debug("fopen {} {}", filename, mode);

  ParseUriResult names;
  if (GetBucketAndObjectNames(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return nullptr;
  }

  Handle *handle;
  int registration_code;
  std::string err_msg;

  switch (mode) {
  case 'r': {
    registration_code = RegisterReader(&handle, std::move(names.bucket),
                                       std::move(names.object));
    err_msg = "Error while opening reader stream";
    break;
  }
  case 'w': {
    registration_code = RegisterWriter(&handle, std::move(names.bucket),
                                       std::move(names.object));
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

    google::cloud::storage::v2_37::ListObjectsReader objects;
    int c;
    if ((c = ListObjects(&objects, names.bucket, names.object))) {
      if (c == -2) {
        // file doesn't exist, fallback to write mode
        registration_code = RegisterWriter(&handle, std::move(names.bucket),
                                           std::move(names.object));
      } else {
        // genuine error
        registration_code = -1;
      }
      err_msg = "Error while opening writer stream";
      break;
    }

    // go to end of list to get the target file name
    auto list_it = objects.begin();
    const auto list_end = objects.end();
    auto to_last_item = list_it;
    list_it++;
    while (list_end != list_it) {
      to_last_item = list_it;
      list_it++;
    }
    if (!to_last_item->ok()) {
      // data is unusable
      registration_code = -1;
      err_msg = "Error opening file in append mode";
      break;
    }

    // get a writer handle
    registration_code = RegisterWriterForAppend(
        &handle, std::move(names.bucket),
        std::string("tmp_object_to_append_") +
            boost::uuids::to_string(boost::uuids::random_generator()()),
        to_last_item->value().name());
    err_msg = "Error opening file in append mode, cannot open tmp object";
    break;
  }
  default:
    GetLogger()->error(std::string("Invalid open mode: ") + mode);
    return nullptr;
  }

  if (registration_code) {
    GetLogger()->error(err_msg);
    return nullptr;
  }

  return handle;
}

int driver_fclose(void *stream) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kFailure;
  }

  if (!stream) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return kFailure;
  };

  GetLogger()->debug("fclose {}", (void *)stream);

  auto stream_it = FindHandle(stream);
  if (stream_it == active_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return kFailure;
  };
  auto &h_ptr = *stream_it;

  int code = 0;

  if (HandleType::kRead != h_ptr->type) {
    code = CloseWriterStream(*h_ptr);
  }

  EraseRemove(stream_it);

  if (code) {
    GetLogger()->error("Error while closing writer stream");
    return kFailure;
  }

  return kSuccess;
}

int driver_fseek(void *stream, long long int offset, int whence) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return -1;
  }

  constexpr long long max_val = std::numeric_limits<long long>::max();

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (-1);
  };

  // confirm stream's presence
  auto to_stream = FindHandle(stream);
  if ((to_stream) == active_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return (-1);
  };

  auto &stream_h = *to_stream;

  if (HandleType::kRead != stream_h->type) {
    GetLogger()->error("Cannot seek on not reading stream");
    return kFailure;
  }

  GetLogger()->debug("fseek {} {} {}", stream, offset, whence);

  Reader &h = stream_h->GetReader();

  tOffset computed_offset{0};

  switch (whence) {
  case std::ios::beg:
    computed_offset = offset;
    break;
  case std::ios::cur:
    if (offset > max_val - h.offset_) {
      GetLogger()->error("Signed overflow prevented");
      return kFailure;
    }
    computed_offset = h.offset_ + offset;
    break;
  case std::ios::end:
    if (h.total_size_ > 0) {
      long long minus1 = h.total_size_ - 1;
      if (offset > max_val - minus1) {
        GetLogger()->error("Signed overflow prevented");
        return kFailure;
      }
    }
    if ((offset == std::numeric_limits<long long>::min()) &&
        (h.total_size_ == 0)) {
      GetLogger()->error("Signed overflow prevented");
      return kFailure;
    }

    computed_offset = (h.total_size_ == 0) ? offset : h.total_size_ + offset;
    break;
  default:
    GetLogger()->error("Invalid seek mode " + std::to_string(whence));
    return kFailure;
  }

  if (computed_offset < 0) {
    GetLogger()->error("Invalid seek offset " +
                       std::to_string(computed_offset));
    return kFailure;
  }
  h.offset_ = computed_offset;
  return kSuccess;
}

const char *driver_getlasterror() {
  GetLogger()->debug("getlasterror");
  static std::string last_error;
  last_error = khiops_driver_common::GetLastError();
  return last_error.empty() ? nullptr : last_error.c_str();
}

long long int driver_fread(void *ptr, size_t size, size_t count, void *stream) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return -1;
  }

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (-1);
  };
  if (!(ptr)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (-1);
  };

  if (0 == size) {
    GetLogger()->error("Error passing size of 0");
    return kFailure;
  }

  // confirm stream's presence
  auto to_stream = FindHandle(stream);
  if (to_stream == active_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return kFailure;
  }

  auto &stream_h = *to_stream;

  if (HandleType::kRead != stream_h->type) {
    GetLogger()->error("Cannot read on not reading stream");
    return kFailure;
  }

  GetLogger()->debug("fread {} {} {} {}", ptr, size, count, stream);

  Reader &h = stream_h->GetReader();

  const tOffset offset = h.offset_;

  // fast exit for 0 read
  if (0 == count) {
    return 0LL;
  }

  // prevent overflow
  if (WillSizeCountProductOverflow(size, count)) {
    GetLogger()->error("product size * count is too large, would overflow");
    return kFailure;
  }

  tOffset to_read{static_cast<tOffset>(size * count)};
  if (offset > std::numeric_limits<long long>::max() - to_read) {
    GetLogger()->error("signed overflow prevented on reading attempt");
    return kFailure;
  }
  // end of overflow prevention

  // normal cases
  GetLogger()->debug("offset = {} to_read = {}", offset, to_read);

  long long nread;
  if (ReadBytesInFile(&nread, h, reinterpret_cast<char *>(ptr), to_read)) {
    GetLogger()->error("Error while reading from file");
    return -1;
  }

  return static_cast<long long>(nread / static_cast<tOffset>(size));
}

long long int driver_fwrite(const void *ptr, size_t size, size_t count,
                            void *stream) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return -1;
  }

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (-1);
  };
  if (!(ptr)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (-1);
  };

  if (0 == size) {
    GetLogger()->error("Error passing size 0 to fwrite");
    return kFailure;
  }

  GetLogger()->debug("fwrite {} {} {} {}", ptr, size, count, stream);

  auto stream_it = FindHandle(stream);
  if ((stream_it) == active_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return (-1);
  };
  Handle &stream_h = **stream_it;

  const HandleType type = stream_h.type;

  if (HandleType::kRead == type) {
    GetLogger()->error("Cannot write on not writing stream");
    return kFailure;
  }

  // fast exit for 0
  if (0 == count) {
    return 0LL;
  }

  // prevent integer overflow
  if (WillSizeCountProductOverflow(size, count)) {
    GetLogger()->error(
        "Error on write: product size * count is too large, would overflow");
    return kFailure;
  }

  const long long to_write = static_cast<long long>(size * count);

  gcs::ObjectWriteStream &writer = stream_h.GetWriter().writer_;
  writer.write(static_cast<const char *>(ptr), to_write);
  if (writer.bad()) {
    GetLogger()->error("Error during upload");
    return kFailure;
  }
  GetLogger()->debug("Write status after write: good {}, bad {}, fail {}",
                     writer.good(), writer.bad(), writer.fail());

  return static_cast<long long>(count);
}

int driver_fflush(void *stream) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return -1;
  }

  if (!(stream)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (-1);
  };

  auto stream_it = FindHandle(stream);
  if ((stream_it) == active_handles.end()) {
    GetLogger()->error("Cannot identify stream");
    return (-1);
  };
  Handle &stream_h = **stream_it;

  if (HandleType::kWrite != stream_h.type &&
      HandleType::kAppend != stream_h.type) {
    GetLogger()->error("Cannot flush on not writing stream");
    return kFailure;
  }

  auto &out_stream = stream_h.GetWriter().writer_;
  if (!out_stream.flush()) {
    GetLogger()->error("Error during upload");
    return kFailure;
  }

  return kSuccess;
}

int driver_remove(const char *filename) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };

  GetLogger()->debug("remove {}", filename);

  const std::string file_to_remove(filename);
  if (file_to_remove.find('*') != std::string::npos) {
    auto maybe_pattern = ParseGlobbingPattern(file_to_remove);
    if (!maybe_pattern) {
      GetLogger()->error("Invalid globbing pattern");
      return kOtherFailure;
    }
  }
  ParseUriResult names;
  if (ParseGcsUri(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  google::cloud::storage::v2_37::ListObjectsReader objects;
  int c;
  if ((c = ListObjects(&objects, names.bucket, names.object))) {
    if (c == -2) {
      return kOtherSuccess; // aucun objet correspondant : rien à faire
    }
    GetLogger()->error("Error listing objects to delete");
    return kOtherFailure;
  }

  bool failure_detected = false;
  for (auto it = objects.begin(); it != objects.end(); ++it) {
    if (!*it) {
      GetLogger()->error("Error iterating objects to delete");
      failure_detected = true;
      continue;
    }

    const std::string &object_name = (*it)->name();
    const auto status = client.DeleteObject(names.bucket, object_name);
    if (!status.ok() && status.code() != gc::StatusCode::kNotFound) {
      GetLogger()->error("Error deleting object '{}'", object_name);
      failure_detected = true;
    }
  }

  return failure_detected ? kOtherFailure : kOtherSuccess;
}

int driver_rmdir(const char *filename) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  if (!HasDirectoryTrailingSlash(filename)) {
    GetLogger()->error("Directory URL must end with '/'");
    return kOtherFailure;
  }

  GetLogger()->debug("rmdir {}", filename);

  ParseUriResult names;
  if (GetBucketAndObjectNames(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  const std::string dir_prefix = NormalizeDirectoryObjectPath(names.object);
  auto objects = client.ListObjects(names.bucket, gcs::Prefix(dir_prefix));

  bool failure_detected = false;

  for (auto it = objects.begin(); it != objects.end(); ++it) {
    if (!(*it)) {
      if (it->status().code() == gc::StatusCode::kNotFound) {
        break;
      }
      GetLogger()->error("Error iterating objects to delete in directory");
      failure_detected = true;
      continue;
    }

    const std::string &object_name = (*it)->name();
    const auto status = client.DeleteObject(names.bucket, object_name);
    if (!status.ok() && status.code() != gc::StatusCode::kNotFound) {
      GetLogger()->error("Error deleting object '{}' while removing directory",
                         object_name);
      failure_detected = true;
    }
  }

  return failure_detected ? kOtherFailure : kOtherSuccess;
}

int driver_mkdir(const char *filename) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };
  if (!HasDirectoryTrailingSlash(filename)) {
    GetLogger()->error("Directory URL must end with '/'");
    return kOtherFailure;
  }

  GetLogger()->debug("mkdir {}", filename);

  ParseUriResult names;
  if (GetBucketAndObjectNames(&names, filename)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  const std::string dir_object_name = NormalizeDirectoryObjectPath(names.object);

  auto writer = client.WriteObject(names.bucket, dir_object_name);
  if (!writer || !writer.IsOpen()) {
    GetLogger()->error("Failed to create slash directory object");
    return kOtherFailure;
  }

  writer.Close();
  if (!writer.metadata()) {
    GetLogger()->error("Failed to finalize slash directory object");
    return kOtherFailure;
  }

  return kOtherSuccess;
}

long long int driver_diskFreeSpace(const char *filename) {
  if (!(filename)) {
    GetLogger()->error(ERR_NULL_ARG, __func__);
    return (kOtherFailure);
  };

  GetLogger()->debug("diskFreeSpace {}", filename);

  constexpr long long free_space{5LL * 1024LL * 1024LL * 1024LL * 1024LL};
  return free_space;
}

int driver_copyToLocal(const char *sSourceFilePathName,
                       const char *sDestFilePathName) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!sSourceFilePathName || !sDestFilePathName) {
    GetLogger()->error("Error passing null pointer to driver_copyToLocal");
    return kOtherFailure;
  }

  GetLogger()->debug("copyToLocal {} {}", sSourceFilePathName,
                     sDestFilePathName);

  ParseUriResult parsedUri;
  if (GetBucketAndObjectNames(&parsedUri, sSourceFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  const std::string &bucket_name = parsedUri.bucket;
  const std::string &object_name = parsedUri.object;

  ReaderPtr reader;
  if (MakeReaderPtr(&reader, bucket_name, object_name)) {
    GetLogger()->error("Error while opening Remote file");
    return kOtherFailure;
  }

  const size_t nb_files = reader->filenames_.size();

  // Open the local file
  std::ofstream file_stream(sDestFilePathName, std::ios::binary);
  if (!file_stream.is_open()) {
    std::ostringstream os;
    os << "Failed to open local file for writing: " << sDestFilePathName;
    GetLogger()->error(os.str());
    return kOtherFailure;
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
      GetLogger()->error("Error initializing download stream");
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
        GetLogger()->error(err_msg);
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
      GetLogger()->error("Error while writing data to local file");
      return false;
    } else if (from.eof()) {
      // short read, copy what remains, if any
      const std::streamsize rem = from.gcount();
      if (rem > 0 && !file_stream.write(buf_data, rem)) {
        // something went wrong on write side, abort
        GetLogger()->error("Error while writing data to local file");
        return false;
      }
    } else if (from.bad()) {
      // something went wrong on read side
      GetLogger()->error("Error while reading from cloud storage");
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
    return kOtherFailure;
  }

  // fast exit
  if (nb_files == 1) {
    return kOtherSuccess;
  }

  // Read from the next files
  const tOffset header_size = reader->commonHeaderLength_;
  const bool skip_header = header_size > 0;
  waste.reserve(static_cast<size_t>(header_size));

  for (size_t i = 1; i < nb_files; i++) {
    if (!operation(read_stream, filenames[i], skip_header, header_size)) {
      return kOtherFailure;
    }
  }

  // done copying
  GetLogger()->debug("Done copying");

  return kOtherSuccess;
}

int driver_copyFromLocal(const char *sSourceFilePathName,
                         const char *sDestFilePathName) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!sSourceFilePathName || !sDestFilePathName) {
    GetLogger()->error(
        "Error passing null pointers as arguments to copyFromLocal");
    return kOtherFailure;
  }

  GetLogger()->debug("copyFromLocal {} {}", sSourceFilePathName,
                     sDestFilePathName);

  ParseUriResult names;
  if (GetBucketAndObjectNames(&names, sDestFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }

  // Open the local file
  std::ifstream file_stream(sSourceFilePathName, std::ios::binary);
  if (!file_stream.is_open()) {
    std::ostringstream os;
    os << "Failed to open local file: " << sSourceFilePathName;
    GetLogger()->error(os.str());
    return kOtherFailure;
  }

  // Create a WriteObject stream
  auto writer = client.WriteObject(names.bucket, names.object);
  if (!writer || !writer.IsOpen()) {
    GetLogger()->error("Error initializing upload stream to remote storage");
    return kOtherFailure;
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
    GetLogger()->error("Error while copying to remote storage");
    return kOtherFailure;
  } else if (file_stream.eof()) {
    // copy what remains in the buffer
    const auto rem = file_stream.gcount();
    if (rem > 0 && !writer.write(buf_data, rem)) {
      GetLogger()->error("Error while copying to remote storage");
      return kOtherFailure;
    }
  } else if (file_stream.bad()) {
    GetLogger()->error("Error while reading on local storage");
    return kOtherFailure;
  }

  // Close the GCS WriteObject stream to complete the upload
  writer.Close();

  auto &maybe_meta = writer.metadata();
  if (!(maybe_meta)) {
    GetLogger()->error("Error during file upload to remote storage");
    return kOtherFailure;
  }

  return kOtherSuccess;
}

int driver_concat(const char *sDestFilePathName,
                  const char **sSourceFilePathNames, size_t nSourceFileCount) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!sDestFilePathName || !sSourceFilePathNames) {
    GetLogger()->error(
        "Error passing null pointers as arguments to driver_concat");
    return kOtherFailure;
  }

  if (nSourceFileCount < 1) {
    GetLogger()->error(
        "Error passing invalid number of files to driver_concat");
    return kOtherFailure;
  }

  GetLogger()->debug("driver_concat {} with {} sources:", sDestFilePathName,
                     nSourceFileCount);

  // Parse destination to get bucket name
  ParseUriResult names;
  if (GetBucketAndObjectNames(&names, sDestFilePathName)) {
    GetLogger()->error(ERR_URL_PARSING);
    return kOtherFailure;
  }
  const std::string &bucket = names.bucket;

  // Validate all source paths belong to the destination bucket
  std::vector<std::string> sources;

  for (size_t i = 0; i < nSourceFileCount; ++i) {
    ParseUriResult parsedUri;
    if (ParseGcsUri(&parsedUri, sSourceFilePathNames[i])) {
      GetLogger()->error(ERR_URL_PARSING);
      return kOtherFailure;
    }

    if (parsedUri.bucket != bucket) {
      std::ostringstream os;
      os << "Source file bucket '" << parsedUri.bucket
         << "' must match destination bucket '" << bucket << "'";
      GetLogger()->error(os.str());
      return kOtherFailure;
    }

    GetLogger()->debug("- {}", sSourceFilePathNames[i]);
    sources.push_back(std::move(parsedUri.object));
  }

  // GCS ComposeObject limit: maximum 32 source objects per operation
  constexpr size_t MAX_COMPOSE_SOURCES = 32;

  bool failure_detected = false;

  // Helper function to generate temporary file names
  size_t temp_counter = 0;
  auto generate_temp_name = [&]() -> std::string {
    std::ostringstream oss;
    oss << ".tmp_concat_" << names.object << "_" << std::setfill('0')
        << std::setw(6) << temp_counter++;
    return oss.str();
  };

  // Helper function to compose sources and delete them
  auto compose_and_delete = [&](const std::vector<std::string> &batch_sources,
                                const std::string &dest_object) -> bool {
    std::vector<gcs::ComposeSourceObject> sourceObjects;
    for (const auto &source : batch_sources) {
      sourceObjects.push_back(gcs::ComposeSourceObject{source, {}, {}});
    }

    auto maybe_compose =
        client.ComposeObject(bucket, sourceObjects, dest_object);
    if (!maybe_compose) {
      GetLogger()->error("Error during composition to {}", dest_object);
      return false;
    }

    // Delete source files after successful composition
    for (const auto &source : batch_sources) {
      auto delete_status = client.DeleteObject(bucket, source);
      if (!delete_status.ok() &&
          delete_status.code() != gc::StatusCode::kNotFound) {
        GetLogger()->error("Error deleting source file '{}'", source);
        failure_detected = true;
      }
    }

    return true;
  };

  // Special case: if we have <= 32 files, compose directly to destination
  if (sources.size() <= MAX_COMPOSE_SOURCES) {
    GetLogger()->debug("Direct composition: {} files to {}", sources.size(),
                       names.object);

    if (!compose_and_delete(sources, names.object)) {
      return kOtherFailure;
    }

    return failure_detected ? kOtherFailure : kOtherSuccess;
  }

  // Iterative concatenation strategy:
  // 1. Compose first 32 files into temp_0
  // 2. Compose temp_0 + next 31 files into temp_1
  // 3. Compose temp_1 + next 31 files into temp_2
  // ... until all files are consumed
  // Final: rename last temp to destination

  std::string current_result;
  size_t files_processed = 0;

  // First batch: compose first 32 files
  {
    std::vector<std::string> first_batch(sources.begin(),
                                         sources.begin() + MAX_COMPOSE_SOURCES);

    current_result = generate_temp_name();

    GetLogger()->debug("Initial batch: composing {} files into {}",
                       first_batch.size(), current_result);

    if (!compose_and_delete(first_batch, current_result)) {
      return kOtherFailure;
    }

    files_processed = MAX_COMPOSE_SOURCES;
  }

  // Subsequent batches: compose current_result + next 31 files
  while (files_processed < sources.size()) {
    size_t remaining = sources.size() - files_processed;
    size_t batch_size = std::min(MAX_COMPOSE_SOURCES - 1, remaining);

    std::vector<std::string> batch;
    batch.reserve(batch_size + 1);

    // Add current result as first element
    batch.push_back(current_result);

    // Add next batch_size files
    batch.insert(batch.end(), sources.begin() + files_processed,
                 sources.begin() + files_processed + batch_size);

    std::string new_result = generate_temp_name();

    GetLogger()->debug("Iterative batch: composing {} files ({} new) into {}",
                       batch.size(), batch_size, new_result);

    if (!compose_and_delete(batch, new_result)) {
      // Clean up current_result on failure
      client.DeleteObject(bucket, current_result);
      return kOtherFailure;
    }

    current_result = new_result;
    files_processed += batch_size;
  }

  // Rename final temp file to destination using CopyObject + Delete
  GetLogger()->debug("Final step: renaming {} to {}", current_result,
                     names.object);

  auto maybe_copy =
      client.CopyObject(bucket, current_result, bucket, names.object);
  if (!maybe_copy) {
    GetLogger()->error("Error renaming final result to destination");
    client.DeleteObject(bucket, current_result);
    return kOtherFailure;
  }

  // Delete the temporary file
  auto delete_status = client.DeleteObject(bucket, current_result);
  if (!delete_status.ok() &&
      delete_status.code() != gc::StatusCode::kNotFound) {
    GetLogger()->error("Error deleting final temporary file");
    failure_detected = true;
  }

  return failure_detected ? kOtherFailure : kOtherSuccess;
}

int driver_composeMultifile(const char *sDestFilePathName,
                            const char **sSourceFilePathNames,
                            size_t nSourceFileCount) {
  if (!bIsConnected) {
    GetLogger()->error(ERR_NOT_CONNECTED);
    return kOtherFailure;
  }

  if (!sDestFilePathName || !sSourceFilePathNames) {
    GetLogger()->error(
        "Error passing null pointers as arguments to driver_composeMultifile");
    return kOtherFailure;
  }

  if (nSourceFileCount < 1) {
    GetLogger()->error(
        "Error passing invalid number of files to driver_composeMultifile");
    return kOtherFailure;
  }

  GetLogger()->debug("driver_composeMultifile {} with {} sources:",
                     sDestFilePathName, nSourceFileCount);

  // Parse and validate the globbing pattern
  auto maybe_pattern = ParseGlobbingPattern(sDestFilePathName);
  if (!maybe_pattern) {
    GetLogger()->error("Invalid globbing pattern");
    return kOtherFailure;
  }

  // ✅ C++14: Décomposer manuellement au lieu d'utiliser structured binding
  const auto &pattern_result = *maybe_pattern;
  const std::string &prefix = pattern_result.first;
  const std::string &suffix = pattern_result.second;

  // Extract bucket and base object path from prefix using ParseGcsUri
  ParseUriResult parsedUri;
  if (ParseGcsUri(&parsedUri, prefix)) {
    GetLogger()->error("Error parsing destination pattern");
    return kOtherFailure;
  }

  const std::string &dest_bucket = parsedUri.bucket;
  const std::string &base_object = parsedUri.object;

  // Validate all source paths are relative
  for (size_t i = 0; i < nSourceFileCount; ++i) {
    if (!IsRelativePath(sSourceFilePathNames[i])) {
      std::ostringstream os;
      os << "Source file path must be relative (no gs:// allowed): "
         << sSourceFilePathNames[i];
      GetLogger()->error(os.str());
      return kOtherFailure;
    }
    GetLogger()->debug("- {}", sSourceFilePathNames[i]);
  }

  // Rename each source file to follow the globbing pattern using CopyObject
  bool failure_detected = false;

  for (size_t i = 0; i < nSourceFileCount; ++i) {
    // Generate the new name with sequence number
    std::string sequence_number = GenerateSequenceNumber(i);

    // ✅ C++14: Utiliser std::ostringstream pour la concaténation
    std::ostringstream new_name_oss;
    new_name_oss << base_object << sequence_number << suffix;
    std::string new_object_name = new_name_oss.str();

    GetLogger()->debug("Renaming {} to {}", sSourceFilePathNames[i],
                       new_object_name);

    // Use CopyObject instead of ComposeObject for better performance
    auto maybe_copy =
        client.CopyObject(dest_bucket,             // source bucket
                          sSourceFilePathNames[i], // source object
                          dest_bucket,             // destination bucket
                          new_object_name          // destination object
        );

    if (!maybe_copy) {
      GetLogger()->error("Error renaming '{}' to '{}'", sSourceFilePathNames[i],
                         new_object_name);
      failure_detected = true;
      continue;
    }

    // Delete the original source file
    auto delete_status =
        client.DeleteObject(dest_bucket, sSourceFilePathNames[i]);
    if (!delete_status.ok() &&
        delete_status.code() != gc::StatusCode::kNotFound) {
      GetLogger()->error("Error deleting original file '{}'",
                         sSourceFilePathNames[i]);
      failure_detected = true;
    }
  }

  return failure_detected ? kOtherFailure : kOtherSuccess;
}
