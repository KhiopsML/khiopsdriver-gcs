#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "gcsplugin.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/rest_options.h"
#include "spdlog/spdlog.h"

#include <algorithm>
#include <assert.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <google/cloud/rest_options.h>
#include <memory>

constexpr const char* version = "0.1.0";
constexpr const char* driver_name = "GCS driver";
constexpr const char* driver_scheme = "gs";
constexpr long long preferred_buffer_size = 4 * 1024 * 1024;

bool bIsConnected = false;

namespace gc = ::google::cloud;
namespace gcs = gc::storage;
gcs::Client client;
// Global bucket name
std::string globalBucketName;

using tOffset = long long;

constexpr int kSuccess{ 0 };
constexpr int kFailure{ 1 };

constexpr int kFalse{ 0 };
constexpr int kTrue{ 1 };

struct MultiPartFile
{
    std::string bucketname_;
    std::string filename_;
    tOffset offset_{ 0 };
    // Added for multifile support
    tOffset commonHeaderLength_{ 0 };
    std::vector<std::string> filenames_;
    std::vector<long long int> cumulativeSize_;
    tOffset total_size_{ 0 };
};

// Definition of helper functions
long long int DownloadFileRangeToBuffer(const std::string& bucket_name,
    const std::string& object_name,
    char* buffer,
    //std::size_t buffer_length,
    std::int64_t start_range,
    std::int64_t end_range) {
    namespace gcs = google::cloud::storage;

    auto reader = client.ReadObject(bucket_name, object_name, gcs::ReadRange(start_range, end_range));
    if (!reader) {
        spdlog::error("Error reading object: {}", reader.status().message());
        return -1;
    }

    reader.read(buffer, end_range - start_range);
    if (reader.bad()/* || reader.fail()*/) {
        spdlog::error("Error during read: {} {}", (int)(reader.status().code()), reader.status().message());
        return -1;
    }

    long long int num_read = static_cast<long long>(reader.gcount());
    spdlog::debug("read = {}", num_read);

    return num_read;
}

bool UploadBufferToGcs(const std::string& bucket_name,
    const std::string& object_name,
    const char* buffer,
    std::size_t buffer_size) {

    auto writer = client.WriteObject(bucket_name, object_name);
    writer.write(buffer, buffer_size);
    writer.Close();

    auto status = writer.metadata();
    if (!status) {
        spdlog::error("Error during upload: {} {}", (int)(status.status().code()), status.status().message());
        return false;
    }

    return true;
}

bool ParseGcsUri(const std::string& gcs_uri, std::string& bucket_name, std::string& object_name)
{
    constexpr const char* prefix{ "gs://" };
    const size_t prefix_size{ std::strlen(prefix) };
    //const size_t prefix_size{prefix.}
    if (gcs_uri.compare(0, prefix_size, prefix) != 0) {
        spdlog::error("Invalid GCS URI: {}", gcs_uri);
        return false;
    }

    const size_t pos = gcs_uri.find('/', prefix_size);
    if (pos == std::string::npos) {
        spdlog::error("Invalid GCS URI, missing object name: {}", gcs_uri);
        return false;
    }

    bucket_name = gcs_uri.substr(prefix_size, pos - prefix_size);
    object_name = gcs_uri.substr(pos + 1);

    return true;
}

void FallbackToDefaultBucket(std::string& bucket_name) {
    if (!bucket_name.empty())
        return;
    if (!globalBucketName.empty()) {
        bucket_name = globalBucketName;
        return;
    }
    spdlog::critical("No bucket specified, and GCS_BUCKET_NAME is not set!");
}

void GetBucketAndObjectNames(const char* sFilePathName, std::string& bucket, std::string& object)
{
    if (!ParseGcsUri(sFilePathName, bucket, object))
    {
        bucket.clear();
        object.clear();
        return;
    }
    FallbackToDefaultBucket(bucket);
}

std::string ToLower(const std::string& str)
{
    std::string low{ str };
    const size_t cnt = low.length();
    for (size_t i = 0; i < cnt; i++)
    {
        low[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(low[i]))); // see https://en.cppreference.com/w/cpp/string/byte/tolower
    }
    return low;
}

std::string GetEnvironmentVariableOrDefault(const std::string& variable_name,
    const std::string& default_value)
{
    char* value = getenv(variable_name.c_str());

    if (value && std::strlen(value) > 0)
    {
        return value;
    }

    const std::string low_key = ToLower(variable_name);
    if (low_key.find("token") || low_key.find("password") || low_key.find("key") || low_key.find("secret"))
    {
        spdlog::debug("No {} specified, using **REDACTED** as default.", variable_name);
    }
    else
    {
        spdlog::debug("No {} specified, using '{}' as default.", variable_name, default_value);
    }

    return default_value;
}


void HandleNoObjectError(const gc::Status& status)
{
    if (status.code() != google::cloud::StatusCode::kNotFound)
    {
        spdlog::error("Error checking object: {}", status.message());
    }
}

// Implementation of driver functions
void test_setClient(::google::cloud::storage::Client && mock_client)
{
    client = std::move(mock_client);
    bIsConnected = kTrue;
}

void test_unsetClient()
{
    client = ::google::cloud::storage::Client{};
}

const char* driver_getDriverName()
{
    return driver_name;
}

const char* driver_getVersion()
{
    return version;
}

const char* driver_getScheme()
{
    return driver_scheme;
}

int driver_isReadOnly()
{
    return kFalse;
}

int driver_connect()
{
    const std::string loglevel = GetEnvironmentVariableOrDefault("GCS_DRIVER_LOGLEVEL", "info");
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
    std::string project = GetEnvironmentVariableOrDefault("CLOUD_ML_PROJECT_ID", "");
    if (!project.empty())
    {
        options.set<gc::QuotaUserOption>(std::move(project));
    }

    std::string gcp_token_filename = GetEnvironmentVariableOrDefault("GCP_TOKEN", "");
    //if (!gcp_token_filename.empty())
    //{
    //    // Initialize from token file
    //    client = 
    //}
    //else
    {
        // Fallback to standard credentials discovery
        client = gcs::Client{ std::move(options) };
    }

    bIsConnected = true;
    return kSuccess;
}

int driver_disconnect()
{
    bIsConnected = false;
    return kSuccess;
}

int driver_isConnected()
{
    return bIsConnected ? 1 : 0;
}

long long int driver_getSystemPreferredBufferSize()
{
    return preferred_buffer_size; // 4 Mo
}

int driver_exist(const char* filename)
{
    if (!filename)
    {
        spdlog::error("Error passing null pointer to exist");
        return kFalse;
    }

    spdlog::debug("exist {}", filename);

    std::string file_uri = filename;
    spdlog::debug("exist file_uri {}", file_uri);
    spdlog::debug("exist last char {}", file_uri.back());

    if (file_uri.back() == '/') {
        return driver_dirExists(filename);
    }
    else {
        return driver_fileExists(filename);
    }
}

int driver_fileExists(const char* sFilePathName)
{
    if (!sFilePathName)
    {
        spdlog::error("Error passing null pointer to fileExists.");
        return kFalse;
    }

    spdlog::debug("fileExist {}", sFilePathName);

    std::string bucket_name;
    std::string object_name;
    GetBucketAndObjectNames(sFilePathName, bucket_name, object_name);

    if (bucket_name.empty() || object_name.empty())
    {
        return kFalse;
    }

    auto status_or_metadata_list = client.ListObjects(bucket_name, gcs::MatchGlob{ object_name });
    const auto first_item_it = status_or_metadata_list.begin();
    if ((first_item_it == status_or_metadata_list.end()) || !(*first_item_it))
    {
        spdlog::error("Error checking object");
        return kFalse;
    }

    spdlog::debug("file {} exists!", sFilePathName);
    return kTrue; // L'objet existe
}

int driver_dirExists(const char* sFilePathName)
{
    if (!sFilePathName)
    {
        spdlog::error("Error passing null pointer to dirExists");
        return kFalse;
    }

    spdlog::debug("dirExist {}", sFilePathName);
    return kTrue;
}

std::string ReadHeader(const std::string& bucket_name, const std::string& filename)
{
    gcs::ObjectReadStream stream = client.ReadObject(bucket_name, filename);
    std::string line;
    std::getline(stream, line, '\n');
    if (stream.bad())
    {
        return "";
    }
    return line;
}


long long GetFileSize(const std::string& bucket_name, const std::string& object_name)
{
    auto list = client.ListObjects(bucket_name, gcs::MatchGlob{ object_name });
    auto list_it = list.begin();
    const auto list_end = list.end();

    if (list_end == list_it)
    {
        //no file, or not a file
        return -1;
    }

    const auto object_metadata = std::move(*list_it);
    if (!object_metadata)
    {
        //unusable data
        return -1;
    }

    long long total_size = static_cast<long long>(object_metadata->size());

    list_it++;
    if (list_end == list_it)
    {
        //unique file
        return total_size;
    }

    // multifile
    // check headers
    const std::string header = ReadHeader(bucket_name, object_metadata->name());
    if (header.empty())
    {
        return -1;
    }
    const long long header_size = static_cast<long long>(header.size());
    int header_to_subtract{ 0 };
    bool same_header{ true };

    for (; list_it != list.end(); list_it++)
    {
        if (same_header)
        {
            const std::string curr_header = ReadHeader(bucket_name, (*list_it)->name());
            if (curr_header.empty())
            {
                return -1;
            }
            same_header = (header == curr_header);
            if (same_header)
            {
                header_to_subtract++;
            }
        }
        total_size += static_cast<long long>((*list_it)->size());
    }

    if (!same_header)
    {
        header_to_subtract = 0;
    }
    return total_size - header_to_subtract * header_size;
}

long long int driver_getFileSize(const char* filename)
{
    if (!filename)
    {
        spdlog::error("Error passing null pointer to getFileSize.");
        return -1;
    }

    spdlog::debug("getFileSize {}", filename);

    std::string bucket_name;
    std::string object_name;
    GetBucketAndObjectNames(filename, bucket_name, object_name);

    if (bucket_name.empty() || object_name.empty())
    {
        return -1;
    }

    return GetFileSize(bucket_name, object_name);
}

int AccumulateNamesAndSizes(MultiPartFile& h)
{
    auto push_back_data = [](MultiPartFile& h, gcs::ListObjectsIterator& list_it) {
        h.filenames_.push_back((*list_it)->name());
        h.cumulativeSize_.push_back(static_cast<long long>((*list_it)->size()));
        };

    const std::string& bucket_name{ h.bucketname_ };
    auto list = client.ListObjects(bucket_name, gcs::MatchGlob{ h.filename_ });
    auto list_it = list.begin();
    const auto list_end = list.end();

    if (list_end == list_it)
    {
        //no file, or not a file
        return kFailure;
    }

    if (!(*list_it))
    {
        //unusable data
        return kFailure;
    }

    push_back_data(h, list_it);

    list_it++;
    if (list_end == list_it)
    {
        //unique file
        h.total_size_ = h.cumulativeSize_[0];
        return kSuccess;
    }

    // multifile
    // check headers
    const std::string header = ReadHeader(bucket_name, h.filenames_[0]);
    const long long header_size = static_cast<long long>(header.size());
    bool same_header{ true };

    for (; list_it != list.end(); list_it++)
    {
        if (!(*list_it))
        {
            //unusable data
            return kFailure;
        }

        push_back_data(h, list_it);
        auto last_size_it = h.cumulativeSize_.rbegin();
        const auto size_before_last_it = last_size_it + 1;
        *last_size_it += *size_before_last_it;

        if (same_header)
        {
            const std::string curr_header = ReadHeader(bucket_name, *(h.filenames_.crbegin()));
            same_header = (header == curr_header);
            if (same_header)
            {
                *last_size_it -= header_size;
            }
        }
    }

    h.total_size_ = *(h.cumulativeSize_.rbegin());

    return kSuccess;
}

void* driver_fopen(const char* filename, char mode)
{
    assert(driver_isConnected());

    if (!filename)
    {
        spdlog::error("Error passing null pointer to fopen.");
        return nullptr;
    }

    spdlog::debug("fopen {} {}", filename, mode);

    auto h = new MultiPartFile;
    GetBucketAndObjectNames(filename, h->bucketname_, h->filename_);

    switch (mode) {
    case 'r':
    {
        if (kFailure == AccumulateNamesAndSizes(*h))
        {
            //h is unusable
            delete h;
            return nullptr;
        }
        return h;
    }
    case 'w':
        return h;

    case 'a':

    default:
        spdlog::error("Invalid open mode {}", mode);
        return 0;
    }
}

int driver_fclose(void* stream)
{
    assert(driver_isConnected());

    if (!stream)
    {
        return kFailure;
    }

    spdlog::debug("fclose {}", (void*)stream);
    delete (reinterpret_cast<MultiPartFile*>(stream));
    return kSuccess;
}

//long long int totalSize(MultiPartFile* h) {
//	return h->cumulativeSize[h->cumulativeSize.size() - 1];
//}

int driver_fseek(void* stream, long long int offset, int whence)
{
    spdlog::debug("fseek {} {} {}", stream, offset, whence);

    assert(stream != NULL);
    MultiPartFile* h = (MultiPartFile*)stream;

    switch (whence) {
    case std::ios::beg:
        h->offset_ = offset;
        return 0;
    case std::ios::cur:
    {
        auto computedOffset = h->offset_ + offset;
        if (computedOffset < 0) {
            spdlog::critical("Invalid seek offset {}", computedOffset);
            return -1;
        }
        h->offset_ = computedOffset;
        return 0;
    }
    case std::ios::end:
    {
        auto computedOffset = h->total_size_ + offset;
        if (computedOffset < 0) {
            spdlog::critical("Invalid seek offset {}", computedOffset);
            return -1;
        }
        h->offset_ = computedOffset;
        return 0;
    }
    default:
        spdlog::critical("Invalid seek mode {}", whence);
        return -1;

    }

}

const char* driver_getlasterror()
{
    spdlog::debug("getlasterror");

    return NULL;
}

long long int driver_fread(void* ptr, size_t size, size_t count, void* stream)
{
    if (!stream)
    {
        spdlog::error("Error passing null stream pointer to fread");
        return -1;
    }
    if (!ptr)
    {
        spdlog::error("Error passing null buffer pointer to fread");
        return -1;
    }

    spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

    MultiPartFile* h = reinterpret_cast<MultiPartFile*>(stream);

    //Refuse to read if offset > totalSize
    const tOffset offset = h->offset_;
    const tOffset total_size = h->total_size_;
    if (offset > total_size)
    {
        return -1;
    }

    tOffset to_read{ static_cast<tOffset>(size * count) };

    if (offset + to_read > total_size)
    {
        to_read = total_size - offset;
        spdlog::debug("offset {}, req len {} exceeds file size ({}) -> reducing len to {}",
            offset, to_read, total_size, to_read);
    }
    else
    {
        spdlog::debug("offset = {} to_read = {}", offset, to_read);
    }

    // Start at first usable file chunk
    // Advance through file chunks, advancing buffer pointer
    // Until last requested byte was read
    // Or error occured

    tOffset bytes_read{ 0 };

    // Lookup item containing initial bytes at requested offset
    const auto& cumul_sizes = h->cumulativeSize_;
    const tOffset common_header_length = h->commonHeaderLength_;
    const std::string& bucket_name = h->bucketname_;
    const auto& filenames = h->filenames_;
    char* buffer_pos = reinterpret_cast<char*>(ptr);

    auto greater_than_offset_it = std::upper_bound(cumul_sizes.begin(), cumul_sizes.end(), offset);
    size_t idx = static_cast<size_t>(std::distance(cumul_sizes.begin(), greater_than_offset_it));

    spdlog::debug("Use item {} to read @ {} (end = {})", idx, offset, *greater_than_offset_it);


    auto download_and_update = [&](tOffset read_start, tOffset read_end) -> tOffset {
        tOffset range_bytes_read = DownloadFileRangeToBuffer(bucket_name, filenames[idx], buffer_pos,
            static_cast<int64_t>(read_start), static_cast<int64_t>(read_end));

        to_read -= range_bytes_read;
        bytes_read += range_bytes_read;
        buffer_pos += range_bytes_read;

        return range_bytes_read;
        };


    //first file read

    const tOffset file_start = (idx == 0) ? offset : offset - cumul_sizes[idx - 1] + common_header_length;
    const tOffset read_end = std::min(file_start + to_read, file_start + cumul_sizes[idx] - offset);

    if (download_and_update(file_start, read_end) == -1)
    {
        return -1;
    }

    while (to_read)
    {
        // read the missing bytes in the next files as necessary
        idx++;
        const tOffset start = common_header_length;
        const tOffset end = std::min(start + to_read, start + cumul_sizes[idx] - cumul_sizes[idx - 1]);

        if (download_and_update(start, end) == -1)
        {
            return -1;
        }
    }

    h->offset_ += bytes_read;
    return bytes_read;

    //// TODO: handle multifile case
    //auto toRead = size * count;
    //spdlog::debug("offset = {} toRead = {}", h->offset, toRead);

    //auto num_read = DownloadFileRangeToBuffer(h->bucketname, h->filename, (char*)ptr, toRead, h->offset,
    //	h->offset + toRead);

    //if (num_read != -1)
    //	h->offset += num_read;
    //return num_read;
}

long long int driver_fwrite(const void* ptr, size_t size, size_t count, void* stream)
{
    spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

    assert(stream != NULL);
    MultiPartFile* h = (MultiPartFile*)stream;

    UploadBufferToGcs(h->bucketname_, h->filename_, (char*)ptr, size * count);

    // TODO proper error handling...
    return size * count;
}

int driver_fflush(void* stream)
{
    spdlog::debug("Flushing (does nothing...)");
    return 0;
}

int driver_remove(const char* filename)
{
    spdlog::debug("remove {}", filename);

    assert(driver_isConnected());
    std::string bucket_name, object_name;
    ParseGcsUri(filename, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    auto status = client.DeleteObject(bucket_name, object_name);
    if (!status.ok()) {
        spdlog::error("Error deleting object: {} {}", (int)(status.code()), status.message());
        return 0;
    }

    return 1;
}

int driver_rmdir(const char* filename)
{
    spdlog::debug("rmdir {}", filename);

    assert(driver_isConnected());
    spdlog::debug("Remove dir (does nothing...)");
    return 1;
}

int driver_mkdir(const char* filename)
{
    spdlog::debug("mkdir {}", filename);

    assert(driver_isConnected());
    return 1;
}

long long int driver_diskFreeSpace(const char* filename)
{
    spdlog::debug("diskFreeSpace {}", filename);

    assert(driver_isConnected());
    return (long long int)5 * 1024 * 1024 * 1024 * 1024;
}

int driver_copyToLocal(const char* sSourceFilePathName, const char* sDestFilePathName)
{
    spdlog::debug("copyToLocal {} {}", sSourceFilePathName, sDestFilePathName);

    assert(driver_isConnected());

    namespace gcs = google::cloud::storage;
    std::string bucket_name, object_name;
    ParseGcsUri(sSourceFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    // Create a ReadObject stream
    auto reader = client.ReadObject(bucket_name, object_name);
    if (!reader) {
        spdlog::error("Error initializing download stream: {} {}", (int)(reader.status().code()), reader.status().message());
        return false;
    }

    // Open the local file
    std::ofstream file_stream(sDestFilePathName, std::ios::binary);
    if (!file_stream.is_open()) {
        spdlog::error("Failed to open local file for writing: {}", sDestFilePathName);
        return false;
    }

    // Read from the GCS object and write to the local file
    std::string buffer(1024, '\0');
    spdlog::debug("Start reading {}", buffer);

    bool complete = false;
    while (!complete) {
        reader.read(&buffer[0], buffer.size());

        if (reader.bad()) {
            spdlog::error("Error during read: {} {}", (int)(reader.status().code()), reader.status().message());
            complete = true;
        }
        spdlog::debug("Read {}", reader.gcount());

        if (reader.gcount() > 0) {
            file_stream.write(buffer.data(), reader.gcount());
        }
        else {
            complete = true;
        }

    }
    spdlog::debug("Close output");
    file_stream.close();

    if (!reader.status().ok()) {
        std::cerr << "Error during download: " << reader.status() << "\n";
        return 0;
    }
    spdlog::debug("Done copying");

    return 1;
}

int driver_copyFromLocal(const char* sSourceFilePathName, const char* sDestFilePathName)
{
    spdlog::debug("copyFromLocal {} {}", sSourceFilePathName, sDestFilePathName);

    assert(driver_isConnected());

    namespace gcs = google::cloud::storage;
    std::string bucket_name, object_name;
    ParseGcsUri(sDestFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    // Open the local file
    std::ifstream file_stream(sSourceFilePathName, std::ios::binary);
    if (!file_stream.is_open()) {
        spdlog::error("Failed to open local file: {}", sSourceFilePathName);
        return false;
    }

    // Create a WriteObject stream
    auto writer = client.WriteObject(bucket_name, object_name, gcs::IfGenerationMatch(0));
    if (!writer) {
        spdlog::error("Error initializing upload stream: {} {}", (int)(writer.metadata().status().code()), writer.metadata().status().message());
        return false;
    }

    // Read from the local file and write to the GCS object
    std::string buffer(1024, '\0');
    while (!file_stream.eof()) {
        file_stream.read(&buffer[0], buffer.size());
        writer.write(buffer.data(), file_stream.gcount());
    }
    file_stream.close();

    // Close the GCS WriteObject stream to complete the upload
    writer.Close();

    auto status = writer.metadata();
    if (!status) {
        spdlog::error("Error during file upload: {} {}", (int)(status.status().code()), status.status().message());

        return false;
    }

    return true;
}


