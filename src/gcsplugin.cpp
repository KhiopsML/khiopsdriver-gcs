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
#include <limits>
#include <limits.h>
#include <memory>

#include "google/cloud/rest_options.h"
#include "google/cloud/storage/client.h"
#include <google/cloud/storage/object_write_stream.h>

#include "spdlog/spdlog.h"


using namespace gcsplugin;

namespace gc = ::google::cloud;
namespace gcs = gc::storage;


constexpr const char* version = "0.1.0";
constexpr const char* driver_name = "GCS driver";
constexpr const char* driver_scheme = "gs";
constexpr long long preferred_buffer_size = 4 * 1024 * 1024;


bool bIsConnected = false;

gcs::Client client;
// Global bucket name
std::string globalBucketName;

// Last error
std::string lastError;

void InitHandle(Handle& h, ReaderPtr&& r_ptr)
{
    h.var.reader = std::move(r_ptr);
}

void InitHandle(Handle& h, WriterPtr&& w_ptr)
{
    h.var.writer = std::move(w_ptr);
}

template<typename VariantPtr, HandleType Type>
HandlePtr MakeHandleFromVariant(VariantPtr&& var_ptr)
{
    HandlePtr h{ new Handle(Type) };
    InitHandle(*h, std::move(var_ptr));
    return h;
}

template<typename VariantPtr, HandleType Type>
Handle* InsertHandle(VariantPtr&& var_ptr)
{
    return active_handles.insert(active_handles.end(),
        MakeHandleFromVariant<VariantPtr, Type>(std::move(var_ptr)))->get();
}

HandleIt FindHandle(void* handle)
{
    return std::find_if(active_handles.begin(), active_handles.end(), [handle](const HandlePtr& act_h_ptr) {
        return handle == static_cast<void*>(act_h_ptr.get());
        });
}

void EraseRemove(HandleIt pos)
{
    *pos = std::move(active_handles.back());
    active_handles.pop_back();
}

// Definition of helper functions
long long int DownloadFileRangeToBuffer(const std::string& bucket_name,
    const std::string& object_name,
    char* buffer,
    std::int64_t start_range,
    std::int64_t end_range)
{
    auto reader = client.ReadObject(bucket_name, object_name, gcs::ReadRange(start_range, end_range));
    if (!reader) {
        spdlog::error("Error reading object: {}", reader.status().message());
        return -1;
    }

    reader.read(buffer, end_range - start_range);
    if (reader.bad()) {
        spdlog::error("Error during read: {} {}", (int)(reader.status().code()), reader.status().message());
        return -1;
    }

    long long int num_read = static_cast<long long>(reader.gcount());
    spdlog::debug("read = {}", num_read);

    return num_read;
}

long long ReadBytesInFile(MultiPartFile& multifile, char* buffer, tOffset to_read)
{
    // Start at first usable file chunk
    // Advance through file chunks, advancing buffer pointer
    // Until last requested byte was read
    // Or error occured

    tOffset bytes_read{ 0 };

    // Lookup item containing initial bytes at requested offset
    const auto& cumul_sizes = multifile.cumulativeSize_;
    const tOffset common_header_length = multifile.commonHeaderLength_;
    const std::string& bucket_name = multifile.bucketname_;
    const auto& filenames = multifile.filenames_;
    char* buffer_pos = buffer;
    tOffset& offset = multifile.offset_;
    const tOffset offset_bak = offset;      // in case of irrecoverable error, leave the multifile in its starting state

    auto greater_than_offset_it = std::upper_bound(cumul_sizes.begin(), cumul_sizes.end(), offset);
    size_t idx = static_cast<size_t>(std::distance(cumul_sizes.begin(), greater_than_offset_it));

    spdlog::debug("Use item {} to read @ {} (end = {})", idx, offset, *greater_than_offset_it);


    auto download_and_update = [&](const std::string& filename, tOffset read_start, tOffset read_end) -> tOffset {
        tOffset range_bytes_read = DownloadFileRangeToBuffer(bucket_name, filename, buffer_pos,
            static_cast<int64_t>(read_start), static_cast<int64_t>(read_end));
        if (-1 == range_bytes_read)
        {
            return -1;
        }

        to_read -= range_bytes_read;
        bytes_read += range_bytes_read;
        buffer_pos += range_bytes_read;
        offset += range_bytes_read;

        return range_bytes_read;
        };


    //first file read

    const tOffset file_start = (idx == 0) ? offset : offset - cumul_sizes[idx - 1] + common_header_length;
    const tOffset read_end = std::min(file_start + to_read, file_start + cumul_sizes[idx] - offset);
    tOffset expected_read = read_end - file_start;

    tOffset actual_read = download_and_update(filenames[idx], file_start, read_end);
    if (-1 == actual_read)
    {
        offset = offset_bak;
        return -1;
    }
    if (actual_read < expected_read)
    {
        spdlog::debug("End of file encountered");
        return bytes_read;
    }

    // continue with the next files
    while (to_read)
    {
        // read the missing bytes in the next files as necessary
        idx++;
        const tOffset start = common_header_length;
        const tOffset end = std::min(start + to_read, start + cumul_sizes[idx] - cumul_sizes[idx - 1]);
        expected_read = end - start;

        actual_read = download_and_update(filenames[idx], start, end);
        if (-1 == actual_read)
        {
            offset = offset_bak;
            return -1;
        }
        if (actual_read < expected_read)
        {
            spdlog::debug("End of file encountered");
            return bytes_read;
        }
    }

    return bytes_read;
}

bool ParseGcsUri(const std::string& gcs_uri, std::string& bucket_name, std::string& object_name)
{
    char const* prefix = "gs://";
    const size_t prefix_size{ std::strlen(prefix) };
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


bool WillSizeCountProductOverflow(size_t size, size_t count)
{
    constexpr size_t max_prod_usable{ static_cast<size_t>(std::numeric_limits<tOffset>::max()) };
    if (max_prod_usable / size < count || max_prod_usable / count < size)
    {
        spdlog::critical("product size * count is too large, would overflow");
        return true;
    }
    return false;
}

// Implementation of driver functions
void test_setClient(::google::cloud::storage::Client&& mock_client)
{
    client = std::move(mock_client);
    bIsConnected = kTrue;
}

void test_unsetClient()
{
    client = ::google::cloud::storage::Client{};
}

void* test_getActiveHandles()
{
    return &active_handles;
}

void* test_addReaderHandle(
    const std::string& bucket,
    const std::string& object,
    long long offset,
    long long commonHeaderLength,
    const std::vector<std::string>& filenames,
    const std::vector<long long int>& cumulativeSize,
    long long total_size)
{
    ReaderPtr reader_ptr{ new MultiPartFile{
        bucket,
        object,
        offset,
        commonHeaderLength,
        filenames,
        cumulativeSize,
        total_size
    }};
    return InsertHandle<ReaderPtr, HandleType::kRead>(std::move(reader_ptr));
}

void* test_addWriterHandle(bool appendMode, bool create_with_mock_client, std::string bucketname, std::string objectname)
{
    if (!create_with_mock_client)
    {
        if (appendMode)
        {
            return InsertHandle<WriterPtr, HandleType::kAppend>(WriterPtr(new WriteFile));
        }
        return InsertHandle<WriterPtr, HandleType::kWrite>(WriterPtr(new WriteFile));
    }

    auto writer = client.WriteObject(bucketname, objectname);
    if (!writer)
    {
        return nullptr;
    }

    WriterPtr writer_struct{ new WriteFile };
    writer_struct->bucketname_ = std::move(bucketname);
    writer_struct->filename_ = std::move(objectname);
    writer_struct->writer_ = std::move(writer);

    if (appendMode)
    {
        return InsertHandle<WriterPtr, HandleType::kAppend>(std::move(writer_struct));
    }
    return InsertHandle<WriterPtr, HandleType::kWrite>(std::move(writer_struct));
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
        options.set<gc::UserProjectOption>(std::move(project));
    }

    std::string gcp_token_filename = GetEnvironmentVariableOrDefault("GCP_TOKEN", "");
    if (!gcp_token_filename.empty())
    {
        // Initialize from token file
        std::ifstream t(gcp_token_filename);
        std::stringstream buffer;
        buffer << t.rdbuf();
        if (t.fail()) {
            return kFailure;
        }
        std::shared_ptr<gc::Credentials> creds = gc::MakeServiceAccountCredentials(buffer.str());
        options.set<gc::UnifiedCredentialsOption>(std::move(creds));
    }

    // Create client with configured options
    client = gcs::Client{ std::move(options) };

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


#define INIT_NAMES_OR_ERROR(pathname, bucketname, objectname, errval)   \
{                                                                       \
    GetBucketAndObjectNames((pathname), (bucketname), (objectname));    \
    if ((bucketname).empty() || (objectname).empty())                   \
    {                                                                   \
        spdlog::error("Error parsing URL.");                            \
        return (errval);                                                \
    }                                                                   \
}                                                                       \


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

    INIT_NAMES_OR_ERROR(sFilePathName, bucket_name, object_name, kFalse);

    auto status_or_metadata_list = client.ListObjects(bucket_name, gcs::MatchGlob{ object_name });
    const auto first_item_it = status_or_metadata_list.begin();
    if (first_item_it == status_or_metadata_list.end())
    {
        spdlog::debug("Object does not exist");
        return kFalse;
    }
    if (!(*first_item_it))
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
    if (!stream.eof())
    {
        line.push_back('\n');
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
        std::string error_message = "No such file or directory";

        spdlog::error("GetFileSize {}: {}", object_name, error_message);
        lastError = std::string(error_message);

        return -1;
    }

    const auto object_metadata = std::move(*list_it);
    if (!object_metadata)
    {
        //unusable data
        std::string error_message = object_metadata.status().message();
        spdlog::error("GetFileSize {}: {} {}", object_name, StatusCodeToString(object_metadata.status().code()), error_message);
        lastError = std::string(error_message);

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
    INIT_NAMES_OR_ERROR(filename, bucket_name, object_name, -1);

    return GetFileSize(bucket_name, object_name);
}

gc::StatusOr<ReaderPtr> MakeReaderPtr(std::string bucketname, std::string objectname)
{
    std::vector<std::string> filenames;
    std::vector<long long> cumulativeSize;

    auto push_back_data = [&](gcs::ListObjectsIterator& list_it) {
        filenames.push_back((*list_it)->name());
        long long size = static_cast<long long>((*list_it)->size());
        if (!cumulativeSize.empty())
        {
            size += cumulativeSize.back();
        }
        cumulativeSize.push_back(size);
        };

    auto list = client.ListObjects(bucketname, gcs::MatchGlob{ objectname });
    auto list_it = list.begin();
    const auto list_end = list.end();

    if (list_end == list_it)
    {
        //no file, or not a file
        return gc::Status(gc::StatusCode::kNotFound, "no file found");
    }

    if (!(*list_it))
    {
        auto& status = (*list_it).status();
        //unusable data
        return gc::Status(status.code(), status.message());
    }

    push_back_data(list_it);

    list_it++;
    if (list_end == list_it)
    {
        //unique file
        const tOffset total_size = cumulativeSize.back();
        return ReaderPtr(new MultiPartFile{
            std::move(bucketname),
            std::move(objectname),
            0,
            0,
            std::move(filenames),
            std::move(cumulativeSize),
            total_size
        });
    }

    // multifile
    // check headers
    const std::string header = ReadHeader(bucketname, filenames.front());
    if (header.empty())
    {
        return gc::Status(gc::StatusCode::kUnknown, "empty header encountered");
    }
    const long long header_size = static_cast<long long>(header.size());
    bool same_header{ true };

    for (; list_it != list.end(); list_it++)
    {
        if (!(*list_it))
        {
            auto& status = (*list_it).status();
            //unusable data
            return gc::Status(status.code(), status.message());
        }

        push_back_data(list_it);

        if (same_header)
        {
            const std::string curr_header = ReadHeader(bucketname, filenames.back());
            if (curr_header.empty())
            {
                return gc::Status(gc::StatusCode::kUnknown, "empty header encountered");
            }
            same_header = (header == curr_header);
            if (same_header)
            {
                cumulativeSize.back() -= header_size;
            }
        }
    }
    
    tOffset total_size = cumulativeSize.back();

    return ReaderPtr(new MultiPartFile{
        std::move(bucketname),
        std::move(objectname),
        0,
        same_header ? header_size : 0,
        std::move(filenames),
        std::move(cumulativeSize),
        total_size
    });
}

gc::StatusOr<WriterPtr> MakeWriterPtr(std::string bucketname, std::string objectname)
{
    auto writer = client.WriteObject(bucketname, objectname);
    if (!writer)
    {
        return writer.last_status();
    }
    WriterPtr writer_struct{ new WriteFile };
    writer_struct->bucketname_ = std::move(bucketname);
    writer_struct->filename_ = std::move(objectname);
    writer_struct->writer_ = std::move(writer);
    return writer_struct;
}

template<typename StreamPtr, HandleType Type>
Handle* RegisterStream(std::function<gc::StatusOr<StreamPtr>(std::string, std::string)> MakeStreamPtr, std::string&& bucket, std::string&& object)
{
    auto maybe_stream = MakeStreamPtr(std::move(bucket), std::move(object));
    if (!maybe_stream)
    {
        spdlog::error("Error while opening stream: {}", maybe_stream.status().message());
        return nullptr;
    }

    return InsertHandle<StreamPtr, Type>(std::move(maybe_stream).value());
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

    std::string bucketname;
    std::string objectname;
    INIT_NAMES_OR_ERROR(filename, bucketname, objectname, nullptr);

    switch (mode) {
    case 'r':
    {
        return RegisterStream<ReaderPtr, HandleType::kRead>(MakeReaderPtr, std::move(bucketname), std::move(objectname));
    }
    case 'w':
    {
        return RegisterStream<WriterPtr, HandleType::kWrite>(MakeWriterPtr, std::move(bucketname), std::move(objectname));
    }
    case 'a':
    {
        // GCS does not as yet provide a way to add data to existing files.
        // This will be the process to emulate an append:
        // - create a multifile struct with the metadata required for a read
        // - gather all the content
        // - close the reading stream
        // - open a writing stream with the same name
        // - rewrite the existing content
        // - return a handle to the open writing streamhere will temporarily open the existing file, gather
        
        auto maybe_reader = MakeReaderPtr(bucketname, objectname);  // no move, the strings are needed below
        if (!maybe_reader)
        {
            // the data is unusable
            spdlog::error("Error while opening file: {}", maybe_reader.status().message());
            return nullptr;
        }

        auto& reader_ptr = maybe_reader.value();
        const long long content_size = reader_ptr->total_size_;
        std::vector<char> content(static_cast<size_t>(content_size));

        const long long bytes_read = ReadBytesInFile(*reader_ptr, content.data(), content_size);
        if (bytes_read == -1)
        {
            spdlog::error("Error while reading file");
            return nullptr;
        }
        else if (bytes_read < content_size)
        {
            spdlog::error("Error while reading file: end of file encountered prematurely");
            return nullptr;
        }

        // content is available for copy, if the file can be opened to write in it
        Handle* out_stream = RegisterStream<WriterPtr, HandleType::kAppend>(MakeWriterPtr, std::move(bucketname), std::move(objectname));

        if (!out_stream)
        {
            return nullptr;
        }

        auto& writer = out_stream->GetWriter().writer_;

        if (!writer.write(content.data(), content_size))
        {
            auto last_status = writer.last_status();
            spdlog::error("Error while rewriting previous file content: {} {}", (int)(last_status.code()), last_status.message());
            return nullptr;
        }

        // content successfully rewritten, return the handle to the writer
        return out_stream;
    }
    default:
        spdlog::error("Invalid open mode {}", mode);
        return nullptr;
    }

}


#define ERROR_NO_STREAM(handle_it, errval)      \
if ((handle_it) == active_handles.end())        \
{                                               \
    spdlog::error("Cannot identify stream");    \
    return (errval);                            \
}                                               \


int driver_fclose(void* stream)
{
    assert(driver_isConnected());

    if (!stream)
    {
        return kFailure;
    }

    spdlog::debug("fclose {}", (void*)stream);

    auto stream_it = FindHandle(stream);
    ERROR_NO_STREAM(stream_it, kFailure);
    auto& h_ptr = *stream_it;
    const HandleType type = h_ptr->type;

    if (HandleType::kWrite == type || HandleType::kAppend == type)
    {
        //close the stream to flush all remaining bytes in the put area
        auto& writer = h_ptr->GetWriter().writer_;
        writer.Close();
        auto& status = writer.metadata();
        if (!status)
        {
            spdlog::error("Error during upload: {} {}", static_cast<int>(status.status().code()), status.status().message());
        }
    }

    EraseRemove(stream_it);

    return kSuccess;
}


int driver_fseek(void* stream, long long int offset, int whence)
{
    constexpr long long max_val = std::numeric_limits<long long>::max();

    if (!stream)
    {
        return -1;
    }

    // confirm stream's presence
    auto to_stream = FindHandle(stream);
    ERROR_NO_STREAM(to_stream, -1);

    auto& stream_h = *to_stream;

    if (HandleType::kRead != stream_h->type)
    {
        spdlog::error("Cannot seek on not reading stream");
        return -1;
    }

    spdlog::debug("fseek {} {} {}", stream, offset, whence);

    MultiPartFile& h = stream_h->GetReader();

    tOffset computed_offset{ 0 };

    switch (whence)
    {
    case std::ios::beg:
        computed_offset = offset;
        break;
    case std::ios::cur:
        if (offset > max_val - h.offset_)
        {
            spdlog::critical("Signed overflow prevented");
            return -1;
        }
        computed_offset = h.offset_ + offset;
        break;
    case std::ios::end:
        if (h.total_size_ > 0)
        {
            long long minus1 = h.total_size_ - 1;
            if (offset > max_val - minus1)
            {
                spdlog::critical("Signed overflow prevented");
                return -1;
            }
        }
        if ((offset == std::numeric_limits<long long>::min()) && (h.total_size_ == 0))
        {
            spdlog::critical("Signed overflow prevented");
            return -1;
        }

        computed_offset = (h.total_size_ == 0) ? offset : h.total_size_ - 1 + offset;
        break;
    default:
        spdlog::critical("Invalid seek mode {}", whence);
        return -1;
    }

    if (computed_offset < 0)
    {
        spdlog::critical("Invalid seek offset {}", computed_offset);
        return -1;
    }
    h.offset_ = computed_offset;
    return 0;
}

const char* driver_getlasterror()
{
    spdlog::debug("getlasterror");

    if (!lastError.empty()) {
        return lastError.c_str();
    }
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

    if (0 == size)
    {
        spdlog::error("Error passing size of 0");
        return -1;
    }

    // confirm stream's presence
    auto to_stream = FindHandle(stream);
    if (to_stream == active_handles.end())
    {
        spdlog::error("Cannot identify stream");
        return -1;
    }

    auto& stream_h = *to_stream;

    if (HandleType::kRead != stream_h->type)
    {
        spdlog::error("Cannot read on not reading stream");
        return -1;
    }

    spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

    MultiPartFile& h = stream_h->GetReader();

    const tOffset offset = h.offset_;

    //fast exit for 0 read
    if (0 == count)
    {
        return 0;
    }

    // prevent overflow
    if (WillSizeCountProductOverflow(size, count))
    {
        return -1;
    }

    tOffset to_read{ static_cast<tOffset>(size * count) };
    if (offset > std::numeric_limits<long long>::max() - to_read)
    {
        spdlog::critical("signed overflow prevented on reading attempt");
        return -1;
    }
    // end of overflow prevention

    // special case: if offset >= total_size, error if not 0 byte required. 0 byte required is already done above
    const tOffset total_size = h.total_size_;
    if (offset >= total_size)
    {
        return -1;
    }

    // normal cases
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

    return ReadBytesInFile(h, reinterpret_cast<char*>(ptr), to_read);
}

long long int driver_fwrite(const void* ptr, size_t size, size_t count, void* stream)
{
    if (!stream)
    {
        spdlog::error("Error passing null stream pointer to fwrite");
        return -1;
    }

    if (!ptr)
    {
        spdlog::error("Error passing null buffer pointer to fwrite");
        return -1;
    }

    if (0 == size)
    {
        spdlog::error("Error passing size 0 to fwrite");
        return -1;
    }

    spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

    auto stream_it = FindHandle(stream);
    ERROR_NO_STREAM(stream_it, -1);
    Handle& stream_h = **stream_it;

    if (HandleType::kWrite != stream_h.type && HandleType::kAppend != stream_h.type)
    {
        spdlog::error("Cannot write on not writing stream");
        return -1;
    }
    
    // fast exit for 0
    if (0 == count)
    {
        return 0;
    }

    // prevent integer overflow
    if (WillSizeCountProductOverflow(size, count))
    {
        return -1;
    }

    const long long to_write = static_cast<long long>(size * count);

    gcs::ObjectWriteStream& writer = stream_h.GetWriter().writer_;
    writer.write(static_cast<const char*>(ptr), to_write);
    if (writer.bad())
    {
        auto last_status = writer.last_status();
        spdlog::error("Error during upload: {} {}", (int)(last_status.code()), last_status.message());
        return -1;
    }
    spdlog::debug("Write status after write: good {}, bad {}, fail {}",
        writer.good(), writer.bad(), writer.fail());

    return to_write;
}

int driver_fflush(void* stream)
{
    if (!stream)
    {
        spdlog::error("Error passing null stream pointer to fflush");
        return -1;
    }

    auto stream_it = FindHandle(stream);
    ERROR_NO_STREAM(stream_it, -1);
    Handle& stream_h = **stream_it;

    if (HandleType::kWrite != stream_h.type)
    {
        spdlog::error("Cannot flush on not writing stream");
        return -1;
    }

    auto& out_stream = stream_h.GetWriter().writer_;
    if (!out_stream.flush())
    {
        spdlog::error("Error during upload");
        return -1;
    }

    return 0;
}

int driver_remove(const char* filename)
{
    if (!filename)
    {
        spdlog::error("Error passing null pointer to remove");
        return kFailure;
    }

    spdlog::debug("remove {}", filename);

    assert(driver_isConnected());

    std::string bucket_name;
    std::string object_name;
    INIT_NAMES_OR_ERROR(filename, bucket_name, object_name, kFailure);

    const auto status = client.DeleteObject(bucket_name, object_name);
    if (!status.ok() && status.code() != gc::StatusCode::kNotFound) {
        spdlog::error("Error deleting object: {} {}", (int)(status.code()), status.message());
        return kFailure;
    }

    return kSuccess;
}

int driver_rmdir(const char* filename)
{
    if (!filename)
    {
        spdlog::error("Error passing null pointer to rmdir");
        return kFailure;
    }

    spdlog::debug("rmdir {}", filename);

    assert(driver_isConnected());
    spdlog::debug("Remove dir (does nothing...)");
    return kSuccess;
}

int driver_mkdir(const char* filename)
{
    if (!filename)
    {
        spdlog::error("Error passing null pointer to mkdir");
        return kFailure;
    }

    spdlog::debug("mkdir {}", filename);

    assert(driver_isConnected());
    return kSuccess;
}

long long int driver_diskFreeSpace(const char* filename)
{
    if (!filename)
    {
        spdlog::error("Error passing null pointer to diskFreeSpace");
        return -1;
    }

    spdlog::debug("diskFreeSpace {}", filename);

    assert(driver_isConnected());
    constexpr long long free_space{ 5LL * 1024LL * 1024LL * 1024LL * 1024LL };
    return free_space;
}

int driver_copyToLocal(const char* sSourceFilePathName, const char* sDestFilePathName)
{
    assert(driver_isConnected());

    if (!sSourceFilePathName || !sDestFilePathName)
    {
        spdlog::error("Error passing null pointer to driver_copyToLocal");
        return kFailure;
    }

    spdlog::debug("copyToLocal {} {}", sSourceFilePathName, sDestFilePathName);

    std::string bucket_name;
    std::string object_name;

    INIT_NAMES_OR_ERROR(sSourceFilePathName, bucket_name, object_name, kFailure);

    auto maybe_reader = MakeReaderPtr(bucket_name, object_name);
    if (!maybe_reader)
    {
        //reader_struct is unusable
        spdlog::error(maybe_reader.status().message());
        return kFailure;
    }

    ReaderPtr reader = std::move(maybe_reader).value();
    const size_t nb_files = reader->filenames_.size();

    // Open the local file
    std::ofstream file_stream(sDestFilePathName, std::ios::binary);
    if (!file_stream.is_open()) {
        spdlog::error("Failed to open local file for writing: {}", sDestFilePathName);
        return kFailure;
    }

    // Allocate a relay buffer
    constexpr size_t buf_size{ 1024 };
    std::array<char, buf_size> buffer{};
    char* buf_data = buffer.data();

    // create a waste buffer now, so the lambdas can reference it
    // memory allocation will occur later, before actual use
    std::vector<char> waste;

    auto read_and_write = [&](gcs::ObjectReadStream& from, bool skip_header = false, std::streamsize header_size = 0) {
        
        if (!from)
        {
            spdlog::error("Error initializing download stream: {} {}", (int)(from.status().code()), from.status().message());
            return false;
        }

        if (skip_header)
        {
            // according to gcs sources, seekg is not implemented
            // waste a read on the first bytes
            if (!from.read(waste.data(), header_size))
            {
                // check failure reasons to give feedback
                if (from.eof())
                {
                    spdlog::error("Error reading header. Shorter header than expected");
                }
                else if (from.bad())
                {
                    spdlog::error("Error reading header. Read failed");
                }
                return false;
            }
        }

        const std::streamsize buf_size_cast = static_cast<std::streamsize>(buf_size);
        while (from.read(buf_data, buf_size_cast) && file_stream.write(buf_data, buf_size_cast)) {}
        // what made the process stop?
        if (!file_stream)
        {
            // something went wrong on write side, abort
            spdlog::error("Error while writing data to local file");
            return false;
        }
        else if (from.eof())
        {
            // short read, copy what remains, if any
            const std::streamsize rem = from.gcount();
            if (rem > 0 && !file_stream.write(buf_data, rem))
            {
                // something went wrong on write side, abort
                spdlog::error("Error while writing data to local file");
                return false;
            }
        }
        else if (from.bad())
        {
            // something went wrong on read side
            spdlog::error("Error while reading from cloud storage");
            return false;
        }

        return true;
        };

    auto operation = [&](gcs::ObjectReadStream& from, const std::string& filename, bool skip_header = false, tOffset header_size = 0) {
        from = client.ReadObject(bucket_name, filename);
        bool res = read_and_write(from, skip_header, header_size);
        from.Close();
        return res;
        };

    auto& filenames = reader->filenames_;
    
    // Read the whole first file
    gcs::ObjectReadStream read_stream;
    if (!operation(read_stream, filenames.front()))
    {
        return kFailure;
    }

    // fast exit
    if (nb_files == 1)
    {
        return kSuccess;
    }

    // Read from the next files
    const tOffset header_size = reader->commonHeaderLength_;
    const bool skip_header = header_size > 0;
    waste.reserve(static_cast<size_t>(header_size));

    for (size_t i = 1; i < nb_files; i++)
    {
        if (!operation(read_stream, filenames[i], skip_header, header_size))
        {
            return kFailure;
        }
    }

    // done copying
    spdlog::debug("Done copying");

    return kSuccess;
}

int driver_copyFromLocal(const char* sSourceFilePathName, const char* sDestFilePathName)
{
    if (!sSourceFilePathName || !sDestFilePathName)
    {
        spdlog::error("Error passing null pointers as arguments to copyFromLocal");
        return kFailure;
    }

    spdlog::debug("copyFromLocal {} {}", sSourceFilePathName, sDestFilePathName);

    assert(driver_isConnected());

    std::string bucket_name;
    std::string object_name;
    INIT_NAMES_OR_ERROR(sDestFilePathName, bucket_name, object_name, kFailure);

    // Open the local file
    std::ifstream file_stream(sSourceFilePathName, std::ios::binary);
    if (!file_stream.is_open()) {
        spdlog::error("Failed to open local file: {}", sSourceFilePathName);
        return kFailure;
    }

    // Create a WriteObject stream
    auto writer = client.WriteObject(bucket_name, object_name);
    if (!writer || !writer.IsOpen()) {
        spdlog::error("Error initializing upload stream: {} {}", (int)(writer.metadata().status().code()), writer.metadata().status().message());
        return kFailure;
    }

    // Read from the local file and write to the GCS object
    constexpr size_t buf_size{ 1024 };
    std::array<char, buf_size> buffer{};
    char* buf_data = buffer.data();

    while (file_stream.read(buf_data, buf_size) && writer.write(buf_data, buf_size)) {}
    // what made the process stop?
    if (!writer)
    {
        spdlog::error("Error while copying to remote storage");
        return kFailure;

    }
    else if (file_stream.eof())
    {
        // copy what remains in the buffer
        const auto rem = file_stream.gcount();
        if (rem > 0 && !writer.write(buf_data, rem))
        {
            spdlog::error("Error while copying to remote storage");
            return kFailure;
        }
    }
    else if (file_stream.bad())
    {
        spdlog::error("Error while reading on local storage");
        return kFailure;
    }

    // Close the GCS WriteObject stream to complete the upload
    writer.Close();

    auto& status = writer.metadata();
    if (!status) {
        spdlog::error("Error during file upload: {} {}", (int)(status.status().code()), status.status().message());

        return kFailure;
    }

    return kSuccess;
}
