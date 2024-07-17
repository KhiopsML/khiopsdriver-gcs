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
#include <limits>
#include <memory>

#include <limits.h>
#include <google/cloud/storage/object_write_stream.h>

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

constexpr int kSuccess{ 1 };
constexpr int kFailure{ 0 };

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

struct WriteFile
{
    std::string bucketname_;
    std::string filename_;
    gcs::ObjectWriteStream writer_;
};

enum class HandleType { kRead, kWrite, kAppend };

using Reader = MultiPartFile;
using Writer = WriteFile;
using ReaderPtr = std::unique_ptr<Reader>;
using WriterPtr = std::unique_ptr<Writer>;

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
            new (&reader) ReaderPtr;
            break;
        case HandleType::kAppend:
        case HandleType::kRead:
        default:
            new (&writer) WriterPtr;
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

    Reader& Reader() { return *(var.reader); }
    Writer& Writer() { return *(var.writer); }
};

using HandlePtr = std::unique_ptr<Handle>;

//TODO would be nice to have a generic version, but cannot find the way in C++11
// 
// 

//HandlePtr MakeHandlePtrFromReader(ReaderPtr&& reader_ptr)
//{
//    HandlePtr h{ new Handle(HandleType::kRead) };
//    h->var.reader = std::move(reader_ptr);
//    return h;
//}
//
//HandlePtr MakeHandlePtrFromWriter(WriterPtr&& writer_ptr)
//{
//    std::unique_ptr<Handle> h{ new Handle(HandleType::kWrite) };
//    h->var.writer = std::move(writer_ptr);
//    return h;
//}


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


using HandleContainer = std::vector<HandlePtr>;
using HandleIt = HandleContainer::iterator;

HandleContainer active_handles;


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

void EraseRemove(HandleIt pos)// void* handle)//std::vector<std::unique_ptr<Handle>>::iterator pos)
{
    /*auto to_erase = FindHandle(handle);
    if (to_erase == active_handles.end())
    {
        return kFailure;
    }*/
    *pos = std::move(active_handles.back());
    active_handles.pop_back();
}

// Definition of helper functions
long long int DownloadFileRangeToBuffer(const std::string& bucket_name,
    const std::string& object_name,
    char* buffer,
    //std::size_t buffer_length,
    std::int64_t start_range,
    std::int64_t end_range)
{
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

bool ParseGcsUri(const std::string& gcs_uri, std::string& bucket_name, std::string& object_name)
{
    char const* prefix = "gs://";
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

void* test_addWriterHandle()
{
    WriterPtr writer_ptr{ new WriteFile };
    return InsertHandle<WriterPtr, HandleType::kWrite>(std::move(writer_ptr));
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
    /*GetBucketAndObjectNames(sFilePathName, bucket_name, object_name);

    if (bucket_name.empty() || object_name.empty())
    {
        return kFalse;
    }*/

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

    INIT_NAMES_OR_ERROR(filename, bucket_name, object_name, -1);

    /*GetBucketAndObjectNames(filename, bucket_name, object_name);
    ERROR_NO_NAME(bucket_name, objectn_name, -1);*/

    return GetFileSize(bucket_name, object_name);
}

gc::StatusOr<ReaderPtr> MakeReaderPtr(const std::string& bucketname, const std::string& objectname)
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
            bucketname,
            objectname,
            0,
            0,
            std::move(filenames),
            std::move(cumulativeSize),
            total_size
        });
        /*h.total_size_ = h.cumulativeSize_[0];
        return kSuccess;*/
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
        //auto last_size_it = h.cumulativeSize_.rbegin();
        //const auto size_before_last_it = last_size_it + 1;
        //*last_size_it += *size_before_last_it;

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
                //*last_size_it -= header_size;
            }
        }
    }
    
    tOffset total_size = cumulativeSize.back();

    return ReaderPtr(new MultiPartFile{
        bucketname,
        objectname,
        0,
        same_header ? header_size : 0,
        std::move(filenames),
        std::move(cumulativeSize),
        total_size
    });
}

//int AccumulateNamesAndSizes(MultiPartFile& h)
//{
//    auto push_back_data = [](MultiPartFile& h, gcs::ListObjectsIterator& list_it) {
//        h.filenames_.push_back((*list_it)->name());
//        h.cumulativeSize_.push_back(static_cast<long long>((*list_it)->size()));
//        };
//
//    const std::string& bucket_name{ h.bucketname_ };
//    auto list = client.ListObjects(bucket_name, gcs::MatchGlob{ h.filename_ });
//    auto list_it = list.begin();
//    const auto list_end = list.end();
//
//    if (list_end == list_it)
//    {
//        //no file, or not a file
//        return kFailure;
//    }
//
//    if (!(*list_it))
//    {
//        //unusable data
//        return kFailure;
//    }
//
//    push_back_data(h, list_it);
//
//    list_it++;
//    if (list_end == list_it)
//    {
//        //unique file
//        h.total_size_ = h.cumulativeSize_[0];
//        return kSuccess;
//    }
//
//    // multifile
//    // check headers
//    const std::string header = ReadHeader(bucket_name, h.filenames_[0]);
//    if (header.empty())
//    {
//        return kFailure;
//    }
//    const long long header_size = static_cast<long long>(header.size());
//    bool same_header{ true };
//
//    for (; list_it != list.end(); list_it++)
//    {
//        if (!(*list_it))
//        {
//            //unusable data
//            return kFailure;
//        }
//
//        push_back_data(h, list_it);
//        auto last_size_it = h.cumulativeSize_.rbegin();
//        const auto size_before_last_it = last_size_it + 1;
//        *last_size_it += *size_before_last_it;
//
//        if (same_header)
//        {
//            const std::string curr_header = ReadHeader(bucket_name, *(h.filenames_.crbegin()));
//            if (curr_header.empty())
//            {
//                return kFailure;
//            }
//            same_header = (header == curr_header);
//            if (same_header)
//            {
//                *last_size_it -= header_size;
//            }
//        }
//    }
//    if (same_header) {
//       h.commonHeaderLength_ = header_size;
//    }
//
//    h.total_size_ = *(h.cumulativeSize_.rbegin());
//
//    return kSuccess;
//}

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

    /*GetBucketAndObjectNames(filename, bucketname, objectname);

    if (bucketname.empty() || objectname.empty())
    {
        spdlog::error("Error parsing URL.");
        return nullptr;
    }*/

    switch (mode) {
    case 'r':
    {
        auto reader_struct = MakeReaderPtr(bucketname, objectname);
        if (!reader_struct)
        {
            //reader_struct is unusable
            spdlog::error(reader_struct.status().message());
            return nullptr;
        }

        return InsertHandle<ReaderPtr, HandleType::kRead>(std::move(reader_struct).value());

        /*ReaderPtr reader_struct{ new MultiPartFile };
        reader_struct->bucketname_ = std::move(bucketname);
        reader_struct->filename_ = std::move(objectname);
        if (kFailure == AccumulateNamesAndSizes(*reader_struct))
        {
            return nullptr;
        }

        return InsertHandle<ReaderPtr, HandleType::kRead>(std::move(reader_struct));*/
    }
    case 'w':
    {
        auto writer = client.WriteObject(bucketname, objectname);
        if (!writer) {
            spdlog::error("Error initializing write stream");// : {} {}", (int)(writer.metadata().status().code()), writer.metadata().status().message());
            return nullptr;
        }
        WriterPtr writer_struct{ new WriteFile };
        writer_struct->bucketname_ = std::move(bucketname);
        writer_struct->filename_ = std::move(objectname);
        writer_struct->writer_ = std::move(writer);
        return InsertHandle<WriterPtr, HandleType::kWrite>(std::move(writer_struct));
    }
    case 'a':

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

    if (HandleType::kWrite == h_ptr->type)
    {
        //close the stream to flush all remaining bytes in the put area
        auto& writer = h_ptr->Writer().writer_;
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

    MultiPartFile& h = stream_h->Reader();

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

    MultiPartFile& h = stream_h->Reader();

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

    // Start at first usable file chunk
    // Advance through file chunks, advancing buffer pointer
    // Until last requested byte was read
    // Or error occured

    tOffset bytes_read{ 0 };

    // Lookup item containing initial bytes at requested offset
    const auto& cumul_sizes = h.cumulativeSize_;
    const tOffset common_header_length = h.commonHeaderLength_;
    const std::string& bucket_name = h.bucketname_;
    const auto& filenames = h.filenames_;
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

    h.offset_ += bytes_read;
    return bytes_read;
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

    if (HandleType::kWrite != stream_h.type)
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

    gcs::ObjectWriteStream& writer = stream_h.Writer().writer_;
    writer.write(static_cast<const char*>(ptr), to_write);
    if (writer.bad())
    {
        auto last_status = writer.last_status();
        spdlog::error("Error during upload: {} {}", (int)(last_status.code()), last_status.message());
        return -1;
    }
    spdlog::debug("Write status after write: good {}, bad {}, fail {}, goodbit {}",
        writer.good(), writer.bad(), writer.fail(), writer.goodbit);

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

    auto& out_stream = stream_h.Writer().writer_;
    if (out_stream.flush().bad())
    {
        spdlog::error("Error during upload");
        return -1;
    }

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
        return kFailure;
    }

    return kSuccess;
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

    /*ParseGcsUri(sSourceFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);*/

    auto maybe_reader = MakeReaderPtr(bucket_name, object_name);
    if (!maybe_reader)
    {
        //reader_struct is unusable
        spdlog::error(maybe_reader.status().message());
        return kFailure;
    }

    /*ReaderPtr reader_struct{ new MultiPartFile };
    reader_struct->bucketname_ = bucket_name;
    reader_struct->filename_ = object_name;
    if (kFailure == AccumulateNamesAndSizes(*reader_struct))
    {
        return kFailure;
    }*/

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

    auto write_to_file = [&](std::streamsize count) {
        if (!file_stream.write(buf_data, count))
        {
            // something went wrong, abort
            spdlog::error("Error while writing data to local file");
            return false;
        }
        // reset buffer for further use
        buffer.fill('\0');
        return true;
        };

    auto read_and_write = [&](gcs::ObjectReadStream& from, bool skip_header = false, std::streamsize header_size = 0) {
        
        if (!from.IsOpen() || !from)
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
        while (from.read(buf_data, buf_size_cast))
        {
            // buf_size bytes were read
            if (!write_to_file(buf_size_cast))
            {
                return false;
            }
        }
        // what made read stop?
        if (from.eof())
        {
            // short read, copy what remains, if any
            const std::streamsize rem = from.gcount();
            if (rem > 0 && !write_to_file(rem))
            {
                return false;
            }
        }
        else if (from.bad())
        {
            // abort
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

    //    // Open the read stream
    //    reader_stream = client.ReadObject(bucket_name, filename);
    //    if (!reader_stream.IsOpen() || !reader_stream)
    //    {
    //        spdlog::error("Error initializing download stream: {} {}", (int)(reader_stream.status().code()), reader_stream.status().message());
    //        return kFailure;
    //    }

    //    // read while taking offset of common header into account
    //}

    //int status = kSuccess;


    //for (size_t i = 0; i < nb_files; i++) {
    //    const char* filename = reader->filenames_[i].data();
    //    spdlog::debug("copyToLocal processing file {} = {}", i, filename);

    //    // Create a ReadObject stream
    //    auto reader_stream = client.ReadObject(bucket_name, filename);
    //    if (!reader_stream) {
    //        spdlog::error("Error initializing download stream: {} {}", (int)(reader_stream.status().code()), reader_stream.status().message());
    //        status = kFailure;
    //        break;
    //    }

    //    // Read from the GCS object and write to the local file
    //    //constexpr size_t buf_size{ 1024 };
    //    //std::vector<char> buffer(buf_size, '\0');
    //    //char* buf_data = buffer.data();     // size is fixed, the vector won't be reallocated
    //    //std::string buffer(1024, '\0');
    //    spdlog::debug("Start reading {}", buffer);

    //    // Read the full first file
    //    while (reader_stream.read(buf_data, buf_size)) {}

    //    if (reader_stream.bad())

    //    bool complete = false;
    //    bool headerlineSkipped = false;
    //    const tOffset header_length{ reader->commonHeaderLength_ };

    //    for(;;) {
    //        reader_stream.read(buffer.data(), buffer.size());

    //        if (reader_stream.bad()) {
    //            spdlog::error("Error during read: {} {}", (int)(reader_stream.status().code()), reader_stream.status().message());
    //            break;
    //        }

    //        const auto extract = reader_stream.gcount();
    //        spdlog::debug("Read {}", extract);

    //        if (extract == 0)
    //        {
    //            break;
    //        }

    //        int offset = 0;
    //        std::streamsize num_bytes = extract;
    //        if (i > 0 && !headerlineSkipped && header_length > 0) {
    //            // TODO: check bounds!
    //            spdlog::debug("Skipping initial {} bytes", header_length);
    //            offset = header_length;
    //            num_bytes -= header_length;
    //            headerlineSkipped = true;
    //        }
    //        file_stream.write(buffer.data()+offset, num_bytes);
    //    }

    //}
    //spdlog::debug("Close output");
    //file_stream.close();

    //spdlog::debug("Done copying");

    //return status;
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
        return kFailure;
    }

    // Create a WriteObject stream
    auto writer = client.WriteObject(bucket_name, object_name);
    if (!writer) {
        spdlog::error("Error initializing upload stream: {} {}", (int)(writer.metadata().status().code()), writer.metadata().status().message());
        return kFailure;
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

        return kFailure;
    }

    return kSuccess;
}