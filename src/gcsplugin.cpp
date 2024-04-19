#ifdef __CYGWIN__
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "gcsplugin.h"
#include "google/cloud/storage/client.h"
#include "spdlog/spdlog.h"
#include <assert.h>
#include <fstream>
#include <iostream>

#define str(s) #s
#define VERSION str(0.1.0)

const char* driver_name = "GCS driver";
const char* driver_scheme = "gs";

int bIsConnected = false;
google::cloud::storage::Client client;
// Global bucket name
std::string globalBucketName = "";

typedef long long int tOffset;

struct MultiPartFile
{
    std::string bucketname;
	std::string filename;
	tOffset offset;
    // Added for multifile support
    tOffset commonHeaderLength;
    std::vector<std::string> filenames;
    std::vector<long long int> cumulativeSize;
};

// Definition of helper functions
bool DownloadFileRangeToBuffer(const std::string& bucket_name,
                               const std::string& object_name,
                               char* buffer,
                               std::size_t buffer_length,
                               std::int64_t start_range,
                               std::int64_t end_range) {
    namespace gcs = google::cloud::storage;

    auto reader = client.ReadObject(bucket_name, object_name, gcs::ReadRange(start_range, end_range));
    if (!reader) {
        spdlog::error("Error reading object: {}", reader.status().message());
        return false;
    }

    reader.read(buffer, buffer_length);

    if (reader.bad()/* || reader.fail()*/) {
        spdlog::error("Error during read: {} {}", (int)(reader.status().code()), reader.status().message());
        return false;
    }

    return true;
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

bool ParseGcsUri(const std::string& gcs_uri, std::string& bucket_name, std::string& object_name) {
    const std::string prefix = "gs://";
    if (gcs_uri.compare(0, prefix.size(), prefix) != 0) {
        std::cerr << "Invalid GCS URI: " << gcs_uri << "\n";
        return false;
    }

    std::size_t pos = gcs_uri.find('/', prefix.size());
    if (pos == std::string::npos) {
        std::cerr << "Invalid GCS URI, missing object name: " << gcs_uri << "\n";
        return false;
    }

    bucket_name = gcs_uri.substr(prefix.size(), pos - prefix.size());
    object_name = gcs_uri.substr(pos + 1);

    return true;
}

void FallbackToDefaultBucket(std::string& bucket_name) {
    if (!bucket_name.empty())
        return;
    if (!globalBucketName.empty())
        bucket_name = globalBucketName;
    
    spdlog::critical("No bucket specified, and GCS_BUCKET_NAME is not set!");
}

std::string GetEnvironmentVariableOrDefault(const std::string& variable_name, 
                                            const std::string& default_value)
{
    const char* value = getenv(variable_name.c_str());
    return value ? value : default_value;
}

// Implementation of driver functions

const char *driver_getDriverName()
{
	return driver_name;
}

const char *driver_getVersion()
{
	return VERSION;
}

const char *driver_getScheme()
{
	return driver_scheme;
}

int driver_isReadOnly()
{
	return 0;
}

int driver_connect()
{
    auto loglevel = GetEnvironmentVariableOrDefault("GCS_DRIVER_LOGLEVEL", "info");
    if (loglevel == "debug")
        spdlog::set_level(spdlog::level::debug);
    else if (loglevel == "trace")
        spdlog::set_level(spdlog::level::trace);
    else
        spdlog::set_level(spdlog::level::info);

    spdlog::debug("Connect {}", loglevel);

	// Initialize variables from environment
    globalBucketName = GetEnvironmentVariableOrDefault("GCS_BUCKET_NAME", "");

    namespace gcs = google::cloud::storage;
    auto client_response = gcs::Client::CreateDefaultClient();

    if (!client_response) {
        spdlog::error("Failed to create Storage Client: {}", (int)(client_response.status().code()), client_response.status().message());
        return false;
    }
    client = client_response.value();
    bIsConnected = true;
	return true;
}

int driver_disconnect()
{
	int nRet = 0;
	return nRet == 0;
}

int driver_isConnected()
{
	return bIsConnected;
}

long long int driver_getSystemPreferredBufferSize()
{
	return 4 * 1024 * 1024; // 4 Mo
}

int driver_exist(const char *filename)
{
    spdlog::debug("exist {}", filename);

    std::string file_uri = filename;
    if (file_uri.back() == '/') {
        return driver_dirExists(filename);
    } else {
        return driver_fileExists(filename);
    }
}

int driver_fileExists(const char *sFilePathName)
{
    spdlog::debug("fileExist {}", sFilePathName);

    std::string bucket_name, object_name;
    ParseGcsUri(sFilePathName, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    auto object_metadata = client.GetObjectMetadata(bucket_name, object_name);
    if (object_metadata) {
        spdlog::debug("file {} exists!", sFilePathName);
        return true; // L'objet existe
    } else if (object_metadata.status().code() == google::cloud::StatusCode::kNotFound) {
        return false; // L'objet n'existe pas
    } else {
        spdlog::error("Error checking object: {}", object_metadata.status().message());
        return false; // Une erreur s'est produite lors de la vérification
    }
}

int driver_dirExists(const char *sFilePathName)
{
    spdlog::debug("dirExist {}", sFilePathName);

    return 1;
}

long long int getFileSize(std::string bucket_name, std::string object_name) {
    auto object_metadata = client.GetObjectMetadata(bucket_name, object_name);
    if (object_metadata) {
        return object_metadata->size();
    } else if (object_metadata.status().code() == google::cloud::StatusCode::kNotFound) {
        return -1; // L'objet n'existe pas
    } else {
        spdlog::error("Error checking object: {}", object_metadata.status().message());
        return -1; // Une erreur s'est produite lors de la vérification
    }
}

long long int driver_getFileSize(const char *filename)
{
    spdlog::debug("getFileSize {}", filename);

    std::string bucket_name, object_name;
    ParseGcsUri(filename, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);

    return getFileSize(bucket_name, object_name);
}

void *driver_fopen(const char *filename, char mode)
{
    spdlog::debug("fopen {} {}", filename, mode);

    std::string bucket_name, object_name;
    ParseGcsUri(filename, bucket_name, object_name);
    FallbackToDefaultBucket(bucket_name);
	assert(driver_isConnected());

    auto h = new MultiPartFile;
    h->bucketname = bucket_name;
    h->filename = object_name;
    h->offset = 0;

	switch (mode) {
        case 'r':
        {
            if (false) /* isMultifile(object_name)*/ {
                // TODO
                // identify files part of multifile glob pattern
                // initialize cumulative size array
                // determine if header is repeated
            } else {
                h->filenames.push_back(object_name);
                long long int fileSize = getFileSize(bucket_name, object_name);
                h->cumulativeSize.push_back(fileSize);
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

int driver_fclose(void *stream)
{
    spdlog::debug("fclose {}", (void*)stream);

    MultiPartFile *multiFile;

	assert(driver_isConnected());
	assert(stream != NULL);
	multiFile = (MultiPartFile *)stream;
    delete ((MultiPartFile *)stream);
    return 0;
}

long long int totalSize(MultiPartFile *h) {
    return h->cumulativeSize[h->cumulativeSize.size()-1];
}

int driver_fseek(void *stream, long long int offset, int whence)
{
    spdlog::debug("fseek {} {} {}", stream, offset, whence);

    assert(stream != NULL);
	MultiPartFile *h = (MultiPartFile *)stream;

    switch (whence) {
        case std::ios::beg:
            h->offset = offset;
            return 0;
        case std::ios::cur:
        {
            auto computedOffset = h->offset + offset;
            if (computedOffset < 0) {
                spdlog::critical("Invalid seek offset {}", computedOffset);
                return -1;
            }
            h->offset = computedOffset;
            return 0;
        }
        case std::ios::end:
        {
            auto computedOffset = totalSize(h) + offset;
            if (computedOffset < 0) {
                spdlog::critical("Invalid seek offset {}", computedOffset);
                return -1;
            }
            h->offset = computedOffset;
            return 0;
        }
        default:
            spdlog::critical("Invalid seek mode {}", whence);
            return -1;

    }

}

const char *driver_getlasterror()
{
    spdlog::debug("getlasterror");

    return NULL;
}

long long int driver_fread(void *ptr, size_t size, size_t count, void *stream)
{
    spdlog::debug("fread {} {} {} {}", ptr, size, count, stream);

    assert(stream != NULL);
	MultiPartFile *h = (MultiPartFile *)stream;

    // TODO: handle multifile case
    auto toRead = size * count;
    spdlog::debug("offset = {} toRead = {}", h->offset, toRead);

    DownloadFileRangeToBuffer(h->bucketname, h->filename, (char*)ptr, toRead, h->offset,
        h->offset + toRead);

    h->offset += toRead;
    return toRead;
}

long long int driver_fwrite(const void *ptr, size_t size, size_t count, void *stream)
{
    spdlog::debug("fwrite {} {} {} {}", ptr, size, count, stream);

    assert(stream != NULL);
	MultiPartFile *h = (MultiPartFile *)stream;

    UploadBufferToGcs(h->bucketname, h->filename, (char*)ptr, size*count);
    
    // TODO proper error handling...
    return size*count;
}

int driver_fflush(void *stream)
{
    spdlog::debug("Flushing (does nothing...)");
    return 0;
}

int driver_remove(const char *filename)
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

int driver_rmdir(const char *filename)
{
    spdlog::debug("rmdir {}", filename);

	assert(driver_isConnected());
    spdlog::debug("Remove dir (does nothing...)");
    return 1;
}

int driver_mkdir(const char *filename)
{
    spdlog::debug("mkdir {}", filename);

	assert(driver_isConnected());
    return 1;
}

long long int driver_diskFreeSpace(const char *filename)
{
    spdlog::debug("diskFreeSpace {}", filename);

	assert(driver_isConnected());
    return (long long int)5 * 1024 * 1024 * 1024 * 1024;
}

int driver_copyToLocal(const char *sSourceFilePathName, const char *sDestFilePathName)
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
        } else {
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

int driver_copyFromLocal(const char *sSourceFilePathName, const char *sDestFilePathName)
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


