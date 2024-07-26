#pragma once

#include <memory>
#include <string>
#include <vector>

#include <google/cloud/storage/object_write_stream.h>

#if defined(__unix__) || defined(__unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#define __unix_or_mac__
#else
#define __windows__
#endif

#ifdef __unix_or_mac__ 
#define VISIBLE __attribute__((visibility("default")))
#else
/* Windows Visual C++ only */
#define VISIBLE __declspec(dllexport)
#endif

/* Use of C linkage from C++ */
#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

    VISIBLE void test_setClient(::google::cloud::storage::Client && mock_client);

    VISIBLE void test_unsetClient();

    VISIBLE void* test_getActiveHandles();

    VISIBLE void* test_addReaderHandle(
        const std::string& bucket,
        const std::string& object,
        long long offset,
        long long commonHeaderLength,
        const std::vector<std::string>& filenames,
        const std::vector<long long int>& cumulativeSize,
        long long total_size);

    VISIBLE void* test_addWriterHandle(bool appendMode = false, bool create_with_mock_client = false, std::string bucketname = {}, std::string objectname = {});

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */


namespace gcsplugin
{
    constexpr int kSuccess{ 1 };
    constexpr int kFailure{ 0 };

    constexpr int kFalse{ 0 };
    constexpr int kTrue{ 1 };


    using tOffset = long long;

    struct MultiPartFile
    {
        std::string bucketname_;
        std::string filename_;
        tOffset offset_{ 0 };
        // Added for multifile support
        tOffset commonHeaderLength_{ 0 };
        std::vector<std::string> filenames_;
        std::vector<tOffset> cumulativeSize_;
        tOffset total_size_{ 0 };
    };

    struct WriteFile
    {
        std::string bucketname_;
        std::string filename_;
        google::cloud::storage::ObjectWriteStream writer_;
    };

    using Reader = MultiPartFile;
    using Writer = WriteFile;
    using ReaderPtr = std::unique_ptr<Reader>;
    using WriterPtr = std::unique_ptr<Writer>;


    enum class HandleType { kRead, kWrite, kAppend };


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
            case HandleType::kRead:
                new (&reader) ReaderPtr;
                break;
            case HandleType::kWrite:
            case HandleType::kAppend:
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
            case HandleType::kAppend:
            case HandleType::kWrite: var.writer.~WriterPtr(); break;
            default: break;
            }
        }

        Reader& GetReader() { return *(var.reader); }
        Writer& GetWriter() { return *(var.writer); }
    };

    using HandlePtr = std::unique_ptr<Handle>;
    using HandleContainer = std::vector<HandlePtr>;
    using HandleIt = HandleContainer::iterator;


    bool operator==(const MultiPartFile& op1, const MultiPartFile& op2)
    {
        return (op1.bucketname_ == op2.bucketname_
            && op1.filename_ == op2.filename_
            && op1.offset_ == op2.offset_
            && op1.commonHeaderLength_ == op2.commonHeaderLength_
            && op1.filenames_ == op2.filenames_
            && op1.cumulativeSize_ == op2.cumulativeSize_
            && op1.total_size_ == op2.total_size_);
    }

    bool operator==(const WriteFile& op1, const WriteFile& op2)
    {
        return (op1.bucketname_ == op2.bucketname_
            && op1.filename_ == op2.filename_);
    }
}