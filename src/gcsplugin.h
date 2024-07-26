#pragma once
#include <sys/types.h>
#include <cstdlib>

#include <memory>
#include "google/cloud/storage/client.h"

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

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Several functions defined below have the same semantic as those defined in the ANSI C API.
	// Obviously, driver_fopen corresponds to fopen in the C ANSI and so on for driver_fclose, driver_fread, driver_fread, driver_fwrite....
	// These methods are not well documented here, please refer to the C library. However, "driver_" methods sometimes differ from
	// the ANSI C methods by their return values.

	// Name of the driver, used for messages or errors only
	VISIBLE const char *driver_getDriverName();

	// Version number of the driver
	// format is N.N.N
	VISIBLE const char *driver_getVersion();

	// Returns the scheme ("hdfs") that prefixes the URI managed by the driver
	VISIBLE const char *driver_getScheme();

	// Returns 1 if the driver implements only read-only methods, 0 otherwise
	VISIBLE int driver_isReadOnly();

	// Connection to the filesystem
	// Returns 1 on success, 0 on error
	VISIBLE int driver_connect();

	// Disconnection from the filesystem
	// Returns 1 on success, 0 on error
	VISIBLE int driver_disconnect();

	// Returns 1 if the driver is connected
	VISIBLE int driver_isConnected();

	// Returns the buffer size (in Bytes) to use when accessing I/O
	// This is a recommendation: when possible, Khiops will use this preferred size or a
	// multiple but in extrem cases, Khiops may use less (but always more than 1 Mb)
	VISIBLE long long int driver_getSystemPreferredBufferSize();

	///////////////////////////////////////////////////////////////////////////////////
	// The following read-only functions are mandatory and they need to be implemented

	// Returns 1 if the file exists, 0 otherwise
	// DEPRECATED see driver_FileExists and driver_DirExists
	VISIBLE int driver_exist(const char *filename);

	// Returns 1 if the file exists, 0 otherwise
	VISIBLE int driver_fileExists(const char *sFilePathName);

	// Returns 1 if the directory exists, 0 otherwise
	VISIBLE int driver_dirExists(const char *sFilePathName);

	// Returns the size of the file, -1 on error
	// Supports files larger than 4 Gb
	VISIBLE long long int driver_getFileSize(const char *filename);

	// Open the file whose name is specified in the parameter filename and returns a stream.
	// The filename must begin with scheme:///path_to_the_file.
	// The file is open as a binary file and can be larger than 4 Gb.
	// The parameter mode contains the file access mode. The supported modes are 'r', 'w' and 'a'.
	// If the driver is read-only, only the 'r' mode needs to be implemented
	// If the file is successfully opened, the function returns a pointer to an object that can be used to identify the stream on future operations.
	// Otherwise, a null pointer is returned and the method driver_getlasterror() returns the error message.
	VISIBLE void *driver_fopen(const char *filename, char mode);

	// Returns 0 on success, EOF on error
	VISIBLE int driver_fclose(void *stream);

	// Returns  the total number of elements read on success, -1 on error.
	// If EOF is reached, the number of elements read is less than size*count
	// Note that the return type is long long int rather than size_t in order to manage the -1 value
	VISIBLE long long int driver_fread(void *ptr, size_t size, size_t count, void *stream);

	// Returns 0 on success, -1 on error
	// Supports files larger than 4 Gb
	VISIBLE int driver_fseek(void *stream, long long int offset, int whence);

	// Returns the last error encountered on the file system
	VISIBLE const char *driver_getlasterror();

	///////////////////////////////////////////////////////////////////////////////////
	// The following write functions are mandatory only is the driver is not read-only.
	// They are ignored otherwise, even if they are implemented

	// The number of elements written is returns in case of success, otherwise the function returns -1
	// Note that the return type is long long int rather than size_t in order to manage the -1 value
	VISIBLE long long int driver_fwrite(const void *ptr, size_t size, size_t count, void *stream);

	// Returns 0 on success, -1 on error.
	VISIBLE int driver_fflush(void *stream);

	// Returns 1 in case of success, 0 otherwise
	VISIBLE int driver_remove(const char *filename);

	// Returns 1 in case of success, 0 otherwise
	VISIBLE int driver_mkdir(const char *pathname);

	// Returns 1 in case of success, 0 otherwise
	VISIBLE int driver_rmdir(const char *pathname);

	// Returns the available space, -1 on error
	VISIBLE long long int driver_diskFreeSpace(const char *filename);

	///////////////////////////////////////////////////////////////////////////////////
	// The following functions are optional and may not be implemented

	// Copy sourcefilename which is on the current file system (s3, hdfs..) to the local file system
	// Returns 1 on success, 0 on error
	VISIBLE int driver_copyToLocal(const char *sourcefilename, const char *destfilename);

	// Copy sourcefilename which is on the local file system to the current file system (s3, hdfs..)
	// If the driver is read-only, this function is ignored even if it is implemented
	// Returns 1 on success, 0 on error
	VISIBLE int driver_copyFromLocal(const char *sourcefilename, const char *destfilename);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
