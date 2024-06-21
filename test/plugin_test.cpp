#include <gtest/gtest.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "path_helper.h"

#if defined(__unix__) || defined(__unix)
	#define __is_unix__
	#define __unix_or_mac__
#elif defined(__APPLE__) && defined(__MACH__)
	#define __is_mac__
	#define __unix_or_mac__
#else 
#define __windows__
#endif

#ifdef __unix_or_mac__
	#include <unistd.h>
	#include <dlfcn.h>
	#include <libgen.h>
	#include <limits.h>
#else
	#include <windows.h>
	#include "errhandlingapi.h"
#endif

#ifdef __is_unix__
#define LIBRARY_NAME "libkhiopsdriver_file_gcs.so"
#elif defined(__is_mac__)
#define LIBRARY_NAME "libkhiopsdriver_file_gcs.dylib"
#else
#define LIBRARY_NAME "khiopsdriver_file_gcs.dll"
#endif

/* API functions definition, that must be defined in the library */
const char *(*ptr_driver_getDriverName)();
const char *(*ptr_driver_getVersion)();
const char *(*ptr_driver_getScheme)();
int (*ptr_driver_isReadOnly)();
int (*ptr_driver_connect)();
int (*ptr_driver_disconnect)();
int (*ptr_driver_isConnected)();
int (*ptr_driver_fileExists)(const char *filename);
int (*ptr_driver_dirExists)(const char *filename);

long long int (*ptr_driver_getFileSize)(const char *filename);
void *(*ptr_driver_fopen)(const char *filename, const char mode);
int (*ptr_driver_fclose)(void *stream);
long long int (*ptr_driver_fread)(void *ptr, size_t size, size_t count, void *stream);
int (*ptr_driver_fseek)(void *stream, long long int offset, int whence);
const char *(*ptr_driver_getlasterror)();

/* API functions definition, that must be defined in the library only if the driver is not read-only */
long long int (*ptr_driver_fwrite)(const void *ptr, size_t size, size_t count, void *stream);
int (*ptr_driver_fflush)(void *stream);
int (*ptr_driver_remove)(const char *filename);
int (*ptr_driver_mkdir)(const char *filename);
int (*ptr_driver_rmdir)(const char *filename);
long long int (*ptr_driver_diskFreeSpace)(const char *filename);

/* API functions definition, that can be defined optionally in the library only if the driver is not read-only */
int (*ptr_driver_copyToLocal)(const char *sourcefilename, const char *destfilename);
int (*ptr_driver_copyFromLocal)(const char *sourcefilename, const char *destfilename);

/* functions prototype */
void *load_shared_library(const char *library_name);
int free_shared_library(void *library_handle);
void *get_shared_library_function(void *library_handle, const char *function_name, int mandatory);

void *init_plugin()
{
	void *library_handle;

	std::string bin_dir = MyPaths::getExecutableDir();
#ifdef __unix_or_mac__
	std::string lib_dir = MyPaths::mergePaths(MyPaths::dirname(bin_dir), std::string("lib"));
	std::string lib_path = MyPaths::mergePaths(lib_dir, LIBRARY_NAME);
#else
	std::string lib_path = LIBRARY_NAME;
#endif

	// DEBUG
	printf("Lib path = %s\n", lib_path.c_str());

	// Try to load the shared library
	library_handle = load_shared_library(lib_path.c_str());


	if (!library_handle)
	{
		fprintf(stderr, "Error while loading library %s", LIBRARY_NAME);
#ifdef __unix_or_mac__
		fprintf(stderr, " (%s). ", dlerror());
		fprintf(stderr, "Check LD_LIBRARY_PATH or set the library with its full path\n");
#else
		fwprintf(stderr, L" (0x%x). ", GetLastError());
		fprintf(stderr, "Check that the library is present in the same folder as the executable\n");
#endif
		exit(EXIT_FAILURE);
	}

	// Bind each mandatory function of the library (and set global_error to 1 in case of errors)
	*(void **)(&ptr_driver_getDriverName) = get_shared_library_function(library_handle, "driver_getDriverName", 1);
	*(void **)(&ptr_driver_getVersion) = get_shared_library_function(library_handle, "driver_getVersion", 1);
	*(void **)(&ptr_driver_getScheme) = get_shared_library_function(library_handle, "driver_getScheme", 1);
	*(void **)(&ptr_driver_isReadOnly) = get_shared_library_function(library_handle, "driver_isReadOnly", 1);
	*(void **)(&ptr_driver_connect) = get_shared_library_function(library_handle, "driver_connect", 1);
	*(void **)(&ptr_driver_disconnect) = get_shared_library_function(library_handle, "driver_disconnect", 1);
	*(void **)(&ptr_driver_isConnected) = get_shared_library_function(library_handle, "driver_isConnected", 1);
	*(void **)(&ptr_driver_fileExists) = get_shared_library_function(library_handle, "driver_fileExists", 1);
	*(void **)(&ptr_driver_dirExists) = get_shared_library_function(library_handle, "driver_dirExists", 1);
	*(void **)(&ptr_driver_getFileSize) = get_shared_library_function(library_handle, "driver_getFileSize", 1);
	*(void **)(&ptr_driver_fopen) = get_shared_library_function(library_handle, "driver_fopen", 1);
	*(void **)(&ptr_driver_fclose) = get_shared_library_function(library_handle, "driver_fclose", 1);
	*(void **)(&ptr_driver_fseek) = get_shared_library_function(library_handle, "driver_fseek", 1);
	*(void **)(&ptr_driver_fread) = get_shared_library_function(library_handle, "driver_fread", 1);
	*(void **)(&ptr_driver_getlasterror) = get_shared_library_function(library_handle, "driver_getlasterror", 1);

	// Bind each read-write function of the library if necessary
	if (!ptr_driver_isReadOnly())
	{
		*(void **)(&ptr_driver_fwrite) = get_shared_library_function(library_handle, "driver_fwrite", 1);
		*(void **)(&ptr_driver_fflush) = get_shared_library_function(library_handle, "driver_fflush", 1);
		*(void **)(&ptr_driver_remove) = get_shared_library_function(library_handle, "driver_remove", 1);
		*(void **)(&ptr_driver_mkdir) = get_shared_library_function(library_handle, "driver_mkdir", 1);
		*(void **)(&ptr_driver_rmdir) = get_shared_library_function(library_handle, "driver_rmdir", 1);
		*(void **)(&ptr_driver_diskFreeSpace) = get_shared_library_function(library_handle, "driver_diskFreeSpace", 1);

		// Bind optional functions (without setting global_error to 1 in case of errors)
		*(void **)(&ptr_driver_copyToLocal) = get_shared_library_function(library_handle, "driver_copyToLocal", 0);
		*(void **)(&ptr_driver_copyFromLocal) = get_shared_library_function(library_handle, "driver_copyFromLocal", 0);
	}

	return library_handle;
}

void deinit_plugin(void *library_handle) {
	free_shared_library(library_handle);
}

void *load_shared_library(const char *library_name)
{
#if defined(__windows__)
	void* handle = (void*)LoadLibrary(library_name);
	return handle;
#elif defined(__unix_or_mac__)
	return dlopen(library_name, RTLD_NOW);
#endif
}

int free_shared_library(void *library_handle)
{
#if defined(__windows__)
	return FreeLibrary((HINSTANCE)library_handle);
#elif defined(__unix_or_mac__)
	return dlclose(library_handle);
#endif
}

void *get_shared_library_function(void *library_handle, const char *function_name, int mandatory)
{
	void *ptr;
#if defined(__windows__)
	ptr = (void *)GetProcAddress((HINSTANCE)library_handle, function_name);
#elif defined(__unix_or_mac__)
	ptr = dlsym(library_handle, function_name);
#endif
	if (ptr == NULL && mandatory)
	{
#ifdef __unix_or_mac__
		fprintf(stderr, "Unable to load %s (%s)\n", function_name, dlerror());
#else
		fprintf(stderr, "Unable to load %s", function_name);
		fwprintf(stderr, L"(0x%x)\n", GetLastError());
#endif
	}
	return ptr;
}


TEST(GCSPluginTest, GetVersion)
{
	auto library_handle = init_plugin();

	ASSERT_STREQ(ptr_driver_getVersion(), "0.1.0");

	deinit_plugin(library_handle);
}

TEST(GCSPluginTest, GetScheme)
{
	auto library_handle = init_plugin();

	ASSERT_STREQ(ptr_driver_getScheme(), "gs");

	deinit_plugin(library_handle);
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);

	auto result = RUN_ALL_TESTS();

	return result;
}
