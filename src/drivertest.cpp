
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__unix__) || defined(__unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#define __unix_or_mac__
#else
#define __windows__
#endif

#ifdef __unix_or_mac__
#include <unistd.h>
#include <dlfcn.h>
#else
#include <windows.h>
#include "errhandlingapi.h"
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
void usage();
void *load_shared_library(const char *library_name);
int free_shared_library(void *library_handle);
void *get_shared_library_function(void *library_handle, const char *function_name, int mandatory);
void test(const char *file_name_input, const char *file_name_output, const char *file_name_local);
void copyFile(const char *file_name_input, const char *file_name_output);
void copyFileWithFseek(const char *file_name_input, const char *file_name_output);
void copyFileWithAppend(const char *file_name_input, const char *file_name_output);
void removeFile(const char *filename);
void compareSize(const char *file_name_output, long long int filesize);

/* error indicator in case of error */
int global_error = 0;

/* default size of buffer passed to driver */
int nBufferSize = 512 * 1024;

int main(int argc, char *argv[])
{
	void *library_handle;
	if (!((argc == 5) || ((argc == 7) && (strcmp(argv[1], "-b") == 0))))
		usage();

	if (argc == 7)
	{
		nBufferSize = atoi(argv[2]);
		printf("Set buffersize to %d bytes\n", nBufferSize);
		argv = argv + 2;
	}

	// Try to load the shared library
	library_handle = load_shared_library(argv[1]);
	if (!library_handle)
	{
		fprintf(stderr, "Error while loading library %s", argv[1]);
#ifdef __unix_or_mac__
		fprintf(stderr, " (%s). ", dlerror());
#else
		fwprintf(stderr, L" (0x%x). ", GetLastError());
#endif
		fprintf(stderr, "Check LD_LIBRARY_PATH or set the library with its full path\n");
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
	if (!global_error && !ptr_driver_isReadOnly())
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

	if (!global_error)
	{
		// Call 2 simple functions
		printf("Test of the driver library '%s' version %s\n", ptr_driver_getDriverName(), ptr_driver_getVersion());

		// Connection to the file system
		bool bIsconnected = ptr_driver_connect();
		if (bIsconnected)
		{
			if (!ptr_driver_isConnected())
			{
				global_error = 1;
				fprintf(stderr, "ERROR : connection is done but driver is not connected\n");
			}
			if (!ptr_driver_fileExists(argv[2]))
			{
				fprintf(stderr, "ERROR : %s is missing\n", argv[2]);
				global_error = 1;
			}
			// The real test begins here (global_error is set to 1 in case of errors)
			if (!global_error)
				test(argv[2], argv[3], argv[4]);
			ptr_driver_disconnect();
		}
		else
		{
			global_error = 1;
			fprintf(stderr, "ERROR : unable to connect to the file system\n");
		}
	}

	free_shared_library(library_handle);
	if (global_error)
	{
		printf("Test has failed\n");
		exit(EXIT_FAILURE);
	}
	printf("! Test is successful !\n");
	exit(EXIT_SUCCESS);
}

/* functions definitions */

void usage()
{
	printf("Usage : khiopsdrivertest [-b buffersize] libraryname input_filename output_filename local_filename\n");
	printf("example : khiopsdrivertest libhdfsdriver.so hdfs:///input_filename hdfs:///output_filename /tmp/copy.txt\n");
	exit(EXIT_FAILURE);
}

void test(const char *file_name_input, const char *file_name_output, const char *file_name_local)
{
	// Basic information of the scheme
	printf("scheme: %s\n", ptr_driver_getScheme());
	printf("is read-only: %d\n", ptr_driver_isReadOnly());

	// Checks size of input file
	long long int filesize = ptr_driver_getFileSize(file_name_input);
	printf("size of %s is %lld\n", file_name_input, filesize);

	if (ptr_driver_fileExists(file_name_input) == 1)
		printf("%s exists\n", file_name_input);
	else
	{
		printf("%s is missing, abort\n", file_name_input);
		global_error = 1;
		return;
	}

	// Show small extract if the driver is read-only
	if (ptr_driver_isReadOnly())
	{
		// Reads the file by steps of nBufferSize and writes to the output file at each step
		char *buffer = new char[nBufferSize + 1];
		long long int sizeRead = nBufferSize;
		const long long int maxTotalRead = 3000;
		long long int totalRead = 0;

		void *file = ptr_driver_fopen(file_name_input, 'r');
		if (file == NULL)
		{
			printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
			global_error = 1;
			return;
		}
		while (sizeRead == nBufferSize && !global_error)
		{
			sizeRead = ptr_driver_fread(&buffer, sizeof(char), nBufferSize, file);
			if (sizeRead == 0)
			{
				global_error = 1;
				printf("error while reading %s : %s\n", file_name_input, ptr_driver_getlasterror());
			}
			else
			{
				buffer[sizeRead] = '\0';
				printf("%s", buffer);
				totalRead += sizeRead;

				// Stop when limit is reached
				if (totalRead > maxTotalRead)
				{
					printf("\n");
					break;
				}
			}
		}
		ptr_driver_fclose(file);
		delete[](buffer);
	}
	// Opens for write if the driver is not read-only
	else
	{

		// Test copying files
		printf("Copy %s to %s\n", file_name_input, file_name_output);
		copyFile(file_name_input, file_name_output);
		if (!global_error)
		{
			compareSize(file_name_output, filesize);
			removeFile(file_name_output);
		}

		// Test copying files with fseek
		printf("Copy with fseek %s to %s ...\n", file_name_input, file_name_output);
		copyFileWithFseek(file_name_input, file_name_output);
		if (!global_error)
		{
			compareSize(file_name_output, filesize);
			removeFile(file_name_output);
		}
		
		// Test copying files with append
		printf("Copy with append %s to %s ...\n", file_name_input, file_name_output);
		copyFileWithAppend(file_name_input, file_name_output);
		if (!global_error)
		{
			compareSize(file_name_output, filesize);
			removeFile(file_name_output);
		}

		// Copy to local if this optional function is available in the librairy
		if (!global_error && ptr_driver_copyToLocal != NULL)
		{
			printf("Copy to local %s to %s ...\n", file_name_input, file_name_local);
			global_error = ptr_driver_copyToLocal(file_name_input, file_name_local) == 0;
			if (global_error)
				printf("Error while copying : %s\n", ptr_driver_getlasterror());
			else
				printf("copy %s to local is done\n", file_name_input);
		}

		// Copy from local if this optional function is available in the librairy
		if (!global_error && ptr_driver_copyFromLocal != NULL)
		{
			printf("Copy from local %s to %s ...\n", file_name_local, file_name_output);
			global_error = ptr_driver_copyFromLocal(file_name_local, file_name_output) == 0;
			if (global_error)
				printf("Error while copying : %s\n", ptr_driver_getlasterror());
			else
				printf("copy %s from local is done\n", file_name_local);
			if (!ptr_driver_fileExists(file_name_output))
			{
				printf("%s is missing !\n", file_name_output);
				global_error = 1;
			}
		}
	}
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
		global_error = 1;
#ifdef __unix_or_mac__
		fprintf(stderr, "Unable to load %s (%s)\n", function_name, dlerror());
#else
		fprintf(stderr, "Unable to load %s", function_name);
		fwprintf(stderr, L"(0x%x)\n", GetLastError());
#endif
	}
	return ptr;
}

// Copy file_name_input to file_name_output by steps of 1Kb
void copyFile(const char *file_name_input, const char *file_name_output)
{

	// Opens for read
	void *fileinput = ptr_driver_fopen(file_name_input, 'r');
	if (fileinput == NULL)
	{
		printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
		global_error = 1;
		return;
	}

	void *fileoutput = ptr_driver_fopen(file_name_output, 'w');
	if (fileoutput == NULL)
	{
		printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
		global_error = 1;
	}
	if (!global_error)
	{
		// Reads the file by steps of nBufferSize and writes to the output file at each step
		char *buffer = new char[nBufferSize];
		long long int sizeRead = nBufferSize;
		long long int sizeWrite;
		ptr_driver_fseek(fileinput, 0, SEEK_SET);
		while (sizeRead == nBufferSize && !global_error)
		{
			sizeRead = ptr_driver_fread(buffer, sizeof(char), nBufferSize, fileinput);
			if (sizeRead == -1)
			{
				global_error = 1;
				printf("error while reading %s : %s\n", file_name_input, ptr_driver_getlasterror());
			}
			else
			{
				sizeWrite = ptr_driver_fwrite(buffer, sizeof(char), (size_t)sizeRead, fileoutput);
				if (sizeWrite == -1)
				{
					global_error = 1;
					printf("error while writing %s : %s\n", file_name_output, ptr_driver_getlasterror());
				}
			}
		}
		ptr_driver_fclose(fileoutput);
		delete[](buffer);
	}
	ptr_driver_fclose(fileinput);
}

// Copy file_name_input to file_name_output by steps of 1Kb by using fseek before each read
void copyFileWithFseek(const char *file_name_input, const char *file_name_output)
{
	// Opens for read
	void *fileinput = ptr_driver_fopen(file_name_input, 'r');
	if (fileinput == NULL)
	{
		printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
		global_error = 1;
		return;
	}

	void *fileoutput = ptr_driver_fopen(file_name_output, 'w');
	if (fileoutput == NULL)
	{
		printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
		global_error = 1;
	}

	if (!global_error)
	{
		// Reads the file by steps of nBufferSize and writes to the output file at each step
		char *buffer = new char[nBufferSize];
		long long int sizeRead = nBufferSize;
		long long int sizeWrite;
		int cummulativeRead = 0;
		ptr_driver_fseek(fileinput, 0, SEEK_SET);
		while (sizeRead == nBufferSize && !global_error)
		{
			ptr_driver_fseek(fileinput, cummulativeRead, SEEK_SET);
			sizeRead = ptr_driver_fread(buffer, sizeof(char), nBufferSize, fileinput);
			cummulativeRead += sizeRead;
			if (sizeRead == -1)
			{
				global_error = 1;
				printf("error while reading %s : %s\n", file_name_input, ptr_driver_getlasterror());
			}
			else
			{
				sizeWrite = ptr_driver_fwrite(buffer, sizeof(char), (size_t)sizeRead, fileoutput);
				if (sizeWrite == -1)
				{
					global_error = 1;
					printf("error while writing %s : %s\n", file_name_output, ptr_driver_getlasterror());
				}
			}
		}
		ptr_driver_fclose(fileoutput);
		delete[](buffer);
	}
	ptr_driver_fclose(fileinput);
}

// Copy file_name_input to file_name_output by steps of 1Kb by using append mode
void copyFileWithAppend(const char *file_name_input, const char *file_name_output)
{
	// Make sure output file doesn't exist
	ptr_driver_remove(file_name_output);

	// Opens for read
	void *fileinput = ptr_driver_fopen(file_name_input, 'r');
	if (fileinput == NULL)
	{
		printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
		global_error = 1;
		return;
	}

	if (!global_error)
	{
		// Reads the file by steps of nBufferSize and writes to the output file at each step
		char *buffer = new char[nBufferSize];
		long long int sizeRead = nBufferSize;
		long long int sizeWrite;
		ptr_driver_fseek(fileinput, 0, SEEK_SET);
		while (sizeRead == nBufferSize && !global_error)
		{
			sizeRead = ptr_driver_fread(buffer, sizeof(char), nBufferSize, fileinput);
			if (sizeRead == -1)
			{
				global_error = 1;
				printf("error while reading %s : %s\n", file_name_input, ptr_driver_getlasterror());
			}
			else
			{
				void *fileoutput = ptr_driver_fopen(file_name_output, 'a');
				if (fileoutput == NULL)
				{
					printf("error : %s : %s\n", file_name_input, ptr_driver_getlasterror());
					global_error = 1;
				}

				sizeWrite = ptr_driver_fwrite(buffer, sizeof(char), (size_t)sizeRead, fileoutput);
				if (sizeWrite == -1)
				{
					global_error = 1;
					printf("error while writing %s : %s\n", file_name_output, ptr_driver_getlasterror());
				}

				ptr_driver_fclose(fileoutput);
			}
		}
		delete[](buffer);
	}
	ptr_driver_fclose(fileinput);
}

void removeFile(const char *filename)
{
	global_error = ptr_driver_remove(filename) == 0;
	if (global_error)
		printf("Error while removing : %s\n", ptr_driver_getlasterror());
	if (ptr_driver_fileExists(filename))
	{
		printf("%s should be removed !\n", filename);
		global_error = 1;
	}
}

void compareSize(const char *file_name_output, long long int filesize)
{
	long long int filesize_output = ptr_driver_getFileSize(file_name_output);
	printf("size of %s is %lld\n", file_name_output, filesize_output);
	if (filesize_output != filesize)
	{
		printf("Sizes of input and output are different\n");
		global_error = 1;
	}
	if (ptr_driver_fileExists(file_name_output))
	{
		printf("%s exists\n", file_name_output);
	}
	else
	{
		printf("something's wrong : %s is missing\n", file_name_output);
		global_error = 1;
	}
}
