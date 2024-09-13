
#include <cassert>
#include <fmt/core.h>
#include <iostream>

#if defined(__unix__) || defined(__unix) ||                                    \
    (defined(__APPLE__) && defined(__MACH__))
#define __unix_or_mac__
#else
#define __windows__
#endif

#ifdef __unix_or_mac__
#include <dlfcn.h>
#include <unistd.h>
#else
#include "errhandlingapi.h"
#include <windows.h>
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
long long int (*ptr_driver_fread)(void *ptr, size_t size, size_t count,
                                  void *stream);
int (*ptr_driver_fseek)(void *stream, long long int offset, int whence);
const char *(*ptr_driver_getlasterror)();

/* API functions definition, that must be defined in the library only if the
 * driver is not read-only */
long long int (*ptr_driver_fwrite)(const void *ptr, size_t size, size_t count,
                                   void *stream);
int (*ptr_driver_fflush)(void *stream);
int (*ptr_driver_remove)(const char *filename);
int (*ptr_driver_mkdir)(const char *filename);
int (*ptr_driver_rmdir)(const char *filename);
long long int (*ptr_driver_diskFreeSpace)(const char *filename);

/* API functions definition, that can be defined optionally in the library only
 * if the driver is not read-only */
int (*ptr_driver_copyToLocal)(const char *sourcefilename,
                              const char *destfilename);
int (*ptr_driver_copyFromLocal)(const char *sourcefilename,
                                const char *destfilename);

/* functions prototype */
void usage();
void *load_shared_library(const char *library_name);
int free_shared_library(void *library_handle);
void *get_shared_library_function(void *library_handle,
                                  const char *function_name, int mandatory);
void do_test(const char *file_name_input, const char *file_name_output,
             const char *file_name_local);

/* error indicator in case of error */
int global_error = 0;

int main(int argc, char *argv[]) {
  void *library_handle;
  if (argc != 5)
    usage();

  // Try to load the shared library
  library_handle = load_shared_library(argv[1]);
  if (!library_handle) {
    std::cerr << fmt::format("Error while loading library {}", argv[1]);
#ifdef __unix_or_mac__
    std::cerr << fmt::format(" ({}).\n", dlerror());
#else
    std::cerr << fmt::format(" ({}).\n", GetLastError());
#endif
    std::cerr
        << "Check LD_LIBRARY_PATH or set the library with its full path\n";
    exit(EXIT_FAILURE);
  }

  // Bind each mandatory function of the library (and set global_error to 1 in
  // case of errors)
  *(void **)(&ptr_driver_getDriverName) =
      get_shared_library_function(library_handle, "driver_getDriverName", 1);
  *(void **)(&ptr_driver_getVersion) =
      get_shared_library_function(library_handle, "driver_getVersion", 1);
  *(void **)(&ptr_driver_getScheme) =
      get_shared_library_function(library_handle, "driver_getScheme", 1);
  *(void **)(&ptr_driver_isReadOnly) =
      get_shared_library_function(library_handle, "driver_isReadOnly", 1);
  *(void **)(&ptr_driver_connect) =
      get_shared_library_function(library_handle, "driver_connect", 1);
  *(void **)(&ptr_driver_disconnect) =
      get_shared_library_function(library_handle, "driver_disconnect", 1);
  *(void **)(&ptr_driver_isConnected) =
      get_shared_library_function(library_handle, "driver_isConnected", 1);
  *(void **)(&ptr_driver_fileExists) =
      get_shared_library_function(library_handle, "driver_fileExists", 1);
  *(void **)(&ptr_driver_dirExists) =
      get_shared_library_function(library_handle, "driver_dirExists", 1);
  *(void **)(&ptr_driver_getFileSize) =
      get_shared_library_function(library_handle, "driver_getFileSize", 1);
  *(void **)(&ptr_driver_fopen) =
      get_shared_library_function(library_handle, "driver_fopen", 1);
  *(void **)(&ptr_driver_fclose) =
      get_shared_library_function(library_handle, "driver_fclose", 1);
  *(void **)(&ptr_driver_fseek) =
      get_shared_library_function(library_handle, "driver_fseek", 1);
  *(void **)(&ptr_driver_fread) =
      get_shared_library_function(library_handle, "driver_fread", 1);
  *(void **)(&ptr_driver_getlasterror) =
      get_shared_library_function(library_handle, "driver_getlasterror", 1);

  // Bind each read-write function of the library if necessary
  if (!global_error && !ptr_driver_isReadOnly()) {
    *(void **)(&ptr_driver_fwrite) =
        get_shared_library_function(library_handle, "driver_fwrite", 1);
    *(void **)(&ptr_driver_fflush) =
        get_shared_library_function(library_handle, "driver_fflush", 1);
    *(void **)(&ptr_driver_remove) =
        get_shared_library_function(library_handle, "driver_remove", 1);
    *(void **)(&ptr_driver_mkdir) =
        get_shared_library_function(library_handle, "driver_mkdir", 1);
    *(void **)(&ptr_driver_rmdir) =
        get_shared_library_function(library_handle, "driver_rmdir", 1);
    *(void **)(&ptr_driver_diskFreeSpace) =
        get_shared_library_function(library_handle, "driver_diskFreeSpace", 1);

    // Bind optional functions (without setting global_error to 1 in case of
    // errors)
    *(void **)(&ptr_driver_copyToLocal) =
        get_shared_library_function(library_handle, "driver_copyToLocal", 0);
    *(void **)(&ptr_driver_copyFromLocal) =
        get_shared_library_function(library_handle, "driver_copyFromLocal", 0);
  }

  if (!global_error) {
    // Call 2 simple functions
    std::cout << fmt::format(
        "Test of the driver library '{}' version {} handling scheme {}\n",
        ptr_driver_getDriverName(), ptr_driver_getVersion(),
        ptr_driver_getScheme());

    // Connection to the file system
    bool bIsconnected = ptr_driver_connect();
    if (bIsconnected) {
      if (!ptr_driver_isConnected()) {
        global_error = 1;
        std::cerr << "ERROR : connection is done but driver is not connected\n";
      }
      if (!ptr_driver_fileExists(argv[2])) {
        std::cerr << fmt::format("ERROR : {} is missing\n", argv[2]);
        global_error = 1;
      }
      // The real test begins here (global_error is set to 1 in case of errors)
      if (!global_error) {
        std::cout << "Test begins\n";
        do_test(argv[2], argv[3], argv[4]);
      }
      ptr_driver_disconnect();
    } else {
      global_error = 1;
      std::cerr << "ERROR : unable to connect to the file system\n";
    }
  }

  free_shared_library(library_handle);
  if (global_error) {
    std::cerr << "Test has failed\n";
    exit(EXIT_FAILURE);
  }
  std::cout << "! Test is successful !\n";
  exit(EXIT_SUCCESS);
}

/* functions definitions */

void usage() {
  std::cerr << "Usage : drivertest libraryname input_filename output_filename "
               "local_filename\n";
  std::cerr << "example : drivertest ./libkhiopsdriver_file_gcs.so "
               "gs:///input_filename gs:///output_filename /tmp/copy.txt\n";
  exit(EXIT_FAILURE);
}

void test_read(const char *file_name_input, void *file,
               long long int maxBytes = 3000) {
  // Reads the file by steps of 1Kb and writes to the output file at each step
  const int nBufferSize = 1024;
  // const int nBufferSize = 5;
  char buffer[nBufferSize + 1];
  long long int sizeRead = nBufferSize;
  const long long int maxTotalRead = 3000;
  long long int totalRead = 0;
  while (sizeRead == nBufferSize && !global_error) {
    int toRead = maxBytes - totalRead;
    if (toRead > nBufferSize)
      toRead = nBufferSize;
    sizeRead = ptr_driver_fread(&buffer, sizeof(char), toRead, file);
    std::cout << fmt::format("Read = {}, total = {}, max = {}\n", sizeRead,
                             totalRead, maxBytes);

    // Message d'erreur si necessaire
    if (sizeRead == 0) {
      global_error = 1;
      std::cout << fmt::format("error while reading {} : {}\n", file_name_input,
                               ptr_driver_getlasterror());
    }
    // Affichage du contenu dans la console sinon
    else {
      // Debug
      /*for (int i=0; i<10 && i<sizeRead; i++) {
              std::cout << fmt::format("{} ", buffer[i]);
      }
      std::cout << fmt::format("\n");*/
      buffer[sizeRead] = '\0';
      std::cout << fmt::format("Buffer = {}", buffer);
      totalRead += sizeRead;

      // Arret quand on a atteint la limite
      if (totalRead > maxTotalRead) {
        std::cout << fmt::format("\n");
        break;
      }
    }
  }
}

void *load_shared_library(const char *library_name) {
#ifdef __windows__
  wchar_t *wString = new wchar_t[4096];
  MultiByteToWideChar(CP_ACP, 0, library_name, -1, wString, 4096);
  void *handle = (void *)LoadLibrary(library_name);
  delete[] wString;
  return handle;
#elif defined(__unix_or_mac__)
  return dlopen(library_name, RTLD_NOW);
#endif
}

int free_shared_library(void *library_handle) {
#ifdef __windows__
  return FreeLibrary((HINSTANCE)library_handle);
#elif defined(__unix_or_mac__)
  return dlclose(library_handle);
#endif
}

void *get_shared_library_function(void *library_handle,
                                  const char *function_name, int mandatory) {
  void *ptr;
#ifdef __windows__
  ptr = (void *)GetProcAddress((HINSTANCE)library_handle, function_name);
#elif defined(__unix_or_mac__)
  ptr = dlsym(library_handle, function_name);
#endif
  if (ptr == NULL && mandatory) {
    global_error = 1;
#ifdef __unix_or_mac__
    std::cerr << fmt::format("Unable to load {} ({})\n", function_name,
                             dlerror());
#else
    std::cerr << fmt::format("Unable to load {} ({})\n", function_name,
                             GetLastError());
#endif
  }
  return ptr;
}

void do_test(const char *file_name_input, const char *file_name_output,
             const char *file_name_local) {
  std::cout << fmt::format("Starting test\n");
  // Basic information of the scheme
  std::cout << fmt::format("scheme: {}\n", ptr_driver_getScheme());
  std::cout << fmt::format("is read-only: {}\n", ptr_driver_isReadOnly());

  // Checks size of input file
  long long int filesize = ptr_driver_getFileSize(file_name_input);
  std::cout << fmt::format("size of {} is {}\n", file_name_input, filesize);

  // Opens for read
  void *file = ptr_driver_fopen(file_name_input, 'r');
  if (file == NULL) {
    std::cout << fmt::format("error : {} : {}\n", file_name_input,
                             ptr_driver_getlasterror());
    global_error = 1;
    return;
  }

  // Show small extract if the driver is not read-only
  std::cout << "Test read from start\n\n";
  test_read(file_name_input, file, filesize);

  auto offset = 3;
  std::cout << fmt::format("Test read from offset {}\n\n", offset);
  ptr_driver_fseek(file, offset, SEEK_SET);
  test_read(file_name_input, file, filesize - offset);

  offset = -20;
  std::cout << fmt::format("Test read from end {}\n\n", offset);
  ptr_driver_fseek(file, offset, SEEK_END);
  test_read(file_name_input, file, 20);

  offset = -40;
  std::cout << fmt::format("Test read from current offset {}\n\n", offset);
  ptr_driver_fseek(file, offset, SEEK_CUR);
  test_read(file_name_input, file, 40);
  // ptr_driver_fclose(file);

  ptr_driver_fseek(file, 0, SEEK_SET);

  // Opens for write if the driver is not read-only
  if (!ptr_driver_isReadOnly()) {
    std::cout << "Writing test\n\n";

    void *fileoutput = ptr_driver_fopen(file_name_output, 'w');
    if (fileoutput == NULL) {
      std::cout << fmt::format("error : {} : {}\n", file_name_output,
                               ptr_driver_getlasterror());
      global_error = 1;
    }
    if (!global_error) {
      // Reads the file by steps of ~1Mb and writes to the output file at each
      // step
      // const int nBufferSize = 1000*1000;
      // const int nBufferSize = 1024*1024;
      const int nBufferSize = 1234 * 5678;
      // const int nBufferSize = 128;
      char *buffer = new char[nBufferSize + 1];
      long long int sizeRead = nBufferSize;
      long long int sizeWrite;
      long long int totalRead = 0;
      while (sizeRead == nBufferSize && !global_error) {
        int toRead = filesize - totalRead;
        if (toRead > nBufferSize)
          toRead = nBufferSize;
        sizeRead = ptr_driver_fread(buffer, sizeof(char), toRead, file);
        std::cout << fmt::format("Read = {}, total = {}, max = {}\n", sizeRead,
                                 totalRead, filesize);
        if (sizeRead == -1) {
          global_error = 1;
          std::cout << fmt::format("error while reading {} : {}\n",
                                   file_name_input, ptr_driver_getlasterror());
        } else {
          sizeWrite = ptr_driver_fwrite(buffer, sizeof(char), (size_t)sizeRead,
                                        fileoutput);
          std::cout << fmt::format("sizeWrite = {}\n", sizeWrite);
          if (sizeWrite == -1) {
            global_error = 1;
            std::cout << fmt::format("error while writing {} : {}\n",
                                     file_name_output,
                                     ptr_driver_getlasterror());
          }
          totalRead += sizeRead;
        }
      }
      delete[] (buffer);
    }
    ptr_driver_fclose(fileoutput);
    ptr_driver_fclose(file);

    // Checks that the two sizes are the same
    if (!global_error) {
      long long int filesize_output = ptr_driver_getFileSize(file_name_output);
      std::cout << fmt::format("size of {} is {}\n", file_name_output,
                               filesize_output);
      if (filesize_output != filesize) {
        std::cout << fmt::format("Sizes of input and output are different\n");
        global_error = 1;
      }
      if (ptr_driver_fileExists(file_name_output)) {
        std::cout << fmt::format("{} exists\n", file_name_output);
      } else {
        std::cout << fmt::format("something's wrong : {} is missing\n",
                                 file_name_output);
        global_error = 1;
      }
    }

    // Copy to local if this optional function is available in the librairy
    if (!global_error && ptr_driver_copyToLocal != NULL) {
      std::cout << fmt::format("\n\ncopyToLocal {} to {}\n", file_name_input,
                               file_name_local);
      global_error =
          ptr_driver_copyToLocal(file_name_input, file_name_local) == 0;
      if (global_error)
        std::cout << fmt::format("Error while copying : {}\n",
                                 ptr_driver_getlasterror());
      else
        std::cout << fmt::format("copy {} to local is done\n", file_name_input);
    }

    // Delete file
    if (!global_error) {
      global_error = ptr_driver_remove(file_name_output) == 0;
      if (global_error)
        std::cout << fmt::format("Error while removing : {}\n",
                                 ptr_driver_getlasterror());
      if (ptr_driver_fileExists(file_name_output)) {
        std::cout << fmt::format("{} should be removed !\n", file_name_output);
        global_error = 1;
      }
    }

    // Copy from local if this optional function is available in the librairy
    if (!global_error && ptr_driver_copyFromLocal != NULL) {
      std::cout << fmt::format("\n\ncopyFromLocal {} to {}\n", file_name_local,
                               file_name_output);
      global_error =
          ptr_driver_copyFromLocal(file_name_local, file_name_output) == 0;
      if (global_error)
        std::cout << fmt::format("Error while copying : {}\n",
                                 ptr_driver_getlasterror());
      else
        std::cout << fmt::format("copy {} from local is done\n",
                                 file_name_local);
      if (!ptr_driver_fileExists(file_name_output)) {
        std::cout << fmt::format("{} is missing !\n", file_name_output);
        global_error = 1;
      }
    }
  }
}
