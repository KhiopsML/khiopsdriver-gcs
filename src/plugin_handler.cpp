#include "plugin_handler.h"

#include <iostream>

//#if defined(__unix__) || defined(__unix) || \
//    (defined(__APPLE__) && defined(__MACH__))
// #define __unix_or_mac__
// #else
// #define __windows__
// #endif
//
// #ifdef __unix_or_mac__
// #include <unistd.h>
// #include <dlfcn.h>
// #else
// #include <windows.h>
// #include "errhandlingapi.h"
// #endif

namespace plgh {
void *PluginHandle::GetSharedLibraryFunction(const char *name) {
  return static_cast<void *>(FunctionLoader_(lib_handle_, name));
  // #if defined(__windows__)
  //		return static_cast<void*>(GetProcAddress(lib_handle_, name));
  // #else
  //		return dlsym(lib_handle_, name);
  // #endif
}

int PluginHandle::FreeSharedLibrary() {
  if (!lib_handle_) {
    return 0;
  }
  return FreeModule_(lib_handle_);
  // #if defined(__windows__)
  //		return FreeLibrary(lib_handle_);
  // #else
  //		return dlclose(lib_handle_);
  // #endif
}

bool PluginHandle::Init() {
  bool load_success =
      LoadLibraryFunction(&ptr_driver_getDriverName, "driver_getDriverName",
                          true) &&
      LoadLibraryFunction(&ptr_driver_getVersion, "driver_getVersion", true) &&
      LoadLibraryFunction(&ptr_driver_getScheme, "driver_getScheme", true) &&
      LoadLibraryFunction(&ptr_driver_isReadOnly, "driver_isReadOnly", true) &&
      LoadLibraryFunction(&ptr_driver_connect, "driver_connect", true) &&
      LoadLibraryFunction(&ptr_driver_disconnect, "driver_disconnect", true) &&
      LoadLibraryFunction(&ptr_driver_isConnected, "driver_isConnected",
                          true) &&
      LoadLibraryFunction(&ptr_driver_fileExists, "driver_fileExists", true) &&
      LoadLibraryFunction(&ptr_driver_dirExists, "driver_dirExists", true)

      && LoadLibraryFunction(&ptr_driver_getFileSize, "driver_getFileSize",
                             true) &&
      LoadLibraryFunction(&ptr_driver_fopen, "driver_fopen", true) &&
      LoadLibraryFunction(&ptr_driver_fclose, "driver_fclose", true) &&
      LoadLibraryFunction(&ptr_driver_fread, "driver_fread", true) &&
      LoadLibraryFunction(&ptr_driver_fseek, "driver_fseek", true) &&
      LoadLibraryFunction(&ptr_driver_getlasterror, "driver_getlasterror",
                          true);

  if (load_success && !ptr_driver_isReadOnly()) {
    /* API functions definition, that must be defined in the library only if the
     * driver is not read-only */
    const bool load_write_funcs_success =
        LoadLibraryFunction(&ptr_driver_fwrite, "driver_fwrite", true) &&
        LoadLibraryFunction(&ptr_driver_fflush, "driver_fflush", true) &&
        LoadLibraryFunction(&ptr_driver_remove, "driver_remove", true) &&
        LoadLibraryFunction(&ptr_driver_mkdir, "driver_mkdir", true) &&
        LoadLibraryFunction(&ptr_driver_rmdir, "driver_rmdir", true) &&
        LoadLibraryFunction(&ptr_driver_diskFreeSpace, "driver_diskFreeSpace",
                            true)

        /* API functions definition, that can be defined optionally in the
           library only if the driver is not read-only */
        && LoadLibraryFunction(&ptr_driver_copyToLocal, "driver_copyToLocal",
                               false) &&
        LoadLibraryFunction(&ptr_driver_copyFromLocal, "driver_copyFromLocal",
                            false);

    load_success = load_success && load_write_funcs_success;
  }

  return load_success;
}

PluginHandle::PluginHandle(const std::string &lib_path) {
#ifdef __windows__
  LibraryLoader_ = LoadLibrary;
  FunctionLoader_ = GetProcAddress;
  FreeModule_ = FreeLibrary;
#else
  using namespace std::placeholders;
  LibraryLoader = std::bind(dlopen, _1, RTLDNOW);
  FunctionLoader = dlsym;
  FreeModule_ = dlclose;
#endif

  lib_handle_ = LoadSharedLibrary(lib_path);
  if (!lib_handle_) {
    return;
  }

  if (!Init()) {
    FreeSharedLibrary();
    lib_handle_ = nullptr;
  }
}

PluginHandle::~PluginHandle() { FreeSharedLibrary(); }

bool PluginHandle::IsValid() const { return lib_handle_ != nullptr; }

} // namespace plgh