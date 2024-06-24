#pragma once

#include <functional>
#include <iostream>
#include <string>

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

namespace plgh
{

	struct PluginHandle
	{
#ifdef __windows__
		using handle_t = HMODULE;
#else
		using handle_t = void*;
#endif

		/* API functions definition, that must be defined in the library */
		const char* (*ptr_driver_getDriverName)();
		const char* (*ptr_driver_getVersion)();
		const char* (*ptr_driver_getScheme)();
		int (*ptr_driver_isReadOnly)();
		int (*ptr_driver_connect)();
		int (*ptr_driver_disconnect)();
		int (*ptr_driver_isConnected)();
		int (*ptr_driver_fileExists)(const char* filename);
		int (*ptr_driver_dirExists)(const char* filename);

		long long (*ptr_driver_getFileSize)(const char* filename);
		void* (*ptr_driver_fopen)(const char* filename, const char mode);
		int (*ptr_driver_fclose)(void* stream);
		long long (*ptr_driver_fread)(void* ptr, size_t size, size_t count, void* stream);
		int (*ptr_driver_fseek)(void* stream, long long int offset, int whence);
		const char* (*ptr_driver_getlasterror)();

		/* API functions definition, that must be defined in the library only if the driver is not read-only */
		long long (*ptr_driver_fwrite)(const void* ptr, size_t size, size_t count, void* stream);
		int (*ptr_driver_fflush)(void* stream);
		int (*ptr_driver_remove)(const char* filename);
		int (*ptr_driver_mkdir)(const char* filename);
		int (*ptr_driver_rmdir)(const char* filename);
		long long (*ptr_driver_diskFreeSpace)(const char* filename);

		/* API functions definition, that can be defined optionally in the library only if the driver is not read-only */
		int (*ptr_driver_copyToLocal)(const char* sourcefilename, const char* destfilename);
		int (*ptr_driver_copyFromLocal)(const char* sourcefilename, const char* destfilename);


		explicit PluginHandle(const std::string& lib_path);
		~PluginHandle();
		PluginHandle(const PluginHandle&) = delete;
		PluginHandle(PluginHandle&&) noexcept = delete;
		PluginHandle& operator=(const PluginHandle&) = delete;
		PluginHandle& operator=(PluginHandle&&) noexcept = delete;

		bool IsValid() const;

	private:
#ifdef __windows__
		std::function<handle_t(const char*)> LibraryLoader_;
		std::function<FARPROC(handle_t, const char*)> FunctionLoader_;
#else
		std::function<handle_t(const char*, int)> LibraryLoader_;
		std::function<void* (handle_t, const char*)> FunctionLoader_;
#endif
		std::function<int(handle_t)> FreeModule_;

		handle_t lib_handle_{};

		handle_t LoadSharedLibrary(const std::string& library_name)
		{
			handle_t handle = LibraryLoader_(library_name.c_str());
			if (!handle)
			{
#ifdef __windows__
				std::cerr << "Failed loading module, error: " << std::hex << GetLastError() << std::dec << '\n';
#else
				std::cerr << "Failed loading module. " << dlerror() << '\n';
#endif
			}
			return handle;
		}

		void* GetSharedLibraryFunction(const char* name);
		int FreeSharedLibrary();
		bool Init();

		template<typename F_PTR>
		bool LoadLibraryFunction(F_PTR* fptr, const char* function_name, bool mandatory)
		{
			*fptr = static_cast<F_PTR>(GetSharedLibraryFunction(function_name));
			return *fptr || !mandatory;
		}
	};
}