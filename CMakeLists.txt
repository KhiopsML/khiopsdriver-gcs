# CMakeLists.txt

# Ensure to pick up the default triplet from the environment if any. This helps 
# driving the vcpkg triplet in the same way either when starting vcpkg directly,
# or when letting CMake start vcpkg at configure/generate time.
# Note: this logic must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}" CACHE STRING "The vcpkg triplet")
endif()

cmake_minimum_required(VERSION 3.20)
# Enforce c++14 standard.
set (CMAKE_CXX_STANDARD 14)

project(khiops-gcs LANGUAGES CXX)

include(GoogleTest)
enable_testing()

# Workaround for windows: https://github.com/actions/virtual-environments/issues/5900
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(GLOBAL PROPERTY MSVC_RUNTIME_LIBRARY MultiThreaded)
else()
    set_property(GLOBAL PROPERTY MSVC_RUNTIME_LIBRARY MultiThreadedDebug)
endif()

# Find dependencies
find_package(fmt CONFIG REQUIRED)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# Hide symbols in the shared libraries
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

# Set the location of the built artifacts:
#
# - Shared and static library target directory: lib
# - Executable target directory: bin
# - We must use these weird generator expressions to avoid the Debug and Release directories in VS
# - More info: https://stackoverflow.com/q/47175912
#
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/lib/>)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/lib/>)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/bin/>)
message(STATUS "Executables will be stored in ${CMAKE_BINARY_DIR}/bin/")
message(STATUS "Libraries will be stored in ${CMAKE_BINARY_DIR}/lib/")

function(set_options target)
  # Show the current target information
  get_target_property(target_type ${target} TYPE)
  message(STATUS "Setting up ${target_type} ${target}")

  # General compiler properties and definitions
  set_property(TARGET ${target} PROPERTY POSITION_INDEPENDENT_CODE TRUE)
  target_compile_definitions(${target} PRIVATE UNICODE _UNICODE)
  target_compile_definitions(${target} PRIVATE $<$<CONFIG:RELEASE,RELWITHDEBINFO>:NOALL>)
  target_compile_definitions(${target} PRIVATE $<$<CONFIG:RELWITHDEBINFO>:__ALPHA__>)

  # Delegate to specialized function for Windows and Unix-like
  if(MSVC)
 
    # Debug only compiling options
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
		# Definitions
		target_compile_definitions(${target} PRIVATE _DEBUG _CONSOLE)

		# Options
		#
		# - /RTC1: Enable run-time error checks.
		# - /ZI: Generates "Format Database for 'Edit and Continue'" debug information format.
		# - /MTd: Use multithread debug runtime (set via MSVC_RUNTIME_LIBRARY property).
		#
		target_compile_options(${target} PRIVATE /RTC1 /ZI)
		set_target_properties(${target} PROPERTIES MSVC_RUNTIME_LIBRARY MultiThreadedDebug)

    # Release only compiling options
    elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
		# Options
		#
		# - /GR-: Do not generate runtime information.
		# - /Oi: Generate instrinsic functions.
		# - /Zi: Generates "Format Database" debug information format.
		# - /MT: Use multithread runtime (set via MSVC_RUNTIME_LIBRARY property).
		#
		target_compile_options(${target} PRIVATE /GR- /Oi /Zi)
		set_target_properties(${target} PROPERTIES MSVC_RUNTIME_LIBRARY MultiThreaded)
    else()
	    message(ERROR "Unsuported MSVC CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    endif()

  endif()
endfunction(set_options)

add_library(khiopsdriver_file_gcs SHARED src/gcsplugin.h src/gcsplugin.cpp)
target_link_options(khiopsdriver_file_gcs PRIVATE $<$<CONFIG:RELEASE>:-s>) # stripping
set_options(khiopsdriver_file_gcs)
target_link_libraries(khiopsdriver_file_gcs PRIVATE google-cloud-cpp::storage spdlog::spdlog)
target_compile_options(khiopsdriver_file_gcs
	PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall>
	PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>
)

add_executable(KhiopsPluginTest src/khiopsplugintest.cpp)
target_link_libraries(KhiopsPluginTest PRIVATE fmt::fmt)

add_executable(drivertest src/drivertest.cpp)
target_link_libraries(drivertest ${CMAKE_DL_LIBS}) # Link to dl

add_subdirectory(test)
