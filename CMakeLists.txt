# CMakeLists.txt

# Ensure to pick up the default triplet from the environment if any. This helps driving the vcpkg triplet in the same
# way either when starting vcpkg directly, or when letting CMake start vcpkg at configure/generate time. Note: this
# logic must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET
      "$ENV{VCPKG_DEFAULT_TRIPLET}"
      CACHE STRING "The vcpkg triplet")
endif()

cmake_minimum_required(VERSION 3.20)

# Enforce c++14 standard.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Get project version from the header file
file(READ "src/gcsplugin.h" H_FILE)
string(REGEX MATCH "#define DRIVER_VERSION KHIOPS_STR\\(([^)]*)\\)" _ ${H_FILE})
set(DRIVER_VERSION ${CMAKE_MATCH_1})
# Extract the main version (up to the first hyphen)
string(REGEX REPLACE "^([0-9]+\\.[0-9]+\\.[0-9]+).*" "\\1" PROJECT_VERSION "${DRIVER_VERSION}")

message(STATUS "Building Khiops driver version: ${DRIVER_VERSION} (CMake version : ${PROJECT_VERSION})")

project(
  khiops-gcs
  LANGUAGES CXX
  VERSION ${DRIVER_VERSION})

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/modules/)

# Exclude googletest from the installation and packages
set(INSTALL_GTEST OFF)
set(INSTALL_GMOCK OFF)

if(BUILD_TESTS)
  include(GoogleTest)
  enable_testing()
endif()

# Default integration-test URL prefix for this repository when not provided by CI/user.
if(BUILD_TESTS AND NOT DEFINED ENV{STORAGE_DRIVER_TEST_URL_PREFIX})
  set(ENV{STORAGE_DRIVER_TEST_URL_PREFIX} "gs://data-test-khiops-driver-gcs")
  message(STATUS "Defaulting STORAGE_DRIVER_TEST_URL_PREFIX to gs://data-test-khiops-driver-gcs")
endif()

# Find dependencies
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(CURL REQUIRED)
find_package(nlohmann_json REQUIRED)
if(BUILD_TESTS)
  find_package(fmt CONFIG REQUIRED)
  find_package(Boost REQUIRED)
endif()

# Hide symbols in the shared libraries
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

# Set the location of the built artifacts:
#
# - Shared and static library target directory: lib
# - Executable target directory: lib
# - We must use these weird generator expressions to avoid the Debug and Release directories in VS
# - More info: https://stackoverflow.com/q/47175912
#
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/lib/>)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/lib/>)
if(WIN32)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
  message(STATUS "Executables will be stored in ${CMAKE_BINARY_DIR}/bin/")
else()
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
  message(STATUS "Executables will be stored in ${CMAKE_BINARY_DIR}/lib/")
endif()
message(STATUS "Libraries will be stored in ${CMAKE_BINARY_DIR}/lib/")

# Avoid generating shared libs with version number in the filename
set(CMAKE_PLATFORM_NO_VERSIONED_SONAME 1)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT CMAKE_BUILD_TYPE STREQUAL "Release")
  message("Build system == Linux and build type != Release  ==>  build with sanitizer tools")
  add_compile_options(-fsanitize=undefined -fsanitize=address -fno-sanitize-recover=all)
  add_link_options(-fsanitize=undefined -fsanitize=address -fno-sanitize-recover=all)
  if(BUILD_TESTS)
    include(CodeCoverage)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_COVERAGE} ${CMAKE_CXX_FLAGS_DEBUG}")
    # Unit-only coverage (tests labeled "unit")
    setup_target_for_coverage(${PROJECT_NAME}_coverage_unit ${CMAKE_CTEST_COMMAND} coverage-unit
                              "--output-on-failure;-L;unit")
    setup_target_for_coverage_cobertura(${PROJECT_NAME}_cobertura_unit ${CMAKE_CTEST_COMMAND} coverage-unit
                                        "--output-on-failure;-L;unit")

    # Full coverage (all tests known by ctest)
    setup_target_for_coverage(${PROJECT_NAME}_coverage_full ${CMAKE_CTEST_COMMAND} coverage-full "--output-on-failure")
    setup_target_for_coverage_cobertura(${PROJECT_NAME}_cobertura_full ${CMAKE_CTEST_COMMAND} coverage-full
                                        "--output-on-failure")

    # Backward-compatible target names, mapped to unit coverage for reliability in local/IDE runs where integration
    # credentials may be missing.
    add_custom_target(${PROJECT_NAME}_coverage DEPENDS ${PROJECT_NAME}_coverage_unit)
    add_custom_target(${PROJECT_NAME}_cobertura DEPENDS ${PROJECT_NAME}_cobertura_unit)
  endif()
endif()

add_library(
  khiopsdriver_file_gcs SHARED
  src/gcsplugin.h
  src/gcsplugin_internal.h
  src/gcsplugin.cpp
  src/utils.h
  src/utils.cpp
  shared/src/khiops_driver_common/logging.hpp
  shared/src/khiops_driver_common/logging.cpp
  shared/src/khiops_driver_common/util.hpp
  src/oauth2_token_manager.h
  src/oauth2_token_manager.cpp)

target_include_directories(khiopsdriver_file_gcs PRIVATE shared/src)
target_link_options(khiopsdriver_file_gcs PRIVATE $<$<CONFIG:RELEASE>:-s>) # stripping
target_link_libraries(khiopsdriver_file_gcs PRIVATE google-cloud-cpp::storage spdlog::spdlog CURL::libcurl
                                                    nlohmann_json::nlohmann_json)

set_target_properties(
  khiopsdriver_file_gcs
  PROPERTIES PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/src/gcsplugin.h"
             SOVERSION ${PROJECT_VERSION_MAJOR}
             VERSION ${PROJECT_VERSION})

# For consistent naming across platforms
if(WIN32)
  set_target_properties(
    khiopsdriver_file_gcs
    PROPERTIES # Override or add Windows-specific properties
               RUNTIME_OUTPUT_NAME "libkhiopsdriver_file_gcs" ARCHIVE_OUTPUT_NAME "libkhiopsdriver_file_gcs")
  # WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

target_compile_options(
  khiopsdriver_file_gcs
  PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/wd4582;/wd4583;/wd4625;/wd4626;/wd4710;/wd4711;/wd4820;/wd4868;/wd4365;/wd5045>
  PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>)

target_compile_definitions(
  khiopsdriver_file_gcs PRIVATE LOGGERNAME="gcsdriver" LOGLEVEL_ENVVARNAME="GCS_DRIVER_LOGLEVEL"
                                LOGFILE_ENVVARNAME="GCS_DRIVER_LOGFILE")

option(BUILD_TESTS "Build test programs" OFF)

if(BUILD_TESTS)
  add_executable(KhiopsPluginTest src/khiopsplugintest.cpp)
  target_compile_options(
    KhiopsPluginTest
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/W4;/wd4101;/wd4625;/wd4710;/wd4711;/wd4365;/wd5045>
    PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>)
  target_link_libraries(KhiopsPluginTest PRIVATE fmt::fmt ${CMAKE_DL_LIBS})

  add_executable(drivertest src/drivertest.cpp)
  target_compile_options(
    drivertest
    PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/W4;/wd4101;/wd4625;/wd4710;/wd4711;/wd4365;/wd5045>
    PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>)
  target_link_libraries(drivertest ${CMAKE_DL_LIBS} google-cloud-cpp::storage)

  add_subdirectory(test)
  set(storage_driver_target_name khiopsdriver_file_gcs)
  add_subdirectory("shared/integrationtests")
endif(BUILD_TESTS)

install(
  TARGETS khiopsdriver_file_gcs
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib # lib on windows
  RUNTIME DESTINATION bin # dll on windows
  PUBLIC_HEADER DESTINATION include)

set(CPACK_PACKAGE_VENDOR Orange)
set(CPACK_PACKAGE_HOMEPAGE_URL https://khiops.org)
set(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_VENDOR "Orange")
set(CPACK_PACKAGE_CONTACT "Khiops Team <khiops.team@orange.com>")
set(CPACK_SOURCE_IGNORE_FILES .git)
set(CPACK_PACKAGE_DESCRIPTION "This driver allows Khiops to access the Google Cloud storage.")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "GCS driver for the Khiops tool")
set(CPACK_PACKAGE_NAME "khiops-driver-gcs")

set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_BINARY_DIR}/packages")

# ########### ARCHIVE Generator #############################
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_ARCHIVE_FILE_NAME khiops-driver-gcs-${PROJECT_VERSION})

# ########### DEB Generator #############################
set(CPACK_DEBIAN_PACKAGE_RELEASE 1)
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEB_COMPONENT_INSTALL YES)
set(CPACK_DEBIAN_PACKAGE_SECTION "math")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_PACKAGE_GENERATE_SHLIBS ON)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS_PRIVATE_DIRS ${CMAKE_BINARY_DIR}/lib/)
set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "khiops")
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)

# ########### RPM Generator #############################
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_LICENSE BSD-3-Clause)
set(CPACK_RPM_PACKAGE_GROUP "Applications/Engineering")
set(CPACK_RPM_PACKAGE_VENDOR Orange)
set(CPACK_RPM_KHIOPS_PACKAGE_AUTOREQ ON)
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)

include(CPack)
