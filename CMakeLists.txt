# CMakeLists.txt

# Ensure to pick up the default triplet from the environment if any. This helps
# driving the vcpkg triplet in the same way either when starting vcpkg directly,
# or when letting CMake start vcpkg at configure/generate time. Note: this logic
# must happen before PROJECT command.
if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
  set(VCPKG_TARGET_TRIPLET
      "$ENV{VCPKG_DEFAULT_TRIPLET}"
      CACHE STRING "The vcpkg triplet")
endif()

cmake_minimum_required(VERSION 3.20)

# Enforce c++14 standard.
set(CMAKE_CXX_STANDARD 14)

project(
  khiops-gcs
  LANGUAGES CXX
  VERSION 0.1.0)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/modules/)

# Exclude googletest from the installation and packages
set(INSTALL_GTEST OFF)
set(INSTALL_GMOCK OFF)

include(GoogleTest)
enable_testing()

# Find dependencies
find_package(fmt CONFIG REQUIRED)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# Hide symbols in the shared libraries
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

# Set the location of the built artifacts:
#
# * Shared and static library target directory: lib
# * Executable target directory: bin
# * We must use these weird generator expressions to avoid the Debug and Release
#   directories in VS
# * More info: https://stackoverflow.com/q/47175912
#
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/lib/>)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/lib/>)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY $<1:${CMAKE_BINARY_DIR}/bin/>)
message(STATUS "Executables will be stored in ${CMAKE_BINARY_DIR}/bin/")
message(STATUS "Libraries will be stored in ${CMAKE_BINARY_DIR}/lib/")

if((CMAKE_SYSTEM_NAME STREQUAL "Linux") AND NOT (CMAKE_BUILD_TYPE STREQUAL
                                                 "Release"))
  message("Build system == Linux, build with sanitizer tools")
  add_compile_options(-fsanitize=undefined -fsanitize=address
                      -fno-sanitize-recover=all)
  add_link_options(-fsanitize=undefined -fsanitize=address
                   -fno-sanitize-recover=all)
  include(CodeCoverage)
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_COVERAGE} ${CMAKE_CXX_FLAGS_DEBUG}")
  setup_target_for_coverage(${PROJECT_NAME}_coverage basic_test coverage)
  setup_target_for_coverage_cobertura(
    ${PROJECT_NAME}_cobertura basic_test coverage
    --gtest_output=xml:coverage.junit.xml)
endif()

add_library(khiopsdriver_file_gcs SHARED
            src/gcsplugin.h src/gcsplugin_internal.h src/gcsplugin.cpp)

target_link_options(khiopsdriver_file_gcs PRIVATE $<$<CONFIG:RELEASE>:-s>
)# stripping
target_link_libraries(khiopsdriver_file_gcs PRIVATE google-cloud-cpp::storage
                                                    spdlog::spdlog)

set_target_properties(
  khiopsdriver_file_gcs PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR}
                                   VERSION ${PROJECT_VERSION})

target_compile_options(
  khiopsdriver_file_gcs
  PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall>
  PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>)

option(BUILD_TESTS "Build test programs" OFF)

if(BUILD_TESTS)
  add_executable(KhiopsPluginTest src/khiopsplugintest.cpp)
  target_link_libraries(KhiopsPluginTest PRIVATE fmt::fmt ${CMAKE_DL_LIBS})
  add_executable(drivertest src/drivertest.cpp)
  target_link_libraries(drivertest ${CMAKE_DL_LIBS} google-cloud-cpp::storage
  )# Link to dl
endif(BUILD_TESTS)

add_subdirectory(test)

include(GNUInstallDirs)
install(
  TARGETS khiopsdriver_file_gcs
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIB}
  ARCHIVE # lib on windows
  RUNTIME # dll on windows
)

set(CPACK_PACKAGE_VENDOR Orange)
set(CPACK_PACKAGE_HOMEPAGE_URL https://khiops.org)
set(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_VENDOR "Orange")
set(CPACK_PACKAGE_CONTACT "Khiops Team <khiops.team@orange.com>")
set(CPACK_SOURCE_IGNORE_FILES .git)
set(CPACK_PACKAGE_DESCRIPTION
    "This driver allows Khiops to access the Google Cloud storage.")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "GCS driver for the Khiops tool")
set(CPACK_PACKAGE_NAME "khiops-driver-gcs")

set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_BINARY_DIR}/packages")
# set(CPACK_PACKAGING_INSTALL_PREFIX "/usr/")

# ########### ARCHIVE Generator #############################

# One package per component
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)

# user friendly archive names
set(CPACK_ARCHIVE_FILE_NAME kkhiops-driver-gcs-${PROJECT_VERSION})

# ########### DEB Generator #############################

# Package release ("This is the numbering of the DEB package itself, i.e. the
# version of the packaging and not the version of the content")
set(CPACK_DEBIAN_PACKAGE_RELEASE 1)

# package name for deb.
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

# Packages version: the full KHIOPS_VERSION (N.N.N_aN) instead of the
# PROJECT_VERSION (N.N.N) set(CPACK_DEBIAN_PACKAGE_VERSION ${KHIOPS_VERSION})

set(CPACK_DEB_COMPONENT_INSTALL YES)
set(CPACK_DEBIAN_PACKAGE_SECTION "math")

# runtime path setting
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_PACKAGE_GENERATE_SHLIBS ON)
# binaries in deb building will search shared library in the build tree. We
# should use directly CMAKE_LIBRARY_OUTPUT_DIRECTORY but it produces a bug in
# dpkg-shlibdeps
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS_PRIVATE_DIRS ${CMAKE_BINARY_DIR}/lib/)

# packages recommends
set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "khiops")

# packages posinst and triggers
set(CPACK_DEBIAN_KNI_PACKAGE_CONTROL_EXTRA
    "${PROJECT_SOURCE_DIR}/packaging/linux/debian/kni/triggers")

# set(CPACK_DEBIAN_PACKAGE_DEBUG ON)
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)

# ########### RPM Generator #############################

set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_LICENSE BSD-3-Clause)
set(CPACK_RPM_PACKAGE_GROUP "Applications/Engineering")
set(CPACK_RPM_PACKAGE_VENDOR Orange)

set(CPACK_RPM_KHIOPS_PACKAGE_AUTOREQ ON)

# Packages version set(CPACK_RPM_PACKAGE_VERSION ${PROJECT_VERSION})

# packages names set(CPACK_RPM_PACKAGE_NAME khiops)

# default file name e.g. khiops-10.0.0-1.x86_64.rpm
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)

# packages summary set(CPACK_RPM_PACKAGE_SUMMARY "Khiops tools")

# packages post/postun install scripts
# set(CPACK_RPM_KNI_POST_INSTALL_SCRIPT_FILE
# "${PROJECT_SOURCE_DIR}/packaging/linux/redhat/kni.post")
# set(CPACK_RPM_KNI_POSTUN_INSTALL_SCRIPT_FILE
# "${PROJECT_SOURCE_DIR}/packaging/linux/redhat/kni.postun")

include(CPack)
