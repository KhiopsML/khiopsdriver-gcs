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

project(khiops-gcs LANGUAGES CXX)

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
  set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/modules/)
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
target_compile_options(
  khiopsdriver_file_gcs
  PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall;/wd4101;/wd4710;/wd4711>
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
