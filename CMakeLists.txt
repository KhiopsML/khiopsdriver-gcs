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

# Find dependencies
find_package(fmt CONFIG REQUIRED)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# Hide symbols in the shared libraries
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

add_library(khiopsdriver_file_gcs MODULE src/gcsplugin.h src/gcsplugin.cpp)
target_link_options(khiopsdriver_file_gcs PRIVATE $<$<CONFIG:RELEASE>:-s>) # stripping
target_link_libraries(khiopsdriver_file_gcs PRIVATE google-cloud-cpp::storage spdlog::spdlog)

add_executable(KhiopsPluginTest src/khiopsplugintest.cpp)
target_link_libraries(KhiopsPluginTest PRIVATE fmt::fmt)

add_executable(drivertest src/drivertest.cpp)
target_link_libraries(drivertest ${CMAKE_DL_LIBS}) # Link to dl

include(generate_tests.cmake)

