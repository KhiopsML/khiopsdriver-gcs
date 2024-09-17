include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "v1.14.0")

set(gtest_force_shared_crt
    ON
    CACHE BOOL "" FORCE)
#[==[
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TARGET gtest PROPERTY MSVC_RUNTIME_LIBRARY MultiThreaded)
    set_property(TARGET gtest_main PROPERTY MSVC_RUNTIME_LIBRARY MultiThreaded)
else()
    set_property(TARGET gtest PROPERTY MSVC_RUNTIME_LIBRARY MultiThreadedDebug)
    set_property(TARGET gtest_main PROPERTY MSVC_RUNTIME_LIBRARY MultiThreadedDebug)
endif()]==]

FetchContent_MakeAvailable(googletest)
# Include and enable googletest
include(GoogleTest)
enable_testing()

add_subdirectory(test)
