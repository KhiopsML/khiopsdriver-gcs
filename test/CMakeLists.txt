# Fetch googletest from its Git repo
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "v1.13.0")

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Workaround for windows: https://github.com/actions/virtual-environments/issues/5900
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_property(TARGET gtest PROPERTY MSVC_RUNTIME_LIBRARY MultiThreaded)
    set_property(TARGET gtest_main PROPERTY MSVC_RUNTIME_LIBRARY MultiThreaded)
else()
    set_property(TARGET gtest PROPERTY MSVC_RUNTIME_LIBRARY MultiThreadedDebug)
    set_property(TARGET gtest_main PROPERTY MSVC_RUNTIME_LIBRARY MultiThreadedDebug)
endif()
  
include(GoogleTest)

add_executable(basic_test basic_test.cpp)

target_compile_options(basic_test
	PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall>
	PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>
)
set_options(basic_test)
target_include_directories(basic_test
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
         ${${PROJECT_NAME}_SOURCE_DIR}/src)
target_link_libraries(basic_test PRIVATE gtest gtest_main gmock gmock_main khiopsdriver_file_gcs)

gtest_discover_tests(basic_test)