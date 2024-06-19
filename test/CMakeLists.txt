# Fetch googletest from its Git repo
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "v1.14.0")

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
  
include(GoogleTest)

add_executable(basic_test basic_test.cpp)

target_compile_options(basic_test
	PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall>
	PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>
)
target_include_directories(basic_test
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
         ${${PROJECT_NAME}_SOURCE_DIR}/src)
target_link_libraries(basic_test PRIVATE gtest khiopsdriver_file_gcs)

gtest_discover_tests(basic_test)