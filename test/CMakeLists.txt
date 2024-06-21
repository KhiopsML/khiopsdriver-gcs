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

add_executable(plugin_test plugin_test.cpp path_helper.cpp)
target_compile_options(plugin_test
	PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall>
	PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>
)
if(WIN32)
  # custom build and test execution for Windows
  target_link_libraries(plugin_test PRIVATE gtest ${CMAKE_DL_LIBS} Shlwapi) # Link to dl
  add_custom_command(
    TARGET plugin_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_RUNTIME_DLLS:khiopsdriver_file_gcs> $<TARGET_FILE_DIR:plugin_test>
    COMMAND_EXPAND_LISTS
  )
else ()
  target_link_libraries(plugin_test PRIVATE gtest ${CMAKE_DL_LIBS}) # Link to dl
endif (MSVC)

gtest_discover_tests(plugin_test DISCOVERY_MODE PRE_TEST)