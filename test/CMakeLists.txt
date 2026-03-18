# Fetch googletest from its Git repo
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "v1.14.0")

set(gtest_force_shared_crt
    ON
    CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

set(STORAGE_DRIVER_TEST_URL_PREFIX_FOR_TESTS "$ENV{STORAGE_DRIVER_TEST_URL_PREFIX}")
if(STORAGE_DRIVER_TEST_URL_PREFIX_FOR_TESTS STREQUAL "")
  set(STORAGE_DRIVER_TEST_URL_PREFIX_FOR_TESTS "gs://data-test-khiops-driver-gcs")
endif()

# Find dependencies
find_package(Boost CONFIG REQUIRED)
add_executable(gcs_test mocking_test.cpp nonmocking_test.cpp oauth2_token_manager_test.cpp test_drivermetadata.cpp
                        ../src/oauth2_token_manager.cpp ../src/gcsplugin.h)

target_compile_options(
  gcs_test
  PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/W4;/wd4101;/wd4625;/wd4710;/wd4711;/wd4365;/wd5045>
  PRIVATE $<$<CXX_COMPILER_ID:AppleClang>:-Wall;-Wextra;-pedantic;-Wno-unused-lambda-capture>
  PRIVATE $<$<CXX_COMPILER_ID:Clang,GNU>:-Wall;-Wextra;-pedantic>)

# MSVC requires /bigobj to build in debug config
target_compile_options(gcs_test PRIVATE $<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:MSVC>>:/bigobj>)

target_include_directories(gcs_test PRIVATE ${${PROJECT_NAME}_SOURCE_DIR}/src)

target_link_libraries(gcs_test PRIVATE GTest::gtest GTest::gmock GTest::gmock_main google-cloud-cpp::storage
                                       khiopsdriver_file_gcs)

gtest_discover_tests(
  gcs_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  PROPERTIES LABELS "unit" ENVIRONMENT "STORAGE_DRIVER_TEST_URL_PREFIX=${STORAGE_DRIVER_TEST_URL_PREFIX_FOR_TESTS}")

add_executable(plugin_test plugin_test.cpp path_helper.cpp)
target_include_directories(plugin_test PRIVATE ${${PROJECT_NAME}_SOURCE_DIR}/src)
target_compile_options(
  plugin_test
  PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/W4;/wd4101;/wd4710;/wd4711;/wd4365;/wd5045>
  PRIVATE $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-pedantic>)
if(WIN32)
  # custom build and test execution for Windows
  target_link_libraries(plugin_test PRIVATE gtest ${CMAKE_DL_LIBS} Shlwapi) # Link to dl
  add_custom_command(
    TARGET plugin_test
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_RUNTIME_DLLS:khiopsdriver_file_gcs>
            $<TARGET_FILE_DIR:plugin_test>
    COMMAND_EXPAND_LISTS)
else()
  target_link_libraries(plugin_test PRIVATE gtest ${CMAKE_DL_LIBS}) # Link to dl
endif()

gtest_discover_tests(
  plugin_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  PROPERTIES LABELS "unit" ENVIRONMENT "STORAGE_DRIVER_TEST_URL_PREFIX=${STORAGE_DRIVER_TEST_URL_PREFIX_FOR_TESTS}")
