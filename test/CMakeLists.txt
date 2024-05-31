add_executable(basic_test "")

target_sources( basic_test
	PRIVATE
	basic_test.cpp
	../src/plugin_handler.cpp
	../src/plugin_handler.h
)

target_compile_options(basic_test
	PRIVATE $<$<CXX_COMPILER_ID:MSVC>:-Wall>
)

target_link_libraries(basic_test GTest::gtest)

gtest_discover_tests(basic_test
	EXTRA_ARGS foo
)