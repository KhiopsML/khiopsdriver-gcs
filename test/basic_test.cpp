#include <gtest/gtest.h>

#include <iostream>

#include "../src/plugin_handler.h"

using namespace plgh;

TEST(PluginHandleSimpleTest, Ctor)
{
	const PluginHandle gcs_handle{ "D:\\Users\\cedric.lecam\\dev\\khiopsdriver-gcs\\builds\\ninja-multi-vcpkg\\test\\Debug\\khiopsdriver_file_gcs.dll" };
	ASSERT_TRUE(gcs_handle.IsValid());
}

class PluginHandleTest : public testing::Test
{
protected:
	PluginHandleTest()
		: gcs_handle_{ driver_path_ }
	{}

	void SetUp() override
	{
		ASSERT_TRUE(gcs_handle_.IsValid());
	}

	static constexpr char* driver_path_{ "D:\\Users\\cedric.lecam\\dev\\khiopsdriver-gcs\\builds\\ninja-multi-vcpkg\\test\\Debug\\khiopsdriver_file_gcs.dll" };
	PluginHandle gcs_handle_;
};

TEST_F(PluginHandleTest, GetDriverName)
{
	ASSERT_STREQ(gcs_handle_.ptr_driver_getDriverName(), "GCS driver");
}

TEST_F(PluginHandleTest, GetVersion)
{
	ASSERT_STREQ(gcs_handle_.ptr_driver_getVersion(), "0.1.0");
}

TEST_F(PluginHandleTest, GetScheme)
{
	ASSERT_STREQ(gcs_handle_.ptr_driver_getScheme(), "gs");
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);

	//check that the arguments are effectively passed from ctest
	for (int i = 0; i < argc; i++)
	{
		std::cout << argv[i] << '\n';
	}

	auto arg1 = argv[1];
	std::cout << arg1 << '\n';

	return RUN_ALL_TESTS();
}
