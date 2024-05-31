#include <gtest/gtest.h>

#include <iostream>

#include "../src/gcsplugin.h"



TEST(GCSDriverTest, GetDriverName)
{
	ASSERT_STREQ(driver_getDriverName(), "GCS driver");
}

TEST(GCSDriverTest, GetVersion)
{
	ASSERT_STREQ(driver_getVersion(), "0.1.0");
}

TEST(GCSDriverTest, GetScheme)
{
	ASSERT_STREQ(driver_getScheme(), "gs");
}

TEST(GCSDriverTest, IsReadOnly)
{
	ASSERT_FALSE(driver_isReadOnly());
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
