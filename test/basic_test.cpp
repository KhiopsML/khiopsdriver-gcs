#include <gtest/gtest.h>

#include <iostream>

#include "../src/gcsplugin.h"

constexpr int kSuccess{ 1 };
constexpr int kFailure{ 0 };

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

TEST(GCSDriverTest, Connect)
{
	//check connection state before call to connect
	ASSERT_FALSE(driver_isConnected());

	//call connect and check connection
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_TRUE(driver_isConnected());

	//call disconnect and check connection
	ASSERT_EQ(driver_disconnect(), kSuccess);
	ASSERT_FALSE(driver_isConnected());
}

TEST(GCSDriverTest, Disconnect)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_disconnect(), kSuccess);
	ASSERT_FALSE(driver_isConnected());
}

TEST(GCSDriverTest, GetFileSize)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(GCSDriverTest, GetSystemPreferredBufferSize)
{
	ASSERT_EQ(driver_getSystemPreferredBufferSize(), 4 * 1024 * 1024);
}


int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);

	//check that the arguments are effectively passed from ctest
	for (int i = 0; i < argc; i++)
	{
		std::cout << argv[i] << '\n';
	}

	return RUN_ALL_TESTS();
}
