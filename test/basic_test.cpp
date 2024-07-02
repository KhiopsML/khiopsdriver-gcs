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

TEST(GCSDriverTest, Connect)
{
	//check connection state before call to connect
	ASSERT_FALSE(driver_isConnected());

	//call connect and check connection
	ASSERT_FALSE(driver_connect());
	ASSERT_TRUE(driver_isConnected());

	//call disconnect and check connection
	ASSERT_FALSE(driver_disconnect());
	ASSERT_FALSE(driver_isConnected());
}

TEST(GCSDriverTest, Disconnect)
{
	ASSERT_FALSE(driver_connect());
	ASSERT_FALSE(driver_disconnect());
	ASSERT_FALSE(driver_isConnected());
}

TEST(GCSDriverTest, GetFileSize)
{
	ASSERT_FALSE(driver_connect());
	ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt"), 5585568);
	ASSERT_FALSE(driver_disconnect());
}

TEST(GCSDriverTest, GetSystemPreferredBufferSize)
{
	ASSERT_EQ(driver_getSystemPreferredBufferSize(), 4 * 1024 * 1024);
}
