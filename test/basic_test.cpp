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

class GCSDriverTestFixture : public ::testing::Test
{
public:
	static void SetUpTestSuite()
	{
		ASSERT_FALSE(driver_connect());
	}

	static void TearDownTestSuite()
	{
		ASSERT_FALSE(driver_disconnect());
	}
};

TEST_F(GCSDriverTestFixture, FileExists)
{
	//single file
	ASSERT_TRUE(driver_fileExists("gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/Adult-split-000000000000.txt"));

	//file glob
	ASSERT_TRUE(driver_fileExists("gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**"));

	//with dir
	ASSERT_FALSE(driver_fileExists("gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/"));
}

TEST_F(GCSDriverTestFixture, GetFileSize)
{
	constexpr const char* test_file = "gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt";
	constexpr long long expected_size{ 5585568 };
	ASSERT_EQ(driver_getFileSize(test_file), expected_size);

	//multifile _ one header line
	constexpr const char* test_multifile_range = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult/Adult-split-0[0-5].txt";
	constexpr long long expected_size_range{ 5634411 };
	ASSERT_EQ(driver_getFileSize(test_multifile_range), expected_size_range);

	//multifile _ header line at each file
	constexpr const char* test_multifile_glob = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/*.txt";
	constexpr long long expected_size_glob{ 5585576 };
	ASSERT_EQ(driver_getFileSize(test_multifile_glob), expected_size_glob);

	//mutifile _ one header _ double star glob pattern
	constexpr const char* test_doublestar_glob = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**/Adult-split-*.txt";
	//sample has same size as multifile _ one header line
	ASSERT_EQ(driver_getFileSize(test_doublestar_glob), expected_size_range);
}