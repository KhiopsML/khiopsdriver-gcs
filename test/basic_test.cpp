#include <gtest/gtest.h>

#include <array>
#include <iostream>

#include "../src/gcsplugin.h"

constexpr int kSuccess{ 0 };
constexpr int kFailure{ 1 };

constexpr int kFalse{ 0 };
constexpr int kTrue{ 0 };


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
	ASSERT_EQ(driver_isReadOnly(), kTrue);
}

TEST(GCSDriverTest, Connect)
{
	//check connection state before call to connect
	ASSERT_EQ(driver_isConnected(), kFalse);

	//call connect and check connection
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_isConnected(), kTrue);

	//call disconnect and check connection
	ASSERT_EQ(driver_disconnect(), kSuccess);
	ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(GCSDriverTest, Disconnect)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_disconnect(), kSuccess);
	ASSERT_EQ(driver_isConnected(), kFalse);
}

TEST(GCSDriverTest, GetMultipartFileSize)
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/Adult-split-00000000000*.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}

TEST(GCSDriverTest, GetSystemPreferredBufferSize)
{
	ASSERT_EQ(driver_getSystemPreferredBufferSize(), 4 * 1024 * 1024);
}

constexpr const char* test_dir_name = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/";

constexpr const char* test_single_file = "gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt";
constexpr const char* test_range_file_one_header = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult/Adult-split-0[0-5].txt";
constexpr const char* test_glob_file_header_each = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/*.txt";
constexpr const char* test_double_glob_header_each = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**/Adult-split-*.txt";

constexpr std::array<const char*, 4> test_files = {
	test_single_file,
	test_range_file_one_header,
	test_glob_file_header_each,
	test_double_glob_header_each
};


class GCSDriverTestFixture : public ::testing::Test
{
	ASSERT_EQ(driver_connect(), kSuccess);
	ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/samples/Adult/Adult.txt"), 5585568);
	ASSERT_EQ(driver_disconnect(), kSuccess);
}

class GCSDriverTestFixture : public ::testing::Test {
public:
	static void SetUpTestSuite()
	{
		ASSERT_EQ(driver_connect(), kSuccess);
	}

	static void TearDownTestSuite()
	{
		ASSERT_EQ(driver_disconnect(), kSuccess);
	}
};

TEST_F(GCSDriverTestFixture, FileExists)
{
	for (const char* file : test_files)
	{
		ASSERT_EQ(driver_fileExists(file), kTrue);
	}
	
	ASSERT_EQ(driver_fileExists(test_dir_name), kFalse);

	ASSERT_EQ(driver_fileExists(nullptr), kFalse);
}

TEST_F(GCSDriverTestFixture, DirExists)
{
	ASSERT_EQ(driver_dirExists("any_name"), kTrue);

	ASSERT_EQ(driver_dirExists(nullptr), kFalse);
}

TEST_F(GCSDriverTestFixture, GetFileSize)
{
	constexpr std::array<long long, 4> expected_sizes = {
		5585568,	// samples/Adult/Adult.txt
		5634411,	// split/Adult/Adult-split-0[0-5].txt
		5585576,	// bq_export/Adult/*.txt
		5634411		// split/Adult_subsplit/**/Adult-split-*.txt
	};

	for (size_t i = 0; i < test_files.size(); i++)
	{
		ASSERT_EQ(driver_getFileSize(test_files[i]), expected_sizes[i]);
	}

	//multifile _ header line at each file
	constexpr const char* test_multifile_glob = "gs://data-test-khiops-driver-gcs/khiops_data/bq_export/Adult/*.txt";
	constexpr long long expected_size_glob{ 5585576 };
	ASSERT_EQ(driver_getFileSize(test_multifile_glob), expected_size_glob);

	//mutifile _ one header _ double star glob pattern
	constexpr const char* test_doublestar_glob = "gs://data-test-khiops-driver-gcs/khiops_data/split/Adult_subsplit/**/Adult-split-*.txt";
	//sample has same size as multifile _ one header line
	ASSERT_EQ(driver_getFileSize(test_doublestar_glob), expected_size_range);
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