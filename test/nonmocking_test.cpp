#include "gcsplugin.h"
#include "gcsplugin_internal.h"

#include <array>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>

#include <boost/process/v1/environment.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <gtest/gtest.h>

using namespace gcsplugin;

namespace gc = ::google::cloud;
namespace gcs = gc::storage;

using LOReturnType = gc::StatusOr<gcs::internal::ListObjectsResponse>;

#ifndef _WIN32
// Setting of environment variables does not work on Windows
TEST(GCSDriverTest, DriverConnectMissingCredentialsFailure) {
  auto env = boost::this_process::environment();
  env["GCP_TOKEN"] = "/tmp/notoken.json";
  ASSERT_EQ(driver_connect(), kOtherFailure);
  env.erase("GCP_TOKEN");
}

void setup_bad_credentials() {
  std::stringstream tempCredsFile;
#ifdef _WIN32
  tempCredsFile << std::getenv("TEMP") << "\\creds-"
                << boost::uuids::random_generator()() << ".json";
#else
  tempCredsFile << "/tmp/creds-" << boost::uuids::random_generator()()
                << ".json";
#endif
  std::ofstream outfile(tempCredsFile.str());
  outfile << "{}" << std::endl;
  outfile.close();
  auto env = boost::this_process::environment();
  env["GCP_TOKEN"] = tempCredsFile.str();
}

void cleanup_bad_credentials() {
  auto env = boost::this_process::environment();
  env.erase("GCP_TOKEN");
}

TEST(GCSDriverTest, GetFileSizeInvalidCredentialsFailure) {
  setup_bad_credentials();
  ASSERT_EQ(driver_connect(), kOtherSuccess);
  ASSERT_EQ(driver_getFileSize("gs://data-test-khiops-driver-gcs/khiops_data/"
                               "samples/Adult/Adult.txt"),
            -1);
  ASSERT_STRNE(driver_getlasterror(), NULL);
  ASSERT_EQ(driver_disconnect(), kOtherSuccess);
  cleanup_bad_credentials();
}
#endif

TEST(GCSDriverTest, Concat) {
  constexpr size_t nsources = 6;
  const char *sources[nsources] = {
      "khiops_data/split/Adult/Adult-split-00.txt",
      "khiops_data/split/Adult/Adult-split-01.txt",
      "khiops_data/split/Adult/Adult-split-02.txt",
      "khiops_data/split/Adult/Adult-split-03.txt",
      "khiops_data/split/Adult/Adult-split-04.txt",
      "khiops_data/split/Adult/Adult-split-05.txt"};

  const std::string bucket = "data-test-khiops-driver-gcs";

  // Generate unique temporary path prefix using UUID
  std::string temp_prefix =
      (std::ostringstream()
       << "tmp_test_concat_" << boost::uuids::random_generator()() << "/")
          .str();

  std::string outputAsString =
      (std::ostringstream() << "gs://" << bucket << "/" << temp_prefix
                            << "driver_concat_test_output")
          .str();

  const char *output = outputAsString.c_str();
  const char *reference = "gs://data-test-khiops-driver-gcs/khiops_data/"
                          "samples/Adult/Adult.txt";

  ASSERT_EQ(driver_connect(), kOtherSuccess) << "Failed to connect";

  // Copy source files to temporary location using test_copyObject
  std::vector<std::string> relative_temp_paths;
  relative_temp_paths.reserve(nsources); // ✅ IMPORTANT: réserver la capacité

  for (size_t i = 0; i < nsources; ++i) {
    // Source object path (relative to bucket)
    std::string source_object = sources[i];

    // Destination object path (relative to bucket, in temp prefix)
    std::string temp_object =
        temp_prefix + "source_" + std::to_string(i) + ".txt";

    // Copy using test helper function
    int copy_status = test_copyObject(bucket,        // source bucket
                                      source_object, // source object
                                      bucket,     // destination bucket (same)
                                      temp_object // destination object
    );

    ASSERT_TRUE(!copy_status)
        << "Failed to copy source " << i << " to temporary location.";

    // Store the source path for concat
    relative_temp_paths.push_back("gs://" + bucket + "/" + temp_object);
  }

  // ✅ Construire le tableau de pointeurs APRÈS avoir fini d'ajouter tous les
  // strings
  std::vector<const char *> temp_path_ptrs;
  temp_path_ptrs.reserve(nsources);

  for (const auto &path : relative_temp_paths) {
    temp_path_ptrs.push_back(path.c_str());
  }

  ASSERT_EQ(driver_fileExists(output), kFalse)
      << "The output file exists before concatenation";

  // Concatenate using relative paths
  ASSERT_EQ(driver_concat(output, temp_path_ptrs.data(), nsources),
            kOtherSuccess)
      << "Concatenation failed";

  // Verify source files were deleted after concatenation
  for (const auto &relative_temp_path : relative_temp_paths) {
    std::string temp_full_path =
        std::string("gs://") + bucket + "/" + relative_temp_path;
    ASSERT_EQ(driver_fileExists(temp_full_path.c_str()), kFalse)
        << "Source file " << temp_full_path
        << " was not deleted after concatenation";
  }

  ASSERT_EQ(driver_fileExists(output), kTrue)
      << "The concatenation created no output file";

  ASSERT_EQ(driver_getFileSize(output), driver_getFileSize(reference))
      << "Incorrect output file size";

  // Clean up: remove output file
  ASSERT_EQ(driver_remove(output), kOtherSuccess)
      << "Failed to remove output file";

  ASSERT_EQ(driver_fileExists(output), kFalse)
      << "Output file still exists after removal";

  ASSERT_EQ(driver_disconnect(), kOtherSuccess) << "Failed to disconnect";
}

TEST(GCSDriverTest, ComposeMultifile) {
  constexpr size_t nsources = 6;
  const char *sources[nsources] = {
      "khiops_data/split/Adult/Adult-split-00.txt",
      "khiops_data/split/Adult/Adult-split-01.txt",
      "khiops_data/split/Adult/Adult-split-02.txt",
      "khiops_data/split/Adult/Adult-split-03.txt",
      "khiops_data/split/Adult/Adult-split-04.txt",
      "khiops_data/split/Adult/Adult-split-05.txt"};

  const std::string bucket = "data-test-khiops-driver-gcs";

  // Generate unique temporary path prefix using UUID
  std::string temp_prefix =
      (std::ostringstream() << "tmp_test_compose_multifile_"
                            << boost::uuids::random_generator()() << "/")
          .str();

  // Globbing pattern for output files
  std::string outputPattern =
      (std::ostringstream()
       << "gs://" << bucket << "/" << temp_prefix << "Adult-renamed-*.txt")
          .str();

  const char *output_pattern = outputPattern.c_str();

  ASSERT_EQ(driver_connect(), kOtherSuccess) << "Failed to connect";

  // Copy source files to temporary location using test_copyObject
  std::vector<std::string> relative_temp_paths;
  relative_temp_paths.reserve(nsources);

  for (size_t i = 0; i < nsources; ++i) {
    // Source object path (relative to bucket)
    std::string source_object = sources[i];

    // Destination object path (relative to bucket, in temp prefix)
    std::string temp_object =
        temp_prefix + "source_" + std::to_string(i) + ".txt";

    // Copy using test helper function
    int copy_status = test_copyObject(bucket,        // source bucket
                                      source_object, // source object
                                      bucket,     // destination bucket (same)
                                      temp_object // destination object
    );

    ASSERT_TRUE(!copy_status)
        << "Failed to copy source " << i << " to temporary location.";

    // Store the relative path for composeMultifile
    relative_temp_paths.push_back(temp_object);
  }

  // Build array of const char* AFTER all strings are in the vector
  std::vector<const char *> relative_temp_path_ptrs;
  relative_temp_path_ptrs.reserve(nsources);

  for (const auto &path : relative_temp_paths) {
    relative_temp_path_ptrs.push_back(path.c_str());
  }

  // Call composeMultifile to rename files according to pattern
  ASSERT_EQ(driver_composeMultifile(output_pattern,
                                    relative_temp_path_ptrs.data(), nsources),
            kOtherSuccess)
      << "ComposeMultifile failed";

  // Verify that source files were deleted after renaming
  for (const auto &relative_temp_path : relative_temp_paths) {
    std::string temp_full_path =
        std::string("gs://") + bucket + "/" + relative_temp_path;
    ASSERT_EQ(driver_fileExists(temp_full_path.c_str()), kFalse)
        << "Source file " << temp_full_path
        << " was not deleted after renaming";
  }

  // Verify that renamed files exist and match the pattern
  // Expected names: Adult-renamed-000000000000.txt,
  // Adult-renamed-000000000001.txt, etc.
  std::vector<std::string> expected_renamed_files;
  expected_renamed_files.reserve(nsources);

  for (size_t i = 0; i < nsources; ++i) {
    std::ostringstream oss;
    oss << temp_prefix << "Adult-renamed-" << std::setfill('0') << std::setw(12)
        << i << ".txt";
    expected_renamed_files.push_back(oss.str());
  }

  // Verify each renamed file exists
  for (const auto &renamed_file : expected_renamed_files) {
    std::string full_path = std::string("gs://") + bucket + "/" + renamed_file;
    ASSERT_EQ(driver_fileExists(full_path.c_str()), kTrue)
        << "Renamed file " << full_path << " does not exist";
  }

  // Verify file count by listing objects matching the pattern
  // Use the globbing pattern to list files
  std::string list_pattern = temp_prefix + "Adult-renamed-*.txt";
  std::string list_uri = std::string("gs://") + bucket + "/" + list_pattern;

  // Get file size to trigger listing (this will list all matching files)
  long long total_size = driver_getFileSize(list_uri.c_str());
  ASSERT_GT(total_size, 0) << "No files found matching pattern " << list_uri;

  // Alternative verification: manually count files using test_listObjects
  auto maybe_list = test_listObjects(bucket, list_pattern);
  ASSERT_TRUE(maybe_list.begin() != maybe_list.end())
      << "ListObjects returned empty result";

  size_t file_count = 0;
  for (auto it = maybe_list.begin(); it != maybe_list.end(); ++it) {
    ASSERT_TRUE(it->ok()) << "Error iterating over listed objects";
    file_count++;
  }

  ASSERT_EQ(file_count, nsources)
      << "Expected " << nsources << " renamed files, found " << file_count;

  // Clean up: remove all renamed files
  for (const auto &renamed_file : expected_renamed_files) {
    std::string full_path = std::string("gs://") + bucket + "/" + renamed_file;
    ASSERT_EQ(driver_remove(full_path.c_str()), kOtherSuccess)
        << "Failed to remove renamed file " << full_path;
  }

  // Verify cleanup
  for (const auto &renamed_file : expected_renamed_files) {
    std::string full_path = std::string("gs://") + bucket + "/" + renamed_file;
    ASSERT_EQ(driver_fileExists(full_path.c_str()), kFalse)
        << "Renamed file " << full_path << " still exists after cleanup";
  }

  // Clean up temporary directory (remove any remaining files)
  std::string temp_dir_pattern = "gs://" + bucket + "/" + temp_prefix + "*";
  driver_remove(temp_dir_pattern.c_str());

  ASSERT_EQ(driver_disconnect(), kOtherSuccess) << "Failed to disconnect";
}