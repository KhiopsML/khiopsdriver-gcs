#include <gtest/gtest.h>
#include "../src/gcsplugin.h"

TEST(DriverMetadataTest, GetDriverName) {
  ASSERT_STREQ(driver_getDriverName(), "GCS driver");
}

TEST(DriverMetadataTest, GetVersion) {
  ASSERT_STREQ(driver_getVersion(), DRIVER_VERSION);
}

TEST(DriverMetadataTest, GetScheme) { ASSERT_STREQ(driver_getScheme(), "gs"); }