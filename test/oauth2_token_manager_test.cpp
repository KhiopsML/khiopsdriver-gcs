#include <gtest/gtest.h>

#include "oauth2_token_manager.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <nlohmann/json.hpp>

namespace {

std::string MakeTempTokenFilePath() {
#ifdef _WIN32
  char temp_value[2048];
  size_t len = 0;
  getenv_s(&len, temp_value, 2048, "TEMP");
  std::ostringstream os;
  os << temp_value << "\\oauth2-token-" << boost::uuids::random_generator()()
     << ".json";
  return os.str();
#else
  std::ostringstream os;
  os << "/tmp/oauth2-token-" << boost::uuids::random_generator()() << ".json";
  return os.str();
#endif
}

void WriteJsonFile(const std::string &path, const nlohmann::json &content) {
  std::ofstream out(path);
  ASSERT_TRUE(out.is_open()) << "Failed to create file: " << path;
  out << content.dump(2);
}

class OAuth2TokenManagerTest : public ::testing::Test {
protected:
  std::string token_file_path_;

  void SetUp() override { token_file_path_ = MakeTempTokenFilePath(); }

  void TearDown() override { std::remove(token_file_path_.c_str()); }
};

} // namespace

TEST_F(OAuth2TokenManagerTest, ConstructorMissingFileThrows) {
  std::remove(token_file_path_.c_str());

  try {
    OAuth2TokenManager manager(token_file_path_);
    (void)manager;
    FAIL() << "Expected runtime_error for missing file";
  } catch (const std::runtime_error &e) {
    EXPECT_NE(std::string(e.what()).find("Could not open token file"),
              std::string::npos);
  }
}

TEST_F(OAuth2TokenManagerTest, ConstructorInvalidExpiryThrows) {
  nlohmann::json token_data = {
      {"token", "token_value"},
      {"refresh_token", "refresh_value"},
      {"token_uri", "https://example.com/token"},
      {"client_id", "client-id"},
      {"client_secret", "client-secret"},
      {"expiry", "invalid-date-format"},
  };
  WriteJsonFile(token_file_path_, token_data);

  try {
    OAuth2TokenManager manager(token_file_path_);
    (void)manager;
    FAIL() << "Expected runtime_error for invalid expiry";
  } catch (const std::runtime_error &e) {
    EXPECT_NE(std::string(e.what()).find("Failed to parse expiry time"),
              std::string::npos);
  }
}

TEST_F(OAuth2TokenManagerTest, GetAccessTokenReturnsLoadedTokenWhenNotExpired) {
  nlohmann::json token_data = {
      {"token", "token_value"},
      {"refresh_token", "refresh_value"},
      {"token_uri", "https://example.com/token"},
      {"client_id", "client-id"},
      {"client_secret", "client-secret"},
      {"expiry", "2099-01-01T00:00:00Z"},
  };
  WriteJsonFile(token_file_path_, token_data);

  OAuth2TokenManager manager(token_file_path_);

  EXPECT_EQ(manager.GetAccessToken(), "token_value");
}

TEST_F(OAuth2TokenManagerTest,
       ExpiredTokenWithoutRefreshTokenReturnsCurrentToken) {
  nlohmann::json token_data = {
      {"token", "stale_token"},
      {"token_uri", "https://example.com/token"},
      {"client_id", "client-id"},
      {"client_secret", "client-secret"},
      {"expiry", "2000-01-01T00:00:00Z"},
  };
  WriteJsonFile(token_file_path_, token_data);

  OAuth2TokenManager manager(token_file_path_);

  // With no refresh token available, refresh is skipped and existing token is
  // returned unchanged.
  EXPECT_EQ(manager.GetAccessToken(), "stale_token");
}

TEST_F(OAuth2TokenManagerTest, MissingExpiryStillAllowsTokenAccess) {
  nlohmann::json token_data = {
      {"token", "token_no_expiry"},
      {"token_uri", "https://example.com/token"},
      {"client_id", "client-id"},
      {"client_secret", "client-secret"},
  };
  WriteJsonFile(token_file_path_, token_data);

  OAuth2TokenManager manager(token_file_path_);

  EXPECT_EQ(manager.GetAccessToken(), "token_no_expiry");
}

TEST_F(OAuth2TokenManagerTest, MakeCredentialsReturnsNonNull) {
  nlohmann::json token_data = {
      {"token", "token_value"},
      {"refresh_token", "refresh_value"},
      {"token_uri", "https://example.com/token"},
      {"client_id", "client-id"},
      {"client_secret", "client-secret"},
      {"expiry", "2099-01-01T00:00:00Z"},
  };
  WriteJsonFile(token_file_path_, token_data);

  OAuth2TokenManager manager(token_file_path_);

  auto creds = manager.MakeCredentials();
  EXPECT_NE(creds, nullptr);
}
