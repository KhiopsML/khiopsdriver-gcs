#include "oauth2_token_manager.h"
#include <curl/curl.h>
#include <fstream>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>

using json = nlohmann::json;

// Helper function for CURL requests
size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                     std::string *s) {
  size_t newLength = size * nmemb;
  s->append((char *)contents, newLength);
  return newLength;
}

OAuth2TokenManager::OAuth2TokenManager(const std::string &token_file_path, const std::string &certificate_path)
    : token_file_path_(token_file_path), certificate_path_(certificate_path) {
  // Load the token data
  LoadTokenData();
}

std::string OAuth2TokenManager::GetAccessToken() {
  // Check if token is expired and refresh if needed
  if (IsTokenExpired()) {
    RefreshAccessToken();
  }
  return access_token_;
}

std::shared_ptr<google::cloud::Credentials>
OAuth2TokenManager::MakeCredentials() {
  std::chrono::system_clock::time_point expiration =
      std::chrono::system_clock::now() + std::chrono::hours(1);

  return google::cloud::MakeAccessTokenCredentials(GetAccessToken(),
                                                   expiration);
}

void OAuth2TokenManager::LoadTokenData() {
  std::ifstream file(token_file_path_);
  if (!file.is_open()) {
    throw std::runtime_error("Could not open token file: " + token_file_path_);
  }

  json token_data = json::parse(file);

  // Extract all the fields from the token file
  access_token_ = token_data.value("token", "");
  // Only assign refresh_token if it exists
  if (token_data.contains("refresh_token")) {
    refresh_token_ = token_data["refresh_token"];
  } else {
    refresh_token_.clear(); // No refresh token available
  }
  token_uri_ = token_data.value("token_uri", "");
  client_id_ = token_data.value("client_id", "");
  client_secret_ = token_data.value("client_secret", "");

  // Parse the expiry time
  std::string expiry_str = token_data.value("expiry", "");
  if (!expiry_str.empty()) {
    std::tm tm = {};
    std::istringstream ss(expiry_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    if (ss.fail()) {
      throw std::runtime_error("Failed to parse expiry time: " + expiry_str);
    }
    token_expiry_ = std::chrono::system_clock::from_time_t(std::mktime(&tm));
  } else {
    // Handle missing expiry if necessary
    token_expiry_ = std::chrono::system_clock::now();
  }
}

bool OAuth2TokenManager::IsTokenExpired() {
  auto now = std::chrono::system_clock::now();
  // Add a 5-minute buffer to ensure we refresh before expiration
  return now > (token_expiry_ - std::chrono::minutes(5));
}

void OAuth2TokenManager::RefreshAccessToken() {
  if (refresh_token_.empty()) return;

  CURL *curl = curl_easy_init();
  if (!curl) throw std::runtime_error("Failed to initialize CURL");

  std::string post_fields =
      "client_id=" + client_id_ + "&client_secret=" + client_secret_ +
      "&refresh_token=" + refresh_token_ + "&grant_type=refresh_token";

  std::string response;

  curl_easy_setopt(curl, CURLOPT_URL, token_uri_.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_fields.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

  if (!certificate_path_.empty()) {
    curl_easy_setopt(curl, CURLOPT_CAINFO, certificate_path_.c_str());
  }

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    std::string err = curl_easy_strerror(res);
    curl_easy_cleanup(curl);
    throw std::runtime_error("Failed to refresh access token: " + err);
  }
  curl_easy_cleanup(curl);

  json response_data = json::parse(response);
  access_token_ = response_data.value("access_token", "");
  int expires_in = response_data.value("expires_in", 0);
  token_expiry_ = std::chrono::system_clock::now() + std::chrono::seconds(expires_in);
  UpdateTokenFile(expires_in);
}

void OAuth2TokenManager::UpdateTokenFile(int expires_in) {
  // Read the current token file
  std::ifstream input_file(token_file_path_);
  if (!input_file.is_open()) {
    throw std::runtime_error("Could not open token file for updating");
  }

  json token_data = json::parse(input_file);
  input_file.close();

  // Update the token and expiry
  token_data["token"] = access_token_;

  // Calculate new expiry time
  auto expiry_time =
      std::chrono::system_clock::now() + std::chrono::seconds(expires_in);
  auto expiry_time_t = std::chrono::system_clock::to_time_t(expiry_time);
  std::tm *tm_ptr = std::gmtime(&expiry_time_t);

  char expiry_buf[30];
  std::strftime(expiry_buf, sizeof(expiry_buf), "%Y-%m-%dT%H:%M:%SZ", tm_ptr);
  token_data["expiry"] = std::string(expiry_buf);

  // Write the updated token data back to the file
  std::ofstream output_file(token_file_path_);
  if (!output_file.is_open()) {
    throw std::runtime_error("Could not open token file for writing");
  }

  output_file << token_data.dump(2);
}
