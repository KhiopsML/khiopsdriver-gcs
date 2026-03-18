#include "utils.h"

// Helper function to validate globbing pattern
gc::StatusOr<std::pair<std::string, std::string>>
ParseGlobbingPattern(const std::string &pattern) {
  // Reject slash-star patterns (e.g. ".../*" or ".../*suffix")
  if (pattern.find("/*") != std::string::npos) {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Globbing pattern must not contain '/*'"};
  }

  // Find the '*' character
  size_t star_pos = pattern.find('*');
  if (star_pos == std::string::npos) {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Globbing pattern must contain '*' character"};
  }

  // Check for multiple '*'
  if (pattern.find('*', star_pos + 1) != std::string::npos) {
    return gc::Status{
        gc::StatusCode::kInvalidArgument,
        "Globbing pattern must contain exactly one '*' character"};
  }

  std::string prefix = pattern.substr(0, star_pos);
  std::string suffix = pattern.substr(star_pos + 1);

  // Validate prefix contains bucket name (must have gs:// and at least one more
  // character)
  if (prefix.length() < 6 || prefix.substr(0, 5) != "gs://") {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Prefix must start with 'gs://' and contain bucket name"};
  }

  // Prefix must not end with '/'
  if (!prefix.empty() && prefix.back() == '/') {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Prefix must not end with '/'"};
  }

  // Check that prefix doesn't end with a digit
  if (!prefix.empty() &&
      std::isdigit(static_cast<unsigned char>(prefix.back()))) {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Prefix must not end with a digit"};
  }

  // Check that suffix doesn't start with a digit
  if (!suffix.empty() &&
      std::isdigit(static_cast<unsigned char>(suffix.front()))) {
    return gc::Status{gc::StatusCode::kInvalidArgument,
                      "Suffix must not start with a digit"};
  }

  return std::make_pair(prefix, suffix);
}

// Helper function to validate relative path
bool IsRelativePath(const char *path) {
  if (!path) {
    return false;
  }

  std::string path_str(path);

  // Check for gs:// prefix
  if (path_str.find("gs://") != std::string::npos) {
    return false;
  }

  // Check for absolute path indicators
  if (!path_str.empty() && path_str[0] == '/') {
    return false;
  }

#ifdef _WIN32
  // Check for Windows absolute paths (e.g., C:\, D:\)
  if (path_str.length() >= 2 && path_str[1] == ':') {
    return false;
  }
#endif

  return true;
}

// Helper function to generate sequence number string
std::string GenerateSequenceNumber(size_t index) {
  std::ostringstream oss;
  oss << std::setfill('0') << std::setw(12) << index;
  return oss.str();
}
