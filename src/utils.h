#include "google/cloud/storage/client.h"

namespace gc = ::google::cloud;

// Helper function to validate globbing pattern
gc::StatusOr<std::pair<std::string, std::string>>
ParseGlobbingPattern(const std::string &pattern);

// Helper function to validate relative path
bool IsRelativePath(const char *path);

// Helper function to generate sequence number string
std::string GenerateSequenceNumber(size_t index);
