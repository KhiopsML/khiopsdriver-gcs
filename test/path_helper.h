#pragma once

#include <string>
namespace MyPaths {

std::string getExecutablePath();
std::string getExecutableDir();
std::string dirname(std::string path);
std::string mergePaths(std::string pathA, std::string pathB);
bool checkIfFileExists(const std::string &filePath);

} // namespace MyPaths