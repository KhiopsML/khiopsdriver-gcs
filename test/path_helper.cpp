#include "path_helper.h"

#if defined(_WIN32)
#include <Shlwapi.h>
#include <io.h>
#include <windows.h>

#define access _access_s
#endif

#ifdef __APPLE__
#include <libgen.h>
#include <limits.h>
#include <mach-o/dyld.h>
#include <unistd.h>
#endif

#ifdef __linux__
#include <libgen.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>
#define PROC_SELF_EXE "/proc/self/exe"

#endif

namespace MyPaths {

#if defined(_WIN32)

std::string getExecutablePath() {
  char rawPathName[MAX_PATH];
  GetModuleFileName(NULL, rawPathName, MAX_PATH);
  return std::string(rawPathName);
}

std::string getExecutableDir() {
  std::string executablePath = getExecutablePath();
  char *exePath = new char[executablePath.length()];
  strcpy(exePath, executablePath.c_str());
  PathRemoveFileSpec(exePath);
  std::string directory = std::string(exePath);
  delete[] exePath;
  return directory;
}

std::string mergePaths(std::string pathA, std::string pathB) {
  char combined[MAX_PATH];
  PathCombine(combined, pathA.c_str(), pathB.c_str());
  std::string mergedPath(combined);
  return mergedPath;
}

#endif

#ifdef __linux__

std::string getExecutablePath() {
  char rawPathName[PATH_MAX];
  if (realpath(PROC_SELF_EXE, rawPathName))
    return std::string(rawPathName);

  return NULL;
}

#endif

#ifdef __APPLE__
std::string getExecutablePath() {
  char rawPathName[PATH_MAX];
  char realPathName[PATH_MAX];
  uint32_t rawPathSize = (uint32_t)sizeof(rawPathName);

  if (!_NSGetExecutablePath(rawPathName, &rawPathSize)) {
    realpath(rawPathName, realPathName);
  }
  return std::string(realPathName);
}
#endif

#if defined(__linux__) || defined(__APPLE__)
std::string getExecutableDir() {
  std::string executablePath = getExecutablePath();
  return MyPaths::dirname(executablePath);
}

std::string dirname(std::string path) {
  char *pathBuffer = new char[path.length() + 1];
  strcpy(pathBuffer, path.c_str());
  std::string pathDir = std::string(::dirname(pathBuffer));
  delete[] pathBuffer;
  return pathDir;
}

std::string mergePaths(std::string pathA, std::string pathB) {
  return pathA + "/" + pathB;
}
#endif

} // namespace MyPaths
