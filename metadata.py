# This module allows py-build-cmake to dynamically determine package version and description.
# It does so by looking at the __version__ variable and at the module docstring (which is directly assigned using __doc__ here).

import re

with open("CMakeLists.txt") as cmlfile:
    cml = cmlfile.read()

if match := re.search(r'set\s*\(\s*CPACK_PACKAGE_DESCRIPTION\s+"([^"]*)"\s*\)', cml):
    __doc__ = match[1]
else:
    raise ValueError("could not find 'set(CPACK_PACKAGE_DESCRIPTION ...)' in CMakeLists.txt")

with open("src/gcsplugin.h") as headerfile:
    header = headerfile.read()

if match := re.search(r'#\s*define\s+DRIVER_VERSION\s+KHIOPS_STR\s*\(([^)]+)\)', header):
    __version__ = match[1]
else:
    raise ValueError("could not find '#define DRIVER_VERSION KHIOPS_STR(...)' in src/gcsplugin.h")

__all__ = ["__doc__", "__version__"]
