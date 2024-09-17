#!/bin/bash

# Set-up the shell to behave more like a general-purpose programming language
set -euo pipefail

# Configure project
cmake --fresh -G Ninja -D CMAKE_BUILD_TYPE=Release -D CMAKE_TOOLCHAIN_FILE=vcpkg/scripts/buildsystems/vcpkg.cmake -B conda-build -S .

# Build
cmake --build conda-build --target khiopsdriver_file_gcs

# Copy binary to conda package
#mkdir -p $PREFIX/bin
cmake --install conda-build --prefix $PREFIX




