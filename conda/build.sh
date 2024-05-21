#!/bin/bash

# Set-up the shell to behave more like a general-purpose programming language
set -euo pipefail

# Configure project
cmake --fresh --preset ninja-multi

# Build 
cmake --build --preset ninja-release --target khiopsdriver_file_gcs

# Copy binary to conda package
mkdir -p $PREFIX/bin
cp ./builds/ninja-multi/Release/libkhiopsdriver_file_gcs.so "$PREFIX/bin/"



