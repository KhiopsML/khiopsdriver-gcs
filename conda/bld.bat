REM Echo all output
@echo on

REM We need to use the subst command to shorten the paths:  
REM "aws-sdk-cpp's buildsystem uses very long paths and may fail on your system. 
REM We recommend moving vcpkg to a short path such as 'C:\src\vcpkg' or using the subst command."
subst W: %CD% 
W:
vcpkg\vcpkg.exe install

REM Configure project
cmake --fresh -G Ninja -D CMAKE_BUILD_TYPE=Release -D VCPKG_BUILD_TYPE=release -D CMAKE_TOOLCHAIN_FILE=vcpkg/scripts/buildsystems/vcpkg.cmake -B builds/conda -S .

REM Build
cmake --build builds/conda --target khiopsdriver_file_gcs

REM Copy binary to conda package
cmake --install builds/conda --prefix $PREFIX