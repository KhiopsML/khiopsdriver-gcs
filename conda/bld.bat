REM Echo all output
@echo on

REM Configure project
cmake --fresh -G Ninja -D CMAKE_BUILD_TYPE=Release -D VCPKG_BUILD_TYPE=release -D CMAKE_TOOLCHAIN_FILE=vcpkg/scripts/buildsystems/vcpkg.cmake -B builds/conda -S .

REM Build
cmake --build builds/conda --target khiopsdriver_file_gcs

REM Copy binary to conda package
cmake --install builds/conda --prefix $PREFIX