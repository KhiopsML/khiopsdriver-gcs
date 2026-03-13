# Khiops driver for Google Cloud Storage aka GCS

This repository hosts the source code for the Khiops filesystem driver enabling transparent manipulation for data stored in GCS buckets.

## Quickstart

If you just want to start using Khiops with your data located on GCS, simply install the driver package next to Khiops.
If you installed Khiops the standard way, the driver package can be installed via conda like so:

    conda install -c conda-forge khiops-driver-gcs

Or, if you have used your system package manager, you will have to install the driver by the same method. For debian/ubuntu, you will do this:

    CODENAME=$(lsb_release -cs) && \
    TEMP_DEB="$(mktemp)" && \
    wget -O "$TEMP_DEB" "https://github.com/KhiopsML/khiopsdriver-gcs/releases/download/0.0.14/khiops-driver-gcs_0.0.14-1-${CODENAME}.amd64.deb" && \
    sudo dpkg -i "$TEMP_DEB && \
    rm -f $TEMP_DEB

or if using Rocky linux, do this:

    sudo yum update -y && sudo yum install wget -y && \
    CENTOS_VERSION=$(rpm -E %{rhel}) && \
    TEMP_RPM="$(mktemp).rpm" && \
    wget -O "$TEMP_RPM" "https://github.com/KhiopsML/khiopsdriver-gcs/releases/download/0.0.14/khiops-driver-gcs_0.0.14-1.el${CENTOS_VERSION}.x86_64.rpm" && \
    sudo yum install "$TEMP_RPM" -y && \
    rm -f $TEMP_RPM

You can check that the driver is installed propery by running

    khiops -s

You should see an output similar to this:

    Khiops 10.3.0

    Drivers:
	    GCS driver (0.0.14) for URI scheme 'gs'
    Environment variables:
        None
    Internal environment variables:
        None

which indicates that the driver was loaded properly and will be used for datafiles following the gs:// pattern.

## Authentication

In order to access the data stored on a GCS bucket, in most cases a valid authentication in required. The Khiops GCS driver by default uses the standard [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) authentication. This means that once you have valid credentials setup in your environment, Khiops will be using these exactly like your python script or google provided tools like gcloud or gsutil.

In order to setup your local environment with these credentials (assuming you have installed the [gcloud CLI](https://cloud.google.com/sdk/docs/install)), you will have to do the following:

    gcloud init
    gcloud auth application-default login

Voilà! You now have access to your data in GCS buckets!
The exact same authentication mechanism will allow a containerized Khiops script to run on the Google infrastructure.

## Example usage

## Khiops usage (low level)
```
khiops -b -i gs://mydatabucket/khiops_samples/scenario.kh
```

## Python sample
```python
# Imports
import os
from khiops import core as kh

# Set the file paths
dictionary_file_path = "gs://mydatabucket/khiops_samples/Adult/Adult.kdic"
data_table_path = "gs://mydatabucket/khiops_samples/Adult/Adult.kdic"
results_dir = "khiops_output"

# Train the predictor
kh.train_predictor(
    dictionary_file_path,
    "Adult",
    data_table_path,
    "class",
    results_dir,
    max_trees=0,
)
```
## Development: Coverage reports

Coverage targets are available on Linux in non-Release builds when `BUILD_TESTS=ON`.
Tests are executed through `ctest` so coverage matches the test registry.

Configure and build in Debug mode:

    cmake --preset ninja-dbg -DBUILD_TESTS=ON
    cmake --build --preset ninja-dbg

Run tests directly with ctest (optional baseline check):

    ctest --preset ninja-dbg --output-on-failure

Generate unit-only coverage (tests labeled `unit`):

    cmake --build --preset ninja-dbg --target khiops-gcs_coverage_unit
    cmake --build --preset ninja-dbg --target khiops-gcs_cobertura_unit

Generate full coverage (all tests known by `ctest`):

    cmake --build --preset ninja-dbg --target khiops-gcs_coverage_full
    cmake --build --preset ninja-dbg --target khiops-gcs_cobertura_full

Artifacts are generated under `build/debug/`:

- HTML reports: `build/debug/coverage-unit/index.html` and `build/debug/coverage-full/index.html`
- Cobertura XML: `build/debug/coverage-unit.xml` and `build/debug/coverage-full.xml`

Legacy targets are still available and map to full coverage:

    cmake --build --preset ninja-dbg --target khiops-gcs_coverage
    cmake --build --preset ninja-dbg --target khiops-gcs_cobertura

## Development: GitHub CI coverage UX

Coverage reporting in CI uses only native GitHub capabilities (no external service).

On Linux workflow runs:

- The workflow writes a `Coverage Report` section to the run summary.
- Pull requests receive a single updatable comment with current coverage status.
- Coverage artifacts are uploaded only when the expected reports are generated.

Artifact names in GitHub Actions:

- `coverage-unit-ubuntu-latest`
- `coverage-full-ubuntu-latest`

If coverage generation fails or skips, upload is skipped consistently and the summary/comment explicitly indicates missing reports.
