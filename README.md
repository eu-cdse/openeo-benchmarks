# openeo-benchmarks

This repository contains regression benchmarks for the openEO instance of Copernicus Dataspace Ecosystem.

## Usage

### Using Conda

1. Set the environment variable `OPENEO_BACKEND_URL` to the openEO backend URL:
    ```bash
    conda env config vars set OPENEO_BACKEND_URL=https://openeo.dataspace.copernicus.eu/
    ```

2. Run pytest:
    ```bash
    pytest
    ```

### Using Linux

1. Set the environment variable `OPENEO_BACKEND_URL` to the openEO backend URL:
    ```bash
    export OPENEO_BACKEND_URL=https://openeo.dataspace.copernicus.eu/
    ```


2. Run pytest:
    ```bash
    pytest
    ```
