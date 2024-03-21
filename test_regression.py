
#%%
import pytest
from pathlib import Path

import xarray as xr
import numpy as np
import os
import openeo

from utils import extract_test_geometries, extract_scenario_parameters, execute_and_assert, extract_reference_band_statistics

# Define the path to reference files
os.environ["ENDPOINT"] = "https://openeo.dataspace.copernicus.eu/"


def test_apply_kernel(auth_connection, tmp_path):

    # Define scenario parameters
    # Load scenario parameters
    scenario_name = "apply_filter"
    params = extract_scenario_parameters(scenario_name)
    
    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Dummy kernel
    filter_window = np.ones([11, 11])
    factor = 1 / filter_window.sum()

    # Load collection, perform spatial aggregation, and download
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).apply_kernel(
        kernel=filter_window,
        factor=factor)
    
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))
    

def test_aggregate_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "aggregate_polygons"
    params = extract_scenario_parameters(scenario_name)


    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Get test geometries
    geometries = extract_test_geometries(params['file_name'])
    
    # Load collection, perform spatial aggregation, and download
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        bands=params['bands']
    ).aggregate_spatial(
        geometries=geometries,
        reducer="mean")
    
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_downsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "downsampling"
    params = extract_scenario_parameters(scenario_name)


    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).resample_spatial(
        resolution=60,
        method='mean')

    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_upsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "upsampling"
    params = extract_scenario_parameters(scenario_name)


    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).resample_spatial(
        resolution=10,
        method='mean')
    
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_reduce_time(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "time_reduction"
    params = extract_scenario_parameters(scenario_name)


    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).reduce_dimension(
        dimension="t",
        reducer="mean")
    
    cube.execute_batch(
        output_path,
        title=scenario_name,
        description='benchmarking-creo'
    )

    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_mask_scl(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "mask_scl"
    params = extract_scenario_parameters(scenario_name)

    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    )

    scl_band = cube.band("SCL")
    cloud_mask = (scl_band == 3) | (scl_band == 8) | (scl_band == 9)

    cube.mask(cloud_mask)

    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))
    
    
    
    



