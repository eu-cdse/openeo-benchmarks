
#%%
import pytest
from pathlib import Path
import numpy as np
import os

from utils import extract_test_geometries, extract_scenario_parameters, execute_and_assert

os.environ["ENDPOINT"] = "https://openeo.dataspace.copernicus.eu/"


def test_apply_kernel(auth_connection, tmp_path):

    # Define scenario parameters
    # Load scenario parameters
    scenario_name = "apply_spatial_kernel"
    params = extract_scenario_parameters(scenario_name)
    
    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Dummy kernel
    filter_window = np.ones([11, 11])
    factor = 1 / filter_window.sum()

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).apply_kernel(
        kernel=filter_window,
        factor=factor)
    
    # Excecute and Assert
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
    
    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        bands=params['bands']
    ).aggregate_spatial(
        geometries=geometries,
        reducer="mean")
    
    # Excecute and assert
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_downsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "downsample_spatial"
    params = extract_scenario_parameters(scenario_name)

    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).resample_spatial(
        resolution=60,
        method='mean')

    # Excecute and assert
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_upsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "upsample_spatial"
    params = extract_scenario_parameters(scenario_name)

    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).resample_spatial(
        resolution=10,
        method='mean')
    
    # Excecute and apply
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))


def test_reduce_time(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = "reduce_time"
    params = extract_scenario_parameters(scenario_name)

    # Set up output directory and path
    output_path = Path(tmp_path) / f"{scenario_name}.nc"

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    ).reduce_dimension(
        dimension="t",
        reducer="mean")

    # Excecute and assert
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

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id=params['collection_id'],
        temporal_extent=params['temporal_extent'],
        spatial_extent=params['spatial_extent'],
        bands=params['bands']
    )

    scl_band = cube.band("SCL")
    cloud_mask = (scl_band == 3) | (scl_band == 8) | (scl_band == 9)

    cube.mask(cloud_mask)

    # Excecute and assert
    try:
        execute_and_assert(cube, output_path, scenario_name)
    except RuntimeError as e:
        pytest.fail(str(e))
    
    
    
    
