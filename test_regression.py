

import pytest
from pathlib import Path
import numpy as np
import os

from utils import extract_test_geometries, execute_and_assert


def test_apply_kernel(auth_connection, tmp_path):

    # Define scenario parameters
    # Load scenario parameters
    scenario_name = 'apply_spatial_kernel'
    
    # Set up output directory and path
    output_path = tmp_path / f'{scenario_name}.nc'

    # Dummy kernel
    filter_window = np.ones([11, 11])
    factor = 1 / filter_window.sum()

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-07-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27},
        bands=['B02']
    ).apply_kernel(
        kernel=filter_window,
        factor=factor)
    
    
    # Excecute and Assert
    execute_and_assert(cube, output_path, scenario_name)

    

def test_aggregate_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'aggregate_polygons'

    # Set up output directory and path
    output_path = tmp_path / f'{scenario_name}.nc'

    # Get test geometries
    geometries = extract_test_geometries('alps_100_polygons.geojson')
    
    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L1C',
        temporal_extent=['2020-01-01', '2020-05-31'],
        bands=['B02', 'B03']
    ).aggregate_spatial(
        geometries=geometries,
        reducer='mean')
    
    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)



def test_downsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'downsample_spatial'

    # Set up output directory and path
    output_path = tmp_path / f'{scenario_name}.nc'

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-07-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27},
        bands=['B02', 'B03', 'B04']
    ).resample_spatial(
        resolution=60,
        method='mean')

    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)



def test_upsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'upsample_spatial'

    # Set up output directory and path
    output_path = tmp_path / f"{scenario_name}.nc"

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L1C',
        temporal_extent=['2020-01-01', '2020-12-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27},
        bands=['B01', 'B09', 'B10']
    ).resample_spatial(
        resolution=10,
        method='mean')
    
    # Excecute and apply
    execute_and_assert(cube, output_path, scenario_name)



def test_reduce_time(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'reduce_time'

    # Set up output directory and path
    output_path = tmp_path / f"{scenario_name}.nc"

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-07-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27},
        bands=['B02', 'B03']
    ).reduce_dimension(
        dimension='t',
        reducer='mean')

    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)



def test_mask_scl(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'mask_scl'

    # Set up output directory and path
    output_path = tmp_path / f'{scenario_name}.nc'

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-12-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27},
        bands=['B05', 'B06', 'SCL']
    )

    scl_band = cube.band('SCL')
    cloud_mask = (scl_band == 3) | (scl_band == 8) | (scl_band == 9)

    cube.mask(cloud_mask)

    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)

