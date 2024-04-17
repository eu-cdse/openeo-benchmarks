#%%
import geopandas as gpd
import geojson
import numpy as np
import openeo
from openeo.processes import if_, is_nan
from pathlib import Path
import requests
from typing import Union
import xarray as xr
import pytest

# Assuming the current working directory is 'A' where you run the tests
from utils import  (calculate_band_statistics, extract_reference_band_statistics)

from utils_BAP import (calculate_cloud_mask, calculate_cloud_coverage_score,
                           calculate_date_score, calculate_distance_to_cloud_score,
                           calculate_distance_to_cloud_score, aggregate_BAP_scores,
                           create_rank_mask)

def assert_band_statistics(output_dict: dict, groundtruth_dict: dict, tolerance: float) -> None:
    """
    Compares and asserts the statistics of different bands in the output against the reference data.

    Parameters:
        output_dict (dict): The output dictionary containing band statistics to be compared.
        groundtruth_dict (dict): The reference dictionary containing expected band statistics.
        tolerance (float): Tolerance value for comparing values.

    Returns:
        None
    """

    for output_band_name, output_band_stats in output_dict.items():
            if output_band_name not in groundtruth_dict:
                msg = f"Warning: Band '{output_band_name}' not found in reference."
                continue

            gt_band_stats = groundtruth_dict[output_band_name]
            for stat_name, gt_value in gt_band_stats.items():
                if stat_name not in output_band_stats:
                    msg = f"Warning: Statistic '{stat_name}' not found for band '{output_band_name}' in output."
                    continue

                assert output_band_stats[stat_name] == pytest.approx(gt_value, rel=tolerance)

def execute_and_assert(cube: openeo.DataCube, 
                       output_path: Union[str, Path], 
                       scenario_name: str,
                       tolerance: float = 0.05) -> None:
    """
    Execute the provided OpenEO cube, save the result to the output path, 
    and assert its statistics against the reference data.

    Parameters:
        cube (openeo.datacube.DataCube): The OpenEO data cube to execute.
        output_path (Union[str, Path]): The path where the output should be saved.
        scenario_name (str): A name identifying the scenario for reference data.

    Returns:
        None

    Raises:
        RuntimeError: If there is an issue during execution, file saving, or assertion.
    """
    
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_band_statistics(output_cube)
    groundtruth_dict = extract_reference_band_statistics(scenario_name)
    assert_band_statistics(output_dict, groundtruth_dict, tolerance)


def test_aggregate_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'aggregate_polygons'

    # Set up output directory and path
    output_path = tmp_path / f'output.nc'

    # Get test geometries
    geojson_url = 'https://artifactory.vgt.vito.be/artifactory/auxdata-public/cdse_benchmarks/geofiles/alps_100_polygons.geojson'

    # Fetch the GeoJSON content
    response = requests.get(geojson_url)
    geometry_collection = geojson.loads(response.text)
    
    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L1C',
        temporal_extent=['2020-01-01', '2020-05-31'],
        bands=['B02', 'B03']
    ).aggregate_spatial(
        geometries=geometry_collection,
        reducer='mean')
    
    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)


def test_apply_kernel(auth_connection, tmp_path):

    # Define scenario parameters
    # Load scenario parameters
    scenario_name = 'apply_spatial_kernel'
    
    # Set up output directory and path
    output_path = tmp_path / f'output.nc'

    # Dummy kernel
    filter_window = np.ones([11, 11])
    factor = 1 / filter_window.sum()

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-07-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27, 'espg': 4326},
        bands=['B02']
    ).apply_kernel(
        kernel=filter_window,
        factor=factor)
    
    # Excecute and Assert
    execute_and_assert(cube, output_path, scenario_name)


def test_downsample_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'downsample_spatial'

    # Set up output directory and path
    output_path = tmp_path / f'output.nc'

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-07-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27, 'espg': 4326},
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
    output_path = tmp_path / f'output.nc'

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L1C',
        temporal_extent=['2020-01-01', '2020-12-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27, 'espg': 4326},
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
    output_path = tmp_path / f'output.nc'

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-07-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27, 'espg': 4326},
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
    output_path = tmp_path / f'output.nc'

    # Load collection, and set up progress graph
    cube = auth_connection.load_collection(
        collection_id='SENTINEL2_L2A',
        temporal_extent=['2020-01-01', '2020-12-31'],
        spatial_extent={'west': 4.34,'south': 51.17,'east': 4.50,'north': 51.27, 'espg': 4326},
        bands=['B05', 'B06', 'SCL']
    )

    scl_band = cube.band('SCL')
    cloud_mask = (scl_band == 3) | (scl_band == 8) | (scl_band == 9)

    cube.mask(cloud_mask)

    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)


def test_BAP(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'BAP'

    # Set up output directory and path
    output_path = tmp_path / f'output.nc'

    # Get test geometries
    geojson_url = 'https://artifactory.vgt.vito.be/artifactory/auxdata-public/cdse_benchmarks/geofiles/BAP.geojson'
    response = requests.get(geojson_url)

    # Write the content to a local file
    filepath = 'BAP.geojson'  # Or specify a different filepath
    with open(filepath, 'wb') as f:
        f.write(response.content)

    spatial_geometries = gpd.read_file(filepath)
    spatial_geometries = spatial_geometries.to_crs(epsg=4326)
    area = eval(spatial_geometries.to_json())
    

    # Parameters for data collection
    collection_id = "SENTINEL2_L2A"
    temporal_extent = ["2022-01-01", "2022-07-31"]
    spatial_resolution = 20
    max_cloud_cover = 80


    # Get the spectral bands of interest
    cube = auth_connection.load_collection(
        collection_id,
        temporal_extent = temporal_extent,
        bands = ["B02", "B03","B04", "B05", "B06", "B07", "B08"],
        max_cloud_cover = max_cloud_cover
    ).resample_spatial(spatial_resolution
    ).filter_spatial(area)

    # Get slc
    scl = auth_connection.load_collection(
        collection_id,
        temporal_extent = temporal_extent,
        bands = ["SCL"],
        max_cloud_cover = max_cloud_cover
    ).resample_spatial(spatial_resolution
    ).filter_spatial(area
    ).apply(lambda x: if_(is_nan(x), 0, x))


    # Get cloud mask
    cloud_mask =  calculate_cloud_mask(scl)

    # Get scores
    coverage_score = calculate_cloud_coverage_score(cloud_mask, area, scl)
    date_score = calculate_date_score(scl)
    dtc_score = calculate_distance_to_cloud_score(cloud_mask, spatial_resolution)

    # Aggregate scores and create a mask
    score = aggregate_BAP_scores(dtc_score, date_score, coverage_score)
    score = score.mask(scl.band("SCL") == 0) #remove pixels that do not contain any data
    rank_mask = create_rank_mask(score)

    # Create composite
    cube = cube.mask(rank_mask
                    ).mask(cloud_mask
                    ).aggregate_temporal_period("month","first")

    
    # Excecute and assert
    execute_and_assert(cube, output_path, scenario_name)
    
