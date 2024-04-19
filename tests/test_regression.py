#%%
import geojson
import geopandas as gpd
import numpy as np
import pytest
import requests
import xarray as xr
from openeo.processes import if_, is_nan

from .utils import calculate_cube_statistics, extract_reference_statistics
from .utils_BAP import (
    aggregate_BAP_scores,
    calculate_cloud_coverage_score,
    calculate_cloud_mask,
    calculate_date_score,
    calculate_distance_to_cloud_score,
    create_rank_mask,
)


def assert_dicts_approx_equal(dict1: dict, dict2: dict, tolerance: float = 0.01) -> None:
    """
    Compares two dictionaries for approximate equality.

    Args:
        dict1 (dict): The first dictionary to compare.
        dict2 (dict): The second dictionary to compare.
        tolerance (float, optional): The relative tolerance for comparing values. Defaults to 0.01.

    Raises:
        AssertionError: If the dictionaries are not approximately equal within the specified tolerance.
    """    
    assert set(dict1.keys()) == set(dict2.keys()), "The keys in both dictionaries must match."
    
    for key in dict1.keys():
        assert pytest.approx(dict1[key], rel=tolerance) == dict2[key], "Values for key '{key}' are not approximately equal."
    

#Below we list the regression tests on common operations in openEO.
#Note, the assert is specifically kept in the test file for tracibility in Jenkins

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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)


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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)



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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)



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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)



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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)



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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)



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
    cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

    output_cube = xr.open_dataset(output_path)
    output_dict = calculate_cube_statistics(output_cube)
    groundtruth_dict = extract_reference_statistics(scenario_name)

    assert_dicts_approx_equal(output_dict, groundtruth_dict)

    

