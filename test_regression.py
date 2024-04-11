#%%
import numpy as np
import geopandas as gpd
from openeo.processes import if_, is_nan


from utils import extract_test_geometries, execute_and_assert

from utils_BAP import (calculate_cloud_mask, calculate_cloud_coverage_score,
                           calculate_date_score, calculate_distance_to_cloud_score,
                           calculate_distance_to_cloud_score, aggregate_BAP_scores,
                           create_rank_mask)


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


def test_aggregate_spatial(auth_connection, tmp_path):

    # Define scenario parameters
    scenario_name = 'aggregate_polygons'

    # Set up output directory and path
    output_path = tmp_path / f'output.nc'

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

    # Parameters for data collection
    collection_id = "SENTINEL2_L2A"
    spatial_geometries = gpd.read_file('.\geofiles\BAP.geojson')
    temporal_extent = ["2022-07-01", "2022-07-31"]
    spatial_resolution = 20

    # Fetch 100km x 100 km area from geojson
    spatial_geometries = spatial_geometries.to_crs(epsg=4326)
    area = eval(spatial_geometries.to_json())

    # Get the spectral bands of interest
    cube = auth_connection.load_collection(
        collection_id,
        temporal_extent = temporal_extent,
        bands = ["B02", "B03","B04"],
        max_cloud_cover=70
    ).resample_spatial(spatial_resolution
    ).filter_spatial(area)

    # Get slc
    scl = auth_connection.load_collection(
        collection_id,
        temporal_extent=temporal_extent,
        bands=["SCL"],
        max_cloud_cover=90
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




    
