
#%%test aggregate special
from pathlib import Path
import xarray as xr
import pytest
import xarray.testing
import numpy as np

from services.benchmarks.helper import setup_logging, setup_debug_connection, read_test_geometries

def calculate_statistics(output_cube):
    """
    Function to calculate statistics for each variable in the output cube.
    """
    statistics = {}

    for var_name in list(output_cube.data_vars):
        # Access the variable
        variable_data = output_cube[var_name]
        
        # Calculate mean
        mean_value = float(variable_data.mean())
        
        # Calculate variance
        variance_value = float(variable_data.var())
        
        # Calculate minimum
        min_value = float(variable_data.min())
        
        # Calculate maximum
        max_value = float(variable_data.max())
        
        # Calculate quantiles (you can specify any quantiles you want)
        quantiles = variable_data.quantile([0.25, 0.5, 0.75]).values
        
        # Store all statistics in a dictionary
        statistics[var_name] = {
            'mean': mean_value,
            'variance': variance_value,
            'min': min_value,
            'max': max_value,
            'quantiles': quantiles
        }

def setup():
    setup_logging()
    session = setup_debug_connection()
    return session

BATCH_JOB_TIMEOUT = 60 * 60


@pytest.mark.batchjob
@pytest.mark.timeout(BATCH_JOB_TIMEOUT)
def test_aggregate_spatial(session):

    # Define scenario parameters
    scenario_name = "aggregate_100_polygons"
    collection_id = "SENTINEL2_L1C"
    bands = ["B02", "B03"]
    temporal_extent = ["2020-01-01", "2020-07-31"]
    file_name = "alps_100_polygons.geojson"

    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    # Get test geometries
    geometries = read_test_geometries(file_name)
    
    # Load collection, perform spatial aggregation, and download
    session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        bands=bands
    ).aggregate_spatial(
        geometries=geometries,
        reducer="mean"
    ).download(output_path)

    #TODO: assert
    #output_cube = xr.open_dataset(output_path)
    #stats = calculate_statistics(output_cube)
    #stats = calculate_statistics(output_cube)
    #assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    #assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    #assert stats['min'] == pytest.approx(0.0, rel=0.01)
    #assert stats['max'] == pytest.approx(100.0, rel=0.01)
    #assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)

def test_apply_kernel(session):

    # Define scenario parameters
    scenario_name = "FilterKernel_Size100"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03"]
    temporal_extent = ["2020-01-01", "2020-07-31"]
    spatial_extent = {"west": 2, "south": 51, "east": 4, "north": 53}


    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    #dummy kernel
    filter_window = np.ones([101, 101])
    factor = 1 / np.prod(filter_window.shape)

    # Load collection, perform spatial aggregation, and download
    session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        spatial_extent = spatial_extent,
        bands=bands
    ).apply_kernel(
        kernel=filter_window,
        factor=factor
    ).download(output_path)
    
    #TODO: assert
    #output_cube = xr.open_dataset(output_path)
    #stats = calculate_statistics(output_cube)
    #assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    #assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    #assert stats['min'] == pytest.approx(0.0, rel=0.01)
    #assert stats['max'] == pytest.approx(100.0, rel=0.01)
    #assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)

def test_downsample_spatial(session):

    # Define scenario parameters
    scenario_name = "downsampling_10m_to_60m"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03"]
    temporal_extent = ["2020-01-01", "2020-02-31"]
    spatial_extent = {"west": 2, "south": 51, "east": 4, "north": 53}

    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        spatial_extent = spatial_extent,
        bands=bands
    ).resample_spatial(
        resolution = 60,
        method ='mean'
    ).download(output_path)

    #TODO: assert
    #output_cube = xr.open_dataset(output_path)
    #stats = calculate_statistics(output_cube)
    #assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    #assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    #assert stats['min'] == pytest.approx(0.0, rel=0.01)
    #assert stats['max'] == pytest.approx(100.0, rel=0.01)
    #assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)

def test_upsample_spatial(session):

    # Define scenario parameters
    scenario_name = "upsampling_60m_to_10m"
    collection_id = "SENTINEL2_L1C"
    bands = ["B01", "B09"]
    temporal_extent = ["2020-01-01", "2020-02-31"]
    spatial_extent = {"west": 2, "south": 51, "east": 4, "north": 53}

    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        spatial_extent = spatial_extent,
        bands=bands
    ).resample_spatial(
        resolution = 10,
        method ='mean'
    ).download(output_path)
    

    #TODO: assert
    #output_cube = xr.open_dataset(output_path)
    #stats = calculate_statistics(output_cube)
    #assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    #assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    #assert stats['min'] == pytest.approx(0.0, rel=0.01)
    #assert stats['max'] == pytest.approx(100.0, rel=0.01)
    #assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)



def test_reduce_time(session):

    # Define scenario parameters
    scenario_name = "time_reduction"
    collection_id = "SENTINEL2_L2A"
    temporal_extent = ["2020-01-01", "2020-02-31"]
    spatial_extent = {"west": 2, "south": 51, "east": 4, "north": 53}

    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        spatial_extent = spatial_extent,
    ).reduce_dimension(
        dimension="t",
        reducer="mean"
    ).download(output_path)


    #TODO: assert
    #output_cube = xr.open_dataset(output_path)
    #stats = calculate_statistics(output_cube)
    #assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    #assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    #assert stats['min'] == pytest.approx(0.0, rel=0.01)
    #assert stats['max'] == pytest.approx(100.0, rel=0.01)
    #assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)

def test_mask_scl(session):

    # Define scenario parameters
    scenario_name = "time_reduction"
    collection_id = "SENTINEL2_L2A"
    bands = ["B05","SCL"]
    temporal_extent = ["2020-01-01", "2020-02-31"]
    spatial_extent = {"west": 2, "south": 51, "east": 4, "north": 53}

    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    cube = session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        spatial_extent = spatial_extent,
        bands = bands
    )

    scl_band = cube.band("SCL")
    cloud_mask = (scl_band == 3) | (scl_band == 8) | (scl_band == 9)

    cube.mask(cloud_mask
    ).download(output_path)
    

    #TODO: assert
    #output_cube = xr.open_dataset(output_path)
    #stats = calculate_statistics(output_cube)
    #assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    #assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    #assert stats['min'] == pytest.approx(0.0, rel=0.01)
    #assert stats['max'] == pytest.approx(100.0, rel=0.01)
    #assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)
    
    

    #%%

print('test_aggregate_spatial')
session = setup()
test_aggregate_spatial(session)

print('test_apply_kernel')
session = setup()
test_apply_kernel(session)

print('test_downsample_spatial')
session = setup()
test_downsample_spatial(session)

print('test_upsample_spatial')
session = setup()
test_upsample_spatial(session)

print('test_reduce_time')
session = setup()
test_reduce_time(session)

print('test_mask_scl')
session = setup()
test_mask_scl(session)
