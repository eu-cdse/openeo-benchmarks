
#%%test aggregate special
from pathlib import Path
import xarray as xr
import pytest
import numpy as np
import re
from services.helper import read_test_geometries

def calculate_band_statistics(output_cube):
    """
    Function to calculate statistics for each variable in the output cube.
    """
    statistics = {}
    var_name_bands = [var_name for var_name in output_cube.data_vars\
                      if re.match(r'^B\d',var_name)]

    for var_name in var_name_bands:

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
            'quantile25': quantiles[0],
            'quantile50': quantiles[1],
            'quantile75': quantiles[2],
        }

    return statistics



BATCH_JOB_TIMEOUT = 60 * 60


@pytest.mark.batchjob
@pytest.mark.timeout(BATCH_JOB_TIMEOUT)
def test_apply_kernel(session):

    # Define scenario parameters
    scenario_name = "FilterKernel_Size100"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03", "B04", "B08"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 2, "south": 50, "east": 3, "north": 50}
    #spatial_extent = {"west": 2, "south": 51, "east": 4, "north": 53}


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
        factor=factor)
    
    cube.execute_batch(output_path,
                    title=scenario_name,
                    description='benchmarking-creo',
                    job_options={'driver-memory': '1g'}
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    assert stats['min'] == pytest.approx(0.0, rel=0.01)
    assert stats['max'] == pytest.approx(100.0, rel=0.01)
    assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)

def test_aggregate_spatial(session):

    # Define scenario parameters
    scenario_name = "aggregate_100_polygons"
    collection_id = "SENTINEL2_L1C"
    bands = ["B02", "B03"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
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
        reducer="mean")
    
    cube.execute_batch(output_path,
                    title=scenario_name,
                    description='benchmarking-creo',
                    job_options={'driver-memory': '1g'}
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    assert stats['min'] == pytest.approx(0.0, rel=0.01)
    assert stats['max'] == pytest.approx(100.0, rel=0.01)
    assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)






def test_downsample_spatial(session):

    # Define scenario parameters
    scenario_name = "downsampling_10m_to_60m"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03", "B04", "B08"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 3.7, "south": 51.3, "east": 3.8, "north": 51.4}

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
        method ='mean')

    cube.execute_batch(output_path,
                    title=scenario_name,
                    description='benchmarking-creo',
                    job_options={'driver-memory': '1g'}
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    assert stats['min'] == pytest.approx(0.0, rel=0.01)
    assert stats['max'] == pytest.approx(100.0, rel=0.01)
    assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)


def test_upsample_spatial(session):

    # Define scenario parameters
    scenario_name = "upsampling_60m_to_10m"
    collection_id = "SENTINEL2_L1C"
    bands = ["B01", "B09"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 3.7, "south": 51.3, "east": 3.8, "north": 51.4}

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
        method ='mean')
    
    cube.execute_batch(output_path,
                    title=scenario_name,
                    description='benchmarking-creo',
                    job_options={'driver-memory': '1g'}
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    assert stats['min'] == pytest.approx(0.0, rel=0.01)
    assert stats['max'] == pytest.approx(100.0, rel=0.01)
    assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)


def test_reduce_time(session):

    # Define scenario parameters
    scenario_name = "time_reduction"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03", "B04",]
    temporal_extent = ["2020-01-01", "2020-12-31" ]
    spatial_extent = {"west": 3.7, "south": 51.3, "east": 3.8, "north": 51.4}

    # Set up output directory and path
    output_directory = Path(__file__).parent / "outcome_integrationtest"
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{scenario_name}.nc"

    # Load collection, perform spatial aggregation, and download
    session.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        spatial_extent = spatial_extent,
        bands = bands
    ).reduce_dimension(
        dimension="t",
        reducer="mean")
    
    cube.execute_batch(output_path,
                title=scenario_name,
                description='benchmarking-creo',
                job_options={'driver-memory': '1g'}
                )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    assert stats['min'] == pytest.approx(0.0, rel=0.01)
    assert stats['max'] == pytest.approx(100.0, rel=0.01)
    assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)


def test_mask_scl(session):

    # Define scenario parameters
    scenario_name = "test_mask_scl"
    collection_id = "SENTINEL2_L2A"
    bands = ["B05","SCL"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 3.7, "south": 51.3, "east": 3.8, "north": 51.4}

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

    cube.mask(cloud_mask)

    cube.execute_batch(output_path,
                title=scenario_name,
                description='benchmarking-creo',
                job_options={'driver-memory': '1g'}
                )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    assert stats['mean'] == pytest.approx(50.0, rel=0.01)
    assert stats['variance'] == pytest.approx(50.0, rel=0.01)
    assert stats['min'] == pytest.approx(0.0, rel=0.01)
    assert stats['max'] == pytest.approx(100.0, rel=0.01)
    assert stats['quantiles'] == pytest.approx([25.0, 50.0, 75.0], rel=0.01)
    
    


#%%
import openeo
conn = openeo.connect('https://openeo.dataspace.copernicus.eu/').authenticate_oidc()
#conn = openeo.connect("openeo-dev.vito.be").authenticate_oidc()

 # Define scenario parameters
scenario_name = "time_reduction"
collection_id = "SENTINEL2_L2A"
temporal_extent = ["2020-01-01", "2020-03-31" ]
spatial_extent = {"west": 3.758216409030558, "south": 51.291835566, "east": 3.8, "north": 51.3927399}


# Set up output directory and path
output_directory = Path(__file__).parent / "outcome_integrationtest"
output_directory.mkdir(parents=True, exist_ok=True)
output_path = output_directory / f"{scenario_name}.nc"

# Load collection, perform spatial aggregation, and download
cube = conn.load_collection(
    collection_id=collection_id,
    temporal_extent=temporal_extent,
    spatial_extent = spatial_extent,
).reduce_dimension(
    dimension="t",
    reducer="mean"
)

cube.execute_batch(output_path)

#%%
cube.execute_batch(output_path,
                title=scenario_name,
                description='benchmarking-creo',
                job_options={'driver-memory': '1g'}
                )

output_cube = xr.open_dataset(output_path)
stats = calculate_band_statistics(output_cube)

#%%
from openeo.udf import XarrayDataCube
output_cube = XarrayDataCube.from_file(output_path, fmt='netcdf')

# %%
