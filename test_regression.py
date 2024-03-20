
#%%test aggregate special
from pathlib import Path
import xarray as xr
import numpy as np

import pytest


from utils import read_test_geometries, calculate_band_statistics, assert_band_statistics


def test_apply_kernel(connection, tmp_path_str):

    # Define scenario parameters
    scenario_name = "FilterKernel_Size100"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03"]
    temporal_extent = ["2020-01-01", "2020-03-31"]
    spatial_extent = {"west": 4.34, "south": 51.17, "east": 4.50, "north": 51.27}


    # Set up output directory and path
    tmp_path = Path(tmp_path_str)
    output_path = tmp_path / f"{scenario_name}.nc"


    #dummy kernel
    filter_window = np.ones([11, 11])
    factor = 1 / filter_window.sum()

    # Load collection, perform spatial aggregation, and download
    cube = connection.load_collection(
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
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    #assert_band_statistics(stats, reference_file)

def test_aggregate_spatial(connection, tmp_path_str):

    # Define scenario parameters
    scenario_name = "aggregate_100_polygons"
    collection_id = "SENTINEL2_L1C"
    bands = ["B02", "B03"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    file_name = "alps_100_polygons.geojson"


    # Set up output directory and path
    tmp_path = Path(tmp_path_str)
    output_path = tmp_path / f"{scenario_name}.nc"

    # Get test geometries
    geometries = read_test_geometries(file_name)

    
    # Load collection, perform spatial aggregation, and download
    cube = connection.load_collection(
        collection_id=collection_id,
        temporal_extent=temporal_extent,
        bands=bands
    ).aggregate_spatial(
        geometries=geometries,
        reducer="mean")
    
    cube.execute_batch(output_path,
                    title=scenario_name,
                    description='benchmarking-creo',
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    #assert_band_statistics(stats, reference_file)




def test_downsample_spatial(connection, tmp_path_str):

    # Define scenario parameters
    scenario_name = "downsampling_10m_to_60m"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03", "B04"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 4.3443, "south": 51.1690, "east": 4.5000, "north": 51.2652}

    # Set up output directory and path
    tmp_path = Path(tmp_path_str)
    output_path = tmp_path / f"{scenario_name}.nc"


    # Load collection, perform spatial aggregation, and download
    cube = connection.load_collection(
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
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    #assert_band_statistics(stats, reference_file)


def test_upsample_spatial(connection, tmp_path_str):

    # Define scenario parameters
    scenario_name = "upsampling_60m_to_10m"
    collection_id = "SENTINEL2_L1C"
    bands = ["B01", "B09", "B10"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 4.3443, "south": 51.1690, "east": 4.5000, "north": 51.2652}

    # Set up output directory and path
    tmp_path = Path(tmp_path_str)
    output_path = tmp_path / f"{scenario_name}.nc"


    # Load collection, perform spatial aggregation, and download
    cube = connection.load_collection(
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
                    )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    #assert_band_statistics(stats, reference_file)


def test_reduce_time(connection, tmp_path_str):

    # Define scenario parameters
    scenario_name = "time_reduction"
    collection_id = "SENTINEL2_L2A"
    bands = ["B02", "B03",]
    temporal_extent = ["2020-01-01", "2020-12-31" ]
    spatial_extent = {"west": 4.3443, "south": 51.1690, "east": 4.5000, "north": 51.2652}

    # Set up output directory and path
    tmp_path = Path(tmp_path_str)
    output_path = tmp_path / f"{scenario_name}.nc"


    # Load collection, perform spatial aggregation, and download
    cube = connection.load_collection(
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
                )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    #assert_band_statistics(stats, reference_file)


def test_mask_scl(connection, tmp_path_str):

    # Define scenario parameters
    scenario_name = "test_mask_scl"
    collection_id = "SENTINEL2_L2A"
    bands = ["B05","SCL"]
    temporal_extent = ["2020-01-01", "2020-12-31"]
    spatial_extent = {"west": 4.3443, "south": 51.1690, "east": 4.5000, "north": 51.2652}

    # Set up output directory and path
    tmp_path = Path(tmp_path_str)
    output_path = tmp_path / f"{scenario_name}.nc"


    # Load collection, perform spatial aggregation, and download
    cube = connection.load_collection(
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
                )

    output_cube = xr.open_dataset(output_path)
    stats = calculate_band_statistics(output_cube)
    
    #assert_band_statistics(stats, reference_file)
    
    

#%%
import openeo  
connection = openeo.connect('https://openeo.dataspace.copernicus.eu/').authenticate_oidc()
tmp = 'C:/Users/VROMPAYH/openeo-benchmarks/outcome_regression'

test_reduce_time(connection,tmp)


#%%
test_apply_kernel(connection,tmp)


print('aggregate')
test_aggregate_spatial(connection,tmp)




print('mask')
test_mask_scl(connection,tmp)

print('reduce')

#%%
filename = './outcome_integrationtest/upsampling_60m_to_10m.nc'

output_cube = xr.open_dataset(filename)
stats = calculate_band_statistics(output_cube)


import json


output_file = 'output.json'

with open(output_file, 'w') as file:
    json.dump(stats, file, indent=4)



# %%
