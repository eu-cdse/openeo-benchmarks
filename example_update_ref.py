#%%
import numpy as np
import geopandas as gpd
from openeo.processes import if_, is_nan
from pathlib import Path
import openeo


from utils import execute_and_update_reference

from utils_BAP import (calculate_cloud_mask, calculate_cloud_coverage_score,
                           calculate_date_score, calculate_distance_to_cloud_score,
                           calculate_distance_to_cloud_score, aggregate_BAP_scores,
                           create_rank_mask)

auth_connection = openeo.connect(url="openeo.dataspace.copernicus.eu").authenticate_oidc()

tmp_path = Path('./')
json_name = 'groundtruth_regression_test.json'



# Define scenario parameters
scenario_name = 'BAP'

# Set up output directory and path
output_path = tmp_path / f'output.nc'

# Parameters for data collection
collection_id = "SENTINEL2_L2A"
spatial_geometries = gpd.read_file('.\geofiles\BAP.geojson')
temporal_extent = ["2020-01-01", "2021-01-01"]
spatial_resolution = 20
max_cloud_cover = 80

# Fetch 100km x 100 km area from geojson
spatial_geometries = spatial_geometries.to_crs(epsg=4326)
area = eval(spatial_geometries.to_json())

# Get the spectral bands of interest
cube = auth_connection.load_collection(
    collection_id,

    temporal_extent = temporal_extent,
    bands = ["B02", "B03", "B04", "B05", "B06", "B07", "B08"],
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
execute_and_update_reference(cube, output_path, scenario_name, json_name)
