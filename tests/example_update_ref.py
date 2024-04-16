#%%
import numpy as np
import geopandas as gpd
from openeo.processes import if_, is_nan
from pathlib import Path
import openeo


from utils import execute_and_update_reference, extract_test_geometries

from utils_BAP import (calculate_cloud_mask, calculate_cloud_coverage_score,

                           calculate_date_score, calculate_distance_to_cloud_score,
                           calculate_distance_to_cloud_score, aggregate_BAP_scores,
                           create_rank_mask)


auth_connection = openeo.connect(url="openeo.dataspace.copernicus.eu").authenticate_oidc()

tmp_path = Path('./')
json_name = 'groundtruth_regression_test.json'

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

cube.execute_batch(outputfile=output_path,
                        title=scenario_name,
                        description='benchmarking-creo',
                        job_options={'driver-memory': '1g'}
                        )

execute_and_update_reference(cube, output_path, scenario_name, json_name)

#%%
import xarray as xr
from utils import calculate_band_statistics, extract_reference_band_statistics, assert_band_statistics, update_json

json_name = 'groundtruth_regression_test.json'

output_cube = xr.open_dataset(output_path)
output_dict = calculate_band_statistics(output_cube)
update_json(json_name, scenario_name, output_dict)
groundtruth_dict = extract_reference_band_statistics(scenario_name)
assert_band_statistics(output_dict, groundtruth_dict, 0.05)

