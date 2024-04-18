#%%
from pathlib import Path
import openeo
from utils import execute_and_update_reference

def main():
    auth_connection = openeo.connect(url="openeo.dataspace.copernicus.eu").authenticate_oidc()

    tmp_path = Path('./')
    json_name = './tests/groundtruth_regression_test.json'

    # Define scenario parameters
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


    execute_and_update_reference(cube, output_path, scenario_name, json_name)


if __name__ == "__main__":
    main()
