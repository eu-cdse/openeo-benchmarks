#%%
import json
import logging
from pathlib import Path
from typing import Union

import numpy as np
import openeo
import xarray as xr

# Configure logging
_log = logging.getLogger(__name__)


def extract_reference_statistics(scenario_name: str) -> dict:
    """
    Loads reference data from a JSON file for a specific scenario.

    Parameters:
        scenario_name (str): The name of the scenario for which reference data is needed.

    Returns:
        dict: The reference data for the specified scenario.
    """
    reference_file = 'tests/groundtruth_regression_test.json'

    _log.info(f'Extracting reference band statistics for {scenario_name}')


    with open(reference_file, 'r') as file:
        all_reference_data = json.load(file)
    
    for scenario_data in all_reference_data:
        if scenario_data['scenario_name'] == scenario_name:
            return scenario_data['reference_data']
    
    raise ValueError(f"No reference data found for scenario '{scenario_name}' in file '{reference_file}'.")


def calculate_cube_statistics(hypercube: xr.Dataset) -> dict:
    """
    Calculate statistics for each band in the output cube.
    
    Parameters:
        hypercube (xarray.Dataset): Input hypercube obtained through opening a netCDF file using xarray.Dataset.
        
    Returns:
        dict: A dictionary containing statistics for each variable in the output cube. Keys are variable names 
              (matching the pattern 'B01, B02, ...') and values are dictionaries containing mean, min, max, 
              and quantile statistics.
    """
    statistics = {}
    band_names = [band_name for band_name in hypercube.data_vars if band_name != 'crs']

    for band_name in band_names:
        band_data = hypercube[band_name]
        mean_value = np.round(float(band_data.mean()),2)
        min_value = np.round(float(band_data.min()),2)
        max_value = np.round(float(band_data.max()),2)
        quantile_25 = np.round(band_data.quantile([0.25]).values,2)
        quantile_50 = np.round(band_data.quantile([0.5]).values,2)
        quantile_75 = np.round(band_data.quantile([0.75]).values,2)
        
        statistics[band_name] = {
            'mean': mean_value,
            'min': min_value,
            'max': max_value,
            'quantile25': quantile_25[0],
            'quantile50': quantile_50[0],
            'quantile75': quantile_75[0],
        }

    return statistics

# functionality for updating the reference

def update_json(json_name:str, scenario_name:str, new_statistics:dict):

    # Convert NumPy arrays in new_statistics to Python lists
    for key, value in new_statistics.items():
        if isinstance(value, np.ndarray):
            new_statistics[key] = value.tolist()

    # Read the JSON file
    with open(json_name, 'r') as file:
        data = json.load(file)

    # Find the scenario in the JSON data
    found_scenario = False
    for item in data:
        if item['scenario_name'] == scenario_name:
            # Update or add statistics for the scenario
            item['reference_data'].update(new_statistics)
            found_scenario = True
            break

    # If the scenario was not found, add it to the JSON data
    if not found_scenario:
        data.append({
            'scenario_name': scenario_name,
            'reference_data': new_statistics
        })

    # Write the updated JSON data back to the file
    with open(json_name, 'w') as file:
        json.dump(data, file, indent=4)

def execute_and_update_reference(cube: openeo.DataCube, 
                       output_path: Union[str, Path], 
                       scenario_name: str,
                       json_name:str
                       ) -> None:
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
    output_dict = calculate_cube_statistics(output_cube)
    update_json(json_name, scenario_name, output_dict)
