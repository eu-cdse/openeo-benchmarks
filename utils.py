#%%
import logging
import geojson
import xarray as xr
import json
import openeo
from typing import Union
from pathlib import Path
import numpy as np

import pytest

# Configure logging
_log = logging.getLogger(__name__)


def extract_test_geometries(filename) -> dict:
    """
    Read the geometries from a test file that is stored within the project
    :param filename: Name of the GeoJSON file to read
    :return: GeoJSON Geometry collection
    """
    path = f'./geofiles/{filename}'
    _log.info(f'Reading geometries from {path}')


    with open(path) as f:
        geometry_collection = geojson.load(f)
    return geometry_collection
    

def extract_reference_band_statistics(scenario_name: str) -> dict:
    """
    Loads reference data from a JSON file for a specific scenario.

    Parameters:
        scenario_name (str): The name of the scenario for which reference data is needed.

    Returns:
        dict: The reference data for the specified scenario.
    """
    reference_file = 'groundtruth_regression_test.json'

    _log.info(f'Extracting reference band statistics for {scenario_name}')


    with open(reference_file, 'r') as file:
        all_reference_data = json.load(file)
    
    for scenario_data in all_reference_data:
        if scenario_data['scenario_name'] == scenario_name:
            return scenario_data['reference_data']
    
    raise ValueError(f"No reference data found for scenario '{scenario_name}' in file '{reference_file}'.")


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
    _log.info('Comparing and asserting the statistics of different bands in the output')

    for output_band_name, output_band_stats in output_dict.items():
            if output_band_name not in groundtruth_dict:
                msg = f"Warning: Band '{output_band_name}' not found in reference."
                _log.warning(msg)
                continue

            gt_band_stats = groundtruth_dict[output_band_name]
            for stat_name, gt_value in gt_band_stats.items():
                if stat_name not in output_band_stats:
                    msg = f"Warning: Statistic '{stat_name}' not found for band '{output_band_name}' in output."
                    _log.warning(msg)
                    continue

                assert output_band_stats[stat_name] == pytest.approx(gt_value, rel=tolerance)
                

def calculate_band_statistics(hypercube: xr.Dataset) -> dict:
    """
    Calculate statistics for each variable in the output cube.
    
    Parameters:
        hypercube (xarray.Dataset): Input hypercube obtained through opening a netCDF file using xarray.Dataset.
        
    Returns:
        dict: A dictionary containing statistics for each variable in the output cube. Keys are variable names 
              (matching the pattern 'B01, B02, ...') and values are dictionaries containing mean, variance, min, max, 
              and quantile statistics.
    """
    statistics = {}
    band_names = [band_name for band_name in hypercube.data_vars if band_name != 'crs']

    for band_name in band_names:
        band_data = hypercube[band_name]
        mean_value = np.round(float(band_data.mean()),2)
        variance_value = np.round(float(band_data.var()),2)
        min_value = np.round(float(band_data.min()),2)
        max_value = np.round(float(band_data.max()),2)
        quantile_25 = np.round(band_data.quantile([0.25]).values,2)
        quantile_50 = np.round(band_data.quantile([0.5]).values,2)
        quantile_75 = np.round(band_data.quantile([0.75]).values,2)
        
        statistics[band_name] = {
            'mean': mean_value,
            'variance': variance_value,
            'min': min_value,
            'max': max_value,
            'quantile25': quantile_25[0],
            'quantile50': quantile_50[0],
            'quantile75': quantile_75[0],
        }

    return statistics


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
    output_dict = calculate_band_statistics(output_cube)
    update_json(json_name, scenario_name, output_dict)
