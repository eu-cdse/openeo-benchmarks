#%%


import geojson
import xarray as xr
import re
import json
import openeo
from typing import Union
from pathlib import Path


def extract_test_geometries(filename) -> dict:
    """
    Read the geometries from a test file that is stored within the project
    :param filename: Name of the GeoJSON file to read
    :return: GeoJSON Geometry collection
    """
    path = f'./geofiles/{filename}'
    print(f'Reading geometries from {path}')

    with open(path) as f:
        geometry_collection = geojson.load(f)
        f.close()
    return geometry_collection


def extract_scenario_parameters(scenario_name: str) -> dict:
    """
    Loads scenario parameters from a JSON file for a specific scenario.

    Parameters:
        scenario_file (str): The path to the JSON file containing the scenario parameters for all scenarios.
        scenario_name (str): The name of the scenario for which parameters are needed.

    Returns:
        dict: The parameters for the specified scenario.
    """
    scenario_file = 'scenarios_regression_test.json'

    with open(scenario_file, 'r') as file:
        all_scenario_parameters = json.load(file)
    
    for scenario_params in all_scenario_parameters:
        if scenario_params['scenario_name'] == scenario_name:
            return scenario_params
    
    raise ValueError(f"No scenario parameters found for scenario '{scenario_name}' in file '{scenario_file}'.")


def calculate_band_statistics(hypercube: xr.Dataset) -> dict:
    """
    Calculate statistics for each variable in the output cube.
    
    Parameters:
        output_cube (xarray.Dataset): Input hypercube obtained through opening a netCDF file using xarray.Dataset.
        
    Returns:
        dict: A dictionary containing statistics for each variable in the output cube. Keys are variable names 
              (matching the pattern 'B01, B02, ...') and values are dictionaries containing mean, variance, min, max, 
              and quantile statistics.
    """

    statistics = {}
    band_names = [band_name for band_name in hypercube.data_vars\
                      if re.match(r'^B\d',band_name)]

    for band_name in band_names:

        band_data = hypercube[band_name]

        mean_value = float(band_data.mean())
        variance_value = float(band_data.var())
        
        min_value = float(band_data.min())
        max_value = float(band_data.max())
        
        quantiles = band_data.quantile([0.25, 0.5, 0.75]).values
        
        statistics[band_name] = {
            'mean': mean_value,
            'variance': variance_value,
            'min': min_value,
            'max': max_value,
            'quantile25': quantiles[0],
            'quantile50': quantiles[1],
            'quantile75': quantiles[2],
        }

    return statistics


def extract_reference_band_statistics(scenario_name: str) -> dict:
    """
    Loads reference data from a JSON file for a specific scenario.

    Parameters:
        reference_file (str): The path to the JSON file containing the reference statistics for all scenarios.
        scenario_name (str): The name of the scenario for which reference data is needed.

    Returns:
        dict: The reference data for the specified scenario.
    """
    reference_file = 'groundtruth_regression_test.json'

    with open(reference_file, 'r') as file:
        all_reference_data = json.load(file)
    
    for scenario_data in all_reference_data:
        if scenario_data['scenario_name'] == scenario_name:
            return scenario_data['reference_data']
    
    raise ValueError(f"No reference data found for scenario '{scenario_name}' in file '{reference_file}'.")
    

def compare_band_statistics(output_band_stats: dict, reference_band_stats: dict, tolerance: float) -> None:
    """
    Compares the statistics of different bands in the output against the reference data.

    Parameters:
        output_band_stats (dict): The output dictionary containing band statistics to be compared.
        reference_band_stats (dict): The reference dictionary containing expected band statistics.
        tolerance (float): Tolerance value for comparing values.

    Returns:
        None
    """
    for band_name, output_stats in output_band_stats.items():
        if band_name not in reference_band_stats:
            print(f"Warning: Band '{band_name}' not found in reference.")
            continue

        ref_band_stats = reference_band_stats[band_name]
        for stat, ref_value in ref_band_stats.items():
            if stat not in output_stats:
                print(f"Warning: Statistic '{stat}' not found for band '{band_name}' in output.")
                continue

            gen_value = output_stats[stat]
            if abs(gen_value - ref_value) > tolerance:
                print(f"Assertion failed: Band '{band_name}', Statistic '{stat}'. Expected: {ref_value}, Actual: {gen_value}")


def assert_band_statistics(output_band_stats: dict, scenario_name: str, tolerance: float = 0.01) -> None:
    """
    Asserts the output band statistics against a reference dictionary stored in a JSON file with tolerance.

    Parameters:
        output_band_stats (dict): The output dictionary containing band statistics to be asserted.
        reference_file (str): The path to the JSON file containing the reference dictionary.
        tolerance (float): Tolerance value for comparing values. Defaults to 0.01.

    Returns:
        None
    """
    try:
        reference_band_stats = extract_reference_band_statistics(scenario_name)
        compare_band_statistics(output_band_stats, reference_band_stats, tolerance)
    except FileNotFoundError:
        print(f"Error: Reference file '{scenario_name}' not found.")
    except json.JSONDecodeError:
        print(f"Error: Unable to parse JSON in file '{scenario_name}'.")


def execute_and_assert(cube: openeo.DataCube, 
                       output_path: Union[str, Path], 
                       scenario_name: str) -> None:
    """
    Avoid code duplication:
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

    try:
        cube.execute_batch(output_path,
                           title=scenario_name,
                           description='benchmarking-creo')

        output_cube = xr.open_dataset(output_path)
        output_band_stats = calculate_band_statistics(output_cube)

        assert_band_statistics(output_band_stats, scenario_name)

    except Exception as e:
        raise RuntimeError(f"Error occurred during execution and assertion: {str(e)}")
