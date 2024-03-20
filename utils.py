#%%

import geojson
import xarray as xr
import re
import json

def read_test_geometries(filename) -> dict:
    """
    Read the geometries from a test file that is stored within the project
    :param filename: Name of the GeoJSON file to read
    :return: GeoJSON Geometry collection
    """
    path = f'./files/{filename}'
    print(f'Reading geometries from {path}')

    with open(path) as f:
        feature_collection = geojson.load(f)
        f.close()
    return feature_collection

def calculate_band_statistics(output_cube: xr.Dataset) -> dict:
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
    var_name_bands = [var_name for var_name in output_cube.data_vars\
                      if re.match(r'^B\d',var_name)]

    for var_name in var_name_bands:

        variable_data = output_cube[var_name]

        mean_value = float(variable_data.mean())
        variance_value = float(variable_data.var())
        
        min_value = float(variable_data.min())
        max_value = float(variable_data.max())
        
        quantiles = variable_data.quantile([0.25, 0.5, 0.75]).values
        
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



def assert_band_statistics(output: dict, reference_file: str) -> None:
    """
    Asserts the output dictionary against a reference dictionary stored in a JSON file.

    Parameters:
        output (dict): The output dictionary to be asserted.
        reference_file (str): The path to the JSON file containing the reference dictionary.

    Returns:
        None
    """
    
    with open(reference_file, 'r') as file:
        reference = json.load(file)

    for key, values in output.items():
        assert key in reference, f"Key '{key}' not found in reference."

        reference_values = reference[key]
        for stat in ['mean', 'variance', 'min', 'max', 'quantile25', 'quantile50', 'quantile75']:
            assert stat in reference_values, f"Statistic '{stat}' not found for key '{key}' in reference."

            assert values[stat] == reference_values[stat], f"Value mismatch for key '{key}', statistic '{stat}'. Expected: {reference_values[stat]}, Actual: {values[stat]}"