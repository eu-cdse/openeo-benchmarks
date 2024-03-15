
import numpy as np
import logging

from services.benchmarks.helper import setup_logging, setup_debug_connection, \
    submit_job, read_scenario_params, read_test_geometries

# Set up the log formatting
setup_logging()

# Set up the connection with OpenEO
session = setup_debug_connection()

# Read the scenario that was passed as a parameter
scenario = read_scenario_params()

# Execute tests
logging.info('Submitting apply kernel')
base_title = f'benchmarks_{scenario["ScenarioName"]}'

#dummy kernel
filter_window = np.ones([100, 100])
factor = 1 / np.prod(filter_window.shape)

# run apply kernel
cube = session.load_collection(collection_id = scenario["CollectionId"], temporal_extent = scenario["dates"],\
                                spatial_extent = scenario["extent"], bands = scenario["bands"]) 
service_ts = cube.apply_kernel(kernel=filter_window, factor=factor)
submit_job(service_ts, f'{base_title}_JSON', 'JSON')
