
import logging

from services.benchmarks.helper import setup_logging, setup_debug_connection, \
    submit_job, read_scenario_params

# Set up the log formatting
setup_logging()

# Set up the connection with OpenEO
session = setup_debug_connection()

#% Read the scenario that was passed as a parameter
scenario = read_scenario_params()

# Execute tests
logging.info('Mask SCL')
base_title = f'benchmarks_{scenario["ScenarioName"]}'


# get the SCL mask and apply
cube = session.load_collection(collection_id = scenario["CollectionId"], temporal_extent = scenario["dates"], \
                                 spatial_extent = scenario["extent"], bands = scenario["bands"])

scl_band = cube.band("SCL")
cloud_mask = (scl_band == 3) | (scl_band == 8) | (scl_band == 9)
service_ts = cube.mask(cloud_mask)

submit_job(service_ts, f'{base_title}_JSON', 'JSON')



