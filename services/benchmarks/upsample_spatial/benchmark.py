import logging

from services.benchmarks.helper import setup_logging, setup_debug_connection, \
    submit_job, read_scenario_params

# Set up the log formatting
setup_logging()

# Set up the connection with OpenEO

session = setup_debug_connection()

# Read the scenario that was passed as a parameter
scenario = read_scenario_params()

# Execute tests
logging.info('Submitting spatial upsampling job')
base_title = f'benchmarks_{scenario["ScenarioName"]}'

service_ts = session \
    .load_collection(collection_id = scenario["CollectionId"], temporal_extent = scenario["dates"], spatial_extent = scenario["extent"], bands = scenario["bands"]) \
    .resample_spatial(resolution = 10,method ='bilinear')
submit_job(service_ts, f'{base_title}_JSON', 'JSON')
