import logging

from services.benchmarks.helper import setup_logging, setup_debug_connection, \
    submit_job, read_scenario_params, read_test_geometries

# Set up the log formatting
setup_logging()

# Set up the connection with OpenEO
session = setup_debug_connection()

# Read the scenario that was passed as a parameter
scenario = read_scenario_params()

# Get geometries
geometries = read_test_geometries(scenario["file"])

# Execute tests
logging.info('Submitting aggregate spatial job')
base_title = f'benchmarks_{scenario["ScenarioName"]}'

service_ts = session \
    .load_collection(collection_id = scenario["CollectionId"], temporal_extent = scenario["dates"], bands = scenario["bands"]) \
    .aggregate_spatial(geometries=geometries, reducer="mean")
submit_job(service_ts, f'{base_title}_JSON', 'JSON')
