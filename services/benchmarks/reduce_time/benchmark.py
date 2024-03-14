import logging

from services.benchmarks.helper import setup_logging, setup_connection, submit_job, read_scenario_params

# Set up the log formatting
setup_logging()

# Set up the connection with OpenEO
session = setup_connection()

# Read the scenario that was passed as a parameter
scenario = read_scenario_params(type_required=False, extent_required = True)

# Execute tests
logging.info('Submitting timeseries job')
base_title = f'benchmarks_timeseries_{scenario["name"]}'

service_ts = session \
    .load_collection(collection_id = "SENTINEL2_L2A", temporal_extent = scenario["dates"], spatial_extent = scenario["extent"], max_cloud_cover=95) \
    .reduce_dimension(dimension="t", reducer="mean")
submit_job(service_ts, f'{base_title}_GTiff', 'GTiff')
