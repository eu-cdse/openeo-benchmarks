#%%

import logging

from services.benchmarks.helper import setup_logging, setup_connection, \
    submit_job, read_scenario_params

# Set up the log formatting
setup_logging()


# Set up the connection with OpenEO

session = setup_connection()

# Read the scenario that was passed as a parameter
scenario = read_scenario_params(type_required=False, extent_required = True)


# Execute tests
logging.info('Submitting spatial downsampling job')
base_title = f'benchmarks_{scenario["name"]}'

service_ts = session \
    .load_collection(collection_id = "SENTINEL2_L2A", temporal_extent = scenario["dates"], spatial_extent = scenario["extent"], bands = ['blue'], max_cloud_cover=95) \
    .resample_spatial(resolution = 60,method ='mean')
submit_job(service_ts, f'{base_title}_JSON', 'JSON')
