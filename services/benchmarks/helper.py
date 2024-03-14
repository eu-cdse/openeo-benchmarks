import argparse
import json
import logging
import os

import geojson
from configparser import ConfigParser

import openeo as openeo
from openeo import Connection


def read_scenarios(type) -> list:
    scenarios = None
    with open(f'./services/benchmarks/{type}/scenarios.json', 'r') as scenario_file:
        scenarios = json.load(scenario_file)
        scenario_file.close()
    return scenarios


def setup_logging() -> None:
    """
    Setup the logging configuration for the benchmark tests
    """
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def setup_connection() -> Connection:
    """
    Setup a connection with an OpenEO backend and authenticate the user
    :return: Connection object to OpenEO
    """
    logging.debug(f'Setting up connection with OpenEO Creodias backend')
    return openeo.connect('openeo.creo.vito.be').authenticate_oidc()

def setup_debug_connection() -> Connection:
    """
    Setup a connection with an OpenEO backend and authenticate the user
    :return: Connection object to OpenEO
    """
    logging.debug(f'Setting up connection with OpenEO Creodias backend')
    return openeo.connect('https://openeo.dataspace.copernicus.eu/').authenticate_oidc()



def read_test_geometries(filename) -> dict:
    """
    Read the geometries from a test file that is stored within the project
    :param filename: Name of the GeoJSON file to read
    :return: GeoJSON Geometry collection
    """
    path = f'./services/benchmarks/files/{filename}'
    logging.debug(f'Reading geometries from {path}')
    with open(path) as f:
        feature_collection = geojson.load(f)
        f.close()
    return feature_collection


def submit_job(datacube, title, output_format) -> None:
    """
    Submit a job to OpenEO
    :param datacube: Datacube to be processed by OpenEO
    :param title:  Title of the job
    :param output_format: Expected output format of the job
    """
    logging.info(f'{title} - Submitting job ')
    job = datacube.create_job(out_format=output_format, title=title, description='benchmarking-creo', job_options={
        'driver-memory': '1g',
    })
    job.start_and_wait()
    logging.info(f'{title} - Submitted job with ID {job.job_id}')


def read_scenario_params(type_required = False, extent_required=False):
    parser = argparse.ArgumentParser(
        description="Execute a benchmark"
    )
    parser.add_argument("-f", "--file", type=str, required=True,
                        help="Name of the file to be processed")

    parser.add_argument("-n", "--name", type=str, required=True,
                        help="Name of the job to create")

    parser.add_argument("-d", "--dates", type=str, required=True,
                        help="Dates for which to execute the benchmark")

    parser.add_argument("-t", "--type", type=str, required=type_required,
                        help="Type of product to process")

    parser.add_argument("-e", "--extent", type=str, required=extent_required,
                        help="Extent for which to execute the benchmark")

    args = parser.parse_args()

    extent = args.extent
    if extent is not None and extent != "null":
        extent = [float(i) for i in args.extent.split(',')]
        extent = dict(zip(["west", "south", "east", "north"], extent))
    return {
        'file': args.file,
        'name': args.name,
        'dates': args.dates.split(','),
        'extent': extent,
        'type': args.type
    }
