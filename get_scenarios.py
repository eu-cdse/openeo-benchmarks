import logging
import os

def command_from_scenario(NameBenchmark: str, ParamsBenchmark: dict) -> list:
    """
    Build a command based on the name and parameters of a unit test.

    Args:
        NameBenchmark (str): The name of the service for which the benchmark is being executed.
        ParamsBenchmark (dict): A dictionary containing scenario parameters like file, name, dates, extent, and type.

    Returns:
        list: A list of command parts.
    """
    command_parts = ['python', '-m', f'services.benchmarks.{NameBenchmark}.benchmark']

    mappings = {
        "file": "-f",
        "ScenarioName": "-s",
        "CollectionId": "-c",
        "dates": "-d",
        "extent": "-e",
        "bands": "-b",
        "type": "-t"
    }

    for key, value in ParamsBenchmark.items():
        if value is not None:
            command_parts.extend([mappings[key], value])

    return command_parts

def execute_benchmark(NameBenchmark: str, ParamsBenchmark: dict):
    """
    Execute the benchmark scenario for the given service.

    Args:
        NameBenchmark (str): The name of the service for which the benchmark is being executed.
        ParamsBenchmark (dict): A dictionary containing scenario parameters like file, name, dates, extent, and type.
    """
    command = command_from_scenario(NameBenchmark, ParamsBenchmark)
    logging.info(f'Executing {" ".join(command)}')
    os.system(" ".join(command))

