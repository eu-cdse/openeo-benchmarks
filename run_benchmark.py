import argparse
import json
import os

parser = argparse.ArgumentParser(description = "Execute a benchmark")
parser.add_argument("service", type = str, help = "Service for which to run the benchmark.")
args = parser.parse_args()

with open(f'./services/benchmarks/{args.service}/scenarios.json') as file:
    scenarios = json.load(file)

for scenario in scenarios:
    print(f'Executing {scenario["name"]}')
    scenario_type = scenario["type"] if "type" in scenario else ""
    command = f'python3 -m services.benchmarks.{args.service}.benchmark ' \
              f'-f {scenario["file"]} -n {scenario["name"]} -d {scenario["dates"]}'
    if 'extent' in scenario:
        command += f' -e {scenario["extent"]}'
    if 'type' in scenario:
        command += f' -t {scenario["type"]}'
    print(command)
    os.system(command)
