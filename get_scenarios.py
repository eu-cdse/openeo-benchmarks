# This file is automatically run by a cron job.
import argparse
import json

parser = argparse.ArgumentParser(
    description="Execute a CDSE benchmark"
)
parser.add_argument("service", type=str,
                    help="Service for which to retrieve the scenarios")

args = parser.parse_args()
#########

with open(f'./services/benchmarks/{args.service}/scenarios.json') as file:
    scenarios = json.load(file)
    for scenario in scenarios:
        if 'type' not in scenario:
            scenario['type'] = 'null'
        if 'extent' not in scenario:
            scenario['extent'] = 'null'
        if 'file' not in scenario:
            scenario['file'] = 'null'
    print(json.dumps(scenarios))
    file.close()