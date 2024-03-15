import argparse
import json
from get_scenarios import execute_benchmark


parser = argparse.ArgumentParser(description = "Execute a benchmark")
parser.add_argument("service", type = str, help = "Service for which to run the benchmark.")
args = parser.parse_args()

with open(f'./services/benchmarks/{args.service}/scenarios.json') as file:
    scenarios = json.load(file)

for scenario in scenarios:
    execute_benchmark(args.service, scenario)



