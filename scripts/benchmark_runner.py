import time
import os
import sys
from tabulate import tabulate
from engines.get_engines import get_engine

# Hardcoded test file path and output folder
DATA_PATH = 'data\\raw\\MTA_Subway_Hourly_Ridership_1GB.csv'
OUTPUT_DIR = 'outputs/benchmarks'

# Engines to test
ENGINE_LIST = [
    "pandas",
    "chunked_pandas",
    "duckdb",
    "dask",
    "modin",
    "pyspark"
]

def run_benchmark():
    os.environ["MODIN_ENGINE"] = "dask"
    os.environ["RAY_DISABLE_DASHBOARD"] = "1"

    results = []

    for engine_name in ENGINE_LIST:
        print(f"\nRunning: {engine_name.upper()}")

        try:
            engine = get_engine(engine_name)
            start = time.time()

            engine.load(DATA_PATH)
            engine.clean()
            engine.aggregate()
            output_path = os.path.join(OUTPUT_DIR, f"{engine_name}_output.csv")
            engine.export(output_path)

            end = time.time()
            runtime = round(end - start, 2)

            # Count output rows
            if os.path.exists(output_path):
                if engine_name == "pyspark":
                    # Spark writes directory with part file
                    part_file = [f for f in os.listdir(output_path) if f.startswith("part")][0]
                    output_file = os.path.join(output_path, part_file)
                else:
                    output_file = output_path

                with open(output_file, 'r', encoding='utf-8') as f:
                    rows = sum(1 for _ in f) - 1  # skip header
            else:
                rows = 0

            results.append([engine_name, runtime, rows, "✅"])

        except Exception as e:
            print(f"Error in {engine_name}: {e}")
            results.append([engine_name, "FAIL", "N/A", "❌"])

    print("\nBenchmark Results:")
    print(tabulate(results, headers=["Engine", "Time (s)", "Rows", "Status"], tablefmt="fancy_grid"))


if __name__ == "__main__":
    run_benchmark()
