# scripts/run_pipeline.py

import argparse
from engines.get_engines import get_engine


def main(engine_name):
    engine = get_engine(engine_name)
    engine.load("data/raw/MTA_Subway_Hourly_Ridership_1GB.csv")
    engine.clean()
    engine.aggregate()
    engine.export(f"outputs/daily_station_summary_{engine_name}.csv")
    print("Pipeline completed using engine:", engine_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, help="Engine to use: pandas")
    args = parser.parse_args()
    main(args.engine)
