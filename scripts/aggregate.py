# scripts/aggregate.py

import pandas as pd
import os

CLEAN_FILE = 'data/processed/cleaned_ridership.csv'
OUTPUT_FILE = 'outputs/daily_station_summary.csv'

def aggregate_data():
    if not os.path.exists(CLEAN_FILE):
        print("Cleaned file not found:", CLEAN_FILE)
        return

    df = pd.read_csv(CLEAN_FILE)

    df['Total_Traffic'] = df['Entries'] + df['Exits']

    summary_df = df.groupby(['Date', 'Station'], as_index=False)['Total_Traffic'].sum()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    summary_df.to_csv(OUTPUT_FILE, index=False)

    print("Aggregation complete.")
    print("Output saved to:", OUTPUT_FILE)
    print("Shape:", summary_df.shape)
    print(summary_df.head())

if __name__ == "__main__":
    aggregate_data()
