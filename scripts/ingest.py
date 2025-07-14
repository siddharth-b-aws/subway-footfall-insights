# scripts/ingest.py

import pandas as pd
import os

RAW_DATA_DIR = 'data/raw'
FILE_NAME = 'MTA_Subway_Daily_Ridership_May2025.csv'
FILE_PATH = os.path.join(RAW_DATA_DIR, FILE_NAME)

def load_data():
    if not os.path.exists(FILE_PATH):
        print(f"File not found: {FILE_PATH}")
        return None
    
    try:
        df = pd.read_csv(FILE_PATH)
        print("File loaded successfully")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

if __name__ == "__main__":
    load_data()

