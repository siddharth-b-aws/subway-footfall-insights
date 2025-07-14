# scripts/clean.py

import pandas as pd
import os

RAW_FILE = 'data/raw/MTA_Subway_Daily_Ridership_May2025.csv'
CLEAN_FILE = 'data/processed/cleaned_ridership.csv'

def clean_data():
    if not os.path.exists(RAW_FILE):
        print(f"Raw file not found: {RAW_FILE}")
        return

    df = pd.read_csv(RAW_FILE)

    # 1. Parse date
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # 2. Strip whitespace in string columns
    df['Station'] = df['Station'].str.strip()
    df['Line'] = df['Line'].str.strip()

    # 3. Convert Entries/Exits to numeric
    df['Entries'] = pd.to_numeric(df['Entries'], errors='coerce')
    df['Exits'] = pd.to_numeric(df['Exits'], errors='coerce')

    # 4. Drop rows with missing critical fields
    df = df.dropna(subset=['Date', 'Station', 'Entries', 'Exits'])

    # 5. Output cleaned data
    os.makedirs(os.path.dirname(CLEAN_FILE), exist_ok=True)
    df.to_csv(CLEAN_FILE, index=False)

    print("Cleaned data saved to:", CLEAN_FILE)
    print(f"Final shape: {df.shape}")
    print(df.head())

if __name__ == "__main__":
    clean_data()
