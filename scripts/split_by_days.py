from tqdm import tqdm
import pandas as pd
import os

INPUT_FILE = "data/raw/MTA_Subway_Hourly_Ridership__2020-2024.csv"
OUTPUT_DIR = "data/raw/daily_csv_chunks"
CHUNK_SIZE = 500_000

os.makedirs(OUTPUT_DIR, exist_ok=True)

total_rows = 0
day_buckets = {}

def process_chunk(chunk):
    global total_rows, day_buckets
    chunk['transit_timestamp'] = pd.to_datetime(
        chunk['transit_timestamp'],
        format='%m/%d/%Y %I:%M:%S %p',
        errors='coerce'
    )
    chunk = chunk.dropna(subset=['transit_timestamp'])
    chunk['date'] = chunk['transit_timestamp'].dt.date

    for date, group in chunk.groupby('date'):
        if date not in day_buckets:
            day_buckets[date] = []
        day_buckets[date].append(group.drop(columns=['date']))

    total_rows += len(chunk)

def flush_to_disk():
    for date, frames in day_buckets.items():
        combined = pd.concat(frames, ignore_index=True)
        output_file = os.path.join(OUTPUT_DIR, f"{date}.csv")
        if os.path.exists(output_file):
            combined.to_csv(output_file, mode='a', header=False, index=False)
        else:
            combined.to_csv(output_file, index=False)
    day_buckets.clear()

def main():
    file_size_mb = os.path.getsize(INPUT_FILE) / (1024 * 1024)
    estimated_total_chunks = int((file_size_mb * 5000) / CHUNK_SIZE)

    with tqdm(total=estimated_total_chunks, desc="Processing Chunks", unit="chunk") as pbar:
        for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, low_memory=False):
            process_chunk(chunk)
            flush_to_disk()
            pbar.update(1)
            pbar.set_postfix_str(f"Rows processed: {total_rows:,}")

if __name__ == "__main__":
    main()
