from .base_engine import BaseEngine
import pandas as pd

class ChunkedPandasEngine(BaseEngine):
    def load(self, filepath):
        self.filepath = filepath
        self.chunk_size = 100_000  # adjust based on your RAM
        self.chunks = []

    def clean(self):
        def clean_chunk(df):
            df['transit_timestamp'] = pd.to_datetime(df['transit_timestamp'], errors='coerce')
            df['ridership'] = pd.to_numeric(df['ridership'], errors='coerce')
            df['transfers'] = pd.to_numeric(df['transfers'], errors='coerce')
            for col in ['station_complex', 'borough', 'payment_method', 'fare_class_category']:
                df[col] = df[col].astype(str).str.strip().str.lower()
            df.dropna(subset=['transit_timestamp', 'borough', 'ridership', 'station_complex'], inplace=True)
            df = df[df['ridership'] >= 0]
            df = df[df['latitude'].between(40, 41)]
            df = df[df['longitude'].between(-74.5, -73)]
            return df

        for chunk in pd.read_csv(self.filepath, chunksize=self.chunk_size):
            self.chunks.append(clean_chunk(chunk))

    def aggregate(self):
        df = pd.concat(self.chunks, ignore_index=True)
        self.result = df.groupby('borough')['ridership'].sum().reset_index()

    def export(self, output_path):
        self.result.to_csv(output_path, index=False)
