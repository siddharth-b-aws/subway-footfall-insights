from .base_engine import BaseEngine
import dask.dataframe as dd
import pandas as pd

class DaskEngine(BaseEngine):
    def load(self, filepath):
        # Use dtype inference + low_memory=False to reduce parser errors
        self.df = dd.read_csv(
            filepath,
            dtype=str,  # to delay dtype parsing
            assume_missing=True,  # avoids dtype inference on ints
            blocksize="64MB"  # adjust for your memory
        )

    def clean(self):
        df = self.df

        # Parse timestamps safely
        df['transit_timestamp'] = dd.to_datetime(df['transit_timestamp'], errors='coerce')

        # Convert numeric
        df['ridership'] = dd.to_numeric(df['ridership'], errors='coerce')
        df['transfers'] = dd.to_numeric(df['transfers'], errors='coerce')
        df['latitude'] = dd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = dd.to_numeric(df['longitude'], errors='coerce')

        # String cleaning
        for col in ['station_complex', 'borough', 'payment_method', 'fare_class_category']:
            df[col] = df[col].astype(str).str.strip().str.lower()

        # Filter invalid/missing rows
        df = df.dropna(subset=['transit_timestamp', 'borough', 'ridership', 'station_complex'])
        df = df[df['ridership'] >= 0]
        df = df[df['latitude'].between(40, 41)]
        df = df[df['longitude'].between(-74.5, -73)]

        self.df = df

    def aggregate(self):
        # Group by borough and sum ridership
        self.result = self.df.groupby('borough')['ridership'].sum().compute().reset_index()

    def export(self, output_path):
        # Export to CSV using pandas
        self.result.to_csv(output_path, index=False)
