from .base_engine import BaseEngine
import modin.pandas as pd

class ModinEngine(BaseEngine):
    def load(self, filepath):
        self.df = pd.read_csv(filepath)

    def clean(self):
        df = self.df

        df['transit_timestamp'] = pd.to_datetime(df['transit_timestamp'], errors='coerce')
        df['ridership'] = pd.to_numeric(df['ridership'], errors='coerce')
        df['transfers'] = pd.to_numeric(df['transfers'], errors='coerce')

        for col in ['station_complex', 'borough', 'payment_method', 'fare_class_category']:
            df[col] = df[col].astype(str).str.strip().str.lower()

        df = df.dropna(subset=['transit_timestamp', 'borough', 'ridership', 'station_complex'])
        df = df[df['ridership'] >= 0]
        df = df[df['latitude'].between(40, 41)]
        df = df[df['longitude'].between(-74.5, -73)]

        self.df = df

    def aggregate(self):
        self.result = self.df.groupby('borough')['ridership'].sum().reset_index()

    def export(self, output_path):
        # Modin's to_csv still delegates to Pandas for single-file writes
        self.result._to_pandas().to_csv(output_path, index=False)
