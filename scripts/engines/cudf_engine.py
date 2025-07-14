import cudf
from .base_engine import BaseEngine

class CuDFEngine(BaseEngine):
    def __init__(self):
        self.df = None
        self.result = None

    def load(self, filepath):
        self.df = cudf.read_csv(filepath)

    def clean(self):
        self.df['Date'] = cudf.to_datetime(self.df['Date'], errors='coerce')
        self.df = self.df.dropna(subset=['Date', 'Entries', 'Exits'])
        self.df['Station'] = self.df['Station'].str.strip()
        self.df['Line'] = self.df['Line'].str.strip()
        self.df['Entries'] = self.df['Entries'].astype('float32').fillna(0)
        self.df['Exits'] = self.df['Exits'].astype('float32').fillna(0)

    def aggregate(self):
        self.df['Total_Traffic'] = self.df['Entries'] + self.df['Exits']
        self.result = self.df.groupby(['Date', 'Station']).agg({'Total_Traffic': 'sum'}).reset_index()

    def export(self, output_path):
        self.result.to_pandas().to_csv(output_path, index=False)
