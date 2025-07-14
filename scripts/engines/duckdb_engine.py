from .base_engine import BaseEngine
import duckdb

class DuckDBEngine(BaseEngine):
    def load(self, filepath):
        self.con = duckdb.connect()
        self.df_name = 'ridership'
        self.con.execute(f"""
            CREATE TABLE {self.df_name} AS 
            SELECT * FROM read_csv_auto('{filepath}', 
                DATEFORMAT='%m/%d/%Y %I:%M:%S %p', 
                ignore_errors=True)
        """)

    def clean(self):
        query = f"""
            CREATE OR REPLACE TABLE {self.df_name} AS
            SELECT * FROM {self.df_name}
            WHERE 
                transit_timestamp IS NOT NULL AND
                borough IS NOT NULL AND
                ridership >= 0 AND
                station_complex IS NOT NULL AND
                latitude BETWEEN 40 AND 41 AND
                longitude BETWEEN -74.5 AND -73
        """
        self.con.execute(query)

    def aggregate(self):
        self.result_df = self.con.execute(f"""
            SELECT borough, SUM(ridership) as ridership 
            FROM {self.df_name}
            GROUP BY borough
        """).fetchdf()

    def export(self, output_path):
        self.result_df.to_csv(output_path, index=False)
