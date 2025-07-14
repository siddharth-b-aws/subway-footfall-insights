from .base_engine import BaseEngine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, lower, trim

class PySparkEngine(BaseEngine):
    def load(self, filepath):
        self.spark = SparkSession.builder.appName("MTA").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.df = self.spark.read.option("header", True).option("inferSchema", True).csv(filepath)

    def clean(self):
        df = self.df

        # Trim + lowercase string columns
        for colname in ['station_complex', 'borough', 'payment_method', 'fare_class_category']:
            df = df.withColumn(colname, lower(trim(col(colname))))

        # Filter bad rows
        df = df.dropna(subset=['transit_timestamp', 'borough', 'ridership', 'station_complex'])
        df = df.withColumn("ridership", col("ridership").cast("int"))
        df = df.withColumn("latitude", col("latitude").cast("float"))
        df = df.withColumn("longitude", col("longitude").cast("float"))
        df = df.filter(col("ridership") >= 0)
        df = df.filter((col("latitude") >= 40) & (col("latitude") <= 41))
        df = df.filter((col("longitude") >= -74.5) & (col("longitude") <= -73))

        self.df = df

    def aggregate(self):
        self.result = self.df.groupBy("borough").agg(_sum("ridership").alias("ridership"))

    def export(self, output_path):
        self.result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
