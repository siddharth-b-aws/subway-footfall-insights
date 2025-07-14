from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SmokeTest").master("local[*]").getOrCreate()
print("✅ Spark version:", spark.version)
spark.stop()
