from prefect import flow
from pyspark.sql import functions as F
from pyspark.sql import types
from spark_cluster import SparkCluster

spark = SparkCluster().setUp()

@flow()
def ghcndToGcsFlow(year):
    file_url = f"data/raw/climate/{year}.csv.gz"
    gcs_path = f"gs://noaa_ghcn_data_lake_swift-arcadia-387709/data/pq/climate/{year}/"
    schema = types.StructType([
        types.StructField("StationId", types.StringType(), True),
        types.StructField("DateStr", types.StringType(), True),
        types.StructField("Element", types.StringType(), True),
        types.StructField("Value", types.DoubleType(), True),
        types.StructField("M-Flag", types.StringType(), True),
        types.StructField("Q-Flag", types.StringType(), True),
        types.StructField("S-Flag", types.StringType(), True),
        types.StructField("Obs-Time", types.StringType(), True)
    ])
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(file_url)
    df = df.withColumn('countryCode', F.substring("StationId", 0, 2))
    df.write.format('parquet') \
    .partitionBy("countryCode") \
    .option('path', gcs_path) \
    .save()

@flow()
def ghcndToBqFlow(year, countryCode):
    url = f"gs://noaa_ghcn_data_lake_swift-arcadia-387709/data/pq/climate/{year}/countryCode={countryCode}/*"
    output_url = f"noaa_ghcn_all.{countryCode}_raw"
    df = spark.read.parquet(url)
    df.write.format('bigquery') \
        .option('table', output_url) \
        .option('temporaryGcsBucket', 'dataproc-temp-asia-southeast1-268226740873-e5cx4k3f') \
        .mode("append") \
        .save()

@flow() 
def ghcndToGcsBqFlow(years = [2020, 2021, 2022, 2023], countryCode = "VM"):
   for year in years:
       ghcndToGcsFlow(year)
       ghcndToBqFlow(year, countryCode)
   