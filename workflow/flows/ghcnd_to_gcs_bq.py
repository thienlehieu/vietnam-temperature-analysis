from prefect import flow
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark import SparkFiles
from spark_cluster import SparkCluster
from setting import *

spark = SparkCluster().setUp()

@flow()
def ghcndToGcsFlow(year):
    file_url = f"https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/{year}.csv.gz"
    spark.sparkContext.addFile(file_url)
    gcs_path = f"gs://{GCS_BUCKET_NAME}/data/pq/climate/{year}/"
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
        .csv("file://" + SparkFiles.get(f"{year}.csv.gz"))
    df = df.withColumn("countryCode", F.substring("StationId", 0, 2))
    df.write.format("parquet") \
        .partitionBy("countryCode") \
        .option("path", gcs_path) \
        .mode("overwrite") \
        .save()

@flow()
def ghcndToBqFlow(year, countryCode):
    url = f"gs://{GCS_BUCKET_NAME}/data/pq/climate/{year}/countryCode={countryCode}/*"
    output_url = f"noaa_ghcn_all.{countryCode}_raw"
    df = spark.read.parquet(url)
    df.write.format("bigquery") \
        .option("parentProject", GCLOUD_PROJECT_ID) \
        .option("table", output_url) \
        .option("temporaryGcsBucket", GCS_TEMP_BUCKET_NAME) \
        .mode("append") \
        .save()

@flow() 
def ghcndToGcsBqFlow(years = [2020, 2021, 2022, 2023], countryCode = "VM"):
   for year in years:
       ghcndToGcsFlow(year)
       ghcndToBqFlow(year, countryCode)
   