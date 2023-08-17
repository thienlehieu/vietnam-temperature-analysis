from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from datetime import timedelta
from workflow.util import write_gcs
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from .setting import *

credentials_location = '/Users/admin/Downloads/swift-arcadia-387709-b04513fcbebe.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('Monkey D.Luffy') \
    .set("spark.jars", f"{GCS_CONNECTOR_LIB},{BQ_CONNECTOR_LIB}") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCLOUD_CREDENTIAL_LOCATION)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", GCLOUD_CREDENTIAL_LOCATION)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .appName('Monkey D.Luffy') \
    .getOrCreate()

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

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    df = pd.read_csv(dataset_url)
    return df

@flow()
def ghcndToGcs(year):
  datasetUrl = f"https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/{year}.csv.gz"
  df = fetch(datasetUrl)
  write_gcs(df, f"data/raw/climate/{year}.csv.gz")

@flow()
def ghcndToGcsFlow(years = [2022]):
   for year in [2022]:
      file_url = f"data/raw/climate/{year}.csv.gz"
      gcs_path = f"gs://noaa_ghcn_data_lake_swift-arcadia-387709/data/pq/climate/{year}/"
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
def writeToBq(years = [2022], countryCode = "US"):
   for year in years:
    url = f"gs://noaa_ghcn_data_lake_swift-arcadia-387709/data/pq/climate/{year}/countryCode={countryCode}/*"
    output_url = f"noaa_ghcn_all.{countryCode}"
    df = spark.read.parquet(url)
    df.write.format('bigquery') \
        .option('table', output_url) \
        .option('temporaryGcsBucket', 'dataproc-temp-asia-southeast1-268226740873-e5cx4k3f') \
        .mode("append") \
        .save()
   

#if __name__ == "__main__":
    # ghcndToGcsFlow()