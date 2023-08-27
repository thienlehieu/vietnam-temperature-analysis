from prefect import flow
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from spark_cluster import SparkCluster
from setting import *

spark = SparkCluster().setUp()
sqlContext = SQLContext(spark.sparkContext)
sqlContext.sql("set spark.sql.caseSensitive=true")

@flow()
def avgTempPerYear(year, countryCode):
    url = f"gs://{GCS_BUCKET_NAME}/data/pq/climate/{year}/countryCode={countryCode}/*"
    stationUrl = f"gs://{GCS_BUCKET_NAME}/data/pq/station/stations.parquet"
    output_url = f"noaa_ghcn_all.{countryCode}_avg_tmp_{year}"

    df = spark.read.parquet(url) \
        .withColumn('Date', F.to_date('DateStr', "yyyyMMdd")) \
        .withColumnRenamed('StationId', 'Station_id')
    df_station = spark.read.parquet(stationUrl)
    df = df \
        .join(df_station, df.Station_id == df_station.stationId) \
        .drop('stationId')
    df.createOrReplaceTempView("data")
    df_avg_tmp = spark.sql("""
        select 
            Station_id, 
            Element, 
            year(Date) AS Year, 
            name as City,
            AVG(Value)/10 as Average_tmp,
            MAX(Value)/10 as Max_tmp,
            Min(Value)/10 as Min_tmp
        from 
            data
        where 
            Element = 'TAVG'
        group by 
            1, 2, 3, 4
    """)

    df_avg_tmp.write.format("bigquery") \
        .option("parentProject", GCLOUD_PROJECT_ID) \
        .option("table", output_url) \
        .option("temporaryGcsBucket", GCS_TEMP_BUCKET_NAME) \
        .mode("append") \
        .save()

@flow()
def writeAvgTempReportToBq(years = [2020, 2021, 2022, 2023], countryCode = "VM"):
    for year in years:
        avgTempPerYear(year, countryCode)