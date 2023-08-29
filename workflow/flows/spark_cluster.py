from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from setting import *

class SparkCluster:
  def __init__(self):
    self.conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('Monkey D.Luffy') \
        .set("spark.jars", f"{GCS_CONNECTOR_LIB_PATH},{BQ_CONNECTOR_LIB_PATH}") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCLOUD_CREDENTIAL_LOCATION)
  
  def setUp(self):
    sc = SparkContext(conf=self.conf)
    self.hadoopConf(sc)
    return SparkSession.builder \
        .appName('Monkey D.Luffy') \
        .getOrCreate()

  def hadoopConf(self, sparkContext):
    self.hadoop_conf = sparkContext._jsc.hadoopConfiguration()
    self.hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    self.hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    self.hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", GCLOUD_CREDENTIAL_LOCATION)
    self.hadoop_conf.set("fs.gs.auth.service.account.enable", "true")