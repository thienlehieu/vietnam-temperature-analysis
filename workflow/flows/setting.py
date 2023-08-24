from environs import Env
import os
from pathlib import Path

env = Env()
env_path = "/Users/admin/Public/project/noaa-climate-datasets/.env"
if Path(env_path).exists():
    env.read_env(env_path)
env.read_env()

GCLOUD_CREDENTIAL_LOCATION = env.str("GCLOUD_CREDENTIAL_LOCATION", default="/app/lib/gcloud_creds.json")
GCS_CONNECTOR_LIB = env.str("GCS_CONNECTOR_LIB", default="/app/lib/gcs-connector-hadoop3-latest.jar")
BQ_CONNECTOR_LIB = env.str("BQ_CONNECTOR_LIB", default="/app/lib/spark-bigquery-with-dependencies_2.12-0.23.2.jar")
GCLOUD_PROJECT_ID = env.str("GCLOUD_PROJECT_ID")
GCS_BUCKET_NAME = env.str("GCS_BUCKET_NAME")
GCS_TEMP_BUCKET_NAME = env.str("GCS_TEMP_BUCKET_NAME")
BQ_DATASET = env.str("BQ_DATASET")

