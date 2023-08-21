from environs import Env
import os
from pathlib import Path

env_path = "/Users/admin/Public/project/noaa-climate-datasets/.env"
env = Env()
env.read_env(env_path)
if not Path(env_path).exists():
    raise IOError("Missing .env file")

GCLOUD_CREDENTIAL_LOCATION = env.str("GCLOUD_CREDENTIAL_LOCATION", default="/app/lib/gcloud_creds.json")
GCS_CONNECTOR_LIB = env.str("GCS_CONNECTOR_LIB", default="/app/lib/gcs-connector-hadoop3-latest.jar")
BQ_CONNECTOR_LIB = env.str("BQ_CONNECTOR_LIB", default="/app/lib/spark-bigquery-with-dependencies_2.12-0.23.2.jar")