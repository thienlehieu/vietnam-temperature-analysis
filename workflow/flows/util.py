from prefect import task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from setting import *
import requests
from datetime import timedelta
import json
import os

@task(log_prints=True)
def write_gcs(df, gcsPath, format="csv_gzip"):
   gcs_block = GcsBucket.load("gcp-block")
   gcs_block.upload_from_dataframe(df, gcsPath, format)

@task(log_prints=True)
def write_bq(df, table):
   gcp_credentials_block = GcpCredentials.load("gcp-creds")
   df.to_gbq(
      destination_table=table,
      project_id=f"{GCLOUD_PROJECT_ID}",
      credentials=gcp_credentials_block.get_credentials_from_service_account(),
      chunksize=500_000,
      if_exists="append",
  )

@task()
def download_file(url, fileName):
   data = requests.get(url)
   with open(os.path.join(os.getcwd(), fileName), "wb+") as file:
      file.write(data.content)

@task()
def createPrefectBlocks():
  f = open(GCLOUD_CREDENTIAL_LOCATION)
  creds = json.load(f)
  credentialsBlock = GcpCredentials(
      service_account_info=creds
  )
  credentialsBlock.save("gcp-creds", overwrite=True)


  bucketBlock = GcsBucket(
      gcp_credentials=GcpCredentials.load("gcp-creds"),
      bucket=GCS_BUCKET_NAME,
  )
  bucketBlock.save("gcp-block", overwrite=True)