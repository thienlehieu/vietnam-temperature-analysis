from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def write_gcs(df, gcsPath, format="csv_gzip"):
   gcs_block = GcsBucket.load("noaa-ghcnd")
   gcs_block.upload_from_dataframe(df, gcsPath, format)

@task(log_prints=True)
def write_bq(df, table):
   gcp_credentials_block = GcpCredentials.load("gcs-dce-creds")
   df.to_gbq(
      destination_table=table,
      project_id="swift-arcadia-387709",
      credentials=gcp_credentials_block.get_credentials_from_service_account(),
      chunksize=500_000,
      if_exists="append",
  )
