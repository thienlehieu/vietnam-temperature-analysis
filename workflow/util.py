from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def write_gcs(df, gcsPath, format="csv_gzip"):
   gcs_block = GcsBucket.load("noaa-ghcnd")
   gcs_block.upload_from_dataframe(df, gcsPath, format)