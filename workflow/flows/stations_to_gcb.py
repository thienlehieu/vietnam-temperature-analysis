from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from datetime import timedelta

@task(log_prints=True)
def write_gcs(df, gcsPath, format):
   gcs_block = GcsBucket.load("noaa-ghcnd")
   gcs_block.upload_from_dataframe(df, gcsPath, format)

@flow(name="Upload weather stations to gcs")
def stationsToGcb():
  d = {"stationId": [], "countryCode": [],"lat": [], "long": [], "elevation": [], 
       "state": [], "name": [], "gsn": [], "hcnCrn": [], "wmoId": []}
  with open("data/raw/station/ghcnd-stations.txt") as f:
     for row in f:
        d["stationId"].append(row[0:11].strip())
        d["countryCode"].append(row[0:2].strip())
        d["lat"].append(row[12:20].strip())
        d["long"].append(row[21:30].strip())
        d["elevation"].append(row[31:37].strip())
        d["state"].append(row[38:40].strip())
        d["name"].append(row[41:71].strip())
        d["gsn"].append(row[72:75].strip())
        d["hcnCrn"].append(row[76:79].strip())
        d["wmoId"].append(row[80:85].strip())
  df = pd.DataFrame(d)
  write_gcs(df, "data/pq/stations.parquet", "parquet")
  #df.to_csv("data/stations.csv.gz", encoding='utf-8', index=False, compression="gzip")
if __name__ == "__main__":
    stationsToGcb()
