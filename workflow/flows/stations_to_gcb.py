import pandas as pd
from prefect import flow
from workflow.util import write_gcs, write_bq


@flow()
def stationsToGcsBq():
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
  write_gcs(df, "data/pq/station/stations.parquet", "parquet")
  write_bq(df, "noaa_ghcn_all.stations")

