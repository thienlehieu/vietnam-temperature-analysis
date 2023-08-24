import pandas as pd
from prefect import flow
from util import *
from setting import *

@flow()
def stationsToGcsBq():
   localPath = "ghcnd-stations.txt"
   fileUrl = "https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt"
   download_file(fileUrl, "ghcnd-stations.txt")
   initDf = {"stationId": [], "countryCode": [],"lat": [], "long": [], "elevation": [], 
         "state": [], "name": [], "gsn": [], "hcnCrn": [], "wmoId": []}
   with open(localPath) as f:
      for row in f:
         initDf["stationId"].append(row[0:11].strip())
         initDf["countryCode"].append(row[0:2].strip())
         initDf["lat"].append(row[12:20].strip())
         initDf["long"].append(row[21:30].strip())
         initDf["elevation"].append(row[31:37].strip())
         initDf["state"].append(row[38:40].strip())
         initDf["name"].append(row[41:71].strip())
         initDf["gsn"].append(row[72:75].strip())
         initDf["hcnCrn"].append(row[76:79].strip())
         initDf["wmoId"].append(row[80:85].strip())
   df = pd.DataFrame(initDf)
   createPrefectBlocks()
   write_gcs(df, "data/pq/station/stations.parquet", "parquet")
   write_bq(df, f"{BQ_DATASET}.stations")

