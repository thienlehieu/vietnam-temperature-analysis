set -e

URL_PREFIX="https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year"
LOCAL_PREFIX="data/raw/climate"
mkdir -p ${LOCAL_PREFIX}

for year in {2015..2022}; do
  URL="${URL_PREFIX}/${year}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${year}.csv.gz"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  wget ${URL} -O ${LOCAL_PATH}
done