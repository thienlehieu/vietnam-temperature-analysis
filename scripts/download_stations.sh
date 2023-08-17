set -e

URL="https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt"

LOCAL_PREFIX="data/raw/station"
LOCAL_PATH="${LOCAL_PREFIX}/ghcnd-stations.txt"

echo "downloading ${URL} to ${LOCAL_PATH}"
mkdir -p ${LOCAL_PREFIX}
wget ${URL} -O ${LOCAL_PATH}