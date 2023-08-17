from environs import Env
import os

env = Env()
env.read_env(path=os.path.join(os.getcwd(), ".env"))

GCLOUD_CREDENTIAL_LOCATION = env.str("GCLOUD_CREDENTIAL_LOCATION")
GCS_CONNECTOR_LIB = env.str("GCS_CONNECTOR_LIB")
BQ_CONNECTOR_LIB = env.str("BQ_CONNECTOR_LIB")