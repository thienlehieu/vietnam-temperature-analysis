FROM google/cloud-sdk:latest
RUN gsutil cp gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar tmp/
RUN gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar tmp/

FROM python:3.9.8-slim-buster AS py3
FROM openjdk:8-slim-buster

COPY --from=py3 / /

COPY requirements.txt .

RUN pip --no-cache-dir install -r requirements.txt

COPY workflow/flows /opt/prefect/flows
ARG WORKDIR=/app
RUN mkdir ${WORKDIR}
WORKDIR ${WORKDIR}
COPY --from=0 tmp lib
