FROM google/cloud-sdk:latest
RUN gsutil cp gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar tmp/
RUN gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar tmp/

FROM python:3.9.8-slim-buster AS py3
FROM openjdk:8-slim-buster

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.4.1
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
RUN pip --no-cache-dir install prefect==2.10.20
RUN pip --no-cache-dir install environs==9.5.0

COPY workflow/flows /opt/prefect/flows
ARG WORKDIR=/app
RUN mkdir ${WORKDIR}
WORKDIR ${WORKDIR}
COPY --from=0 tmp lib
