FROM prefecthq/prefect:2.7.7-python3.9
RUN pip install pyspark==3.4.1
RUN apt-get update \
  && apt-get install wget unzip zip -y
COPY workflow/flows /opt/prefect/flows
ARG WORKDIR=/app
RUN mkdir $WORKDIR
WORKDIR $WORKDIR
COPY lib lib
COPY scripts scripts
ENTRYPOINT ["sh", "./scripts/download_raw.sh" ]
CMD [ "2023" ]

