# Spark 3.5 image for SparkApplications. Build from repo root: make image-batch
# VERIFY (BAT-02): tag exists and ships Python 3.10+; Iceberg/Hadoop jars are pulled via spark.jars.packages
# (or bake them into /opt/spark/jars for offline/cloud runs).
FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
WORKDIR /app
COPY batch/requirements.txt /app/batch/requirements.txt
RUN pip3 install --no-cache-dir -r /app/batch/requirements.txt
COPY libs/common /app/libs/common
RUN pip3 install --no-cache-dir /app/libs/common
COPY config /app/config
COPY contracts /app/contracts
COPY batch/src /app/batch/src
COPY batch/ddl /app/batch/ddl
ENV CG_ROOT=/app PYTHONPATH=/app/batch/src
USER spark
