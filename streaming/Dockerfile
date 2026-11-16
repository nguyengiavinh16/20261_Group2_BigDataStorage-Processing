# One PyFlink image for both jobs (normalizer, detector). Build from repo root: make image-streaming
# VERIFY (INF-06/ING-06): Flink version must match the operator's supported versions and the Kafka connector jar.
FROM flink:1.20

ARG KAFKA_CONNECTOR=flink-sql-connector-kafka-3.3.0-1.20.jar
RUN apt-get update -y && apt-get install -y --no-install-recommends python3 python3-pip python3-dev \
    && rm -rf /var/lib/apt/lists/* && ln -sf /usr/bin/python3 /usr/bin/python
RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/${KAFKA_CONNECTOR}

WORKDIR /app
COPY streaming/requirements.txt /app/streaming/requirements.txt
RUN pip3 install --no-cache-dir -r /app/streaming/requirements.txt
COPY libs/common /app/libs/common
RUN pip3 install --no-cache-dir /app/libs/common
COPY config /app/config
COPY contracts /app/contracts
COPY streaming/normalizer/src /app/streaming/normalizer/src
COPY streaming/detector/src /app/streaming/detector/src
ENV CG_ROOT=/app PYTHONPATH=/app/streaming/normalizer/src:/app/streaming/detector/src PYTHONUNBUFFERED=1
