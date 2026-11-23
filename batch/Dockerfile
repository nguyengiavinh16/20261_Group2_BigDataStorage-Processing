# Spark 3.5 image for SparkApplications. Build from repo root: make image-batch
# Iceberg/Hadoop/Postgres jars are baked into /opt/spark/jars (BAT-02) so pods do not download them on every
# run. Versions MUST match cg_batch.spark_session.PACKAGES (checked by batch/tests/test_ddl.py).
# aws-java-sdk-bundle is the transitive dependency of hadoop-aws 3.3.4 (hadoop-project pom: 1.12.262).
FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
WORKDIR /app
# Ubuntu 22.04's pip 22 + setuptools 59 ignore PEP 621 [project] metadata: libs/common got installed as
# "UNKNOWN" with no modules and no deps (seen in BAT-02). Upgrade first.
RUN pip3 install --no-cache-dir -U pip "setuptools>=68" wheel
COPY batch/requirements.txt /app/batch/requirements.txt
RUN pip3 install --no-cache-dir -r /app/batch/requirements.txt
COPY libs/common /app/libs/common
RUN pip3 install --no-cache-dir /app/libs/common
COPY config /app/config
COPY contracts /app/contracts
COPY batch/src /app/batch/src
COPY batch/ddl /app/batch/ddl
ENV CG_ROOT=/app PYTHONPATH=/app/batch/src CG_SPARK__JARS_PACKAGES=false
USER spark
