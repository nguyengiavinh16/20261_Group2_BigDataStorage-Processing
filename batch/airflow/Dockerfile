# Airflow image with CryptoGuard DAGs baked in. Build from repo root: make image-airflow
# VERIFY (INF-10): pin the same Airflow version as the Helm chart's default.
FROM apache/airflow:2.10.5-python3.11
COPY libs/common /opt/cg/libs/common
RUN pip install --no-cache-dir /opt/cg/libs/common apache-airflow-providers-cncf-kubernetes boto3
COPY config /opt/cg/config
COPY contracts /opt/cg/contracts
COPY batch/airflow/dags /opt/airflow/dags
ENV CG_ROOT=/opt/cg
