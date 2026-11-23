# Airflow image with CryptoGuard DAGs baked in. Build from repo root: make image-airflow
# Pinned to Airflow 2.10.5 = default of the official Helm chart 1.16.0 (chart ≥ 1.17 defaults to Airflow 3,
# verified in BAT-03). DAGs import cg_batch (planning/transfer logic), so batch/src is on PYTHONPATH.
FROM apache/airflow:2.10.5-python3.11
# --chown: pip runs as user airflow and must be able to rewrite a host-built *.egg-info in the context
COPY --chown=airflow:0 libs/common /opt/cg/libs/common
# The official image already ships the cncf.kubernetes and amazon providers (boto3). Installing providers
# without Airflow's constraints file can upgrade Airflow core itself, so only libs/common, with core pinned.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" /opt/cg/libs/common
COPY config /opt/cg/config
COPY contracts /opt/cg/contracts
COPY batch/src /opt/cg/batch/src
COPY batch/airflow/dags /opt/airflow/dags
ENV CG_ROOT=/opt/cg PYTHONPATH=/opt/cg/batch/src
