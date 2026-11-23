"""SparkSession factory with Iceberg (JDBC catalog) + S3-compatible storage, driven by config (BAT-02).

Local: MinIO via S3FileIO / s3a. Cloud (P4): GCS. ADR-0002 decides between S3 interoperability and GCSFileIO;
only `iceberg_storage_conf` should need to change.
"""

# VERIFY (BAT-02): keep versions consistent with the Spark image (3.5.x, Scala 2.12).
PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    "org.postgresql:postgresql:42.7.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]


def spark_conf(cfg: dict) -> dict[str, str]:
    """All Spark settings as a dict (pure → unit-testable, and reusable in SparkApplication manifests)."""
    st, pg, ice = cfg["storage"], cfg["postgres"], cfg["iceberg"]
    cat = f"spark.sql.catalog.{ice['catalog_name']}"
    warehouse = f"s3://{st['bucket']}/{st['warehouse_prefix']}"
    access, secret = st.get("access_key", ""), st.get("secret_key", "")
    sp = cfg.get("spark", {})
    conf = {}
    # The cg-batch image bakes the jars into /opt/spark/jars and sets CG_SPARK__JARS_PACKAGES=false,
    # so pods do not download ~356 MB from Maven Central on every run. Host runs resolve them via Ivy.
    if sp.get("jars_packages", True):
        conf["spark.jars.packages"] = ",".join(PACKAGES)
    # Windows host without winutils.exe: packages go through SparkContext.addFile → chmod → winutils and
    # fail. In local mode the driver JVM runs the tasks, so putting the (already resolved) jars on its
    # classpath is enough, e.g. CG_SPARK__DRIVER_EXTRA_CLASSPATH=C:/Users/<you>/.ivy2/jars/*
    if sp.get("driver_extra_classpath"):
        conf["spark.driver.extraClassPath"] = str(sp["driver_extra_classpath"])
    # Only effective for host runs (local mode); SparkApplication sets driver memory in its spec.
    if sp.get("driver_memory"):
        conf["spark.driver.memory"] = str(sp["driver_memory"])
    return conf | {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.session.timeZone": "UTC",
        # JVM default zone leaks into the Postgres JDBC handshake; Windows maps Vietnam to the legacy
        # "Asia/Saigon", which postgres:16 rejects (seen in BAT-02). Everything is UTC anyway (ADR-0004).
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{cat}.uri": f"jdbc:postgresql://{pg['host']}:{pg['port']}/{ice['jdbc_db']}",
        f"{cat}.jdbc.user": pg["user"],
        f"{cat}.jdbc.password": str(pg.get("password", "")),
        f"{cat}.warehouse": warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": st["endpoint"],
        f"{cat}.s3.path-style-access": str(st.get("path_style", True)).lower(),
        f"{cat}.s3.access-key-id": access,
        f"{cat}.s3.secret-access-key": secret,
        f"{cat}.client.region": st.get("region", "us-east-1"),
        # s3a:// for reading raw Bronze files (CSV/zip/Parquet/JSONL)
        "spark.hadoop.fs.s3a.endpoint": st["endpoint"],
        "spark.hadoop.fs.s3a.path.style.access": str(st.get("path_style", True)).lower(),
        "spark.hadoop.fs.s3a.access.key": access,
        "spark.hadoop.fs.s3a.secret.key": secret,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }


def bronze_uri(cfg: dict, *parts: str) -> str:
    st = cfg["storage"]
    return "/".join([f"s3a://{st['bucket']}", st["bronze_prefix"], *parts])


def get_spark(app_name: str, cfg: dict):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_conf(cfg).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
