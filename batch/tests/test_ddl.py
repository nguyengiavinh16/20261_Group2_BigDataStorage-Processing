from cg_batch.ddl import created_object, default_ddl_path, split_statements, with_catalog
from cg_batch.spark_session import PACKAGES, spark_conf
from cryptoguard_common.config import load_config
from cryptoguard_common.paths import repo_root


def test_real_ddl_file_parses_into_namespaces_and_tables():
    statements = split_statements(default_ddl_path().read_text("utf-8"))
    objects = [created_object(s) for s in statements]
    assert None not in objects  # nothing but CREATE … IF NOT EXISTS in the file
    assert [o for o in objects if o[0] == "namespace"] == [
        ("namespace", "cg.silver"),
        ("namespace", "cg.gold"),
    ]
    tables = [name for kind, name in objects if kind == "table"]
    assert len(tables) == 11
    assert {"cg.silver.trades", "cg.gold.pump_cases", "cg.gold.data_quality_daily"} <= set(tables)
    assert all("--" not in s for s in statements)


def test_split_keeps_quoted_semicolons_and_dashes():
    sql = """
    -- header; with a semicolon
    CREATE TABLE IF NOT EXISTS cg.silver.t (a STRING COMMENT 'x;y -- z'); -- trailing
    CREATE NAMESPACE IF NOT EXISTS cg.gold;
    """
    assert split_statements(sql) == [
        "CREATE TABLE IF NOT EXISTS cg.silver.t (a STRING COMMENT 'x;y -- z')",
        "CREATE NAMESPACE IF NOT EXISTS cg.gold",
    ]


def test_with_catalog_renames_only_the_catalog():
    stmt = "CREATE TABLE IF NOT EXISTS cg.silver.trades (cg_id STRING)"
    assert with_catalog(stmt, "cg") == stmt
    assert (
        with_catalog(stmt, "lake") == "CREATE TABLE IF NOT EXISTS lake.silver.trades (cg_id STRING)"
    )


def test_spark_conf_jars_switch():
    # explicit values: load_config also reads the developer's .env, which may hold the Windows override
    default = {"CG_SPARK__JARS_PACKAGES": "true", "CG_SPARK__DRIVER_EXTRA_CLASSPATH": ""}
    conf = spark_conf(load_config("local", env=default))
    assert "spark.jars.packages" in conf and "spark.driver.extraClassPath" not in conf
    assert conf["spark.driver.extraJavaOptions"] == "-Duser.timezone=UTC"
    assert conf["spark.driver.memory"] == "1g"  # config/local.yaml spark.driver_memory
    baked = spark_conf(load_config("local", env={**default, "CG_SPARK__JARS_PACKAGES": "false"}))
    assert "spark.jars.packages" not in baked
    assert "spark.driver.extraClassPath" not in baked
    assert baked["spark.sql.catalog.cg.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    windows_host = spark_conf(
        load_config(
            "local",
            env={
                "CG_SPARK__JARS_PACKAGES": "false",
                "CG_SPARK__DRIVER_EXTRA_CLASSPATH": "C:/Users/me/.ivy2/jars/*",
            },
        )
    )
    assert windows_host["spark.driver.extraClassPath"] == "C:/Users/me/.ivy2/jars/*"
    assert "spark.jars.packages" not in windows_host


def test_dockerfile_bakes_the_same_jars_as_packages():
    dockerfile = (repo_root() / "batch" / "Dockerfile").read_text("utf-8")
    assert "CG_SPARK__JARS_PACKAGES=false" in dockerfile
    for coordinate in PACKAGES:
        group, artifact, version = coordinate.split(":")
        path = f"/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar"
        assert path in dockerfile, f"{coordinate} not baked into batch/Dockerfile"
