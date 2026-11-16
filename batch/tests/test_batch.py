import pytest

from cg_batch.graph.taint import hops_from_seeds, propagate_taint
from cg_batch.spark_session import spark_conf
from cryptoguard_common.config import load_config

#   bad ──100──▶ a ──50──▶ b
#   clean ─100─▶ a          b ──10──▶ exch ──10──▶ c
EDGES = [
    ("bad", "a", 100.0),
    ("clean", "a", 100.0),
    ("a", "b", 50.0),
    ("b", "exch", 10.0),
    ("exch", "c", 10.0),
]


def test_taint_haircut_and_decay():
    t = propagate_taint(EDGES, {"bad"}, decay=0.5, max_hops=3, exchange_wallets=frozenset({"exch"}))
    assert t["bad"] == 1.0
    assert t["a"] == pytest.approx(0.25)  # 0.5 * (1.0*100)/200
    assert t["b"] == pytest.approx(0.125)  # 0.5 * 0.25
    assert t["exch"] == pytest.approx(0.0625)  # exchange receives taint...
    assert "c" not in t  # ...but does not propagate it


def test_hops():
    d = hops_from_seeds(EDGES, {"bad"}, max_hops=3, exchange_wallets=frozenset({"exch"}))
    assert d == {"bad": 0, "a": 1, "b": 2, "exch": 3}


def test_spark_conf_uses_config():
    conf = spark_conf(load_config("local", env={}))
    assert conf["spark.sql.catalog.cg.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert conf["spark.sql.catalog.cg.uri"] == "jdbc:postgresql://localhost:30432/iceberg_catalog"
    assert conf["spark.sql.catalog.cg.warehouse"] == "s3://cg-lake/warehouse"
