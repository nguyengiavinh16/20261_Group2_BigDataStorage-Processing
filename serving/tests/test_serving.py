import pytest

from cg_serving.mongo_schema import INDEXES


def test_every_index_documents_its_access_pattern():
    for collection, specs in INDEXES.items():
        for keys, _options, pattern in specs:
            assert keys and pattern, collection


def test_api_health():
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from cg_serving.api import app

    client = TestClient(app)
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/alerts").status_code == 501  # until MLS-03
