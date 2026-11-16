"""Contract checks: schemas are valid, samples behave, topic registry is consistent."""

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

CONTRACTS = Path(__file__).resolve().parents[1]
SCHEMAS = CONTRACTS / "schemas"
SAMPLES = CONTRACTS / "samples"


def _schema_files():
    return sorted(SCHEMAS.glob("*.schema.json"))


def _load_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


@pytest.mark.parametrize("schema_path", _schema_files(), ids=lambda p: p.name)
def test_schema_is_valid_draft_2020_12(schema_path):
    Draft202012Validator.check_schema(_load_schema(schema_path))


@pytest.mark.parametrize("schema_path", _schema_files(), ids=lambda p: p.name)
def test_every_schema_has_valid_and_invalid_samples(schema_path):
    stem = schema_path.name.replace(".v1.schema.json", "")
    assert (SAMPLES / f"{stem}.valid.jsonl").exists(), f"missing samples/{stem}.valid.jsonl"
    assert (SAMPLES / f"{stem}.invalid.jsonl").exists(), f"missing samples/{stem}.invalid.jsonl"


@pytest.mark.parametrize("schema_path", _schema_files(), ids=lambda p: p.name)
def test_samples(schema_path):
    validator = Draft202012Validator(_load_schema(schema_path))
    stem = schema_path.name.replace(".v1.schema.json", "")
    for i, msg in enumerate(_read_jsonl(SAMPLES / f"{stem}.valid.jsonl")):
        errors = list(validator.iter_errors(msg))
        assert not errors, f"{stem}.valid line {i + 1}: {errors[0].message}"
    for i, msg in enumerate(_read_jsonl(SAMPLES / f"{stem}.invalid.jsonl")):
        assert not validator.is_valid(msg), f"{stem}.invalid line {i + 1} unexpectedly valid"


def test_topic_registry_references_existing_schemas():
    registry = yaml.safe_load((CONTRACTS / "kafka-topics.yaml").read_text(encoding="utf-8"))
    names = [t["name"] for t in registry["topics"]]
    assert len(names) == len(set(names)), "duplicate topic names"
    for topic in registry["topics"]:
        assert (SCHEMAS / topic["schema"]).exists(), (
            f"{topic['name']}: unknown schema {topic['schema']}"
        )
        assert topic["name"].split(".")[-1].startswith("v"), f"{topic['name']}: missing .v<major>"
        for field in ("key", "producer", "consumers", "retention_hours"):
            assert field in topic, f"{topic['name']}: missing {field}"
