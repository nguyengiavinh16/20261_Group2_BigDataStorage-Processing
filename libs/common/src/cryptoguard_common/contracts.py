"""Access to contracts/: JSON Schema validation and the Kafka topic registry."""

import json
from functools import cache, lru_cache

import yaml
from jsonschema import Draft202012Validator

from cryptoguard_common.paths import contracts_dir


class ContractError(ValueError):
    """Raised when a record does not match its schema."""


@cache
def _validator(schema_name: str) -> Draft202012Validator:
    path = contracts_dir() / "schemas" / f"{schema_name}.schema.json"
    return Draft202012Validator(json.loads(path.read_text(encoding="utf-8")))


def validate(record: dict, schema_name: str) -> None:
    """Validate `record` against e.g. 'canonical_trade.v1'. Raises ContractError with all errors."""
    errors = sorted(_validator(schema_name).iter_errors(record), key=lambda e: list(e.path))
    if errors:
        detail = "; ".join(f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors)
        raise ContractError(f"{schema_name}: {detail}")


def is_valid(record: dict, schema_name: str) -> bool:
    return _validator(schema_name).is_valid(record)


@lru_cache(maxsize=1)
def topic_registry() -> dict:
    return yaml.safe_load((contracts_dir() / "kafka-topics.yaml").read_text(encoding="utf-8"))


def topic(name: str) -> dict:
    """Registry entry for a topic; raises KeyError for unknown names (catches typos early)."""
    for entry in topic_registry()["topics"]:
        if entry["name"] == name:
            return entry
    raise KeyError(f"topic {name!r} is not in contracts/kafka-topics.yaml")


def replay_topic(name: str, prefix: str = "replay.") -> str:
    """Mirror topic used in replay mode, e.g. alerts.v1 -> replay.alerts.v1 (must exist in registry)."""
    mirrored = f"{prefix}{name}"
    topic(mirrored)
    return mirrored
