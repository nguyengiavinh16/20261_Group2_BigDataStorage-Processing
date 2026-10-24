from pathlib import Path

import pytest

from fraud_detection.config import load_yaml


def test_load_yaml_mapping(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("threshold: 0.8\nname: demo\n", encoding="utf-8")
    assert load_yaml(config_path) == {"threshold": 0.8, "name": "demo"}


def test_load_yaml_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_yaml(tmp_path / "missing.yaml")


def test_load_yaml_rejects_non_mapping(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("- item\n- item2\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_yaml(config_path)
