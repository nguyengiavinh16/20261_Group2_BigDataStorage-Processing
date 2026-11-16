"""Layered configuration: config/base.yaml < config/<profile>.yaml < .env < environment variables.

Env override format: CG_<SECTION>__<KEY>[__<SUBKEY>...]=value, e.g.
    CG_KAFKA__BOOTSTRAP_SERVERS=localhost:32100
    CG_DETECTOR__PUMP_RULE__VOLUME_Z_MIN=4.5
Values are parsed with YAML, so numbers/booleans/lists keep their types.
"""

import copy
import os
from pathlib import Path
from typing import Any

import yaml

from cryptoguard_common.paths import config_dir, repo_root

ENV_PREFIX = "CG_"


def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _read_dotenv(path: Path) -> dict[str, str]:
    """Minimal .env parser (KEY=VALUE, # comments). Does not override real env vars."""
    if not path.exists():
        return {}
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        result[key.strip()] = value.strip().strip('"').strip("'")
    return result


def _env_overrides(env: dict[str, str]) -> dict:
    tree: dict[str, Any] = {}
    for key, raw in env.items():
        if not key.startswith(ENV_PREFIX) or "__" not in key or key == "CG_PROFILE":
            continue
        parts = [p.lower() for p in key[len(ENV_PREFIX) :].split("__")]
        node = tree
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = yaml.safe_load(raw) if raw != "" else ""
    return tree


def load_config(profile: str | None = None, env: dict[str, str] | None = None) -> dict:
    """Return the merged config dict. `profile` defaults to $CG_PROFILE or 'local'."""
    dotenv = _read_dotenv(repo_root() / ".env")
    merged_env = {**dotenv, **(env if env is not None else os.environ)}
    profile = profile or merged_env.get("CG_PROFILE", "local")

    cfg = _read_yaml(config_dir() / "base.yaml")
    cfg = _deep_merge(cfg, _read_yaml(config_dir() / f"{profile}.yaml"))
    cfg = _deep_merge(cfg, _env_overrides(merged_env))
    cfg["profile"] = profile
    return cfg
