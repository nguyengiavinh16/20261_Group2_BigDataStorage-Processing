"""Locate the repository root (or an explicit override) so config/contracts work from any CWD and in images."""

import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def repo_root() -> Path:
    """Directory containing `config/base.yaml` and `contracts/`.

    Order: $CG_ROOT, then walk up from CWD, then walk up from this file. Images copy `config/` and
    `contracts/` to /app and set CG_ROOT=/app.
    """
    env = os.environ.get("CG_ROOT")
    if env:
        return Path(env).resolve()
    for start in (Path.cwd(), Path(__file__).resolve()):
        for candidate in (start, *start.parents):
            if (candidate / "config" / "base.yaml").exists() and (candidate / "contracts").is_dir():
                return candidate
    raise FileNotFoundError("Cannot find repo root (config/base.yaml). Set CG_ROOT.")


def contracts_dir() -> Path:
    return repo_root() / "contracts"


def config_dir() -> Path:
    return repo_root() / "config"
