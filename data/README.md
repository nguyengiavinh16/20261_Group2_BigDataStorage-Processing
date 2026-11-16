# data/: local scratch data (git-ignored)

Everything in this folder except this README is ignored by git. Use it for quick local experiments only;
pipelines read and write the object store (MinIO locally), not this folder.

Suggested layout: `data/raw/`, `data/tmp/`, `data/notebook-outputs/`.
Respect the local data budget in `config/local.yaml` (`data_budget`).
