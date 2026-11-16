# Contributing

## 1. Branches

- `main` is protected: no direct pushes, PR + 1 approval + green CI required.
- Branch name: `<type>/<task-id>-<short-slug>`, e.g. `feat/ING-03-binance-collector`, `fix/DET-05-zscore-nan`, `docs/BAT-01-source-notes`.
- `<type>`: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `infra`.
- Keep branches short-lived (≤ 3 days). Rebase on `main` before opening the PR.

## 2. Commits ([Conventional Commits](https://www.conventionalcommits.org/))

```
<type>(<scope>): <summary in imperative, ≤ 72 chars> [<TASK-ID>]

<optional body: why, not what>
```

Scopes = top-level areas: `contracts`, `common`, `infra`, `ingest`, `normalizer`, `detector`, `batch`, `graph`, `ml`, `serving`, `docs`, `ci`.

Examples:
```
feat(ingest): add Binance combined-stream collector with 24h rotation [ING-03]
fix(normalizer): map Coinbase maker side to taker_side correctly [ING-05]
docs(contracts): document alert reason codes [DET-05]
```

## 3. What to commit / what NOT to commit

| Commit ✅ | Never commit ❌ |
|---|---|
| Source code, tests, Dockerfiles | Any real data (`data/` is git-ignored), Parquet/CSV/zip dumps |
| K8s manifests, Helm values, scripts | `.env`, kubeconfig, cloud credentials, HMAC keys, API keys |
| JSON Schemas, topic registry, reference CSVs (small, curated) | Model binaries (they go to MLflow) |
| Small samples in `contracts/samples/` (< 200 KB each, no secrets) | Jupyter outputs (strip with `nbstripout` before commit) |
| Docs, ADRs, diagrams (source + exported PNG/SVG) | Generated files that `make` can rebuild (`infra/k8s/generated/` is the exception, see 6) |
| Notebooks in `ml/notebooks/` with outputs stripped | Files > 5 MB |

## 4. Ownership and reviews

- [.github/CODEOWNERS](.github/CODEOWNERS) maps folders to people. The owner must approve PRs touching their folder.
- You may open PRs outside your folder (help is welcome), but the owner decides.
- **`contracts/` is special:** a change needs approval from the producer **and every consumer** of that contract (listed in `contracts/kafka-topics.yaml` / `docs/data-contracts.md`).
  - Additive change (new optional field) → bump minor `schema_version` (`1.0` → `1.1`).
  - Breaking change (rename/remove/retype, change key meaning) → new major file (`*.v2.schema.json`) and new topic `*.v2`, plus an ADR in `docs/decisions/`.

## 5. Pull requests

- Use the template. Link the task ID. Say how you tested it (unit test, local cluster, screenshot).
- PR size: aim for < 400 changed lines. Split infra from logic.
- CI must pass: `ruff`, `pytest`, contract validation, generated-topics check.

## 6. Code conventions

- Python 3.11, formatted and linted by `ruff` (`make lint`, `make fmt`).
- **Business logic in pure functions** (no Kafka/Flink/Spark imports) in testable modules; engine code (Flink/Spark/Airflow) only wires I/O. This lets us unit-test everything and swap engines if needed (ADR-0003).
- All timestamps in messages: integer **epoch microseconds UTC**, field suffix `_ts_us` (ADR-0004).
- All config via `config/*.yaml` + env vars (`CG_*`), never hard-coded hosts, paths or thresholds. Use `cryptoguard_common.config.load_config()`.
- Logging: `cryptoguard_common.log.get_logger(__name__)`, JSON lines to stdout.
- Metrics: Prometheus client, metric names prefixed `cg_<component>_`.
- `infra/k8s/generated/` is produced by `make gen-topics` from `contracts/kafka-topics.yaml` and **is committed** so the cluster can be applied without Python; CI checks it is up to date.

## 7. Definition of Done (every task)

1. Code + unit tests merged to `main`, CI green.
2. Runs on the **local** profile via `make` / documented command.
3. Inputs and outputs conform to `contracts/` (validated in tests).
4. Component README updated (how to run, config keys, metrics).
5. Task status updated in your `team/0X-*.md` file (section "Task board").
