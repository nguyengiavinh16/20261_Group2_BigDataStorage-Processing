# Instructions for AI coding agents

You are helping one member of a 5-person student team build **CryptoGuard**, a Big Data project (crypto market-manipulation and on-chain laundering detection on Kubernetes). Follow these rules strictly.

## Before writing any code

1. Read `team/00-shared-context.md` completely.
2. Ask the human which role they are (1–5) if they did not say so, then read that role file in `team/` completely:
   - `team/01-platform-infra.md`
   - `team/02-ingestion-normalization.md`
   - `team/03-realtime-detection.md`
   - `team/04-batch-lakehouse-graph.md`
   - `team/05-ml-serving-evaluation.md`
3. Read `docs/data-contracts.md` and the schemas in `contracts/schemas/` that the task consumes or produces.
4. Identify the task ID (e.g. `ING-03`) and its acceptance criteria in the role file. If the request does not match a task, say so and ask before proceeding.

## Hard rules

- **Stay in scope.** Only edit files in the directories owned by the role (listed in the role file). Touching other areas: propose the change in your answer instead of editing, unless the human explicitly asks.
- **Never change `contracts/`** (schemas, topic registry, reference data structure) unless the human explicitly asks. If a contract seems wrong, explain why and suggest an additive change. Breaking changes need an ADR.
- **Local first.** Current phase targets the local kind cluster and the `local` config profile. Do not create cloud resources, do not add cloud-only dependencies, do not hard-code GCP/AWS endpoints. Storage access goes through the S3-compatible abstraction configured in `config/` (MinIO locally, GCS later).
- **Respect the local data budget**: small symbol lists and short date ranges from `config/local.yaml`. Never write code that downloads full history by default.
- **Pure logic, thin engines.** Put parsing/feature/rule/metric logic in pure Python functions with unit tests; Flink/Spark/Airflow code only wires I/O.
- **Contracts are validated in tests.** Every producer test must validate output against the JSON Schema (`cryptoguard_common.contracts.validate`).
- **Time:** integer epoch microseconds UTC in messages (`*_ts_us`). Use `cryptoguard_common.timeutil`.
- **Config, not constants:** thresholds, hosts, topics, symbols come from `config/*.yaml` via `cryptoguard_common.config.load_config()`; topic names from `contracts/kafka-topics.yaml`.
- **No secrets** in code or commits. Use env vars and `.env` (git-ignored).
- **Python 3.11**, `ruff` clean, `pytest` green. Run `make lint test` before declaring done.
- **Do not invent facts about external APIs.** When unsure about an exchange/API field meaning, leave a `# VERIFY:` comment citing what must be checked in the official docs, and tell the human.
- Commit messages follow `CONTRIBUTING.md` (Conventional Commits + task ID). Do not commit or push unless the human asks.

## When done

Summarize: what changed, how it was tested, which acceptance criteria are met / not met, and any contract or cross-team impact the human must communicate to teammates.
