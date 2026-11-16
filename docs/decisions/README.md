# Architecture Decision Records

One file per decision: `ADR-XXXX-short-title.md`. Status: Proposed → Accepted → (Superseded by ADR-YYYY).
Write one whenever you: change a contract incompatibly, pick/replace a technology, or change a cross-team convention.

| ADR | Title | Status |
|---|---|---|
| [0001](ADR-0001-local-first-kubernetes.md) | Local-first Kubernetes (kind), then GCP VMs | Accepted |
| [0002](ADR-0002-object-storage.md) | S3-compatible object storage: MinIO locally, GCS in cloud | Accepted |
| [0003](ADR-0003-pyflink-pure-logic.md) | PyFlink for streaming; business logic in pure Python | Accepted |
| [0004](ADR-0004-microsecond-timestamps.md) | Integer epoch-microsecond timestamps in all messages | Accepted |
| [0005](ADR-0005-iceberg-jdbc-catalog.md) | Iceberg tables with a JDBC (Postgres) catalog | Accepted |

Template:

```markdown
# ADR-XXXX: Title
- Status: Proposed | Accepted | Superseded by ADR-YYYY
- Date: YYYY-MM-DD
- Deciders: R?
## Context
## Decision
## Consequences (good, bad, follow-ups)
## Alternatives considered
```
