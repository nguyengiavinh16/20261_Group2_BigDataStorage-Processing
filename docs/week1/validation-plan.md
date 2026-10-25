# Validation plan — Week 1

| Check | Expected policy | Owner |
|---|---|---|
| Required field absent / empty ID | Reject and log clear error; do not invent missing ID/time | Đức Anh |
| Wrong field type / invalid timestamp | Reject or quarantine | Đức Anh |
| Negative amount | Flag/reject in contract fixture, but inspect actual data and document domain policy before ETL filtering | Thái + Đạt |
| Unknown transaction type | Count and quarantine or accept under documented unknown-category policy; profile values first | Đạt + Công Anh |
| Malformed JSON | Reject message, log error, continue if consumer supports per-message error handling | Công Anh |
| Duplicate ID | Detect; use deterministic ID and idempotent upsert; do not silently count as a new unique transaction | Công Anh |
| Duplicate identical source rows | Preserve source-row identity or define explicit deduplication policy; do not deduplicate on values alone without review | Đạt + Thái |
| Label leakage | `isFraud`/`isFlaggedFraud` absent from scoring event/features | Thái |
| Unknown schema version | Reject/quarantine rather than silently parse as v1 | Đức Anh |

## Source CSV checks

1. Exact header, encoding, file checksum and source terms.
2. Row count and null count per column.
3. Duplicate rows and duplicate candidate IDs.
4. Numeric ranges and transaction-type frequencies.
5. Count `amount < 0`; do not assume source semantics without inspection.
6. Check balance fields but do not mark all discrepancies invalid without justification.
7. Verify `step` unit and range.
8. Keep labels separate from inference features.
9. Save actual findings under `reports/experiments/`.

## Acceptance

Valid fixtures pass JSON parsing/schema validation. Missing ID, negative amount and malformed JSON fail for the expected reason. Validation output identifies file/record and error. The actual source mapping remains unapproved until the real dataset is inspected.
