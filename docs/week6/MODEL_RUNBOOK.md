# P3-W6A/B — Model card, reproducible evaluation, independent reload

## Release model contract

Freeze the exact `git_commit`, `silver_snapshot_id`, `source_snapshot_sha256` (mapped to `dataset_sha256` in W6 model card), input feature URI, offline label snapshot, seed, Spark version, feature set/order, threshold, metric evaluation report, score tolerance and artifact URI. Never use a moving `latest` path.

**Feature A** = `[transaction_type, log_amount]`; **A+H** = `[transaction_type, log_amount, dest_prior24_count, dest_prior24_amount_cents]`. A+H historical fields count/total in cents over strictly earlier steps `[step-24,step-1]`; order matters. See `src/ml/lr_w4.py` for actual signature and manifest. Training preprocessing fits **train only**; threshold selected on **validation**; test 379..743 must not be used to tune threshold. Labels are an offline-only sidecar, not event features.

The W4 manifest field `source_snapshot_sha256` can be mapped to W6 `dataset_sha256` only if it refers to precisely the same source object and has been cross-checked. W6 `artifact_sha256` is a hash of an immutable exported artifact/package or checksum recorded by verified HDFS traversal, **not** an invented hash for a directory URI.

## Reproduction workflow

1. Reviewer independently fetches pinned source and model artifact. Confirm checksum and `artifact_status=COMPLETE`.
2. Read `src/ml/lr_w2.py` or `src/ml/lr_w4.py` by actual frozen release; verify preprocessing, feature signature, model_version and threshold.
3. Use a **new Spark application/process**, not a warmed in-memory model; run:

```sh
# inside configured Spark driver, with exact Iceberg/Kafka/hadoop connector versions used in W2/W4
spark-submit scripts/week6/independent_model_reload.py \
  --model-kind A+H \
  --artifact hdfs://namenode:8020/fraud/models/FROZEN \
  --features hdfs://namenode:8020/fraud/features/FROZEN \
  --expected hdfs://namenode:8020/fraud/golden/FROZEN \
  --report reports/evidence/w6-model-reload.json
```

This script checks **all IDs**, duplicate ID absence, score maximum delta and decision equality (default `1e-8`, only use frozen tolerance if previously documented). It fails on missing/extra IDs; no `collect()` of all predictions. Live execution is **pending** on cluster. For A use `--model-kind A` and corresponding frozen stateless model.

4. The model card generator expects a **reviewed normalized W6 manifest** (not raw W4 manifest if fields differ). Missing fields → INCONCLUSIVE. It does not read test labels automatically and will not invent AP/F1:

```powershell
python -m scripts.week6.cli model-card --manifest ..\release\model-manifest-reviewed.json --output ..\release\model-card.md --audit-output ..\release\model-card-audit.json
```

5. For metrics, use W5 locked evaluation on test 379..743 after freezing validation threshold. `AP` means average precision over *all scored test rows*; report precision/recall/F1 at frozen threshold, coverage, confusion matrix and fraud amount recall. Every number should have a verified claim→report→source/run link. Include ablation A vs A+H **only on same population and snapshot**.

## Questions for defense

- Why not accuracy? Fraud is rare, so AP/PR and amount recall reveal minority-class tradeoffs.
- Why 24-step warm-up? Features require prior history. Same-step rows cannot see each other.
- What is leakage? Using post-transaction balance, simulator fraud label, future steps, or fitting transform on test.
- Why not conclude real-bank effectiveness? PaySim is synthetic; changed sampling/behavior can harm calibration.
- What guarantees? Deterministic feature contract for tested ordered replay, **not** arbitrary out-of-order streaming, production HA, perfect calibration or exactly-once across all sinks.

## Known limitations

No model performance number is pre-populated. Actual Spark process independent reload has not been run in this generated patch environment. Release artifact and golden scores must be furnished on HDFS by the team.
