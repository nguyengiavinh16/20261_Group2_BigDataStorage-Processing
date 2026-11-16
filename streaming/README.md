# streaming/: Flink jobs (normalizer: R2, detector: R3)

```
streaming/
├── Dockerfile                  # one PyFlink image (cg-streaming) for both jobs
├── requirements.txt
├── k8s/                        # FlinkDeployment manifests
├── normalizer/                 # R2: raw.* → canonical.* (+ DLQ, dedup, watermarks)
│   ├── src/normalizer/adapters.py    # pure, per exchange      (ING-05) ✅ binance, okx · ⏳ coinbase, kraken
│   ├── src/normalizer/normalize.py   # envelope → trades | DLQ (ING-05) ✅
│   └── src/normalizer/job.py         # Flink wiring            (ING-06) ⏳
└── detector/                   # R3: canonical.trades → features + alerts
    ├── src/detector/rules.py         # pump-v1, dump-v1        (DET-05) ✅ starting version
    ├── src/detector/cases.py         # case grouping + Alert   (DET-06) ✅ starting version
    └── src/detector/job.py           # Flink wiring            (DET-04) ⏳
```

Shared feature definitions live in `libs/common/src/cryptoguard_common/features.py` (used by batch too).

## Engine decision

PyFlink (DataStream API) is the default (ADR-0003). All business logic is in pure Python modules, so if PyFlink
blocks the team, the same functions can be wired in Spark Structured Streaming without touching contracts.

## Dev loop

1. Unit-test pure logic: `pytest streaming/` (no cluster needed).
2. Run the job on the host against the local Kafka (`localhost:32100`) with PyFlink's local mini-cluster
   (needs Java 11/17 + `pip install -r streaming/requirements.txt` + the Kafka connector jar via `CG_FLINK_JARS`).
3. Deploy: `make image-streaming && kubectl apply -f streaming/k8s/normalizer.flinkdeployment.yaml`.
4. Inspect: `kubectl -n cg-stream get flinkdeployment`, Flink UI via `kubectl -n cg-stream port-forward svc/cg-normalizer-rest 8081`.

## Replay mode (detector)

`--mode replay` reads `replay.canonical.trades.v1` and writes `replay.features.symbol_1m.v1` / `replay.alerts.v1`.
Run it as a separate FlinkDeployment (`cg-detector-replay`) so live and replay state never mix.
