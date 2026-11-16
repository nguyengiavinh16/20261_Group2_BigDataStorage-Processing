# ingest/: collectors, replay, archiver (owner: R2)

| Module | Task | Status | Output |
|---|---|---|---|
| `src/collectors/base.py` | ING-02 | skeleton implemented, needs live test | RawEnvelope v1 → `raw.trades.<source>.v1` |
| `src/collectors/binance.py` | ING-03 | implemented (unit-tested), needs 1h live run | same |
| `src/collectors/okx.py` | ING-04 | implemented, VERIFY channel + ping | same |
| `src/collectors/coinbase.py`, `kraken.py` | ING-12 (P4) | stub | same |
| `src/collectors/eth_rpc.py` | ING-10 (P3) | stub | `raw.eth.transfers.v1` |
| `src/replay/binance_vision.py` | ING-07 | parser done, loop TODO | `replay.canonical.trades.v1` |
| `src/archiver/` | ING-08 | design note | Bronze `bronze/kafka/<topic>/dt=.../` |

## Run on the host (fast dev loop)

```bash
make up                                   # Kafka reachable at localhost:32100
source .venv/bin/activate && pip install -r ingest/requirements.txt
PYTHONPATH=ingest/src python -m collectors.main --source binance
# watch messages:
kubectl -n cg-kafka exec -it cg-kafka-dual-0 -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw.trades.binance.v1 --max-messages 5
```

## Run in the cluster

```bash
make image-ingest
kubectl apply -f ingest/k8s/collector.yaml
kubectl -n cg-ingest logs deploy/collector-binance -f
```

## Metrics (port 9108)

`cg_collector_messages_total{source}`, `cg_collector_control_messages_total{source}`, `cg_collector_reconnects_total{source,reason}`, `cg_collector_last_message_ts_seconds{source}`.
