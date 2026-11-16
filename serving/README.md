# serving/: sink, API, dashboard (owner: R5)

```
serving/
├── src/cg_serving/
│   ├── mongo_schema.py   # collections + indexes, each mapped to an access pattern  ✅
│   ├── sink.py           # Kafka → MongoDB (MLS-04)                                 ⏳
│   └── api.py            # FastAPI routes (shapes fixed, handlers TODO)             ⏳
├── dashboard/            # UI (MLS-05). Reuse ideas from the W1 mock dashboard (archived in _backup/)
├── Dockerfile
└── tests/
```

## Develop without waiting for the detector

```bash
make up && make up-serving
make seed-samples                       # puts sample alerts/features into Kafka
PYTHONPATH=serving/src python -m cg_serving.mongo_schema
PYTHONPATH=serving/src uvicorn cg_serving.api:app --reload --port 8000
```

## Outputs owned here (see brief §6)

O1 alert feed · O2 case page · O3 wallet risk API · O4 graph view (P4) · O5 market dashboard (Superset/Grafana) · O6 evaluation report (ml/).
