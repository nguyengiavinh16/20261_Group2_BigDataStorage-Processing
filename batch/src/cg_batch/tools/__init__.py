"""One-off command-line tools run from the host (not Spark jobs). Each module documents its usage.

| Module | Task | Network | Writes |
|---|---|---|---|
| pump_candidates | BAT-01 | HEAD requests to data.binance.vision only | contracts/reference/pump_events_candidates.csv |
| source_spike | BAT-01 | Parquet footer range reads on AWS (no full download) | JSON summary to stdout / --out |
"""
