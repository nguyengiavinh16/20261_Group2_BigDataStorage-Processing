# ml/: training & evaluation (owner: R5)

```
ml/
├── src/cg_ml/evaluation/metrics.py   # evaluation protocol (recall, precision, delay, false alerts/day) ✅
├── src/cg_ml/training/               # MLS-08 (P4)
├── notebooks/                        # exploration only; strip outputs (nbstripout) before commit
└── tests/
```

## Evaluation protocol (MLS-06): what the final report (O6) is based on

1. Ground truth: `cg.gold.pump_cases` with `has_trade_data = true` (built by R4 from the pump dataset; P0 selects events).
2. For each evaluation run: replay the event days **plus** an equal number of normal days into
   `replay.canonical.trades.v1` (ING-07, one `replay_run_id`), run the detector in replay mode (DET-07).
3. Collect `replay.alerts.v1` (Mongo `alerts_replay` or `cg.gold.alerts_history` filtered by run id).
4. Compute `cg_ml.evaluation.metrics.evaluate` + `precision_at_k`; store results JSON in MLflow under the run id.
5. Report per rule/model version: recall, precision, precision@10, median detection delay, false alerts/day.

Matching window defaults: 10 min before to 30 min after the announced pump time (configurable, report the values used).
