# Roadmap

Durations are suggestions; adjust to the course deadline. **Everything up to P3 runs on laptops (local profile).
Cloud starts in P4.** Task IDs refer to the role files in `team/`.

| Phase | Goal | Exit criteria (demo) | Main tasks |
|---|---|---|---|
| **P0 Kickoff** (~1 wk) | Shared understanding, data spikes, contracts frozen v1 | Everyone ran `make dev-install test`; each source fetched once; list of evaluable pump events | INF-01/02, ING-01, DET-01, BAT-01, MLS-01/02 |
| **P1 Local foundation** (~1–2 wk) | kind cluster + Kafka + MinIO + Postgres; first live data | `make up` works on all laptops; Binance + OKX raw messages in Kafka; Bronze has binance.vision days | INF-03…09, ING-02/03/04, BAT-02/03, MLS-03 |
| **P2 Local E2E MVP** (~2 wk) | Live and replay path end-to-end | A replayed real pump appears as an alert on the dashboard; Silver/Gold trades & features exist | ING-05…09, DET-02…07, BAT-04…06, MLS-04…06, INF-10…12 |
| **P3 On-chain & graph** (~2 wk) | Wallet risk and on-chain stream | `GET /wallets/{addr}/risk` returns path to an OFAC/darklist wallet; detector adds RISKY_INFLOW reason | ING-10/11, DET-08, BAT-07…10, MLS-07 |
| **P4 Cloud + scale + advanced** (~2–3 wk) | GCP VMs, bigger data, more exchanges, ML, evaluation | Same system on GCP with 3–6 months of data; evaluation report O6; chaos demo | INF-20…26, ING-12/13, DET-09…13, BAT-11…14, MLS-08…10 |
| **P5 Finish** (~1 wk) | Report, slides, rehearsed demo | Recorded backup demo video; report merged in docs/ | all |

## Dependencies that can block others (watch these first)

1. **INF-03/04** (cluster + Kafka) → everyone's integration work. Mitigation: develop pure logic + unit tests first.
2. **ING-05/06** (canonical.trades) → detector. Mitigation: `make seed-samples`, binance.vision files via replay.
3. **BAT-01 + MLS-01** (evaluable pump events) → the whole evaluation story. Do it in P0.
4. **BAT-05** (baselines in Redis) → volume_z in the detector. Mitigation: detector falls back to an in-stream EMA baseline (DET-03).
