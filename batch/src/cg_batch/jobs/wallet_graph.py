"""silver.onchain_transfers + silver.labels → gold.wallet_edges, gold.wallet_risk (BAT-08/09). NOT IMPLEMENTED.

1. wallet_edges: group transfers (status='confirmed', tracked tokens) by (from,to,token) over the window.
2. seeds: wallet labels valid at as_of (sanctioned/scam/phishing/illicit/mixer), weighted by confidence.
3. taint: Pregel/iterative joins reproducing cg_batch.graph.taint.propagate_taint exactly (tests compare both).
4. graph features: PageRank (GraphFrames), fan_in/fan_out, totals, first/last seen.
5. risk_score: documented formula in batch/README.md (start: max(label_conf, taint) blended with features).
6. Export: Redis 'wallet_risk:<address>' (only risk_score ≥ 0.1 to bound memory) and Mongo 'wallet_risk'.
"""


def main() -> None:
    raise NotImplementedError("BAT-08/BAT-09")


if __name__ == "__main__":
    main()
