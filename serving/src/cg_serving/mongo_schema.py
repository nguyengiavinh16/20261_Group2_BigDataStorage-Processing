"""MongoDB collections and indexes (MLS-03). Each index exists for a specific API access pattern.

Apply with `python -m cg_serving.mongo_schema` (idempotent).
"""

# collection -> list of (keys, options, access pattern)
INDEXES: dict[str, list[tuple[list[tuple[str, int]], dict, str]]] = {
    "alerts": [  # Alert v1 documents, _id = alert_id
        ([("detected_at_ts_us", -1)], {}, "GET /alerts latest feed"),
        ([("symbol", 1), ("detected_at_ts_us", -1)], {}, "GET /alerts?symbol="),
        ([("case_id", 1), ("window_start_ts_us", 1)], {}, "GET /cases/{id} alerts of a case"),
        ([("alert_type", 1), ("risk_score", -1)], {}, "GET /alerts?type=&min_risk="),
    ],
    "alerts_replay": [  # replay.alerts.v1, separate collection so the live feed never shows replays
        ([("replay_run_id", 1), ("window_start_ts_us", 1)], {}, "evaluation by run"),
    ],
    "cases": [  # one doc per case_id (upserted by the sink from alerts)
        ([("last_alert_ts_us", -1)], {}, "GET /cases latest"),
        ([("symbol", 1), ("last_alert_ts_us", -1)], {}, "GET /cases?symbol="),
        ([("status", 1), ("max_risk", -1)], {}, "GET /cases?status=open sorted by risk"),
    ],
    "symbol_features_recent": [  # features.symbol_1m.v1, kept 7 days for charts
        (
            [("symbol", 1), ("exchange", 1), ("window_start_ts_us", 1)],
            {"unique": True},
            "GET /symbols/{s}/features",
        ),
        (
            [("computed_at", 1)],
            {"expireAfterSeconds": 7 * 24 * 3600},
            "TTL 7 days (computed_at is a Date)",
        ),
    ],
    "wallet_risk": [  # WalletRisk v1, _id = address (written by batch export, R4)
        ([("risk_score", -1)], {}, "top risky wallets"),
    ],
}


def apply(db) -> None:
    for collection, specs in INDEXES.items():
        for keys, options, _pattern in specs:
            db[collection].create_index(keys, **options)


if __name__ == "__main__":
    from pymongo import MongoClient

    from cryptoguard_common.config import load_config

    cfg = load_config()
    apply(MongoClient(cfg["mongo"]["uri"])[cfg["mongo"]["db"]])
    print("indexes applied")
