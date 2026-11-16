"""Publish contracts/samples/*.valid.jsonl to their topics so consumers can be developed before producers exist.

    python infra/scripts/seed_samples.py [--only alert] [--repeat 10]
Requires the local cluster (`make up`) and `confluent-kafka` (in requirements-dev.txt).
"""

import argparse
import json

from confluent_kafka import Producer

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import topic
from cryptoguard_common.paths import contracts_dir

# sample stem -> (topic or callable(record) -> topic)
TARGETS = {
    "raw_envelope": lambda r: (
        "raw.eth.transfers.v1" if r["source"] == "eth_rpc" else f"raw.trades.{r['source']}.v1"
    ),
    "canonical_trade": lambda r: "canonical.trades.v1",
    "onchain_transfer": lambda r: "canonical.onchain_transfers.v1",
    "symbol_features_1m": lambda r: "features.symbol_1m.v1",
    "alert": lambda r: "alerts.v1",
    "dlq_record": lambda r: "dlq.normalizer.v1",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=sorted(TARGETS))
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()

    cfg = load_config()
    producer = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})
    sent = 0
    for stem, target in TARGETS.items():
        if args.only and stem != args.only:
            continue
        path = contracts_dir() / "samples" / f"{stem}.valid.jsonl"
        records = [
            json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()
        ]
        for _ in range(args.repeat):
            for rec in records:
                name = target(rec)
                key_field = topic(name)["key"]
                key = rec.get(key_field)
                producer.produce(name, key=None if key is None else str(key), value=json.dumps(rec))
                sent += 1
    producer.flush(10)
    print(f"sent {sent} messages")


if __name__ == "__main__":
    main()
