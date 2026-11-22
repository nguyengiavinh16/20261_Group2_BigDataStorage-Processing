# I01 – Binance Raw Data Mapping


## Goal
Collect live Binance trade events using WebSocket and preserve the original Combined Stream message in JSONL.

## Symbols
- BTCUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- XRPUSDT

## Raw trade fields
| Field | Meaning |
|---|---|
| `e` | Event type |
| `E` | Event time (milliseconds) |
| `s` | Symbol |
| `t` | Trade ID |
| `p` | Price (string) |
| `q` | Quantity (string) |
| `T` | Trade time (milliseconds) |
| `m` | Buyer is maker flag |

## Collector-added fields
| Field | Meaning |
|---|---|
| `source` | Exchange/source name (`binance`) |
| `ingest_ts` | UTC timestamp when the collector received the message |
| `raw_message` | Original combined-stream JSON message |

## Notes
- This spike does not normalize or cast values. Raw values are preserved.
- The collector writes to `data/raw/binance/trades.jsonl` and overwrites that file on each run.
- Event time (`T`) and collector ingest time (`ingest_ts`) are different measurements and should both be retained.
