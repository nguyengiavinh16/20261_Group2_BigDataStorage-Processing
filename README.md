# CryptoGuard – I01 Binance WebSocket Spike

This starter kit connects to Binance's public WebSocket trade streams for five symbols, saves up to 1,000 raw events as JSON Lines, and provides a small script to inspect the saved file.


## Requirements
- Python 3.10+ recommended
- Internet access to Binance WebSocket endpoint

## Setup (Windows PowerShell)
From the project root:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Collect
```powershell
python ingest/collectors/binance/trade_collector.py
```
The collector stops after 1,000 total messages and overwrites `data/raw/binance/trades.jsonl` each run.
## Inspect
```powershell
python ingest/collectors/binance/inspect_raw.py
```
## Output
`data/raw/binance/trades.jsonl`

## Scope
This is only I01 (WebSocket data collection spike). Kafka, Flink, second exchange, reconnection strategy, and canonical normalization are not implemented yet.
