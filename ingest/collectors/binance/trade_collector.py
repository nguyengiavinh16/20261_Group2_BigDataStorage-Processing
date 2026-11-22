import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path


import websockets


SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt"]
WS_URL = "wss://stream.binance.com:9443/stream"
MAX_TRADES = 1000
PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "binance"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "trades.jsonl"



def build_stream_url(symbols):
    streams = [f"{symbol}@trade" for symbol in symbols]
    return f"{WS_URL}?streams={'/'.join(streams)}"



async def collect_trades():
    url = build_stream_url(SYMBOLS)
    trade_count = 0

    print("=" * 60)
    print("CRYPTOGUARD - BINANCE TRADE COLLECTOR")
    print("=" * 60)
    print(f"Symbols: {SYMBOLS}")
    print(f"Target trades: {MAX_TRADES}")
    print(f"Output file: {OUTPUT_FILE}\n")
    print("Connecting to Binance WebSocket...")

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
            print("Connected successfully!\nWaiting for trade events...\n")
            with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
                while trade_count < MAX_TRADES:
                    message = await websocket.recv()
                    received_at = datetime.now(timezone.utc).isoformat()
                    data = json.loads(message)
                    trade = data["data"]

                    raw_record = {
                        "source": "binance",
                        "ingest_ts": received_at,
                        "raw_message": data,
                    }
                    file.write(json.dumps(raw_record) + "\n")
                    file.flush()
                    trade_count += 1

                    print(
                        f"[{trade_count:04d}] {trade.get('s')} | "
                        f"Price: {trade.get('p')} | Qty: {trade.get('q')} | "
                        f"Trade ID: {trade.get('t')}"
                    )

            print(f"\nCollection completed! Total trades: {trade_count}")
            print(f"Saved to: {OUTPUT_FILE}")

    except websockets.exceptions.ConnectionClosed as exc:
        print(f"WebSocket connection closed: {exc}")
    except Exception as exc:
        print(f"Unexpected error: {exc}")


if __name__ == "__main__":
    asyncio.run(collect_trades())
