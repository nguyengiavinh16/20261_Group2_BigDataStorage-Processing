"""Binance spot trade stream (ING-03). Docs: github.com/binance/binance-spot-api-docs (web-socket-streams.md).

Combined stream URL: <ws_url>?streams=btcusdt@trade/ethusdt@trade ; frames look like
{"stream": "btcusdt@trade", "data": {"e": "trade", "s": "BTCUSDT", ...}}.
Binance closes connections after 24h → BaseCollector rotates at max_connection_hours.
Server pings are answered automatically by the websockets library.
"""

from collectors.base import BaseCollector


class BinanceCollector(BaseCollector):
    source = "binance"
    channel = "trade"

    def url(self) -> str:
        streams = "/".join(f"{s.lower()}@trade" for s in self.native_symbols)
        return f"{self.cfg['ws_url']}?streams={streams}"

    def is_data_message(self, msg: dict) -> bool:
        return "data" in msg and isinstance(msg["data"], dict) and msg["data"].get("e") == "trade"

    def native_symbol_of(self, msg: dict) -> str | None:
        return msg.get("data", {}).get("s")
