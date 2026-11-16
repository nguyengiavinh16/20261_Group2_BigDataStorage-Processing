"""Coinbase Exchange 'matches' channel (ING-12, phase P4). Not implemented yet.

URL: wss://ws-feed.exchange.coinbase.com
Subscribe: {"type":"subscribe","product_ids":["BTC-USD"],"channels":["matches"]}
Data: {"type":"match", "trade_id":..., "side":"sell", "size":"...", "price":"...", "product_id":"BTC-USD",
       "sequence":..., "time":"2026-...Z"}. NOTE: 'side' is the MAKER side (normalizer inverts it).
Use 'sequence' for gap detection. VERIFY all of this in ING-01.
"""

from collectors.base import BaseCollector


class CoinbaseCollector(BaseCollector):
    source = "coinbase"

    def url(self) -> str:
        return self.cfg["ws_url"]

    def subscribe_messages(self) -> list[dict]:
        return [{"type": "subscribe", "product_ids": self.native_symbols, "channels": ["matches"]}]

    def is_data_message(self, msg: dict) -> bool:
        raise NotImplementedError("ING-12")

    def native_symbol_of(self, msg: dict) -> str | None:
        raise NotImplementedError("ING-12")
