"""Kraken WebSocket v2 'trade' channel (ING-12, phase P4). Not implemented yet.

URL: wss://ws.kraken.com/v2
Subscribe: {"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"]}}
Data: {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":...,"qty":...,
       "ord_type":"market","trade_id":...,"timestamp":"...Z"}]}. 'side' = taker side. Kraken uses XBT in some
APIs: symbol_map handles aliases. VERIFY in ING-01.
"""

from collectors.base import BaseCollector


class KrakenCollector(BaseCollector):
    source = "kraken"

    def url(self) -> str:
        return self.cfg["ws_url"]

    def subscribe_messages(self) -> list[dict]:
        return [
            {"method": "subscribe", "params": {"channel": "trade", "symbol": self.native_symbols}}
        ]

    def is_data_message(self, msg: dict) -> bool:
        raise NotImplementedError("ING-12")

    def native_symbol_of(self, msg: dict) -> str | None:
        raise NotImplementedError("ING-12")
