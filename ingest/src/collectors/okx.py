"""OKX public trades channel (ING-04). Docs: okx.com/docs-v5 (WebSocket public channel: trades).

Subscribe: {"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}, ...]}
Data:      {"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{...}, ...]}
VERIFY (ING-01): OKX expects the client to send the text 'ping' if no message for < 30s; also check whether
"trades" aggregates fills (vs "trades-all") and pick the channel that gives one record per trade.
"""

import asyncio

from collectors.base import BaseCollector


class OkxCollector(BaseCollector):
    source = "okx"
    channel = "trade"

    def url(self) -> str:
        return self.cfg["ws_url"]

    def subscribe_messages(self) -> list[dict]:
        return [
            {
                "op": "subscribe",
                "args": [{"channel": "trades", "instId": s} for s in self.native_symbols],
            }
        ]

    def is_data_message(self, msg: dict) -> bool:
        return "data" in msg and msg.get("arg", {}).get("channel") == "trades"

    def native_symbol_of(self, msg: dict) -> str | None:
        return msg.get("arg", {}).get("instId")

    async def heartbeat(self, ws) -> None:
        while True:
            await asyncio.sleep(20)
            await ws.send("ping")
