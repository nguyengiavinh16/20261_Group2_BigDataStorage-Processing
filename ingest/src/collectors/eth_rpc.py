"""Ethereum ERC-20 Transfer logs via public RPC WebSocket (ING-10, phase P3). Not implemented yet.

Plan:
- eth_subscribe "logs" with filter {address: [tracked token contracts], topics: [TRANSFER_TOPIC]}
- each notification → RawEnvelope(source="eth_rpc", channel="transfer_log", payload=<log object>)
- logs carry `removed: true` on reorg → keep them; the normalizer emits status="retracted"
- confirmations are computed by the normalizer using newHeads (channel="new_head") → also subscribe newHeads
- public endpoints rate-limit: keep the filter narrow (config ethereum.tracked_tokens)
"""

from collectors.base import BaseCollector

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class EthRpcCollector(BaseCollector):
    source = "eth_rpc"
    channel = "transfer_log"

    def url(self) -> str:
        return self.cfg["ws_url"]

    def is_data_message(self, msg: dict) -> bool:
        raise NotImplementedError("ING-10")

    def native_symbol_of(self, msg: dict) -> str | None:
        return None
