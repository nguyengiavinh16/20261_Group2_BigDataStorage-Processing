"""Collector framework (ING-02). One subclass per source; this base owns connection lifecycle.

Responsibilities:
- connect, subscribe, keep alive, rotate the connection before the exchange's hard limit (Binance: 24h),
- reconnect with exponential backoff + jitter,
- wrap every data message in a RawEnvelope v1 (unchanged payload) and hand it to a sink (Kafka in prod,
  a list in tests),
- expose Prometheus metrics.

Subclasses implement only the source-specific parts (URL, subscribe messages, how to recognise data
messages and extract the native symbol, heartbeat).
"""

import asyncio
import json
import random
import socket
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable

from prometheus_client import Counter, Gauge

from cryptoguard_common.log import get_logger
from cryptoguard_common.timeutil import now_us

MESSAGES = Counter("cg_collector_messages_total", "Data messages received", ["source"])
NON_DATA = Counter("cg_collector_control_messages_total", "Acks/heartbeats/other", ["source"])
RECONNECTS = Counter("cg_collector_reconnects_total", "Reconnects", ["source", "reason"])
LAST_MESSAGE = Gauge(
    "cg_collector_last_message_ts_seconds", "Wall clock of last data message", ["source"]
)

Sink = Callable[[str, str | None, dict], None]  # (topic, key, envelope) -> None


def make_envelope(
    *,
    source: str,
    channel: str,
    native_symbol: str | None,
    payload: dict | str,
    collector_id: str,
    conn_id: str,
    seq: int,
    ingest_ts_us: int | None = None,
) -> dict:
    """Build a RawEnvelope v1. Pure function (unit-tested against the schema)."""
    return {
        "schema_version": "1.0",
        "source": source,
        "channel": channel,
        "native_symbol": native_symbol,
        "ingest_ts_us": ingest_ts_us if ingest_ts_us is not None else now_us(),
        "collector_id": collector_id,
        "conn_id": conn_id,
        "seq": seq,
        "replay_run_id": None,
        "payload": payload,
    }


class BaseCollector(ABC):
    source: str = ""
    channel: str = "trade"

    def __init__(self, cfg: dict, native_symbols: list[str], sink: Sink, topic: str):
        self.cfg = cfg
        self.native_symbols = native_symbols
        self.sink = sink
        self.topic = topic
        self.collector_id = socket.gethostname()
        self.log = get_logger(f"collector.{self.source}")
        self.max_connection_seconds = float(cfg.get("max_connection_hours", 23)) * 3600

    # ---- source-specific -------------------------------------------------
    @abstractmethod
    def url(self) -> str: ...

    def subscribe_messages(self) -> list[dict]:
        """Messages sent right after connecting (empty if subscription is in the URL)."""
        return []

    @abstractmethod
    def is_data_message(self, msg: dict) -> bool: ...

    @abstractmethod
    def native_symbol_of(self, msg: dict) -> str | None: ...

    async def heartbeat(self, ws) -> None:
        """Override when the source needs app-level pings (e.g. OKX expects 'ping' text < 30s)."""
        return None

    # ---- generic -----------------------------------------------------------
    def handle_raw(self, raw: str, conn_id: str, seq: int) -> bool:
        """Parse one frame, publish if it is data. Returns True if published. Pure apart from the sink."""
        try:
            msg: dict | str = json.loads(raw)
        except json.JSONDecodeError:
            msg = raw  # keep it: the normalizer will DLQ it with PARSE_ERROR
        if isinstance(msg, dict) and not self.is_data_message(msg):
            NON_DATA.labels(self.source).inc()
            return False
        native = self.native_symbol_of(msg) if isinstance(msg, dict) else None
        envelope = make_envelope(
            source=self.source,
            channel=self.channel,
            native_symbol=native,
            payload=msg,
            collector_id=self.collector_id,
            conn_id=conn_id,
            seq=seq,
        )
        self.sink(self.topic, native, envelope)
        MESSAGES.labels(self.source).inc()
        LAST_MESSAGE.labels(self.source).set(envelope["ingest_ts_us"] / 1e6)
        return True

    async def run_forever(self) -> None:
        import websockets  # imported lazily so unit tests don't need it

        backoff = 1.0
        while True:
            conn_id = f"{self.source[:2]}-{uuid.uuid4().hex[:8]}"
            try:
                async with websockets.connect(self.url(), ping_interval=20, max_size=2**22) as ws:
                    for sub in self.subscribe_messages():
                        await ws.send(json.dumps(sub))
                    self.log.info(f"connected conn_id={conn_id} symbols={len(self.native_symbols)}")
                    backoff = 1.0
                    hb = asyncio.create_task(self.heartbeat(ws))
                    try:
                        await asyncio.wait_for(
                            self._consume(ws, conn_id), timeout=self.max_connection_seconds
                        )
                    except asyncio.TimeoutError:
                        RECONNECTS.labels(self.source, "rotation").inc()
                        self.log.info(f"rotating connection conn_id={conn_id}")
                    finally:
                        hb.cancel()
            except Exception as exc:  # noqa: BLE001 - any network error → reconnect
                RECONNECTS.labels(self.source, type(exc).__name__).inc()
                self.log.warning(f"connection error {exc!r}; retry in {backoff:.1f}s")
                await asyncio.sleep(backoff + random.uniform(0, backoff / 2))
                backoff = min(backoff * 2, 60.0)

    async def _consume(self, ws, conn_id: str) -> None:
        seq = 0
        async for raw in ws:
            if self.handle_raw(
                raw if isinstance(raw, str) else raw.decode("utf-8", "replace"), conn_id, seq
            ):
                seq += 1
