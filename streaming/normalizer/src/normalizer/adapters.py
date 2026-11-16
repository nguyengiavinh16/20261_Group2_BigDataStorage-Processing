"""Per-exchange adapters: RawEnvelope payload → list of CanonicalTrade v1 dicts (ING-05).

Each adapter is a pure function `(payload: dict, ingest_ts_us: int) -> list[dict]` and raises AdapterError
with a DLQ error_type when the message cannot be normalized. Side semantics MUST be verified against official
docs (ING-01) and covered by tests using real captured samples.
"""

from collections.abc import Callable

from cryptoguard_common.reference import USD_QUOTES, symbol_map
from cryptoguard_common.timeutil import epoch_to_us, iso_to_us


class AdapterError(Exception):
    def __init__(self, error_type: str, message: str):
        super().__init__(message)
        self.error_type = error_type


def _trade(
    exchange: str,
    native_symbol: str,
    trade_id,
    price,
    qty,
    taker_side: str,
    event_ts_us: int,
    ingest_ts_us: int,
    replay_run_id: str | None,
) -> dict:
    info = symbol_map().get((exchange, native_symbol))
    if info is None:
        raise AdapterError(
            "UNKNOWN_SYMBOL", f"{exchange} native symbol {native_symbol} not in symbol_map"
        )
    try:
        price_f, qty_f = float(price), float(qty)
    except (TypeError, ValueError) as exc:
        raise AdapterError("INVALID_VALUE", f"price/qty not numeric: {price!r}/{qty!r}") from exc
    if price_f <= 0 or qty_f <= 0:
        raise AdapterError("INVALID_VALUE", f"non-positive price/qty: {price_f}/{qty_f}")
    quote_qty = price_f * qty_f
    return {
        "schema_version": "1.0",
        "event_id": f"{exchange}:{native_symbol}:{trade_id}",
        "exchange": exchange,
        "symbol": info.symbol,
        "base": info.base,
        "quote": info.quote,
        "native_symbol": native_symbol,
        "trade_id": str(trade_id),
        "price": price_f,
        "qty": qty_f,
        "quote_qty": quote_qty,
        "quote_qty_usd": quote_qty if info.quote in USD_QUOTES else None,
        "taker_side": taker_side,
        "event_ts_us": event_ts_us,
        "ingest_ts_us": ingest_ts_us,
        "replay_run_id": replay_run_id,
    }


def binance(payload: dict, ingest_ts_us: int, replay_run_id: str | None = None) -> list[dict]:
    data = payload.get("data", payload)
    if data.get("e") != "trade":
        raise AdapterError("SCHEMA_VIOLATION", f"unexpected binance event type {data.get('e')!r}")
    try:
        # m = "is the buyer the market maker?" → true means the taker SOLD
        side = "sell" if data["m"] else "buy"
        return [
            _trade(
                "binance",
                data["s"],
                data["t"],
                data["p"],
                data["q"],
                side,
                epoch_to_us(data["T"]),
                ingest_ts_us,
                replay_run_id,
            )
        ]
    except KeyError as exc:
        raise AdapterError("SCHEMA_VIOLATION", f"binance trade missing field {exc}") from exc


def okx(payload: dict, ingest_ts_us: int, replay_run_id: str | None = None) -> list[dict]:
    try:
        return [
            # VERIFY (ING-01): OKX trades 'side' = taker side
            _trade(
                "okx",
                d["instId"],
                d["tradeId"],
                d["px"],
                d["sz"],
                d["side"],
                epoch_to_us(d["ts"]),
                ingest_ts_us,
                replay_run_id,
            )
            for d in payload["data"]
        ]
    except KeyError as exc:
        raise AdapterError("SCHEMA_VIOLATION", f"okx trade missing field {exc}") from exc


def coinbase(payload: dict, ingest_ts_us: int, replay_run_id: str | None = None) -> list[dict]:
    """ING-12. 'side' in Coinbase matches is the MAKER side → taker_side is the opposite (VERIFY)."""
    if payload.get("type") not in ("match", "last_match"):
        raise AdapterError("SCHEMA_VIOLATION", f"unexpected coinbase type {payload.get('type')!r}")
    maker_side = payload["side"]
    taker = {"buy": "sell", "sell": "buy"}.get(maker_side, "unknown")
    return [
        _trade(
            "coinbase",
            payload["product_id"],
            payload["trade_id"],
            payload["price"],
            payload["size"],
            taker,
            iso_to_us(payload["time"]),
            ingest_ts_us,
            replay_run_id,
        )
    ]


def kraken(payload: dict, ingest_ts_us: int, replay_run_id: str | None = None) -> list[dict]:
    """ING-12. Kraken v2 trade: 'side' = taker side (VERIFY)."""
    if payload.get("channel") != "trade":
        raise AdapterError(
            "SCHEMA_VIOLATION", f"unexpected kraken channel {payload.get('channel')!r}"
        )
    return [
        _trade(
            "kraken",
            d["symbol"],
            d["trade_id"],
            d["price"],
            d["qty"],
            d["side"],
            iso_to_us(d["timestamp"]),
            ingest_ts_us,
            replay_run_id,
        )
        for d in payload["data"]
    ]


ADAPTERS: dict[str, Callable[..., list[dict]]] = {
    "binance": binance,
    "okx": okx,
    "coinbase": coinbase,
    "kraken": kraken,
}
