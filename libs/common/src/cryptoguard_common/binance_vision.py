"""Parser for data.binance.vision spot `trades` CSV files (shared by replay [R2] and batch Bronze→Silver [R4]).

File: data/spot/daily/trades/<SYMBOL>/<SYMBOL>-trades-YYYY-MM-DD.zip, CSV WITHOUT header, columns:
    trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match
`time` is ms in older files and µs for spot data from 2025-01-01 (VERIFY in BAT-01); epoch_to_us handles both.
"""

from cryptoguard_common.reference import USD_QUOTES, symbol_map
from cryptoguard_common.timeutil import epoch_to_us

COLUMNS = ("trade_id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "is_best_match")


def _bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1")


def trade_row_to_canonical(
    row: list[str] | tuple, native_symbol: str, ingest_ts_us: int, replay_run_id: str | None = None
) -> dict:
    """One CSV row → CanonicalTrade v1 dict. Raises KeyError if the symbol is not in symbol_map."""
    info = symbol_map()[("binance", native_symbol)]
    rec = dict(zip(COLUMNS, row, strict=False))
    price, qty = float(rec["price"]), float(rec["qty"])
    quote_qty = float(rec["quote_qty"]) if rec.get("quote_qty") not in (None, "") else price * qty
    trade_id = str(rec["trade_id"]).strip()
    return {
        "schema_version": "1.0",
        "event_id": f"binance:{native_symbol}:{trade_id}",
        "exchange": "binance",
        "symbol": info.symbol,
        "base": info.base,
        "quote": info.quote,
        "native_symbol": native_symbol,
        "trade_id": trade_id,
        "price": price,
        "qty": qty,
        "quote_qty": quote_qty,
        "quote_qty_usd": quote_qty if info.quote in USD_QUOTES else None,
        # buyer is maker → the aggressor (taker) sold
        "taker_side": "sell" if _bool(rec["is_buyer_maker"]) else "buy",
        "event_ts_us": epoch_to_us(rec["time"]),
        "ingest_ts_us": ingest_ts_us,
        "replay_run_id": replay_run_id,
    }
