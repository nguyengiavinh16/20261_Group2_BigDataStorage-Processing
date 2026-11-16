"""Loaders for contracts/reference/*.csv (curated by R4)."""

import csv
from dataclasses import dataclass
from functools import lru_cache

from cryptoguard_common.paths import contracts_dir


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    base: str
    quote: str


@dataclass(frozen=True)
class TokenInfo:
    token_address: str
    token_symbol: str
    decimals: int
    usd_pegged: bool
    tracked: bool


def _rows(name: str) -> list[dict]:
    with open(contracts_dir() / "reference" / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@lru_cache(maxsize=1)
def symbol_map() -> dict[tuple[str, str], SymbolInfo]:
    """(exchange, native_symbol) -> canonical SymbolInfo, active rows only."""
    return {
        (r["exchange"], r["native_symbol"]): SymbolInfo(r["symbol"], r["base"], r["quote"])
        for r in _rows("symbol_map.csv")
        if r["active"].strip().lower() == "true"
    }


@lru_cache(maxsize=1)
def token_registry() -> dict[str, TokenInfo]:
    """lowercase token_address -> TokenInfo (chain=eth)."""
    return {
        r["token_address"].lower(): TokenInfo(
            token_address=r["token_address"].lower(),
            token_symbol=r["token_symbol"],
            decimals=int(r["decimals"]),
            usd_pegged=r["usd_pegged"].strip().lower() == "true",
            tracked=r["tracked"].strip().lower() == "true",
        )
        for r in _rows("token_registry.csv")
        if r["chain"] == "eth"
    }


USD_QUOTES = frozenset({"USD", "USDT", "USDC", "FDUSD"})


def native_symbols(exchange: str, canonical: list[str]) -> list[str]:
    """Exchange-native symbols for a canonical watchlist (e.g. binance + BTC-USDT → BTCUSDT).

    Canonical symbols not listed for that exchange are skipped (e.g. Coinbase has no BTC-USDT pair).
    """
    wanted = set(canonical)
    return sorted(
        native
        for (ex, native), info in symbol_map().items()
        if ex == exchange and info.symbol in wanted
    )
