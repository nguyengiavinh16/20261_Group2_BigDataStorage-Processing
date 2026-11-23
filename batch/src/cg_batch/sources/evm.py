"""EVM address helpers shared by the OFAC, darklist and AWS token_transfers parsers."""

import re

_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
_PADDED_WORD = re.compile(r"^0x0{24}[0-9a-f]{40}$")


def normalize_address(value: str | None) -> str | None:
    """Lowercase 20-byte `0x…` address, or None if `value` is not an EVM address.

    Also accepts 32-byte left-padded words (66 chars): AWS Public Blockchain `token_transfers` stores
    `from_address`/`to_address` that way (seen in date=2026-11-20, BAT-01). A 32-byte word whose
    high 12 bytes are not zero is not an address and returns None.
    """
    if value is None:
        return None
    v = str(value).strip().lower()
    if _ADDRESS.match(v):
        return v
    if _PADDED_WORD.match(v):
        return "0x" + v[-40:]
    return None
