from __future__ import annotations

import re

# Centralized asset exclusion rules used by both local analysis and Parquet collection.
# The universe must contain independently tradable crypto assets, not stablecoins or
# wrapped/tokenized representations of another underlying asset.

# Stablecoins whose ticker does not reliably contain USD/EUR, plus legacy/algorithmic
# stable assets that can still appear in market-cap feeds.
STABLECOIN_SYMBOLS = {
    "DAI", "FRAX", "GHO", "MIM", "DOLA", "FEI", "RAI", "FPI",
    "UST", "USTC", "EUSD", "USK", "VAI",
}

# Wrapped, liquid-staking, tokenized commodity/treasury and duplicate-underlying assets.
# These are excluded separately from stablecoins because the market-scan mandate is
# non-stable/non-wrapped and should not spend a top-100 slot on a proxy for another asset.
WRAPPED_OR_TOKENIZED_SYMBOLS = {
    "WBTC", "WETH", "STETH", "WSTETH", "WEETH", "WBETH", "CBETH", "RETH", "EZETH",
    "WBNB", "JITOSOL", "MSOL", "BNSOL", "SOLVBTC", "LBTC", "CBBTC", "TBTC", "RSETH",
    "SUSDE", "SUSDS", "BUIDL", "USYC", "USDTB",
    "PAXG", "XAUT",
}

# Covers current and future fiat-pegged tickers such as USDT, USDC, USDE, USDS,
# FDUSD, PYUSD, RLUSD, BFUSD, USD1, crvUSD, EURC, EURI, AEUR, etc. The pattern is
# deliberately anchored so governance tokens such as USUAL are not excluded.
_FIAT_TICKER_RE = re.compile(r"^(?:USD[A-Z0-9]*|[A-Z0-9]*USD|EUR[A-Z0-9]*|[A-Z0-9]*EUR)$")

_STABLE_NAME_MARKERS = (
    "stablecoin",
    "stable coin",
    "usd stable",
    "dollar stable",
    "euro stable",
    "fiat-backed",
    "fiat backed",
)


def is_stablecoin(symbol: str, name: str = "", provider_id: str | None = None) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    if sym in STABLECOIN_SYMBOLS or _FIAT_TICKER_RE.fullmatch(sym):
        return True

    text = " ".join((str(name or ""), str(provider_id or ""))).lower()
    return any(marker in text for marker in _STABLE_NAME_MARKERS)


def is_wrapped_or_tokenized(symbol: str) -> bool:
    return str(symbol or "").upper().strip() in WRAPPED_OR_TOKENIZED_SYMBOLS


def is_excluded_asset(symbol: str, name: str = "", provider_id: str | None = None) -> bool:
    return is_stablecoin(symbol, name, provider_id) or is_wrapped_or_tokenized(symbol)
