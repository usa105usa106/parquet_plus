from __future__ import annotations

# Coolify Trading Signal Bot v020

import asyncio
import logging
import math
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from statistics import mean, median
from typing import Any
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from asset_filters import is_excluded_asset

log = logging.getLogger(__name__)

BINANCE_SPOT_BASE = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
MEXC_CONTRACT_BASE = os.getenv("MEXC_CONTRACT_BASE_URL", "https://api.mexc.com").rstrip("/")
COINPAPRIKA_BASE = os.getenv("COINPAPRIKA_BASE_URL", "https://api.coinpaprika.com/v1").rstrip("/")
COINGECKO_BASE = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3").rstrip("/")
FF_CAL_THIS_WEEK = os.getenv("FF_CAL_THIS_WEEK_URL", "https://nfs.faireconomy.media/ff_calendar_thisweek.json")
FF_CAL_NEXT_WEEK_PAGE = os.getenv("FF_CAL_NEXT_WEEK_PAGE_URL", "https://www.forexfactory.com/calendar?week=next")
GOOGLE_NEWS_RSS = os.getenv("GOOGLE_NEWS_RSS_URL", "https://news.google.com/rss/search")
HTTP_TIMEOUT = float(os.getenv("MARKET_HTTP_TIMEOUT", "25"))
ENABLE_NEWS_FILTER = os.getenv("ENABLE_NEWS_FILTER", "true").lower() in {"1", "true", "yes", "on"}

# Stablecoin/wrapped filtering is centralized in asset_filters.py.

# Spot ticker aliases where market-cap source and Binance use different symbols.
SPOT_ALIASES = {
    "MATIC": "POLUSDT",
}


COMMODITY_SYMBOLS = {
    "XAU/USD": "XAU_USDT",
    "XAG/USD": "SILVER_USDT",
    "USOIL": "USOIL_USDT",
}

# v020 local commodity candles come directly from MEXC Futures. DXY/US10Y are
# not fetched locally; the crypto regime remains based on broad crypto data and BTC dominance.

IMPORTANT_EVENT_KEYWORDS = (
    "non-farm", "nonfarm", "employment change", "unemployment", "average hourly",
    "consumer price", "cpi", "producer price", "ppi", "jolts", "job openings",
    "ism manufacturing", "ism services", "fomc", "fed chair", "powell",
    "adp", "crude oil inventories", "eia",
)

POSITIVE_NEWS_WORDS = (
    "approval", "approved", "upgrade", "launch", "partnership", "adoption", "buyback",
    "burn", "record inflow", "integrat", "mainnet", "etf inflow", "treasury purchase",
)
NEGATIVE_NEWS_WORDS = (
    "hack", "exploit", "breach", "outage", "lawsuit", "investigation", "delist", "unlock",
    "insolv", "bankrupt", "charges", "attack", "drain", "vulnerability", "sec sues",
)


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _retry_after_seconds(response: httpx.Response | None) -> float | None:
    if response is None:
        return None
    raw = str(response.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return max(0.0, value) if math.isfinite(value) else None


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    attempts: int = 3,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            delay = 0.6 * (2**attempt)
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {418, 429}:
                retry_after = _retry_after_seconds(exc.response)
                if retry_after is not None:
                    if retry_after > 120.0:
                        log.warning(
                            "HTTP rate-limit abort status=%s retry_after_sec=%.3f url=%s",
                            exc.response.status_code, retry_after, url,
                        )
                        break
                    delay = max(delay, retry_after)
                log.warning(
                    "HTTP rate-limit backoff status=%s retry_after_sec=%s sleep_sec=%.3f url=%s",
                    exc.response.status_code, retry_after, delay, url,
                )
            await asyncio.sleep(delay)
    assert last_error is not None
    raise last_error


async def _get_text(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    attempts: int = 2,
) -> str:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                await asyncio.sleep(0.5 * (2**attempt))
    assert last_error is not None
    raise last_error


def ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2 / (period + 1)
    current = mean(values[:period])
    for value in values[period:]:
        current = alpha * value + (1 - alpha) * current
    return current


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    for i in range(period + 1, len(values)):
        delta = values[i] - values[i - 1]
        avg_gain = ((avg_gain * (period - 1)) + max(delta, 0.0)) / period
        avg_loss = ((avg_loss * (period - 1)) + max(-delta, 0.0)) / period
    if avg_gain == 0 and avg_loss == 0:
        return 50.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def _technical_from_rows(rows: list[list[Any]], interval: str) -> dict[str, Any]:
    # v020: keep younger listings usable instead of requiring a full 365D
    # history. Twenty-one closed bars are enough for a cautious partial view;
    # EMA50/EMA200 simply remain unavailable until enough history exists.
    if len(rows) < 21:
        raise ValueError(f"not enough {interval} candles: {len(rows)}")

    opens = [float(row[1]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    volumes = [float(row[7]) if len(row) > 7 and row[7] is not None else 0.0 for row in rows]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50) if len(closes) >= 50 else None
    e200 = ema(closes, 200) if len(closes) >= 200 else None
    r14 = rsi(closes, 14)

    true_ranges: list[float] = []
    for i in range(1, len(closes)):
        true_ranges.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr = mean(true_ranges[-14:])
    close = closes[-1]

    lookback = 20 if interval == "1d" else 30 if interval == "4h" else 48
    if len(highs) <= lookback + 1:
        lookback = max(10, len(highs) - 2)
    prior_high = max(highs[-lookback - 1:-1])
    prior_low = min(lows[-lookback - 1:-1])
    range_pos = (close - prior_low) / (prior_high - prior_low) if prior_high > prior_low else 0.5

    if e200 is not None and close > e20 > e50 > e200:
        trend = "bullish"
    elif e200 is not None and close < e20 < e50 < e200:
        trend = "bearish"
    elif e20 is not None and e50 is not None and close > e20 > e50:
        trend = "bullish_early"
    elif e20 is not None and e50 is not None and close < e20 < e50:
        trend = "bearish_early"
    else:
        trend = "mixed"

    return {
        "close": close,
        "trend": trend,
        "ema20": e20,
        "ema50": e50,
        "ema200": e200,
        "rsi14": r14,
        "atr14": atr,
        "atr14_pct": (atr / close * 100) if close else None,
        "prior_range_high": prior_high,
        "prior_range_low": prior_low,
        "range_position": range_pos,
        "return_5_bars_pct": ((close / closes[-6]) - 1) * 100 if len(closes) >= 6 else 0.0,
        "return_20_bars_pct": ((close / closes[-21]) - 1) * 100 if len(closes) >= 21 else 0.0,
        "recent_quote_volume": sum(volumes[-lookback:]),
        "history_bars": len(rows),
        "history_status": "FULL" if (interval != "1d" or len(rows) >= 365) else "PARTIAL_HISTORY",
        "last_candle": {
            "open": opens[-1],
            "high": highs[-1],
            "low": lows[-1],
            "close": closes[-1],
        },
    }


def _coinpaprika_top100(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    ranked = sorted((row for row in rows if isinstance(row, dict)), key=lambda x: int(x.get("rank") or 999999))
    for row in ranked:
        symbol = str(row.get("symbol") or "").upper()
        name = str(row.get("name") or row.get("symbol") or "")
        provider_id = row.get("id")
        if is_excluded_asset(symbol, name, provider_id):
            continue
        q = (row.get("quotes") or {}).get("USD") or {}
        result.append({
            "id": row.get("id"),
            "rank": int(row.get("rank") or 0),
            "symbol": symbol,
            "name": name,
            "price": _f(q.get("price"), 0.0) or 0.0,
            "change_24h_pct": _f(q.get("percent_change_24h"), 0.0) or 0.0,
            "change_7d_pct": _f(q.get("percent_change_7d"), 0.0) or 0.0,
            "market_cap": _f(q.get("market_cap"), 0.0) or 0.0,
            "volume_24h": _f(q.get("volume_24h"), 0.0) or 0.0,
            "circulating_supply": _f(row.get("circulating_supply"), None),
            "total_supply": _f(row.get("total_supply"), None),
            "max_supply": _f(row.get("max_supply"), None),
        })
        if len(result) == 100:
            break
    return result




def _coingecko_top100(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        name = str(row.get("name") or row.get("symbol") or "")
        provider_id = row.get("id")
        if is_excluded_asset(symbol, name, provider_id):
            continue
        result.append({
            "id": row.get("id"),
            "rank": int(row.get("market_cap_rank") or 0),
            "symbol": symbol,
            "name": name,
            "price": _f(row.get("current_price"), 0.0) or 0.0,
            "change_24h_pct": _f(row.get("price_change_percentage_24h"), 0.0) or 0.0,
            "change_7d_pct": _f(row.get("price_change_percentage_7d_in_currency"), 0.0) or 0.0,
            "market_cap": _f(row.get("market_cap"), 0.0) or 0.0,
            "volume_24h": _f(row.get("total_volume"), 0.0) or 0.0,
            "circulating_supply": _f(row.get("circulating_supply"), None),
            "total_supply": _f(row.get("total_supply"), None),
            "max_supply": _f(row.get("max_supply"), None),
        })
        if len(result) == 100:
            break
    return result


async def _fetch_universe(client: httpx.AsyncClient) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    try:
        tickers_raw, global_raw = await asyncio.gather(
            _get_json(client, f"{COINPAPRIKA_BASE}/tickers", params={"quotes": "USD"}),
            _get_json(client, f"{COINPAPRIKA_BASE}/global"),
        )
        top100 = _coinpaprika_top100(tickers_raw)
        global_summary = {
            "market_cap_usd": _f(global_raw.get("market_cap_usd"), None) if isinstance(global_raw, dict) else None,
            "volume_24h_usd": _f(global_raw.get("volume_24h_usd"), None) if isinstance(global_raw, dict) else None,
            "btc_dominance_pct": _f(global_raw.get("bitcoin_dominance_percentage"), None) if isinstance(global_raw, dict) else None,
            "market_cap_change_24h_pct": _f(global_raw.get("market_cap_change_24h"), None) if isinstance(global_raw, dict) else None,
        }
        if len(top100) >= 80:
            return top100, global_summary, "coinpaprika"
    except Exception as exc:
        log.warning("CoinPaprika universe unavailable: %s", exc)

    # Free/no-key fallback. It is intentionally a fallback so the normal scan does not hit both services.
    markets, global_raw = await asyncio.gather(
        _get_json(client, f"{COINGECKO_BASE}/coins/markets", params={
            "vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 1,
            "sparkline": "false", "price_change_percentage": "24h,7d",
        }),
        _get_json(client, f"{COINGECKO_BASE}/global"),
    )
    gd = global_raw.get("data", {}) if isinstance(global_raw, dict) else {}
    global_summary = {
        "market_cap_usd": _f((gd.get("total_market_cap") or {}).get("usd"), None),
        "volume_24h_usd": _f((gd.get("total_volume") or {}).get("usd"), None),
        "btc_dominance_pct": _f((gd.get("market_cap_percentage") or {}).get("btc"), None),
        "market_cap_change_24h_pct": _f(gd.get("market_cap_change_percentage_24h_usd"), None),
    }
    return _coingecko_top100(markets), global_summary, "coingecko_fallback"


def _pre_score_crypto(
    top100: list[dict[str, Any]],
    spot_tickers: dict[str, dict[str, Any]],
    spot_map: dict[str, str],
) -> tuple[list[str], list[str]]:
    btc = next((coin for coin in top100 if coin["symbol"] == "BTC"), None) or {}
    btc24 = float(btc.get("change_24h_pct") or 0.0)
    btc7 = float(btc.get("change_7d_pct") or 0.0)

    scored: list[tuple[str, float, float]] = []
    for coin in top100:
        symbol = coin["symbol"]
        if is_excluded_asset(symbol, str(coin.get("name") or ""), coin.get("id")) or symbol not in spot_map:
            continue
        ticker = spot_tickers.get(spot_map[symbol], {})
        spot_volume = _f(ticker.get("quoteVolume"), 0.0) or 0.0
        if spot_volume < 12_000_000 or coin["market_cap"] < 100_000_000:
            continue
        rel24 = coin["change_24h_pct"] - btc24
        rel7 = coin["change_7d_pct"] - btc7
        liquidity = max(0.0, min(5.0, math.log10(max(spot_volume, 1.0)) - 6.5))
        # Keep the pre-screen broad. The detailed 1D/4H/1H score is calculated later.
        long_score = 0.55 * max(-25.0, min(25.0, rel7)) + 0.35 * max(-12.0, min(12.0, rel24)) + 1.7 * liquidity
        short_score = -0.55 * max(-25.0, min(25.0, rel7)) - 0.35 * max(-12.0, min(12.0, rel24)) + 1.7 * liquidity
        scored.append((symbol, long_score, short_score))

    long_symbols = [s for s, _, _ in sorted(scored, key=lambda x: x[1], reverse=True)[:18]]
    short_symbols = [s for s, _, _ in sorted(scored, key=lambda x: x[2], reverse=True)[:18]]
    return long_symbols, short_symbols


async def _fetch_binance_spot_details(
    client: httpx.AsyncClient,
    symbols: list[str],
    spot_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Fetch closed Binance Spot candles for the local analyzer.

    v020 requests up to 365 closed 1D candles and 288 closed 15m candles.
    A shorter 1D history is retained as PARTIAL_HISTORY; it is never rejected
    merely because the listing is younger than 365 days. 15m is optional: if
    that single request fails the 1D/4H/1H core remains usable.
    """
    sem = asyncio.Semaphore(8)
    now_ms = int(time.time() * 1000)
    interval_ms = {"1d": 86_400_000, "4h": 14_400_000, "1h": 3_600_000, "15m": 900_000}
    cutoffs = {name: (now_ms // size) * size for name, size in interval_ms.items()}
    log.info(
        "local_analysis Binance closed-candle cutoffs 1d=%s 4h=%s 1h=%s 15m=%s",
        cutoffs["1d"], cutoffs["4h"], cutoffs["1h"], cutoffs["15m"],
    )

    async def fetch_one(symbol: str) -> tuple[str, dict[str, Any]]:
        pair = spot_map[symbol]
        async with sem:
            results = await asyncio.gather(
                _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "1d", "limit": 365, "endTime": cutoffs["1d"] - 1}),
                _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "4h", "limit": 180, "endTime": cutoffs["4h"] - 1}),
                _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "1h", "limit": 180, "endTime": cutoffs["1h"] - 1}),
                _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "15m", "limit": 288, "endTime": cutoffs["15m"] - 1}),
                return_exceptions=True,
            )
            core = results[:3]
            if any(isinstance(item, Exception) for item in core):
                exc = next(item for item in core if isinstance(item, Exception))
                log.info("Spot core candle fetch failed for %s: %s", symbol, exc)
                return symbol, {"spot_symbol": pair, "status": f"unavailable:{type(exc).__name__}"}
            try:
                k1d, k4h, k1h = core
                data: dict[str, Any] = {
                    "spot_symbol": pair,
                    "1d": _technical_from_rows(k1d, "1d"),
                    "4h": _technical_from_rows(k4h, "4h"),
                    "1h": _technical_from_rows(k1h, "1h"),
                    "closed_candles_only": True,
                }
            except Exception as exc:
                log.info("Spot technical build failed for %s: %s", symbol, exc)
                return symbol, {"spot_symbol": pair, "status": f"unavailable:{type(exc).__name__}"}

            k15 = results[3]
            if isinstance(k15, Exception):
                data["15m_status"] = f"unavailable:{type(k15).__name__}"
            else:
                try:
                    data["15m"] = _technical_from_rows(k15, "15m")
                    data["15m_status"] = "ok"
                except Exception as exc:
                    data["15m_status"] = f"unavailable:{type(exc).__name__}"
            return symbol, data

    pairs = await asyncio.gather(*(fetch_one(symbol) for symbol in symbols))
    return dict(pairs)


async def _fetch_binance_spot_liquidity(
    client: httpx.AsyncClient,
    symbols: list[str],
    spot_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Best-effort L2 confirmation for only the local shortlist.

    Uses up to 500 Binance Spot levels and only scores ±0.5% imbalance when the
    returned snapshot actually covers both sides of that band. Failure or partial
    depth never aborts analysis.
    """
    sem = asyncio.Semaphore(4)

    async def one(symbol: str) -> tuple[str, dict[str, Any]]:
        pair = spot_map.get(symbol)
        if not pair:
            return symbol, {"status": "unavailable:no_spot_pair"}
        async with sem:
            try:
                payload = await _get_json(
                    client, f"{BINANCE_SPOT_BASE}/api/v3/depth",
                    params={"symbol": pair, "limit": 500}, attempts=2,
                )
                bids = [(float(p), float(q)) for p, q in (payload.get("bids") or []) if float(p) > 0 and float(q) >= 0]
                asks = [(float(p), float(q)) for p, q in (payload.get("asks") or []) if float(p) > 0 and float(q) >= 0]
                if not bids or not asks:
                    return symbol, {"status": "unavailable:empty_book"}
                best_bid, best_ask = bids[0][0], asks[0][0]
                mid = (best_bid + best_ask) / 2.0
                spread_bps = ((best_ask - best_bid) / mid * 10_000.0) if mid else None
                lower, upper = mid * 0.995, mid * 1.005
                covers_band = min(p for p, _ in bids) <= lower and max(p for p, _ in asks) >= upper
                bid_depth = sum(p * q for p, q in bids if p >= lower)
                ask_depth = sum(p * q for p, q in asks if p <= upper)
                total = bid_depth + ask_depth
                imbalance = (bid_depth - ask_depth) / total if total > 0 else 0.0
                return symbol, {
                    "status": "ok" if covers_band else "partial_depth",
                    "spot_symbol": pair,
                    "spread_bps": spread_bps,
                    "coverage_0_5pct": covers_band,
                    "book_bid_levels": len(bids),
                    "book_ask_levels": len(asks),
                    "bid_depth_0_5pct_quote": bid_depth,
                    "ask_depth_0_5pct_quote": ask_depth,
                    "imbalance_0_5pct": imbalance if covers_band else None,
                }
            except Exception as exc:
                return symbol, {"status": f"unavailable:{type(exc).__name__}"}

    pairs = await asyncio.gather(*(one(symbol) for symbol in symbols))
    return dict(pairs)


def _compute_local_breadth(top100: list[dict[str, Any]]) -> dict[str, Any]:
    changes: list[float] = []
    btc_change: float | None = None
    for coin in top100:
        change = _f(coin.get("binance_spot_change_24h_pct"), None)
        if change is None:
            change = _f(coin.get("change_24h_pct"), None)
        if change is None:
            continue
        changes.append(change)
        if str(coin.get("symbol") or "").upper() == "BTC":
            btc_change = change
    if not changes:
        return {"status": "unavailable:no_changes"}
    adv = sum(1 for x in changes if x > 0)
    dec = sum(1 for x in changes if x < 0)
    flat = len(changes) - adv - dec
    above_btc = None
    if btc_change is not None:
        above_btc = 100.0 * sum(1 for x in changes if x > btc_change) / len(changes)
    return {
        "status": "ok",
        "assets": len(changes),
        "advancers": adv,
        "decliners": dec,
        "unchanged": flat,
        "positive_pct": 100.0 * adv / len(changes),
        "median_change_24h_pct": median(changes),
        "above_btc_24h_pct": above_btc,
        "btc_change_24h_pct": btc_change,
    }


async def _fetch_mexc_derivatives(
    client: httpx.AsyncClient,
    symbols: list[str],
) -> tuple[dict[str, dict[str, Any]], str]:
    """Fetch MEXC perpetual funding in one public/no-auth request.

    MEXC's all-contract ticker includes fundingRate, 24h move/turnover and holdVol.
    In v020 MEXC is the only funding source; Binance Futures is not queried.
    """
    try:
        payload = await _get_json(
            client,
            f"{MEXC_CONTRACT_BASE}/api/v1/contract/ticker",
            attempts=2,
        )
    except Exception as exc:
        return {}, f"unavailable:{type(exc).__name__}"

    if not isinstance(payload, dict) or not payload.get("success"):
        return {}, "unavailable:bad_response"
    data = payload.get("data") or []
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return {}, "unavailable:bad_response"

    wanted = set(symbols)
    result: dict[str, dict[str, Any]] = {}
    for ticker in data:
        if not isinstance(ticker, dict):
            continue
        contract_symbol = str(ticker.get("symbol") or "").upper()
        if not contract_symbol.endswith("_USDT"):
            continue
        base = contract_symbol[:-5]
        # Some venues use a multiplier prefix for tiny-price assets. If such a
        # symbol appears, also try matching it to the underlying top-100 symbol.
        candidates = [base]
        for prefix in ("1000000", "10000", "1000"):
            if base.startswith(prefix) and len(base) > len(prefix):
                candidates.append(base[len(prefix):])
        matched = next((candidate for candidate in candidates if candidate in wanted), None)
        if not matched:
            continue

        funding = _f(ticker.get("fundingRate"), None)
        rise_fall = _f(ticker.get("riseFallRate"), None)
        last_price = _f(ticker.get("lastPrice"), None)
        fair_price = _f(ticker.get("fairPrice"), None)
        index_price = _f(ticker.get("indexPrice"), None)
        basis_pct = ((fair_price / index_price) - 1.0) * 100.0 if fair_price is not None and index_price not in (None, 0) else None
        row: dict[str, Any] = {
            "symbol": contract_symbol,
            "funding_source": "mexc",
            "funding_pct": funding * 100 if funding is not None else None,
            "futures_change_24h_pct": rise_fall * 100 if rise_fall is not None else None,
            "futures_quote_volume": _f(ticker.get("amount24"), None),
            "mexc_hold_volume": _f(ticker.get("holdVol"), None),
            "mexc_last_price": last_price,
            "mexc_fair_price": fair_price,
            "mexc_index_price": index_price,
            "basis_pct": basis_pct,
        }
        result[matched] = row

    return result, "ok"


async def _fetch_derivatives(
    client: httpx.AsyncClient,
    symbols: list[str],
    *,
    funding_symbols: list[str] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Fetch derivatives context from MEXC only.

    Funding is intentionally MEXC-only in v020. Binance Futures is not called at
    all, so an unavailable Binance Futures API cannot delay or alter the scan.
    """
    mexc_rows, mexc_status = await _fetch_mexc_derivatives(client, symbols)
    funding_check = funding_symbols if funding_symbols is not None else symbols
    funding_status = "mexc" if any(
        (mexc_rows.get(symbol) or {}).get("funding_pct") is not None for symbol in funding_check
    ) else "unavailable"
    return mexc_rows, {
        "funding": funding_status,
        "mexc_futures": mexc_status,
    }

def _mexc_kline_rows(
    payload: dict[str, Any],
    *,
    interval_seconds: int,
    cutoff_ms: int,
    max_rows: int,
) -> list[list[Any]]:
    """Convert a MEXC Futures kline response to the local technical row shape.

    The request is made with ``end=cutoff-1``; this function still defensively
    removes any bar whose opening timestamp is at/after the closed-candle cutoff.
    """
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise ValueError(f"MEXC kline bad response: {str(payload)[:160]}")
    data = payload.get("data") or {}
    times = data.get("time") or []
    opens = data.get("open") or []
    closes = data.get("close") or []
    highs = data.get("high") or []
    lows = data.get("low") or []
    vols = data.get("vol") or []
    amounts = data.get("amount") or []
    rows: list[list[Any]] = []
    interval_ms = interval_seconds * 1000
    for i, ts in enumerate(times):
        try:
            open_ms = int(ts) * 1000
            if open_ms >= cutoff_ms:
                continue
            o = float(opens[i])
            h = float(highs[i])
            l = float(lows[i])
            c = float(closes[i])
        except (IndexError, TypeError, ValueError):
            continue
        vol = float(vols[i]) if i < len(vols) and vols[i] is not None else 0.0
        amount = float(amounts[i]) if i < len(amounts) and amounts[i] is not None else vol
        rows.append([
            open_ms,
            o,
            h,
            l,
            c,
            vol,
            open_ms + interval_ms - 1,
            amount,
        ])
    rows.sort(key=lambda row: int(row[0]))
    return rows[-max_rows:]


async def _fetch_mexc_commodity_asset(
    client: httpx.AsyncClient,
    label: str,
    contract: str,
) -> tuple[str, dict[str, Any]]:
    """Fetch native closed MEXC Futures candles for XAU/XAG/USOIL.

    No alternate chart-provider fallback is used in v020. 1D is retained up to 365 bars, native 4H
    and 1H are used directly, and 15m is best-effort entry confirmation.
    """
    now_ms = int(time.time() * 1000)
    specs = {
        "1d": ("Day1", 86_400, 365),
        "4h": ("Hour4", 14_400, 180),
        "1h": ("Min60", 3_600, 180),
        "15m": ("Min15", 900, 288),
    }
    cutoffs = {
        name: (now_ms // (seconds * 1000)) * (seconds * 1000)
        for name, (_, seconds, _) in specs.items()
    }

    async def fetch_tf(name: str) -> Any:
        interval, _, _ = specs[name]
        return await _get_json(
            client,
            f"{MEXC_CONTRACT_BASE}/api/v1/contract/kline/{contract}",
            params={"interval": interval, "end": int(cutoffs[name] // 1000) - 1},
            attempts=3 if name != "15m" else 2,
        )

    results = await asyncio.gather(
        fetch_tf("1d"), fetch_tf("4h"), fetch_tf("1h"), fetch_tf("15m"),
        return_exceptions=True,
    )
    if any(isinstance(item, Exception) for item in results[:3]):
        exc = next(item for item in results[:3] if isinstance(item, Exception))
        raise RuntimeError(f"MEXC commodity core unavailable {contract}: {exc}")

    rows_by_tf: dict[str, list[list[Any]]] = {}
    for index, name in enumerate(("1d", "4h", "1h")):
        _, seconds, limit = specs[name]
        rows_by_tf[name] = _mexc_kline_rows(
            results[index], interval_seconds=seconds, cutoff_ms=cutoffs[name], max_rows=limit,
        )

    data: dict[str, Any] = {
        "source": "mexc_futures",
        "source_symbol": contract,
        "mexc_contract": contract,
        "1d": _technical_from_rows(rows_by_tf["1d"], "1d"),
        "4h": _technical_from_rows(rows_by_tf["4h"], "4h"),
        "1h": _technical_from_rows(rows_by_tf["1h"], "1h"),
        "closed_candles_only": True,
    }
    min15 = results[3]
    if isinstance(min15, Exception):
        data["15m_status"] = f"unavailable:{type(min15).__name__}"
    else:
        try:
            _, seconds, limit = specs["15m"]
            rows15 = _mexc_kline_rows(
                min15, interval_seconds=seconds, cutoff_ms=cutoffs["15m"], max_rows=limit,
            )
            data["15m"] = _technical_from_rows(rows15, "15m")
            data["15m_status"] = "ok"
        except Exception as exc:
            data["15m_status"] = f"unavailable:{type(exc).__name__}"
    return label, data


async def _fetch_news_score(client: httpx.AsyncClient, query: str) -> dict[str, Any]:
    if not ENABLE_NEWS_FILTER:
        return {"score": 0.0, "titles": [], "status": "disabled"}
    try:
        xml_text = await _get_text(
            client,
            GOOGLE_NEWS_RSS,
            params={"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"},
            attempts=1,
        )
        root = ET.fromstring(xml_text)
        titles: list[str] = []
        score = 0.0
        now = datetime.now(timezone.utc)
        for item in root.findall(".//item")[:12]:
            title = (item.findtext("title") or "").strip()
            if not title:
                continue
            pub = item.findtext("pubDate") or ""
            try:
                dt = parsedate_to_datetime(pub)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_days = max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds() / 86400)
            except Exception:
                age_days = 7.0
            if age_days > 14:
                continue
            weight = 1.0 if age_days <= 3 else 0.55
            low = title.lower()
            score += weight * sum(1 for word in POSITIVE_NEWS_WORDS if word in low)
            score -= weight * sum(1 for word in NEGATIVE_NEWS_WORDS if word in low)
            titles.append(title[:180])
        return {"score": max(-4.0, min(4.0, score)), "titles": titles[:6], "status": "ok"}
    except Exception as exc:
        return {"score": 0.0, "titles": [], "status": f"unavailable:{type(exc).__name__}"}


def _closest_calendar_date(date_text: str, reference: datetime) -> datetime | None:
    """Resolve 'Sun Aug 16' to the year closest to the next-week reference."""
    clean = " ".join(str(date_text).split())
    for year in (reference.year - 1, reference.year, reference.year + 1):
        try:
            candidate = datetime.strptime(f"{clean} {year}", "%a %b %d %Y")
        except ValueError:
            continue
        if abs((candidate.date() - reference.date()).days) <= 200:
            return candidate
    return None


def _ff_impact(cell: Any) -> str:
    if cell is None:
        return ""
    classes: list[str] = []
    for tag in [cell, *cell.find_all(True)]:
        classes.extend(str(x) for x in (tag.get("class") or []))
    joined = " ".join(classes).lower()
    if "impact-red" in joined:
        return "High"
    if "impact-ora" in joined:
        return "Medium"
    if "impact-gra" in joined:
        return "Low"
    if "impact-yel" in joined:
        return "Non-economic"
    return ""


def _parse_forex_factory_next_week_html(html_text: str, *, now: datetime | None = None) -> list[dict[str, Any]]:
    """Parse the public Forex Factory next-week HTML page into JSON-like rows.

    The public weekly JSON export only covers 'this week'; Forex Factory's
    next-week page is therefore used as a failsafe for the 3–14 day horizon.
    The page-advertised IANA timezone is parsed and converted to UTC.
    """
    now = now or datetime.now(timezone.utc)
    soup = BeautifulSoup(html_text, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    tz_match = __import__("re").search(r"Calendar Time Zone:\s*([A-Za-z_+-]+/[A-Za-z0-9_+.-]+)", page_text)
    try:
        source_tz = ZoneInfo(tz_match.group(1)) if tz_match else timezone.utc
    except Exception:
        source_tz = timezone.utc

    table = soup.find("table", class_="calendar__table")
    if table is None:
        raise ValueError("ForexFactory calendar table not found")

    reference = now + timedelta(days=7)
    rows: list[dict[str, Any]] = []
    last_time = ""
    for row in table.find_all("tr", class_="calendar__row"):
        classes = set(row.get("class") or [])
        if "calendar__row--day-breaker" in classes:
            continue
        date_row = row.find_previous("tr", class_="calendar__row--day-breaker")
        date_text = date_row.get_text(" ", strip=True) if date_row else ""
        base_date = _closest_calendar_date(date_text, reference)
        if base_date is None:
            continue

        def cell_text(class_name: str) -> str:
            cell = row.find("td", class_=class_name)
            return cell.get_text(" ", strip=True) if cell else ""

        time_text = cell_text("calendar__time")
        if time_text:
            last_time = time_text
        else:
            time_text = last_time
        currency = cell_text("calendar__currency").upper()
        title = cell_text("calendar__event")
        if not time_text or not currency or not title:
            continue
        try:
            parsed_time = datetime.strptime(time_text.lower(), "%I:%M%p").time()
        except ValueError:
            # 'All Day', 'Tentative', and date-range rows do not have a reliable
            # clock time and cannot safely drive the 3-hour READY suppression.
            continue
        local_dt = datetime(
            base_date.year, base_date.month, base_date.day,
            parsed_time.hour, parsed_time.minute, tzinfo=source_tz,
        )
        impact_cell = row.find("td", class_="calendar__impact")
        rows.append({
            "country": currency,
            "title": title,
            "date": local_dt.astimezone(timezone.utc).isoformat(),
            "impact": _ff_impact(impact_cell),
            "forecast": cell_text("calendar__forecast") or None,
            "previous": cell_text("calendar__previous") or None,
            "source": "forexfactory_next_week_html",
        })
    return rows


async def _fetch_next_week_calendar_page(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    response = await client.get(FF_CAL_NEXT_WEEK_PAGE, timeout=HTTP_TIMEOUT, follow_redirects=True)
    response.raise_for_status()
    return _parse_forex_factory_next_week_html(response.text)


async def _fetch_calendar(client: httpx.AsyncClient) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    statuses: list[str] = []

    # Forex Factory's public JSON export is for the current week only.
    try:
        payload = await _get_json(client, FF_CAL_THIS_WEEK, attempts=1)
        if isinstance(payload, list):
            rows.extend(row for row in payload if isinstance(row, dict))
            statuses.append("this_week_json:ok")
        else:
            statuses.append("this_week_json:invalid")
    except Exception as exc:
        statuses.append(f"this_week_json:unavailable:{type(exc).__name__}")

    # The old ff_calendar_nextweek.json URL returns 404. v020 instead parses
    # Forex Factory's actual ?week=next page as an independent fallback layer.
    try:
        next_rows = await _fetch_next_week_calendar_page(client)
        rows.extend(next_rows)
        statuses.append(f"next_week_html:ok:{len(next_rows)}")
    except Exception as exc:
        statuses.append(f"next_week_html:unavailable:{type(exc).__name__}")

    now = datetime.now(timezone.utc)
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        country = str(row.get("country") or row.get("Country") or "").upper()
        title = str(row.get("title") or row.get("event") or row.get("Event") or "").strip()
        date_raw = row.get("date") or row.get("Date")
        impact = str(row.get("impact") or row.get("Importance") or "").strip()
        if country not in {"USD", "US", "USA", "UNITED STATES"}:
            continue
        if not any(keyword in title.lower() for keyword in IMPORTANT_EVENT_KEYWORDS):
            continue
        try:
            dt = datetime.fromisoformat(str(date_raw).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
        except Exception:
            continue
        if dt < now:
            continue
        key = (title, dt.isoformat())
        if key in seen:
            continue
        seen.add(key)
        events.append({
            "title": title,
            "time_utc": dt.isoformat(),
            "impact": impact,
            "forecast": row.get("forecast") or row.get("Forecast"),
            "previous": row.get("previous") or row.get("Previous"),
        })
    events.sort(key=lambda x: x["time_utc"])
    status = ";".join(statuses) or "unavailable"
    log.info("local_analysis calendar status=%s key_usd_events=%s", status, len(events))
    return events, status


async def fetch_market_bundle() -> dict[str, Any]:
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "Mozilla/5.0 (compatible; CoolifyTradingSignalBot/v020)",
    }
    timeout = httpx.Timeout(HTTP_TIMEOUT, connect=min(10.0, HTTP_TIMEOUT))
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        # Full market-cap universe + Binance Spot execution data. All sources are public/no-key.
        universe_task = asyncio.create_task(_fetch_universe(client))
        spot_exchange_task = asyncio.create_task(_get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/exchangeInfo"))
        spot_24h_task = asyncio.create_task(_get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/ticker/24hr"))
        (top100, global_summary, universe_source), spot_exchange, spot_24h = await asyncio.gather(
            universe_task, spot_exchange_task, spot_24h_task
        )

        spot_tradable = {
            str(row.get("symbol"))
            for row in (spot_exchange.get("symbols") or [])
            if row.get("status") == "TRADING" and row.get("quoteAsset") == "USDT" and "SPOT" in (row.get("permissions") or ["SPOT"])
        }
        spot_tickers = {str(row.get("symbol")): row for row in spot_24h if isinstance(row, dict)}

        spot_map: dict[str, str] = {}
        for coin in top100:
            symbol = coin["symbol"]
            direct = f"{symbol}USDT"
            alias = SPOT_ALIASES.get(symbol)
            if direct in spot_tradable:
                spot_map[symbol] = direct
            elif alias and alias in spot_tradable:
                spot_map[symbol] = alias

        long_pre, short_pre = _pre_score_crypto(top100, spot_tickers, spot_map)
        candidate_symbols: list[str] = []
        for symbol in long_pre + short_pre + ["BTC", "ETH"]:
            if symbol in spot_map and symbol not in candidate_symbols:
                candidate_symbols.append(symbol)

        # Spot candles are the source of truth for crypto technical analysis.
        spot_details_task = _fetch_binance_spot_details(client, candidate_symbols, spot_map)
        mexc_context_symbols = list(candidate_symbols)
        for contract in COMMODITY_SYMBOLS.values():
            base = contract[:-5] if contract.endswith("_USDT") else contract
            if base not in mexc_context_symbols:
                mexc_context_symbols.append(base)
        derivatives_task = _fetch_derivatives(client, mexc_context_symbols, funding_symbols=candidate_symbols)

        commodity_tasks = {
            label: asyncio.create_task(_fetch_mexc_commodity_asset(client, label, contract))
            for label, contract in COMMODITY_SYMBOLS.items()
        }
        calendar_task = asyncio.create_task(_fetch_calendar(client))

        spot_details, (derivatives, derivative_sources) = await asyncio.gather(spot_details_task, derivatives_task)

        # Compact Spot order book only for the strongest pre-screened local candidates.
        local_shortlist: list[str] = []
        for symbol in long_pre[:4] + short_pre[:4]:
            if symbol in spot_map and symbol not in local_shortlist:
                local_shortlist.append(symbol)
        spot_liquidity = await _fetch_binance_spot_liquidity(client, local_shortlist, spot_map)

        # News/fundamental filter only for the strongest pre-screened names to keep scans quick.
        coin_lookup = {coin["symbol"]: coin for coin in top100}
        news_symbols: list[str] = []
        for symbol in long_pre[:6] + short_pre[:6]:
            if symbol not in news_symbols:
                news_symbols.append(symbol)
        news_pairs = await asyncio.gather(*(
            _fetch_news_score(client, f'"{coin_lookup[symbol]["name"]}" cryptocurrency token')
            for symbol in news_symbols
        ))
        crypto_news = dict(zip(news_symbols, news_pairs))

        commodities: dict[str, dict[str, Any]] = {}
        for label, task in commodity_tasks.items():
            try:
                _, data = await task
                commodities[label] = data
            except Exception as exc:
                log.info("MEXC commodity unavailable asset=%s: %s", label, exc)
                commodities[label] = {
                    "source": "mexc_futures",
                    "source_symbol": COMMODITY_SYMBOLS[label],
                    "status": f"unavailable:{type(exc).__name__}",
                }

        for label, contract in COMMODITY_SYMBOLS.items():
            row = commodities.get(label)
            if not isinstance(row, dict):
                continue
            base = contract[:-5] if contract.endswith("_USDT") else contract
            live_price = (derivatives.get(base) or {}).get("mexc_last_price")
            if live_price is not None:
                row["current_price"] = live_price

        for asset, row in commodities.items():
            log.info(
                "local_analysis commodity source=mexc_futures asset=%s contract=%s status=%s bars_1d=%s bars_4h=%s bars_1h=%s bars_15m=%s 15m_status=%s",
                asset,
                row.get("source_symbol") if isinstance(row, dict) else None,
                row.get("status", "ok") if isinstance(row, dict) else "unavailable",
                ((row.get("1d") or {}).get("history_bars") if isinstance(row, dict) else None),
                ((row.get("4h") or {}).get("history_bars") if isinstance(row, dict) else None),
                ((row.get("1h") or {}).get("history_bars") if isinstance(row, dict) else None),
                ((row.get("15m") or {}).get("history_bars") if isinstance(row, dict) else None),
                row.get("15m_status") if isinstance(row, dict) else None,
            )

        # Keep the macro key for analyzer compatibility; DXY/US10Y are intentionally
        # not collected locally and therefore contribute zero.
        macro: dict[str, dict[str, Any]] = {}

        events, calendar_status = await calendar_task

        # Free RSS context for metals/oil. It is only a small score modifier, never a source of exact levels.
        commodity_news_pairs = await asyncio.gather(
            _fetch_news_score(client, 'gold XAU Federal Reserve dollar yields geopolitics'),
            _fetch_news_score(client, 'silver XAG industrial demand dollar yields'),
            _fetch_news_score(client, 'WTI crude oil EIA OPEC supply demand geopolitics'),
        )
        commodity_news = dict(zip(COMMODITY_SYMBOLS.keys(), commodity_news_pairs))

        # Enrich top100 with Binance Spot execution/liquidity data.
        for coin in top100:
            pair = spot_map.get(coin["symbol"])
            ticker = spot_tickers.get(pair or "", {})
            coin["binance_spot_symbol"] = pair
            coin["binance_spot_quote_volume_24h"] = _f(ticker.get("quoteVolume"), None) if pair else None
            coin["binance_spot_change_24h_pct"] = _f(ticker.get("priceChangePercent"), None) if pair else None
            coin["binance_spot_last_price"] = _f(ticker.get("lastPrice"), None) if pair else None

        local_breadth = _compute_local_breadth(top100)
        log.info(
            "local_analysis breadth status=%s assets=%s positive_pct=%s median_24h=%s above_btc=%s",
            local_breadth.get("status"), local_breadth.get("assets"), local_breadth.get("positive_pct"),
            local_breadth.get("median_change_24h_pct"), local_breadth.get("above_btc_24h_pct"),
        )
        log.info(
            "local_analysis 15m crypto_ok=%s/%s commodity_ok=%s/%s spot_depth_ok=%s/%s",
            sum(1 for row in spot_details.values() if isinstance(row, dict) and row.get("15m_status") == "ok"), len(spot_details),
            sum(1 for row in commodities.values() if isinstance(row, dict) and row.get("15m_status") == "ok"), len(commodities),
            sum(1 for row in spot_liquidity.values() if row.get("status") == "ok"), len(spot_liquidity),
        )

        return {
            "snapshot_time_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "top100": top100,
            "global_crypto": global_summary,
            "pre_screen_long": long_pre,
            "pre_screen_short": short_pre,
            "crypto_technicals": spot_details,
            "crypto_derivatives": {symbol: derivatives[symbol] for symbol in candidate_symbols if symbol in derivatives},
            "crypto_spot_liquidity": spot_liquidity,
            "local_breadth": local_breadth,
            "crypto_news": crypto_news,
            "commodities": commodities,
            "commodity_news": commodity_news,
            "macro": macro,
            "events": events,
            "source_status": {
                "top100_source": universe_source,
                "binance_spot": "ok",
                "funding": derivative_sources.get("funding", "unavailable"),
                "mexc_futures": derivative_sources.get("mexc_futures", "unavailable"),
                "mexc_commodities": "ok" if all(isinstance(row, dict) and "1d" in row for row in commodities.values()) else "partial_or_unavailable",
                "local_macro_dxy_us10y": "not_collected",
                "spot_depth": "ok" if any(row.get("status") == "ok" for row in spot_liquidity.values()) else "unavailable",
                "breadth": local_breadth.get("status", "unavailable"),
                "calendar": calendar_status,
                "news_filter": "enabled" if ENABLE_NEWS_FILTER else "disabled",
            },
        }


def _history_bar_from_binance(row: list[Any], now_ms: int) -> dict[str, Any]:
    return {
        "open_time": float(row[0]) / 1000.0,
        "close_time": float(row[6]) / 1000.0,
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "closed": int(row[6]) < now_ms,
    }


def _history_bars_from_mexc(payload: dict[str, Any], now_ts: float) -> list[dict[str, Any]]:
    """Convert MEXC Futures Min60 klines to completed 1H signal-history bars."""
    cutoff_ms = int((int(now_ts) // 3600) * 3600 * 1000)
    rows = _mexc_kline_rows(
        payload,
        interval_seconds=3600,
        cutoff_ms=cutoff_ms,
        max_rows=2000,
    )
    bars: list[dict[str, Any]] = []
    for row in rows:
        open_time = float(row[0]) / 1000.0
        close_time = open_time + 3600.0
        bars.append({
            "open_time": open_time,
            "close_time": close_time,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "closed": close_time <= now_ts,
        })
    return bars


async def fetch_signal_histories(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Fetch lightweight 1H history for v020 signal statistics.

    Crypto outcomes are measured on Binance Spot. Commodity outcomes use the
    same MEXC Futures contract that generated the published XAU/XAG/USOIL setup.
    The history starts from the signal's issue hour and is capped by the 14-day
    strategy horizon.
    """
    open_records = [
        row for row in records
        if isinstance(row, dict) and row.get("state") in {"pending", "active"} and row.get("id")
    ]
    if not open_records:
        return {}

    now_ts = datetime.now(timezone.utc).timestamp()
    now_ms = int(now_ts * 1000)
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "Mozilla/5.0 (compatible; CoolifyTradingSignalBot/v020)",
    }
    timeout = httpx.Timeout(HTTP_TIMEOUT, connect=min(10.0, HTTP_TIMEOUT))
    sem = asyncio.Semaphore(6)
    # Keep MEXC signal-history checks deliberately conservative. The generic
    # history semaphore caps all sources; this dedicated limiter prevents a
    # burst of pending commodity records from hitting MEXC Futures at once.
    mexc_history_sem = asyncio.Semaphore(2)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        async def fetch_one(record: dict[str, Any]) -> tuple[str, list[dict[str, Any]] | None]:
            record_id = str(record["id"])
            issued_at = float(record.get("issued_at") or now_ts)
            start_hour = int(issued_at // 3600) * 3600
            source_symbol = str(record.get("source_symbol") or "").strip()
            if record.get("market") == "commodity":
                # Migrate pending legacy commodity records transparently: v020 always
                # evaluates commodity outcomes on the current MEXC contract.
                source_symbol = COMMODITY_SYMBOLS.get(str(record.get("asset") or ""), source_symbol)
            if not source_symbol:
                return record_id, None
            async with sem:
                try:
                    if record.get("market") == "crypto":
                        payload = await _get_json(
                            client,
                            f"{BINANCE_SPOT_BASE}/api/v3/klines",
                            params={
                                "symbol": source_symbol,
                                "interval": "1h",
                                "startTime": start_hour * 1000,
                                "endTime": now_ms,
                                "limit": 500,
                            },
                            attempts=2,
                        )
                        bars = [_history_bar_from_binance(row, now_ms) for row in payload if isinstance(row, list) and len(row) >= 7]
                        return record_id, bars

                    async with mexc_history_sem:
                        payload = await _get_json(
                            client,
                            f"{MEXC_CONTRACT_BASE}/api/v1/contract/kline/{source_symbol}",
                            params={
                                "interval": "Min60",
                                "start": start_hour,
                                "end": int(now_ts),
                            },
                            attempts=2,
                        )
                    bars = [bar for bar in _history_bars_from_mexc(payload, now_ts) if bar["open_time"] >= start_hour]
                    return record_id, bars
                except Exception as exc:
                    log.info("Signal history unavailable id=%s asset=%s: %s", record_id, record.get("asset"), exc)
                    return record_id, None

        pairs = await asyncio.gather(*(fetch_one(record) for record in open_records))
        return {record_id: bars for record_id, bars in pairs if bars is not None}
