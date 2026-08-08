from __future__ import annotations

# Coolify Trading Signal Bot v008

import asyncio
import logging
import math
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from statistics import mean
from typing import Any
from urllib.parse import quote_plus

import httpx

from asset_filters import is_excluded_asset

log = logging.getLogger(__name__)

BINANCE_SPOT_BASE = os.getenv("BINANCE_SPOT_BASE_URL", "https://api.binance.com").rstrip("/")
MEXC_CONTRACT_BASE = os.getenv("MEXC_CONTRACT_BASE_URL", "https://contract.mexc.com").rstrip("/")
COINPAPRIKA_BASE = os.getenv("COINPAPRIKA_BASE_URL", "https://api.coinpaprika.com/v1").rstrip("/")
COINGECKO_BASE = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3").rstrip("/")
YAHOO_CHART_BASE = os.getenv("YAHOO_CHART_BASE_URL", "https://query1.finance.yahoo.com/v8/finance/chart").rstrip("/")
FF_CAL_THIS_WEEK = os.getenv("FF_CAL_THIS_WEEK_URL", "https://nfs.faireconomy.media/ff_calendar_thisweek.json")
FF_CAL_NEXT_WEEK = os.getenv("FF_CAL_NEXT_WEEK_URL", "https://nfs.faireconomy.media/ff_calendar_nextweek.json")
GOOGLE_NEWS_RSS = os.getenv("GOOGLE_NEWS_RSS_URL", "https://news.google.com/rss/search")
HTTP_TIMEOUT = float(os.getenv("MARKET_HTTP_TIMEOUT", "25"))
ENABLE_NEWS_FILTER = os.getenv("ENABLE_NEWS_FILTER", "true").lower() in {"1", "true", "yes", "on"}

# Stablecoin/wrapped filtering is centralized in asset_filters.py.

# Spot ticker aliases where market-cap source and Binance use different symbols.
SPOT_ALIASES = {
    "MATIC": "POLUSDT",
}


COMMODITY_SYMBOLS = {
    "XAU/USD": ["XAUUSD=X", "GC=F"],
    "XAG/USD": ["XAGUSD=X", "SI=F"],
    "USOIL": ["CL=F"],
}

MACRO_SYMBOLS = {
    "DXY": ["DX-Y.NYB"],
    "US10Y": ["^TNX"],
}

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
            if attempt < attempts - 1:
                await asyncio.sleep(0.6 * (2**attempt))
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
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def _technical_from_rows(rows: list[list[Any]], interval: str) -> dict[str, Any]:
    if len(rows) < 55:
        raise ValueError(f"not enough {interval} candles: {len(rows)}")

    opens = [float(row[1]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    volumes = [float(row[7]) if len(row) > 7 and row[7] is not None else 0.0 for row in rows]

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
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
    sem = asyncio.Semaphore(8)

    async def fetch_one(symbol: str) -> tuple[str, dict[str, Any]]:
        pair = spot_map[symbol]
        async with sem:
            try:
                k1d, k4h, k1h = await asyncio.gather(
                    _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "1d", "limit": 220}),
                    _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "4h", "limit": 180}),
                    _get_json(client, f"{BINANCE_SPOT_BASE}/api/v3/klines", params={"symbol": pair, "interval": "1h", "limit": 180}),
                )
                return symbol, {
                    "spot_symbol": pair,
                    "1d": _technical_from_rows(k1d, "1d"),
                    "4h": _technical_from_rows(k4h, "4h"),
                    "1h": _technical_from_rows(k1h, "1h"),
                }
            except Exception as exc:
                log.info("Spot candle fetch failed for %s: %s", symbol, exc)
                return symbol, {"spot_symbol": pair, "status": f"unavailable:{type(exc).__name__}"}

    pairs = await asyncio.gather(*(fetch_one(symbol) for symbol in symbols))
    return dict(pairs)


async def _fetch_mexc_derivatives(
    client: httpx.AsyncClient,
    symbols: list[str],
) -> tuple[dict[str, dict[str, Any]], str]:
    """Fetch MEXC perpetual funding in one public/no-auth request.

    MEXC's all-contract ticker includes fundingRate, 24h move/turnover and holdVol.
    In v008 MEXC is the only funding source; Binance Futures is not queried.
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
        row: dict[str, Any] = {
            "symbol": contract_symbol,
            "funding_source": "mexc",
            "funding_pct": funding * 100 if funding is not None else None,
            "futures_change_24h_pct": rise_fall * 100 if rise_fall is not None else None,
            "futures_quote_volume": _f(ticker.get("amount24"), None),
            "mexc_hold_volume": _f(ticker.get("holdVol"), None),
            "mexc_fair_price": _f(ticker.get("fairPrice"), None),
            "mexc_index_price": _f(ticker.get("indexPrice"), None),
        }
        result[matched] = row

    return result, "ok"


async def _fetch_derivatives(
    client: httpx.AsyncClient,
    symbols: list[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Fetch derivatives context from MEXC only.

    Funding is intentionally MEXC-only in v008. Binance Futures is not called at
    all, so an unavailable Binance Futures API cannot delay or alter the scan.
    """
    mexc_rows, mexc_status = await _fetch_mexc_derivatives(client, symbols)
    funding_status = "mexc" if any(
        (mexc_rows.get(symbol) or {}).get("funding_pct") is not None for symbol in symbols
    ) else "unavailable"
    return mexc_rows, {
        "funding": funding_status,
        "mexc_futures": mexc_status,
    }

def _yahoo_rows(payload: dict[str, Any]) -> list[list[Any]]:
    chart = payload.get("chart") or {}
    results = chart.get("result") or []
    if not results:
        raise ValueError("Yahoo chart returned no result")
    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_rows = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quote_rows.get("open") or []
    highs = quote_rows.get("high") or []
    lows = quote_rows.get("low") or []
    closes = quote_rows.get("close") or []
    volumes = quote_rows.get("volume") or []
    rows: list[list[Any]] = []
    for i, ts in enumerate(timestamps):
        try:
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        except IndexError:
            continue
        if any(v is None for v in (o, h, l, c)):
            continue
        volume = volumes[i] if i < len(volumes) and volumes[i] is not None else 0.0
        rows.append([int(ts) * 1000, float(o), float(h), float(l), float(c), float(volume), int(ts) * 1000, float(volume)])
    return rows


def _aggregate_4h(hourly: list[list[Any]]) -> list[list[Any]]:
    buckets: dict[int, list[list[Any]]] = {}
    for row in hourly:
        timestamp_seconds = int(row[0] // 1000)
        bucket = timestamp_seconds - (timestamp_seconds % (4 * 3600))
        buckets.setdefault(bucket, []).append(row)
    output: list[list[Any]] = []
    for bucket in sorted(buckets):
        rows = buckets[bucket]
        if not rows:
            continue
        output.append([
            bucket * 1000,
            rows[0][1],
            max(r[2] for r in rows),
            min(r[3] for r in rows),
            rows[-1][4],
            sum(r[5] for r in rows),
            (bucket + 4 * 3600) * 1000,
            sum(r[7] for r in rows),
        ])
    return output


async def _fetch_yahoo_asset(client: httpx.AsyncClient, aliases: list[str]) -> tuple[str, dict[str, Any]]:
    last_exc: Exception | None = None
    for symbol in aliases:
        try:
            daily_payload, hourly_payload = await asyncio.gather(
                _get_json(client, f"{YAHOO_CHART_BASE}/{symbol}", params={"interval": "1d", "range": "1y", "includePrePost": "false", "events": "div,splits"}, attempts=2),
                _get_json(client, f"{YAHOO_CHART_BASE}/{symbol}", params={"interval": "1h", "range": "3mo", "includePrePost": "false", "events": "div,splits"}, attempts=2),
            )
            daily = _yahoo_rows(daily_payload)
            hourly = _yahoo_rows(hourly_payload)
            four_hour = _aggregate_4h(hourly)
            return symbol, {
                "source_symbol": symbol,
                "1d": _technical_from_rows(daily, "1d"),
                "4h": _technical_from_rows(four_hour, "4h"),
                "1h": _technical_from_rows(hourly, "1h"),
            }
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"Yahoo unavailable for {aliases}: {last_exc}")


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


async def _fetch_calendar(client: httpx.AsyncClient) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    statuses: list[str] = []
    for url in (FF_CAL_THIS_WEEK, FF_CAL_NEXT_WEEK):
        try:
            payload = await _get_json(client, url, attempts=1)
            if isinstance(payload, list):
                rows.extend(row for row in payload if isinstance(row, dict))
                statuses.append("ok")
        except Exception as exc:
            statuses.append(f"unavailable:{type(exc).__name__}")

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
    return events, "ok" if events else ";".join(statuses) or "unavailable"


async def fetch_market_bundle() -> dict[str, Any]:
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "Mozilla/5.0 (compatible; CoolifyTradingSignalBot/v008)",
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
        derivatives_task = _fetch_derivatives(client, candidate_symbols)

        commodity_tasks = {
            label: asyncio.create_task(_fetch_yahoo_asset(client, aliases))
            for label, aliases in COMMODITY_SYMBOLS.items()
        }
        macro_tasks = {
            label: asyncio.create_task(_fetch_yahoo_asset(client, aliases))
            for label, aliases in MACRO_SYMBOLS.items()
        }
        calendar_task = asyncio.create_task(_fetch_calendar(client))

        spot_details, (derivatives, derivative_sources) = await asyncio.gather(spot_details_task, derivatives_task)

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
                source_symbol, data = await task
                commodities[label] = data
            except Exception as exc:
                commodities[label] = {"status": f"unavailable:{type(exc).__name__}"}

        macro: dict[str, dict[str, Any]] = {}
        for label, task in macro_tasks.items():
            try:
                source_symbol, data = await task
                macro[label] = data
            except Exception as exc:
                macro[label] = {"status": f"unavailable:{type(exc).__name__}"}

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

        return {
            "snapshot_time_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "top100": top100,
            "global_crypto": global_summary,
            "pre_screen_long": long_pre,
            "pre_screen_short": short_pre,
            "crypto_technicals": spot_details,
            "crypto_derivatives": derivatives,
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


def _history_bars_from_yahoo(payload: dict[str, Any], now_ts: float) -> list[dict[str, Any]]:
    rows = _yahoo_rows(payload)
    bars: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        open_time = float(row[0]) / 1000.0
        close_time = open_time + 3600.0
        bars.append({
            "open_time": open_time,
            "close_time": close_time,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            # Yahoo may expose the current partial hourly candle. Treat it as
            # closed only after its nominal hour has elapsed.
            "closed": close_time <= now_ts,
        })
    return bars


async def fetch_signal_histories(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Fetch lightweight 1H history for v008 signal statistics.

    Crypto outcomes are measured on Binance Spot only. Commodity outcomes use
    the exact Yahoo symbol that generated the published setup. No Futures API is
    involved. The history starts from the signal's issue hour and is capped by
    the 14-day strategy horizon.
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
        "User-Agent": "Mozilla/5.0 (compatible; CoolifyTradingSignalBot/v008)",
    }
    timeout = httpx.Timeout(HTTP_TIMEOUT, connect=min(10.0, HTTP_TIMEOUT))
    sem = asyncio.Semaphore(6)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        async def fetch_one(record: dict[str, Any]) -> tuple[str, list[dict[str, Any]] | None]:
            record_id = str(record["id"])
            issued_at = float(record.get("issued_at") or now_ts)
            start_hour = int(issued_at // 3600) * 3600
            source_symbol = str(record.get("source_symbol") or "").strip()
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

                    payload = await _get_json(
                        client,
                        f"{YAHOO_CHART_BASE}/{source_symbol}",
                        params={
                            "interval": "1h",
                            "period1": start_hour,
                            "period2": int(now_ts) + 3600,
                            "includePrePost": "false",
                            "events": "div,splits",
                        },
                        attempts=2,
                    )
                    bars = [bar for bar in _history_bars_from_yahoo(payload, now_ts) if bar["open_time"] >= start_hour]
                    return record_id, bars
                except Exception as exc:
                    log.info("Signal history unavailable id=%s asset=%s: %s", record_id, record.get("asset"), exc)
                    return record_id, None

        pairs = await asyncio.gather(*(fetch_one(record) for record in open_records))
        return {record_id: bars for record_id, bars in pairs if bars is not None}
