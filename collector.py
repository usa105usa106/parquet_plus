from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import shutil
import time
import zipfile
from collections import deque
from statistics import median
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from config import Settings
from asset_filters import is_excluded_asset

log = logging.getLogger(__name__)

# Stablecoin/wrapped filtering is centralized in asset_filters.py.

# Market-cap provider aliases -> Binance Spot base asset.
BASE_ALIASES = {
    "MATIC": "POL",
}

COMMODITY_CONTRACTS = {
    "XAU": "XAU_USDT",
    "XAG": "SILVER_USDT",
    "USOIL": "USOIL_USDT",
}


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_iso(ms_or_sec: int | float | None, *, ms: bool = True) -> str | None:
    if ms_or_sec is None:
        return None
    try:
        value = float(ms_or_sec)
        if ms:
            value /= 1000.0
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _closed_interval_start_ms(interval_ms: int, now_ms: int | None = None) -> int:
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    return (now_ms // interval_ms) * interval_ms


def _closed_hour_start_ms(now_ms: int | None = None) -> int:
    return _closed_interval_start_ms(60 * 60 * 1000, now_ms)


class RequestRateLimiter:
    """Simple rolling-window limiter for MEXC public endpoints.

    MEXC documents 20 requests / 2 seconds for the funding-history and kline
    endpoints. We deliberately stay below that with 8 starts / second.
    """

    def __init__(self, max_calls: int = 8, period: float = 1.0):
        self.max_calls = max_calls
        self.period = period
        self._times: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._times and now - self._times[0] >= self.period:
                    self._times.popleft()
                if len(self._times) < self.max_calls:
                    self._times.append(now)
                    return
                delay = self.period - (now - self._times[0]) + 0.01
            await asyncio.sleep(max(0.01, delay))


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
    limiter: RequestRateLimiter | None = None,
) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        if limiter is not None:
            await limiter.wait()
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last = exc
            if attempt + 1 >= attempts:
                break
            delay = 0.45 * (2**attempt)
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {418, 429}:
                retry_after = _retry_after_seconds(exc.response)
                if retry_after is not None:
                    # Never ignore Binance's requested backoff. For a long IP ban,
                    # fail this build instead of keeping a Telegram task blocked for minutes/hours.
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
    assert last is not None
    raise last


async def _fetch_marketcap_candidates(
    client: httpx.AsyncClient,
    settings: Settings,
    target_count: int,
) -> tuple[list[dict[str, Any]], str]:
    """Get enough ranked assets to fill the requested Binance-Spot-eligible universe."""
    # Top-100 keeps the original single-page CoinGecko request. Larger universes
    # need a deeper market-cap pool because stable/wrapped assets and coins without
    # a TRADING Binance Spot USDT pair are excluded later.
    candidate_target = 250 if target_count <= 100 else 500 if target_count <= 200 else 1000
    try:
        market_rows: list[dict[str, Any]] = []
        pages = max(1, (candidate_target + 249) // 250)
        for page in range(1, pages + 1):
            rows = await _get_json(
                client,
                f"{settings.coingecko_base_url}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 250,
                    "page": page,
                    "sparkline": "false",
                    "price_change_percentage": "24h,7d",
                },
                attempts=2,
            )
            if not isinstance(rows, list):
                raise RuntimeError("CoinGecko universe response is not a list")
            market_rows.extend(row for row in rows if isinstance(row, dict))
            if len(rows) < 250:
                break
        if len(market_rows) >= target_count:
            result: list[dict[str, Any]] = []
            for row in market_rows:
                result.append({
                    "rank": _int(row.get("market_cap_rank")),
                    "symbol": str(row.get("symbol") or "").upper(),
                    "name": str(row.get("name") or ""),
                    "provider_id": row.get("id"),
                    "provider_price": _num(row.get("current_price")),
                    "market_cap": _num(row.get("market_cap")),
                    "fully_diluted_valuation": _num(row.get("fully_diluted_valuation")),
                    "provider_volume_24h": _num(row.get("total_volume")),
                    "change_24h_pct": _num(row.get("price_change_percentage_24h")),
                    "change_7d_pct": _num(row.get("price_change_percentage_7d_in_currency")),
                    "circulating_supply": _num(row.get("circulating_supply")),
                    "total_supply": _num(row.get("total_supply")),
                    "max_supply": _num(row.get("max_supply")),
                })
            return result, "coingecko"
    except Exception as exc:  # noqa: BLE001
        log.warning("CoinGecko universe unavailable, falling back to CoinPaprika: %s", exc)

    rows = await _get_json(client, f"{settings.coinpaprika_base_url}/tickers", params={"quotes": "USD"})
    if not isinstance(rows, list):
        raise RuntimeError("CoinPaprika universe response is not a list")
    result = []
    for row in sorted(rows, key=lambda x: int(x.get("rank") or 999999)):
        q = (row.get("quotes") or {}).get("USD") or {}
        price = _num(q.get("price"))
        total_supply = _num(row.get("total_supply"))
        max_supply = _num(row.get("max_supply"))
        fdv_supply = max_supply or total_supply
        result.append({
            "rank": _int(row.get("rank")),
            "symbol": str(row.get("symbol") or "").upper(),
            "name": str(row.get("name") or ""),
            "provider_id": row.get("id"),
            "provider_price": price,
            "market_cap": _num(q.get("market_cap")),
            "fully_diluted_valuation": (price * fdv_supply) if price is not None and fdv_supply is not None else None,
            "provider_volume_24h": _num(q.get("volume_24h")),
            "change_24h_pct": _num(q.get("percent_change_24h")),
            "change_7d_pct": _num(q.get("percent_change_7d")),
            "circulating_supply": _num(row.get("circulating_supply")),
            "total_supply": total_supply,
            "max_supply": max_supply,
        })
    return result, "coinpaprika_fallback"


def _spot_pairs(exchange_info: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in exchange_info.get("symbols") or []:
        if not isinstance(row, dict):
            continue
        if row.get("status") != "TRADING" or row.get("quoteAsset") != "USDT":
            continue
        if row.get("isSpotTradingAllowed") is False:
            continue
        base = str(row.get("baseAsset") or "").upper()
        symbol = str(row.get("symbol") or "").upper()
        if base and symbol:
            result.setdefault(base, symbol)
    return result


def _select_top_assets(
    candidates: list[dict[str, Any]],
    spot_pairs: dict[str, str],
    tickers: dict[str, dict[str, Any]],
    top_limit: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_pairs: set[str] = set()
    for coin in sorted(candidates, key=lambda x: int(x.get("rank") or 999999)):
        symbol = str(coin.get("symbol") or "").upper()
        if not symbol or is_excluded_asset(symbol, str(coin.get("name") or ""), coin.get("provider_id")):
            continue
        base = BASE_ALIASES.get(symbol, symbol)
        pair = spot_pairs.get(base)
        if not pair or pair in seen_pairs:
            continue
        ticker = tickers.get(pair) or {}
        item = dict(coin)
        item.update({
            "analysis_rank": len(selected) + 1,
            "binance_base_asset": base,
            "binance_symbol": pair,
            "binance_last_price": _num(ticker.get("lastPrice")),
            "binance_quote_volume_24h": _num(ticker.get("quoteVolume")),
            "binance_base_volume_24h": _num(ticker.get("volume")),
            "binance_trade_count_24h": _int(ticker.get("count")),
            "binance_price_change_24h_pct": _num(ticker.get("priceChangePercent")),
            "binance_high_24h": _num(ticker.get("highPrice")),
            "binance_low_24h": _num(ticker.get("lowPrice")),
        })
        selected.append(item)
        seen_pairs.add(pair)
        if len(selected) == top_limit:
            break
    if len(selected) < top_limit:
        raise RuntimeError(f"Could not build {top_limit} eligible Binance Spot assets; got {len(selected)}")
    return selected


async def _fetch_binance_ohlcv_interval(
    client: httpx.AsyncClient,
    settings: Settings,
    universe: list[dict[str, Any]],
    *,
    interval: str,
    limit: int,
    cutoff_ms: int,
    sem: asyncio.Semaphore,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    all_rows: list[dict[str, Any]] = []
    statuses: dict[str, dict[str, Any]] = {}

    async def one(asset: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        symbol = str(asset["symbol"])
        pair = str(asset["binance_symbol"])
        async with sem:
            try:
                payload = await _get_json(
                    client,
                    f"{settings.binance_spot_base_url}/api/v3/klines",
                    params={
                        "symbol": pair,
                        "interval": interval,
                        "limit": limit,
                        "endTime": cutoff_ms - 1,
                    },
                    attempts=3,
                )
                if not isinstance(payload, list):
                    raise RuntimeError("klines response is not a list")
                rows: list[dict[str, Any]] = []
                for k in payload:
                    if not isinstance(k, list) or len(k) < 11:
                        continue
                    open_ms = int(k[0])
                    if open_ms >= cutoff_ms:
                        continue
                    rows.append({
                        "asset": symbol,
                        "binance_symbol": pair,
                        "timeframe": interval,
                        "timestamp_ms": open_ms,
                        "timestamp_utc": _utc_iso(open_ms),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume_base": float(k[5]),
                        "close_time_ms": int(k[6]),
                        "quote_volume": float(k[7]),
                        "trade_count": int(k[8]),
                        "taker_buy_base_volume": float(k[9]),
                        "taker_buy_quote_volume": float(k[10]),
                    })
                rows.sort(key=lambda r: r["timestamp_ms"])
                rows = rows[-limit:]
                status = {
                    "source": "binance_spot",
                    "pair": pair,
                    "timeframe": interval,
                    "status": "OK" if len(rows) == limit else "PARTIAL_HISTORY",
                    "candles": len(rows),
                    "requested": limit,
                    "first_utc": rows[0]["timestamp_utc"] if rows else None,
                    "last_utc": rows[-1]["timestamp_utc"] if rows else None,
                }
                return symbol, rows, status
            except Exception as exc:  # noqa: BLE001
                return symbol, [], {
                    "source": "binance_spot",
                    "pair": pair,
                    "timeframe": interval,
                    "status": f"ERROR:{type(exc).__name__}",
                    "error": str(exc)[:300],
                    "candles": 0,
                    "requested": limit,
                }

    results = await asyncio.gather(*(one(asset) for asset in universe))
    for symbol, rows, status in results:
        all_rows.extend(rows)
        statuses[symbol] = status
    all_rows.sort(key=lambda r: (r["asset"], r["timestamp_ms"]))
    return all_rows, statuses


def _depth_band_sums(levels: list[Any], mid: float, pct: float, *, is_bid: bool) -> tuple[float, float, int]:
    base_sum = 0.0
    quote_sum = 0.0
    count = 0
    boundary = mid * (1.0 - pct) if is_bid else mid * (1.0 + pct)
    for level in levels:
        if not isinstance(level, list) or len(level) < 2:
            continue
        price = _num(level[0])
        qty = _num(level[1])
        if price is None or qty is None:
            continue
        inside = price >= boundary if is_bid else price <= boundary
        if not inside:
            continue
        base_sum += qty
        quote_sum += price * qty
        count += 1
    return base_sum, quote_sum, count


async def _fetch_binance_spot_liquidity(
    client: httpx.AsyncClient,
    settings: Settings,
    universe: list[dict[str, Any]],
    sem: asyncio.Semaphore,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    statuses: dict[str, dict[str, Any]] = {}

    async def one(asset: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        symbol = str(asset["symbol"])
        pair = str(asset["binance_symbol"])
        captured_ms = int(time.time() * 1000)
        async with sem:
            try:
                payload = await _get_json(
                    client,
                    f"{settings.binance_spot_base_url}/api/v3/depth",
                    params={"symbol": pair, "limit": settings.binance_depth_limit},
                    attempts=3,
                )
                bids = payload.get("bids") if isinstance(payload, dict) else None
                asks = payload.get("asks") if isinstance(payload, dict) else None
                if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
                    raise RuntimeError("depth response has no bids/asks")
                best_bid = _num(bids[0][0])
                best_ask = _num(asks[0][0])
                if best_bid is None or best_ask is None or best_bid <= 0 or best_ask <= 0:
                    raise RuntimeError("invalid best bid/ask")
                mid = (best_bid + best_ask) / 2.0
                row: dict[str, Any] = {
                    "asset": symbol,
                    "binance_symbol": pair,
                    "snapshot_timestamp_ms": captured_ms,
                    "snapshot_timestamp_utc": _utc_iso(captured_ms),
                    "last_update_id": _int(payload.get("lastUpdateId")),
                    "depth_limit_requested": settings.binance_depth_limit,
                    "levels_bid_returned": len(bids),
                    "levels_ask_returned": len(asks),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "mid_price": mid,
                    "spread_bps": (best_ask - best_bid) / mid * 10000.0,
                }
                for label, pct in (("0_1pct", 0.001), ("0_5pct", 0.005), ("1_0pct", 0.01)):
                    bbase, bquote, bcount = _depth_band_sums(bids, mid, pct, is_bid=True)
                    abase, aquote, acount = _depth_band_sums(asks, mid, pct, is_bid=False)
                    total = bquote + aquote
                    row[f"bid_depth_base_{label}"] = bbase
                    row[f"ask_depth_base_{label}"] = abase
                    row[f"bid_depth_quote_{label}"] = bquote
                    row[f"ask_depth_quote_{label}"] = aquote
                    row[f"bid_levels_{label}"] = bcount
                    row[f"ask_levels_{label}"] = acount
                    row[f"imbalance_{label}"] = ((bquote - aquote) / total) if total > 0 else None
                lowest_bid = _num(bids[-1][0])
                highest_ask = _num(asks[-1][0])
                row["bid_coverage_pct"] = ((mid - lowest_bid) / mid * 100.0) if lowest_bid is not None else None
                row["ask_coverage_pct"] = ((highest_ask - mid) / mid * 100.0) if highest_ask is not None else None
                status = {
                    "source": "binance_spot_depth",
                    "pair": pair,
                    "status": "OK",
                    "levels_bid": len(bids),
                    "levels_ask": len(asks),
                    "spread_bps": row["spread_bps"],
                    "bid_coverage_pct": row["bid_coverage_pct"],
                    "ask_coverage_pct": row["ask_coverage_pct"],
                }
                return symbol, row, status
            except Exception as exc:  # noqa: BLE001
                return symbol, {
                    "asset": symbol,
                    "binance_symbol": pair,
                    "snapshot_timestamp_ms": captured_ms,
                    "snapshot_timestamp_utc": _utc_iso(captured_ms),
                    "status": f"ERROR:{type(exc).__name__}",
                }, {
                    "source": "binance_spot_depth",
                    "pair": pair,
                    "status": f"ERROR:{type(exc).__name__}",
                    "error": str(exc)[:300],
                }

    results = await asyncio.gather(*(one(asset) for asset in universe))
    for symbol, row, status in results:
        rows.append(row)
        statuses[symbol] = status
    rows.sort(key=lambda r: r["asset"])
    return rows, statuses


def _safe_median(values: list[float]) -> float | None:
    clean = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return float(median(clean)) if clean else None


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    chunk = values[-length:]
    return sum(chunk) / length


def _build_market_breadth(universe: list[dict[str, Any]], daily_rows: list[dict[str, Any]], generated_ms: int) -> list[dict[str, Any]]:
    by_asset: dict[str, list[dict[str, Any]]] = {}
    for row in daily_rows:
        by_asset.setdefault(str(row["asset"]), []).append(row)
    for rows in by_asset.values():
        rows.sort(key=lambda r: r["timestamp_ms"])

    above: dict[int, int] = {20: 0, 50: 0, 200: 0}
    coverage: dict[int, int] = {20: 0, 50: 0, 200: 0}
    adv = dec = flat = 0
    daily_returns: list[float] = []
    highs20 = lows20 = 0
    for asset in universe:
        symbol = str(asset["symbol"])
        rows = by_asset.get(symbol) or []
        closes = [float(r["close"]) for r in rows if _num(r.get("close")) is not None]
        if len(closes) >= 2 and closes[-2] != 0:
            ret = (closes[-1] / closes[-2] - 1.0) * 100.0
            daily_returns.append(ret)
            if ret > 0.000001:
                adv += 1
            elif ret < -0.000001:
                dec += 1
            else:
                flat += 1
        for length in (20, 50, 200):
            avg = _sma(closes, length)
            if avg is not None:
                coverage[length] += 1
                if closes[-1] > avg:
                    above[length] += 1
        if len(rows) >= 20:
            recent = rows[-20:]
            last_close = float(recent[-1]["close"])
            prior_high = max(float(r["high"]) for r in recent[:-1]) if len(recent) > 1 else last_close
            prior_low = min(float(r["low"]) for r in recent[:-1]) if len(recent) > 1 else last_close
            if last_close > prior_high:
                highs20 += 1
            if last_close < prior_low:
                lows20 += 1

    c24 = [_num(r.get("change_24h_pct")) for r in universe]
    c7 = [_num(r.get("change_7d_pct")) for r in universe]
    c24v = [v for v in c24 if v is not None]
    c7v = [v for v in c7 if v is not None]
    btc = next((r for r in universe if str(r.get("symbol")) == "BTC"), {})
    btc24 = _num(btc.get("change_24h_pct"))
    btc7 = _num(btc.get("change_7d_pct"))
    out24 = sum(1 for v in c24v if btc24 is not None and v > btc24)
    out7 = sum(1 for v in c7v if btc7 is not None and v > btc7)
    total = len(universe)
    return [{
        "snapshot_timestamp_ms": generated_ms,
        "snapshot_timestamp_utc": _utc_iso(generated_ms),
        "assets_total": total,
        "assets_with_daily_return": adv + dec + flat,
        "advancers_1d": adv,
        "decliners_1d": dec,
        "unchanged_1d": flat,
        "advance_decline_ratio": (adv / dec) if dec else (float(adv) if adv else None),
        "median_latest_1d_return_pct": _safe_median(daily_returns),
        "positive_24h_count": sum(1 for v in c24v if v > 0),
        "positive_24h_pct": (sum(1 for v in c24v if v > 0) / len(c24v) * 100.0) if c24v else None,
        "median_change_24h_pct": _safe_median(c24v),
        "positive_7d_count": sum(1 for v in c7v if v > 0),
        "positive_7d_pct": (sum(1 for v in c7v if v > 0) / len(c7v) * 100.0) if c7v else None,
        "median_change_7d_pct": _safe_median(c7v),
        "btc_change_24h_pct": btc24,
        "btc_change_7d_pct": btc7,
        "outperform_btc_24h_count": out24 if btc24 is not None else None,
        "outperform_btc_24h_pct": (out24 / len(c24v) * 100.0) if btc24 is not None and c24v else None,
        "outperform_btc_7d_count": out7 if btc7 is not None else None,
        "outperform_btc_7d_pct": (out7 / len(c7v) * 100.0) if btc7 is not None and c7v else None,
        "above_sma20_1d_count": above[20],
        "above_sma20_1d_coverage": coverage[20],
        "above_sma20_1d_pct": (above[20] / coverage[20] * 100.0) if coverage[20] else None,
        "above_sma50_1d_count": above[50],
        "above_sma50_1d_coverage": coverage[50],
        "above_sma50_1d_pct": (above[50] / coverage[50] * 100.0) if coverage[50] else None,
        "above_sma200_1d_count": above[200],
        "above_sma200_1d_coverage": coverage[200],
        "above_sma200_1d_pct": (above[200] / coverage[200] * 100.0) if coverage[200] else None,
        "new_20d_high_count": highs20,
        "new_20d_low_count": lows20,
    }]

def _mexc_ticker_list(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        return []
    data = payload.get("data") or []
    if isinstance(data, dict):
        return [data]
    return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []


def _mexc_contract_map(tickers: list[dict[str, Any]], wanted: set[str]) -> dict[str, str]:
    exact: dict[str, str] = {}
    multiplied: dict[str, str] = {}
    for row in tickers:
        contract = str(row.get("symbol") or "").upper()
        if not contract.endswith("_USDT"):
            continue
        base = contract[:-5]
        if base in wanted:
            exact[base] = contract
            continue
        for prefix in ("1000000", "10000", "1000"):
            if base.startswith(prefix) and base[len(prefix) :] in wanted:
                multiplied.setdefault(base[len(prefix) :], contract)
                break
    result = dict(multiplied)
    result.update(exact)
    return result



def _build_mexc_derivatives(
    universe: list[dict[str, Any]],
    all_tickers: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    wanted = {str(row["symbol"]).upper() for row in universe}
    contract_map = _mexc_contract_map(all_tickers, wanted)
    ticker_by_contract = {str(row.get("symbol") or "").upper(): row for row in all_tickers}
    rows: list[dict[str, Any]] = []
    statuses: dict[str, dict[str, Any]] = {}
    captured_ms = int(time.time() * 1000)
    for asset in universe:
        symbol = str(asset["symbol"]).upper()
        contract = contract_map.get(symbol)
        if not contract:
            rows.append({
                "asset": symbol,
                "mexc_contract": None,
                "snapshot_timestamp_ms": captured_ms,
                "snapshot_timestamp_utc": _utc_iso(captured_ms),
                "status": "NOT_ON_MEXC",
            })
            statuses[symbol] = {"status": "NOT_ON_MEXC", "contract": None}
            continue
        ticker = ticker_by_contract.get(contract) or {}
        last_price = _num(ticker.get("lastPrice"))
        fair_price = _num(ticker.get("fairPrice"))
        index_price = _num(ticker.get("indexPrice"))
        bid = _num(ticker.get("bid1"))
        ask = _num(ticker.get("ask1"))
        row = {
            "asset": symbol,
            "mexc_contract": contract,
            "snapshot_timestamp_ms": _int(ticker.get("timestamp")) or captured_ms,
            "snapshot_timestamp_utc": _utc_iso(_int(ticker.get("timestamp")) or captured_ms),
            "last_price": last_price,
            "fair_price": fair_price,
            "index_price": index_price,
            "basis_last_vs_index_pct": ((last_price / index_price - 1.0) * 100.0) if last_price is not None and index_price not in (None, 0) else None,
            "basis_fair_vs_index_pct": ((fair_price / index_price - 1.0) * 100.0) if fair_price is not None and index_price not in (None, 0) else None,
            "funding_rate": _num(ticker.get("fundingRate")),
            "funding_rate_pct": (_num(ticker.get("fundingRate")) * 100.0) if _num(ticker.get("fundingRate")) is not None else None,
            "open_interest_hold_vol": _num(ticker.get("holdVol")),
            "volume_24h_contract_units": _num(ticker.get("volume24")),
            "amount_24h": _num(ticker.get("amount24")),
            "bid1": bid,
            "ask1": ask,
            "spread_bps": ((ask - bid) / ((ask + bid) / 2.0) * 10000.0) if bid not in (None, 0) and ask not in (None, 0) else None,
            "change_24h_pct": (_num(ticker.get("riseFallRate")) * 100.0) if _num(ticker.get("riseFallRate")) is not None else None,
            "status": "OK",
        }
        rows.append(row)
        statuses[symbol] = {
            "status": "OK",
            "contract": contract,
            "open_interest_hold_vol": row["open_interest_hold_vol"],
            "basis_last_vs_index_pct": row["basis_last_vs_index_pct"],
        }
    rows.sort(key=lambda r: r["asset"])
    return rows, statuses

async def _fetch_mexc_funding(
    client: httpx.AsyncClient,
    settings: Settings,
    universe: list[dict[str, Any]],
    all_tickers: list[dict[str, Any]],
    limiter: RequestRateLimiter,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    wanted = {str(row["symbol"]).upper() for row in universe}
    contract_map = _mexc_contract_map(all_tickers, wanted)
    ticker_by_contract = {str(row.get("symbol") or "").upper(): row for row in all_tickers}
    sem = asyncio.Semaphore(8)
    rows_out: list[dict[str, Any]] = []
    statuses: dict[str, dict[str, Any]] = {}

    async def fetch_contract(
        symbol: str, contract: str
    ) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]], str | None, str | None]:
        current_data: dict[str, Any] | None = None
        current_error: str | None = None
        history_records: list[dict[str, Any]] = []
        history_error: str | None = None
        async with sem:
            try:
                payload = await _get_json(
                    client,
                    f"{settings.mexc_contract_base_url}/api/v1/contract/funding_rate/{contract}",
                    attempts=2,
                    limiter=limiter,
                )
                data = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(data, dict):
                    raise RuntimeError("bad_current_funding_response")
                current_data = data
            except Exception as exc:  # noqa: BLE001
                current_error = f"{type(exc).__name__}:{str(exc)[:180]}"

            if settings.funding_history_count > 0:
                try:
                    payload = await _get_json(
                        client,
                        f"{settings.mexc_contract_base_url}/api/v1/contract/funding_rate/history",
                        params={"symbol": contract, "page_num": 1, "page_size": settings.funding_history_count},
                        attempts=2,
                        limiter=limiter,
                    )
                    data = payload.get("data") if isinstance(payload, dict) else None
                    result_list = (data or {}).get("resultList") if isinstance(data, dict) else None
                    if not isinstance(result_list, list):
                        raise RuntimeError("bad_history_response")
                    for item in result_list[: settings.funding_history_count]:
                        if not isinstance(item, dict):
                            continue
                        settle_ms = _int(item.get("settleTime"))
                        hist_rate = _num(item.get("fundingRate"))
                        history_records.append({
                            "asset": symbol,
                            "mexc_contract": contract,
                            "record_type": "history",
                            "funding_rate": hist_rate,
                            "funding_rate_pct": hist_rate * 100 if hist_rate is not None else None,
                            "funding_rate_source": "mexc_funding_history_endpoint",
                            "settle_time_ms": settle_ms,
                            "settle_time_utc": _utc_iso(settle_ms),
                            "next_settle_time_ms": None,
                            "next_settle_time_utc": None,
                            "collect_cycle_hours": None,
                            "fair_price": None,
                            "index_price": None,
                            "status": "OK",
                        })
                except Exception as exc:  # noqa: BLE001
                    history_error = f"{type(exc).__name__}:{str(exc)[:180]}"

        return symbol, current_data, history_records, current_error, history_error

    tasks: list[asyncio.Task[Any]] = []
    for asset in universe:
        symbol = str(asset["symbol"]).upper()
        contract = contract_map.get(symbol)
        if not contract:
            rows_out.append({
                "asset": symbol,
                "mexc_contract": None,
                "record_type": "current",
                "funding_rate": None,
                "funding_rate_pct": None,
                "funding_rate_source": None,
                "settle_time_ms": None,
                "settle_time_utc": None,
                "next_settle_time_ms": None,
                "next_settle_time_utc": None,
                "collect_cycle_hours": None,
                "fair_price": None,
                "index_price": None,
                "status": "NOT_ON_MEXC",
            })
            statuses[symbol] = {
                "status": "NOT_ON_MEXC", "contract": None, "history_records": 0,
                "current_metadata_status": "NOT_ON_MEXC",
            }
            continue
        tasks.append(asyncio.create_task(fetch_contract(symbol, contract)))

    if tasks:
        results = await asyncio.gather(*tasks)
        for symbol, current_data, history_records, current_error, history_error in results:
            contract = contract_map[symbol]
            ticker = ticker_by_contract.get(contract) or {}
            endpoint_rate = _num((current_data or {}).get("fundingRate"))
            ticker_rate = _num(ticker.get("fundingRate"))
            rate = endpoint_rate if endpoint_rate is not None else ticker_rate
            rate_source = (
                "mexc_funding_rate_endpoint" if endpoint_rate is not None
                else "mexc_contract_ticker_fallback" if ticker_rate is not None
                else None
            )
            next_settle = _int((current_data or {}).get("nextSettleTime"))
            collect_cycle = _int((current_data or {}).get("collectCycle"))
            current_status = "OK" if rate is not None else "CURRENT_UNAVAILABLE"
            metadata_status = "OK" if current_data is not None else "UNAVAILABLE"
            rows_out.append({
                "asset": symbol,
                "mexc_contract": contract,
                "record_type": "current",
                "funding_rate": rate,
                "funding_rate_pct": rate * 100 if rate is not None else None,
                "funding_rate_source": rate_source,
                "settle_time_ms": None,
                "settle_time_utc": None,
                "next_settle_time_ms": next_settle,
                "next_settle_time_utc": _utc_iso(next_settle),
                "collect_cycle_hours": collect_cycle,
                "fair_price": _num(ticker.get("fairPrice")),
                "index_price": _num(ticker.get("indexPrice")),
                "status": current_status,
            })
            rows_out.extend(history_records)
            statuses[symbol] = {
                "status": current_status,
                "contract": contract,
                "history_records": len(history_records),
                "current_metadata_status": metadata_status,
                "funding_rate_source": rate_source,
                "next_settle_time_ms": next_settle,
                "collect_cycle_hours": collect_cycle,
            }
            if current_error:
                statuses[symbol]["current_metadata_error"] = current_error
            if history_error:
                statuses[symbol]["history_error"] = history_error
                if statuses[symbol]["status"] == "OK":
                    statuses[symbol]["status"] = "HISTORY_UNAVAILABLE"

    rows_out.sort(key=lambda r: (r["asset"], 0 if r["record_type"] == "current" else 1, r.get("settle_time_ms") or 0))
    return rows_out, statuses


async def _fetch_mexc_commodities_interval(
    client: httpx.AsyncClient,
    settings: Settings,
    *,
    interval: str,
    limit: int,
    cutoff_ms: int,
    all_tickers: list[dict[str, Any]],
    limiter: RequestRateLimiter,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    ticker_by_contract = {str(row.get("symbol") or "").upper(): row for row in all_tickers}
    rows_out: list[dict[str, Any]] = []
    statuses: dict[str, dict[str, Any]] = {}

    async def one(asset: str, contract: str) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        try:
            payload = await _get_json(
                client,
                f"{settings.mexc_contract_base_url}/api/v1/contract/kline/{contract}",
                params={"interval": interval, "end": int(cutoff_ms // 1000) - 1},
                attempts=3,
                limiter=limiter,
            )
            if not isinstance(payload, dict) or payload.get("success") is not True:
                raise RuntimeError(f"bad response: {str(payload)[:160]}")
            data = payload.get("data") or {}
            times = data.get("time") or []
            opens = data.get("open") or []
            closes = data.get("close") or []
            highs = data.get("high") or []
            lows = data.get("low") or []
            vols = data.get("vol") or []
            amounts = data.get("amount") or []
            records: list[dict[str, Any]] = []
            for i, ts in enumerate(times):
                try:
                    open_ms = int(ts) * 1000
                    if open_ms >= cutoff_ms:
                        continue
                    records.append({
                        "asset": asset,
                        "mexc_contract": contract,
                        "timeframe": interval,
                        "timestamp_ms": open_ms,
                        "timestamp_utc": _utc_iso(open_ms),
                        "open": float(opens[i]),
                        "high": float(highs[i]),
                        "low": float(lows[i]),
                        "close": float(closes[i]),
                        "volume": float(vols[i]) if i < len(vols) and vols[i] is not None else None,
                        "amount": float(amounts[i]) if i < len(amounts) and amounts[i] is not None else None,
                    })
                except (IndexError, TypeError, ValueError):
                    continue
            records.sort(key=lambda r: r["timestamp_ms"])
            records = records[-limit:]
            ticker = ticker_by_contract.get(contract) or {}
            status = {
                "source": "mexc_futures",
                "contract": contract,
                "timeframe": interval,
                "status": "OK" if len(records) == limit else "PARTIAL_HISTORY",
                "candles": len(records),
                "requested": limit,
                "first_utc": records[0]["timestamp_utc"] if records else None,
                "last_utc": records[-1]["timestamp_utc"] if records else None,
                "last_price": _num(ticker.get("lastPrice")),
                "fair_price": _num(ticker.get("fairPrice")),
                "index_price": _num(ticker.get("indexPrice")),
            }
            return asset, records, status
        except Exception as exc:  # noqa: BLE001
            return asset, [], {
                "source": "mexc_futures",
                "contract": contract,
                "timeframe": interval,
                "status": f"ERROR:{type(exc).__name__}",
                "error": str(exc)[:300],
                "candles": 0,
                "requested": limit,
            }

    results = await asyncio.gather(*(one(asset, contract) for asset, contract in COMMODITY_CONTRACTS.items()))
    for asset, rows, status in results:
        rows_out.extend(rows)
        statuses[asset] = status
    rows_out.sort(key=lambda r: (r["asset"], r["timestamp_ms"]))
    return rows_out, statuses


def _option_atm_iv(rows: list[dict[str, Any]], target_days: int) -> tuple[float | None, int | None]:
    usable = [r for r in rows if _num(r.get("mark_iv")) is not None and _num(r.get("strike")) is not None and _num(r.get("underlying_price")) not in (None, 0) and _num(r.get("days_to_expiry")) is not None and float(r["days_to_expiry"]) > 0]
    if not usable:
        return None, None
    expiries = sorted({int(r["expiration_timestamp_ms"]) for r in usable if r.get("expiration_timestamp_ms")})
    if not expiries:
        return None, None
    now_ms = int(time.time() * 1000)
    expiry = min(expiries, key=lambda e: abs((e - now_ms) / 86400000.0 - target_days))
    exp_rows = [r for r in usable if int(r.get("expiration_timestamp_ms") or 0) == expiry]
    underlying_vals = [_num(r.get("underlying_price")) for r in exp_rows]
    under = _safe_median([v for v in underlying_vals if v is not None])
    if under in (None, 0):
        return None, expiry
    vals: list[float] = []
    for opt_type in ("call", "put"):
        side = [r for r in exp_rows if r.get("option_type") == opt_type]
        if not side:
            continue
        nearest = min(side, key=lambda r: abs(float(r["strike"]) - float(under)))
        iv = _num(nearest.get("mark_iv"))
        if iv is not None:
            vals.append(iv)
    return (_safe_median(vals), expiry)


async def _fetch_deribit_options(
    client: httpx.AsyncClient,
    settings: Settings,
    now_ms: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Fetch a fresh BTC/ETH options snapshot without making archive creation brittle.

    Deribit is an enrichment layer. If it is temporarily unavailable, the archive
    is still produced and status.json records the failure instead of inventing data.
    """
    option_rows: list[dict[str, Any]] = []
    regime_rows: list[dict[str, Any]] = []
    statuses: dict[str, dict[str, Any]] = {}

    async def fetch_summary(currency: str) -> list[dict[str, Any]]:
        payload = await _get_json(
            client,
            f"{settings.deribit_base_url}/public/get_book_summary_by_currency",
            params={"currency": currency, "kind": "option"},
            attempts=3,
        )
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, list):
            raise RuntimeError("Deribit book summary bad response")
        return [r for r in result if isinstance(r, dict)]

    async def fetch_dvol(currency: str) -> tuple[float | None, int | None, str | None]:
        try:
            payload = await _get_json(
                client,
                f"{settings.deribit_base_url}/public/get_volatility_index_data",
                params={
                    "currency": currency,
                    "start_timestamp": now_ms - 15 * 60 * 1000,
                    "end_timestamp": now_ms,
                    "resolution": "60",
                },
                attempts=2,
            )
            result = payload.get("result") if isinstance(payload, dict) else None
            data = (result or {}).get("data") if isinstance(result, dict) else None
            if not isinstance(data, list) or not data:
                return None, None, "EMPTY"
            last = data[-1]
            if not isinstance(last, list) or len(last) < 5:
                return None, None, "BAD_RESPONSE"
            return _num(last[4]), _int(last[0]), None
        except Exception as exc:  # noqa: BLE001
            return None, None, f"{type(exc).__name__}:{str(exc)[:160]}"

    for idx, currency in enumerate(("BTC", "ETH")):
        if idx:
            # get_instruments has a special sustained rate limit; do not burst it.
            await asyncio.sleep(1.05)
        try:
            instruments_payload = await _get_json(
                client,
                f"{settings.deribit_base_url}/public/get_instruments",
                params={"currency": currency, "kind": "option", "expired": "false"},
                attempts=3,
            )
            summaries, (dvol_value, dvol_ts, dvol_error) = await asyncio.gather(
                fetch_summary(currency), fetch_dvol(currency)
            )
            instruments = instruments_payload.get("result") if isinstance(instruments_payload, dict) else None
            if not isinstance(instruments, list):
                raise RuntimeError("Deribit instruments bad response")
            meta = {
                str(r.get("instrument_name") or ""): r
                for r in instruments
                if isinstance(r, dict)
            }

            asset_rows: list[dict[str, Any]] = []
            for item in summaries:
                name = str(item.get("instrument_name") or "")
                ins = meta.get(name) or {}
                expiry_ms = _int(ins.get("expiration_timestamp"))
                snapshot_ms = _int(item.get("creation_timestamp")) or now_ms
                row = {
                    "asset": currency,
                    "instrument_name": name,
                    "option_type": str(ins.get("option_type") or "") or None,
                    "strike": _num(ins.get("strike")),
                    "expiration_timestamp_ms": expiry_ms,
                    "expiration_timestamp_utc": _utc_iso(expiry_ms),
                    "days_to_expiry": ((expiry_ms - now_ms) / 86400000.0) if expiry_ms is not None else None,
                    "settlement_period": ins.get("settlement_period"),
                    "state": ins.get("state"),
                    "underlying_price": _num(item.get("underlying_price")),
                    "underlying_index": item.get("underlying_index"),
                    "bid_price": _num(item.get("bid_price")),
                    "ask_price": _num(item.get("ask_price")),
                    "mid_price": _num(item.get("mid_price")),
                    "mark_price": _num(item.get("mark_price")),
                    "last_price": _num(item.get("last")),
                    "mark_iv": _num(item.get("mark_iv")),
                    "open_interest": _num(item.get("open_interest")),
                    "volume_24h": _num(item.get("volume")),
                    "volume_usd_24h": _num(item.get("volume_usd")),
                    "interest_rate": _num(item.get("interest_rate")),
                    "summary_timestamp_ms": snapshot_ms,
                    "summary_timestamp_utc": _utc_iso(snapshot_ms),
                }
                asset_rows.append(row)
                option_rows.append(row)

            # If Deribit answered but returned no usable option rows, do not
            # manufacture a zero-filled options regime. Treat this asset as
            # unavailable; if both BTC and ETH are empty, options files are
            # omitted from the ZIP entirely.
            if not asset_rows:
                statuses[currency] = {
                    "status": "EMPTY",
                    "active_options": 0,
                    "dvol": dvol_value,
                    "dvol_status": "OK" if dvol_error is None else dvol_error,
                }
                continue

            call_rows = [r for r in asset_rows if r.get("option_type") == "call"]
            put_rows = [r for r in asset_rows if r.get("option_type") == "put"]
            call_oi = sum(_num(r.get("open_interest")) or 0.0 for r in call_rows)
            put_oi = sum(_num(r.get("open_interest")) or 0.0 for r in put_rows)
            call_vol = sum(_num(r.get("volume_24h")) or 0.0 for r in call_rows)
            put_vol = sum(_num(r.get("volume_24h")) or 0.0 for r in put_rows)
            iv7, exp7 = _option_atm_iv(asset_rows, 7)
            iv30, exp30 = _option_atm_iv(asset_rows, 30)
            iv60, exp60 = _option_atm_iv(asset_rows, 60)
            underlying = _safe_median([v for v in (_num(r.get("underlying_price")) for r in asset_rows) if v is not None])
            regime_rows.append({
                "asset": currency,
                "snapshot_timestamp_ms": now_ms,
                "snapshot_timestamp_utc": _utc_iso(now_ms),
                "underlying_price": underlying,
                "active_option_count": len(asset_rows),
                "call_count": len(call_rows),
                "put_count": len(put_rows),
                "call_open_interest": call_oi,
                "put_open_interest": put_oi,
                "total_open_interest": call_oi + put_oi,
                "put_call_oi_ratio": (put_oi / call_oi) if call_oi > 0 else None,
                "call_volume_24h": call_vol,
                "put_volume_24h": put_vol,
                "total_volume_24h": call_vol + put_vol,
                "put_call_volume_ratio": (put_vol / call_vol) if call_vol > 0 else None,
                "atm_iv_near_7d": iv7,
                "atm_iv_7d_expiration_ms": exp7,
                "atm_iv_near_30d": iv30,
                "atm_iv_30d_expiration_ms": exp30,
                "atm_iv_near_60d": iv60,
                "atm_iv_60d_expiration_ms": exp60,
                "dvol_close": dvol_value,
                "dvol_timestamp_ms": dvol_ts,
                "dvol_timestamp_utc": _utc_iso(dvol_ts),
                "dvol_status": "OK" if dvol_error is None else dvol_error,
            })
            statuses[currency] = {
                "status": "OK",
                "active_options": len(asset_rows),
                "dvol": dvol_value,
                "dvol_status": "OK" if dvol_error is None else dvol_error,
            }
        except Exception as exc:  # noqa: BLE001
            statuses[currency] = {
                "status": "SKIPPED_DERIBIT_UNAVAILABLE",
                "error_type": type(exc).__name__,
                "error": str(exc)[:300],
                "active_options": 0,
                "dvol_status": "UNAVAILABLE",
            }

    option_rows.sort(key=lambda r: (r["asset"], r.get("expiration_timestamp_ms") or 0, r.get("strike") or 0, str(r.get("option_type"))))
    regime_rows.sort(key=lambda r: r["asset"])
    return option_rows, regime_rows, statuses


async def _fetch_deribit_options_optional(
    client: httpx.AsyncClient,
    settings: Settings,
    now_ms: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Best-effort Deribit enrichment. Never fail the market archive.

    BTC/ETH options are useful confirmation data, but they are explicitly optional.
    Any unexpected Deribit/network/parser failure is converted into an unavailable
    status and the rest of the archive continues normally.
    """
    try:
        return await _fetch_deribit_options(client, settings, now_ms)
    except Exception as exc:  # noqa: BLE001
        log.warning("market_scan Deribit options skipped error_type=%s error=%s", type(exc).__name__, str(exc)[:300])
        error_type = type(exc).__name__
        error_text = str(exc)[:300]
        return [], [], {
            "BTC": {
                "status": "SKIPPED_DERIBIT_UNAVAILABLE", "active_options": 0,
                "dvol_status": "UNAVAILABLE", "error_type": error_type, "error": error_text,
            },
            "ETH": {
                "status": "SKIPPED_DERIBIT_UNAVAILABLE", "active_options": 0,
                "dvol_status": "UNAVAILABLE", "error_type": error_type, "error": error_text,
            },
            "_source": {
                "status": "SKIPPED_DERIBIT_UNAVAILABLE",
                "error_type": error_type,
                "error": error_text,
            },
        }


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # A valid empty parquet is more useful than a missing file: downstream
        # analysis can distinguish "source returned no rows" from archive damage.
        table = pa.table({"status": pa.array([], type=pa.string())})
    else:
        table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd", use_dictionary=True)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ArchiveBuildResult:
    archive_path: Path
    archive_size_bytes: int
    archive_sha256: str
    generated_at_utc: str
    generated_at_msk: str
    universe_count: int
    binance_full_count: int
    binance_partial_count: int
    mexc_funding_coverage: int
    commodities_ok_count: int
    duration_seconds: float


async def build_market_archive(settings: Settings, *, progress: Callable[[str], Any] | None = None, top_limit: int = 100) -> ArchiveBuildResult:
    """Build one fresh market snapshot from zero; no candle cache is read or written."""
    if top_limit not in {100, 150, 200, 250, 300}:
        raise ValueError(f"Unsupported top_limit={top_limit}; expected 100, 150, 200, 250 or 300")
    started = time.monotonic()
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    msk = now.astimezone(ZoneInfo(settings.user_timezone))
    hour_start_ms = _closed_interval_start_ms(60 * 60 * 1000, now_ms)
    m15_start_ms = _closed_interval_start_ms(15 * 60 * 1000, now_ms)
    day_start_ms = _closed_interval_start_ms(24 * 60 * 60 * 1000, now_ms)
    stamp = msk.strftime("%Y%m%d_%H%M%S")
    run_dir = settings.work_dir / f"market_scan_{stamp}_{time.time_ns()}"
    run_dir.mkdir(parents=True, exist_ok=False)

    async def say(text: str) -> None:
        if progress is not None:
            value = progress(text)
            if asyncio.iscoroutine(value):
                await value

    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": f"CoolifyMarketScanBot/{settings.app_version}",
    }
    timeout = httpx.Timeout(settings.market_http_timeout, connect=min(10.0, settings.market_http_timeout))

    try:
        await say(f"Получаю top-{top_limit}, Binance Spot и MEXC Futures…")
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            candidates_task = asyncio.create_task(_fetch_marketcap_candidates(client, settings, top_limit))
            exchange_task = asyncio.create_task(_get_json(client, f"{settings.binance_spot_base_url}/api/v3/exchangeInfo"))
            tickers_task = asyncio.create_task(_get_json(client, f"{settings.binance_spot_base_url}/api/v3/ticker/24hr"))
            mexc_tickers_task = asyncio.create_task(_get_json(client, f"{settings.mexc_contract_base_url}/api/v1/contract/ticker", attempts=2))

            (candidates, universe_source), exchange_info, binance_tickers_raw, mexc_tickers_raw = await asyncio.gather(
                candidates_task, exchange_task, tickers_task, mexc_tickers_task
            )
            pairs = _spot_pairs(exchange_info)
            tickers = {
                str(row.get("symbol") or "").upper(): row
                for row in (binance_tickers_raw if isinstance(binance_tickers_raw, list) else [])
                if isinstance(row, dict)
            }
            universe = _select_top_assets(candidates, pairs, tickers, top_limit)
            log.info(
                "market_scan universe selected source=%s assets=%s symbols=%s",
                universe_source,
                len(universe),
                ",".join(str(row.get("symbol") or "") for row in universe),
            )
            mexc_tickers = _mexc_ticker_list(mexc_tickers_raw)
            mexc_derivative_rows, mexc_derivative_status = _build_mexc_derivatives(universe, mexc_tickers)
            log.info(
                "market_scan MEXC derivatives prepared matched=%s requested=%s missing=%s",
                sum(1 for v in mexc_derivative_status.values() if v.get("contract")),
                len(universe),
                ",".join(sorted(k for k, v in mexc_derivative_status.items() if not v.get("contract"))) or "none",
            )

            await say("Скачиваю Binance Spot: 999×1H, 365×1D, 3 дня×15m и стакан…")
            binance_sem = asyncio.Semaphore(16)
            crypto_1h_task = asyncio.create_task(_fetch_binance_ohlcv_interval(
                client, settings, universe, interval="1h", limit=settings.candle_limit,
                cutoff_ms=hour_start_ms, sem=binance_sem,
            ))
            crypto_1d_task = asyncio.create_task(_fetch_binance_ohlcv_interval(
                client, settings, universe, interval="1d", limit=settings.daily_candle_limit,
                cutoff_ms=day_start_ms, sem=binance_sem,
            ))
            crypto_15m_task = asyncio.create_task(_fetch_binance_ohlcv_interval(
                client, settings, universe, interval="15m", limit=settings.m15_candle_limit,
                cutoff_ms=m15_start_ms, sem=binance_sem,
            ))
            liquidity_task = asyncio.create_task(_fetch_binance_spot_liquidity(client, settings, universe, binance_sem))
            (crypto_1h_rows, crypto_1h_status), (crypto_1d_rows, crypto_1d_status), (crypto_15m_rows, crypto_15m_status), (spot_liquidity_rows, spot_liquidity_status) = await asyncio.gather(
                crypto_1h_task, crypto_1d_task, crypto_15m_task, liquidity_task
            )
            log.info(
                "market_scan Binance candles complete rows_1h=%s rows_1d=%s rows_15m=%s full_1h=%s partial_1h=%s full_1d=%s partial_1d=%s full_15m=%s partial_15m=%s",
                len(crypto_1h_rows),
                len(crypto_1d_rows),
                len(crypto_15m_rows),
                sum(1 for v in crypto_1h_status.values() if v.get("status") == "OK"),
                sum(1 for v in crypto_1h_status.values() if v.get("status") == "PARTIAL_HISTORY"),
                sum(1 for v in crypto_1d_status.values() if v.get("status") == "OK"),
                sum(1 for v in crypto_1d_status.values() if v.get("status") == "PARTIAL_HISTORY"),
                sum(1 for v in crypto_15m_status.values() if v.get("status") == "OK"),
                sum(1 for v in crypto_15m_status.values() if v.get("status") == "PARTIAL_HISTORY"),
            )
            for timeframe, tf_status in (("1H", crypto_1h_status), ("1D", crypto_1d_status), ("15m", crypto_15m_status)):
                for symbol, st in sorted(tf_status.items()):
                    if st.get("status") != "OK":
                        log.info(
                            "market_scan Binance history timeframe=%s symbol=%s status=%s candles=%s requested=%s first=%s last=%s error=%s policy=%s",
                            timeframe, symbol, st.get("status"), st.get("candles"), st.get("requested"),
                            st.get("first_utc"), st.get("last_utc"), st.get("error"),
                            "keep_asset_use_all_available" if timeframe == "1D" and st.get("status") == "PARTIAL_HISTORY" else "standard",
                        )
            log.info(
                "market_scan Binance Spot liquidity complete rows=%s ok=%s non_ok=%s",
                len(spot_liquidity_rows),
                sum(1 for v in spot_liquidity_status.values() if v.get("status") == "OK"),
                sum(1 for v in spot_liquidity_status.values() if v.get("status") != "OK"),
            )
            for symbol, st in sorted(spot_liquidity_status.items()):
                if st.get("status") != "OK":
                    log.info(
                        "market_scan Binance Spot liquidity detail symbol=%s status=%s error=%s",
                        symbol, st.get("status"), st.get("error"),
                    )
            breadth_rows = _build_market_breadth(universe, crypto_1d_rows, now_ms)
            if breadth_rows:
                b = breadth_rows[0]
                log.info(
                    "market_scan breadth calculated assets=%s adv=%s dec=%s positive_pct=%s above_sma20_pct=%s above_sma50_pct=%s above_sma200_pct=%s",
                    b.get("assets_total"), b.get("advancers_1d"), b.get("decliners_1d"), b.get("positive_24h_pct"),
                    b.get("above_sma20_1d_pct"), b.get("above_sma50_1d_pct"), b.get("above_sma200_1d_pct"),
                )

            await say("Получаю MEXC funding и XAU/XAG/USOIL на 1H/1D/15m…")
            mexc_limiter = RequestRateLimiter(max_calls=8, period=1.0)
            funding_task = asyncio.create_task(_fetch_mexc_funding(client, settings, universe, mexc_tickers, mexc_limiter))
            commodities_1h_task = asyncio.create_task(_fetch_mexc_commodities_interval(
                client, settings, interval="Min60", limit=settings.candle_limit, cutoff_ms=hour_start_ms,
                all_tickers=mexc_tickers, limiter=mexc_limiter,
            ))
            commodities_1d_task = asyncio.create_task(_fetch_mexc_commodities_interval(
                client, settings, interval="Day1", limit=settings.daily_candle_limit, cutoff_ms=day_start_ms,
                all_tickers=mexc_tickers, limiter=mexc_limiter,
            ))
            commodities_15m_task = asyncio.create_task(_fetch_mexc_commodities_interval(
                client, settings, interval="Min15", limit=settings.m15_candle_limit, cutoff_ms=m15_start_ms,
                all_tickers=mexc_tickers, limiter=mexc_limiter,
            ))
            options_task = asyncio.create_task(_fetch_deribit_options_optional(client, settings, now_ms))
            (funding_rows, funding_status), (commodity_1h_rows, commodity_1h_status), (commodity_1d_rows, commodity_1d_status), (commodity_15m_rows, commodity_15m_status), (option_rows, option_regime_rows, option_status) = await asyncio.gather(
                funding_task, commodities_1h_task, commodities_1d_task, commodities_15m_task, options_task
            )
            log.info(
                "market_scan MEXC funding complete rows=%s matched=%s missing=%s history_requested=%s current_metadata_ok=%s next_settle_present=%s cycle_present=%s",
                len(funding_rows),
                sum(1 for v in funding_status.values() if v.get("contract")),
                ",".join(sorted(k for k, v in funding_status.items() if not v.get("contract"))) or "none",
                settings.funding_history_count,
                sum(1 for v in funding_status.values() if v.get("current_metadata_status") == "OK"),
                sum(1 for v in funding_status.values() if v.get("next_settle_time_ms") is not None),
                sum(1 for v in funding_status.values() if v.get("collect_cycle_hours") is not None),
            )
            for symbol, st in sorted(funding_status.items()):
                if st.get("contract") and (st.get("current_metadata_status") != "OK" or st.get("history_error")):
                    log.info(
                        "market_scan MEXC funding detail symbol=%s status=%s metadata_status=%s source=%s history_records=%s current_metadata_error=%s history_error=%s",
                        symbol, st.get("status"), st.get("current_metadata_status"), st.get("funding_rate_source"),
                        st.get("history_records"), st.get("current_metadata_error"), st.get("history_error"),
                    )
            log.info(
                "market_scan commodities complete rows_1h=%s rows_1d=%s rows_15m=%s status_1h=%s status_1d=%s status_15m=%s",
                len(commodity_1h_rows), len(commodity_1d_rows), len(commodity_15m_rows),
                {k: v.get("status") for k, v in commodity_1h_status.items()},
                {k: v.get("status") for k, v in commodity_1d_status.items()},
                {k: v.get("status") for k, v in commodity_15m_status.items()},
            )
            for timeframe, tf_status in (("1H", commodity_1h_status), ("1D", commodity_1d_status), ("15m", commodity_15m_status)):
                for asset, st in sorted(tf_status.items()):
                    if st.get("status") != "OK":
                        log.info(
                            "market_scan commodity detail timeframe=%s asset=%s contract=%s status=%s candles=%s requested=%s error=%s",
                            timeframe, asset, st.get("contract"), st.get("status"), st.get("candles"), st.get("requested"), st.get("error"),
                        )
            log.info(
                "market_scan Deribit options optional complete raw_rows=%s regime_rows=%s status=%s",
                len(option_rows), len(option_regime_rows),
                {
                    k: {
                        "status": v.get("status"),
                        "active_options": v.get("active_options"),
                        "dvol_status": v.get("dvol_status"),
                        "error_type": v.get("error_type"),
                        "error": v.get("error"),
                    }
                    for k, v in option_status.items()
                },
            )

        for row in universe:
            symbol = str(row["symbol"])
            s1h = crypto_1h_status.get(symbol) or {}
            s1d = crypto_1d_status.get(symbol) or {}
            s15 = crypto_15m_status.get(symbol) or {}
            fstat = funding_status.get(symbol) or {}
            dstat = mexc_derivative_status.get(symbol) or {}
            lstat = spot_liquidity_status.get(symbol) or {}
            row["candle_count_1h"] = int(s1h.get("candles") or 0)
            row["candle_status_1h"] = s1h.get("status")
            row["candle_count_1d"] = int(s1d.get("candles") or 0)
            row["candle_status_1d"] = s1d.get("status")
            row["candle_count_15m"] = int(s15.get("candles") or 0)
            row["candle_status_15m"] = s15.get("status")
            row["binance_depth_status"] = lstat.get("status")
            row["mexc_contract"] = fstat.get("contract") or dstat.get("contract")
            row["mexc_funding_status"] = fstat.get("status")
            row["mexc_funding_history_records"] = int(fstat.get("history_records") or 0)
            row["mexc_derivatives_status"] = dstat.get("status")
            row["mexc_open_interest_hold_vol"] = dstat.get("open_interest_hold_vol")
            row["mexc_basis_last_vs_index_pct"] = dstat.get("basis_last_vs_index_pct")

        await say("Пишу Parquet и собираю ZIP…")
        _write_parquet(run_dir / "crypto_ohlcv_1h.parquet", crypto_1h_rows)
        _write_parquet(run_dir / "crypto_ohlcv_1d.parquet", crypto_1d_rows)
        _write_parquet(run_dir / "crypto_ohlcv_15m.parquet", crypto_15m_rows)
        _write_parquet(run_dir / "commodities_ohlcv_1h.parquet", commodity_1h_rows)
        _write_parquet(run_dir / "commodities_ohlcv_1d.parquet", commodity_1d_rows)
        _write_parquet(run_dir / "commodities_ohlcv_15m.parquet", commodity_15m_rows)
        _write_parquet(run_dir / "funding_mexc.parquet", funding_rows)
        _write_parquet(run_dir / "mexc_derivatives.parquet", mexc_derivative_rows)
        _write_parquet(run_dir / "binance_spot_liquidity.parquet", spot_liquidity_rows)
        _write_parquet(run_dir / "market_breadth.parquet", breadth_rows)
        options_available = bool(option_rows or option_regime_rows)
        if options_available:
            _write_parquet(run_dir / "options_deribit.parquet", option_rows)
            _write_parquet(run_dir / "options_regime.parquet", option_regime_rows)
        _write_parquet(run_dir / "universe.parquet", universe)
        prompt_source = Path(__file__).with_name("PROMPT_FOR_CHATGPT.txt")
        if not prompt_source.is_file():
            raise RuntimeError("PROMPT_FOR_CHATGPT.txt is missing from bot image")
        prompt_text = prompt_source.read_text(encoding="utf-8").replace("top-100", f"top-{top_limit}")
        (run_dir / "PROMPT_FOR_CHATGPT.txt").write_text(prompt_text, encoding="utf-8")

        full_count = sum(1 for v in crypto_1h_status.values() if v.get("status") == "OK")
        partial_count = sum(1 for v in crypto_1h_status.values() if v.get("status") == "PARTIAL_HISTORY")
        full_daily_count = sum(1 for v in crypto_1d_status.values() if v.get("status") == "OK")
        partial_daily_count = sum(1 for v in crypto_1d_status.values() if v.get("status") == "PARTIAL_HISTORY")
        full_15m_count = sum(1 for v in crypto_15m_status.values() if v.get("status") == "OK")
        partial_15m_count = sum(1 for v in crypto_15m_status.values() if v.get("status") == "PARTIAL_HISTORY")
        depth_ok_count = sum(1 for v in spot_liquidity_status.values() if v.get("status") == "OK")
        funding_coverage = sum(1 for v in funding_status.values() if v.get("contract"))
        mexc_derivatives_coverage = sum(1 for v in mexc_derivative_status.values() if v.get("contract"))
        commodities_ok = sum(1 for v in commodity_1h_status.values() if v.get("status") == "OK")
        generated_utc = now.isoformat()
        generated_msk = msk.isoformat()
        options_assets = sorted({str(r.get("asset")) for r in option_rows if r.get("asset")})
        archive_files = [
            "crypto_ohlcv_1h.parquet",
            "crypto_ohlcv_1d.parquet",
            "crypto_ohlcv_15m.parquet",
            "commodities_ohlcv_1h.parquet",
            "commodities_ohlcv_1d.parquet",
            "commodities_ohlcv_15m.parquet",
            "funding_mexc.parquet",
            "mexc_derivatives.parquet",
            "binance_spot_liquidity.parquet",
            "market_breadth.parquet",
        ]
        if options_available:
            archive_files.extend(["options_deribit.parquet", "options_regime.parquet"])
        archive_files.extend([
            "universe.parquet",
            "PROMPT_FOR_CHATGPT.txt",
            "manifest.json",
            "status.json",
        ])

        manifest = {
            "app_version": settings.app_version,
            "archive_type": "market_scan_parquet",
            "analysis_prompt_file": "PROMPT_FOR_CHATGPT.txt",
            "analysis_prompt_output_mode": "FINAL_VERDICT_ONLY",
            "generated_at_utc": generated_utc,
            "generated_at_msk": generated_msk,
            "timezone": settings.user_timezone,
            "no_cache": True,
            "candle_policy": {
                "crypto_1h": {"requested_closed_candles": settings.candle_limit, "history_days_equivalent": settings.candle_limit / 24, "current_open_candle_excluded": True},
                "crypto_1d": {"requested_closed_candles": settings.daily_candle_limit, "history_days": settings.daily_candle_limit, "current_open_candle_excluded": True},
                "crypto_15m": {"requested_closed_candles": settings.m15_candle_limit, "history_days": settings.m15_candle_limit / 96, "current_open_candle_excluded": True},
                "derive_4h_from_1h_downstream": True,
            },
            "crypto": {
                "selection": f"{top_limit} highest-market-cap non-stable/non-wrapped assets with a TRADING Binance Spot USDT pair",
                "universe_source": universe_source,
                "ohlcv_source": "Binance Spot",
                "requested_assets": top_limit,
                "full_999_1h_assets": full_count,
                "partial_1h_assets": partial_count,
                "full_365_1d_assets": full_daily_count,
                "partial_1d_assets_kept": partial_daily_count,
                "daily_history_policy": "up to 365 closed 1D candles; keep asset and use all available history when fewer exist",
                "full_288_15m_assets": full_15m_count,
                "partial_15m_assets": partial_15m_count,
                "spot_depth_source": "Binance Spot",
                "spot_depth_limit_per_side": settings.binance_depth_limit,
                "spot_depth_ok_assets": depth_ok_count,
            },
            "market_breadth": {"source": f"calculated locally from top-{top_limit} universe and Binance Spot daily candles"},
            "funding": {
                "source": "MEXC Futures only",
                "current_rate_endpoint": "/api/v1/contract/funding_rate/{symbol}",
                "current_rate_fallback": "contract ticker fundingRate only; metadata marked unavailable",
                "requested_assets": top_limit,
                "matched_contracts": funding_coverage,
                "history_records_requested_per_contract": settings.funding_history_count,
                "binance_futures_used": False,
            },
            "mexc_derivatives": {
                "source": "MEXC Futures contract ticker",
                "matched_contracts": mexc_derivatives_coverage,
                "fields": ["holdVol/open interest", "last/index/fair basis", "funding", "24h volume", "bid1/ask1"],
            },
            "options": {
                "source": "Deribit public API",
                "optional": True,
                "available": options_available,
                "requested_assets": ["BTC", "ETH"],
                "available_assets": options_assets,
                "raw_chain_file": "options_deribit.parquet" if options_available else None,
                "regime_file": "options_regime.parquet" if options_available else None,
                "failure_policy": "skip options and continue archive",
            },
            "commodities": {
                "source": "MEXC Futures",
                "contracts": COMMODITY_CONTRACTS,
                "full_999_1h_assets": commodities_ok,
                "extra_timeframes": ["365 closed 1D", "288 closed 15m"],
            },
            "files": archive_files,
        }
        status = {
            "app_version": settings.app_version,
            "generated_at_utc": generated_utc,
            "generated_at_msk": generated_msk,
            "crypto_1h": crypto_1h_status,
            "crypto_1d": crypto_1d_status,
            "crypto_15m": crypto_15m_status,
            "binance_spot_liquidity": spot_liquidity_status,
            "funding_mexc": funding_status,
            "mexc_derivatives": mexc_derivative_status,
            "commodities_1h": commodity_1h_status,
            "commodities_1d": commodity_1d_status,
            "commodities_15m": commodity_15m_status,
            "options_deribit": option_status,
        }
        _write_json(run_dir / "manifest.json", manifest)
        _write_json(run_dir / "status.json", status)

        archive_path = settings.exports_dir / f"market_scan_{stamp}.zip"
        if archive_path.exists():
            archive_path = settings.exports_dir / f"market_scan_{stamp}_{time.time_ns() % 1_000_000:06d}.zip"
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for name in manifest["files"]:
                zf.write(run_dir / name, arcname=name)
        with zipfile.ZipFile(archive_path, "r") as zf:
            broken = zf.testzip()
            if broken:
                raise RuntimeError(f"ZIP integrity failed at {broken}")

        digest = _sha256(archive_path)
        duration = time.monotonic() - started
        log.info(
            "market_scan archive complete version=%s file=%s bytes=%s sha256=%s duration_sec=%.3f universe=%s full_1h=%s partial_1h=%s full_1d=%s partial_1d_kept=%s full_15m=%s partial_15m=%s depth_ok=%s mexc_funding=%s mexc_derivatives=%s options_available=%s options_assets=%s",
            settings.app_version, archive_path.name, archive_path.stat().st_size, digest, duration, len(universe), full_count, partial_count,
            full_daily_count, partial_daily_count, full_15m_count, partial_15m_count, depth_ok_count, funding_coverage,
            mexc_derivatives_coverage, options_available, ",".join(options_assets) or "none",
        )
        await say("Архив готов.")
        return ArchiveBuildResult(
            archive_path=archive_path,
            archive_size_bytes=archive_path.stat().st_size,
            archive_sha256=digest,
            generated_at_utc=generated_utc,
            generated_at_msk=generated_msk,
            universe_count=len(universe),
            binance_full_count=full_count,
            binance_partial_count=partial_count,
            mexc_funding_coverage=funding_coverage,
            commodities_ok_count=commodities_ok,
            duration_seconds=duration,
        )
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)

