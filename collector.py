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


def _closed_hour_start_ms(now_ms: int | None = None) -> int:
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    hour_ms = 60 * 60 * 1000
    return (now_ms // hour_ms) * hour_ms


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
            if attempt + 1 < attempts:
                await asyncio.sleep(0.45 * (2**attempt))
    assert last is not None
    raise last


async def _fetch_marketcap_candidates(client: httpx.AsyncClient, settings: Settings) -> tuple[list[dict[str, Any]], str]:
    """Get enough ranked assets to fill 100 Binance-Spot-eligible slots."""
    try:
        rows = await _get_json(
            client,
            f"{settings.coingecko_base_url}/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h,7d",
            },
            attempts=2,
        )
        if isinstance(rows, list) and len(rows) >= 100:
            result: list[dict[str, Any]] = []
            for row in rows:
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


def _select_top100(
    candidates: list[dict[str, Any]],
    spot_pairs: dict[str, str],
    tickers: dict[str, dict[str, Any]],
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
        if len(selected) == 100:
            break
    if len(selected) < 100:
        raise RuntimeError(f"Could not build 100 eligible Binance Spot assets; got {len(selected)}")
    return selected


async def _fetch_binance_ohlcv(
    client: httpx.AsyncClient,
    settings: Settings,
    universe: list[dict[str, Any]],
    hour_start_ms: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    sem = asyncio.Semaphore(10)
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
                        "interval": "1h",
                        "limit": settings.candle_limit,
                        "endTime": hour_start_ms - 1,
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
                    if open_ms >= hour_start_ms:
                        continue
                    rows.append({
                        "asset": symbol,
                        "binance_symbol": pair,
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
                rows = rows[-settings.candle_limit :]
                status = {
                    "source": "binance_spot",
                    "pair": pair,
                    "status": "OK" if len(rows) == settings.candle_limit else "PARTIAL_HISTORY",
                    "candles": len(rows),
                    "first_utc": rows[0]["timestamp_utc"] if rows else None,
                    "last_utc": rows[-1]["timestamp_utc"] if rows else None,
                }
                return symbol, rows, status
            except Exception as exc:  # noqa: BLE001
                return symbol, [], {
                    "source": "binance_spot",
                    "pair": pair,
                    "status": f"ERROR:{type(exc).__name__}",
                    "error": str(exc)[:300],
                    "candles": 0,
                }

    results = await asyncio.gather(*(one(asset) for asset in universe))
    for symbol, rows, status in results:
        all_rows.extend(rows)
        statuses[symbol] = status
    all_rows.sort(key=lambda r: (r["asset"], r["timestamp_ms"]))
    return all_rows, statuses


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

    async def history(symbol: str, contract: str) -> tuple[str, list[dict[str, Any]], str | None]:
        if settings.funding_history_count <= 0:
            return symbol, [], None
        async with sem:
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
                    return symbol, [], "bad_response"
                records: list[dict[str, Any]] = []
                for item in result_list[: settings.funding_history_count]:
                    if not isinstance(item, dict):
                        continue
                    settle_ms = _int(item.get("settleTime"))
                    records.append({
                        "asset": symbol,
                        "mexc_contract": contract,
                        "record_type": "history",
                        "funding_rate": _num(item.get("fundingRate")),
                        "funding_rate_pct": (_num(item.get("fundingRate")) * 100) if _num(item.get("fundingRate")) is not None else None,
                        "settle_time_ms": settle_ms,
                        "settle_time_utc": _utc_iso(settle_ms),
                        "next_settle_time_ms": None,
                        "next_settle_time_utc": None,
                        "collect_cycle_hours": None,
                        "fair_price": None,
                        "index_price": None,
                        "status": "OK",
                    })
                return symbol, records, None
            except Exception as exc:  # noqa: BLE001
                return symbol, [], f"{type(exc).__name__}:{str(exc)[:180]}"

    tasks = []
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
                "settle_time_ms": None,
                "settle_time_utc": None,
                "next_settle_time_ms": None,
                "next_settle_time_utc": None,
                "collect_cycle_hours": None,
                "fair_price": None,
                "index_price": None,
                "status": "NOT_ON_MEXC",
            })
            statuses[symbol] = {"status": "NOT_ON_MEXC", "contract": None, "history_records": 0}
            continue
        ticker = ticker_by_contract.get(contract) or {}
        rate = _num(ticker.get("fundingRate"))
        next_settle = _int(ticker.get("nextSettleTime"))
        rows_out.append({
            "asset": symbol,
            "mexc_contract": contract,
            "record_type": "current",
            "funding_rate": rate,
            "funding_rate_pct": rate * 100 if rate is not None else None,
            "settle_time_ms": None,
            "settle_time_utc": None,
            "next_settle_time_ms": next_settle,
            "next_settle_time_utc": _utc_iso(next_settle),
            "collect_cycle_hours": _int(ticker.get("collectCycle")),
            "fair_price": _num(ticker.get("fairPrice")),
            "index_price": _num(ticker.get("indexPrice")),
            "status": "OK" if rate is not None else "CURRENT_UNAVAILABLE",
        })
        statuses[symbol] = {
            "status": "OK" if rate is not None else "CURRENT_UNAVAILABLE",
            "contract": contract,
            "history_records": 0,
        }
        tasks.append(history(symbol, contract))

    if tasks:
        history_results = await asyncio.gather(*tasks)
        for symbol, records, error in history_results:
            rows_out.extend(records)
            statuses[symbol]["history_records"] = len(records)
            if error:
                statuses[symbol]["history_error"] = error
                if statuses[symbol]["status"] == "OK":
                    statuses[symbol]["status"] = "HISTORY_UNAVAILABLE"

    rows_out.sort(key=lambda r: (r["asset"], 0 if r["record_type"] == "current" else 1, r.get("settle_time_ms") or 0), reverse=False)
    return rows_out, statuses


async def _fetch_mexc_commodities(
    client: httpx.AsyncClient,
    settings: Settings,
    hour_start_ms: int,
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
                params={"interval": "Min60", "end": int(hour_start_ms // 1000) - 1},
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
            records = []
            for i, ts in enumerate(times):
                try:
                    ts_sec = int(ts)
                    open_ms = ts_sec * 1000
                    if open_ms >= hour_start_ms:
                        continue
                    records.append({
                        "asset": asset,
                        "mexc_contract": contract,
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
            records = records[-settings.candle_limit :]
            ticker = ticker_by_contract.get(contract) or {}
            status = {
                "source": "mexc_futures",
                "contract": contract,
                "status": "OK" if len(records) == settings.candle_limit else "PARTIAL_HISTORY",
                "candles": len(records),
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
                "status": f"ERROR:{type(exc).__name__}",
                "error": str(exc)[:300],
                "candles": 0,
            }

    results = await asyncio.gather(*(one(asset, contract) for asset, contract in COMMODITY_CONTRACTS.items()))
    for asset, rows, status in results:
        rows_out.extend(rows)
        statuses[asset] = status
    rows_out.sort(key=lambda r: (r["asset"], r["timestamp_ms"]))
    return rows_out, statuses


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


async def build_market_archive(settings: Settings, *, progress: Callable[[str], Any] | None = None) -> ArchiveBuildResult:
    """Build one fresh market snapshot from zero; no candle cache is read or written."""
    started = time.monotonic()
    now = datetime.now(timezone.utc)
    msk = now.astimezone(ZoneInfo(settings.user_timezone))
    hour_start_ms = _closed_hour_start_ms(int(now.timestamp() * 1000))
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
        await say("Получаю top-100 и список Binance Spot…")
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            candidates_task = asyncio.create_task(_fetch_marketcap_candidates(client, settings))
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
            universe = _select_top100(candidates, pairs, tickers)
            mexc_tickers = _mexc_ticker_list(mexc_tickers_raw)

            await say("Скачиваю 999 закрытых 1H свечей Binance Spot по 100 активам…")
            crypto_rows, crypto_status = await _fetch_binance_ohlcv(client, settings, universe, hour_start_ms)

            await say("Получаю MEXC funding и XAU/XAG/USOIL…")
            # One shared limiter covers funding history and commodity klines so
            # concurrent tasks stay below MEXC's documented 20 requests / 2 sec.
            mexc_limiter = RequestRateLimiter(max_calls=8, period=1.0)
            funding_task = asyncio.create_task(_fetch_mexc_funding(client, settings, universe, mexc_tickers, mexc_limiter))
            commodities_task = asyncio.create_task(_fetch_mexc_commodities(client, settings, hour_start_ms, mexc_tickers, mexc_limiter))
            (funding_rows, funding_status), (commodity_rows, commodity_status) = await asyncio.gather(
                funding_task, commodities_task
            )

        # Add candle coverage back into the universe table so one file gives a
        # complete inventory of every requested asset, including partial/new listings.
        for row in universe:
            symbol = str(row["symbol"])
            cstat = crypto_status.get(symbol) or {}
            fstat = funding_status.get(symbol) or {}
            row["candle_count_1h"] = int(cstat.get("candles") or 0)
            row["candle_status"] = cstat.get("status")
            row["mexc_contract"] = fstat.get("contract")
            row["mexc_funding_status"] = fstat.get("status")
            row["mexc_funding_history_records"] = int(fstat.get("history_records") or 0)

        await say("Пишу Parquet и собираю ZIP…")
        _write_parquet(run_dir / "crypto_ohlcv.parquet", crypto_rows)
        _write_parquet(run_dir / "commodities_ohlcv.parquet", commodity_rows)
        _write_parquet(run_dir / "funding_mexc.parquet", funding_rows)
        _write_parquet(run_dir / "universe.parquet", universe)
        prompt_source = Path(__file__).with_name("PROMPT_FOR_CHATGPT.txt")
        if not prompt_source.is_file():
            raise RuntimeError("PROMPT_FOR_CHATGPT.txt is missing from bot image")
        shutil.copy2(prompt_source, run_dir / "PROMPT_FOR_CHATGPT.txt")

        full_count = sum(1 for v in crypto_status.values() if v.get("status") == "OK")
        partial_count = sum(1 for v in crypto_status.values() if v.get("status") == "PARTIAL_HISTORY")
        funding_coverage = sum(1 for v in funding_status.values() if v.get("contract"))
        commodities_ok = sum(1 for v in commodity_status.values() if v.get("status") == "OK")
        generated_utc = now.isoformat()
        generated_msk = msk.isoformat()

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
                "timeframe": "1h",
                "requested_closed_candles": settings.candle_limit,
                "history_hours": settings.candle_limit,
                "history_days_equivalent": settings.candle_limit / 24,
                "current_open_hour_excluded": True,
                "derive_4h_and_1d_downstream": True,
            },
            "crypto": {
                "selection": "100 highest-market-cap non-stable/non-wrapped assets with a TRADING Binance Spot USDT pair",
                "universe_source": universe_source,
                "ohlcv_source": "Binance Spot",
                "requested_assets": 100,
                "full_999_assets": full_count,
                "partial_history_assets": partial_count,
            },
            "funding": {
                "source": "MEXC Futures only",
                "requested_assets": 100,
                "matched_contracts": funding_coverage,
                "history_records_requested_per_contract": settings.funding_history_count,
                "binance_futures_used": False,
            },
            "commodities": {
                "source": "MEXC Futures",
                "contracts": COMMODITY_CONTRACTS,
                "full_999_assets": commodities_ok,
            },
            "files": [
                "crypto_ohlcv.parquet",
                "commodities_ohlcv.parquet",
                "funding_mexc.parquet",
                "universe.parquet",
                "PROMPT_FOR_CHATGPT.txt",
                "manifest.json",
                "status.json",
            ],
        }
        status = {
            "app_version": settings.app_version,
            "generated_at_utc": generated_utc,
            "generated_at_msk": generated_msk,
            "crypto": crypto_status,
            "funding_mexc": funding_status,
            "commodities": commodity_status,
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
