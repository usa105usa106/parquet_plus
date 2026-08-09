from __future__ import annotations

# Coolify Trading Signal Bot v019

import math
import time
import uuid
from typing import Any

from analyzer import Signal

BOT_VERSION = "v019"
SIGNAL_HORIZON_SECONDS = 14 * 24 * 60 * 60

OPEN_STATES = {"pending", "active"}
CLOSED_STATES = {"tp", "stop", "timeout"}
INACTIVE_STATES = {"expired", "superseded"}


def ensure_stats(raw: Any) -> dict[str, Any]:
    """Return a JSON-safe v019 stats object, preserving compatible records."""
    if not isinstance(raw, dict):
        raw = {}
    records = raw.get("records")
    if not isinstance(records, list):
        records = []
    clean_records: list[dict[str, Any]] = []
    for row in records:
        if not isinstance(row, dict):
            continue
        if not row.get("id") or not row.get("asset") or not row.get("direction"):
            continue
        clean_records.append(dict(row))
    return {
        "schema": 1,
        "records": clean_records[-1000:],
        "last_reconciled_at": raw.get("last_reconciled_at"),
    }


def _num(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _risk(record: dict[str, Any]) -> float | None:
    entry = _num(record.get("entry_price"))
    stop = _num(record.get("stop"))
    if entry is None or stop is None:
        return None
    distance = abs(entry - stop)
    return distance if distance > 0 else None


def _same_setup(record: dict[str, Any], category: str, signal: Signal) -> bool:
    if record.get("category") != category or record.get("asset") != signal.asset or record.get("direction") != signal.direction:
        return False
    old_entry = _num(record.get("entry_price"))
    new_entry = None
    if signal.entry_low is not None and signal.entry_high is not None:
        new_entry = (float(signal.entry_low) + float(signal.entry_high)) / 2
    if old_entry is None or new_entry is None or new_entry == 0:
        return False
    old_stop = _num(record.get("stop"))
    new_stop = _num(signal.stop)
    # Treat small level drift as the same published setup so a 15-minute timer
    # does not manufacture dozens of duplicate signals.
    entry_drift = abs(old_entry - new_entry) / abs(new_entry)
    stop_drift = abs((old_stop or 0) - (new_stop or 0)) / abs(new_entry)
    return entry_drift <= 0.006 and stop_drift <= 0.008


def _source_symbol(category: str, signal: Signal, bundle: dict[str, Any]) -> str | None:
    if category.startswith("crypto_"):
        tech = (bundle.get("crypto_technicals") or {}).get(signal.asset) or {}
        return tech.get("spot_symbol")
    tech = (bundle.get("commodities") or {}).get(signal.asset) or {}
    return tech.get("source_symbol")


def _supersede_pending_for_category(records: list[dict[str, Any]], category: str, now_ts: float) -> None:
    for record in records:
        if record.get("category") == category and record.get("state") == "pending":
            record["state"] = "superseded"
            record["closed_at"] = now_ts
            record["last_seen_at"] = now_ts


def register_selected_signals(
    stats: dict[str, Any],
    selected: dict[str, Signal],
    bundle: dict[str, Any],
    now_ts: float | None = None,
) -> None:
    """Register unique actionable setups from one published verdict.

    Pending setup duplicates are coalesced. If the bot changes its recommendation
    before entry, the older pending setup becomes `superseded` rather than a loss.
    Active trades remain tracked until TP/stop/timeout.
    """
    now_ts = float(now_ts or time.time())
    records: list[dict[str, Any]] = stats.setdefault("records", [])

    for category, signal in selected.items():
        if signal.status == "NO TRADE" or signal.entry_low is None or signal.entry_high is None or signal.stop is None or signal.target is None:
            _supersede_pending_for_category(records, category, now_ts)
            continue

        duplicate = next(
            (
                record
                for record in reversed(records)
                if record.get("state") in OPEN_STATES and _same_setup(record, category, signal)
            ),
            None,
        )
        if duplicate is not None:
            duplicate["last_seen_at"] = now_ts
            duplicate["last_published_status"] = signal.status
            continue

        _supersede_pending_for_category(records, category, now_ts)

        entry_price = (float(signal.entry_low) + float(signal.entry_high)) / 2
        state = "active" if signal.status == "READY" else "pending"
        record = {
            "id": uuid.uuid4().hex[:16],
            "version": BOT_VERSION,
            "category": category,
            "market": "crypto" if category.startswith("crypto_") else "commodity",
            "asset": signal.asset,
            "direction": signal.direction,
            "entry_low": float(signal.entry_low),
            "entry_high": float(signal.entry_high),
            "entry_price": entry_price,
            "trigger_level": _num(signal.trigger_level),
            "stop": float(signal.stop),
            "target": float(signal.target),
            "rr": _num(signal.rr),
            "completion_pct_at_issue": int(signal.completion_pct) if signal.completion_pct is not None else None,
            "status_at_issue": signal.status,
            "last_published_status": signal.status,
            "source_symbol": _source_symbol(category, signal, bundle),
            "issued_at": now_ts,
            "last_seen_at": now_ts,
            "state": state,
            "activated_at": now_ts if state == "active" else None,
            "closed_at": None,
            "result_r": None,
            "outcome_note": None,
        }
        records.append(record)

    # Bound state-file growth while keeping enough history for meaningful stats.
    if len(records) > 1000:
        del records[:-1000]


def _bar_after_issue(record: dict[str, Any], bar: dict[str, Any]) -> bool:
    """Use only complete 1H bars that started after the issue hour.

    Skipping the partially elapsed issue-hour prevents retroactive activation from
    highs/lows that happened before the Telegram signal was published.
    """
    issued_at = float(record.get("issued_at") or 0.0)
    issue_hour = int(issued_at // 3600) * 3600
    return float(bar.get("open_time") or 0.0) >= issue_hour + 3600


def _touches_zone(record: dict[str, Any], bar: dict[str, Any]) -> bool:
    low = _num(bar.get("low"))
    high = _num(bar.get("high"))
    entry_low = _num(record.get("entry_low"))
    entry_high = _num(record.get("entry_high"))
    if None in (low, high, entry_low, entry_high):
        return False
    return bool(low <= entry_high and high >= entry_low)


def _activation_index(record: dict[str, Any], bars: list[dict[str, Any]]) -> int | None:
    status = str(record.get("status_at_issue") or "")
    direction = str(record.get("direction") or "")
    trigger = _num(record.get("trigger_level"))

    if status == "READY":
        return -1

    trigger_seen = False
    for index, bar in enumerate(bars):
        if not _bar_after_issue(record, bar) or not bool(bar.get("closed", False)):
            continue
        close = _num(bar.get("close"))
        open_ = _num(bar.get("open"))
        if close is None or open_ is None:
            continue

        if status == "WAIT FOR BREAKOUT + RETEST":
            if trigger is not None and not trigger_seen and close > trigger:
                trigger_seen = True
                continue
            if trigger_seen and _touches_zone(record, bar) and (trigger is None or close >= trigger):
                return index
            continue

        if status == "WAIT FOR BREAKDOWN + RETEST":
            if trigger is not None and not trigger_seen and close < trigger:
                trigger_seen = True
                continue
            if trigger_seen and _touches_zone(record, bar) and (trigger is None or close <= trigger):
                return index
            continue

        if status == "WAIT FOR PULLBACK":
            if _touches_zone(record, bar) and close > open_ and (trigger is None or close >= trigger):
                return index
            continue

        if status == "WAIT FOR BOUNCE":
            if _touches_zone(record, bar) and close < open_ and (trigger is None or close <= trigger):
                return index
            continue

        # Unknown future status: require a full-hour touch rather than guessing.
        if _touches_zone(record, bar):
            return index

    return None


def evaluate_record(record: dict[str, Any], bars: list[dict[str, Any]], now_ts: float | None = None) -> None:
    if record.get("state") not in OPEN_STATES:
        return
    now_ts = float(now_ts or time.time())
    bars = sorted((bar for bar in bars if isinstance(bar, dict)), key=lambda x: float(x.get("open_time") or 0.0))

    activation_index = _activation_index(record, bars)
    if activation_index is None:
        if now_ts - float(record.get("issued_at") or now_ts) >= SIGNAL_HORIZON_SECONDS:
            record["state"] = "expired"
            record["closed_at"] = min(now_ts, float(record.get("issued_at") or now_ts) + SIGNAL_HORIZON_SECONDS)
            record["outcome_note"] = "не активировался за 14 дней"
        return

    if record.get("activated_at") is None:
        if activation_index >= 0:
            record["activated_at"] = float(bars[activation_index].get("close_time") or bars[activation_index].get("open_time") or now_ts)
        else:
            record["activated_at"] = float(record.get("issued_at") or now_ts)
    record["state"] = "active"

    direction = str(record.get("direction") or "")
    stop = _num(record.get("stop"))
    target = _num(record.get("target"))
    if stop is None or target is None:
        return

    # A WAIT signal activates on the closing confirmation, so outcome evaluation
    # starts with the next 1H bar. READY signals start with the first full hour
    # after publication. If TP and stop occur in one bar, count stop first — a
    # conservative rule because 1H OHLC does not reveal intrabar order.
    outcome_start = max(0, activation_index + 1)
    for bar in bars[outcome_start:]:
        if not _bar_after_issue(record, bar):
            continue
        low = _num(bar.get("low"))
        high = _num(bar.get("high"))
        close_time = float(bar.get("close_time") or bar.get("open_time") or now_ts)
        if low is None or high is None:
            continue
        if direction == "long":
            stop_hit = low <= stop
            target_hit = high >= target
        else:
            stop_hit = high >= stop
            target_hit = low <= target

        if stop_hit:
            record["state"] = "stop"
            record["closed_at"] = close_time
            record["result_r"] = -1.0
            record["outcome_note"] = "стоп"
            return
        if target_hit:
            rr = _num(record.get("rr"))
            if rr is None:
                risk = _risk(record)
                entry = _num(record.get("entry_price"))
                rr = abs(target - entry) / risk if risk and entry is not None else 0.0
            record["state"] = "tp"
            record["closed_at"] = close_time
            record["result_r"] = float(rr or 0.0)
            record["outcome_note"] = "основная цель"
            return

    deadline = float(record.get("issued_at") or now_ts) + SIGNAL_HORIZON_SECONDS
    if now_ts >= deadline:
        # The prompt's horizon is 3–14 days. Mark unresolved activated trades as
        # timeout at the last observed close and express that mark-to-market in R.
        last_bar = next((bar for bar in reversed(bars) if _bar_after_issue(record, bar) and _num(bar.get("close")) is not None), None)
        result_r = 0.0
        if last_bar is not None:
            entry = _num(record.get("entry_price"))
            close = _num(last_bar.get("close"))
            risk = _risk(record)
            if entry is not None and close is not None and risk:
                pnl = (close - entry) if direction == "long" else (entry - close)
                result_r = pnl / risk
        record["state"] = "timeout"
        record["closed_at"] = deadline
        record["result_r"] = round(float(result_r), 4)
        record["outcome_note"] = "14 дней"


def reconcile_stats(stats: dict[str, Any], histories: dict[str, list[dict[str, Any]]], now_ts: float | None = None) -> None:
    now_ts = float(now_ts or time.time())
    for record in stats.get("records", []):
        record_id = str(record.get("id"))
        if record.get("state") in OPEN_STATES and record_id in histories:
            evaluate_record(record, histories[record_id], now_ts)
    stats["last_reconciled_at"] = now_ts


def summary(stats: dict[str, Any]) -> dict[str, Any]:
    records = [row for row in stats.get("records", []) if isinstance(row, dict)]
    issued = len(records)
    activated_records = [row for row in records if row.get("activated_at") is not None]
    closed = [row for row in records if row.get("state") in CLOSED_STATES]
    tp = [row for row in records if row.get("state") == "tp"]
    stops = [row for row in records if row.get("state") == "stop"]
    timeouts = [row for row in records if row.get("state") == "timeout"]
    pending = [row for row in records if row.get("state") == "pending"]
    active = [row for row in records if row.get("state") == "active"]
    expired = [row for row in records if row.get("state") == "expired"]
    superseded = [row for row in records if row.get("state") == "superseded"]

    decisive = len(tp) + len(stops)
    win_rate = (len(tp) / decisive * 100.0) if decisive else None
    results = [_num(row.get("result_r")) for row in closed]
    results = [value for value in results if value is not None]
    average_r = (sum(results) / len(results)) if results else None
    gross_profit = sum(value for value in results if value > 0)
    gross_loss = abs(sum(value for value in results if value < 0))
    if gross_loss > 0:
        profit_factor: float | None = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = math.inf
    else:
        profit_factor = None

    return {
        "issued": issued,
        "activated": len(activated_records),
        "closed": len(closed),
        "tp": len(tp),
        "stop": len(stops),
        "timeout": len(timeouts),
        "pending": len(pending),
        "active": len(active),
        "expired": len(expired),
        "superseded": len(superseded),
        "win_rate": win_rate,
        "average_r": average_r,
        "profit_factor": profit_factor,
    }
