from __future__ import annotations

# Coolify Trading Signal Bot v017

BOT_VERSION = "v017"

import math
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

USER_TIMEZONE = "Europe/Moscow"  # v017: all user-visible clock times are fixed to MSK (UTC+3)
MIN_CRYPTO_SCORE = float(os.getenv("MIN_CRYPTO_SCORE", "7.0"))
MIN_COMMODITY_SCORE = float(os.getenv("MIN_COMMODITY_SCORE", "5.0"))

TREND_SCORE = {
    "bullish": 3.0,
    "bullish_early": 1.8,
    "mixed": 0.0,
    "bearish_early": -1.8,
    "bearish": -3.0,
}


@dataclass
class Signal:
    asset: str
    direction: str
    entry_low: float | None = None
    entry_high: float | None = None
    trigger_level: float | None = None
    condition: str = ""
    stop: float | None = None
    target: float | None = None
    rr: float | None = None
    status: str = "NO TRADE"
    score: float = -999.0
    completion_pct: int | None = None


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _liquidity_points(volume: float | None) -> float:
    if not volume or volume <= 0:
        return 0.0
    return _clip(math.log10(volume) - 6.8, 0.0, 4.5)


def _bar_is_bullish(tf: dict[str, Any]) -> bool:
    candle = tf.get("last_candle") or {}
    return float(candle.get("close") or 0) > float(candle.get("open") or 0)


def _bar_is_bearish(tf: dict[str, Any]) -> bool:
    candle = tf.get("last_candle") or {}
    return float(candle.get("close") or 0) < float(candle.get("open") or 0)


def _direction_score(tech: dict[str, Any], direction: str) -> float:
    d1, h4, h1 = tech["1d"], tech["4h"], tech["1h"]
    raw = 2.4 * TREND_SCORE.get(d1.get("trend"), 0.0) + 2.0 * TREND_SCORE.get(h4.get("trend"), 0.0) + 0.9 * TREND_SCORE.get(h1.get("trend"), 0.0)
    if direction == "short":
        raw = -raw
    return raw


def _extension_penalty(tech: dict[str, Any], direction: str) -> float:
    h4 = tech["4h"]
    h1 = tech["1h"]
    rsi4 = float(h4.get("rsi14") or 50.0)
    rsi1 = float(h1.get("rsi14") or 50.0)
    pos = float(h4.get("range_position") or 0.5)
    penalty = 0.0
    if direction == "long":
        if rsi4 > 72:
            penalty += (rsi4 - 72) * 0.18
        if rsi1 > 76:
            penalty += (rsi1 - 76) * 0.12
        if pos > 0.90:
            penalty += 2.4
    else:
        if rsi4 < 28:
            penalty += (28 - rsi4) * 0.18
        if rsi1 < 24:
            penalty += (24 - rsi1) * 0.12
        if pos < 0.10:
            penalty += 2.4
    return penalty


def _derivative_adjustment(
    row: dict[str, Any],
    direction: str,
    spot_change_24h_pct: float | None = None,
) -> float:
    """Small MEXC confirmation layer: funding, basis and futures/spot divergence."""
    if not row:
        return 0.0
    funding = float(row.get("funding_pct") or 0.0)
    basis = float(row.get("basis_pct") or 0.0)
    futures_change = float(row.get("futures_change_24h_pct") or 0.0)
    divergence = futures_change - float(spot_change_24h_pct or 0.0)
    adj = 0.0
    if direction == "long":
        if funding > 0.05:
            adj -= min(2.5, (funding - 0.05) * 18)
        elif funding < -0.03:
            adj += min(1.2, abs(funding + 0.03) * 12)
        if basis > 0.65:
            adj -= min(1.4, (basis - 0.65) * 1.2)
        elif -0.45 <= basis < 0:
            adj += 0.25
        if divergence > 4.0:
            adj -= min(1.2, (divergence - 4.0) * 0.18)
        elif divergence < -3.0:
            adj += 0.35
    else:
        if funding < -0.05:
            adj -= min(2.5, abs(funding + 0.05) * 18)
        elif funding > 0.03:
            adj += min(1.2, (funding - 0.03) * 12)
        if basis < -0.65:
            adj -= min(1.4, (abs(basis) - 0.65) * 1.2)
        elif 0 < basis <= 0.45:
            adj += 0.25
        if divergence < -4.0:
            adj -= min(1.2, (abs(divergence) - 4.0) * 0.18)
        elif divergence > 3.0:
            adj += 0.35
    return adj


def _news_adjustment(row: dict[str, Any], direction: str) -> float:
    score = float((row or {}).get("score") or 0.0)
    return _clip(score, -3.0, 3.0) * (0.8 if direction == "long" else -0.8)


def _relative_strength_score(coin: dict[str, Any], btc: dict[str, Any], direction: str) -> float:
    rel24 = float(coin.get("change_24h_pct") or 0.0) - float(btc.get("change_24h_pct") or 0.0)
    rel7 = float(coin.get("change_7d_pct") or 0.0) - float(btc.get("change_7d_pct") or 0.0)
    score = 0.22 * _clip(rel24, -12, 12) + 0.30 * _clip(rel7, -20, 20)
    return score if direction == "long" else -score


def _fifteen_minute_confirmation(tech: dict[str, Any], direction: str) -> bool | None:
    tf = tech.get("15m")
    if not isinstance(tf, dict):
        return None
    close = float(tf.get("close") or 0.0)
    ema20 = tf.get("ema20")
    rsi14 = float(tf.get("rsi14") or 50.0)
    trend = str(tf.get("trend") or "mixed")
    if close <= 0 or ema20 is None:
        return None
    if direction == "long":
        return bool(close >= float(ema20) and rsi14 >= 48 and trend not in {"bearish", "bearish_early"} and _bar_is_bullish(tf))
    return bool(close <= float(ema20) and rsi14 <= 52 and trend not in {"bullish", "bullish_early"} and _bar_is_bearish(tf))


def _entry_confirmation_adjustment(tech: dict[str, Any], direction: str) -> float:
    confirmed = _fifteen_minute_confirmation(tech, direction)
    if confirmed is True:
        return 1.4
    if confirmed is False:
        return -1.1
    return -0.25


def _crypto_history_adjustment(tech: dict[str, Any]) -> float:
    bars = int(float((tech.get("1d") or {}).get("history_bars") or 0))
    if bars >= 300:
        return 0.0
    if bars >= 200:
        return -0.25
    if bars >= 100:
        return -0.7
    if bars >= 50:
        return -1.2
    if bars >= 21:
        return -2.2
    return -4.0


def _breadth_adjustment(bundle: dict[str, Any], direction: str) -> float:
    breadth = bundle.get("local_breadth") or {}
    if breadth.get("status") != "ok":
        return 0.0
    positive = float(breadth.get("positive_pct") or 50.0)
    median24 = float(breadth.get("median_change_24h_pct") or 0.0)
    raw = _clip((positive - 50.0) / 12.0, -1.5, 1.5) + _clip(median24 / 3.0, -0.8, 0.8)
    return raw if direction == "long" else -raw


def _spot_liquidity_adjustment(row: dict[str, Any], direction: str) -> float:
    if not row or row.get("status") != "ok":
        return 0.0
    spread = float(row.get("spread_bps") or 0.0)
    imbalance = float(row.get("imbalance_0_5pct") or 0.0)
    adj = _clip(imbalance * 2.1, -1.1, 1.1)
    if direction == "short":
        adj = -adj
    if spread > 12:
        adj -= min(1.4, (spread - 12) / 12)
    elif spread <= 4:
        adj += 0.2
    return adj


def _crypto_regime_adjustment(bundle: dict[str, Any], asset: str, direction: str) -> float:
    macro = bundle.get("macro") or {}
    global_crypto = bundle.get("global_crypto") or {}
    score = 0.0
    market_change = float(global_crypto.get("market_cap_change_24h_pct") or 0.0)
    score += _clip(market_change / 3.5, -1.0, 1.0)
    dominance = global_crypto.get("btc_dominance_pct")
    if asset != "BTC" and dominance is not None:
        dom = float(dominance)
        # Level-only dominance is a weak regime cue, so keep its weight small.
        if dom >= 60:
            score -= 0.35
        elif dom <= 50:
            score += 0.25
    dxy = macro.get("DXY") or {}
    us10 = macro.get("US10Y") or {}
    if isinstance(dxy, dict) and "1d" in dxy:
        score -= 0.28 * TREND_SCORE.get(dxy["1d"].get("trend"), 0.0)
    if isinstance(us10, dict) and "1d" in us10:
        score -= 0.18 * TREND_SCORE.get(us10["1d"].get("trend"), 0.0)
    score = _clip(score, -2.0, 2.0)
    return score if direction == "long" else -score


def _estimate_completion_pct(
    signal: Signal,
    *,
    threshold: float,
) -> int | None:
    """Heuristic current-setup confidence, not historical winrate.

    Convert the final composite score only once. Raw trend/15m/breadth/depth/
    derivatives/news inputs are already inside ``signal.score`` and are not
    re-added here, preventing double weighting in the displayed percentage.
    """
    if signal.status == "NO TRADE":
        return None
    edge = signal.score - threshold
    pct = 59.0 + _clip(edge * 2.0, -9.0, 22.0)
    # Execution phase is not a second market-data vote: it only expresses how
    # close the already-scored setup is to an actionable entry.
    if signal.status == "READY":
        pct += 5.0
    elif signal.status in {"WAIT FOR PULLBACK", "WAIT FOR BOUNCE"}:
        pct += 2.0
    return int(round(_clip(pct, 50.0, 86.0)))


def _select_support(price: float, tech: dict[str, Any]) -> float | None:
    h4, h1 = tech["4h"], tech["1h"]
    values = [h4.get("ema20"), h4.get("ema50"), h1.get("prior_range_low")]
    below = [float(v) for v in values if v is not None and 0 < float(v) < price]
    return max(below) if below else None


def _select_resistance(price: float, tech: dict[str, Any]) -> float | None:
    h4, h1 = tech["4h"], tech["1h"]
    values = [h4.get("ema20"), h4.get("ema50"), h1.get("prior_range_high")]
    above = [float(v) for v in values if v is not None and float(v) > price]
    return min(above) if above else None


def _make_setup(asset: str, tech: dict[str, Any], direction: str, score: float) -> Signal:
    h4, h1 = tech["4h"], tech["1h"]
    price = float(h1["close"])
    atr = float(h4.get("atr14") or (price * 0.02))
    atr = max(atr, price * 0.002)
    d1_trend = tech["1d"].get("trend")
    h4_trend = h4.get("trend")

    if direction == "long":
        aligned = d1_trend in {"bullish", "bullish_early"} and h4_trend in {"bullish", "bullish_early"}
        support = _select_support(price, tech)
        if aligned and support and 0 < (price - support) <= 2.7 * atr:
            entry_low = max(0.0, support - 0.18 * atr)
            entry_high = support + 0.28 * atr
            trigger = max(support, float(h1.get("ema20") or support))
            stop = entry_low - 0.72 * atr
            risk = ((entry_low + entry_high) / 2) - stop
            target = ((entry_low + entry_high) / 2) + 2.7 * risk
            inside = entry_low <= price <= entry_high
            midrange = 0.35 <= float(h4.get("range_position") or 0.5) <= 0.65
            confirmed = inside and _bar_is_bullish(h1) and price > float(h1.get("ema20") or 0) and _fifteen_minute_confirmation(tech, "long") is True and not midrange
            return Signal(asset, direction, entry_low, entry_high, trigger, f"возврата выше {trigger}", stop, target, 2.7, "READY" if confirmed else "WAIT FOR PULLBACK", score)

        breakout = float(h4.get("prior_range_high") or price)
        entry_low = breakout - 0.12 * atr
        entry_high = breakout + 0.22 * atr
        stop = breakout - 0.88 * atr
        risk = ((entry_low + entry_high) / 2) - stop
        target = ((entry_low + entry_high) / 2) + 2.6 * risk
        return Signal(asset, direction, entry_low, entry_high, breakout, f"1H закрытия выше {breakout} и ретеста сверху", stop, target, 2.6, "WAIT FOR BREAKOUT + RETEST", score)

    aligned = d1_trend in {"bearish", "bearish_early"} and h4_trend in {"bearish", "bearish_early"}
    resistance = _select_resistance(price, tech)
    if aligned and resistance and 0 < (resistance - price) <= 2.7 * atr:
        entry_low = resistance - 0.28 * atr
        entry_high = resistance + 0.18 * atr
        stop = entry_high + 0.72 * atr
        risk = stop - ((entry_low + entry_high) / 2)
        target = ((entry_low + entry_high) / 2) - 2.8 * risk
        inside = entry_low <= price <= entry_high
        midrange = 0.35 <= float(h4.get("range_position") or 0.5) <= 0.65
        confirmed = inside and _bar_is_bearish(h1) and price < float(h1.get("ema20") or price * 2) and _fifteen_minute_confirmation(tech, "short") is True and not midrange
        return Signal(asset, direction, entry_low, entry_high, resistance, f"медвежьего отказа от {resistance}", stop, target, 2.8, "READY" if confirmed else "WAIT FOR BOUNCE", score)

    breakdown = float(h4.get("prior_range_low") or price)
    entry_low = breakdown - 0.22 * atr
    entry_high = breakdown + 0.12 * atr
    stop = breakdown + 0.88 * atr
    risk = stop - ((entry_low + entry_high) / 2)
    target = ((entry_low + entry_high) / 2) - 2.7 * risk
    return Signal(asset, direction, entry_low, entry_high, breakdown, f"1H закрытия ниже {breakdown} и ретеста снизу", stop, target, 2.7, "WAIT FOR BREAKDOWN + RETEST", score)


def _crypto_candidates(bundle: dict[str, Any], direction: str) -> list[Signal]:
    top100 = bundle["top100"]
    btc = next((coin for coin in top100 if coin.get("symbol") == "BTC"), {})
    lookup = {coin["symbol"]: coin for coin in top100}
    techs = bundle.get("crypto_technicals", {})
    derivatives = bundle.get("crypto_derivatives", {})
    liquidity = bundle.get("crypto_spot_liquidity", {})
    news = bundle.get("crypto_news", {})

    signals: list[Signal] = []
    for symbol, tech in techs.items():
        if not isinstance(tech, dict) or "1d" not in tech:
            continue
        coin = lookup.get(symbol)
        if not coin:
            continue
        volume = coin.get("binance_spot_quote_volume_24h") or coin.get("volume_24h")
        spot_change = coin.get("binance_spot_change_24h_pct")
        if spot_change is None:
            spot_change = coin.get("change_24h_pct")
        derivative_adj = _derivative_adjustment(derivatives.get(symbol, {}), direction, float(spot_change or 0.0))
        news_adj = _news_adjustment(news.get(symbol, {}), direction)
        breadth_adj = _breadth_adjustment(bundle, direction)
        depth_adj = _spot_liquidity_adjustment(liquidity.get(symbol, {}), direction)
        regime_adj = _crypto_regime_adjustment(bundle, symbol, direction)
        entry_adj = _entry_confirmation_adjustment(tech, direction)
        history_adj = _crypto_history_adjustment(tech)

        score = _direction_score(tech, direction)
        score += _relative_strength_score(coin, btc, direction)
        score += 1.2 * _liquidity_points(float(volume or 0.0))
        score += derivative_adj + news_adj + breadth_adj + depth_adj + regime_adj + entry_adj + history_adj
        score -= _extension_penalty(tech, direction)

        if direction == "long" and float(tech["4h"].get("range_position") or 0.5) > 0.96:
            score -= 2.0
        if direction == "short" and float(tech["4h"].get("range_position") or 0.5) < 0.04:
            score -= 2.0

        signal = _make_setup(symbol, tech, direction, score)
        signal.completion_pct = _estimate_completion_pct(
            signal, threshold=MIN_CRYPTO_SCORE,
        )
        signals.append(signal)
    return sorted(signals, key=lambda s: s.score, reverse=True)


def _macro_bias(bundle: dict[str, Any], asset: str, direction: str) -> float:
    macro = bundle.get("macro", {})
    dxy = macro.get("DXY", {})
    us10 = macro.get("US10Y", {})
    score = 0.0
    if asset in {"XAU/USD", "XAG/USD"}:
        if "1d" in dxy:
            dxy_trend = TREND_SCORE.get(dxy["1d"].get("trend"), 0.0)
            score += -0.65 * dxy_trend if direction == "long" else 0.65 * dxy_trend
        if "1d" in us10:
            yield_trend = TREND_SCORE.get(us10["1d"].get("trend"), 0.0)
            score += -0.40 * yield_trend if direction == "long" else 0.40 * yield_trend
    return score


def _commodity_candidates(bundle: dict[str, Any], direction: str) -> list[Signal]:
    signals: list[Signal] = []
    for asset, tech in (bundle.get("commodities") or {}).items():
        if not isinstance(tech, dict) or "1d" not in tech:
            continue
        macro_adj = _macro_bias(bundle, asset, direction)
        news_adj = _news_adjustment((bundle.get("commodity_news") or {}).get(asset, {}), direction) * 0.65
        entry_adj = _entry_confirmation_adjustment(tech, direction)
        score = _direction_score(tech, direction)
        score += macro_adj + news_adj + entry_adj
        score -= _extension_penalty(tech, direction)
        if asset == "XAG/USD":
            score -= 0.35
        signal = _make_setup(asset, tech, direction, score)
        signal.completion_pct = _estimate_completion_pct(
            signal, threshold=MIN_COMMODITY_SCORE,
        )
        signals.append(signal)
    return sorted(signals, key=lambda s: s.score, reverse=True)


def _fmt_price(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "—"
    abs_value = abs(value)
    if abs_value >= 1000:
        decimals = 0 if abs_value >= 10000 else 1
    elif abs_value >= 100:
        decimals = 2
    elif abs_value >= 10:
        decimals = 2
    elif abs_value >= 1:
        decimals = 3
    elif abs_value >= 0.1:
        decimals = 4
    elif abs_value >= 0.01:
        decimals = 5
    else:
        decimals = 7
    text = f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", " ")
    return f"${text}"


def _fmt_level_for_condition(value: float | None) -> str:
    return _fmt_price(value)


def _clean_condition(signal: Signal) -> str:
    if signal.trigger_level is None:
        return signal.condition
    trigger = _fmt_level_for_condition(signal.trigger_level)
    if signal.status == "WAIT FOR PULLBACK":
        return f"возврата выше {trigger}"
    if signal.status == "WAIT FOR BOUNCE":
        return f"медвежьего отказа от {trigger}"
    if signal.status == "WAIT FOR BREAKOUT + RETEST":
        return f"1H закрытия выше {trigger} и ретеста"
    if signal.status == "WAIT FOR BREAKDOWN + RETEST":
        return f"1H закрытия ниже {trigger} и ретеста"
    return f"подтверждения 1H у {trigger}"


def _signal_block(signal: Signal, title: str, include_rr: bool) -> str:
    if signal.status == "NO TRADE" or signal.entry_low is None or signal.entry_high is None:
        lines = [
            f"{title}: NO TRADE",
            "Вход: —",
            "Стоп: —",
            "Основная цель: —",
        ]
        if include_rr:
            lines.append("R/R: —")
        lines.append("Статус: NO TRADE")
        return "\n".join(lines)

    lines = [
        f"{title}: {signal.asset}",
        f"Отработка: {signal.completion_pct}%" if signal.completion_pct is not None else "Отработка: —",
        f"Вход: {_fmt_price(signal.entry_low)}–{_fmt_price(signal.entry_high)} после {_clean_condition(signal)}",
        f"Стоп: {_fmt_price(signal.stop)}",
        f"Основная цель: {_fmt_price(signal.target)}",
    ]
    if include_rr:
        rr = str(round(float(signal.rr or 0.0), 1)).replace(".", ",")
        lines.append(f"R/R: около {rr}")
    lines.append(f"Статус: {signal.status}")
    return "\n".join(lines)


def _no_trade(direction: str, asset: str = "NO TRADE") -> Signal:
    return Signal(asset=asset, direction=direction, status="NO TRADE", score=-999)


def _event_title_ru(title: str) -> str:
    low = title.lower()
    if "non-farm" in low or "nonfarm" in low or "employment change" in low:
        return "отчёт по занятости США"
    if "unemployment" in low:
        return "безработица США"
    if "consumer price" in low or "cpi" in low:
        return "CPI США"
    if "producer price" in low or "ppi" in low:
        return "PPI США"
    if "jolts" in low or "job openings" in low:
        return "JOLTS США"
    if "ism manufacturing" in low:
        return "ISM Manufacturing США"
    if "ism services" in low:
        return "ISM Services США"
    if "fomc" in low:
        return "FOMC"
    if "powell" in low or "fed chair" in low:
        return "выступление Пауэлла"
    if "adp" in low:
        return "ADP США"
    if "crude oil inventories" in low or "eia" in low:
        return "запасы нефти EIA"
    return title


def _next_event_text(bundle: dict[str, Any]) -> str:
    events = bundle.get("events") or []
    if not events:
        return "Следующий ключевой риск — календарь не подтверждён; перед входом проверьте CPI/NFP/FOMC/EIA."
    event = events[0]
    try:
        tz = ZoneInfo(USER_TIMEZONE)
    except Exception:
        tz = ZoneInfo("UTC")
    dt = datetime.fromisoformat(event["time_utc"]).astimezone(tz)
    month_names = ["янв.", "февр.", "марта", "апр.", "мая", "июня", "июля", "авг.", "сент.", "окт.", "нояб.", "дек."]
    title = _event_title_ru(str(event.get("title") or "макрособытие"))
    return f"Следующий ключевой риск — {title} {dt.day} {month_names[dt.month - 1]} в {dt:%H:%M} МСК (UTC+3)."


def select_final_signals(bundle: dict[str, Any]) -> dict[str, Signal]:
    """Select the four published setups without formatting them.

    The structured result is used by v017 statistics tracking so outcomes are
    measured from the exact levels that were shown to the user, not by parsing
    Telegram text.
    """
    crypto_long_list = _crypto_candidates(bundle, "long")
    crypto_short_list = _crypto_candidates(bundle, "short")
    commodity_long_list = _commodity_candidates(bundle, "long")
    commodity_short_list = _commodity_candidates(bundle, "short")

    crypto_long = crypto_long_list[0] if crypto_long_list and crypto_long_list[0].score >= MIN_CRYPTO_SCORE else _no_trade("long")
    crypto_short = crypto_short_list[0] if crypto_short_list and crypto_short_list[0].score >= MIN_CRYPTO_SCORE else _no_trade("short")
    commodity_long = commodity_long_list[0] if commodity_long_list and commodity_long_list[0].score >= MIN_COMMODITY_SCORE else _no_trade("long")
    commodity_short = commodity_short_list[0] if commodity_short_list and commodity_short_list[0].score >= MIN_COMMODITY_SCORE else _no_trade("short")

    # Never publish opposite crypto directions for the same ticker. Prefer the
    # stronger score and use the next qualifying candidate on the other side.
    if crypto_long.asset != "NO TRADE" and crypto_long.asset == crypto_short.asset:
        if crypto_long.score >= crypto_short.score:
            alternative = next((s for s in crypto_short_list if s.asset != crypto_long.asset and s.score >= MIN_CRYPTO_SCORE), None)
            crypto_short = alternative or _no_trade("short")
        else:
            alternative = next((s for s in crypto_long_list if s.asset != crypto_short.asset and s.score >= MIN_CRYPTO_SCORE), None)
            crypto_long = alternative or _no_trade("long")

    # Do not publish simultaneous opposite directions on the same commodity unless one is clearly superior.
    if commodity_long.asset != "NO TRADE" and commodity_long.asset == commodity_short.asset:
        if commodity_long.score >= commodity_short.score:
            alternative = next((s for s in commodity_short_list if s.asset != commodity_long.asset and s.score >= MIN_COMMODITY_SCORE), None)
            commodity_short = alternative or _no_trade("short")
        else:
            alternative = next((s for s in commodity_long_list if s.asset != commodity_short.asset and s.score >= MIN_COMMODITY_SCORE), None)
            commodity_long = alternative or _no_trade("long")

    selected = {
        "crypto_long": crypto_long,
        "crypto_short": crypto_short,
        "commodity_long": commodity_long,
        "commodity_short": commodity_short,
    }

    # If a major event is very close, avoid READY status. The setup stays valid but waits for confirmation after the event.
    events = bundle.get("events") or []
    if events:
        try:
            event_dt = datetime.fromisoformat(events[0]["time_utc"])
            now_dt = datetime.fromisoformat(bundle["snapshot_time_utc"])
            hours_to_event = (event_dt - now_dt).total_seconds() / 3600
            if 0 <= hours_to_event <= 3:
                for signal in selected.values():
                    if signal.status == "READY":
                        signal.status = "WAIT FOR PULLBACK" if signal.direction == "long" else "WAIT FOR BOUNCE"
                        if signal.completion_pct is not None:
                            signal.completion_pct = max(50, signal.completion_pct - 6)
        except Exception:
            pass

    return selected


def build_final_analysis(bundle: dict[str, Any]) -> tuple[str, dict[str, Signal]]:
    selected = select_final_signals(bundle)
    verdict = "\n\n".join([
        f"ФИНАЛЬНЫЙ ВЕРДИКТ · {BOT_VERSION}",
        _signal_block(selected["crypto_long"], "Крипто LONG", include_rr=True),
        _signal_block(selected["crypto_short"], "Крипто SHORT", include_rr=True),
        _signal_block(selected["commodity_long"], "Сырьевой LONG", include_rr=False),
        _signal_block(selected["commodity_short"], "Сырьевой SHORT", include_rr=False),
        _next_event_text(bundle),
    ])
    return verdict, selected


def build_final_verdict(bundle: dict[str, Any]) -> str:
    return build_final_analysis(bundle)[0]
