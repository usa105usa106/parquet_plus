from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time

import httpx
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
from datetime import datetime

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatAction
from telegram.error import NetworkError, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

from collector import ArchiveBuildResult, build_market_archive
from analyzer import DEFAULT_COMMODITY_SCORE, build_final_analysis
from market_data import fetch_market_bundle, fetch_signal_histories
from signal_stats import ensure_stats, reconcile_stats, register_selected_signals, summary as stats_summary
from config import APP_VERSION, Settings, load_settings
from gmail_oauth import (
    ArchiveIdentity,
    GmailArchiveChanged,
    GmailAttachmentTooLarge,
    GmailOAuthError,
    GmailOAuthManager,
    GmailSendUncertain,
)
from security import SecretStore
from logging_utils import build_full_log_report, configure_console_logging, enable_full_file_logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
configure_console_logging(LOG_LEVEL)
log = logging.getLogger("market-scan-bot")

BOT_VERSION = APP_VERSION
PROCESS_STARTED = time.monotonic()

TIME_MODES: list[tuple[str, int]] = [
    ("выкл", 0),
    ("15 мин", 15 * 60),
    ("30 мин", 30 * 60),
    ("1 час", 60 * 60),
    ("4 часа", 4 * 60 * 60),
]

SCORE_S_MIN = 0.0
SCORE_S_MAX = 30.0
FULL_LOG_HOURS = 24

TELEGRAM_CONNECT_TIMEOUT_SECONDS = 5.0
TELEGRAM_RETRY_DELAYS_SECONDS: tuple[float, ...] = (2.0, 3.0)


def _validated_commodity_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return float(DEFAULT_COMMODITY_SCORE)
    if not math.isfinite(score) or not SCORE_S_MIN <= score <= SCORE_S_MAX:
        return float(DEFAULT_COMMODITY_SCORE)
    return round(score, 3)

def _score_text(value: float) -> str:
    return f"{float(value):.3f}".rstrip("0").rstrip(".")


def _is_connect_failure(exc: BaseException) -> bool:
    """Return True only when Telegram failed before an HTTP request was established."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (httpx.ConnectTimeout, httpx.ConnectError)):
            return True
        current = current.__cause__ or current.__context__
    return False


async def _telegram_progress_once(operation: str, call_factory: Any) -> Any | None:
    """Best-effort Telegram UI update. Never block the scan with retries."""
    try:
        return await call_factory()
    except TelegramError as exc:
        log.warning(
            "telegram_progress_skipped operation=%s error_type=%s error=%s",
            operation, type(exc).__name__, exc,
        )
        return None


async def _telegram_delivery_with_retry(operation: str, call_factory: Any) -> Any | None:
    """Retry confirmed Telegram connect failures after 2s and 3s, then skip delivery.

    Read/write timeouts are not retried because Telegram may already have accepted
    the message/file, and retrying an uncertain delivery could create duplicates.
    """
    attempts = 1 + len(TELEGRAM_RETRY_DELAYS_SECONDS)
    for attempt in range(1, attempts + 1):
        try:
            result = await call_factory()
            if attempt > 1:
                log.info(
                    "telegram_delivery_recovered operation=%s attempt=%s/%s",
                    operation, attempt, attempts,
                )
            return result
        except NetworkError as exc:
            if not _is_connect_failure(exc):
                log.error(
                    "telegram_delivery_uncertain operation=%s attempt=%s/%s error_type=%s; no retry to avoid duplicate",
                    operation, attempt, attempts, type(exc).__name__,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                return None
            if attempt >= attempts:
                log.error(
                    "telegram_delivery_skipped operation=%s attempts=%s error_type=%s",
                    operation, attempts, type(exc).__name__,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                return None
            delay = TELEGRAM_RETRY_DELAYS_SECONDS[attempt - 1]
            log.warning(
                "telegram_connect_retry operation=%s attempt=%s/%s retry_in_sec=%.1f error_type=%s",
                operation, attempt, attempts, delay, type(exc).__name__,
            )
            await asyncio.sleep(delay)
    return None

BTN_GMAIL_TEST = "gmail_test"
BTN_GMAIL_DISCONNECT = "gmail_disconnect"
BTN_GMAIL_IMPORT = "gmail_import"

# Callback ids from older builds. v020 never launches OAuth/callback setup.
STALE_GMAIL_CALLBACKS = {
    "gmail_check",
    "gmail_config",
    "gmail_login",
    "gmail_oauth_setup",
}



class Runtime:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.secret_store = SecretStore(
            settings.secrets_dir,
            settings.state_dir,
            settings.secret_encryption_key,
            settings.gmail_backup_root,
            settings.gmail_session_only,
        )
        self.gmail = GmailOAuthManager(settings, self.secret_store, log)
        self.state = self._load_state()
        self.chat_locks: dict[int, asyncio.Lock] = {}
        self.auto_tasks: dict[int, asyncio.Task[Any]] = {}
        self.auto_scanning: set[int] = set()
        self.scan_semaphore = asyncio.Semaphore(settings.max_concurrent_scans)
        self.last_scan_started: dict[int, float] = {}
        self.awaiting: dict[int, dict[str, Any]] = {}
        self.full_log_path = settings.logs_dir / "full.log"

    def _load_state(self) -> dict[str, dict[str, Any]]:
        path = self.settings.state_file
        try:
            raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not read state: %s", exc)
            raw = {}
        result: dict[str, dict[str, Any]] = {}
        if not isinstance(raw, dict):
            return result
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            try:
                chat_id = str(int(key))
            except Exception:
                continue
            idx = int(value.get("mode_index", 0) or 0)
            if not 0 <= idx < len(TIME_MODES):
                idx = 0
            result[chat_id] = {
                "mode_index": idx,
                "armed": bool(value.get("armed", False)) and idx > 0,
                "auto_action": value.get("auto_action") if value.get("auto_action") in {"analysis", "parquet"} else None,
                "last_completed_at": value.get("last_completed_at"),
                "last_duration_seconds": value.get("last_duration_seconds"),
                "last_action": value.get("last_action"),
                "last_analysis_at": value.get("last_analysis_at"),
                "last_analysis_duration_seconds": value.get("last_analysis_duration_seconds"),
                "last_parquet_at": value.get("last_parquet_at"),
                "last_parquet_duration_seconds": value.get("last_parquet_duration_seconds"),
                "signal_stats": ensure_stats(value.get("signal_stats")),
                "last_archive_name": value.get("last_archive_name"),
                "last_archive_size_bytes": value.get("last_archive_size_bytes"),
                "last_universe_count": value.get("last_universe_count"),
                "last_binance_full_count": value.get("last_binance_full_count"),
                "last_binance_partial_count": value.get("last_binance_partial_count"),
                "last_mexc_funding_coverage": value.get("last_mexc_funding_coverage"),
                "last_commodities_ok_count": value.get("last_commodities_ok_count"),
                "last_gmail_status": value.get("last_gmail_status"),
                "scans_created": int(value.get("scans_created", 0) or 0),
                "telegram_archives_sent": int(value.get("telegram_archives_sent", 0) or 0),
                "gmail_archives_sent": int(value.get("gmail_archives_sent", 0) or 0),
                "commodity_score_threshold": _validated_commodity_score(value.get("commodity_score_threshold", DEFAULT_COMMODITY_SCORE)),
                "scan_busy": False,
            }
        return result

    def save_state(self) -> None:
        path = self.settings.state_file
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            serializable: dict[str, dict[str, Any]] = {}
            for key, value in self.state.items():
                serializable[key] = {k: v for k, v in value.items() if k != "scan_busy"}
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not persist state: %s", exc)

    def chat_state(self, chat_id: int) -> dict[str, Any]:
        key = str(chat_id)
        if key not in self.state:
            self.state[key] = {
                "mode_index": 0,
                "armed": False,
                "auto_action": None,
                "last_completed_at": None,
                "last_duration_seconds": None,
                "last_action": None,
                "last_analysis_at": None,
                "last_analysis_duration_seconds": None,
                "last_parquet_at": None,
                "last_parquet_duration_seconds": None,
                "signal_stats": ensure_stats(None),
                "last_archive_name": None,
                "last_archive_size_bytes": None,
                "last_universe_count": None,
                "last_binance_full_count": None,
                "last_binance_partial_count": None,
                "last_mexc_funding_coverage": None,
                "last_commodities_ok_count": None,
                "last_gmail_status": None,
                "scans_created": 0,
                "telegram_archives_sent": 0,
                "gmail_archives_sent": 0,
                "commodity_score_threshold": float(DEFAULT_COMMODITY_SCORE),
                "scan_busy": False,
            }
        return self.state[key]

    def lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        return self.chat_locks[chat_id]

    def mode(self, chat_id: int) -> tuple[str, int]:
        state = self.chat_state(chat_id)
        idx = int(state.get("mode_index", 0) or 0)
        if not 0 <= idx < len(TIME_MODES):
            idx = 0
            state["mode_index"] = 0
        return TIME_MODES[idx]

    def commodity_score(self, chat_id: int) -> float:
        state = self.chat_state(chat_id)
        score = _validated_commodity_score(state.get("commodity_score_threshold", DEFAULT_COMMODITY_SCORE))
        state["commodity_score_threshold"] = score
        return score

    def set_commodity_score(self, chat_id: int, value: float) -> float:
        score = _validated_commodity_score(value)
        state = self.chat_state(chat_id)
        state["commodity_score_threshold"] = score
        self.save_state()
        return score


def _runtime(context: ContextTypes.DEFAULT_TYPE) -> Runtime:
    return context.application.bot_data["runtime"]


def _allowed(update: Update, runtime: Runtime) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return False
    ids = runtime.settings.allowed_chat_ids
    return not ids or chat.id in ids or user.id in ids


async def incoming_update_audit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log every incoming Telegram operation without logging secret message bodies."""
    runtime = _runtime(context)
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = update.effective_user.id if update.effective_user else None
    if not _allowed(update, runtime):
        log.warning("telegram_update rejected update_id=%s chat_id=%s user_id=%s", update.update_id, chat_id, user_id)
        return
    if update.callback_query:
        log.info(
            "telegram_update callback update_id=%s chat_id=%s user_id=%s data=%s",
            update.update_id, chat_id, user_id, update.callback_query.data or "",
        )
        return
    message = update.effective_message
    if message is None:
        log.info("telegram_update other update_id=%s chat_id=%s user_id=%s", update.update_id, chat_id, user_id)
        return
    text = (message.text or "").strip()
    flow = runtime.awaiting.get(user_id or 0) if user_id is not None else None
    if flow:
        log.info(
            "telegram_update sensitive_input update_id=%s chat_id=%s user_id=%s step=%s chars=%s content=REDACTED",
            update.update_id, chat_id, user_id, flow.get("step"), len(text),
        )
    elif text.startswith("/"):
        log.info(
            "telegram_update command update_id=%s chat_id=%s user_id=%s command=%s",
            update.update_id, chat_id, user_id, text.split(maxsplit=1)[0][:80],
        )
    elif text.lower() in {"1", "анализ", "parquet", "отправить parquet", "почта", "подключить почту", "пинг"} or text.lower().startswith("время"):
        log.info(
            "telegram_update action update_id=%s chat_id=%s user_id=%s action=%s",
            update.update_id, chat_id, user_id, text[:80],
        )
    else:
        log.info(
            "telegram_update text update_id=%s chat_id=%s user_id=%s chars=%s content=NOT_LOGGED",
            update.update_id, chat_id, user_id, len(text),
        )


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    update_id = getattr(update, "update_id", None)
    error = context.error
    exc_info = (type(error), error, error.__traceback__) if error is not None else None
    log.error(
        "telegram_handler_error update_id=%s error_type=%s",
        update_id,
        type(error).__name__ if error else "unknown",
        exc_info=exc_info,
    )


def _keyboard(runtime: Runtime, chat_id: int) -> ReplyKeyboardMarkup:
    label, _ = runtime.mode(chat_id)
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Анализ"), KeyboardButton(f"Время {label}")],
            [KeyboardButton("Parquet"), KeyboardButton("Почта")],
            [KeyboardButton("Пинг")],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Нажмите «Анализ»",
    )


def _msk_text(runtime: Runtime, timestamp: float | int | None) -> str:
    if not isinstance(timestamp, (float, int)):
        return "—"
    dt = datetime.fromtimestamp(float(timestamp), tz=ZoneInfo(runtime.settings.user_timezone))
    return dt.strftime("%d.%m %H:%M:%S МСК")


def _elapsed(seconds: float | int | None) -> str:
    if seconds is None:
        return "—"
    total = max(0, int(round(float(seconds))))
    if total < 60:
        return f"{total} сек"
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours} ч {minutes} мин {secs} сек"
    return f"{minutes} мин {secs} сек"


def _uptime(seconds: float) -> str:
    total = max(0, int(seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days} д {hours} ч {minutes} мин"
    if hours:
        return f"{hours} ч {minutes} мин {secs} сек"
    if minutes:
        return f"{minutes} мин {secs} сек"
    return f"{secs} сек"


def _rss_mb() -> float | None:
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return float(line.split()[1]) / 1024.0
    except Exception:
        return None
    return None


def _human_bytes(value: int | float | None) -> str:
    if value is None:
        return "—"
    size = float(value)
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if size < 1024 or unit == "ГБ":
            return f"{size:.1f} {unit}" if unit != "Б" else f"{int(size)} Б"
        size /= 1024
    return f"{size:.1f} ГБ"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    log.info("start requested chat_id=%s", chat_id)
    await update.effective_message.reply_text(
        f"Trading + Market Data Bot {BOT_VERSION}\n"
        "Анализ/1: бот сам делает свежий анализ и присылает только финальный вердикт.\n"
        "Parquet: собирает top-100 свечи/данные без выбора LONG/SHORT, отправляет ZIP в Telegram, затем в Gmail.\n"
        "Время: выкл → 15 мин → 30 мин → 1 час → 4 часа; повторяет то действие, которым запущен цикл.\n"
        "Почта: импорт сохранённой Gmail-авторизации одной Base64-строкой; без callback/OAuth.\n"
        "Пинг: версия/отклик/uptime/RAM. /status — состояние. /log_full — журнал за последние 24 часа.\n"
        "/help — данные и команды; /score_s — порог score для XAU/XAG/USOIL.",
        reply_markup=_keyboard(runtime, chat_id),
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    log.info("status requested chat_id=%s", chat_id)
    state = runtime.chat_state(chat_id)
    label, interval = runtime.mode(chat_id)
    busy = bool(state.get("scan_busy"))
    armed = bool(state.get("armed")) and interval > 0
    action = state.get("auto_action")
    action_label = "Анализ" if action == "analysis" else "Parquet" if action == "parquet" else "—"
    last_completed = state.get("last_completed_at")
    if armed and isinstance(last_completed, (int, float)):
        target = float(last_completed) + interval
        remain = max(0, target - time.time())
        next_text = f"через {_elapsed(remain)} ({_msk_text(runtime, target)})"
    else:
        next_text = "—"
    mem = _rss_mb()
    stats = ensure_stats(state.get("signal_stats"))
    state["signal_stats"] = stats
    ss = stats_summary(stats)
    win_rate = "—" if ss["win_rate"] is None else f"{ss['win_rate']:.1f}%"
    avg_r = "—" if ss["average_r"] is None else f"{ss['average_r']:+.2f}R"
    pf = "—" if ss["profit_factor"] is None else ("∞" if ss["profit_factor"] == float("inf") else f"{ss['profit_factor']:.2f}")
    lines = [
        f"Версия: {BOT_VERSION}",
        f"Статус: {'занят' if busy else 'готов'}",
        f"Последнее действие: {state.get('last_action') or '—'}",
        f"Последнее завершение: {_msk_text(runtime, last_completed)}",
        f"Длительность: {_elapsed(state.get('last_duration_seconds'))}",
        f"Автопроверка: {label}{' (активна)' if armed else ' (не запущена)' if interval else ''}",
        f"Автодействие: {action_label if armed else '—'}",
        f"Следующий запуск: {next_text}",
        "",
        "Анализ бота:",
        f"Последний анализ: {_msk_text(runtime, state.get('last_analysis_at'))}",
        f"Время анализа: {_elapsed(state.get('last_analysis_duration_seconds'))}",
        f"Score сырья XAU/XAG/USOIL: {_score_text(runtime.commodity_score(chat_id))}",
        f"Сетапов: {ss['issued']} | Активировались: {ss['activated']} | Закрыто: {ss['closed']}",
        f"TP: {ss['tp']} | стоп: {ss['stop']} | таймаут: {ss['timeout']}",
        f"Win rate: {win_rate} | Средний: {avg_r} | PF: {pf}",
        "",
        "Последний parquet:",
        f"Время: {_msk_text(runtime, state.get('last_parquet_at'))}",
        f"Сбор: {_elapsed(state.get('last_parquet_duration_seconds'))}",
        f"Файл: {state.get('last_archive_name') or '—'}",
        f"Размер: {_human_bytes(state.get('last_archive_size_bytes'))}",
        f"Binance Spot: {state.get('last_binance_full_count') if state.get('last_binance_full_count') is not None else '—'}/100 полных × 999 1H",
        f"Частичная история: {state.get('last_binance_partial_count') if state.get('last_binance_partial_count') is not None else '—'}",
        "Доп. данные: 365×1D · 288×15m · Spot depth · breadth · MEXC OI/basis · BTC/ETH options",
        f"MEXC funding: {state.get('last_mexc_funding_coverage') if state.get('last_mexc_funding_coverage') is not None else '—'}/100 контрактов",
        f"XAU/XAG/USOIL: {state.get('last_commodities_ok_count') if state.get('last_commodities_ok_count') is not None else '—'}/3 полных × 999 1H",
        f"Gmail: {runtime.gmail.status_text()} · последний архив: {state.get('last_gmail_status') or '—'}",
        f"Архивов в Telegram: {state.get('telegram_archives_sent', 0)} | Gmail: {state.get('gmail_archives_sent', 0)}",
        "",
        f"Память: {mem:.1f} МБ" if mem is not None else "Память: —",
        "Часовой пояс: МСК (UTC+3)",
    ]
    runtime.save_state()
    await update.effective_message.reply_text("\n".join(lines), reply_markup=_keyboard(runtime, chat_id))


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    log.info("ping requested chat_id=%s", chat_id)
    started = time.perf_counter()
    try:
        await context.bot.get_me()
        latency_ms = (time.perf_counter() - started) * 1000
        latency = f"{latency_ms:.0f} мс"
    except Exception:
        latency = "ошибка"
    mem = _rss_mb()
    await update.effective_message.reply_text(
        f"Версия бота: {BOT_VERSION}\n"
        f"Время отклика: {latency}\n"
        f"Время работы: {_uptime(time.monotonic() - PROCESS_STARTED)}\n"
        f"Память: {mem:.1f} МБ" if mem is not None else
        f"Версия бота: {BOT_VERSION}\nВремя отклика: {latency}\nВремя работы: {_uptime(time.monotonic() - PROCESS_STARTED)}\nПамять: —",
        reply_markup=_keyboard(runtime, chat_id),
    )


async def _progress_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, holder: dict[str, Any]) -> None:
    now = time.monotonic()
    # Avoid Telegram edit spam: the collector can emit several stages quickly.
    if holder.get("last_edit") and now - float(holder["last_edit"]) < 1.0:
        holder["pending"] = text
        return
    holder["last_edit"] = now
    msg = holder.get("message")
    if msg is None:
        holder["message"] = await _telegram_progress_once(
            "parquet progress",
            lambda: context.bot.send_message(
                chat_id=chat_id,
                text=f"⏳ {text}",
                connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
            ),
        )
        return
    try:
        await msg.edit_text(f"⏳ {text}")
    except Exception:
        pass


async def _send_archive_then_gmail(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    runtime: Runtime,
    result: ArchiveBuildResult,
) -> str:
    path = result.archive_path
    log.info("archive_delivery started chat_id=%s filename=%s size=%s", chat_id, path.name, result.archive_size_bytes)
    if result.archive_size_bytes > runtime.settings.telegram_send_limit_mb * 1024 * 1024:
        raise RuntimeError(
            f"Архив {_human_bytes(result.archive_size_bytes)} превышает лимит Telegram "
            f"{runtime.settings.telegram_send_limit_mb} МБ"
        )

    # User-required order: Telegram first, Gmail second. The expected ZIP
    # identity was already computed by the collector, so Gmail can still verify
    # that the local bytes are exactly the same after Telegram delivery.
    identity = ArchiveIdentity(
        name=path.name,
        size=result.archive_size_bytes,
        sha256=result.archive_sha256,
    )

    telegram_name = path.name
    await _telegram_progress_once(
        "parquet upload action",
        lambda: context.bot.send_chat_action(
            chat_id=chat_id,
            action=ChatAction.UPLOAD_DOCUMENT,
            connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
        ),
    )

    async def _send_archive_once() -> Any:
        with path.open("rb") as fh:
            return await context.bot.send_document(
                chat_id=chat_id,
                document=fh,
                filename=telegram_name,
                caption=(
                    f"Market Scan {BOT_VERSION} · 999×1H + 365×1D + 288×15m\n"
                    f"Binance Spot: {result.binance_full_count}/100 полных 1H · "
                    f"MEXC funding: {result.mexc_funding_coverage}/100 · commodities: {result.commodities_ok_count}/3\n"
                    "ZIP: breadth + Spot depth + MEXC OI/basis/funding + BTC/ETH options + prompt."
                ),
                connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
            )

    sent = await _telegram_delivery_with_retry("parquet archive", _send_archive_once)
    if sent is None:
        log.error("archive_delivery skipped chat_id=%s filename=%s reason=telegram_connect_failed", chat_id, telegram_name)
        return "TELEGRAM_FAILED"

    delivered_name = getattr(getattr(sent, "document", None), "file_name", None) or telegram_name
    if delivered_name != telegram_name:
        raise RuntimeError(f"Telegram изменил имя файла: {delivered_name} != {telegram_name}")
    log.info("archive_delivery telegram_sent chat_id=%s filename=%s", chat_id, delivered_name)

    state = runtime.chat_state(chat_id)
    state["telegram_archives_sent"] = int(state.get("telegram_archives_sent", 0)) + 1
    runtime.save_state()

    if not runtime.settings.gmail_auto_send_archives:
        log.info("archive_delivery gmail_skipped chat_id=%s reason=auto_send_off", chat_id)
        return "AUTO_SEND_OFF"
    if not runtime.gmail.connected:
        log.info("archive_delivery gmail_skipped chat_id=%s reason=not_connected", chat_id)
        return "NOT_CONNECTED"
    log.info("archive_delivery gmail_started chat_id=%s filename=%s", chat_id, path.name)
    gmail_result = await runtime.gmail.send_archive(
        path,
        subject_prefix=f"Market Scan {BOT_VERSION}",
        expected_identity=identity,
        telegram_filename=delivered_name,
    )
    if gmail_result.get("duplicate_skipped"):
        return f"DUPLICATE:{gmail_result.get('status') or 'unknown'}"
    state["gmail_archives_sent"] = int(state.get("gmail_archives_sent", 0)) + 1
    runtime.save_state()
    log.info("archive_delivery gmail_sent chat_id=%s filename=%s", chat_id, path.name)
    return "SENT"


async def _safe_delete(message: Any) -> None:
    if message is None:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def _perform_analysis(application: Application, chat_id: int, *, show_progress: bool) -> None:
    """Run the free local trading analysis from scratch and send only the verdict."""
    runtime: Runtime = application.bot_data["runtime"]
    lock = runtime.lock(chat_id)
    async with lock:
        state = runtime.chat_state(chat_id)
        state["scan_busy"] = True
        started = time.monotonic()
        log.info("analysis started chat_id=%s show_progress=%s", chat_id, show_progress)
        progress = None
        semaphore_acquired = False
        try:
            if show_progress:
                progress = await _telegram_progress_once(
                    "analysis progress",
                    lambda: application.bot.send_message(
                        chat_id=chat_id,
                        text="⏳ Сканирую рынок…",
                        connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                    ),
                )
            await runtime.scan_semaphore.acquire()
            semaphore_acquired = True
            bundle = await fetch_market_bundle()
            stats = ensure_stats(state.get("signal_stats"))
            state["signal_stats"] = stats
            try:
                histories = await fetch_signal_histories(stats.get("records", []))
                reconcile_stats(stats, histories)
            except Exception as exc:  # noqa: BLE001
                log.warning("Signal stats reconciliation skipped: %s", exc)

            commodity_score_threshold = runtime.commodity_score(chat_id)
            log.info("analysis commodity_score_threshold chat_id=%s value=%.3f", chat_id, commodity_score_threshold)
            verdict, selected = build_final_analysis(
                bundle,
                commodity_score_threshold=commodity_score_threshold,
            )
            for category, signal in selected.items():
                log.info(
                    "local_analysis selected category=%s asset=%s direction=%s status=%s score=%.3f completion_pct=%s rr=%s",
                    category, signal.asset, signal.direction, signal.status, float(signal.score),
                    signal.completion_pct, signal.rr,
                )
            await _safe_delete(progress)
            delivered = await _telegram_delivery_with_retry(
                "analysis verdict",
                lambda: application.bot.send_message(
                    chat_id=chat_id,
                    text=verdict,
                    parse_mode="HTML",
                    reply_markup=_keyboard(runtime, chat_id),
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                ),
            )
            if delivered is None:
                log.error("analysis delivery skipped chat_id=%s; current round result discarded", chat_id)
                return
            register_selected_signals(stats, selected, bundle, now_ts=time.time())
            state["signal_stats"] = stats
            log.info("Completed self-analysis chat_id=%s", chat_id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Self-analysis failed chat=%s", chat_id)
            await _safe_delete(progress)
            await _telegram_delivery_with_retry(
                "analysis error",
                lambda: application.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "Сканирование не удалось. Старые сигналы не подставляю.\n"
                        f"Ошибка источника данных: {type(exc).__name__}."
                    ),
                    reply_markup=_keyboard(runtime, chat_id),
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                ),
            )
        finally:
            if semaphore_acquired:
                runtime.scan_semaphore.release()
            finished = time.time()
            duration = max(0.0, time.monotonic() - started)
            state["scan_busy"] = False
            state["last_completed_at"] = finished
            state["last_duration_seconds"] = duration
            state["last_action"] = "Анализ"
            state["last_analysis_at"] = finished
            state["last_analysis_duration_seconds"] = duration
            runtime.save_state()
            log.info("analysis finished chat_id=%s duration_sec=%.3f", chat_id, duration)


async def _perform_parquet(application: Application, chat_id: int, *, show_progress: bool) -> None:
    """Build a fresh Parquet archive, send ZIP to Telegram, then .zip.jpg to Gmail."""
    runtime: Runtime = application.bot_data["runtime"]
    lock = runtime.lock(chat_id)
    async with lock:
        state = runtime.chat_state(chat_id)
        state["scan_busy"] = True
        started = time.monotonic()
        log.info("parquet started chat_id=%s show_progress=%s", chat_id, show_progress)
        holder: dict[str, Any] = {}
        semaphore_acquired = False
        try:
            if show_progress:
                holder["message"] = await _telegram_progress_once(
                    "parquet progress start",
                    lambda: application.bot.send_message(
                        chat_id=chat_id,
                        text="⏳ Собираю свежий Parquet без кэша…",
                        connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                    ),
                )
                holder["last_edit"] = time.monotonic()
            await runtime.scan_semaphore.acquire()
            semaphore_acquired = True
            async def progress(text: str) -> None:
                log.info("parquet progress chat_id=%s stage=%s", chat_id, text)
                if show_progress:
                    await _progress_message(application, chat_id, text, holder)  # type: ignore[arg-type]

            result = await build_market_archive(runtime.settings, progress=progress)
            state["scans_created"] = int(state.get("scans_created", 0)) + 1
            state["last_archive_name"] = result.archive_path.name
            state["last_archive_size_bytes"] = result.archive_size_bytes
            state["last_universe_count"] = result.universe_count
            state["last_binance_full_count"] = result.binance_full_count
            state["last_binance_partial_count"] = result.binance_partial_count
            state["last_mexc_funding_coverage"] = result.mexc_funding_coverage
            state["last_commodities_ok_count"] = result.commodities_ok_count
            runtime.save_state()
            log.info(
                "parquet archive_built chat_id=%s filename=%s size=%s universe=%s binance_full=%s funding=%s commodities=%s",
                chat_id, result.archive_path.name, result.archive_size_bytes, result.universe_count,
                result.binance_full_count, result.mexc_funding_coverage, result.commodities_ok_count,
            )

            await _safe_delete(holder.get("message"))
            gmail_status = await _send_archive_then_gmail(application, chat_id, runtime, result)  # type: ignore[arg-type]
            state["last_gmail_status"] = gmail_status
            runtime.save_state()
            if gmail_status == "TELEGRAM_FAILED":
                log.error("parquet delivery skipped chat_id=%s; current round result discarded", chat_id)
                return

            if gmail_status == "SENT":
                follow = "📧 Следом отправлено в Gmail как .zip.jpg."
            elif gmail_status == "NOT_CONNECTED":
                follow = "⚠️ Gmail не подключён. ZIP остался в Telegram. Нажми «Почта»."
            elif gmail_status.startswith("DUPLICATE"):
                follow = f"♻️ Gmail повтор заблокирован: {gmail_status}."
            else:
                follow = f"⚠️ Gmail: {gmail_status}."
            await _telegram_delivery_with_retry(
                "parquet summary",
                lambda: application.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✅ Parquet собран за {_elapsed(result.duration_seconds)}. {follow}\n"
                        f"Архив: {_human_bytes(result.archive_size_bytes)} · SHA-256 {result.archive_sha256[:12]}…"
                    ),
                    reply_markup=_keyboard(runtime, chat_id),
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                ),
            )
        except (GmailAttachmentTooLarge, GmailArchiveChanged, GmailSendUncertain, GmailOAuthError) as exc:
            log.exception("Gmail delivery failed chat=%s", chat_id)
            state["last_gmail_status"] = f"ERROR:{type(exc).__name__}"
            await _telegram_delivery_with_retry(
                "parquet gmail error",
                lambda: application.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ ZIP в Telegram отправлен, но Gmail завершился ошибкой: {exc}",
                    reply_markup=_keyboard(runtime, chat_id),
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Market parquet build failed chat=%s", chat_id)
            await _safe_delete(holder.get("message"))
            await _telegram_delivery_with_retry(
                "parquet error",
                lambda: application.bot.send_message(
                    chat_id=chat_id,
                    text=f"❌ Parquet не собран: {type(exc).__name__}: {str(exc)[:500]}",
                    reply_markup=_keyboard(runtime, chat_id),
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT_SECONDS,
                ),
            )
        finally:
            if semaphore_acquired:
                runtime.scan_semaphore.release()
            finished = time.time()
            duration = max(0.0, time.monotonic() - started)
            state["scan_busy"] = False
            state["last_completed_at"] = finished
            state["last_duration_seconds"] = duration
            state["last_action"] = "Parquet"
            state["last_parquet_at"] = finished
            state["last_parquet_duration_seconds"] = duration
            runtime.save_state()
            log.info("parquet finished chat_id=%s duration_sec=%.3f", chat_id, duration)


def _cancel_auto(runtime: Runtime, chat_id: int) -> None:
    task = runtime.auto_tasks.get(chat_id)
    if task is None or task.done():
        runtime.auto_tasks.pop(chat_id, None)
        return
    if chat_id in runtime.auto_scanning:
        return
    task.cancel()
    runtime.auto_tasks.pop(chat_id, None)


def _schedule_auto(application: Application, chat_id: int) -> None:
    runtime: Runtime = application.bot_data["runtime"]
    state = runtime.chat_state(chat_id)
    _, seconds = runtime.mode(chat_id)
    if not state.get("armed") or seconds <= 0 or state.get("auto_action") not in {"analysis", "parquet"}:
        return
    existing = runtime.auto_tasks.get(chat_id)
    if existing is not None and not existing.done():
        return
    runtime.auto_tasks[chat_id] = asyncio.create_task(_auto_loop(application, chat_id), name=f"auto-action-{chat_id}")
    log.info("timer scheduled chat_id=%s interval_sec=%s action=%s", chat_id, seconds, state.get("auto_action"))


async def _auto_loop(application: Application, chat_id: int) -> None:
    runtime: Runtime = application.bot_data["runtime"]
    try:
        while True:
            state = runtime.chat_state(chat_id)
            _, interval = runtime.mode(chat_id)
            action = state.get("auto_action")
            if not state.get("armed") or interval <= 0 or action not in {"analysis", "parquet"}:
                return
            last = state.get("last_completed_at")
            delay = max(0.0, float(last) + interval - time.time()) if isinstance(last, (int, float)) else float(interval)
            log.info("timer waiting chat_id=%s delay_sec=%.3f action=%s", chat_id, delay, action)
            await asyncio.sleep(delay)
            state = runtime.chat_state(chat_id)
            _, interval = runtime.mode(chat_id)
            action = state.get("auto_action")
            if not state.get("armed") or interval <= 0 or action not in {"analysis", "parquet"}:
                return
            runtime.auto_scanning.add(chat_id)
            log.info("timer triggered chat_id=%s action=%s", chat_id, action)
            try:
                if action == "analysis":
                    await _perform_analysis(application, chat_id, show_progress=False)
                else:
                    await _perform_parquet(application, chat_id, show_progress=False)
            except NetworkError as exc:
                log.warning(
                    "timer telegram network error swallowed chat_id=%s action=%s error_type=%s",
                    chat_id, action, type(exc).__name__,
                )
            finally:
                runtime.auto_scanning.discard(chat_id)
    except asyncio.CancelledError:
        raise
    finally:
        if runtime.auto_tasks.get(chat_id) is asyncio.current_task():
            runtime.auto_tasks.pop(chat_id, None)


async def _manual_action(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    now = time.monotonic()
    last = runtime.last_scan_started.get(chat_id)
    if last is not None and now - last < runtime.settings.scan_cooldown_seconds:
        await update.effective_message.reply_text(
            f"Подожди {int(runtime.settings.scan_cooldown_seconds - (now - last)) + 1} сек перед повторным ручным запуском.",
            reply_markup=_keyboard(runtime, chat_id),
        )
        return
    runtime.last_scan_started[chat_id] = now
    _cancel_auto(runtime, chat_id)
    _, interval = runtime.mode(chat_id)
    state = runtime.chat_state(chat_id)
    state["auto_action"] = action
    state["armed"] = interval > 0
    runtime.save_state()
    if action == "analysis":
        await _perform_analysis(context.application, chat_id, show_progress=True)
    else:
        await _perform_parquet(context.application, chat_id, show_progress=True)
    _schedule_auto(context.application, chat_id)


async def analysis(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _manual_action(update, context, "analysis")


async def parquet_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _manual_action(update, context, "parquet")


async def time_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    state = runtime.chat_state(chat_id)
    state["mode_index"] = (int(state.get("mode_index", 0) or 0) + 1) % len(TIME_MODES)
    label, seconds = runtime.mode(chat_id)
    state["armed"] = False
    state["auto_action"] = None
    _cancel_auto(runtime, chat_id)
    if seconds == 0:
        text = "Время выкл. «Анализ» и «Parquet» выполняются по одному разу."
    else:
        text = (
            f"Выбрано: Время {label}. Теперь нажми «Анализ» или «Parquet». "
            "Бот будет повторять именно выбранное действие; новый интервал отсчитывается после полного завершения предыдущего запуска."
        )
    runtime.save_state()
    log.info("timer mode_changed chat_id=%s label=%s interval_sec=%s", chat_id, label, seconds)
    await update.effective_message.reply_text(text, reply_markup=_keyboard(runtime, chat_id))


# ------------------------- Gmail session import UI -------------------------

def _gmail_connected_menu(runtime: Runtime) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧪 Отправить тест", callback_data=BTN_GMAIL_TEST)],
        [InlineKeyboardButton("♻️ Заменить авторизацию", callback_data=BTN_GMAIL_IMPORT)],
        [InlineKeyboardButton("❌ Отключить Gmail", callback_data=BTN_GMAIL_DISCONNECT)],
    ])


async def _request_gmail_import(message: Any, runtime: Runtime, user_id: int | None, chat_id: int) -> None:
    if user_id is not None:
        runtime.awaiting[user_id] = {"step": "gmail_import_bundle"}
    log.info("gmail_import requested chat_id=%s", chat_id)
    await message.reply_text(
        f"📥 Gmail import · {BOT_VERSION}\n"
        "Отправь ОДНИМ сообщением сохранённую Base64-строку авторизации из старого бота.\n"
        "Сообщение со строкой удалю сразу после получения. Авторизация действует до следующего redeploy.\n"
        "/cancel — отмена."
    )


async def gmail_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    if runtime.gmail.connected:
        await update.effective_message.reply_text(
            f"✅ Gmail подключён: {runtime.gmail.account_email}\n"
            f"Версия: {BOT_VERSION}\n"
            "Режим: сессионный — после redeploy импортируй ту же строку снова.\n"
            "Архив: сначала .zip в Telegram, затем те же ZIP-байты в Gmail как .zip.jpg.",
            reply_markup=_gmail_connected_menu(runtime),
        )
        return
    await _request_gmail_import(
        update.effective_message,
        runtime,
        update.effective_user.id if update.effective_user else None,
        chat_id,
    )


async def gmail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    query = update.callback_query
    if not query or not update.effective_chat:
        return
    await query.answer()
    chat_id = update.effective_chat.id
    data = query.data or ""

    if data == BTN_GMAIL_IMPORT:
        await _request_gmail_import(
            query.message,
            runtime,
            update.effective_user.id if update.effective_user else None,
            chat_id,
        )
        return

    if data == BTN_GMAIL_TEST:
        try:
            await runtime.gmail.send_test()
            await query.message.reply_text(f"✅ Тестовое письмо отправлено: {runtime.gmail.account_email}")
        except Exception as exc:  # noqa: BLE001
            log.warning("gmail_test failed chat_id=%s error_type=%s", chat_id, type(exc).__name__)
            await query.message.reply_text(f"❌ Gmail test: {type(exc).__name__}")
        return

    if data == BTN_GMAIL_DISCONNECT:
        runtime.gmail.disconnect()
        if update.effective_user:
            runtime.awaiting.pop(update.effective_user.id, None)
        log.info("gmail_session disconnected chat_id=%s", chat_id)
        await query.message.reply_text(
            "Gmail отключён. Нажми «Почта» и снова отправь сохранённую строку авторизации."
        )
        return

    if data in STALE_GMAIL_CALLBACKS or data.startswith("gmail_"):
        if update.effective_user:
            runtime.awaiting.pop(update.effective_user.id, None)
        log.info("stale_gmail_callback blocked chat_id=%s data=%s", chat_id, data)
        await query.message.reply_text(
            f"Эта кнопка от старой версии. В {BOT_VERSION} callback/OAuth отключён. "
            "Нажми обычную кнопку «Почта» и отправь сохранённую строку авторизации."
        )
        return


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if update.effective_user:
        runtime.awaiting.pop(update.effective_user.id, None)
    log.info("interactive flow cancelled chat_id=%s", update.effective_chat.id if update.effective_chat else None)
    await update.effective_message.reply_text("Отменено.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    score = runtime.commodity_score(chat_id)
    log.info("help requested chat_id=%s commodity_score=%.3f", chat_id, score)
    text = (
        f"Trading + Market Data Bot {BOT_VERSION}\n\n"
        "Локальный Анализ:\n"
        "• Крипто: Binance Spot 1D/4H/1H/15m, breadth, Spot depth, MEXC funding/OI/basis, RSS/news и календарь USD-событий.\n"
        "• Сырьё: MEXC Futures XAU_USDT / SILVER_USDT / USOIL_USDT на 1D/4H/1H/15m.\n"
        "• Крипто score: фиксированный порог 7.0.\n"
        f"• Сырьевой score: сейчас {_score_text(score)}; по умолчанию {_score_text(DEFAULT_COMMODITY_SCORE)}.\n\n"
        "Команды:\n"
        "/score_s — показать текущий сырьевой score.\n"
        "/score_s 4.5 — поставить один порог для XAU/XAG/USOIL.\n"
        f"/score_s reset — вернуть {_score_text(DEFAULT_COMMODITY_SCORE)}.\n"
        "Чем ниже score, тем больше сетапов и ниже строгость отбора; чем выше — тем меньше и строже.\n"
        "/log_full — подробный журнал только за последние 24 часа.\n"
        "/status — состояние бота и последнего анализа/Parquet.\n"
        "/scan — Анализ; /parquet — собрать Parquet; /ping — проверка бота; /gmail — подключение почты.\n\n"
        "Parquet: top-100 Binance Spot, 1H/1D/15m, Spot depth, breadth, MEXC funding/derivatives, XAU/XAG/USOIL и optional Deribit BTC/ETH options."
    )
    await update.effective_message.reply_text(text, reply_markup=_keyboard(runtime, chat_id))


async def score_s(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    current = runtime.commodity_score(chat_id)
    args = list(context.args or [])
    if not args:
        await update.effective_message.reply_text(
            f"Score сырья XAU/XAG/USOIL: {_score_text(current)}\n"
            f"По умолчанию: {_score_text(DEFAULT_COMMODITY_SCORE)}\n"
            "Изменить: /score_s 4.5\n"
            "Сбросить: /score_s reset",
            reply_markup=_keyboard(runtime, chat_id),
        )
        return

    raw = args[0].strip().lower()
    if raw in {"reset", "default", "сброс"}:
        value = float(DEFAULT_COMMODITY_SCORE)
    else:
        try:
            value = float(raw.replace(",", "."))
        except ValueError:
            await update.effective_message.reply_text(
                "Неверное значение. Пример: /score_s 4.5",
                reply_markup=_keyboard(runtime, chat_id),
            )
            return
        if not math.isfinite(value) or not SCORE_S_MIN <= value <= SCORE_S_MAX:
            await update.effective_message.reply_text(
                f"Score должен быть от {SCORE_S_MIN:.1f} до {SCORE_S_MAX:.1f}. Пример: /score_s 4.5",
                reply_markup=_keyboard(runtime, chat_id),
            )
            return

    new_score = runtime.set_commodity_score(chat_id, value)
    log.info(
        "commodity_score changed chat_id=%s old=%.3f new=%.3f default=%.3f",
        chat_id, current, new_score, float(DEFAULT_COMMODITY_SCORE),
    )
    await update.effective_message.reply_text(
        f"✅ Score сырья XAU/XAG/USOIL: {_score_text(new_score)}\n"
        "Новый порог применяется со следующего Анализа.",
        reply_markup=_keyboard(runtime, chat_id),
    )


async def log_mail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    try:
        report = await asyncio.to_thread(runtime.gmail.build_diagnostic_report, chat_id)
        with report.open("rb") as fh:
            await context.bot.send_document(chat_id=chat_id, document=fh, filename=report.name)
    except Exception as exc:  # noqa: BLE001
        await update.effective_message.reply_text(f"❌ log_mail: {exc}")


async def log_full(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    chat_id = update.effective_chat.id
    log.info("full_log requested chat_id=%s", chat_id)
    try:
        report = await asyncio.to_thread(
            build_full_log_report,
            runtime.full_log_path,
            runtime.settings.exports_dir,
            hours=FULL_LOG_HOURS,
        )
        with report.open("rb") as fh:
            await context.bot.send_document(chat_id=chat_id, document=fh, filename=report.name)
        log.info("full_log sent chat_id=%s filename=%s size=%s", chat_id, report.name, report.stat().st_size)
    except Exception as exc:  # noqa: BLE001
        log.exception("full_log failed chat_id=%s", chat_id)
        await update.effective_message.reply_text(f"❌ log_full: {type(exc).__name__}")


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime = _runtime(context)
    if not _allowed(update, runtime):
        return
    text = (update.effective_message.text or "").strip()
    user = update.effective_user
    if user and user.id in runtime.awaiting:
        flow = runtime.awaiting[user.id]
        step = flow.get("step")
        try:
            await update.effective_message.delete()
        except Exception:
            pass
        if step == "gmail_import_bundle":
            log.info(
                "Gmail session import payload received chat_id=%s user_id=%s chars=%s content=REDACTED",
                update.effective_chat.id,
                user.id,
                len(text),
            )
            try:
                runtime.secret_store.import_gmail_runtime_bundle(text)
                email_value = await runtime.gmail.verify_connection()
                runtime.awaiting.pop(user.id, None)
                log.info("Gmail session import verified chat_id=%s email=%s", update.effective_chat.id, email_value)
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=(
                        f"✅ Почта подключена: {email_value}\n"
                        "Авторизация хранится только в текущей сессии и исчезнет после redeploy."
                    ),
                    reply_markup=_keyboard(runtime, update.effective_chat.id),
                )
            except Exception as exc:  # noqa: BLE001
                runtime.secret_store.clear_runtime_gmail()
                log.warning(
                    "Gmail session import failed chat_id=%s error_type=%s",
                    update.effective_chat.id,
                    type(exc).__name__,
                )
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Импорт Gmail не принят: {type(exc).__name__}. Отправь строку ещё раз или /cancel.",
                )
            return


    low = text.lower()
    if text == "1" or low == "анализ":
        await analysis(update, context)
    elif low in {"parquet", "отправить parquet"}:
        await parquet_export(update, context)
    elif low.startswith("время"):
        await time_toggle(update, context)
    elif low in {"почта", "подключить почту"}:
        await gmail_command(update, context)
    elif low == "пинг":
        await ping(update, context)
    else:
        await update.effective_message.reply_text(
            "Используй «Анализ», «Время», «Parquet», «Почта», «Пинг» или отправь 1.",
            reply_markup=_keyboard(runtime, update.effective_chat.id),
        )


async def post_init(application: Application) -> None:
    runtime: Runtime = application.bot_data["runtime"]
    await runtime.gmail.start_web_server(application.bot)
    # Restore armed timers after a Coolify restart. The next run remains based
    # on last_completed_at, not process startup time.
    for key, state in list(runtime.state.items()):
        try:
            chat_id = int(key)
        except Exception:
            continue
        _, seconds = runtime.mode(chat_id)
        if state.get("armed") and seconds > 0:
            _schedule_auto(application, chat_id)


async def post_shutdown(application: Application) -> None:
    runtime: Runtime = application.bot_data["runtime"]
    for task in list(runtime.auto_tasks.values()):
        task.cancel()
    await runtime.gmail.stop_web_server()


def main() -> None:
    settings = load_settings()
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is empty")
    full_log_path = enable_full_file_logging(settings.logs_dir, LOG_LEVEL)
    log.info("Full operation logging enabled path=%s", full_log_path)
    runtime = Runtime(settings)
    app = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["runtime"] = runtime
    app.add_handler(TypeHandler(Update, incoming_update_audit), group=-1)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("score_s", score_s))
    app.add_handler(CommandHandler("scan", analysis))
    app.add_handler(CommandHandler("parquet", parquet_export))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("gmail", gmail_command))
    app.add_handler(CommandHandler("log_mail", log_mail))
    app.add_handler(CommandHandler("log_full", log_full))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CallbackQueryHandler(gmail_callback, pattern="^gmail_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(telegram_error_handler)
    log.info("Trading + Market Data Bot started version=%s gmail_mode=session_import_only log_full=enabled", BOT_VERSION)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
