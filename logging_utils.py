from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Iterable


# Defense-in-depth redaction. Sensitive Telegram import payloads are never passed
# to loggers in the first place; these patterns protect against accidental
# inclusion by exception text or third-party libraries.
_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)(Authorization\s*[:=]\s*[\"']?Bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)([\"']authorization[\"']\s*:\s*[\"']Bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1<REDACTED>"),
    (re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]{16,}"), r"\1<REDACTED>"),
    (re.compile(r"(?i)([\"']?(?:refresh_token|access_token|client_secret|fernet_key|secret_encryption_key|telegram_bot_token|api_secret)[\"']?\s*[:=]\s*[\"']?)([^\"'\s,}\]]+)"), r"\1<REDACTED>"),
    (re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"), "<TELEGRAM_TOKEN_REDACTED>"),
    (re.compile(r"\bGOCSPX-[A-Za-z0-9_-]{8,}\b"), "<GOOGLE_CLIENT_SECRET_REDACTED>"),
    (re.compile(r"\bgAAAAA[A-Za-z0-9_-]{40,}\b"), "<FERNET_TOKEN_REDACTED>"),
    # The import bundle is a long Base64 blob. It should never be logged, but
    # redact any such accidental blob if one reaches a formatter.
    (re.compile(r"(?<![A-Za-z0-9+/=_-])[A-Za-z0-9+/=_-]{180,}(?![A-Za-z0-9+/=_-])"), "<LONG_SECRET_REDACTED>"),
)

_LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+\|")


def redact_text(value: str) -> str:
    text = str(value)
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


class RedactingFormatter(logging.Formatter):
    # Keep file/console timestamps deterministic and aligned with the UTC API
    # timestamps used elsewhere in diagnostics.
    converter = __import__("time").gmtime

    def format(self, record: logging.LogRecord) -> str:
        return redact_text(super().format(record))


def _formatter() -> RedactingFormatter:
    return RedactingFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def configure_console_logging(level_name: str = "INFO") -> None:
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(_formatter())
        root.addHandler(handler)
    else:
        for handler in root.handlers:
            handler.setLevel(level)
            handler.setFormatter(_formatter())


def enable_full_file_logging(logs_dir: Path, level_name: str = "INFO") -> Path:
    """Enable hourly log rotation and keep roughly one day on disk.

    /log_full applies an exact timestamp filter, so the exported report is
    strictly limited to the requested retention window even across rotations.
    """
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / "full.log"
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    resolved = str(path.resolve())
    for handler in root.handlers:
        if isinstance(handler, TimedRotatingFileHandler) and str(Path(handler.baseFilename).resolve()) == resolved:
            return path

    handler = TimedRotatingFileHandler(
        path,
        when="H",
        interval=1,
        backupCount=24,
        encoding="utf-8",
        utc=True,
    )
    handler.setLevel(level)
    handler.setFormatter(_formatter())
    root.addHandler(handler)

    # Remove stale legacy size-rotation files from versions that used .1 ... .5
    # once they are already older than the one-day diagnostic window.
    cutoff = datetime.now(timezone.utc).timestamp() - 24 * 3600
    for idx in range(1, 6):
        legacy = path.with_name(f"{path.name}.{idx}")
        try:
            if legacy.is_file() and legacy.stat().st_mtime < cutoff:
                legacy.unlink()
        except OSError:
            pass
    return path


def _rotated_files(path: Path) -> Iterable[Path]:
    """Yield all current/rotated full.log files in chronological order."""
    candidates = [p for p in path.parent.glob(f"{path.name}*") if p.is_file()]
    for candidate in sorted(candidates, key=lambda p: (p.stat().st_mtime, p.name)):
        yield candidate


def _timestamped_blocks(text: str) -> list[tuple[datetime | None, list[str]]]:
    """Split a log into timestamped records, keeping traceback continuation lines."""
    blocks: list[tuple[datetime | None, list[str]]] = []
    current_ts: datetime | None = None
    current_lines: list[str] = []
    for line in text.splitlines():
        match = _LOG_TS_RE.match(line)
        if match:
            if current_lines:
                blocks.append((current_ts, current_lines))
            try:
                current_ts = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                current_ts = None
            current_lines = [line]
        else:
            if current_lines:
                current_lines.append(line)
    if current_lines:
        blocks.append((current_ts, current_lines))
    return blocks


def build_full_log_report(
    log_path: Path,
    exports_dir: Path,
    *,
    hours: int = 24,
    max_bytes: int = 20 * 1024 * 1024,
) -> Path:
    """Build /log_full containing only records from the last N hours."""
    exports_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max(1, int(hours)))
    stamp = now.strftime("%Y%m%d_%H%M%S")
    report = exports_dir / f"full_log_{stamp}.txt"
    chunks: list[str] = [
        f"FULL OPERATION LOG · LAST {max(1, int(hours))} HOURS",
        f"Window UTC: {cutoff:%Y-%m-%d %H:%M:%S} → {now:%Y-%m-%d %H:%M:%S}",
        "Sensitive OAuth/session data, encryption keys and access tokens are redacted or never logged.",
        "=" * 88,
    ]

    used = 0
    kept_records = 0
    for source in _rotated_files(log_path):
        try:
            text = source.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        selected_lines: list[str] = []
        for ts, block_lines in _timestamped_blocks(text):
            if ts is not None and ts >= cutoff:
                selected_lines.extend(block_lines)
                kept_records += 1
        if not selected_lines:
            continue
        rendered = redact_text("\n".join(selected_lines)).rstrip() + "\n"
        data = rendered.encode("utf-8")
        remaining = max_bytes - used
        if remaining <= 0:
            chunks.append("[log output truncated by max_bytes]")
            break
        if len(data) > remaining:
            # Prefer the most recent tail if an unusually noisy single rotation
            # exceeds the Telegram-friendly report size.
            data = data[-remaining:]
            rendered = data.decode("utf-8", errors="replace")
            chunks.append(f"\n--- {source.name} (24h tail; truncated) ---")
        else:
            chunks.append(f"\n--- {source.name} ---")
        chunks.append(rendered.rstrip())
        used += min(len(data), remaining)

    if kept_records == 0:
        chunks.append(f"No operations were logged in the last {max(1, int(hours))} hours.")
    report.write_text("\n".join(chunks).rstrip() + "\n", encoding="utf-8")
    return report
