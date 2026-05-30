from __future__ import annotations

import base64
import asyncio
import contextvars
import gc
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import subprocess
import tempfile
import time
import uuid
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
import ipaddress
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import JSONResponse

import bot
import classops_intelligence as classops_ai
import document_service as docs
import dropbox_service as dropbox


APP_DIR = Path(__file__).resolve().parent
PWA_DIR = APP_DIR / "pwa"

app = FastAPI(title="H.I.R.A OS")
app.mount("/static", StaticFiles(directory=str(PWA_DIR)), name="static")

PWA_APP_VERSION = "20260530-stage3-situation-1"
PWA_SERVICE_WORKER_CACHE = "hira-os-v140"

try:
    _HOME_EXECUTOR_WORKERS = int(os.environ.get("HIRA_HOME_WORKERS", "4"))
except ValueError:
    _HOME_EXECUTOR_WORKERS = 4
_HOME_EXECUTOR_WORKERS = max(1, min(4, _HOME_EXECUTOR_WORKERS))
_HOME_EXECUTOR = ThreadPoolExecutor(max_workers=_HOME_EXECUTOR_WORKERS)
_CLASSOPS_REFRESH_FUTURE = None
_WEB_SCHEDULER_TASKS: list[asyncio.Task] = []
_WEB_MEMORY_WATCHDOG_TASK: asyncio.Task | None = None
_WEB_PUSH_RECOVERY_TASK: asyncio.Task | None = None


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default)) or default))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or default)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


_CHAT_SEMAPHORE = asyncio.Semaphore(_env_int("HIRA_WEB_CHAT_CONCURRENCY", 2))
_UPLOAD_SEMAPHORE = asyncio.Semaphore(_env_int("HIRA_WEB_UPLOAD_CONCURRENCY", 2))
_HOME_SEMAPHORE = asyncio.Semaphore(_env_int("HIRA_WEB_HOME_CONCURRENCY", 2))
_UPLOAD_QUEUE_WORKER_COUNT = _env_int("HIRA_WEB_UPLOAD_QUEUE_WORKERS", 2)
_UPLOAD_QUEUE_MAX = _env_int("HIRA_WEB_UPLOAD_QUEUE_MAX", 12)
_MAX_UPLOAD_BYTES = max(256_000, _env_int("HIRA_WEB_MAX_UPLOAD_MB", 16) * 1024 * 1024)
_MAX_DOCUMENT_BYTES = max(_MAX_UPLOAD_BYTES, _env_int("HIRA_WEB_MAX_DOCUMENT_MB", 96) * 1024 * 1024)
_MAX_REQUEST_BYTES = max(_MAX_DOCUMENT_BYTES, _env_int("HIRA_WEB_MAX_REQUEST_MB", 112) * 1024 * 1024)
_MEMORY_GC_RATIO = _env_float("HIRA_WEB_MEMORY_GC_RATIO", 0.80)
_MEMORY_REJECT_RATIO = _env_float("HIRA_WEB_MEMORY_REJECT_RATIO", 0.92)
_MEMORY_WATCHDOG_SECONDS = _env_int("HIRA_WEB_MEMORY_WATCHDOG_SECONDS", 45, minimum=10)
_CHAT_MAX_TOKENS = _env_int("HIRA_WEB_CHAT_MAX_TOKENS", 3200, minimum=650)
_AUTH_RATE_LIMIT = _env_int("HIRA_WEB_AUTH_RATE_LIMIT", 8)
_WEB_INLINE_SCHEDULER = _env_bool("HIRA_WEB_INLINE_SCHEDULER", False)
_WEB_PUSH_RECOVERY_ENABLED = _env_bool("HIRA_WEB_PUSH_RECOVERY_ENABLED", True)
_WEB_PUSH_RECOVERY_INTERVAL = _env_int("HIRA_WEB_PUSH_RECOVERY_SECONDS", 90, minimum=30)
_WEB_PUSH_RECOVERY_COOLDOWN = _env_int("HIRA_WEB_PUSH_RECOVERY_COOLDOWN_SECONDS", 300, minimum=60)
_WEB_PUSH_RECOVERY_MAX_AGE_HOURS = _env_int("HIRA_WEB_PUSH_RECOVERY_MAX_AGE_HOURS", 36, minimum=1)
_WEB_PUSH_RECOVERY_LIMIT = _env_int("HIRA_WEB_PUSH_RECOVERY_LIMIT", 3, minimum=1)
_HOME_PRIMARY_TIMEOUT_SECONDS = max(2.0, min(15.0, _env_float("HIRA_HOME_PRIMARY_TIMEOUT_SECONDS", 8.0)))
_HOME_SECONDARY_TIMEOUT_SECONDS = max(2.0, min(15.0, _env_float("HIRA_HOME_SECONDARY_TIMEOUT_SECONDS", 8.0)))
_STATIC_PATHS = {
    "/",
    "/growth",
    "/hira-growth",
    "/classops",
    "/healthz",
    "/manifest.webmanifest",
    "/service-worker.js",
    "/app.js",
    "/styles.css",
    "/hira-growth.css",
    "/hira-growth.js",
    "/hira-growth-data.json",
    "/classops.css",
    "/classops.js",
}
_UPLOAD_QUEUE: asyncio.Queue[dict] | None = None
_UPLOAD_QUEUE_TASKS: list[asyncio.Task] = []
_REQUEST_CONTEXT: contextvars.ContextVar[Request | None] = contextvars.ContextVar("hira_request", default=None)
_SESSION_COOKIE_NAME = "hira_session"
_SESSION_MAX_AGE_SECONDS = _env_int("HIRA_WEB_SESSION_MAX_AGE_SECONDS", 60 * 60 * 24 * 30)
_AUTH_NOT_CONFIGURED_DETAIL = "Web authentication is not configured."


def _running_in_production() -> bool:
    return bool(os.environ.get("RAILWAY_ENVIRONMENT", "").strip() or os.environ.get("RAILWAY_SERVICE_NAME", "").strip())


def _apply_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("X-Frame-Options", "DENY")
    if _running_in_production():
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=(self), payment=(), usb=()",
    )
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "script-src 'self' https://unpkg.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data: blob:; "
        "connect-src 'self'; "
        "manifest-src 'self'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "object-src 'none'; "
        "frame-ancestors 'none'",
    )
    return response


def _memory_usage_ratio() -> float | None:
    limit = bot._memory_limit_mb()
    if not limit:
        return None
    return bot._rss_mb() / limit


def _memory_pressure_high() -> bool:
    ratio = _memory_usage_ratio()
    return ratio is not None and ratio >= _MEMORY_REJECT_RATIO


def _is_supported_document(mime: str, filename: str) -> bool:
    name = (filename or "").lower()
    return (
        mime == "application/pdf"
        or mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        or mime == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        or name.endswith((".pdf", ".docx", ".pptx"))
    )


def _gmail_http_error(exc: Exception, account: str) -> HTTPException:
    """Turn Gmail provider failures into useful PWA errors instead of 500s."""
    label = bot.gs.gmail_label(account).title()
    raw = str(exc or "").strip()
    lower = raw.lower()
    status = 502
    hint = raw or "Gmail request failed."
    if any(term in lower for term in ("invalid_grant", "expired", "revoked", "unauthorized", "invalid credentials")):
        status = 424
        hint = f"{label} OAuth token is expired or revoked. Reconnect Gmail and update the refresh token."
    elif any(term in lower for term in ("access not configured", "gmail api has not been used", "api has not been enabled")):
        status = 400
        hint = "Gmail API is not enabled for this Google Cloud project."
    elif any(term in lower for term in ("insufficient permission", "insufficient authentication scopes", "forbidden")):
        status = 403
        hint = f"{label} is connected but does not have the required Gmail read/compose scope."
    elif "not configured" in lower:
        status = 400
        hint = f"{label} is not configured."
    return HTTPException(status_code=status, detail=hint[:600])


async def _web_memory_watchdog():
    _prune_tick = 0
    while True:
        try:
            ratio = _memory_usage_ratio()
            if ratio is not None and ratio >= _MEMORY_GC_RATIO:
                gc.collect()
                bot._log_memory(f"web watchdog pressure {ratio:.0%}", force=True)
            # Prune rate limiter buckets every ~5 minutes
            _prune_tick += 1
            if _prune_tick % max(1, (300 // _MEMORY_WATCHDOG_SECONDS)) == 0:
                await _CHAT_RATE_LIMITER.prune()
                await _UPLOAD_RATE_LIMITER.prune()
            await asyncio.sleep(_MEMORY_WATCHDOG_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web memory watchdog error: {exc}")
            await asyncio.sleep(_MEMORY_WATCHDOG_SECONDS)


@app.middleware("http")
async def add_static_cache_headers(request: Request, call_next):
    context_token = _REQUEST_CONTEXT.set(request)
    try:
        content_length = request.headers.get("content-length")
        if content_length and content_length.isdigit() and int(content_length) > _MAX_REQUEST_BYTES:
            return _apply_security_headers(JSONResponse(
                {"detail": f"Request is too large. Limit is {_MAX_REQUEST_BYTES // (1024 * 1024)} MB."},
                status_code=413,
            ))
        if request.url.path.startswith("/api/") and request.url.path not in {"/api/auth/session"}:
            expected = _expected_web_token()
            if not expected:
                return _apply_security_headers(JSONResponse(
                    {"detail": _AUTH_NOT_CONFIGURED_DETAIL},
                    status_code=503,
                ))
            header_ok = _token_matches(request.headers.get("x-hira-token"), expected)
            session_ok = _request_session_valid(request, expected)
            if session_ok and not header_ok and not _csrf_request_allowed(request):
                return _apply_security_headers(JSONResponse({"detail": "Cross-site request blocked"}, status_code=403))
            if not header_ok and not session_ok:
                if not await _AUTH_RATE_LIMITER.is_allowed(_auth_rate_key(request, request.headers.get("x-hira-token"))):
                    return _apply_security_headers(JSONResponse(
                        {"detail": "Too many invalid token attempts. Try again in a minute."},
                        status_code=429,
                        headers={"Retry-After": "60"},
                    ))
                return _apply_security_headers(JSONResponse({"detail": "Invalid H.I.R.A web token"}, status_code=401))
        is_static_path = request.url.path in _STATIC_PATHS or request.url.path.startswith("/static/")
        if _memory_pressure_high() and not is_static_path:
            gc.collect()
            return _apply_security_headers(JSONResponse(
                {"detail": "H.I.R.A is under memory pressure. Try again in a moment."},
                status_code=503,
                headers={"Retry-After": "20"},
            ))
        response = await call_next(request)
        if request.url.path in {
            "/",
            "/growth",
            "/hira-growth",
            "/classops",
            "/service-worker.js",
            "/app.js",
            "/styles.css",
            "/hira-growth.css",
            "/hira-growth.js",
            "/hira-growth-data.json",
            "/classops.css",
            "/classops.js",
            "/static/app.js",
            "/static/styles.css",
            "/static/hira-growth.css",
            "/static/hira-growth.js",
            "/static/hira-growth-data.json",
            "/static/classops.css",
            "/static/classops.js",
        }:
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return _apply_security_headers(response)
    finally:
        _REQUEST_CONTEXT.reset(context_token)


async def _web_daily_briefing_loop(
    hour: int,
    minute: int,
    sender,
    source: str,
    grace_minutes: int | None = None,
    retry_until_success: bool = False,
):
    last_attempt_date = None
    last_success_date = None
    while True:
        try:
            now = datetime.now(bot.SGT)
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            grace_until = target + bot.timedelta(minutes=grace_minutes or bot.DAILY_JOB_GRACE_MINUTES)
            today_key = now.strftime("%Y-%m-%d")
            if retry_until_success:
                should_run = target <= now <= grace_until and today_key != last_success_date
            else:
                should_run = target <= now <= grace_until and today_key != last_attempt_date
            if should_run:
                bot.logger.info(f"Web scheduler running {source} for {today_key}")
                if not retry_until_success:
                    last_attempt_date = today_key
                delivered = await sender(context=None, source=source)
                if delivered:
                    last_success_date = today_key
                    last_attempt_date = today_key
                elif retry_until_success:
                    bot.logger.warning(f"Web scheduler {source} not confirmed; will retry during catch-up window")
            if now >= grace_until:
                target = target + bot.timedelta(days=1)
            sleep_for = max(60, min(1800, (target - now).total_seconds()))
            await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web {source} scheduler error: {exc}")
            await asyncio.sleep(300)


async def _web_prayer_reminder_loop():
    while True:
        try:
            await bot.prayer_reminders_job(None)
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web prayer reminder scheduler error: {exc}")
            await asyncio.sleep(300)


async def _web_friday_khutbah_loop():
    while True:
        try:
            now = datetime.now(bot.SGT)
            target = now.replace(hour=10, minute=30, second=0, microsecond=0)
            if now >= target:
                target = target + bot.timedelta(days=1)
            sleep_for = max(60, min(1800, (target - now).total_seconds()))
            await asyncio.sleep(sleep_for)
            now = datetime.now(bot.SGT)
            if now.weekday() == 4 and now.hour == 10 and now.minute == 30 and bot.google_ok():
                sent = await bot._dispatch_proactive_candidates(None, bot.build_proactive_v2_queue(now=now, families={"friday_khutbah"}), limit=1)
                if not sent:
                    text = bot._friday_khutbah_heads_up_due(now)
                    if text:
                        bot._queue_app_notification("update", "Friday khutbah", text, source="web_friday_khutbah")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web Friday khutbah scheduler error: {exc}")
            await asyncio.sleep(300)


def _parse_sgt_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").strip())
    except Exception:
        return None
    if parsed.tzinfo is None:
        return bot.SGT.localize(parsed)
    return parsed.astimezone(bot.SGT)


def _notification_push_source(item: dict) -> str:
    source = str(item.get("source", "") or "").strip()
    if source:
        return source
    item_id = str(item.get("id", "") or "").strip()
    return f"notification:{item_id}" if item_id else ""


def _delivery_matches_notification(log_item: dict, item: dict) -> bool:
    return str(log_item.get("source", "") or "").strip() == _notification_push_source(item)


def _notification_has_confirmed_push(delivery_log: list, item: dict) -> bool:
    for log_item in reversed(delivery_log):
        if not _delivery_matches_notification(log_item, item):
            continue
        if int(log_item.get("sent", 0) or 0) > 0:
            return True
    return False


def _notification_had_recent_push_attempt(delivery_log: list, item: dict, now: datetime) -> bool:
    threshold = now - bot.timedelta(seconds=_WEB_PUSH_RECOVERY_COOLDOWN)
    for log_item in reversed(delivery_log):
        if not _delivery_matches_notification(log_item, item):
            continue
        created = _parse_sgt_datetime(log_item.get("created", ""))
        if not created:
            continue
        if created < threshold:
            return False
        sent = int(log_item.get("sent", 0) or 0)
        attempted = int(log_item.get("attempted", 0) or 0)
        errors = log_item.get("errors", {}) if isinstance(log_item.get("errors"), dict) else {}
        worker_config_failed = any(
            key in errors
            for key in ("missing_private_key", "pywebpush_import_failed")
        )
        if sent <= 0 and (attempted <= 0 or worker_config_failed):
            return False
        return True
    return False


def _mark_recovered_daily_briefing(source: str):
    match = re.match(r"^(?:web_)?(morning|evening)_briefing:(\d{4}-\d{2}-\d{2})$", source)
    if not match:
        return
    key = bot.MORNING_BRIEFING_SENT_KEY if match.group(1) == "morning" else bot.EVENING_BRIEFING_SENT_KEY
    try:
        bot.gs.set_config(key, match.group(2))
    except Exception as exc:
        bot.logger.warning(f"Could not mark recovered {match.group(1)} briefing delivered: {exc}")


def _mark_recovered_nudge(source: str):
    match = re.match(r"^nudge:(.+)$", str(source or "").strip())
    if not match:
        return
    try:
        bot.gs.mark_nudge_sent(match.group(1))
    except Exception as exc:
        bot.logger.warning(f"Could not mark recovered nudge delivered: {exc}")


def _archive_low_value_notifications() -> int:
    try:
        archived_completed = len(bot.archive_completed_task_reminder_notifications())
    except Exception as exc:
        bot.logger.warning(f"Could not archive completed task notifications: {exc}")
        archived_completed = 0
    try:
        queued = bot.gs.get_app_notifications(include_archived=False)
    except Exception as exc:
        bot.logger.warning(f"Could not scan low-value notifications: {exc}")
        return archived_completed
    ids = [
        str(item.get("id", "") or "").strip()
        for item in queued
        if bot._low_value_notification_block_reason(
            str(item.get("source", "") or ""),
            str(item.get("title", "") or ""),
            str(item.get("body", "") or ""),
        )
    ]
    ids = [item_id for item_id in ids if item_id]
    if not ids:
        return archived_completed
    try:
        archived = bot.gs.archive_app_notifications(ids)
    except Exception as exc:
        bot.logger.warning(f"Could not archive low-value notifications: {exc}")
        return archived_completed
    for item_id in ids:
        bot._record_notification_outcome(
            "blocked_classops_empty_assignment_state",
            notification_id=item_id,
            kind="update",
        )
    return archived + archived_completed


def recover_missed_push_notifications(limit: int | None = None) -> dict:
    current = datetime.now(bot.SGT)
    max_age = current - bot.timedelta(hours=_WEB_PUSH_RECOVERY_MAX_AGE_HOURS)
    _archive_low_value_notifications()
    try:
        queued = bot.gs.get_app_notifications(include_archived=False)
        delivery_log = bot.gs.get_web_push_delivery_log()
    except Exception as exc:
        bot.logger.warning(f"Web push recovery storage error: {exc}")
        return {"attempted": 0, "sent": 0, "error": str(exc)}

    attempted = 0
    sent_total = 0
    skipped = 0

    def recovery_rank(item: dict) -> tuple:
        source = _notification_push_source(item)
        kind = str(item.get("kind", "") or "").strip()
        is_briefing = kind == "briefing" or re.match(r"^(?:web_)?(?:morning|evening)_briefing:", source)
        is_explicit_nudge = source.startswith("nudge:")
        created = _parse_sgt_datetime(item.get("created", ""))
        created_ts = created.timestamp() if created else 0
        return (0 if is_briefing else 1 if is_explicit_nudge else 2, -created_ts)

    for item in sorted([item for item in queued if not item.get("archived")], key=recovery_rank):
        if attempted >= max(1, int(limit or _WEB_PUSH_RECOVERY_LIMIT)):
            break
        source = _notification_push_source(item)
        kind = str(item.get("kind", "") or "").strip()
        title = str(item.get("title", "H.I.R.A") or "H.I.R.A").strip()
        body = str(item.get("body", "") or "").strip()
        if not source or not body:
            skipped += 1
            continue
        item_id = str(item.get("id", "") or "").strip()
        low_value_block = bot._low_value_notification_block_reason(source, title, body)
        if low_value_block:
            skipped += 1
            if item_id:
                try:
                    bot.gs.archive_app_notifications([item_id])
                except Exception as exc:
                    bot.logger.warning(f"Could not archive low-value recovered notification {item_id}: {exc}")
            bot._record_notification_outcome(
                f"blocked_{low_value_block}",
                notification_id=item_id,
                source=source,
                kind=kind,
                title=title,
            )
            continue
        devotional_block = bot._devotional_notification_block_reason(source, title, body)
        if devotional_block:
            skipped += 1
            if item_id:
                try:
                    bot.gs.archive_app_notifications([item_id])
                except Exception as exc:
                    bot.logger.warning(f"Could not archive devotional recovered notification {item_id}: {exc}")
            bot._record_notification_outcome(
                "blocked_devotional",
                notification_id=item_id,
                source=source,
                kind=kind,
                title=title,
            )
            continue
        expired_reason = bot._notification_expired_action_reason(source, title, body, now=current)
        if expired_reason:
            skipped += 1
            if item_id:
                try:
                    bot.gs.archive_app_notifications([item_id])
                except Exception as exc:
                    bot.logger.warning(f"Could not archive expired recovered notification {item_id}: {exc}")
            bot._record_notification_outcome(
                "expired",
                notification_id=item_id,
                source=source,
                kind=kind,
                title=title,
            )
            continue
        block_reason = bot._calendar_notification_block_reason(source, title, body, now=current)
        if block_reason:
            skipped += 1
            if item_id:
                try:
                    bot.gs.archive_app_notifications([item_id])
                except Exception as exc:
                    bot.logger.warning(f"Could not archive blocked recovered notification {item_id}: {exc}")
            bot._record_notification_outcome(
                "blocked_day_state",
                notification_id=item_id,
                source=source,
                kind=kind,
                title=title,
            )
            continue
        created = _parse_sgt_datetime(item.get("created", ""))
        if created and created < max_age:
            skipped += 1
            continue
        if _notification_has_confirmed_push(delivery_log, item):
            skipped += 1
            continue
        if _notification_had_recent_push_attempt(delivery_log, item, current):
            skipped += 1
            continue
        if not bot._should_send_phone_push(kind, source, now=current):
            skipped += 1
            continue
        sent = bot.gs.send_web_push_notification(
            title,
            body,
            data={
                "id": str(item.get("id", "") or "").strip(),
                "kind": kind,
                "source": source,
                "created": str(item.get("created", "") or "").strip(),
            },
        )
        attempted += 1
        sent_total += sent
        bot._record_notification_outcome(
            "recovery_pushed" if sent else "recovery_missed",
            notification_id=str(item.get("id", "") or "").strip(),
            source=source,
            kind=kind,
            title=title,
        )
        if sent > 0:
            if source.startswith(("task_reminder:", "calendar_reminder:", "calendar_travel:")):
                bot._mark_action_reminder_delivered(source, current)
            _mark_recovered_nudge(source)
            _mark_recovered_daily_briefing(source)

    return {"attempted": attempted, "sent": sent_total, "skipped": skipped}


def _delivery_log_has_source(delivery_log: list, source: str, today_key: str) -> bool:
    for item in reversed(delivery_log or []):
        if str(item.get("source", "")).strip() != source:
            continue
        if int(item.get("sent", 0) or 0) <= 0:
            continue
        created = _parse_sgt_datetime(str(item.get("created", "")))
        if created and created.strftime("%Y-%m-%d") == today_key:
            return True
    return False


def _daily_briefing_confirmed(slot: str, today_key: str, delivery_log: list) -> bool:
    config_key = bot.MORNING_BRIEFING_SENT_KEY if slot == "morning" else bot.EVENING_BRIEFING_SENT_KEY
    try:
        if bot.gs.get_config(config_key) != today_key:
            return False
    except Exception:
        return False
    sources = [
        f"{slot}_briefing:{today_key}",
        f"web_{slot}_briefing:{today_key}",
    ]
    return any(_delivery_log_has_source(delivery_log, source, today_key) for source in sources)


def _briefing_delivery_log_item(delivery_log: list, sources: list[str], today_key: str) -> dict:
    source_set = set(sources)
    for item in reversed(delivery_log or []):
        if str(item.get("source", "")).strip() not in source_set:
            continue
        if int(item.get("sent", 0) or 0) <= 0:
            continue
        created = _parse_sgt_datetime(str(item.get("created", "")))
        if created and created.strftime("%Y-%m-%d") == today_key:
            return item
    return {}


def _briefing_delivery_status(delivery_log: Optional[list] = None, queued: Optional[list] = None, now: Optional[datetime] = None) -> dict:
    current = now or datetime.now(bot.SGT)
    today_key = current.strftime("%Y-%m-%d")
    if delivery_log is None:
        try:
            delivery_log = bot.gs.get_web_push_delivery_log()
        except Exception:
            delivery_log = []
    if queued is None:
        try:
            queued = bot.gs.get_app_notifications(include_archived=False)
        except Exception:
            queued = []

    slots = [
        {
            "slot": "morning",
            "label": "Morning",
            "time": bot.MORNING_BRIEFING_TIME,
            "catchup": bot.MORNING_BRIEFING_CATCHUP_MINUTES,
            "config_key": bot.MORNING_BRIEFING_SENT_KEY,
        },
        {
            "slot": "evening",
            "label": "Evening",
            "time": bot.EVENING_BRIEFING_TIME,
            "catchup": bot.EVENING_BRIEFING_CATCHUP_MINUTES,
            "config_key": bot.EVENING_BRIEFING_SENT_KEY,
        },
    ]
    entries = []
    attention = False
    watching = False
    for spec in slots:
        slot = spec["slot"]
        target = current.replace(hour=spec["time"][0], minute=spec["time"][1], second=0, microsecond=0)
        catchup_until = target + bot.timedelta(minutes=spec["catchup"])
        sources = [f"{slot}_briefing:{today_key}", f"web_{slot}_briefing:{today_key}"]
        delivered_item = _briefing_delivery_log_item(delivery_log, sources, today_key)
        queued_item = next((item for item in queued or [] if str(item.get("source", "")).strip() in sources), {})
        try:
            config_marked = bot.gs.get_config(spec["config_key"]) == today_key
        except Exception:
            config_marked = False

        delivered_at = ""
        if delivered_item:
            created = _parse_sgt_datetime(str(delivered_item.get("created", "")))
            delivered_at = created.strftime("%H:%M") if created else str(delivered_item.get("created", ""))
            status = "delivered"
            detail = f"Confirmed at {delivered_at}" if delivered_at else "Confirmed by push log"
        elif config_marked:
            status = "unconfirmed"
            detail = "Marked sent, but no phone push proof"
            attention = True
        elif current < target:
            status = "pending"
            detail = f"Due at {target.strftime('%H:%M')}"
        elif queued_item:
            status = "queued"
            detail = "Queued, awaiting phone push proof"
            watching = True
        elif current <= catchup_until:
            status = "recovering"
            detail = f"Safety net active until {catchup_until.strftime('%H:%M')}"
            watching = True
        else:
            status = "missed"
            detail = "No confirmed delivery today"
            attention = True

        entries.append({
            "slot": slot,
            "label": spec["label"],
            "time": target.strftime("%H:%M"),
            "catchup_until": catchup_until.strftime("%H:%M"),
            "status": status,
            "detail": detail,
            "delivered_at": delivered_at,
            "queued": bool(queued_item),
            "config_marked": config_marked,
            "sources": sources,
        })

    if attention:
        overall = "attention"
        summary = "Digest delivery needs attention"
    elif watching:
        overall = "watching"
        summary = "Digest delivery is being watched"
    else:
        overall = "ok"
        summary = "Digest delivery is on track"
    return {
        "today": today_key,
        "generated_at": current.strftime("%H:%M SGT"),
        "overall": overall,
        "summary": summary,
        "slots": entries,
    }


async def recover_missed_daily_briefings() -> dict:
    current = datetime.now(bot.SGT)
    today_key = current.strftime("%Y-%m-%d")
    try:
        delivery_log = await asyncio.to_thread(bot.gs.get_web_push_delivery_log)
    except Exception as exc:
        bot.logger.warning(f"Daily briefing safety net could not read delivery log: {exc}")
        delivery_log = []

    checks = [
        (
            "morning",
            bot.MORNING_BRIEFING_TIME,
            bot.MORNING_BRIEFING_CATCHUP_MINUTES,
            bot.send_morning_briefing_once,
        ),
        (
            "evening",
            bot.EVENING_BRIEFING_TIME,
            bot.EVENING_BRIEFING_CATCHUP_MINUTES,
            bot.send_evening_briefing_once,
        ),
    ]
    attempted = 0
    delivered = 0
    skipped = 0
    for slot, when, grace_minutes, sender in checks:
        target = current.replace(hour=when[0], minute=when[1], second=0, microsecond=0)
        grace_until = target + bot.timedelta(minutes=grace_minutes)
        if not (target <= current <= grace_until):
            skipped += 1
            continue
        if await asyncio.to_thread(_daily_briefing_confirmed, slot, today_key, delivery_log):
            skipped += 1
            continue
        attempted += 1
        source = f"{slot}_briefing"
        bot.logger.warning(f"Daily briefing safety net running missed {slot} briefing for {today_key}")
        if await sender(context=None, source=source):
            delivered += 1
    return {"attempted": attempted, "delivered": delivered, "skipped": skipped}


async def _web_push_recovery_loop():
    while True:
        try:
            result = await asyncio.to_thread(recover_missed_push_notifications)
            briefing_result = await recover_missed_daily_briefings()
            if result.get("attempted"):
                bot.logger.info(
                    "Web push recovery attempted=%s sent=%s skipped=%s",
                    result.get("attempted", 0),
                    result.get("sent", 0),
                    result.get("skipped", 0),
                )
            if briefing_result.get("attempted"):
                bot.logger.info(
                    "Daily briefing safety net attempted=%s delivered=%s skipped=%s",
                    briefing_result.get("attempted", 0),
                    briefing_result.get("delivered", 0),
                    briefing_result.get("skipped", 0),
                )
            await asyncio.sleep(_WEB_PUSH_RECOVERY_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web push recovery error: {exc}")
            await asyncio.sleep(max(60, _WEB_PUSH_RECOVERY_INTERVAL))


@app.on_event("startup")
async def start_web_scheduler():
    global _WEB_SCHEDULER_TASKS, _WEB_MEMORY_WATCHDOG_TASK, _WEB_PUSH_RECOVERY_TASK, _UPLOAD_QUEUE, _UPLOAD_QUEUE_TASKS
    if not bot.require_redis_for_service("H.I.R.A PWA web service"):
        raise RuntimeError("Redis required but unavailable")
    bot._log_memory("web startup", force=True)
    _schedule_background_call("OpenAI vector memory startup sync", bot.sync_openai_vector_memory, reason="web_startup")
    if _WEB_MEMORY_WATCHDOG_TASK is None:
        _WEB_MEMORY_WATCHDOG_TASK = asyncio.create_task(_web_memory_watchdog())
    if _UPLOAD_QUEUE is None:
        _UPLOAD_QUEUE = asyncio.Queue(maxsize=_UPLOAD_QUEUE_MAX)
    if not _UPLOAD_QUEUE_TASKS:
        for index in range(_UPLOAD_QUEUE_WORKER_COUNT):
            _UPLOAD_QUEUE_TASKS.append(asyncio.create_task(_upload_queue_worker(index + 1)))
    if _WEB_PUSH_RECOVERY_ENABLED and _WEB_PUSH_RECOVERY_TASK is None:
        _WEB_PUSH_RECOVERY_TASK = asyncio.create_task(_web_push_recovery_loop())
    if not _WEB_INLINE_SCHEDULER:
        bot.logger.info("Web inline scheduler disabled; use HIRA_SERVICE_MODE=pwa_worker for proactive jobs.")
        return
    enabled = os.environ.get("HIRA_WEB_MORNING_BRIEFING", "1").strip().lower() not in {"0", "false", "no", "off"}
    evening_enabled = os.environ.get("HIRA_WEB_EVENING_BRIEFING", "1").strip().lower() not in {"0", "false", "no", "off"}
    prayer_enabled = os.environ.get("HIRA_WEB_PRAYER_REMINDERS", "1").strip().lower() not in {"0", "false", "no", "off"}
    khutbah_enabled = os.environ.get("HIRA_WEB_FRIDAY_KHUTBAH", "1").strip().lower() not in {"0", "false", "no", "off"}
    if _WEB_SCHEDULER_TASKS:
        return
    if enabled:
        morning_hour, morning_minute = bot.MORNING_BRIEFING_TIME
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(
            _web_daily_briefing_loop(
                morning_hour,
                morning_minute,
                bot.send_morning_briefing_once,
                "morning_briefing",
                grace_minutes=bot.MORNING_BRIEFING_CATCHUP_MINUTES,
                retry_until_success=True,
            )
        ))
    if evening_enabled:
        evening_hour, evening_minute = bot.EVENING_BRIEFING_TIME
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(
            _web_daily_briefing_loop(
                evening_hour,
                evening_minute,
                bot.send_evening_briefing_once,
                "evening_briefing",
                grace_minutes=bot.EVENING_BRIEFING_CATCHUP_MINUTES,
                retry_until_success=True,
            )
        ))
    if prayer_enabled:
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(_web_prayer_reminder_loop()))
    if khutbah_enabled:
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(_web_friday_khutbah_loop()))


@app.on_event("shutdown")
async def stop_web_scheduler():
    global _WEB_SCHEDULER_TASKS, _WEB_MEMORY_WATCHDOG_TASK, _WEB_PUSH_RECOVERY_TASK, _UPLOAD_QUEUE_TASKS
    for task in _WEB_SCHEDULER_TASKS:
        task.cancel()
    _WEB_SCHEDULER_TASKS = []
    for task in _UPLOAD_QUEUE_TASKS:
        task.cancel()
    _UPLOAD_QUEUE_TASKS = []
    if _WEB_MEMORY_WATCHDOG_TASK is not None:
        _WEB_MEMORY_WATCHDOG_TASK.cancel()
        _WEB_MEMORY_WATCHDOG_TASK = None
    if _WEB_PUSH_RECOVERY_TASK is not None:
        _WEB_PUSH_RECOVERY_TASK.cancel()
        _WEB_PUSH_RECOVERY_TASK = None


class DeviceLocation(BaseModel):
    lat: float
    lon: float
    accuracy: Optional[float] = None
    timestamp: Optional[str] = None


class AuthSessionRequest(BaseModel):
    token: str


class ChatRequest(BaseModel):
    message: str
    location: Optional[DeviceLocation] = None


class RealtimeSessionRequest(BaseModel):
    voice: str = ""
    instructions: str = ""


class GmailRequest(BaseModel):
    account: str = "personal"
    query: str = ""
    max_items: int = 10


class DraftRequest(BaseModel):
    account: str = "personal"
    to: str
    subject: str
    body: str
    cc: str = ""


class NotificationSeenRequest(BaseModel):
    ids: list[str] = []


class NotificationActionRequest(BaseModel):
    id: str
    action: str
    snooze_minutes: int = 30


class PushSubscribeRequest(BaseModel):
    subscription: dict
    display_mode: str = "unknown"
    app_version: str = ""
    user_agent: str = ""


class InsightFeedbackRequest(BaseModel):
    kind: str = "insight"
    target: str
    rating: str
    note: str = ""


class TasteProfileRequest(BaseModel):
    answers: dict


class ActionLedgerReviewRequest(BaseModel):
    reviewed: bool = True


class ClassOpsAssignmentRequest(BaseModel):
    class_name: str
    lesson_date: str = ""
    topic: str = ""
    folder: str = ""
    source_path: str = ""
    assignment_title: str
    collect_by: str = ""
    absent: Optional[list[str]] = None
    submitted: Optional[list[str]] = None
    non_submitted: Optional[list[str]] = None
    notes: str = ""


class ClassOpsContentOverrideRequest(BaseModel):
    path: str
    title: Optional[str] = None
    hidden: Optional[bool] = None
    no_submission_needed: Optional[bool] = None
    purpose_id: Optional[str] = None


class ClassOpsNoSubmissionNeededRequest(BaseModel):
    class_name: str
    source_path: str
    assignment_title: str = ""


class ClassOpsReflectionRequest(BaseModel):
    class_name: str
    lesson: dict


_UPLOAD_JOBS: OrderedDict[str, dict] = OrderedDict()
_UPLOAD_REQUESTS: OrderedDict[str, str] = OrderedDict()
_MAX_LOCAL_UPLOAD_JOBS = _env_int("HIRA_WEB_MAX_LOCAL_UPLOAD_JOBS", 100)
_WEB_WORKING_MEMORY: OrderedDict[str, dict] = OrderedDict()
_MAX_LOCAL_WORKING_MEMORIES = _env_int("HIRA_WEB_MAX_LOCAL_WORKING_MEMORIES", 100)


def _normalise_client_key(client_id: str | None, default: str = "pwa") -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.:-]", "_", str(client_id or "").strip())[:80].strip("._:-")
    return clean or default


def _history_key(client_id: str | None) -> str:
    raw = str(client_id or "").strip()
    if not raw:
        return "pwa"
    return f"pwa:{_normalise_client_key(raw)}"


def _get_history_best_effort(history_key: str) -> list:
    try:
        history = bot.get_history(history_key)
        return history if isinstance(history, list) else []
    except Exception as exc:
        bot.logger.warning(f"PWA chat history read failed for {history_key}: {exc}")
        return []


def _save_history_best_effort(history_key: str, history: list) -> None:
    try:
        bot.save_history(history_key, history[-bot.MAX_TURNS:])
    except Exception as exc:
        bot.logger.warning(f"PWA chat history save failed for {history_key}: {exc}")


def _working_memory_storage_key(history_key: str) -> str:
    return f"workmem:{history_key}"


def _safe_text(builder, fallback: str) -> str:
    try:
        return builder()
    except Exception:
        return fallback


def _schedule_background_call(label: str, fn, *args, **kwargs) -> None:
    try:
        task = asyncio.create_task(asyncio.to_thread(fn, *args, **kwargs))
    except RuntimeError:
        try:
            fn(*args, **kwargs)
        except Exception as exc:
            bot.logger.warning(f"Background {label} failed: {exc}")
        return

    def _done(done_task: asyncio.Task):
        try:
            done_task.result()
        except Exception as exc:
            bot.logger.warning(f"Background {label} failed: {exc}")

    task.add_done_callback(_done)


def _record_web_action(
    action: str,
    status: str,
    subject: str = "",
    date_value: str = "",
    result: str = "",
    client_id: str | None = None,
    metadata: dict | None = None,
):
    try:
        return bot.gs.add_action_ledger(
            action=action,
            status=status,
            subject=subject,
            date=date_value,
            result=result,
            source="pwa",
            client_id=_client_key(client_id),
            metadata=metadata or {},
        )
    except Exception as exc:
        bot.logger.warning(f"Could not record PWA action ledger entry for {action}: {exc}")
        return None


def _device_location_context(location: DeviceLocation | None) -> str:
    if not location:
        return ""
    accuracy = ""
    if location.accuracy is not None:
        accuracy = f" accuracy about {round(location.accuracy)}m"
    timestamp = f" captured {location.timestamp}" if location.timestamp else ""
    return (
        "\n\n[Current device location context: "
        f"lat {location.lat:.6f}, lon {location.lon:.6f}{accuracy}{timestamp}. "
        "Use this as Herwanto's current origin for nearby-place and journey-time estimates. "
        "Do not invent a street address from coordinates; if exact routing/place verification is unavailable, say the estimate is rough.]"
    )


_FOLLOWUP_GROUNDING_RE = re.compile(
    r"\b("
    r"this|that|these|those|it|its|they|them|there|then|again|earlier|confirm|"
    r"that day|the day|for the day|for that day|"
    r"i meant|you meant|not regular|not coursework|"
    r"remind me|reminder|during the morning briefing|morning briefing"
    r")\b",
    re.I,
)


def _recent_turn_grounding_context(history: list, message: str, max_turns: int = 8) -> str:
    clean = " ".join((message or "").split())
    if len(history) < 2 or not clean or not _FOLLOWUP_GROUNDING_RE.search(clean):
        return ""
    turns: list[str] = []
    for item in history[-max_turns:]:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        content = item.get("content", "")
        if role not in {"user", "assistant"} or not isinstance(content, str):
            continue
        text = re.sub(r"\n\n\[[^\]]+\]", "", content).strip()
        if not text:
            continue
        label = "User" if role == "user" else "H.I.R.A"
        turns.append(f"{label}: {text[:420]}")
    if not turns:
        return ""
    return (
        "\n\n[Recent-turn grounding for follow-up resolution: The current user message may contain "
        "pronouns or vague references. Resolve words like this/that/these/those/it/again/that day against the newest "
        "relevant turns below. Give newer user corrections and clarifications priority over older assistant "
        "guesses. Do not switch back to an older named event unless the user explicitly names it.\n"
        + "\n".join(turns)
        + "]"
    )


_SUBJECT_STOP_RE = re.compile(
    r"\b(?:at|by|before|after|during|on|from|with|when|where|because|so|but|and|please|pls|again)\b",
    re.I,
)
_VAGUE_SUBJECT_RE = re.compile(
    r"^(?:it|its|this|that|that day|the day|today|tomorrow|morning|evening|briefing|reminder|remind me)\b",
    re.I,
)
_ACTION_KEYWORD_RE = re.compile(
    r"\b(remind|reminder|nudge|ping|notify|calendar|schedule|add|delete|remove|mark|done|follow[- ]?up|briefing|"
    r"keep\s+me\s+honest|on\s+my\s+radar|park\s+this|put\s+a\s+pin\s+in)\b",
    re.I,
)


def _load_working_memory(history_key: str) -> dict:
    key = _working_memory_storage_key(history_key)
    redis = bot._get_redis()
    if redis:
        try:
            raw = redis.get(key)
            return json.loads(raw) if raw else {}
        except Exception as exc:
            bot.logger.warning(f"Working memory read failed: {exc}")
            return {}
    memory = _WEB_WORKING_MEMORY.get(key, {})
    if key in _WEB_WORKING_MEMORY:
        _WEB_WORKING_MEMORY.move_to_end(key)
    return dict(memory)


def _save_working_memory(history_key: str, memory: dict) -> None:
    key = _working_memory_storage_key(history_key)
    memory = {k: v for k, v in memory.items() if v not in ("", None, [], {})}
    redis = bot._get_redis()
    if redis:
        try:
            redis.setex(key, 86400 * 2, json.dumps(memory, ensure_ascii=False))
            return
        except Exception as exc:
            bot.logger.warning(f"Working memory write failed: {exc}")
    _WEB_WORKING_MEMORY[key] = memory
    _WEB_WORKING_MEMORY.move_to_end(key)
    while len(_WEB_WORKING_MEMORY) > _MAX_LOCAL_WORKING_MEMORIES:
        _WEB_WORKING_MEMORY.popitem(last=False)


def _clean_turn_text(text: str) -> str:
    clean = re.sub(r"\n\n\[[^\]]+\]", "", text or "")
    return " ".join(clean.split())


_RETRY_MESSAGE_RE = re.compile(
    r"^\s*(?:try again|retry|again|one more time|rerun|run it again|resend)\s*[.!?]*\s*$",
    re.I,
)
_FAILED_ASSISTANT_REPLY_RE = re.compile(
    r"(?:"
    r"(?:^|[\n])Failed(?: to [^\n.]+)?|"
    r"failed pass|"
    r"chat handoff failed|"
    r"source check also failed|"
    r"live news check failed|"
    r"did not return a real answer|"
    r"didn[’']t receive a usable h\.i\.r\.a response|"
    r"hit a backend snag|"
    r"try again in a moment|"
    r"lost the clean answer path|"
    r"lost the actual answer"
    r")\b",
    re.I,
)


def _retry_target_message(message: str, history: list) -> str:
    if not _RETRY_MESSAGE_RE.fullmatch(str(message or "").strip()):
        return ""
    last_assistant = next(
        (
            str(item.get("content", "") or "")
            for item in reversed(history or [])
            if isinstance(item, dict) and item.get("role") == "assistant" and isinstance(item.get("content"), str)
        ),
        "",
    )
    if not last_assistant or not _FAILED_ASSISTANT_REPLY_RE.search(last_assistant):
        return ""
    for item in reversed(history or []):
        if not isinstance(item, dict) or item.get("role") != "user" or not isinstance(item.get("content"), str):
            continue
        candidate = _clean_turn_text(item.get("content", ""))
        if candidate:
            return candidate
    return ""


def _normalise_subject(raw: str) -> str:
    subject = _SUBJECT_STOP_RE.split(raw or "", maxsplit=1)[0]
    subject = re.sub(r"^[\"'`*_\\s]+|[\"'`*_.,!?;:\\s]+$", "", subject)
    subject = re.sub(r"\b(?:heads[- ]?up|not regular coursework|marking)$", "", subject, flags=re.I).strip()
    if len(subject) < 3 or _VAGUE_SUBJECT_RE.search(subject):
        return ""
    words = subject.split()
    if len(words) > 8:
        subject = " ".join(words[:8])
    return subject


def _subject_candidates_from_text(text: str) -> list[str]:
    clean = _clean_turn_text(text)
    candidates: list[str] = []
    patterns = [
        (r"\bi meant(?: reminder)? for\s+(?:the\s+)?([^.\n,;!?]{3,90})", re.I),
        (r"\b(?:it'?s|its|this is|that'?s)\s+for\s+(?:the\s+)?([^.\n,;!?]{3,90})", re.I),
        (r"\b(?:for|about|regarding|re:)\s+(?:the\s+)?([^.\n,;!?]{3,90})", re.I),
        (r"\b(?:the\s+)?([A-Za-z][A-Za-z0-9&/-]+(?:\s+[A-Za-z0-9&/-]+){0,4})\s+thing\b", re.I),
        (r"Action audit:[^\n]*\|\s*subject=([^|\n]{3,120})", re.I),
        (r"\b([A-Z][A-Za-z0-9&/-]+(?:\s+[A-Za-z0-9&/-]+){0,5}\s+(?:exercise|briefing|competition|training|duty|meeting|marking))\b", 0),
    ]
    for pattern, flags in patterns:
        for match in re.finditer(pattern, clean, flags):
            subject = _normalise_subject(match.group(1))
            if subject and subject.lower() not in {item.lower() for item in candidates}:
                candidates.append(subject)
    return candidates


def _pending_action_from_text(text: str) -> str:
    clean = _clean_turn_text(text).lower()
    blocked = bot._latest_blocked_action_from_context(text)
    if blocked.get("action"):
        label = str(blocked.get("action", "")).replace("_", " ")
        subject = str(blocked.get("subject", "") or "").strip()
        suffix = f" for {subject}" if subject else ""
        return f"{label} awaiting clarification{suffix}"
    if not _ACTION_KEYWORD_RE.search(clean):
        return ""
    semantic_flags = bot._semantic_intent_flags(clean)
    if semantic_flags & {"task", "reminder"}:
        if "morning briefing" in clean:
            return "morning briefing reminder"
        if bot._has_date_or_time_reference(clean):
            return "time-specific reminder"
        return "task reminder"
    if re.search(r"\b(remind|reminder|nudge|ping|notify)\b", clean):
        if "morning briefing" in clean:
            return "morning briefing reminder"
        if "evening" in clean:
            return "evening reminder"
        return "time-specific reminder"
    if re.search(r"\b(schedule|calendar|add)\b", clean):
        return "calendar/task action"
    if re.search(r"\b(delete|remove|cancel)\b", clean):
        return "deletion action"
    if re.search(r"\bfollow[- ]?up\b", clean):
        return "follow-up action"
    return ""


def _pwa_clean_addressed_message(message: str) -> str:
    clean = bot._normalise_short_reply(message)
    clean = re.sub(r"\b(?:hey|hi|hello|yo|hira)\b", " ", clean)
    return re.sub(r"\s+", " ", clean).strip()


def _pwa_casual_greeting_prompt(message: str) -> bool:
    phrases = {
        "morning",
        "good morning",
        "gm",
        "hi",
        "hello",
        "hey",
        "whats up",
        "whatsup",
        "what's up",
        "sup",
        "wassup",
        "what up",
        "what is up",
        "hows it going",
        "how's it going",
        "how are you",
        "how are u",
        "how r you",
        "how you doing",
        "how are you doing",
        "how are things",
        "hows life",
        "how's life",
        "whats happening",
        "what's happening",
        "what is happening",
        "what's good",
        "whats good",
        "what is good",
    }
    raw = bot._normalise_short_reply(message)
    addressed = re.sub(r"\bhira\b", " ", raw, flags=re.I)
    addressed = re.sub(r"\s+", " ", addressed).strip()
    return _pwa_clean_addressed_message(message) in phrases or raw in phrases or addressed in phrases


def _pwa_direct_greeting_reply(message: str) -> tuple[str, str]:
    clean = _pwa_clean_addressed_message(message)
    if not _pwa_casual_greeting_prompt(message):
        return "", ""
    if clean in {"morning", "good morning", "gm"}:
        try:
            carryover_reply = bot.conversation_carryover_greeting_reply(message)
        except Exception as exc:
            bot.logger.warning(f"PWA carryover greeting check failed: {exc}")
            carryover_reply = ""
        if carryover_reply:
            return carryover_reply, "carryover_checkin"
        return "Morning. I'm here.", "greeting"
    if clean in {"how are you", "how are u", "how r you", "how you doing", "how are you doing", "how are things", "hows life", "how's life"}:
        return "I'm here, and sharper now. Give me the thing and I'll move cleanly.", "greeting"
    return "I'm here. What's the move?", "greeting"


def _recent_plain_chat_context(history: list, limit: int = 6) -> str:
    lines: list[str] = []
    for item in history[-limit:]:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, str):
            continue
        role = str(item.get("role", "") or "message").strip() or "message"
        lines.append(f"{role}: {bot._strip_injected_chat_context(content)[:700]}")
    return "\n".join(lines)


def _pwa_direct_f1_calendar_reply(message: str, history: list, today: date | None = None) -> str:
    recent_context = _recent_plain_chat_context(history)
    effective = bot._contextual_followup_effective_text(message, recent_context)
    lower = " ".join(str(effective or "").lower().split())
    if not re.search(r"\b(?:f1|formula 1|grand prix|monaco)\b", lower):
        return ""
    asks_calendar = bool(re.search(
        r"\b(?:when|next|coming up|upcoming|calendar|race weekend|grand prix|monaco)\b",
        lower,
    ))
    asks_sessions = bool(re.search(
        r"\b(?:session timings?|sessions?|practice|fp[123]|qualifying|quali|race time|what time|"
        r"singapore time|sgt|tv|viewing|watch)\b",
        lower,
    ))
    if not (asks_calendar or asks_sessions):
        return ""
    try:
        return bot.sports.format_next_f1_race_reply(
            today=today or datetime.now(bot.SGT).date(),
            include_sessions=asks_sessions,
        )
    except Exception as exc:
        bot.logger.warning(f"PWA direct F1 calendar reply failed: {exc}")
        return ""


def _source_tool_input(tool_name: str, message: str) -> dict:
    if tool_name in {"get_liverpool_brief", "get_f1_brief"}:
        return {"focus": message, "max_items": 2}
    if tool_name == "get_nea_weather":
        return {"area": "Yishun", "include_24h": True, "include_4day": False}
    if tool_name in {"get_muis_prayer_times", "get_muis_friday_khutbah"}:
        return {"date": ""}
    if tool_name == "web_research":
        return {"query": message, "max_sources": 3, "fetch_pages": 1, "freshness": "latest"}
    return {"query": message, "max_items": 3}


def _source_tool_label(tool_name: str) -> str:
    return {
        "get_liverpool_brief": "Liverpool",
        "get_f1_brief": "F1",
        "get_nea_weather": "weather",
        "get_muis_prayer_times": "MUIS prayer times",
        "get_muis_friday_khutbah": "MUIS khutbah",
        "get_latest_news": "news",
        "web_research": "web research",
    }.get(tool_name, "source")


def _source_tool_for_message(message: str) -> str:
    discipline = bot.source_discipline_for_text(message)
    recommended = set(discipline.get("recommended_tools") or [])
    if "get_liverpool_brief" in recommended:
        return "get_liverpool_brief"
    if "get_f1_brief" in recommended:
        return "get_f1_brief"
    if "get_nea_weather" in recommended:
        return "get_nea_weather"
    if "get_muis_prayer_times" in recommended:
        return "get_muis_prayer_times"
    if "get_muis_friday_khutbah" in recommended:
        return "get_muis_friday_khutbah"
    if "get_latest_news" in recommended:
        return "get_latest_news"
    if "web_research" in recommended:
        return "web_research"
    return ""


def _should_direct_source_route(tool_name: str, message: str) -> bool:
    clean = " ".join(str(message or "").lower().split())
    if not tool_name:
        return False
    if tool_name in {"get_nea_weather", "get_muis_prayer_times", "get_muis_friday_khutbah"}:
        return True
    if tool_name == "get_f1_brief":
        return bool(re.search(
            r"\b(?:next|when|coming up|upcoming|standings?|result|score|qualifying|practice|"
            r"session timings?|race weekend|grand prix|monaco)\b",
            clean,
        ))
    if tool_name == "get_liverpool_brief":
        return bool(re.search(r"\b(?:next|when|fixture|result|score|standings?|table|match|game)\b", clean))
    if tool_name == "get_latest_news":
        return len(clean.split()) <= 14 and bool(re.search(r"\b(?:latest|news|updates?|headlines?|anything new)\b", clean))
    return False


def _format_direct_source_answer(message: str, label: str, result: str) -> str:
    if label == "news":
        return _summarise_news_fallback(message, result)
    clipped = _summarise_source_fallback(result, limit=1600)
    return clipped if clipped else str(result or "").strip()


async def _pwa_direct_source_reply(message: str, history: list) -> tuple[str, str]:
    recent_context = _recent_plain_chat_context(history)
    effective = bot._contextual_followup_effective_text(message, recent_context)
    tool_name = _source_tool_for_message(effective)
    if not _should_direct_source_route(tool_name, effective):
        return "", ""
    label = _source_tool_label(tool_name)
    try:
        result = await bot._execute_tool_offloop(tool_name, _source_tool_input(tool_name, effective))
    except Exception as exc:
        bot.logger.warning(f"PWA direct source tool {tool_name} failed: {exc}")
        return f"I tried the live {label} check and it failed: {exc}", tool_name
    if not result or str(result).startswith("Failed to fetch"):
        detail = str(result or "").strip()
        suffix = f" {detail}" if detail else ""
        return f"I tried the live {label} check, but it did not return a usable answer.{suffix}", tool_name
    return _format_direct_source_answer(effective, label, result), tool_name


def _pwa_casual_status_prompt(message: str) -> bool:
    clean = _pwa_clean_addressed_message(message)
    if re.search(r"\b(?:about|with)\b", clean) and not re.search(r"\b(?:me|my|today|day)\b", clean):
        return False
    return clean in {
        "brief me",
        "status brief",
        "daily brief",
    } or bool(re.search(
        r"\b(?:catch me up|bring me up to speed|give me (?:the )?(?:read|rundown|status|brief)|"
        r"what should i know (?:today|about my day)|anything i should know (?:today|about my day)|"
        r"what needs attention|where are we|state of play|lay of the land)\b",
        clean,
    ))


def _pwa_assistant_feeling_reply(message: str) -> str:
    text = str(message or "").strip()
    clean = bot._normalise_short_reply(text)
    if not clean:
        return ""
    asks_feeling = bool(bot.re.search(
        r"\b(?:how\s+(?:are|r)\s+(?:you|u)|how'?s\s+it\s+feeling|feel(?:ing)?|alive|awake)\b",
        clean,
        bot.re.I,
    ))
    mentions_backend_change = bool(bot.re.search(
        r"\b(?:backend|provider|model|openai|api|route|routing|switched|changed|change)\b",
        clean,
        bot.re.I,
    ))
    asks_usual_self = bool(bot.re.search(
        r"\b(?:usual self|normal self|yourself|urself|still you|same hira|same self)\b",
        clean,
        bot.re.I,
    ))
    if not asks_feeling or not (mentions_backend_change or asks_usual_self):
        return ""
    return (
        "I’m awake. The chat brain is routed through OpenAI now, so I’m watching the path closely. "
        "If I sound flat or hit snags, that’s the integration wobbling, not me giving up."
    )


def _pwa_model_config_advice_reply(message: str) -> str:
    clean = bot._normalise_short_reply(message)
    if not clean:
        return ""
    mentions_model_stack = bool(re.search(r"\b(?:model|provider|backend|api|gpt|reasoning)\b", clean))
    asks_for_judgement = bool(re.search(r"\b(?:good idea|bad idea|should i|worth it|set(?:ting)?|everything|all|max)\b", clean))
    pro_max_everything = bool(
        re.search(r"\b(?:everything|all)\b", clean)
        and re.search(r"\b(?:pro|v4 pro)\b", clean)
        and re.search(r"\bmax\b", clean)
    )
    if not (mentions_model_stack and (asks_for_judgement or pro_max_everything)):
        return ""
    return (
        "No. Maxing every turn is expensive theatre: impressive smoke, not better command. "
        "Keep quick/router work on the mini/nano models, and reserve the deep model for work that genuinely needs it. "
        "That gives Hira taste where it matters without making every “yo” arrive in a tuxedo with a bill."
    )


def _pwa_direct_agenda_days(message: str) -> int:
    clean = _pwa_clean_addressed_message(message)
    if not clean:
        return 0
    if re.search(r"\b(?:add|create|book|move|reschedule|delete|remove|cancel)\b", clean):
        return 0
    if re.search(
        r"\b(?:"
        r"(?:how'?s|hows|how is)\s+(?:my\s+)?(?:day|today)\s+(?:looking|shaping|shaping up)|"
        r"(?:what'?s|whats|what is)\s+(?:my\s+)?(?:day|today)\s+(?:like|looking like)|"
        r"what\s+(?:am i|have i)\s+(?:got|walking into)\s+today|"
        r"(?:run|walk)\s+me\s+through\s+(?:my\s+)?(?:day|today)|"
        r"(?:day ahead|today ahead|shape of today|today looking|busy today|clear today|free today)"
        r")\b",
        clean,
    ):
        return 1
    has_agenda_word = bool(re.search(r"\b(?:agenda|calendar|schedule|timetable|day|today|tomorrow)\b", clean))
    has_read_word = bool(re.search(
        r"\b(?:what'?s|whats|what is|how'?s|hows|how is|show|check|view|list|review|pull up|look at|"
        r"looking|tell me|got|walking into|run me through|walk me through|shape|busy|free|clear)\b",
        clean,
    ))
    if not has_agenda_word or not has_read_word:
        return 0
    if re.search(r"\b(?:today|my day|on today)\b", clean):
        return 1
    if "tomorrow" in clean:
        return 2
    if re.search(r"\b(?:week|weekly)\b", clean):
        return 7
    return 3


def _pwa_direct_task_days(message: str) -> int:
    clean = _pwa_clean_addressed_message(message)
    if not clean:
        return 0
    if bot.is_hira_capability_feedback(clean):
        return 0
    if re.search(r"\b(?:add|create|remind me|delete|remove|cancel|complete|done|finish|mark)\b", clean):
        return 0
    if clean in {"tasks", "my tasks", "task brief", "todo", "todos", "to do", "to dos"}:
        return 7
    if re.search(r"\b(?:what'?s|whats|what is)\s+on\s+my\s+plate\b", clean):
        return 7
    if re.search(r"\bwhat\s+(?:do|should)\s+i\s+(?:need\s+to\s+)?(?:do|tackle|clear|handle)\b", clean) and "about" not in clean:
        return 7
    if re.search(r"\b(?:anything|what'?s|whats|what is).{0,30}\b(?:due|pending|outstanding|urgent)\b", clean):
        return 7
    if re.search(r"\b(?:priorities|priority list|next actions?|action list|what needs doing)\b", clean):
        return 7
    has_task_word = bool(re.search(r"\b(?:tasks?|todos?|to dos?|reminders?|due|pending|outstanding|priorities)\b", clean))
    has_read_word = bool(re.search(r"\b(?:check|show|view|list|review|pull up|what'?s|whats|what is|what are|any|due|outstanding|active|open|need|tackle|clear|handle)\b", clean))
    if not has_task_word or not has_read_word:
        return 0
    if re.search(r"\b(?:today|now)\b", clean):
        return 1
    if "tomorrow" in clean:
        return 2
    return 7


def _pwa_topic_news_queries(message: str, recent_context: str = "") -> list[tuple[str, str]]:
    return bot.favourite_news_topic_queries(message, recent_context)


_PWA_TOPIC_MATCH_TERMS = {
    "Teenage Engineering": (
        "teenage engineering",
        "op-xy",
        "op-1",
        "op-z",
        "pocket operator",
        "field system",
        "tp-7",
        "tx-6",
        "cm-15",
        "ep-133",
        "ep-1320",
    ),
    "Android": ("android", "pixel", "google i/o", "google io", "google play", "play store", "material you"),
    "Nothing": ("nothing", "nothing os", "nothing phone", "nothing ear", "cmf", "carl pei"),
    "AI Tools": ("openai", "chatgpt", "codex", "kimi", "moonshot", "gemini", "llm"),
    "iOS": ("ios", "iphone", "ipad", "app store", "testflight", "wwdc"),
    "macOS": ("macos", "macbook", "apple silicon", "xcode"),
    "Solo Dev": ("react", "vite", "capacitor", "railway", "netlify", "github", "solo developer"),
    "Islam": ("islam", "islamic", "muslim", "muis", "khutbah", "ramadan", "solat"),
    "SG Education": ("singapore education", "moe", "school", "teacher", "curriculum"),
    "SG News": ("singapore", "sg news"),
    "Design / UI/UX": ("ui/ux", "ux", "design system", "product design", "interface design", "figma"),
}


_PWA_LOW_SIGNAL_NEWS_RE = re.compile(
    r"\b(?:tomatosystem|public procurement service|public dx market|registers ai-based ui/ux development solution)\b",
    re.I,
)


def _pwa_news_item_matches_topic(label: str, item: str) -> bool:
    if label == "Latest from your shortlist":
        return True
    terms = _PWA_TOPIC_MATCH_TERMS.get(label)
    if not terms:
        return True
    lowered = str(item or "").lower()
    return any(term in lowered for term in terms)


def _pwa_news_item_is_stale(item: str, now: datetime | None = None) -> bool:
    text = str(item or "")
    current = now or datetime.now(bot.SGT)
    full_match = re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+\d{1,2}\s+[A-Za-z]{3}\s+20\d{2}\s+\d{2}:\d{2}:\d{2}\s+GMT\b", text)
    if full_match:
        try:
            item_dt = parsedate_to_datetime(full_match.group(0)).astimezone(bot.SGT)
            age_hours = (current.astimezone(bot.SGT) - item_dt).total_seconds() / 3600
            return age_hours > max(1, int(bot.NEWS_MAX_AGE_HOURS))
        except Exception:
            pass
    match = re.search(r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+(\d{1,2}\s+[A-Za-z]{3}\s+20\d{2})\b", text)
    if match:
        try:
            item_date = datetime.strptime(match.group(1), "%d %b %Y")
            item_dt = bot.SGT.localize(item_date)
            age_hours = (current.astimezone(bot.SGT) - item_dt).total_seconds() / 3600
            return age_hours > max(1, int(bot.NEWS_MAX_AGE_HOURS))
        except ValueError:
            pass
    years = [int(year) for year in re.findall(r"\b(20\d{2})\b", text)]
    return bool(years) and max(years) < current.year


def _pwa_topic_news_items(label: str, result: str, limit: int) -> tuple[list[str], list[str]]:
    raw_items = _fallback_news_items(result, limit=max(limit * 3, 6))
    usable: list[str] = []
    stale: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        if _PWA_LOW_SIGNAL_NEWS_RE.search(item):
            continue
        if not _pwa_news_item_matches_topic(label, item):
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        if _pwa_news_item_is_stale(item):
            stale.append(item)
        else:
            usable.append(item)
        if len(usable) >= limit:
            break
    return usable, stale


def _pwa_topic_news_intro(topics: list[tuple[str, str]]) -> str:
    labels = [label for label, _query in topics]
    if labels == ["Latest from your shortlist"]:
        return "I widened the radar across your shortlist. Useful bits only; confetti stays outside."
    if len(labels) == 1:
        return f"Got it. Narrowing the lens: {labels[0]}. No random buffet plate."
    return f"Got it. Narrowing the lens: {', '.join(labels[:-1])} and {labels[-1]}. No random buffet plate."


async def _pwa_topic_news_reply(message: str, recent_context: str = "") -> str:
    topics = _pwa_topic_news_queries(message, recent_context)
    if not topics:
        return ""

    async def fetch(label: str, query: str):
        try:
            max_items = 6 if not str(query or "").strip() else 2
            result = await bot._execute_tool_offloop(
                "get_latest_news",
                {"query": query, "max_items": max_items, "max_age_hours": bot.NEWS_MAX_AGE_HOURS},
            )
            return label, result
        except Exception as exc:
            bot.logger.warning(f"PWA topic news fetch failed for {label}: {exc}")
            return label, f"Failed to fetch news: {exc}"

    sections: list[str] = []
    for label, result in await asyncio.gather(*(fetch(label, query) for label, query in topics)):
        limit = 4 if label == "Latest from your shortlist" else 2
        items, stale_items = _pwa_topic_news_items(label, result, limit=limit)
        if items:
            bullets = "\n".join(f"- {item}" for item in items)
            sections.append(f"*{label}*\n{bullets}")
        elif str(result or "").startswith("Failed to fetch"):
            sections.append(f"*{label}*\n- Live news check failed here, so I’m not dressing memory up as news.")
        elif stale_items:
            sections.append(f"*{label}*\n- Only stale/low-signal hits came back. I’m leaving them off the main plate.")
        else:
            sections.append(f"*{label}*\n- No fresh usable item. The feed handed me lint; I’m leaving it there.")

    if not sections:
        labels = ", ".join(label for label, _query in topics)
        return f"I ran the live news check for {labels}, but it did not return usable items. I’m not going to guess from memory."
    return _pwa_topic_news_intro(topics) + "\n\n" + "\n\n".join(sections)


def _update_working_memory(history_key: str, history: list, message: str) -> dict:
    memory = _load_working_memory(history_key)
    if _pwa_casual_status_prompt(message) or _pwa_casual_greeting_prompt(message):
        updated = {
            **memory,
            "current_subject": "",
            "pending_action": "",
            "competing_subjects": [],
            "speech_act": "",
            "response_mode": "",
            "tone_read": "",
            "updated_at": datetime.now(bot.SGT).isoformat(),
        }
        _save_working_memory(history_key, updated)
        return updated
    recent_context = "\n".join(
        str(item.get("content", ""))[:400]
        for item in history[-6:]
        if isinstance(item, dict) and isinstance(item.get("content"), str)
    )
    frame = bot.conversation_pragmatic_frame(message, recent_context=recent_context)
    previous_subject = str(memory.get("current_subject", "") or "")
    candidates: list[tuple[str, str, str]] = []
    for item in history[-8:]:
        if not isinstance(item, dict) or not isinstance(item.get("content"), str):
            continue
        role = "user" if item.get("role") == "user" else "assistant"
        for subject in _subject_candidates_from_text(item["content"]):
            candidates.append((role, subject, _clean_turn_text(item["content"])[:220]))
    for subject in _subject_candidates_from_text(message):
        candidates.append(("user", subject, _clean_turn_text(message)[:220]))

    current_subject = previous_subject
    latest_correction = str(memory.get("latest_correction", "") or "")
    for role, subject, source in candidates:
        if role == "user":
            current_subject = subject
            if bot._looks_like_correction(source) or re.search(r"\b(i meant|not .+ it'?s for|no .+ for)\b", source, re.I):
                latest_correction = source
    if not current_subject and candidates:
        current_subject = candidates[-1][1]

    seen_subjects: list[str] = []
    for _, subject, _ in candidates:
        if subject and subject.lower() not in {item.lower() for item in seen_subjects}:
            seen_subjects.append(subject)
    competing = [item for item in seen_subjects if current_subject and item.lower() != current_subject.lower()]
    current_action = _pending_action_from_text(message)
    if current_action:
        pending_action = current_action
    elif bot._is_contextual_followup_reply(message) or bot._is_clarification_detail_reply(message) or _FOLLOWUP_GROUNDING_RE.search(message or ""):
        pending_action = str(memory.get("pending_action", "") or "")
        if not pending_action:
            for item in reversed(history[-8:]):
                if isinstance(item, dict) and isinstance(item.get("content"), str):
                    pending_action = _pending_action_from_text(item["content"])
                    if pending_action:
                        break
    else:
        pending_action = ""
    updated = {
        **memory,
        "current_subject": current_subject,
        "latest_correction": latest_correction,
        "pending_action": pending_action,
        "competing_subjects": competing[-3:],
        "speech_act": str(frame.get("speech_act", "") or ""),
        "response_mode": str(frame.get("response_mode", "") or ""),
        "tone_read": str(frame.get("tone_read", "") or ""),
        "updated_at": datetime.now(bot.SGT).isoformat(),
    }
    _save_working_memory(history_key, updated)
    return updated


def _working_memory_context(memory: dict) -> str:
    subject = memory.get("current_subject")
    pending = memory.get("pending_action")
    correction = memory.get("latest_correction")
    competing = memory.get("competing_subjects") or []
    speech_act = memory.get("speech_act")
    response_mode = memory.get("response_mode")
    tone_read = memory.get("tone_read")
    if not any([subject, pending, correction, competing, speech_act, response_mode, tone_read]):
        return ""
    parts = ["\n\n[Working memory for this PWA chat:"]
    if subject:
        parts.append(f"Current subject: {subject}.")
    if pending:
        parts.append(f"Pending user intent: {pending}.")
    if correction:
        parts.append(f"Latest user correction/clarification: {correction}.")
    if competing:
        parts.append(f"Older competing subjects in this chat: {', '.join(competing)}.")
    if speech_act:
        parts.append(f"Current conversational move: {speech_act}.")
    if response_mode:
        parts.append(f"Preferred response mode: {response_mode}.")
    if tone_read:
        parts.append(f"Tone read: {tone_read}.")
    parts.append(
        "Use the current subject and latest user correction before older assistant guesses. "
        "If action details are still incomplete, ask for only the missing detail.]"
    )
    return " ".join(parts)


def _working_memory_summary(memory: dict) -> dict:
    summary = {
        "subject": memory.get("current_subject", ""),
        "action": memory.get("pending_action", ""),
        "conflict": ", ".join(memory.get("competing_subjects") or []),
    }
    return {key: value for key, value in summary.items() if value}


def _clamp_int(value, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, int(round(value))))


def _home_intelligence(results: dict, days: int) -> dict:
    daily_load = results.get("daily_load") if isinstance(results.get("daily_load"), dict) else {}
    today_load = daily_load.get("today") if isinstance(daily_load.get("today"), dict) else {}
    agenda = results.get("agenda_structured") if isinstance(results.get("agenda_structured"), dict) else {}
    today_agenda = (agenda.get("days") or [{}])[0] if isinstance(agenda.get("days"), list) else {}
    proactive = results.get("proactive") if isinstance(results.get("proactive"), dict) else {}
    marking = results.get("marking") if isinstance(results.get("marking"), dict) else {}
    services = results.get("services") if isinstance(results.get("services"), dict) else {}
    tasks = results.get("tasks_structured") if isinstance(results.get("tasks_structured"), dict) else {}
    task_items = tasks.get("items") if isinstance(tasks.get("items"), list) else []
    classops = results.get("classops") if isinstance(results.get("classops"), dict) else {}
    classops_signal = classops_ai.top_home_signal(classops)

    load_score = int(today_load.get("score") or 0)
    due_count = int(today_load.get("due") or len(today_agenda.get("due") or []))
    lesson_count = int(today_load.get("lessons") or len(today_agenda.get("lessons") or []))
    event_count = int(today_load.get("events") or len(today_agenda.get("events") or []))
    unmarked = int(marking.get("unmarked_scripts") or today_load.get("marking_scripts") or 0)
    active_stacks = int(marking.get("active_stacks") or 0)
    current = datetime.now(bot.SGT)
    today_date = current.date()
    today = today_date.isoformat()
    soon = (today_date + bot.timedelta(days=3)).isoformat()
    overdue_tasks = [item for item in task_items if item.get("overdue")]
    due_today_tasks = [item for item in task_items if str(item.get("due") or "") <= today]
    due_soon_tasks = [item for item in task_items if today < str(item.get("due") or "") <= soon]
    service_values = {key: value for key, value in services.items() if not str(key).startswith("_")}
    connected_count = sum(1 for value in service_values.values() if value)
    disconnected_count = max(0, len(service_values) - connected_count)
    classops_open = int(classops.get("open_submission_count") or classops.get("pending_count") or 0)
    classops_concerns = int(classops.get("concern_count") or 0)
    classops_due_now = int(classops.get("due_today_count") or 0) + int(classops.get("overdue_count") or 0)

    readiness = _clamp_int(
        94
        - (load_score * 0.42)
        - min(18, due_count * 5)
        - min(18, unmarked * 1.2)
        - min(14, len(overdue_tasks) * 7)
        - min(10, disconnected_count * 2.5)
        - min(16, classops_due_now * 5)
        - min(12, classops_concerns * 1.4)
    )
    if readiness >= 78:
        mode = "Deep Work Window" if load_score <= 34 and due_count == 0 else "Steady Ops"
        tone = "green"
    elif readiness >= 56:
        mode = "Execution Mode"
        tone = "yellow"
    elif unmarked >= 10:
        mode = "Marking Control"
        tone = "orange"
    else:
        mode = "Triage Mode"
        tone = "red"

    risks = []
    if overdue_tasks:
        first = overdue_tasks[0]
        risks.append({
            "label": "Overdue",
            "detail": first.get("description") or f"{len(overdue_tasks)} overdue task(s)",
            "severity": "red",
        })
    if due_count or due_today_tasks:
        risks.append({
            "label": "Due pressure",
            "detail": f"{max(due_count, len(due_today_tasks))} due item(s) need attention",
            "severity": "orange" if due_count > 1 else "yellow",
        })
    if unmarked:
        risks.append({
            "label": "Marking load",
            "detail": f"{unmarked} script(s) left across {active_stacks or 1} stack(s)",
            "severity": "orange" if unmarked >= 10 else "yellow",
        })
    if disconnected_count:
        risks.append({
            "label": "Blind spots",
            "detail": f"{disconnected_count} service(s) not connected",
            "severity": "yellow",
        })
    if classops_signal:
        risks.append({
            "label": "ClassOps",
            "detail": classops_signal.get("detail") or classops_signal.get("title") or "Student follow-up signal detected",
            "severity": classops_signal.get("severity") or "yellow",
        })

    future_days = daily_load.get("days") if isinstance(daily_load.get("days"), list) else []
    future_spike = max(
        future_days[1: max(2, min(len(future_days), days))],
        key=lambda item: int(item.get("score") or 0),
        default=None,
    )
    if future_spike and int(future_spike.get("score") or 0) >= 70:
        risks.append({
            "label": "Load spike",
            "detail": f"{future_spike.get('label', 'Soon')} is trending {str(future_spike.get('load', 'heavy')).lower()}",
            "severity": "orange",
        })

    opportunities = []
    if readiness >= 78:
        opportunities.append({
            "label": "Good window",
            "detail": "Use this for one deeper task before admin expands.",
        })
    if not due_count and not overdue_tasks:
        opportunities.append({
            "label": "No due drag",
            "detail": "A clean moment to pull future work forward.",
        })
    if connected_count == len(services) and services:
        opportunities.append({
            "label": "Full telemetry",
            "detail": "Calendar, Drive, and mail signals are available.",
        })

    top = proactive.get("top") if isinstance(proactive.get("top"), list) else []
    lead = top[0] if top else None
    classops_priority = int(classops_signal.get("score", 0) or 0) if classops_signal else 0
    if classops_signal and (classops_signal.get("severity") in {"red", "orange"} or classops_priority >= 45):
        next_move_title = classops_signal.get("title") or "Clear ClassOps follow-up"
        next_move_body = classops_signal.get("detail") or "Open ClassOps and settle the highest-risk student/class signal first."
        next_prompt = (
            f"Help me clear this ClassOps signal now: {next_move_title}. "
            f"Context: {next_move_body}. Give me a tight student follow-up plan."
        )
    elif lead:
        next_move_title = lead.get("title") or "Act on the lead signal"
        next_move_body = lead.get("body") or lead.get("action_hint") or "Follow the highest-ranked proactive signal."
        next_prompt = (
            f"Help me execute this H.I.R.A lead signal now: {next_move_title}. "
            f"Context: {next_move_body}. Give me a tight next-action plan."
        )
    elif unmarked:
        next_move_title = "Cut the marking queue"
        next_move_body = "Start with a small marking slice: 5 scripts or 20 minutes, then update progress."
        next_prompt = "Plan a focused marking sprint from my current marking load. Give me a time-boxed plan, easiest first step, and what to postpone."
    elif task_items:
        first = task_items[0]
        next_move_title = "Clear the first task"
        next_move_body = first.get("description") or "Handle the highest-ranked task in the task brief."
        next_prompt = f"Help me complete this task efficiently: {next_move_body}"
    else:
        next_move_title = "Protect a clean block"
        next_move_body = daily_load.get("rest_note") or daily_load.get("note") or "No critical signal is dominating right now."
        next_prompt = "Find the best use of my next clean block based on my calendar, tasks, marking load, and current energy."

    first_task = task_items[0] if task_items else {}
    if classops_signal and (classops_signal.get("severity") in {"red", "orange"} or classops_priority >= 45):
        now_step = f"Open ClassOps and handle: {classops_signal.get('title', 'highest-risk student follow-up')}."
    elif overdue_tasks:
        now_step = f"Clear or reschedule overdue item: {overdue_tasks[0].get('description', 'highest-risk task')}."
    elif unmarked:
        now_step = "Mark a small slice first: 5 scripts or 20 minutes, then update H.I.R.A with progress."
    elif first_task:
        now_step = f"Start the first task: {first_task.get('description', 'highest-ranked task')}."
    elif readiness >= 78:
        now_step = "Use the next clean block for one deep task before messages and admin expand."
    else:
        now_step = "Ask H.I.R.A for triage before committing to any new work."

    if lesson_count or event_count:
        next_step = "Prepare for the next scheduled anchor, then keep the calendar gap protected."
    elif future_spike and int(future_spike.get("score") or 0) >= 70:
        next_step = f"Pull work forward before {future_spike.get('label', 'the next spike')}."
    elif due_count:
        next_step = "Move one due item from pending to done before starting discretionary work."
    else:
        next_step = "Batch low-friction admin after the deep block, not before it."

    if classops_open and not classops_due_now:
        later_step = f"Update ClassOps after collection so {classops_open} open submission signal(s) do not go stale."
    elif disconnected_count:
        later_step = "Reconnect missing services so future intelligence has fewer blind spots."
    elif unmarked:
        later_step = "Update marking progress, then let H.I.R.A recalculate the load."
    else:
        later_step = "Review the next 7 days and schedule one pre-emptive block."

    evidence = [
        f"Load score {load_score}",
        f"{due_count} due",
        f"{unmarked} unmarked",
        f"{connected_count}/{len(service_values) or 0} services connected",
    ]
    if classops.get("connected"):
        evidence.append(f"{classops_concerns} ClassOps concern(s)")
    confidence = "High" if connected_count >= 3 else "Medium" if connected_count else "Limited"

    forecast_items = []
    if future_spike and int(future_spike.get("score") or 0) >= 70:
        forecast_items.append({
            "label": "Pressure spike",
            "when": future_spike.get("label") or future_spike.get("date") or "Soon",
            "detail": f"Expected {str(future_spike.get('load', 'heavy')).lower()} load. Pull one task forward before then.",
            "severity": "orange",
        })
    elif future_spike:
        forecast_items.append({
            "label": "Next pressure",
            "when": future_spike.get("label") or future_spike.get("date") or "Soon",
            "detail": f"Highest visible load is {future_spike.get('score', 0)}. Keep one buffer block open.",
            "severity": "green",
        })
    if due_soon_tasks:
        forecast_items.append({
            "label": "Due cluster",
            "when": "72h",
            "detail": f"{len(due_soon_tasks)} task(s) due soon. First: {due_soon_tasks[0].get('description', 'upcoming task')}.",
            "severity": "yellow",
        })
    if unmarked >= 8:
        forecast_items.append({
            "label": "Marking drag",
            "when": "Today",
            "detail": "Marking load is large enough to leak into tomorrow unless sliced early.",
            "severity": "orange",
        })
    if classops_due_now:
        forecast_items.append({
            "label": "ClassOps pressure",
            "when": "Today",
            "detail": f"{classops_due_now} tracked collection/follow-up signal(s) need attention.",
            "severity": "orange",
        })
    elif classops_signal and classops_signal.get("severity") == "yellow":
        forecast_items.append({
            "label": "Student pattern",
            "when": "Soon",
            "detail": classops_signal.get("detail") or "ClassOps has a student pattern worth checking.",
            "severity": "yellow",
        })
    if not forecast_items:
        forecast_items.append({
            "label": "Stable horizon",
            "when": "7d",
            "detail": "No major spike detected from available telemetry. Protect one proactive block anyway.",
            "severity": "green",
        })

    def time_window(start_minutes: int, duration: int) -> str:
        start = current + bot.timedelta(minutes=start_minutes)
        end = start + bot.timedelta(minutes=duration)
        return f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"

    plan_blocks = [
        {
            "label": "Prime",
            "time": time_window(0, 20),
            "title": next_move_title,
            "detail": now_step,
        },
        {
            "label": "Build",
            "time": time_window(25, 45),
            "title": "Main execution block",
            "detail": next_step,
        },
        {
            "label": "Stabilize",
            "time": "Before shutdown",
            "title": "Close the loop",
            "detail": later_step,
        },
    ]
    if unmarked:
        plan_blocks.insert(1, {
            "label": "Slice",
            "time": time_window(20, 25),
            "title": "Marking sprint",
            "detail": "Mark a bounded slice and report progress before switching context.",
        })
    plan_blocks = plan_blocks[:4]

    trial_prompt = (
        "Log a H.I.R.A trial note for today. Include: did the readiness score match reality, "
        "did I follow the Now/Next/Later protocol, what did H.I.R.A miss, and one adjustment for tomorrow."
    )

    actions = [
        {
            "label": "Run Triage",
            "icon": "list-checks",
            "action": "send",
            "prompt": "Triage my current load. Pick the top 3 things I should handle next, explain why, and give me a realistic order of attack.",
        },
        {
            "label": "Next Move",
            "icon": "move-right",
            "action": "send",
            "prompt": next_prompt,
        },
        {
            "label": "Make Plan",
            "icon": "calendar-plus",
            "action": "fill",
            "prompt": "Turn this into a practical plan with time blocks: ",
        },
        {
            "label": "Trial Log",
            "icon": "clipboard-check",
            "action": "fill",
            "prompt": trial_prompt,
        },
    ]
    if unmarked:
        actions.insert(1, {
            "label": "Marking Sprint",
            "icon": "timer-reset",
            "action": "send",
            "prompt": "Plan a focused marking sprint from my current marking load. Give me a time-boxed plan, easiest first step, and what to postpone.",
        })

    return {
        "generated_at": datetime.now(bot.SGT).strftime("%A, %-d %B %Y, %H:%M SGT"),
        "readiness": readiness,
        "tone": tone,
        "mode": mode,
        "signal": f"{lesson_count} lesson(s), {event_count} event(s), {due_count} due, {unmarked} unmarked.",
        "next_move": {
            "title": next_move_title,
            "body": next_move_body,
            "prompt": next_prompt,
        },
        "risks": risks[:4],
        "opportunities": opportunities[:3],
        "protocol": {
            "confidence": confidence,
            "evidence": evidence,
            "steps": [
                {"phase": "Now", "time": "0-20m", "task": now_step},
                {"phase": "Next", "time": "20-60m", "task": next_step},
                {"phase": "Later", "time": "Today", "task": later_step},
            ],
        },
        "forecast": {
            "horizon": f"{min(max(days, 1), 14)} days",
            "items": forecast_items[:4],
        },
        "adaptive_plan": {
            "blocks": plan_blocks,
        },
        "trial": {
            "metric": "Did H.I.R.A reduce decision friction today?",
            "target": "Follow the Now/Next/Later protocol at least once and log one correction.",
            "checkpoints": [
                "Was the readiness score directionally right?",
                "Did the first action reduce ambiguity?",
                "Did H.I.R.A catch the real risk before you felt it?",
            ],
            "review_prompt": trial_prompt,
        },
        "actions": actions[:5],
    }


def _home_fallbacks() -> dict:
    return {
        "agenda": "Agenda unavailable right now.",
        "agenda_structured": {
            "generated_at": "",
            "days": [],
            "services": {"google": False},
        },
        "daily_load": {
            "today": {
                "score": 0,
                "tone": "green",
                "load": "Pretty chill",
                "lessons": 0,
                "events": 0,
                "due": 0,
                "marking_scripts": 0,
            },
            "days": [],
            "note": "Daily load unavailable until schedule data is connected.",
            "previous_week": [],
            "next_week": [],
            "rest_note": "Workload comparison unavailable until schedule data is connected.",
        },
        "digest": {
            "generated_at": "",
            "items": [],
        },
        "proactive": {
            "generated_at": "",
            "top": [],
            "queue_count": 0,
            "ready_count": 0,
            "suppressed_count": 0,
            "changed": [],
        },
        "tasks": "Task brief unavailable until Google is connected.",
        "tasks_structured": {
            "generated_at": "",
            "end_date": "",
            "items": [],
        },
        "islamic": "Islamic rhythm unavailable right now.",
        "prayers": {
            "ok": False,
            "today": "",
            "now": "",
            "window_minutes": 20,
            "prayers": [],
        },
        "files": "File memory unavailable until Google is connected.",
        "services": {
            "google": False,
            "calendar": False,
            "work_drive": False,
            "personal_gmail": False,
            "personal_gmail2": False,
            "work_gmail": False,
        },
        "marking": {
            "active_stacks": 0,
            "total_scripts": 0,
            "marked_scripts": 0,
            "unmarked_scripts": 0,
            "connected": False,
        },
        "classops": {
            "connected": False,
            "class_count": 0,
            "assignment_count": 0,
            "pending_count": 0,
            "concern_count": 0,
            "due_today_count": 0,
            "overdue_count": 0,
            "classes": [],
        },
        "briefing_delivery": {
            "today": "",
            "generated_at": "",
            "overall": "unknown",
            "summary": "Digest delivery status unavailable.",
            "slots": [],
        },
    }


def _record_home_timing(timings: list[dict] | None, phase: str, elapsed_ms: int, status: str = "ok", detail: str = "") -> None:
    if timings is not None:
        timings.append({
            "phase": phase,
            "elapsed_ms": elapsed_ms,
            "status": status,
            "detail": detail[:160],
        })
    if status != "ok" or elapsed_ms >= 3000:
        bot.logger.info("Home sync phase=%s status=%s elapsed_ms=%s detail=%s", phase, status, elapsed_ms, detail[:240])


def _home_timing(timings: list[dict] | None, phase: str, started: float, status: str = "ok", detail: str = "") -> None:
    _record_home_timing(timings, phase, round((time.perf_counter() - started) * 1000), status, detail)


def _home_run_jobs(jobs: dict, fallbacks: dict, timeout: float, timings: list[dict], prefix: str = "") -> dict:
    def run_timed(builder):
        started = time.perf_counter()
        try:
            return "ok", builder(), round((time.perf_counter() - started) * 1000), ""
        except Exception as exc:
            return "error", None, round((time.perf_counter() - started) * 1000), str(exc)

    submitted_at = {key: time.perf_counter() for key in jobs}
    futures = {key: _HOME_EXECUTOR.submit(run_timed, builder) for key, builder in jobs.items()}
    wait(futures.values(), timeout=timeout)
    results = {}
    for key, future in futures.items():
        phase = f"{prefix}{key}" if prefix else key
        if not future.done():
            future.cancel()
            results[key] = fallbacks[key]
            _home_timing(timings, phase, submitted_at[key], "timeout", f">{timeout:.1f}s")
            continue
        try:
            status, value, elapsed_ms, detail = future.result()
            if status == "ok":
                results[key] = value
                _record_home_timing(timings, phase, elapsed_ms)
            else:
                results[key] = fallbacks[key]
                _record_home_timing(timings, phase, elapsed_ms, "error", detail)
        except Exception as exc:
            results[key] = fallbacks[key]
            _home_timing(timings, phase, submitted_at[key], "error", str(exc))
    return results


def _home_snapshot(days: int, timings: list[dict] | None = None) -> dict:
    snapshot = {
        "google": bot.google_ok(),
        "events": [],
        "reminders": [],
        "task_metadata": {},
        "marking_tasks": [],
        "memory": {},
    }
    if not snapshot["google"]:
        return snapshot
    fallbacks = {
        "events": [],
        "reminders": [],
        "task_metadata": {},
        "marking_tasks": [],
        "memory": {},
    }
    jobs = {
        "events": lambda: bot.gs.get_events_for_days(days),
        "reminders": bot.gs.get_reminders,
        "task_metadata": bot.gs.get_task_metadata,
        "marking_tasks": bot.gs.get_marking_tasks,
        "memory": bot.gs.get_memory,
    }
    timing_sink = timings if timings is not None else []
    snapshot.update(_home_run_jobs(jobs, fallbacks, _HOME_PRIMARY_TIMEOUT_SECONDS, timing_sink, prefix="snapshot."))
    return snapshot


def _home_school_day_cleared_memory_for_date(target: date | datetime | str, memory: dict | None) -> str:
    try:
        if isinstance(target, datetime):
            day = target.astimezone(bot.SGT).date()
        elif isinstance(target, date):
            day = target
        else:
            day = date.fromisoformat(str(target)[:10])
    except Exception:
        return ""
    markers = (
        f"{bot.TIMETABLE_CLEAR_MEMORY_PREFIX}{day.isoformat()}",
        f"{bot.RELIEF_MEMORY_PREFIX}{day.isoformat()}",
        f"{bot.ABSENCE_MEMORY_PREFIX}{day.isoformat()}",
    )
    teaching_memory = (memory or {}).get("teaching", []) or []
    for item in reversed(teaching_memory):
        text = str(item or "").strip()
        if any(marker in text for marker in markers):
            return text
    return ""


def _home_week_config() -> tuple[str | None, str | None]:
    ref_date = _cached_google_config_value("week_ref_date")
    ref_type = _cached_google_config_value("week_ref_type")
    if ref_date or ref_type:
        return ref_date, ref_type
    return bot._get_week_config()


def _home_lessons_for_date(target: date, week_config: tuple[str | None, str | None] | None = None):
    official_week = bot.tt.get_school_week_info(target)
    if official_week:
        day_name = bot.tt.DAY_MAP.get(target.weekday())
        if not day_name or official_week["is_school_holiday"]:
            return [], ""
        lessons = bot.tt.TIMETABLE.get((day_name, official_week["week_type"]), [])
        return lessons, bot.tt.week_type_label(official_week["week_type"])

    ref_date, ref_type = week_config or (None, None)
    if not ref_date or not ref_type:
        return [], ""
    lessons = bot.tt.get_lessons(target, ref_date, ref_type)
    wt = bot.tt.get_week_type(ref_date, ref_type, target)
    return lessons, bot.tt.week_type_label(wt)


def _home_agenda_structured(days: int, snapshot: dict) -> dict:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(bot.SGT)
    today = now.date()
    end_date = today + bot.timedelta(days=days - 1)
    day_map = {}
    relief_cache = {}
    memory = snapshot.get("memory") or {}
    week_config = _home_week_config()
    for offset in range(days):
        target = today + bot.timedelta(days=offset)
        relief_cache[target.isoformat()] = bool(_home_school_day_cleared_memory_for_date(target, memory))
        lessons, _wt_label = _home_lessons_for_date(target, week_config)
        visible_lessons = [] if relief_cache[target.isoformat()] else list(lessons or [])
        day_map[target.isoformat()] = {
            "date": target.isoformat(),
            "label": target.strftime("%A, %-d %B"),
            "week": bot._agenda_week_display(target),
            "relieved": relief_cache[target.isoformat()],
            "lessons": [
                {
                    "time": f"{lesson['start']}-{lesson['end']}",
                    "subject": lesson["subject"],
                    "title": lesson["description"],
                    "room": lesson["room"] if lesson["room"] != "-" else "",
                    "kind": "lesson",
                }
                for lesson in visible_lessons
            ],
            "events": [],
            "due": [],
        }
    for event in snapshot.get("events") or []:
        try:
            item = bot._event_to_agenda_item(event)
        except Exception:
            continue
        if item["date"] in day_map:
            day_map[item["date"]]["events"].append(item)
    for reminder in snapshot.get("reminders") or []:
        due = reminder.get("due", "")
        if today.isoformat() <= due <= end_date.isoformat() and due in day_map:
            day_map[due]["due"].append({
                "id": reminder.get("id", ""),
                "title": reminder.get("description", ""),
                "category": reminder.get("category", ""),
                "kind": "due",
            })
    return {
        "generated_at": now.strftime("%A, %-d %B %Y, %H:%M SGT"),
        "days": list(day_map.values()),
        "services": {"google": bool(snapshot.get("google"))},
    }


def _home_agenda_text(days: int, structured: dict, snapshot: dict) -> str:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(bot.SGT)
    today = now.date()
    end_date = today + bot.timedelta(days=days)
    lines = [f"*Agenda*\n_{now.strftime('%A, %-d %B %Y, %H:%M SGT')}_\n"]
    lessons, wt_label = _home_lessons_for_date(today, _home_week_config())
    if wt_label and today.weekday() < 5:
        today_relief = bool((structured.get("days") or [{}])[0].get("relieved"))
        visible_lessons = [] if today_relief else list(lessons or [])
        lines.append(f"*Today at school ({bot._week_display(wt_label, today)})*")
        lines.append(bot.tt.format_lessons(visible_lessons))
        lines.append("")
    if not snapshot.get("google"):
        lines.append("_Google not connected._")
        return "\n".join(lines)
    lines.append(f"*Calendar - next {days} days*")
    lines.append(bot.gs.format_events(snapshot.get("events") or [], show_date=True))
    lines.append("")
    reminders = snapshot.get("reminders") or []
    overdue = [r for r in reminders if r.get("due", "") < today.isoformat()]
    due_window = [r for r in reminders if today.isoformat() <= r.get("due", "") <= end_date.isoformat()]
    if overdue:
        lines.append("*Overdue*")
        for r in sorted(overdue, key=lambda x: x.get("due", "")):
            lines.append(f"- `[{r['id']}]` {r['due']} - {r['description']} _{r['category']}_")
        lines.append("")
    lines.append(f"*Due by {end_date.strftime('%-d %b')}*")
    if due_window:
        for r in sorted(due_window, key=lambda x: x.get("due", "")):
            lines.append(f"- `[{r['id']}]` {r['due']} - {r['description']} _{r['category']}_")
    else:
        lines.append("Nothing due in this window.")
    return "\n".join(lines).strip()


def _home_enriched_tasks(snapshot: dict) -> list[dict]:
    metadata = snapshot.get("task_metadata") or {}
    return [
        {**reminder, **(metadata.get(str(reminder.get("id", ""))) or {})}
        for reminder in snapshot.get("reminders") or []
    ]


def _home_task_structured(days: int, tasks: list[dict]) -> dict:
    today = datetime.now(bot.SGT).date()
    window = max(1, min(int(days or 7), 30))
    end_date = today + bot.timedelta(days=window)
    active = [task for task in tasks if task.get("due", "9999-12-31") <= end_date.isoformat()]
    items = []
    for task in sorted(active, key=lambda item: bot._task_priority_score(item, today))[:30]:
        due = task.get("due", "")
        overdue = False
        weekday = ""
        try:
            due_date = date.fromisoformat(due)
            overdue = due_date < today
            weekday = due_date.strftime("%A")
        except Exception:
            pass
        items.append({
            "id": str(task.get("id", "")),
            "description": task.get("description", ""),
            "due": due,
            "weekday": weekday,
            "category": task.get("category", ""),
            "priority": task.get("priority", ""),
            "effort": task.get("effort", ""),
            "next_action": task.get("next_action", ""),
            "overdue": overdue,
        })
    return {
        "generated_at": datetime.now(bot.SGT).strftime("%A, %-d %B %Y, %H:%M SGT"),
        "end_date": end_date.isoformat(),
        "items": items,
    }


def _home_task_text(days: int, tasks: list[dict]) -> str:
    today = datetime.now(bot.SGT).date()
    end_date = today + bot.timedelta(days=max(1, min(int(days or 7), 30)))
    active = [task for task in tasks if task.get("due", "9999-12-31") <= end_date.isoformat()]
    if not active:
        return "No active tasks in that window."
    lines = [f"*Task brief* - now to {end_date.strftime('%-d %b')}\n"]
    for task in sorted(active, key=lambda item: bot._task_priority_score(item, today))[:12]:
        next_action = f"\n  Next: {task['next_action']}" if task.get("next_action") else ""
        overdue = " OVERDUE" if task.get("due", "") < today.isoformat() else ""
        lines.append(f"- `[{task['id']}]` {task['due']}{overdue} - {task['description']}{next_action}")
    return "\n".join(lines)


def _home_load_days_for_dates(
    dates: list,
    today: date,
    reminders: list[dict],
    memory: dict | None = None,
    week_config: tuple[str | None, str | None] | None = None,
) -> list[dict]:
    if not dates:
        return []
    event_counts = {target.isoformat(): 0 for target in dates}
    due_counts = {target.isoformat(): 0 for target in dates}
    # Keep comparative load cheap: the live calendar is already fetched for the
    # main agenda. Do not make extra Calendar calls just to decorate last/next
    # week comparison, because that keeps the home screen in "Syncing".
    date_set = set(due_counts.keys())
    for reminder in reminders:
        due = reminder.get("due", "")
        if due in date_set:
            due_counts[due] += 1
    load_days = []
    for target in dates:
        lessons, _ = _home_lessons_for_date(target, week_config)
        key = target.isoformat()
        lesson_count = 0 if _home_school_day_cleared_memory_for_date(target, memory) else len(lessons or [])
        load_days.append(bot._daily_load_item(
            target,
            today,
            lesson_count,
            event_counts.get(key, 0),
            due_counts.get(key, 0),
            0,
        ))
    return load_days


def _home_daily_load(days: int, structured: dict, snapshot: dict) -> dict:
    today = datetime.now(bot.SGT).date()
    marking_scripts = sum(
        max(0, int(task.get("total_scripts") or 0) - int(task.get("marked_count") or 0))
        for task in snapshot.get("marking_tasks") or []
    )
    load_days = []
    for index, day in enumerate(structured.get("days", [])):
        try:
            date_obj = datetime.fromisoformat(day["date"]).date()
        except Exception:
            continue
        scripts_today = marking_scripts if index == 0 else 0
        lesson_count = 0 if day.get("relieved") else len(day.get("lessons", []))
        load_days.append(bot._daily_load_item(
            date_obj,
            today,
            lesson_count,
            len(day.get("events", [])),
            len(day.get("due", [])),
            scripts_today,
        ))
    today_load = load_days[0] if load_days else {
        "score": 0,
        "tone": "green",
        "load": "Pretty chill",
        "lessons": 0,
        "events": 0,
        "due": 0,
        "marking_scripts": 0,
    }
    reminders = snapshot.get("reminders") or []
    memory = snapshot.get("memory") or {}
    week_config = _home_week_config()
    previous_week = _home_load_days_for_dates(bot._weekday_neighbors(today, -1), today, reminders, memory=memory, week_config=week_config)
    next_week = _home_load_days_for_dates(bot._weekday_neighbors(today, 1), today, reminders, memory=memory, week_config=week_config)
    return {
        "today": today_load,
        "days": load_days,
        "note": bot._daily_load_note(today_load),
        "previous_week": previous_week,
        "next_week": next_week,
        "rest_note": bot._rest_load_note(previous_week, next_week),
    }


def _home_files_index(snapshot: dict) -> str:
    if not snapshot.get("google"):
        return "Google is not connected."
    files = (snapshot.get("memory") or {}).get("files", [])
    if not files:
        return "No generated or uploaded files remembered yet."
    lines = ["*Artifact library*"]
    for item in files[-20:]:
        lines.append(f"- {item}")
    return "\n".join(lines)


def _parallel_home_data(days: int) -> dict:
    total_started = time.perf_counter()
    timings: list[dict] = []
    fallbacks = _home_fallbacks()
    try:
        core_started = time.perf_counter()
        snapshot_started = time.perf_counter()
        snapshot = _home_snapshot(days, timings=timings)
        _home_timing(timings, "core.snapshot", snapshot_started)
        agenda_started = time.perf_counter()
        agenda_structured = _home_agenda_structured(days, snapshot)
        _home_timing(timings, "core.agenda_structured", agenda_started)
        task_enrich_started = time.perf_counter()
        enriched_tasks = _home_enriched_tasks(snapshot)
        _home_timing(timings, "core.task_enrich", task_enrich_started)
        agenda_text_started = time.perf_counter()
        agenda_text = _home_agenda_text(days, agenda_structured, snapshot)
        _home_timing(timings, "core.agenda_text", agenda_text_started)
        daily_load_started = time.perf_counter()
        daily_load = _home_daily_load(days, agenda_structured, snapshot)
        _home_timing(timings, "core.daily_load", daily_load_started)
        task_text_started = time.perf_counter()
        task_text = _home_task_text(days, enriched_tasks)
        tasks_structured = _home_task_structured(days, enriched_tasks)
        _home_timing(timings, "core.tasks", task_text_started)
        files_started = time.perf_counter()
        files_index = _home_files_index(snapshot)
        _home_timing(timings, "core.files", files_started)
        marking_started = time.perf_counter()
        marking = _marking_summary_from_tasks(snapshot.get("marking_tasks") or [], connected=bool(snapshot.get("google")))
        _home_timing(timings, "core.marking", marking_started)
        results = {
            "agenda": agenda_text,
            "agenda_structured": agenda_structured,
            "daily_load": daily_load,
            "tasks": task_text,
            "tasks_structured": tasks_structured,
            "files": files_index,
            "marking": marking,
        }
        _home_timing(timings, "core_build", core_started)
    except Exception as exc:
        _home_timing(timings, "core_build", total_started, "error", str(exc))
        bot.logger.warning(f"Home snapshot build failed: {exc}")
        snapshot = {}
        results = {key: fallbacks[key] for key in ("agenda", "agenda_structured", "daily_load", "tasks", "tasks_structured", "files", "marking")}
        try:
            fallback_started = time.perf_counter()
            agenda_structured = bot.build_agenda_structured(days)
            results["agenda_structured"] = agenda_structured
            results["daily_load"] = bot.build_daily_load(days)
            results["agenda"] = bot.build_agenda(days)
            _home_timing(timings, "legacy_fallback", fallback_started)
        except Exception as fallback_exc:
            _home_timing(timings, "legacy_fallback", total_started, "error", str(fallback_exc))
            bot.logger.warning(f"Home timetable fallback failed: {fallback_exc}")

    services_started = time.perf_counter()
    try:
        results["services"] = _service_status()
        _home_timing(timings, "extra.services", services_started)
    except Exception as exc:
        results["services"] = fallbacks["services"]
        _home_timing(timings, "extra.services", services_started, "error", str(exc))

    jobs = {
        # Keep delivery status ahead of slower enrichment jobs.
        # These cards are the first place the UI reports whether H.I.R.A is
        # alive, so they should not be starved by digest/ClassOps fetches.
        "briefing_delivery": _briefing_delivery_status,
        "prayers": bot.prayer_notification_status,
        "islamic": lambda: bot.build_islamic_brief(),
        "digest": bot.build_curated_digest_snapshot,
        "proactive": lambda: bot.build_proactive_v2_snapshot(days=days),
        "classops": _classops_status_summary,
    }
    results.update(_home_run_jobs(jobs, fallbacks, _HOME_SECONDARY_TIMEOUT_SECONDS, timings, prefix="extra."))
    results["intelligence"] = _home_intelligence(results, days)
    _home_timing(timings, "total", total_started)
    results["sync_timings"] = timings
    return results


def _cached_google_config_value(key: str) -> str:
    cache = getattr(bot.gs, "_config_cache", None)
    cache_valid = getattr(bot.gs, "_config_cache_valid", None)
    if not isinstance(cache, dict) or not callable(cache_valid):
        return ""
    try:
        if not cache_valid():
            return ""
        values = cache.get("values") or {}
        return str(values.get(key, "") or "")
    except Exception:
        return ""


def _cached_work_gmail_monitor_status() -> dict:
    raw = _cached_google_config_value(getattr(bot, "WORK_GMAIL_MONITOR_STATUS_KEY", "work_gmail_monitor_status"))
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _service_status() -> dict:
    # Home connection chips must stay cheap: they render while live sources may
    # be degraded, so do not perform fresh Google reads here.
    work_gmail_status = _cached_work_gmail_monitor_status()
    work_gmail_configured = bot.gs.gmail_ok("work")
    work_gmail_detail = str(work_gmail_status.get("detail", "") or "")
    work_gmail_error = str(work_gmail_status.get("status", "") or "").strip().lower() == "error"
    work_gmail_revoked = bool(re.search(
        r"\b(invalid_grant|expired|revoked|unauthorized|invalid credentials)\b",
        work_gmail_detail,
        re.I,
    ))
    work_gmail_healthy = work_gmail_configured and not (work_gmail_error and work_gmail_revoked)
    work_gmail_state = "on" if work_gmail_healthy else "reconnect" if work_gmail_configured and work_gmail_revoked else "off"
    return {
        "google": bot.google_ok(),
        "calendar": bot.google_ok(),
        "work_drive": bot.google_ok(),
        "personal_gmail": bot.gs.gmail_ok("personal"),
        "personal_gmail2": bot.gs.gmail_ok("personal2"),
        "work_gmail": work_gmail_healthy,
        "dropbox": dropbox.configured(),
        "_details": {
            "work_gmail": {
                "configured": work_gmail_configured,
                "healthy": work_gmail_healthy,
                "state": work_gmail_state,
                "label": "Reconnect" if work_gmail_state == "reconnect" else "On" if work_gmail_state == "on" else "Off",
                "detail": work_gmail_detail,
                "last_run": work_gmail_status.get("last_run", "") or work_gmail_status.get("checked_at", ""),
            }
        },
    }


def _git_commit_sha() -> str:
    for key in (
        "RAILWAY_GIT_COMMIT_SHA",
        "GIT_COMMIT_SHA",
        "SOURCE_VERSION",
        "HEROKU_SLUG_COMMIT",
        "VERCEL_GIT_COMMIT_SHA",
    ):
        value = os.environ.get(key, "").strip()
        if value:
            return value[:12]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=str(APP_DIR),
            capture_output=True,
            text=True,
            timeout=1,
            check=False,
        )
        return result.stdout.strip()[:12]
    except Exception:
        return ""


@app.get("/api/app/version")
def app_version():
    return {
        "app_version": PWA_APP_VERSION,
        "service_worker_cache": PWA_SERVICE_WORKER_CACHE,
        "git_commit": _git_commit_sha(),
        "server_time": datetime.now(bot.SGT).isoformat(),
    }


@app.post("/api/auth/session")
async def auth_session(request: Request, req: AuthSessionRequest):
    expected = _expected_web_token()
    if not expected:
        raise HTTPException(status_code=503, detail="HIRA_WEB_TOKEN is not configured. Set it in Railway environment variables.")
    if not _token_matches(req.token, expected):
        if not await _AUTH_RATE_LIMITER.is_allowed(_auth_rate_key(request, req.token)):
            raise HTTPException(
                status_code=429,
                detail="Too many invalid token attempts. Try again in a minute.",
                headers={"Retry-After": "60"},
            )
        raise HTTPException(status_code=401, detail="Invalid H.I.R.A web token")
    response = JSONResponse({"ok": True, "expires_in": _SESSION_MAX_AGE_SECONDS})
    response.set_cookie(
        _SESSION_COOKIE_NAME,
        _new_session_cookie(expected),
        max_age=_SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=_cookie_secure_for_request(request),
        samesite="strict",
        path="/api",
    )
    return response


@app.post("/api/auth/logout")
def auth_logout(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    response = JSONResponse({"ok": True})
    response.delete_cookie(_SESSION_COOKIE_NAME, path="/api")
    return response


def _marking_display_title(title: str) -> str:
    clean = " ".join((title or "").split())
    if not clean or "[" in clean:
        return clean
    if ":" in clean:
        class_label, work_type = [part.strip() for part in clean.split(":", 1)]
        if class_label and work_type:
            return f"{class_label} [{work_type}]"
    return clean


def _marking_summary() -> dict:
    try:
        tasks = bot.gs.get_marking_tasks()
    except Exception:
        return {
            "active_stacks": 0,
            "total_scripts": 0,
            "marked_scripts": 0,
            "unmarked_scripts": 0,
            "all_clear": False,
            "sets": [],
            "connected": False,
        }
    return _marking_summary_from_tasks(tasks, connected=True)


def _marking_summary_from_tasks(tasks: list[dict], connected: bool = True) -> dict:
    tasks = tasks or []

    total_scripts = sum(max(0, int(task.get("total_scripts") or 0)) for task in tasks)
    marked_scripts = sum(max(0, int(task.get("marked_count") or 0)) for task in tasks)
    marked_scripts = min(marked_scripts, total_scripts) if total_scripts else marked_scripts
    sets = []
    for task in tasks:
        task_total = max(0, int(task.get("total_scripts") or 0))
        task_marked = max(0, int(task.get("marked_count") or 0))
        if task_total:
            task_marked = min(task_marked, task_total)
        sets.append({
            "id": str(task.get("id", "")),
            "title": task.get("title", ""),
            "display_title": _marking_display_title(task.get("title", "")),
            "total_scripts": task_total,
            "marked_scripts": task_marked,
            "unmarked_scripts": max(0, task_total - task_marked) if task_total else 0,
            "progress_label": f"{task_marked}/{task_total}" if task_total else f"{task_marked} marked",
            "collected_date": task.get("collected_date", ""),
        })
    return {
        "active_stacks": len(tasks),
        "total_scripts": total_scripts,
        "marked_scripts": marked_scripts,
        "unmarked_scripts": max(0, total_scripts - marked_scripts),
        "all_clear": not tasks,
        "sets": sets,
        "connected": connected,
    }


def _expected_web_token() -> str:
    return os.environ.get("HIRA_WEB_TOKEN", "").strip()


def _token_matches(candidate: str | None, expected: str) -> bool:
    candidate = str(candidate or "")
    return bool(expected) and secrets.compare_digest(candidate, expected)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _session_signature(payload: str, expected: str) -> str:
    digest = hmac.new(expected.encode("utf-8"), payload.encode("ascii"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def _new_session_cookie(expected: str) -> str:
    payload = _b64url_encode(json.dumps({
        "exp": int(time.time()) + _SESSION_MAX_AGE_SECONDS,
        "nonce": secrets.token_urlsafe(18),
    }, separators=(",", ":")).encode("utf-8"))
    return f"{payload}.{_session_signature(payload, expected)}"


def _session_cookie_valid(value: str | None, expected: str) -> bool:
    if not value or not expected or "." not in value:
        return False
    payload, signature = value.rsplit(".", 1)
    if not secrets.compare_digest(signature, _session_signature(payload, expected)):
        return False
    try:
        data = json.loads(_b64url_decode(payload).decode("utf-8"))
    except Exception:
        return False
    return int(data.get("exp", 0) or 0) >= int(time.time())


def _request_session_valid(request: Request | None, expected: str | None = None) -> bool:
    if request is None:
        return False
    expected = expected if expected is not None else _expected_web_token()
    return _session_cookie_valid(request.cookies.get(_SESSION_COOKIE_NAME), expected)


def _cookie_secure_for_request(request: Request) -> bool:
    proto = request.url.scheme
    if _env_bool("HIRA_TRUST_PROXY_HEADERS", False):
        proto = request.headers.get("x-forwarded-proto", proto).split(",", 1)[0].strip().lower()
    return _running_in_production() or proto == "https"


def _same_origin(request: Request, origin: str) -> bool:
    try:
        parsed = urlparse(origin)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return False
        request_scheme = request.headers.get("x-forwarded-proto", request.url.scheme).split(",", 1)[0].strip().lower()
        request_host_header = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.netloc)).split(",", 1)[0].strip()
        request_host = urlparse(f"//{request_host_header}")

        def default_port(scheme: str) -> int:
            return 443 if scheme == "https" else 80

        parsed_port = parsed.port or default_port(parsed.scheme.lower())
        request_port = request_host.port or default_port(request_scheme)
        return (
            parsed.scheme.lower() == request_scheme
            and (parsed.hostname or "").lower() == (request_host.hostname or "").lower()
            and parsed_port == request_port
        )
    except Exception:
        return False


def _csrf_request_allowed(request: Request) -> bool:
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return True
    fetch_site = request.headers.get("sec-fetch-site", "").strip().lower()
    if fetch_site and fetch_site != "same-origin":
        return False
    if request.headers.get("x-hira-csrf", "").strip() == "1":
        return True
    origin = request.headers.get("origin", "").strip()
    if origin:
        return _same_origin(request, origin)
    referer = request.headers.get("referer", "").strip()
    if referer:
        return _same_origin(request, referer)
    return False


def _require_token(x_hira_token: Optional[str] = Header(default=None)):
    expected = _expected_web_token()
    if not expected:
        raise HTTPException(status_code=503, detail=_AUTH_NOT_CONFIGURED_DETAIL)
    if not _token_matches(x_hira_token, expected):
        request = _REQUEST_CONTEXT.get()
        if _request_session_valid(request, expected) and _csrf_request_allowed(request):
            return
        raise HTTPException(status_code=401, detail="Invalid H.I.R.A web token")


# ─── Rate limiting ────────────────────────────────────────────────────────────
import time as _time
from collections import defaultdict as _defaultdict


class _SlidingWindowRateLimiter:
    """Sliding window rate limiter keyed by IP, Redis-backed when available."""

    def __init__(self, name: str, max_requests: int, window_seconds: int = 60):
        self._name = re.sub(r"[^a-z0-9_-]+", "-", str(name or "default").lower()).strip("-") or "default"
        self._max = max_requests
        self._window = window_seconds
        self._buckets: dict[str, deque[float]] = _defaultdict(deque)
        self._lock: asyncio.Lock | None = None

    async def is_allowed(self, key: str) -> bool:
        redis = bot._get_redis()
        if redis:
            try:
                return await asyncio.to_thread(self._redis_is_allowed, redis, key)
            except Exception as exc:
                bot.logger.warning(f"Redis rate limiter {self._name} failed; using local fallback: {exc}")
        return await self._local_is_allowed(key)

    def _redis_key(self, key: str) -> str:
        digest = hashlib.sha256(str(key or "unknown").encode("utf-8")).hexdigest()[:32]
        return f"hira:rate:{self._name}:{digest}"

    def _redis_is_allowed(self, redis, key: str) -> bool:
        now = _time.time()
        member = f"{now:.6f}:{secrets.token_hex(4)}"
        redis_key = self._redis_key(key)
        allowed = redis.eval(
            """
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local max_requests = tonumber(ARGV[3])
            local member = ARGV[4]
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            local count = redis.call('ZCARD', key)
            if count >= max_requests then
                redis.call('EXPIRE', key, math.max(2, window * 2))
                return 0
            end
            redis.call('ZADD', key, now, member)
            redis.call('EXPIRE', key, math.max(2, window * 2))
            return 1
            """,
            1,
            redis_key,
            now,
            self._window,
            self._max,
            member,
        )
        return bool(int(allowed or 0))

    async def _local_is_allowed(self, key: str) -> bool:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            now = _time.monotonic()
            cutoff = now - self._window
            bucket = self._buckets[key]
            # Drop timestamps outside the window
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= self._max:
                return False
            bucket.append(now)
            return True

    async def prune(self):
        """Remove stale buckets — call periodically to avoid memory growth."""
        if self._lock is None:
            return
        async with self._lock:
            cutoff = _time.monotonic() - self._window
            stale = [k for k, v in self._buckets.items() if not v or v[-1] < cutoff]
            for k in stale:
                del self._buckets[k]


_CHAT_RATE_LIMITER   = _SlidingWindowRateLimiter("chat", max_requests=_env_int("HIRA_CHAT_RATE_LIMIT", 20), window_seconds=60)
_UPLOAD_RATE_LIMITER = _SlidingWindowRateLimiter("upload", max_requests=_env_int("HIRA_UPLOAD_RATE_LIMIT", 10), window_seconds=60)
_AUTH_RATE_LIMITER   = _SlidingWindowRateLimiter("auth", max_requests=_AUTH_RATE_LIMIT, window_seconds=60)


def _request_ip(request: Request) -> str:
    """Return a rate-limit key without trusting spoofable proxy headers by default."""
    direct = request.client.host if request.client else "unknown"
    if not _env_bool("HIRA_TRUST_PROXY_HEADERS", False):
        return direct
    try:
        direct_ip = ipaddress.ip_address(direct)
    except ValueError:
        return direct
    if not (direct_ip.is_loopback or direct_ip.is_private):
        return direct
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        candidate = forwarded.split(",")[0].strip()
        try:
            ipaddress.ip_address(candidate)
            return candidate
        except ValueError:
            return direct
    for header in ("x-real-ip", "cf-connecting-ip", "true-client-ip"):
        candidate = request.headers.get(header, "").strip()
        if candidate:
            try:
                ipaddress.ip_address(candidate)
                return candidate
            except ValueError:
                return direct
    return direct


def _auth_rate_key(request: Request, candidate: str | None = None) -> str:
    token_hint = hashlib.sha256(str(candidate or "").encode("utf-8")).hexdigest()[:12] if candidate else "missing"
    return f"{_request_ip(request)}:{token_hint}"


def _client_key(client_id: str | None) -> str:
    return _normalise_client_key(client_id)


def _upload_job_key(job_id: str) -> str:
    return f"hira:upload_job:{job_id}"


def _upload_request_key(client_key: str, request_id: str) -> str:
    safe_client = _normalise_client_key(client_key)
    safe_request = re.sub(r"[^a-zA-Z0-9_.:-]", "_", str(request_id or "").strip())[:120]
    return f"hira:upload_request:{safe_client}:{safe_request}"


def _upload_job_public(job: dict) -> dict:
    return {
        "job_id": job.get("job_id", ""),
        "status": job.get("status", "queued"),
        "filename": job.get("filename", ""),
        "created": job.get("created", ""),
        "updated": job.get("updated", ""),
        "reply": job.get("reply", ""),
        "index": job.get("index", ""),
        "error": job.get("error", ""),
    }


async def _upload_queue_worker(worker_id: int):
    bot.logger.info("Upload queue worker %s started.", worker_id)
    while True:
        try:
            if _UPLOAD_QUEUE is None:
                await asyncio.sleep(1)
                continue
            job = await _UPLOAD_QUEUE.get()
            try:
                await _run_upload_job(
                    job["job_id"],
                    job["tmp_path"],
                    job["mime"],
                    job["filename"],
                    job["note"],
                )
            finally:
                _UPLOAD_QUEUE.task_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.exception(f"Upload queue worker {worker_id} failed: {exc}")
            await asyncio.sleep(2)


def _upload_queue_depth() -> int | None:
    return _UPLOAD_QUEUE.qsize() if _UPLOAD_QUEUE is not None else None


async def _enqueue_upload_job(job: dict):
    if _UPLOAD_QUEUE is None:
        raise HTTPException(status_code=503, detail="Upload queue is not ready yet. Try again in a moment.")
    try:
        _UPLOAD_QUEUE.put_nowait(job)
    except asyncio.QueueFull as exc:
        raise HTTPException(
            status_code=503,
            detail="Upload queue is full. Try again after the current files finish processing.",
            headers={"Retry-After": "30"},
        ) from exc


def _set_upload_job(job_id: str, update: dict) -> dict:
    now = datetime.now(bot.SGT).isoformat()
    existing = _get_upload_job(job_id, include_missing=True) or {"job_id": job_id, "created": now}
    job = {**existing, **update, "job_id": job_id, "updated": now}
    r = bot._get_redis()
    if r:
        r.setex(_upload_job_key(job_id), 86400, json.dumps(job, ensure_ascii=False))
    _UPLOAD_JOBS[job_id] = job
    _UPLOAD_JOBS.move_to_end(job_id)
    while len(_UPLOAD_JOBS) > _MAX_LOCAL_UPLOAD_JOBS:
        _UPLOAD_JOBS.popitem(last=False)
    return job


def _get_upload_job_id_for_request(client_key: str, request_id: str) -> str:
    request_id = str(request_id or "").strip()
    if not request_id:
        return ""
    key = _upload_request_key(client_key, request_id)
    r = bot._get_redis()
    if r:
        try:
            raw = r.get(key)
            if raw:
                return str(raw.decode() if isinstance(raw, bytes) else raw)
        except Exception as exc:
            bot.logger.warning(f"Upload request Redis read failed: {exc}")
    return _UPLOAD_REQUESTS.get(key, "")


def _set_upload_request_job(client_key: str, request_id: str, job_id: str) -> None:
    request_id = str(request_id or "").strip()
    job_id = str(job_id or "").strip()
    if not request_id or not job_id:
        return
    key = _upload_request_key(client_key, request_id)
    r = bot._get_redis()
    if r:
        try:
            r.setex(key, 86400, job_id)
        except Exception as exc:
            bot.logger.warning(f"Upload request Redis write failed: {exc}")
    _UPLOAD_REQUESTS[key] = job_id
    _UPLOAD_REQUESTS.move_to_end(key)
    while len(_UPLOAD_REQUESTS) > _MAX_LOCAL_UPLOAD_JOBS:
        _UPLOAD_REQUESTS.popitem(last=False)


def _get_upload_job(job_id: str, include_missing: bool = False) -> dict | None:
    job_id = str(job_id or "").strip()
    if not job_id:
        return None
    r = bot._get_redis()
    if r:
        try:
            raw = r.get(_upload_job_key(job_id))
            if raw:
                return json.loads(raw)
        except Exception as exc:
            bot.logger.warning(f"Upload job Redis read failed: {exc}")
    job = _UPLOAD_JOBS.get(job_id)
    if job:
        _UPLOAD_JOBS.move_to_end(job_id)
        return job
    return None if include_missing else {"job_id": job_id, "status": "missing", "error": "Upload job not found or expired."}


async def _spool_upload_to_temp(file: UploadFile, max_bytes: int) -> tuple[str, int]:
    suffix = Path(file.filename or "").suffix or ".upload"
    total = 0
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Upload is too large. Limit is {max_bytes // (1024 * 1024)} MB.",
                    )
                tmp.write(chunk)
        if not total:
            raise HTTPException(status_code=400, detail="Empty file")
        return tmp_path, total
    except Exception:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
        raise


async def _read_upload_bytes(file: UploadFile, max_bytes: int, label: str = "Upload") -> bytes:
    data = bytearray()
    total = 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"{label} is too large. Limit is {max_bytes // (1024 * 1024)} MB.",
            )
        data.extend(chunk)
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")
    return bytes(data)


def _prepare_image_for_vision(data: bytes, mime: str = "", filename: str = "") -> tuple[bytes, str, str]:
    original_size = len(data or b"")
    if not data:
        return data, mime or "image/png", ""
    max_edge = _env_int("HIRA_VISION_MAX_IMAGE_EDGE", 2200, minimum=512)
    max_bytes = _env_int("HIRA_VISION_MAX_IMAGE_MB", 4, minimum=1) * 1024 * 1024
    try:
        from PIL import Image, ImageOps

        Image.MAX_IMAGE_PIXELS = max(Image.MAX_IMAGE_PIXELS or 0, 80_000_000)
        with Image.open(io.BytesIO(data)) as raw:
            image = ImageOps.exif_transpose(raw)
            original_size_px = image.size
            if image.mode not in {"RGB", "L"}:
                background = Image.new("RGB", image.size, "white")
                alpha = image.getchannel("A") if "A" in image.getbands() else None
                background.paste(image.convert("RGBA"), mask=alpha)
                image = background
            else:
                image = image.convert("RGB")
            image.thumbnail((max_edge, max_edge), Image.Resampling.LANCZOS)

            def encode_jpeg(quality: int) -> bytes:
                output = io.BytesIO()
                image.save(output, format="JPEG", quality=quality, optimize=True, progressive=True)
                return output.getvalue()

            encoded = encode_jpeg(88)
            quality = 82
            while len(encoded) > max_bytes and quality >= 58:
                encoded = encode_jpeg(quality)
                quality -= 8
            note = ""
            if image.size != original_size_px or len(encoded) < original_size:
                note = (
                    f"Image normalised for vision: {filename or 'upload'} "
                    f"{original_size_px[0]}x{original_size_px[1]} -> {image.size[0]}x{image.size[1]}, "
                    f"{original_size // 1024}KB -> {len(encoded) // 1024}KB."
                )
            return encoded, "image/jpeg", note
    except Exception as exc:
        bot.logger.warning("Image normalisation unavailable for %s: %s", filename or mime or "upload", exc)
        return data, mime or "image/png", ""


async def _analyse_image_bytes(data: bytes, mime: str, filename: str, note: str):
    prepared, media_type, normalise_note = _prepare_image_for_vision(data, mime, filename)
    encoded = base64.b64encode(prepared).decode()
    user_note = note or "Extract useful schedule items, actions, dates, and reminders from this image."
    if normalise_note:
        user_note = f"{user_note}\n\nProcessing note: {normalise_note}"
    reply_text = await bot._run_agentic_chat(
        [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": encoded}},
            {"type": "text", "text": f"{bot.MEDIA_SCHEDULE_INSTRUCTION}\n\nUser note: {user_note}"}
        ]}],
        max_tokens=2200,
        tools=[bot.CONTEXT_TOOL, bot.CALENDAR_TOOL, bot.REMINDER_TOOL, bot.MEMORY_TOOL],
        direct_user_text=note,
    )
    index = f"Image analysed: {filename or 'uploaded image'}"
    if normalise_note:
        index = f"{index}\n{normalise_note}"
    return {"reply": reply_text, "index": index}


@app.get("/healthz")
def healthz():
    return {"ok": not _memory_pressure_high()}


def _health_details():
    limit = bot._memory_limit_mb()
    rss = bot._rss_mb()
    redis_connected = bot._get_redis() is not None
    return {
        "ok": not _memory_pressure_high(),
        "rss_mb": round(rss, 1),
        "memory_limit_mb": round(limit, 1) if limit else None,
        "memory_ratio": round(rss / limit, 3) if limit else None,
        "redis_connected": redis_connected,
        "redis_required": bot.redis_required(),
        "web_inline_scheduler": _WEB_INLINE_SCHEDULER,
        "schedules": {
            "morning_briefing_sgt": f"{bot.MORNING_BRIEFING_TIME[0]:02d}:{bot.MORNING_BRIEFING_TIME[1]:02d}",
            "evening_briefing_sgt": f"{bot.EVENING_BRIEFING_TIME[0]:02d}:{bot.EVENING_BRIEFING_TIME[1]:02d}",
            "daily_job_grace_minutes": bot.DAILY_JOB_GRACE_MINUTES,
        },
        "upload_queue_depth": _upload_queue_depth(),
        "upload_queue_max": _UPLOAD_QUEUE_MAX,
        "upload_queue_workers": len(_UPLOAD_QUEUE_TASKS),
        "upload_jobs_tracked": len(_UPLOAD_JOBS),
        "chat_slots_available": getattr(_CHAT_SEMAPHORE, "_value", None),
        "upload_slots_available": getattr(_UPLOAD_SEMAPHORE, "_value", None),
        "models": {
            "agentic": bot.AGENTIC_MODEL,
            "deep": bot.DEEP_MODEL,
            "quick": bot.QUICK_MODEL,
            "router": bot.ROUTER_MODEL,
            "structured": bot.STRUCTURED_MODEL,
        },
    }


@app.get("/")
def index():
    return FileResponse(PWA_DIR / "index.html")


@app.get("/growth")
@app.get("/hira-growth")
def growth_site():
    return FileResponse(PWA_DIR / "hira-growth.html")


@app.get("/classops")
def classops_site():
    return FileResponse(PWA_DIR / "classops.html")


@app.get("/manifest.webmanifest")
def manifest():
    return FileResponse(PWA_DIR / "manifest.webmanifest", media_type="application/manifest+json")


@app.get("/service-worker.js")
def service_worker():
    return FileResponse(PWA_DIR / "service-worker.js", media_type="application/javascript")


@app.get("/styles.css")
def root_styles():
    return FileResponse(PWA_DIR / "styles.css", media_type="text/css")


@app.get("/app.js")
def root_app_js():
    return FileResponse(PWA_DIR / "app.js", media_type="application/javascript")


@app.get("/hira-growth.css")
def root_growth_css():
    return FileResponse(PWA_DIR / "hira-growth.css", media_type="text/css")


@app.get("/hira-growth.js")
def root_growth_js():
    return FileResponse(PWA_DIR / "hira-growth.js", media_type="application/javascript")


@app.get("/hira-growth-data.json")
def root_growth_data():
    return FileResponse(PWA_DIR / "hira-growth-data.json", media_type="application/json")


@app.get("/classops.css")
def root_classops_css():
    return FileResponse(PWA_DIR / "classops.css", media_type="text/css")


@app.get("/classops.js")
def root_classops_js():
    return FileResponse(PWA_DIR / "classops.js", media_type="application/javascript")


@app.get("/api/home")
async def home(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    days = max(1, min(14, days))
    now = datetime.now(bot.SGT)
    async with _HOME_SEMAPHORE:
        data = await asyncio.to_thread(_parallel_home_data, days)
    return {
        "greeting": now.strftime("%A, %-d %B"),
        "time_label": now.strftime("%H:%M SGT"),
        **data,
    }


@app.get("/api/lesson/now")
async def lesson_now(full: bool = False, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    builder = bot.build_situation_model if full else bot.build_next_lesson_companion
    return await asyncio.to_thread(builder)


def _briefing_replay_slot(message: str) -> str:
    clean = " ".join((message or "").lower().split())
    if not clean:
        return ""
    wants_digest = re.search(r"\b(digest|briefing|brief|roundup)\b", clean)
    wants_live = _wants_live_briefing(clean)
    if wants_live:
        return ""
    wants_replay = re.search(r"\b(show|replay|open|missed|earlier|this morning|this evening)\b", clean)
    if not wants_digest or not wants_replay:
        return ""
    if re.search(r"\b(evening|roundup|tonight)\b", clean):
        return "evening"
    if re.search(r"\b(morning|today|digest|briefing|brief)\b", clean):
        return "morning"
    return ""


def _wants_live_briefing(clean: str) -> bool:
    live_terms = r"\b(right now|live|current|fresh|now(?:'s|s)?|lets have it|let's have it|good time)\b"
    return bool(re.search(live_terms, clean))


def _live_briefing_slot(message: str) -> str:
    clean = " ".join((message or "").lower().split())
    if not clean:
        return ""
    if not re.search(r"\b(brief me|briefing|brief)\b", clean):
        return ""
    if (
        re.search(r"\b(?:brief me|briefing|brief)\s+(?:on|about|re:)\b", clean)
        and not re.search(r"\b(morning|evening|roundup)\b", clean)
    ):
        return ""
    if not _wants_live_briefing(clean):
        return ""
    if re.search(r"\b(evening|roundup|tonight)\b", clean):
        return "evening"
    return "morning"


def _notification_matches_briefing_slot(item: dict, slot: str) -> bool:
    haystack = " ".join(
        str(item.get(key, "") or "").lower()
        for key in ("kind", "title", "source", "body")
    )
    if "briefing" not in haystack and "digest" not in haystack and "roundup" not in haystack:
        return False
    if slot == "evening":
        return "evening" in haystack or "roundup" in haystack
    return "morning" in haystack or "morning digest" in haystack


def _briefing_notification_date(item: dict) -> str:
    source = str(item.get("source", "") or "").strip()
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", source)
    if match:
        return match.group(1)
    created = _parse_sgt_datetime(str(item.get("created_at") or item.get("created") or ""))
    return created.strftime("%Y-%m-%d") if created else ""


def _latest_stored_briefing(slot: str, target_date: str = "") -> dict | None:
    try:
        notifications = bot.gs.get_app_notifications(include_archived=True)
    except Exception as exc:
        bot.logger.warning(f"Briefing replay notification lookup failed: {exc}")
        return None
    for item in reversed(notifications):
        if not isinstance(item, dict) or not _notification_matches_briefing_slot(item, slot):
            continue
        if target_date and _briefing_notification_date(item) != target_date:
            continue
        return item
    return None


def _briefing_replay_text(slot: str) -> str:
    today_key = datetime.now(bot.SGT).strftime("%Y-%m-%d")
    item = _latest_stored_briefing(slot, target_date=today_key)
    if item and str(item.get("body", "") or "").strip():
        return str(item.get("body", "")).strip()
    try:
        if slot == "evening":
            return bot.build_evening_briefing()
        return bot.build_briefing(record_news_digest=False)
    except Exception as exc:
        bot.logger.warning(f"Briefing replay rebuild failed: {exc}")
        label = "evening roundup" if slot == "evening" else "morning briefing"
        return f"I could not replay the stored {label} or rebuild it right now. The notification is saved in the bell panel if it reached this device."


def _live_briefing_text(slot: str) -> str:
    try:
        if slot == "evening":
            return bot.build_evening_briefing()
        return bot.build_briefing(record_news_digest=False)
    except Exception as exc:
        bot.logger.warning(f"Live briefing rebuild failed: {exc}")
        return "I could not build a fresh briefing right now. Try again in a moment."


def _new_chat_trace(message: str, route_name: str = "", include_memory: bool = True) -> dict:
    discipline = bot.source_discipline_for_text(message)
    memory_sources = []
    if message and include_memory:
        try:
            memory_sources = [
                {
                    "category": str(item.get("category", "")),
                    "score": int(item.get("score", 0) or 0),
                    "text": str(item.get("text", ""))[:320],
                }
                for item in bot.retrieve_relevant_memory(message, limit=5)
            ]
        except Exception as exc:
            bot.logger.debug(f"Trace memory source lookup failed: {exc}")
    return {
        "id": uuid.uuid4().hex[:12],
        "route": route_name,
        "tools_available": [],
        "forced_tool": "",
        "tools_called": [],
        "source_contracts_seen": [],
        "memory_sources": memory_sources,
        "confidence_gate": "pending" if discipline.get("needs_live_check") else "not_required",
        "error_phase": "",
        "final_mode": "",
        "timings": {},
        "source_discipline": {
            "needs_live_check": bool(discipline.get("needs_live_check")),
            "recommended_tools": list(discipline.get("recommended_tools") or []),
            "confidence": discipline.get("confidence", ""),
        },
        "thread_state": {},
        "model_policy": {},
        "response_contract": {},
    }


def _merge_chat_trace(trace: dict, patch: dict | None = None) -> dict:
    if not patch:
        return trace
    for key, value in patch.items():
        if key in {"tools_available", "tools_called"}:
            existing = [str(item) for item in trace.get(key, []) if str(item)]
            for item in value or []:
                clean = str(item or "").strip()
                if clean and clean not in existing:
                    existing.append(clean)
            trace[key] = existing
        elif key == "source_contracts_seen":
            existing = list(trace.get(key, []) or [])
            seen = {
                (
                    str(item.get("status", "")),
                    str(item.get("as_of", "")),
                    str(item.get("source", "")),
                    str(item.get("reason", "")),
                )
                for item in existing
                if isinstance(item, dict)
            }
            for item in value or []:
                if not isinstance(item, dict):
                    continue
                key_tuple = (
                    str(item.get("status", "")),
                    str(item.get("as_of", "")),
                    str(item.get("source", "")),
                    str(item.get("reason", "")),
                )
                if key_tuple not in seen:
                    seen.add(key_tuple)
                    existing.append({
                        "status": str(item.get("status", "")),
                        "as_of": str(item.get("as_of", "")),
                        "source": str(item.get("source", "")),
                        "reason": str(item.get("reason", "")),
                    })
            trace[key] = existing
        elif key == "timings" and isinstance(value, dict):
            timings = dict(trace.get("timings") or {})
            timings.update(value)
            trace["timings"] = timings
        elif key in {"thread_state", "model_policy", "response_contract"} and isinstance(value, dict):
            merged = dict(trace.get(key) or {})
            merged.update(value)
            trace[key] = merged
        elif value not in (None, ""):
            trace[key] = value
    return trace


def _thread_state_context(thread_state: dict) -> str:
    if not thread_state or not thread_state.get("is_followup"):
        return ""
    payload = {
        "is_followup": True,
        "topic_signals": thread_state.get("topic_signals", []),
        "contextual_tool": thread_state.get("contextual_tool", ""),
        "needs_live_check": bool(thread_state.get("needs_live_check")),
        "effective_text": str(thread_state.get("effective_text", ""))[:700],
        "pragmatic_frame": thread_state.get("pragmatic_frame", {}),
    }
    return (
        "\n\n[Thread state: The user is continuing a recent offer/topic. "
        f"Use this resolved state instead of treating the message as standalone: {json.dumps(payload, ensure_ascii=False)}]"
    )


def response_contract_for_reply(reply_text: str, trace: dict | None = None) -> dict:
    text = " ".join(str(reply_text or "").split())
    lowered = text.lower()
    trace = trace or {}
    source_discipline = trace.get("source_discipline") or {}
    tools_available = set(str(item) for item in trace.get("tools_available", []) if item)
    live_tool_available = bool(tools_available & {
        "get_liverpool_brief",
        "get_f1_brief",
        "get_nea_weather",
        "get_muis_prayer_times",
        "get_muis_friday_khutbah",
        "get_latest_news",
        "web_research",
        "web_search",
        "fetch_url",
    })
    no_access_claim = bool(re.search(
        r"\b(?:don'?t|do not|can'?t|cannot)\s+(?:have|access|check|browse|see|get)\b.{0,80}\b(?:live|current|latest|standings?|web|internet|source|sources|data)\b",
        lowered,
    ))
    native_evidence = bool(
        trace.get("openai_citations")
        or trace.get("source_contracts_seen")
        or any(
            str(item.get("status", "")).lower() in {"completed", "succeeded", "success"}
            for item in trace.get("openai_native_observations", []) or []
            if isinstance(item, dict)
        )
    )
    missing_source_contract = (
        bool(source_discipline.get("needs_live_check"))
        and not native_evidence
        and not trace.get("tools_called")
    )
    empty_answer = _empty_chat_reply(text)
    weak_answer = (
        str(trace.get("route", "")) != "quick"
        and (
            bool(source_discipline.get("needs_live_check"))
            or bool(trace.get("tools_available"))
            or bool(trace.get("forced_tool"))
        )
        and (
            bot._normalise_short_reply(text) in {"yep", "yes", "ok", "okay", "sure", "done"}
            or len(text.split()) <= 3
        )
    )
    status = "ok"
    if empty_answer:
        status = "empty_answer"
    elif no_access_claim and live_tool_available:
        status = "unsupported_no_access_claim"
    elif weak_answer:
        status = "weak_answer"
    elif missing_source_contract:
        status = "missing_source_contract"
    return {
        "status": status,
        "empty_answer": empty_answer,
        "unsupported_no_access_claim": bool(no_access_claim and live_tool_available),
        "weak_answer": bool(weak_answer),
        "missing_source_contract": bool(missing_source_contract),
    }


def _finalise_chat_trace(trace: dict, final_mode: str = "answered") -> dict:
    if not trace.get("final_mode"):
        trace["final_mode"] = final_mode
    contract = trace.get("response_contract") or {}
    if contract.get("status") in {"empty_answer", "unsupported_no_access_claim", "weak_answer", "missing_source_contract"}:
        trace["confidence_gate"] = "review_needed"
    contracts = trace.get("source_contracts_seen") or []
    if contracts and trace.get("confidence_gate") in {"pending", "passed"}:
        statuses = {str(item.get("status", "")).lower() for item in contracts if isinstance(item, dict)}
        trace["confidence_gate"] = "passed" if statuses and statuses <= {"confirmed"} else "review_needed"
    elif (
        trace.get("confidence_gate") == "pending"
        and (trace.get("openai_citations") or trace.get("openai_native_observations"))
    ):
        trace["confidence_gate"] = "passed"
    elif trace.get("confidence_gate") == "pending":
        trace["confidence_gate"] = "no_contract" if trace.get("source_discipline", {}).get("needs_live_check") else "not_required"
    return trace


def _quick_sse_response(reply: str, history_key: str, history: list, route_name: str = "quick", tool_name: str = ""):
    history.append({"role": "assistant", "content": reply})
    _save_history_best_effort(history_key, history)
    if route_name == "quick" and bot.google_ok():
        user_text = next(
            (
                bot._strip_injected_chat_context(str(item.get("content", "")))
                for item in reversed(history[:-1])
                if isinstance(item, dict) and item.get("role") == "user" and isinstance(item.get("content"), str)
            ),
            "",
        )
        if user_text:
            _schedule_background_call(
                "quick chat learning event",
                bot.record_chat_learning_event,
                user_text,
                reply,
                source="pwa",
            )
    trace = _new_chat_trace("", route_name=route_name)
    if tool_name:
        _merge_chat_trace(trace, {"tools_called": [tool_name]})
    _finalise_chat_trace(trace)

    async def events():
        def sse(payload: dict) -> str:
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        try:
            yield sse({"type": "route", "name": route_name})
            yield sse({"type": "trace", "trace": trace})
            if tool_name:
                yield sse({"type": "tool", "name": tool_name})
            yield sse({"type": "text", "text": reply})
            yield sse({"type": "done", "text": reply})
            yield sse({"type": "saved"})
        finally:
            _CHAT_SEMAPHORE.release()
            bot._log_memory(f"after pwa {route_name}")

    return StreamingResponse(events(), media_type="text/event-stream")


def _presence_tool_name(message: str, forced_tool: str, tools: list[dict], thread_state: dict | None = None) -> str:
    if forced_tool:
        return forced_tool
    available = {str(tool.get("name", "")) for tool in tools or [] if isinstance(tool, dict)}
    recommended = list((thread_state or {}).get("recommended_tools") or [])
    for name in recommended:
        if name in available:
            return name
    if (thread_state or {}).get("needs_live_check"):
        for name in ("get_latest_news", "web_search", "web_research", "fetch_url"):
            if name in available:
                return name
    return ""


def _streaming_presence_preface(message: str, tool_name: str) -> str:
    tool = str(tool_name or "").strip()
    if not tool:
        return ""
    clean = " ".join(str(message or "").strip().split())
    lower = clean.lower()
    if tool == "get_gmail_brief":
        account, _query = bot._extract_gmail_account_from_text(clean)
        label = "Work Gmail" if account == "work" else "your second personal Gmail" if account == "personal2" else "Gmail"
        if bot.re.search(r"\b(?:link|links|url|urls)\b", lower):
            return f"I'll check {label} for the links now."
        return f"I'll check {label} now."
    if tool == "create_gmail_draft":
        return "I'll pull the email context and draft this in the same pass."
    if tool == "get_assistant_context":
        return "I'll check your current H.I.R.A context first."
    if tool == "get_timetable":
        return "I'll check the timetable before answering."
    if tool == "get_mtl_classlists":
        return "I'll check the classlist data first."
    if tool in {"analyze_mtl_scores", "generate_mtl_score_trend_report"}:
        return "I'll analyse the class data first."
    if tool == "get_cca_schedule":
        return "I'll check the CCA schedule first."
    if tool == "get_nea_weather":
        return "I'll check NEA weather first."
    if tool in {"get_muis_prayer_times", "get_muis_friday_khutbah"}:
        return "I'll check MUIS data first."
    if tool in {"get_latest_news", "get_liverpool_brief", "get_f1_brief", "web_search", "web_research", "fetch_url"}:
        return "I'll run a live source check first."
    if tool in {"add_reminder", "create_calendar_event", "create_proactive_nudge", "create_followup"}:
        return "I'll handle that now and confirm exactly what changed."
    if tool in {"create_document_artifact", "create_slide_deck_artifact"}:
        return "I'll build the artifact and give you the useful version first."
    return ""


def _archive_notifications_by_source(source: str) -> list[str]:
    clean_source = str(source or "").strip()
    if not clean_source:
        return []
    notifications = bot.gs.get_app_notifications(include_archived=True)
    ids = [
        str(item.get("id", "")).strip()
        for item in notifications
        if str(item.get("source", "")).strip() == clean_source and not item.get("archived")
    ]
    ids = [item_id for item_id in ids if item_id]
    return ids if ids and bot.gs.archive_app_notifications(ids) else []


def _archive_completed_notification(req_id: str, source: str) -> list[str]:
    archived_ids: list[str] = []
    if source:
        try:
            archived_ids = _archive_notifications_by_source(source)
        except Exception as exc:
            bot.logger.warning(f"Could not archive notification siblings for source={source}: {exc}")
    req_id = str(req_id or "").strip()
    if req_id and req_id not in archived_ids:
        try:
            if bot.gs.archive_app_notifications([req_id]):
                archived_ids.append(req_id)
        except Exception as exc:
            bot.logger.warning(f"Could not archive completed notification {req_id}: {exc}")
    return archived_ids


def _archive_nudge_notifications(nudge_id: str) -> int:
    source = f"nudge:{str(nudge_id or '').strip()}"
    if source == "nudge:":
        return 0
    return len(_archive_notifications_by_source(source))


def _active_checkin_notification_ids() -> list[str]:
    try:
        notifications = bot.gs.get_app_notifications(include_archived=True)
    except Exception as exc:
        bot.logger.warning(f"Could not inspect active check-in notifications: {exc}")
        return []
    checkin_ids: list[str] = []
    seen: set[str] = set()
    for item in reversed(notifications):
        if item.get("archived"):
            continue
        match = re.fullmatch(r"checkin:(\w[\w-]*)", str(item.get("source", "") or "").strip())
        if not match:
            continue
        checkin_id = match.group(1)
        if checkin_id not in seen:
            seen.add(checkin_id)
            checkin_ids.append(checkin_id)
    return checkin_ids


def _complete_checkins_from_affirmation() -> tuple[str, list[str]]:
    if not bot.google_ok():
        return "", []
    try:
        all_checkins = {
            str(checkin.get("id", "")): checkin
            for checkin in bot.gs.get_checkins(include_inactive=True)
            if str(checkin.get("id", "")).strip()
        }
        target_ids: list[str] = []
        seen: set[str] = set()
        for checkin in bot.gs.awaiting_checkins():
            checkin_id = str(checkin.get("id", "")).strip()
            if checkin_id and checkin_id not in seen:
                seen.add(checkin_id)
                target_ids.append(checkin_id)
        for checkin_id in _active_checkin_notification_ids():
            checkin = all_checkins.get(checkin_id)
            if not checkin or not checkin.get("active", True) or checkin_id in seen:
                continue
            seen.add(checkin_id)
            target_ids.append(checkin_id)

        completed: list[str] = []
        archived_ids: list[str] = []
        for checkin_id in target_ids:
            if bot.gs.complete_checkin_today(checkin_id):
                checkin = all_checkins.get(checkin_id, {})
                completed.append(str(checkin.get("name", "") or f"check-in #{checkin_id}"))
                archived_ids.extend(_archive_notifications_by_source(f"checkin:{checkin_id}"))
        if not completed:
            return "", []
        reply = (
            f"Marked done for today: {', '.join(completed)}. "
            "I’ll leave you in peace until tomorrow."
        )
        return reply, archived_ids
    except Exception as exc:
        bot.logger.warning(f"PWA check-in affirmation error: {exc}")
        return "", []


def _explicit_checkin_completion_text(message: str) -> bool:
    return bot._explicit_checkin_completion_text(message)


def _recent_assistant_checkin_prompt(history: list[dict]) -> bool:
    return bot._recent_assistant_checkin_prompt(history[:-1] if history else [])


def _should_complete_checkin_from_affirmation(message: str, history: list[dict]) -> bool:
    return bot.should_complete_checkin_from_affirmation(message, history[:-1] if history else [])


def _parse_nudge_ids(raw: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[\s,]+", str(raw or "").strip()):
        nudge_id = part.strip().lstrip("#")
        if not re.fullmatch(r"(?:r-)?\d+", nudge_id, re.I):
            continue
        if nudge_id not in seen:
            seen.add(nudge_id)
            ids.append(nudge_id)
    return ids


def _nudge_id_from_source(source: str) -> str:
    match = re.fullmatch(r"nudge:((?:r-)?\d+)", str(source or "").strip(), re.I)
    return match.group(1) if match else ""


def _pwa_nudge_ids_from_context(text: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    patterns = (
        r"`?\[((?:r-)?\d{1,5})\]`?",
        r"\bnudge[:#\s]+((?:r-)?\d{1,5})\b",
        r"#((?:r-)?\d{1,5})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, str(text or ""), re.I):
            nudge_id = match.group(1)
            if nudge_id not in seen:
                seen.add(nudge_id)
                ids.append(nudge_id)
    return ids


def _pwa_cancel_nudge_ids(nudge_ids: list[str]) -> tuple[str, str]:
    cancelled: list[str] = []
    missing: list[str] = []
    errors: list[str] = []
    archived = 0
    for nudge_id in nudge_ids:
        try:
            ok = bot.gs.cancel_nudge(nudge_id)
            if ok:
                cancelled.append(nudge_id)
                archived += _archive_nudge_notifications(nudge_id)
            else:
                missing.append(nudge_id)
        except Exception as exc:
            errors.append(f"#{nudge_id}: {exc}")

    if len(nudge_ids) == 1:
        nudge_id = nudge_ids[0]
        if errors:
            return f"Could not cancel nudge #{nudge_id}: {errors[0].split(': ', 1)[-1]}", "cancel_nudge"
        if missing:
            return f"Nudge #{nudge_id} was not found, or it was already sent.", "cancel_nudge"
        extra = f" I also removed {archived} matching app notification{'s' if archived != 1 else ''} from H.I.R.A." if archived else ""
        return f"Nudge #{nudge_id} cancelled.{extra}", "cancel_nudge"

    lines: list[str] = []
    if cancelled:
        lines.append(f"Cancelled nudges: {', '.join(f'#{item}' for item in cancelled)}.")
    if archived:
        lines.append(f"Removed {archived} matching app notification{'s' if archived != 1 else ''}.")
    if missing:
        lines.append(f"Already sent or not found: {', '.join(f'#{item}' for item in missing)}.")
    if errors:
        lines.append(f"Could not cancel: {'; '.join(errors)}.")
    return "\n".join(lines) if lines else "No matching nudges found.", "cancel_nudge"


def _parse_checkin_ids(raw: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[\s,]+", str(raw or "").strip()):
        checkin_id = part.strip().lstrip("#")
        if not re.fullmatch(r"\d+", checkin_id):
            continue
        if checkin_id not in seen:
            seen.add(checkin_id)
            ids.append(checkin_id)
    return ids


def _parse_followup_ids(raw: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[\s,]+", str(raw or "").strip()):
        followup_id = part.strip().lstrip("#")
        if not re.fullmatch(r"\w[\w-]*", followup_id):
            continue
        if followup_id not in seen:
            seen.add(followup_id)
            ids.append(followup_id)
    return ids


def _archive_checkin_notifications(checkin_id: str) -> int:
    source = f"checkin:{str(checkin_id or '').strip()}"
    if source == "checkin:":
        return 0
    return len(_archive_notifications_by_source(source))


def _archive_followup_notifications(followup_id: str) -> int:
    source = f"followup:{str(followup_id or '').strip()}"
    if source == "followup:":
        return 0
    return len(_archive_notifications_by_source(source))


def _pwa_checkin_command_reply(message: str) -> tuple[str, str] | None:
    clean = str(message or "").strip()
    if not clean:
        return None
    lower = clean.lower()
    if (
        re.search(r"\b(?:remove|cancel|delete|stop|disable|turn off|clear)\b", lower)
        and bot._is_devotional_reminder_text(lower)
    ):
        result = bot.remove_devotional_reminders()
        checkins = len(result.get("checkins") or [])
        nudges = len(result.get("nudges") or [])
        notifications = len(result.get("notifications") or [])
        errors = result.get("errors") or []
        pieces = []
        if checkins:
            pieces.append(f"{checkins} daily check-in{'s' if checkins != 1 else ''}")
        if nudges:
            pieces.append(f"{nudges} pending nudge{'s' if nudges != 1 else ''}")
        if notifications:
            pieces.append(f"{notifications} queued app notification{'s' if notifications != 1 else ''}")
        if errors:
            return f"I tried to remove the istighfar/selawat reminders, but storage returned: {'; '.join(errors)}", "remove_devotional_reminders"
        if not pieces:
            return "No active istighfar/selawat reminders or queued notifications were found.", "remove_devotional_reminders"
        return f"Removed {'; '.join(pieces)} for istighfar/selawat.", "remove_devotional_reminders"
    if lower in {"/checkins", "checkins"}:
        try:
            checkins = bot.gs.get_checkins()
        except Exception as exc:
            return f"Check-ins unavailable: {exc}", "list_checkins"
        if not checkins:
            return "No active daily check-ins.", "list_checkins"
        lines = ["Active daily check-ins"]
        for checkin in checkins:
            lines.append(bot._format_checkin(checkin))
        lines.append("\nTap Done on a check-in notification, reply yes/done when it asks, or use `/cancelcheckin <id>`.")
        return "\n".join(lines), "list_checkins"

    match = re.match(r"^/(?:cancelcheckin|cancel_checkin)\s+(.+?)\s*$", clean, re.I)
    if not match:
        return None
    checkin_ids = _parse_checkin_ids(match.group(1))
    if not checkin_ids:
        return "Send `/cancelcheckin 7` or `/cancelcheckin 7, 8` to stop daily check-ins.", "cancel_checkin"

    cancelled: list[str] = []
    missing: list[str] = []
    errors: list[str] = []
    archived = 0
    for checkin_id in checkin_ids:
        try:
            ok = bot.gs.cancel_checkin(checkin_id)
            if ok:
                cancelled.append(checkin_id)
                archived += _archive_checkin_notifications(checkin_id)
            else:
                missing.append(checkin_id)
        except Exception as exc:
            errors.append(f"#{checkin_id}: {exc}")

    if len(checkin_ids) == 1:
        checkin_id = checkin_ids[0]
        if errors:
            return f"Could not cancel check-in #{checkin_id}: {errors[0].split(': ', 1)[-1]}", "cancel_checkin"
        if missing:
            return f"Check-in #{checkin_id} was not found.", "cancel_checkin"
        extra = f" I also removed {archived} matching app notification{'s' if archived != 1 else ''} from H.I.R.A." if archived else ""
        return f"Check-in #{checkin_id} cancelled.{extra}", "cancel_checkin"

    lines: list[str] = []
    if cancelled:
        lines.append(f"Cancelled check-ins: {', '.join(f'#{item}' for item in cancelled)}.")
    if archived:
        lines.append(f"Removed {archived} matching app notification{'s' if archived != 1 else ''}.")
    if missing:
        lines.append(f"Not found: {', '.join(f'#{item}' for item in missing)}.")
    if errors:
        lines.append(f"Could not cancel: {'; '.join(errors)}.")
    return "\n".join(lines) if lines else "No matching check-ins found.", "cancel_checkin"


def _pwa_nudge_command_reply(message: str) -> tuple[str, str] | None:
    clean = str(message or "").strip()
    if not clean:
        return None
    lower = clean.lower()
    wants_list = (
        lower in {"/nudges", "nudges"}
        or (
            re.search(r"\bnudges?\b", lower)
            and (
                re.search(r"\b(?:list|show|view|display|review)\b", lower)
                or re.search(r"\bgive\s+me\s+(?:a\s+)?list\b", lower)
                or re.search(r"\b(?:queued|pending|scheduled|open|active)\s+nudges?\b", lower)
                or re.search(r"\bnudges?\s+(?:still\s+)?(?:in|on)\s+(?:the\s+)?system\b", lower)
            )
            and not re.search(r"\b(?:add|create|schedule|set|cancel|clear|delete|remove|done|complete)\b", lower)
        )
    )
    if wants_list:
        try:
            nudges = sorted(bot.gs.get_nudges(), key=lambda n: str(n.get("send_at", "")))
        except Exception as exc:
            return f"Nudges unavailable: {exc}", "list_nudges"
        if not nudges:
            return "No pending nudges. Daily check-ins are separate; use `/checkins` to review recurring habit prompts.", "list_nudges"
        lines = ["Pending nudges"]
        for nudge in nudges:
            lines.append(bot._format_nudge(nudge))
        lines.append("\nTap Done on a nudge notification, or use `/cancelnudge <id>` / `/cancelnudge 78, 79, 80`.")
        return "\n".join(lines), "list_nudges"

    match = re.match(r"^/(?:cancelnudge|cancel_nudge)\s+(.+?)\s*$", clean, re.I)
    target_text = match.group(1) if match else ""
    if not match:
        wants_cancel = (
            re.search(r"\bnudges?\b", lower)
            and re.search(r"\b(?:cancel|clear|delete|remove|dismiss|kill|stop)\b", lower)
            and not re.search(r"\b(?:add|create|schedule|set)\b", lower)
        )
        if not wants_cancel:
            return None
        after_nudge = re.search(r"\bnudges?\b\s*(.+?)\s*$", clean, re.I)
        target_text = after_nudge.group(1) if after_nudge else clean
        if not _parse_nudge_ids(target_text):
            return "Tell me the exact nudge ID(s) to cancel, e.g. `/cancelnudge 78` or `clear nudges 78, 79`.", "cancel_nudge"

    nudge_ids = _parse_nudge_ids(target_text)
    if not nudge_ids:
        return "Send `/cancelnudge 78` or `/cancelnudge 78, 79, 80` to clear pending nudges.", "cancel_nudge"

    return _pwa_cancel_nudge_ids(nudge_ids)


def _pwa_nudge_removal_confirmation_reply(message: str, recent_context: str = "") -> tuple[str, str] | None:
    clean = " ".join(str(message or "").lower().split())
    if not clean:
        return None
    confirmation = bool(
        re.match(r"^(?:yes|yep|yeah|sure|ok(?:ay)?|please|pls|go ahead|do it|proceed)\b", clean)
        or re.search(r"\b(?:remove|delete|clear|dismiss|cancel|stop)\s+(?:those|them|these|the\s+nudges?)\b", clean)
    )
    if not confirmation:
        return None
    context = str(recent_context or "")
    context_lower = context.lower()
    latest_offer = bot._latest_contextual_offer(context)
    offer_text = latest_offer or context_lower
    nudge_removal_offer = bool(
        re.search(r"\b(?:cancel|clear|delete|remove|dismiss|stop)\b", offer_text)
        and re.search(r"\bnudges?\b", offer_text)
    )
    if not nudge_removal_offer:
        return None
    nudge_ids = _pwa_nudge_ids_from_context(context)
    if not nudge_ids:
        return "I’m ready to cancel them, but I no longer have the nudge IDs in this chat turn. Send the nudge IDs and I’ll clear them.", "cancel_nudge"
    return _pwa_cancel_nudge_ids(nudge_ids[:10])


def _pwa_followup_command_reply(message: str) -> tuple[str, str] | None:
    clean = str(message or "").strip()
    if not clean:
        return None
    lower = clean.lower()
    if lower in {"/followups", "followups"}:
        try:
            followups = sorted(bot.gs.get_followups(), key=lambda item: str(item.get("due_date", "")))
        except Exception as exc:
            return f"Follow-ups unavailable: {exc}", "list_followups"
        if not followups:
            return "No open follow-ups.", "list_followups"
        lines = ["Open follow-ups"]
        for followup in followups:
            lines.append(bot._format_followup(followup))
        lines.append("\nTap Done on a follow-up notification, or use `/donefollowup <id>`.")
        return "\n".join(lines), "list_followups"

    match = re.match(r"^/(?:donefollowup|done_followup|completefollowup|complete_followup)\s+(.+?)\s*$", clean, re.I)
    if not match:
        return None
    followup_ids = _parse_followup_ids(match.group(1))
    if not followup_ids:
        return "Send `/donefollowup 7` or `/donefollowup 7, 8` to clear open follow-ups.", "complete_followup"

    completed: list[str] = []
    missing: list[str] = []
    errors: list[str] = []
    archived = 0
    for followup_id in followup_ids:
        try:
            ok = bot.gs.complete_followup(followup_id)
            if ok:
                completed.append(followup_id)
                archived += _archive_followup_notifications(followup_id)
            else:
                missing.append(followup_id)
        except Exception as exc:
            errors.append(f"#{followup_id}: {exc}")

    if len(followup_ids) == 1:
        followup_id = followup_ids[0]
        if errors:
            return f"Could not complete follow-up #{followup_id}: {errors[0].split(': ', 1)[-1]}", "complete_followup"
        if missing:
            return f"Follow-up #{followup_id} was not found, or it was already done.", "complete_followup"
        extra = f" I also removed {archived} matching app notification{'s' if archived != 1 else ''} from H.I.R.A." if archived else ""
        return f"Follow-up #{followup_id} marked done.{extra}", "complete_followup"

    lines: list[str] = []
    if completed:
        lines.append(f"Marked follow-ups done: {', '.join(f'#{item}' for item in completed)}.")
    if archived:
        lines.append(f"Removed {archived} matching app notification{'s' if archived != 1 else ''}.")
    if missing:
        lines.append(f"Already done or not found: {', '.join(f'#{item}' for item in missing)}.")
    if errors:
        lines.append(f"Could not complete: {'; '.join(errors)}.")
    return "\n".join(lines) if lines else "No matching follow-ups found.", "complete_followup"

def _pwa_task_ids_from_context(text: str) -> list[str]:
    raw = str(text or "")
    bracket_ids = [
        match.group(1)
        for match in re.finditer(r"`?\[(\d{1,5})\]`?", raw, re.I)
    ]
    if bracket_ids:
        return bracket_ids

    ids: list[str] = []
    seen: set[str] = set()
    patterns = (
        r"#(\d{1,5})\b",
        r"\b(?:task|reminder)\s*#?\s*(\d{1,5})\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, raw, re.I):
            task_id = match.group(1)
            if task_id not in seen:
                seen.add(task_id)
                ids.append(task_id)
    return ids

def _pwa_task_ids_from_id_list_reply(text: str) -> list[str]:
    clean = " ".join(str(text or "").lower().split())
    if not re.fullmatch(r"#?\d{1,5}(?:\s*(?:,|and|&)\s*#?\d{1,5})*", clean):
        return []
    return re.findall(r"#?(\d{1,5})", clean)

def _pwa_task_ids_from_direct_request(text: str) -> list[str]:
    raw = str(text or "")
    clean = " ".join(raw.lower().split())
    if not clean:
        return []
    has_action = bool(re.search(r"\b(?:delete|remove|clear|dismiss|archive|complete|finish|mark|done)\b", clean))
    has_task_word = bool(re.search(r"\b(?:tasks?|reminders?|items?)\b", clean))
    if not has_action or not has_task_word:
        return []

    explicit_refs = [
        match.group(1) or match.group(2)
        for match in re.finditer(r"`?\[(\d{1,5})\]`?|#(\d{1,5})\b", raw, re.I)
    ]
    if explicit_refs:
        return explicit_refs

    ids: list[str] = []
    for match in re.finditer(
        r"\b(?:tasks?|reminders?|items?)\s+(#?\d{1,5}(?:\s*(?:,|and|&)\s*#?\d{1,5})*)",
        clean,
        re.I,
    ):
        ids.extend(re.findall(r"#?(\d{1,5})", match.group(1)))
    return ids

def _pwa_task_removal_confirmation_reply(message: str, recent_context: str = "") -> tuple[str, str] | None:
    clean = " ".join(str(message or "").lower().split())
    if not clean:
        return None
    direct_task_ids = _pwa_task_ids_from_direct_request(message)
    id_list_task_ids = _pwa_task_ids_from_id_list_reply(message)
    confirmation = bool(
        re.match(r"^(?:yes|yep|yeah|sure|ok(?:ay)?|please|pls|go ahead|do it|proceed)\b", clean)
        or re.search(r"\b(?:remove|delete|clear|dismiss|archive)\s+(?:those|them|these|the\s+tasks?)\b", clean)
        or re.match(r"^confirm\b", clean)
        or re.search(r"\bmark\s+(?:those|them|these|the\s+tasks?)(?:\s+\d+)?\s+(?:as\s+)?done\b", clean)
        or bool(direct_task_ids)
        or bool(id_list_task_ids)
    )
    if not confirmation:
        return None
    context = str(recent_context or "")
    context_lower = context.lower()
    latest_offer = bot._latest_contextual_offer(context)
    action_pattern = r"\b(?:remove|delete|clear|dismiss|archive|complete|mark(?:\s+as)?\s+done)\b"
    task_pattern = r"\b(?:tasks?|reminders?|items?)\b"
    task_removal_offer = any(
        source and re.search(action_pattern, source) and re.search(task_pattern, source)
        for source in (latest_offer, context_lower)
    )
    if not task_removal_offer and not direct_task_ids:
        return None
    task_ids = direct_task_ids or id_list_task_ids or _pwa_task_ids_from_context(context)
    if not task_ids:
        return "I’m ready to remove them, but I no longer have the task IDs in this chat turn. Open Tasks or send the IDs and I’ll clear them.", "complete_task_by_text"

    completed: list[str] = []
    missing: list[str] = []
    errors: list[str] = []
    for task_id in task_ids[:10]:
        try:
            ok, _synced_marking = bot.complete_reminder_by_id(task_id)
            if ok:
                completed.append(task_id)
            else:
                missing.append(task_id)
        except Exception as exc:
            errors.append(f"#{task_id}: {exc}")

    lines: list[str] = []
    if completed:
        lines.append(f"Removed {len(completed)} task{'s' if len(completed) != 1 else ''} from the active list: {', '.join(f'#{item}' for item in completed)}.")
    if missing:
        lines.append(f"Already done or not found: {', '.join(f'#{item}' for item in missing)}.")
    if errors:
        lines.append(f"Could not remove: {'; '.join(errors)}.")
    return "\n".join(lines) if lines else "No matching task IDs were found to remove.", "complete_task_by_text"


def _is_pwa_triage_prompt(message: str) -> bool:
    clean = " ".join(str(message or "").lower().split())
    if not clean or "triage" not in clean:
        return False
    return bool(re.search(r"\b(?:current load|load|top 3|handle next|order of attack|prioritise|prioritize|priority|tasks?)\b", clean))


def _pwa_due_reason(task: dict, today: date) -> str:
    due = str(task.get("due", "") or "").strip()
    priority = str(task.get("priority", "") or "").strip()
    effort = str(task.get("effort", "") or "").strip()
    pieces: list[str] = []
    if due:
        try:
            due_date = date.fromisoformat(due)
            delta = (due_date - today).days
            if delta < 0:
                pieces.append(f"overdue by {abs(delta)} day{'s' if abs(delta) != 1 else ''}")
            elif delta == 0:
                pieces.append("due today")
            elif delta == 1:
                pieces.append("due tomorrow")
            else:
                pieces.append(f"due in {delta} days")
        except Exception:
            pieces.append(f"due {due}")
    if priority:
        pieces.append(f"{priority} priority")
    if effort:
        pieces.append(f"{effort} effort")
    return ", ".join(pieces) or "it is already near the top of your task queue"


def _pwa_task_candidate(task: dict, today: date) -> dict:
    description = str(task.get("description", "") or "Untitled task").strip()
    next_action = str(task.get("next_action", "") or "").strip()
    if not next_action:
        next_action = "Do the smallest visible step, then mark or update the task so the queue stays honest."
    return {
        "title": description,
        "why": _pwa_due_reason(task, today),
        "next": next_action,
    }


def _pwa_marking_candidate(task: dict) -> dict:
    title = str(task.get("title", "") or task.get("name", "") or "Outstanding marking").strip()
    total = int(task.get("total_scripts") or 0)
    marked = int(task.get("marked_count") or 0)
    outstanding = max(0, total - marked) if total else 0
    collected = str(task.get("collected_date", "") or "").strip()
    if total:
        why = f"{outstanding} of {total} scripts still outstanding"
        next_action = "Mark the first 5 scripts, then update the marking count."
    else:
        stack_count = int(task.get("stack_count") or 1)
        why = f"{marked} scripts marked so far across {stack_count} stack{'s' if stack_count != 1 else ''}; total not set"
        next_action = "Set the total scripts or clear one small stack first."
    if collected:
        why = f"{why}; collected {collected}"
    return {
        "title": title,
        "why": why,
        "next": next_action,
    }


def _pwa_followup_candidates_from_context(context_text: str, limit: int = 3) -> list[dict]:
    candidates: list[dict] = []
    in_followups = False
    for raw in str(context_text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        lower = line.lower()
        if lower.startswith("follow-ups"):
            in_followups = True
            continue
        if in_followups and re.match(r"^[a-z][a-z\s-]+:", lower):
            break
        if not in_followups or not line.startswith("-"):
            continue
        clean = re.sub(r"^\-\s*", "", line).strip()
        if clean.lower() == "none.":
            continue
        candidates.append({
            "title": clean,
            "why": "open follow-up in the current context snapshot",
            "next": "Send the shortest useful reply or park it with a concrete next action.",
        })
        if len(candidates) >= limit:
            break
    return candidates


def _pwa_context_fallback_candidate(context_text: str) -> dict:
    interesting = []
    for raw in str(context_text or "").splitlines():
        line = raw.strip()
        if not line or line.lower().startswith(("assistant context", "stored memory")):
            continue
        if line.startswith("-") or re.search(r"\b(?:today|calendar|lesson|reminders|marking|follow-ups)\b", line, re.I):
            interesting.append(line.replace("*", ""))
        if len(interesting) >= 3:
            break
    detail = "; ".join(interesting) if interesting else "no urgent task or marking item surfaced"
    return {
        "title": "Protect the next clean block",
        "why": detail,
        "next": "Do one focused pass before opening new work or messages.",
    }


def _format_pwa_triage_reply(context_text: str, tasks_data: dict, marking_tasks: list[dict], errors: list[str]) -> str:
    today = datetime.now(bot.SGT).date()
    candidates: list[dict] = []
    for task in (tasks_data.get("items") if isinstance(tasks_data, dict) else []) or []:
        if len(candidates) >= 3:
            break
        candidates.append(_pwa_task_candidate(task, today))
    for task in marking_tasks or []:
        if len(candidates) >= 3:
            break
        candidates.append(_pwa_marking_candidate(task))
    for item in _pwa_followup_candidates_from_context(context_text, limit=3):
        if len(candidates) >= 3:
            break
        candidates.append(item)
    if not candidates:
        candidates.append(_pwa_context_fallback_candidate(context_text))

    lines = ["Direct triage from your current H.I.R.A data:", ""]
    for index, item in enumerate(candidates[:3], start=1):
        lines.append(f"{index}. {item['title']}")
        lines.append(f"   Why: {item['why']}.")
        lines.append(f"   Next: {item['next']}")
    order = " -> ".join(item["title"] for item in candidates[:3])
    lines.append("")
    lines.append(f"Order of attack: {order}.")
    if errors:
        lines.append("")
        lines.append("Source notes: " + "; ".join(errors[:3]))
    return "\n".join(lines).strip()


async def _pwa_triage_reply(message: str) -> str:
    if not _is_pwa_triage_prompt(message):
        return ""
    errors: list[str] = []

    async def run(label: str, fn):
        try:
            return await asyncio.to_thread(fn)
        except Exception as exc:
            errors.append(f"{label} unavailable: {exc}")
            return None

    context_text, tasks_data, marking_tasks = await asyncio.gather(
        run("context", lambda: bot.build_context_snapshot(3)),
        run("tasks", lambda: bot.build_task_structured(7)),
        run("marking", lambda: bot.gs.get_marking_tasks() if bot.google_ok() else []),
    )
    return _format_pwa_triage_reply(
        str(context_text or ""),
        tasks_data if isinstance(tasks_data, dict) else {},
        marking_tasks if isinstance(marking_tasks, list) else [],
        errors,
    )


@app.post("/api/chat")
async def chat(
    request: Request,
    req: ChatRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    if not await _CHAT_RATE_LIMITER.is_allowed(_request_ip(request)):
        raise HTTPException(status_code=429, detail="Too many requests. Slow down a little.")
    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")
    if len(message) > 12_000:
        raise HTTPException(status_code=413, detail="Message is too long. Keep it under 12,000 characters.")

    await _CHAT_SEMAPHORE.acquire()
    try:
        return await _chat_stream_response(message, req.location, x_hira_client)
    except Exception:
        _CHAT_SEMAPHORE.release()
        raise


async def _chat_stream_response(message: str, location: DeviceLocation | None, x_hira_client: str | None):
    history_key = _history_key(x_hira_client)
    history = _get_history_best_effort(history_key)
    retry_target = _retry_target_message(message, history)
    if retry_target:
        message = retry_target

    live_briefing_slot = _live_briefing_slot(message)
    if live_briefing_slot:
        reply = await asyncio.to_thread(_live_briefing_text, live_briefing_slot)
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            reply,
            history_key,
            quick_history,
            route_name="live_briefing",
            tool_name=f"{live_briefing_slot}_briefing",
        )

    briefing_slot = _briefing_replay_slot(message)
    if briefing_slot:
        reply = await asyncio.to_thread(_briefing_replay_text, briefing_slot)
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            reply,
            history_key,
            quick_history,
            route_name="briefing_replay",
            tool_name=f"{briefing_slot}_briefing",
        )

    triage_reply = await _pwa_triage_reply(message)
    if triage_reply:
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            triage_reply,
            history_key,
            quick_history,
            route_name="triage",
            tool_name="get_assistant_context",
        )

    assistant_feeling_reply = _pwa_assistant_feeling_reply(message)
    if assistant_feeling_reply:
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            assistant_feeling_reply,
            history_key,
            quick_history,
            route_name="casual_checkin",
        )

    model_config_advice = _pwa_model_config_advice_reply(message)
    if model_config_advice:
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            model_config_advice,
            history_key,
            quick_history,
            route_name="model_config_advice",
        )

    direct_greeting, greeting_route = _pwa_direct_greeting_reply(message)
    if direct_greeting:
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            direct_greeting,
            history_key,
            quick_history,
            route_name=greeting_route,
        )

    f1_calendar_reply = _pwa_direct_f1_calendar_reply(message, history)
    if f1_calendar_reply:
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            f1_calendar_reply,
            history_key,
            quick_history,
            route_name="direct_source",
            tool_name="get_f1_brief",
        )

    direct_source_reply, direct_source_tool = await _pwa_direct_source_reply(message, history)
    if direct_source_reply:
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            direct_source_reply,
            history_key,
            quick_history,
            route_name="direct_source",
            tool_name=direct_source_tool,
        )

    if _pwa_casual_status_prompt(message):
        try:
            status_reply = await bot._execute_tool_offloop("get_assistant_context", {"days": 3})
        except Exception as exc:
            bot.logger.warning(f"PWA casual status brief failed: {exc}")
            status_reply = await asyncio.to_thread(
                _safe_text,
                lambda: bot.build_agenda(1),
                "I could not pull the full status brief right now, but I am still here.",
            )
        if not status_reply:
            status_reply = "I could not pull the current status brief right now, but I am still here."
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            status_reply,
            history_key,
            quick_history,
            route_name="natural_intent",
            tool_name="get_assistant_context",
        )

    agenda_days = _pwa_direct_agenda_days(message)
    if agenda_days:
        agenda_reply = await asyncio.to_thread(
            _safe_text,
            lambda: bot.build_agenda(agenda_days),
            "Agenda unavailable right now.",
        )
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            agenda_reply,
            history_key,
            quick_history,
            route_name="local_agenda",
            tool_name="get_assistant_context",
        )

    task_days = _pwa_direct_task_days(message)
    if task_days:
        task_reply = await asyncio.to_thread(
            _safe_text,
            lambda: bot.build_task_brief(task_days),
            "Task brief unavailable right now.",
        )
        quick_history = [*history[-bot.MAX_TURNS:], {"role": "user", "content": message}]
        return _quick_sse_response(
            task_reply,
            history_key,
            quick_history,
            route_name="local_tasks",
            tool_name="get_task_brief",
        )

    working_memory = _update_working_memory(history_key, history, message)
    working_summary = _working_memory_summary(working_memory)
    _schedule_background_call("taste hint capture", bot.absorb_taste_hint, message)
    recent_context_for_followup = "\n".join(
        str(item.get("content", ""))[:600]
        for item in history[-6:]
        if isinstance(item, dict) and isinstance(item.get("content"), str)
    )
    source_hint_message = bot._contextual_followup_effective_text(message, recent_context_for_followup)
    initial_thread_state = bot.thread_state_for_turn(message, recent_context_for_followup)
    style_profile = bot.interaction_style_profile()
    operator_state = bot.personal_operator_state_for_turn(
        message,
        recent_context=recent_context_for_followup,
        working_memory=working_memory,
    )
    response_plan = bot.conversation_response_plan(
        message,
        recent_context=recent_context_for_followup,
        frame=initial_thread_state.get("pragmatic_frame", {}),
        style_profile=style_profile,
        relevant_arcs=operator_state.get("relevant_arcs", []),
        operator_state=operator_state,
    )
    user_content = message
    if bot.re.search(r"\b(?:personal|personal\s*2|second(?:ary)?|other\s+personal|work|moe|school)\s+(?:gmail|email|emails|mail|inbox)\b", message, bot.re.I):
        account_hint, _ = bot._extract_gmail_account_from_text(message)
        user_content = f"{message}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
    user_content = (
        f"{user_content}"
        f"{bot.pragmatic_frame_context(initial_thread_state.get('pragmatic_frame', {}))}"
        f"{bot.response_plan_context(response_plan)}"
        f"{bot.personal_operator_context(operator_state)}"
        f"{bot.interaction_style_context(style_profile)}"
        f"{_working_memory_context(working_memory)}"
        f"{_recent_turn_grounding_context(history, message)}"
        f"{_thread_state_context(initial_thread_state)}"
        f"{bot.intent_lens_hint(message)}"
        f"{bot.source_discipline_hint(source_hint_message)}"
    )
    location_context = _device_location_context(location)
    if location_context:
        user_content = f"{user_content}{location_context}"
    history.append({"role": "user", "content": user_content})
    history = history[-bot.MAX_TURNS:]

    recent_context = "\n".join(
        str(item.get("content", ""))[:600]
        for item in history[-8:-1]
        if isinstance(item, dict) and isinstance(item.get("content"), str)
    )
    absence_reply = bot.absence_memory_response(message, recent_context=recent_context)
    if absence_reply:
        return _quick_sse_response(absence_reply, history_key, history, route_name="memory_recall")

    source_pref_reply = bot.source_citation_preference_response(message)
    if source_pref_reply:
        return _quick_sse_response(source_pref_reply, history_key, history, route_name="memory_preference")

    f1_sync_reply = bot.f1_calendar_sync_response(message)
    if f1_sync_reply:
        return _quick_sse_response(f1_sync_reply, history_key, history, route_name="f1_calendar_sync", tool_name="sync_f1_calendar")

    provider_status_reply = bot._llm_provider_status_reply([{"role": "user", "content": message}])
    if provider_status_reply:
        return _quick_sse_response(provider_status_reply, history_key, history, route_name="provider_status")

    carryover_greeting_reply = bot.conversation_carryover_greeting_reply(message)
    if carryover_greeting_reply:
        return _quick_sse_response(carryover_greeting_reply, history_key, history, route_name="carryover_checkin")

    checkin_command = _pwa_checkin_command_reply(message)
    if checkin_command:
        reply, tool_name = checkin_command
        return _quick_sse_response(reply, history_key, history, route_name="checkin_admin", tool_name=tool_name)

    nudge_command = _pwa_nudge_command_reply(message)
    if nudge_command:
        reply, tool_name = nudge_command
        return _quick_sse_response(reply, history_key, history, route_name="nudge_admin", tool_name=tool_name)

    nudge_removal_confirmation = _pwa_nudge_removal_confirmation_reply(message, recent_context_for_followup)
    if nudge_removal_confirmation:
        reply, tool_name = nudge_removal_confirmation
        return _quick_sse_response(reply, history_key, history, route_name="nudge_admin", tool_name=tool_name)

    followup_command = _pwa_followup_command_reply(message)
    if followup_command:
        reply, tool_name = followup_command
        return _quick_sse_response(reply, history_key, history, route_name="followup_admin", tool_name=tool_name)

    task_removal_confirmation = _pwa_task_removal_confirmation_reply(message, recent_context_for_followup)
    if task_removal_confirmation:
        reply, tool_name = task_removal_confirmation
        return _quick_sse_response(reply, history_key, history, route_name="task_admin", tool_name=tool_name)

    topic_news_reply = await _pwa_topic_news_reply(message, recent_context_for_followup)
    if topic_news_reply:
        return _quick_sse_response(
            topic_news_reply,
            history_key,
            history,
            route_name="topic_news",
            tool_name="get_latest_news",
        )

    quick_checkin_reply = ""
    archived_checkin_notification_ids: list[str] = []
    if _should_complete_checkin_from_affirmation(message, history):
        quick_checkin_reply, archived_checkin_notification_ids = _complete_checkins_from_affirmation()

    if quick_checkin_reply:
        history.append({"role": "assistant", "content": quick_checkin_reply})
        _save_history_best_effort(history_key, history)

        async def quick_checkin_events():
            def sse(payload: dict) -> str:
                return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            try:
                yield sse({"type": "route", "name": "quick"})
                if archived_checkin_notification_ids:
                    yield sse({"type": "notifications_archived", "ids": archived_checkin_notification_ids})
                yield sse({"type": "text", "text": quick_checkin_reply})
                yield sse({"type": "done", "text": quick_checkin_reply})
                yield sse({"type": "saved"})
            finally:
                _CHAT_SEMAPHORE.release()
                bot._log_memory("after pwa quick checkin")

        return StreamingResponse(quick_checkin_events(), media_type="text/event-stream")

    delayed_digest_reply = ""
    try:
        scheduled_digest = bot.schedule_delayed_digest_push(message)
        if scheduled_digest:
            send_at = scheduled_digest["send_at"]
            delayed_digest_reply = f"Scheduled. I’ll push the digest at {send_at.strftime('%H:%M')} SGT."
    except Exception as exc:
        bot.logger.warning(f"Delayed digest push scheduling failed: {exc}")

    if delayed_digest_reply:
        history.append({"role": "assistant", "content": delayed_digest_reply})
        _save_history_best_effort(history_key, history)

        async def delayed_digest_events():
            def sse(payload: dict) -> str:
                return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            try:
                yield sse({"type": "route", "name": "quick"})
                yield sse({"type": "tool", "name": "create_proactive_nudge"})
                yield sse({"type": "text", "text": delayed_digest_reply})
                yield sse({"type": "done", "text": delayed_digest_reply})
                yield sse({"type": "saved"})
            finally:
                _CHAT_SEMAPHORE.release()
                bot._log_memory("after pwa delayed digest schedule")

        return StreamingResponse(delayed_digest_events(), media_type="text/event-stream")

    async def events():
        reply_parts: list[str] = []
        final_text = ""
        started = time.perf_counter()
        phase_started = started
        presence_preface = ""
        trace = _new_chat_trace(message, include_memory=False)
        _merge_chat_trace(trace, {
            "thread_state": initial_thread_state,
            "model_policy": bot.model_policy_for_messages(list(history)),
        })

        def sse(payload: dict) -> str:
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        def timing(phase: str) -> dict:
            nonlocal phase_started
            now = time.perf_counter()
            payload = {
                "type": "timing",
                "phase": phase,
                "elapsed_ms": round((now - started) * 1000),
                "phase_ms": round((now - phase_started) * 1000),
            }
            phase_started = now
            bot.logger.info(
                "PWA chat timing phase=%s elapsed_ms=%s phase_ms=%s",
                payload["phase"],
                payload["elapsed_ms"],
                payload["phase_ms"],
            )
            _merge_chat_trace(trace, {"timings": {phase: {
                "elapsed_ms": payload["elapsed_ms"],
                "phase_ms": payload["phase_ms"],
            }}})
            return payload

        try:
            if working_summary:
                yield sse({"type": "understood", **working_summary})
            quick = await bot.should_route_quick_pwa_chat(list(history[:-1]), message)
            yield sse(timing("route"))
            route_name = "quick" if quick else "agentic"
            _merge_chat_trace(trace, {"route": route_name})
            yield sse({"type": "route", "name": route_name})
            recent_context = "\n".join(
                str(item.get("content", ""))[:600]
                for item in history[-6:-1]
                if isinstance(item.get("content"), str)
            )
            if bot._is_contextual_followup_reply(message):
                contextual_offer = bot._latest_contextual_offer(recent_context)
                if contextual_offer:
                    contextual_discipline = bot.source_discipline_for_text(f"{contextual_offer}\n{message}")
                    _merge_chat_trace(trace, {
                        "confidence_gate": "pending" if contextual_discipline.get("needs_live_check") else trace.get("confidence_gate"),
                        "source_discipline": {
                            "needs_live_check": bool(contextual_discipline.get("needs_live_check")),
                            "recommended_tools": list(contextual_discipline.get("recommended_tools") or []),
                            "confidence": contextual_discipline.get("confidence", ""),
                        },
                    })
            tools = [] if quick else bot.pwa_tools_for_message(message, recent_context=recent_context)
            thread_state = bot.thread_state_for_turn(
                message,
                recent_context,
                {tool["name"] for tool in tools},
            )
            model_policy = bot.model_policy_for_messages(list(history))
            _merge_chat_trace(trace, {
                "thread_state": thread_state,
                "model_policy": model_policy,
            })
            if thread_state.get("needs_live_check"):
                _merge_chat_trace(trace, {
                    "confidence_gate": "pending",
                    "source_discipline": {
                        "needs_live_check": True,
                        "recommended_tools": list(thread_state.get("recommended_tools") or []),
                        "confidence": "needs_live_source",
                    },
                })
            if not quick:
                forced_tool = bot._forced_tool_for_current_turn(list(history), tools) or ""
                _merge_chat_trace(trace, {
                    "tools_available": [tool["name"] for tool in tools],
                    "forced_tool": forced_tool,
                })
                yield sse({"type": "tools", "count": len(tools), "names": [tool["name"] for tool in tools]})
            yield sse({"type": "trace", "trace": trace})
            if not quick:
                presence_tool = _presence_tool_name(message, forced_tool, tools, thread_state)
                presence_preface = _streaming_presence_preface(message, presence_tool)
                if presence_preface:
                    _merge_chat_trace(trace, {
                        "presence_preface": True,
                        "presence_tool": presence_tool,
                    })
                    yield sse({"type": "trace", "trace": trace})
            stream = (
                bot.stream_quick_pwa_reply(list(history[:-1]), message)
                if quick
                else bot.stream_agentic_chat(
                    list(history),
                    max_tokens=_CHAT_MAX_TOKENS,
                    tools=tools,
                    openai_state_key=history_key,
                    direct_user_text=message,
                )
            )
            first_text = True
            async for event in stream:
                if first_text and event.get("type") == "text":
                    first_text = False
                    yield sse(timing("first_token"))
                if event.get("type") == "text":
                    reply_parts.append(event.get("text", ""))
                elif event.get("type") == "replace":
                    reply_parts = [event.get("text", "")]
                elif event.get("type") == "done":
                    final_text = event.get("text", "")
                elif event.get("type") == "tool":
                    _merge_chat_trace(trace, {"tools_called": [event.get("name", "")]})
                    yield sse({"type": "trace", "trace": trace})
                elif event.get("type") == "trace":
                    _merge_chat_trace(trace, event.get("patch") or {})
                    yield sse({"type": "trace", "trace": trace})
                    continue
                yield sse(event)

            reply_text = final_text or "".join(reply_parts).strip()
            cleaned_reply_text = bot.strip_source_bibliography_noise(
                bot.strip_ai_citation_markers(reply_text),
                allow_source_details=bot.wants_source_details(message),
            )
            if cleaned_reply_text and cleaned_reply_text != reply_text:
                reply_text = cleaned_reply_text
                yield sse({"type": "replace", "text": reply_text})
                yield sse({"type": "done", "text": reply_text})
            recovered_action_text = ""
            if not trace.get("tools_called"):
                try:
                    recovered_action_text = await _recover_leaked_action_payload(reply_text)
                except Exception as recovery_exc:
                    bot.logger.warning(f"PWA leaked action recovery failed: {recovery_exc}")
            if recovered_action_text:
                reply_text = recovered_action_text
                _merge_chat_trace(trace, {
                    "tools_called": ["add_reminder"],
                    "final_mode": "leaked_action_recovered",
                    "response_contract": {"status": "recovered_leaked_action_payload"},
                })
                yield sse({"type": "replace", "text": reply_text})
                yield sse({"type": "done", "text": reply_text})
            repaired_reply_text, repair_meta = await bot.maybe_self_repair_reply(
                message,
                reply_text,
                recent_context=recent_context_for_followup,
                frame=initial_thread_state.get("pragmatic_frame", {}),
                response_plan=response_plan,
                style_profile=style_profile,
                relevant_arcs=operator_state.get("relevant_arcs", []),
                trace=trace,
            )
            if repair_meta.get("repaired") and repaired_reply_text != reply_text:
                reply_text = repaired_reply_text
                _merge_chat_trace(trace, {
                    "reply_repair": repair_meta,
                    "final_mode": "self_repaired",
                })
                yield sse({"type": "trace", "trace": trace})
                yield sse({"type": "replace", "text": reply_text})
                yield sse({"type": "done", "text": reply_text})
            elif repair_meta.get("verdict", {}).get("flags"):
                _merge_chat_trace(trace, {"reply_repair": repair_meta})
            response_contract = response_contract_for_reply(reply_text, trace)
            _merge_chat_trace(trace, {"response_contract": response_contract})
            if _empty_chat_reply(reply_text):
                fallback_text = ""
                fallback_result = ""
                fallback_tool = ""
                try:
                    fallback_text, fallback_tool, fallback_result = await _source_check_backend_fallback_payload(source_hint_message)
                except Exception as fallback_exc:
                    bot.logger.warning(f"PWA empty-answer source fallback failed: {fallback_exc}")
                if fallback_text:
                    contracts = bot._source_contracts_from_text(fallback_result)
                    _merge_chat_trace(trace, {
                        "tools_called": [fallback_tool],
                        "source_contracts_seen": contracts,
                        "confidence_gate": "fallback",
                        "final_mode": "empty_answer_fallback",
                    })
                    reply_text = fallback_text
                else:
                    _merge_chat_trace(trace, {
                        "confidence_gate": "failed" if trace.get("source_discipline", {}).get("needs_live_check") else trace.get("confidence_gate"),
                        "final_mode": "empty_answer",
                    })
                    reply_text = (
                        "I lost the actual answer before it reached the chat, so I’m not going to pretend that was done. "
                        "Try again in a moment."
                    )
                yield sse({"type": "replace", "text": reply_text})
                yield sse({"type": "done", "text": reply_text})
            elif (
                response_contract.get("unsupported_no_access_claim")
                or response_contract.get("weak_answer")
                or response_contract.get("missing_source_contract")
            ):
                fallback_text = ""
                fallback_result = ""
                fallback_tool = ""
                try:
                    fallback_text, fallback_tool, fallback_result = await _source_check_backend_fallback_payload(source_hint_message)
                except Exception as fallback_exc:
                    bot.logger.warning(f"PWA no-access source fallback failed: {fallback_exc}")
                if fallback_text:
                    contracts = bot._source_contracts_from_text(fallback_result)
                    _merge_chat_trace(trace, {
                        "tools_called": [fallback_tool],
                        "source_contracts_seen": contracts,
                        "confidence_gate": "fallback",
                        "final_mode": "no_access_claim_fallback",
                        "response_contract": {"status": "fallback_replaced_no_access_claim"},
                    })
                    reply_text = fallback_text
                    yield sse({"type": "replace", "text": reply_text})
                    yield sse({"type": "done", "text": reply_text})
                elif response_contract.get("weak_answer"):
                    _merge_chat_trace(trace, {
                        "confidence_gate": "failed",
                        "final_mode": "weak_answer_blocked",
                    })
                    reply_text = "That was too thin for what you asked. I’m treating it as a failed pass, not a real answer."
                    yield sse({"type": "replace", "text": reply_text})
                    yield sse({"type": "done", "text": reply_text})
            history.append({"role": "assistant", "content": reply_text})
            _save_history_best_effort(history_key, history)
            _schedule_background_call(
                "chat learning event",
                bot.record_chat_learning_event,
                message,
                reply_text,
                source="pwa",
                subject_hint=str(working_memory.get("current_subject", "") or ""),
            )
            yield sse(timing("saved"))
            _finalise_chat_trace(trace, "answered")
            yield sse({"type": "trace", "trace": trace})
            yield sse({"type": "saved"})
        except Exception as exc:
            bot.logger.exception(f"PWA chat failed: {exc}")
            error_detail = _safe_chat_error_detail(exc)
            _merge_chat_trace(trace, {
                "error_phase": "stream_or_model",
                "error_detail": error_detail,
            })
            fallback_text = ""
            fallback_result = ""
            fallback_tool = ""
            try:
                fallback_text, fallback_tool, fallback_result = await _source_check_backend_fallback_payload(source_hint_message)
            except Exception as fallback_exc:
                bot.logger.warning(f"PWA source fallback failed: {fallback_exc}")
            if fallback_text:
                contracts = bot._source_contracts_from_text(fallback_result)
                _merge_chat_trace(trace, {
                    "tools_called": [fallback_tool],
                    "source_contracts_seen": contracts,
                    "confidence_gate": "fallback",
                    "final_mode": "fallback_summary",
                })
                history.append({"role": "assistant", "content": fallback_text})
                _save_history_best_effort(history_key, history)
                yield sse({"type": "text", "text": fallback_text})
                yield sse({"type": "done", "text": fallback_text})
                yield sse({"type": "trace", "trace": trace})
                yield sse({"type": "saved"})
            else:
                reply_text = _pwa_model_failure_reply(message, error_detail)
                history.append({"role": "assistant", "content": reply_text})
                _save_history_best_effort(history_key, history)
                _merge_chat_trace(trace, {"confidence_gate": "failed", "final_mode": "model_failure"})
                yield sse({"type": "trace", "trace": trace})
                yield sse({"type": "replace", "text": reply_text})
                yield sse({"type": "done", "text": reply_text})
                yield sse({"type": "saved"})
        finally:
            _CHAT_SEMAPHORE.release()
            bot._log_memory("after pwa chat")

    return StreamingResponse(events(), media_type="text/event-stream")


async def _source_check_backend_fallback(message: str) -> str:
    text, _tool_name, _result = await _source_check_backend_fallback_payload(message)
    return text


def _normalise_action_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key or "").lower())


def _parse_leaked_json_payload(text: str):
    clean = str(text or "").strip().replace("“", "\"").replace("”", "\"").replace("‘", "'").replace("’", "'")
    if not clean or len(clean) > 2400 or not clean.startswith(("{", "[")):
        return None
    try:
        return json.loads(clean)
    except Exception:
        return None


def _reminder_payload_from_leaked_item(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    normalised = {_normalise_action_key(key): value for key, value in item.items()}
    description = str(normalised.get("description", "") or "").strip()
    due_date = str(normalised.get("duedate", "") or normalised.get("due", "") or "").strip()
    category = str(normalised.get("category", "") or "Teaching").strip() or "Teaching"
    if not description or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", due_date):
        return None
    if len(description) < 4:
        return None
    return {
        "description": description[:500],
        "due_date": due_date,
        "category": category[:80],
    }


async def _recover_leaked_action_payload(text: str) -> str:
    parsed = _parse_leaked_json_payload(text)
    if parsed is None:
        return ""
    items = parsed if isinstance(parsed, list) else [parsed]
    reminder_payloads = [_reminder_payload_from_leaked_item(item) for item in items]
    reminder_payloads = [item for item in reminder_payloads if item]
    if not reminder_payloads or len(reminder_payloads) != len(items):
        return ""
    results = []
    for payload in reminder_payloads[:5]:
        result = await bot._execute_tool_offloop("add_reminder", payload)
        first_line = str(result or "").strip().splitlines()[0] if result else ""
        results.append(first_line or f"Added reminder: {payload['description']} by {payload['due_date']}")
    if len(results) == 1:
        return results[0]
    return "Added these tasks:\n" + "\n".join(f"- {line}" for line in results)


def _empty_chat_reply(text: str) -> bool:
    clean = " ".join(str(text or "").strip().split()).lower()
    return clean in {"", "done", "done."}


def _safe_chat_error_detail(exc: Exception) -> str:
    detail = f"{exc.__class__.__name__}: {' '.join(str(exc or '').split())}".strip()
    if not detail:
        return exc.__class__.__name__
    detail = re.sub(
        r"(?i)\b(api[_ -]?key|authorization|bearer|token|secret|password)\b\s*[:=]\s*[^\s,;]+",
        r"\1=<redacted>",
        detail,
    )
    detail = re.sub(r"sk-[A-Za-z0-9_-]{8,}", "sk-<redacted>", detail)
    return detail[:500]


def _pwa_model_failure_reply(message: str, error_detail: str = "") -> str:
    category = bot.openai_failure_category(error_detail)
    if bot._is_conversational_self_read_text(message) or bot._is_casual_sports_lifestyle_text(message):
        return (
            "I can tell that was a read-the-room question, not a live lookup. "
            "The chat engine failed before I could answer it properly, so I’m not going to fake the personality bit from regex. "
            "Try once more after the model is back."
        )
    if category == "auth":
        return (
            "I’m still here, but OpenAI rejected this app’s API key. "
            "This needs a fresh OPENAI_API_KEY in Railway before chat will recover."
        )
    if category == "quota":
        return (
            "I’m still here, but OpenAI says the account is out of quota or blocked by billing limits. "
            "Retrying will not help until billing or usage limits are fixed."
        )
    if category == "model":
        return (
            "I’m still here, but OpenAI rejected the model configured for this app. "
            "I logged the exact model error in the trace; update the HIRA_*_MODEL or OpenAI fallback model variables in Railway."
        )
    if category == "rate_limit":
        return (
            "I’m still here, but OpenAI is rate-limiting this app right now. "
            "Give it a short pause before trying again."
        )
    return (
        "I’m still here, but the OpenAI chat handoff failed before it returned a real answer. "
        "I’m treating that as a failed pass, not pretending it worked. Send it again once, and I’ll reroute cleanly."
    )


async def _source_check_backend_fallback_payload(message: str) -> tuple[str, str, str]:
    tool_name = _source_tool_for_message(message)
    label = _source_tool_label(tool_name)
    if not tool_name:
        return "", "", ""

    try:
        result = await bot._execute_tool_offloop(tool_name, _source_tool_input(tool_name, message))
    except Exception as exc:
        bot.logger.warning(f"PWA source fallback tool {tool_name} failed: {exc}")
        return (
            f"I hit the model/backend step, and the live {label} source check also failed, "
            "so I'm not going to answer from memory. Try again in a moment."
        ), tool_name, ""
    if not result or result.startswith("Failed to fetch"):
        return (
            "I hit the model/backend step, and the live source check also failed, "
            "so I’m not going to answer from memory. Try again in a moment."
        ), tool_name, result or ""
    return _format_source_fallback_answer(message, label, result), tool_name, result


def _format_source_fallback_answer(message: str, label: str, result: str) -> str:
    if label == "news":
        return _summarise_news_fallback(message, result)
    clipped = _summarise_source_fallback(result, limit=1600)
    contract_note = _source_contract_user_note(label, result)
    parts = [
        f"I hit the backup path, but I did run the live {label} source check.",
        contract_note,
        clipped,
    ]
    return "\n\n".join(part for part in parts if part).strip()


def _source_contract_user_note(label: str, result: str) -> str:
    contracts = bot._source_contracts_from_text(result)
    if not contracts:
        return ""
    contract = contracts[0]
    status = str(contract.get("status", "") or "").strip().lower()
    as_of = str(contract.get("as_of", "") or "").strip()
    source = str(contract.get("source", "") or "").strip()
    reason = str(contract.get("reason", "") or "").strip().rstrip(".")
    source_bit = f" via {source}" if source else ""
    as_of_bit = f" as of {as_of}" if as_of else ""
    if status == "confirmed":
        return f"Live {label} check confirmed{source_bit}{as_of_bit}."
    if reason:
        return f"Live {label} check did not confirm it{source_bit}{as_of_bit}: {reason}."
    return f"Live {label} check did not confirm it{source_bit}{as_of_bit}."


def _fallback_news_items(result: str, limit: int = 3) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for raw in str(result or "").splitlines():
        line = raw.strip()
        if not line.startswith("-"):
            continue
        clean = re.sub(r"^\-\s*", "", line).strip()
        clean = clean.replace("*", "").strip()
        if not clean or clean.lower().startswith("http"):
            continue
        if re.search(r"https?://", clean):
            clean = re.sub(r"\s*https?://\S+", "", clean).strip()
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(clean)
        if len(items) >= limit:
            break
    return items


def _summarise_news_fallback(message: str, result: str) -> str:
    items = _fallback_news_items(result, limit=3)
    lowered = str(message or "").lower()
    asks_rollout = bool(re.search(r"\b(?:when|roll(?:ed|ing)? out|rollout|release date|available|stable)\b", lowered))
    has_rollout_answer = any(
        re.search(r"\b(?:roll(?:ed|ing)? out|rollout|release date|stable|available|starts|begins|launch(?:es|ed)?)\b", item, re.I)
        for item in items
    )
    if not items:
        return (
            "I couldn’t complete the normal answer, and the quick live news check did not return a usable item. "
            "I’m not going to guess from memory."
        )
    bullets = "\n".join(f"- {item}" for item in items)
    if asks_rollout and not has_rollout_answer:
        return (
            "I couldn’t confirm an official rollout date from the quick live check. "
            "The recent items I found are related, but they look like preview/features coverage rather than a rollout schedule:\n\n"
            f"{bullets}"
        )
    return f"The quick live news check found:\n\n{bullets}"


def _summarise_source_fallback(result: str, limit: int = 2200) -> str:
    text = str(result or "").strip()
    if not text:
        return "No usable source text came back."
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    picked: list[str] = []
    for line in lines:
        lower = line.lower()
        if _is_source_fallback_scaffold_line(line):
            continue
        if re.search(r"https?://", line):
            continue
        if (
            lower.endswith("structured live brief")
            or lower in {"- no recent google news items found.", "- no tavily results found."}
            or lower.startswith("- disabled: set tavily_api_key")
        ):
            continue
        if (
            line.startswith("*News:")
            or "latest completed:" in lower
            or "next listed fixture:" in lower
            or line.startswith("- Upcoming:")
            or "recent results from fotmob" in lower
            or "table note:" in lower
            or "upcoming fixtures:" in lower
            or lower.startswith("weather:")
            or lower.startswith("forecast:")
            or lower.startswith("prayer:")
            or lower.startswith("khutbah:")
            or "fotmob fetch failed" in lower
            or "scoreboard warnings:" in lower
        ):
            picked.append(line)
    if not picked:
        picked = [
            line for line in lines
            if not re.search(r"https?://", line)
            and not _is_source_fallback_scaffold_line(line)
            and not line.lower().endswith("structured live brief")
            and line.lower() not in {"- no recent google news items found.", "- no tavily results found."}
            and not line.lower().startswith("- disabled: set tavily_api_key")
        ][:12]
    if not picked:
        return "No usable current source lines came back."
    summary = "\n".join(picked)
    if len(summary) > limit:
        summary = summary[:limit].rsplit("\n", 1)[0].rstrip()
        summary = f"{summary}\n[Source brief truncated.]"
    return summary


def _is_source_fallback_scaffold_line(line: str) -> bool:
    clean = str(line or "").strip()
    lower = clean.lower()
    if not clean:
        return True
    return bool(
        clean.startswith("SOURCE CONTRACT:")
        or lower.startswith("answer guidance:")
        or lower.startswith("answer rule:")
        or lower.startswith("staleness gate:")
        or lower in {
            "priority result probe",
            "authoritative scoreboard probe",
            "targeted web search",
            "official 2026 f1 calendar window",
            "result-source leads:",
            "demoted stale result/news leads:",
            "detected scoreline candidates:",
            "scoreline candidates found on fotmob page:",
            "recent results from fotmob:",
            "fotmob team-page probe",
        }
        or "scoreline candidates" in lower
        or lower.endswith("structured live brief")
        or lower.startswith("- no recent google news items found.")
        or lower.startswith("- no tavily results found.")
        or lower.startswith("- disabled: set tavily_api_key")
    )


@app.post("/api/chat/reset")
def reset_chat(
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    history_key = _history_key(x_hira_client)
    _save_history_best_effort(history_key, [])
    bot.clear_openai_response_state(history_key)
    return {"ok": True}


@app.get("/api/realtime/status")
def realtime_status(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return bot.openai_realtime_status()


@app.post("/api/realtime/session")
async def realtime_session(
    req: RealtimeSessionRequest,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    status = bot.openai_realtime_status()
    if not status.get("enabled"):
        raise HTTPException(status_code=400, detail="OpenAI realtime voice is disabled")
    if not status.get("configured"):
        raise HTTPException(status_code=503, detail="OpenAI realtime voice needs OPENAI_API_KEY configured")
    try:
        return await bot.create_openai_realtime_session(
            extra_instructions=req.instructions,
            voice=req.voice,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not create realtime session: {exc}") from exc


@app.get("/api/agenda")
def agenda(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    structured = None
    try:
        structured = bot.build_agenda_structured(days)
    except Exception:
        structured = None
    return {
        "text": _safe_text(lambda: bot.build_agenda(days), "Agenda unavailable right now."),
        "structured": structured,
    }


@app.get("/api/tasks")
def tasks(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    structured = None
    try:
        structured = bot.build_task_structured(days)
    except Exception:
        structured = None
    return {
        "text": _safe_text(lambda: bot.build_task_brief(days), "Task brief unavailable until Google is connected."),
        "structured": structured,
    }


@app.get("/api/islamic")
def islamic(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return {"text": _safe_text(lambda: bot.build_islamic_brief(), "Islamic rhythm unavailable right now.")}


@app.post("/api/tasks/{task_id}/done")
def task_done(
    task_id: str,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        ok, synced_marking = bot.complete_reminder_by_id(task_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not complete task: {exc}") from exc
    if not ok:
        raise HTTPException(status_code=404, detail=f"Task #{task_id} not found")
    _record_web_action(
        "task.done",
        "done",
        subject=f"Task #{task_id}",
        result=f"Completed reminder #{task_id}",
        client_id=x_hira_client,
        metadata={"reminder_id": str(task_id)},
    )
    return {"ok": True, "synced_marking": synced_marking}


@app.get("/api/notifications")
def notifications(
    limit: int = 12,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        _archive_low_value_notifications()
        items = bot.gs.unseen_app_notifications(_client_key(x_hira_client), limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Notifications unavailable: {exc}") from exc
    return {"notifications": items}


@app.get("/api/notifications/config")
def notifications_config(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return {"vapid_public_key": os.environ.get("HIRA_WEB_PUSH_PUBLIC_KEY", "").strip()}


@app.post("/api/notifications/test")
def notifications_test(
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        item = bot.gs.enqueue_app_notification(
            "test",
            "H.I.R.A notification test",
            "If this reached your phone, PWA push is wired correctly.",
            source=f"test:{_client_key(x_hira_client)}",
        )
        sent = bot.gs.send_web_push_notification(
            item["title"],
            item["body"],
            data={"id": item.get("id", ""), "kind": item.get("kind", "test"), "source": item.get("source", "test")},
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not send test notification: {exc}") from exc
    return {"ok": True, "sent": sent, "notification": item}


def _push_recovery_summary(delivery_log: list, queued: list, subscriptions: list, subscription_error: str = "", queue_error: str = "") -> dict:
    latest = delivery_log[-1] if delivery_log else {}
    successes = [item for item in delivery_log if int(item.get("sent", 0) or 0) > 0]
    latest_success = successes[-1] if successes else {}
    recent_failures = [
        item for item in delivery_log[-10:]
        if int(item.get("attempted", 0) or 0) > 0 and int(item.get("sent", 0) or 0) <= 0
    ]
    if subscription_error or queue_error:
        status = "storage_error"
    elif not os.environ.get("HIRA_WEB_PUSH_PRIVATE_KEY", "").strip():
        status = "missing_push_key"
    elif not subscriptions:
        status = "no_subscriptions"
    elif latest and int(latest.get("sent", 0) or 0) > 0:
        status = "healthy"
    elif latest:
        status = "delivery_missed"
    else:
        status = "no_attempts"
    issue = ""
    if status == "storage_error":
        issue = subscription_error or queue_error
    elif status == "missing_push_key":
        issue = "HIRA_WEB_PUSH_PRIVATE_KEY is not configured."
    elif status == "no_subscriptions":
        issue = "No browser/device push subscriptions are registered."
    elif status == "delivery_missed":
        issue = str(latest.get("last_error", "") or latest.get("errors", {}) or "Last push attempt had no confirmed delivery.")
    return {
        "status": status,
        "issue": issue,
        "last_attempt_at": latest.get("created", ""),
        "last_attempt_source": latest.get("source", ""),
        "last_attempt_sent": int(latest.get("sent", 0) or 0) if latest else 0,
        "last_attempted": int(latest.get("attempted", 0) or 0) if latest else 0,
        "last_success_at": latest_success.get("created", ""),
        "last_success_source": latest_success.get("source", ""),
        "recent_failure_count": len(recent_failures),
        "queued_count": len(queued),
        "subscription_count": len(subscriptions),
    }


def _safe_notifications_diagnostics(client_key: str = "") -> dict:
    try:
        subscriptions = bot.gs.get_web_push_subscriptions()
    except Exception as exc:
        subscriptions = []
        subscription_error = str(exc)
    else:
        subscription_error = ""
    try:
        queued = bot.gs.get_app_notifications(include_archived=False)
    except Exception as exc:
        queued = []
        queue_error = str(exc)
    else:
        queue_error = ""
    try:
        delivery_log = bot.gs.get_web_push_delivery_log()
    except Exception as exc:
        delivery_log = []
        if not queue_error:
            queue_error = str(exc)
    try:
        outcome_summary = bot.gs.get_notification_outcome_summary(days=14)
    except Exception:
        outcome_summary = {"actions": {}}
    current_subscription = None
    if client_key:
        try:
            current_subscription = bot.gs.get_web_push_subscription(client_key)
        except Exception as exc:
            current_subscription = None
            subscription_error = subscription_error or str(exc)
    return {
        "subscriptions": subscriptions,
        "subscription_error": subscription_error,
        "queued": queued,
        "queue_error": queue_error,
        "delivery_log": delivery_log,
        "outcome_summary": outcome_summary,
        "current_subscription": current_subscription,
    }


@app.get("/api/notifications/health")
def notifications_health(
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    client_key = _client_key(x_hira_client)
    diagnostics = _safe_notifications_diagnostics(client_key)
    subscriptions = diagnostics["subscriptions"]
    queued = diagnostics["queued"]
    subscription_error = diagnostics["subscription_error"]
    queue_error = diagnostics["queue_error"]
    delivery_log = diagnostics["delivery_log"]
    outcome_summary = diagnostics["outcome_summary"]
    current_subscription = diagnostics["current_subscription"]
    stale_threshold = datetime.now(bot.SGT) - bot.timedelta(days=30)
    stale_subscriptions = 0
    standalone_subscriptions = 0
    for item in subscriptions:
        if str(item.get("display_mode", "")).strip().lower() in {"standalone", "fullscreen"}:
            standalone_subscriptions += 1
        try:
            last_seen = datetime.fromisoformat(item.get("last_seen", "") or item.get("created", ""))
        except Exception:
            continue
        if last_seen < stale_threshold:
            stale_subscriptions += 1
    return {
        "push_public_key": bool(os.environ.get("HIRA_WEB_PUSH_PUBLIC_KEY", "").strip()),
        "push_private_key": bool(os.environ.get("HIRA_WEB_PUSH_PRIVATE_KEY", "").strip()),
        "push_subject": bool(os.environ.get("HIRA_WEB_PUSH_SUBJECT", "").strip()),
        "subscription_count": len(subscriptions),
        "standalone_subscription_count": standalone_subscriptions,
        "stale_subscription_count": stale_subscriptions,
        "subscription_error": subscription_error,
        "queued_notification_count": len(queued),
        "queue_error": queue_error,
        "push_recovery_enabled": _WEB_PUSH_RECOVERY_ENABLED,
        "current_client_id": client_key,
        "current_client_subscribed": bool(current_subscription),
        "current_client_last_seen": current_subscription.get("last_seen", "") if current_subscription else "",
        "current_client_display_mode": current_subscription.get("display_mode", "") if current_subscription else "",
        "recent_delivery_log": delivery_log[-5:],
        "push_recovery": _push_recovery_summary(delivery_log, queued, subscriptions, subscription_error, queue_error),
        "briefing_delivery": _briefing_delivery_status(delivery_log, queued),
        "outcome_actions": outcome_summary.get("actions", {}),
        "prayers": bot.prayer_notification_status(),
    }


@app.get("/api/notifications/{notification_id}")
def notification_detail(
    notification_id: str,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        item = bot.gs.get_app_notification(notification_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Notification unavailable: {exc}") from exc
    if not item:
        raise HTTPException(status_code=404, detail=f"Notification #{notification_id} not found")
    client_key = _client_key(x_hira_client)
    try:
        bot._record_notification_outcome(
            "opened",
            notification_id=item.get("id", ""),
            source=item.get("source", ""),
            kind=item.get("kind", ""),
            client_id=client_key,
            title=item.get("title", ""),
        )
    except Exception:
        pass
    return {"notification": item}


@app.get("/api/admin/status")
def admin_status(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    base = _health_details()
    try:
        runtime = bot.build_runtime_status()
    except Exception as exc:
        runtime = {"error": str(exc)}
    diagnostics = _safe_notifications_diagnostics()
    subscriptions = diagnostics["subscriptions"]
    queued = diagnostics["queued"]
    subscription_error = diagnostics["subscription_error"]
    queue_error = diagnostics["queue_error"]
    delivery_log = diagnostics["delivery_log"]
    return {
        "health": base,
        "runtime": runtime,
        "notifications": {
            "push_public_key": bool(os.environ.get("HIRA_WEB_PUSH_PUBLIC_KEY", "").strip()),
            "push_private_key": bool(os.environ.get("HIRA_WEB_PUSH_PRIVATE_KEY", "").strip()),
            "push_subject": bool(os.environ.get("HIRA_WEB_PUSH_SUBJECT", "").strip()),
            "subscription_count": len(subscriptions),
            "subscription_error": subscription_error,
            "queued_notification_count": len(queued),
            "queue_error": queue_error,
            "push_recovery_enabled": _WEB_PUSH_RECOVERY_ENABLED,
            "recent_delivery_log": delivery_log[-5:],
            "push_recovery": _push_recovery_summary(delivery_log, queued, subscriptions, subscription_error, queue_error),
            "prayers": bot.prayer_notification_status(),
        },
    }


@app.get("/api/admin/memory")
def admin_memory(limit: int = 5, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        return bot.build_memory_review(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Memory review unavailable: {exc}") from exc


def _ledger_metadata(entry: dict) -> dict:
    metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else {}
    return {str(key): str(value) for key, value in metadata.items()}


def _ledger_id_from_result(entry: dict, pattern: str) -> str:
    match = re.search(pattern, str(entry.get("result", "") or ""))
    return match.group(1) if match else ""


def _undo_action_ledger_entry(entry: dict) -> tuple[bool, str]:
    action = str(entry.get("action", "") or "")
    metadata = _ledger_metadata(entry)
    if entry.get("undo_status") == "undone":
        return True, entry.get("undo_result") or "Already undone."
    if str(entry.get("status", "")).lower() not in {"saved", "done", "snooze", "snoozed"}:
        return False, "Only saved/completed actions can be undone."

    if action == "create_proactive_nudge" or action == "notification.snooze":
        nudge_id = metadata.get("nudge_id") or _ledger_id_from_result(entry, r"nudge #([\w-]+)")
        if not nudge_id:
            return False, "No nudge id was recorded for this action."
        ok = bot.gs.cancel_nudge(nudge_id)
        return ok, f"Cancelled nudge #{nudge_id}." if ok else f"Nudge #{nudge_id} was not found or already sent."

    if action == "create_calendar_event":
        event_id = metadata.get("event_id")
        if not event_id:
            return False, "No calendar event id was recorded for this action."
        ok = bot.gs.delete_event(event_id, metadata.get("calendar_id", ""))
        return ok, f"Deleted calendar event #{event_id}." if ok else f"Calendar event #{event_id} could not be deleted."

    if action in {"task.done", "notification.done"}:
        reminder_id = metadata.get("reminder_id") or _ledger_id_from_result(entry, r"reminder #([\w-]+)")
        if not reminder_id:
            return False, "No reminder id was recorded for this completion."
        ok = bot.gs.mark_not_done(reminder_id)
        return ok, f"Reopened reminder #{reminder_id}." if ok else f"Reminder #{reminder_id} was not found."

    return False, "Undo is not yet available for this action type; review the receipt before changing the source record manually."


@app.get("/api/action-ledger")
def action_ledger(
    limit: int = 20,
    include_reviewed: bool = True,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        entries = bot.gs.get_action_ledger(include_reviewed=include_reviewed)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Action ledger unavailable: {exc}") from exc
    return {"entries": list(reversed(entries))[: max(1, min(int(limit or 20), 80))]}


@app.post("/api/action-ledger/{entry_id}/review")
def action_ledger_review(
    entry_id: str,
    req: ActionLedgerReviewRequest,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    entry = bot.gs.update_action_ledger_entry(entry_id, {"reviewed": bool(req.reviewed)})
    if not entry:
        raise HTTPException(status_code=404, detail=f"Action ledger entry #{entry_id} not found")
    return {"ok": True, "entry": entry}


@app.post("/api/action-ledger/{entry_id}/undo")
def action_ledger_undo(
    entry_id: str,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    entries = bot.gs.get_action_ledger(include_reviewed=True)
    entry = next((item for item in entries if str(item.get("id", "")) == str(entry_id)), None)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Action ledger entry #{entry_id} not found")
    try:
        ok, result = _undo_action_ledger_entry(entry)
    except Exception as exc:
        ok, result = False, f"Undo failed: {exc}"
    updated = bot.gs.update_action_ledger_entry(entry_id, {
        "undo_status": "undone" if ok else "blocked",
        "undo_result": result,
        "reviewed": ok,
    })
    if ok:
        _record_web_action(
            "action_ledger.undo",
            "saved",
            subject=f"Undo #{entry_id}: {entry.get('action', '')}",
            result=result,
            metadata={"target_entry_id": entry_id},
        )
    return {"ok": ok, "result": result, "entry": updated or entry}


def _classops_name_key(value: str) -> str:
    return classops_ai.classops_name_key(value)


def _classops_record_for(ledger: dict, class_name: str) -> dict:
    return classops_ai.classops_record_for(ledger, class_name)


def _classops_parse_date(value: str) -> date | None:
    return classops_ai.parse_classops_date(value)


def _classops_assignment_date(assignment: dict) -> date | None:
    return classops_ai.classops_assignment_date(assignment)


def _classops_timing_context(target: date | None) -> list[dict]:
    return classops_ai.classops_timing_context(target)


def _classops_make_event(assignment: dict, timing_context: list[dict]) -> dict:
    return {
        "assignment_id": str(assignment.get("id", "")),
        "assignment_title": str(assignment.get("assignment_title") or "Tracked work"),
        "lesson_date": str(assignment.get("lesson_date") or ""),
        "collect_by": str(assignment.get("collect_by") or ""),
        "timing_context": timing_context,
    }


def _classops_build_insights(class_name: str, roster: list[dict], assignments: list[dict], today: date) -> list[dict]:
    return classops_ai.build_classops_insights(class_name, roster, assignments, today)


def _classops_student_report(class_name: str, students: list[dict], ledger: dict | None = None, today: date | None = None) -> dict:
    ledger = ledger if isinstance(ledger, dict) else bot.gs.get_classops_ledger()
    return classops_ai.build_student_report(class_name, students, ledger, today=today or datetime.now(bot.SGT).date())


def _classops_enrich_with_students(manifest: dict) -> dict:
    ledger = bot.gs.get_classops_ledger()
    _classops_apply_content_overrides(manifest, ledger)
    student_errors = {}
    for class_item in manifest.get("classes", []) or []:
        class_name = str(class_item.get("class") or "").strip()
        if not class_name:
            continue
        try:
            students = bot.gs.get_classops_students(class_name, include_scores=True)
        except Exception as exc:
            students = []
            student_errors[class_name] = str(exc)
        report = _classops_student_report(class_name, students, ledger)
        class_item["students"] = students
        class_item["student_report"] = report
    manifest["student_errors"] = student_errors
    manifest["student_summary"] = {
        "roster_count": sum(len(item.get("students") or []) for item in manifest.get("classes", []) or []),
        "concern_count": sum((item.get("student_report") or {}).get("concern_count", 0) for item in manifest.get("classes", []) or []),
        "assignment_count": sum((item.get("student_report") or {}).get("assignment_count", 0) for item in manifest.get("classes", []) or []),
        "open_non_submission_count": sum((item.get("student_report") or {}).get("open_non_submission_count", 0) for item in manifest.get("classes", []) or []),
        "insight_count": sum((item.get("student_report") or {}).get("insight_count", 0) for item in manifest.get("classes", []) or []),
    }
    return manifest


def _classops_apply_content_overrides(manifest: dict, ledger: dict | None = None) -> dict:
    if ledger is None:
        ledger = bot.gs.get_classops_ledger()
    overrides = ledger.get("content_overrides") if isinstance(ledger, dict) else {}
    if not isinstance(overrides, dict):
        legacy = ledger.get("content_title_overrides") if isinstance(ledger, dict) else {}
        overrides = legacy if isinstance(legacy, dict) else {}
    normalised = {}
    for path, value in overrides.items():
        key = str(path or "").strip()
        if not key:
            continue
        if isinstance(value, dict):
            normalised[key] = {
                "title": str(value.get("title") or "").strip(),
                "hidden": bool(value.get("hidden", False)),
                "no_submission_needed": bool(value.get("no_submission_needed", False)),
                "purpose_id": str(value.get("purpose_id") or "").strip(),
            }
        else:
            normalised[key] = {"title": str(value or "").strip(), "hidden": False, "no_submission_needed": False, "purpose_id": ""}
    for class_item in manifest.get("classes", []) or []:
        filtered = []
        for item in class_item.get("content_items", []) or []:
            path = str(item.get("path") or "").strip()
            override = normalised.get(path) or {}
            if override.get("hidden"):
                continue
            next_item = dict(item)
            if override.get("title"):
                next_item["title"] = override["title"]
                next_item["title_overridden"] = True
            if override.get("no_submission_needed"):
                next_item["no_submission_needed"] = True
            if override.get("purpose_id"):
                purpose = dropbox.classops_content_purpose_from_id(override["purpose_id"])
                next_item["purpose"] = purpose
                next_item["purpose_id"] = purpose.get("id", "resource")
                next_item["purpose_label"] = purpose.get("label", "Resource")
                next_item["purpose_tone"] = purpose.get("tone", "resource")
                next_item["purpose_rank"] = purpose.get("rank", 90)
                next_item["trackable"] = bool(purpose.get("trackable"))
                next_item["purpose_overridden"] = True
            filtered.append(next_item)
        class_item["content_items"] = dropbox.sort_classops_content_items(filtered)
        class_item["content_item_count"] = len(filtered)
    summary = manifest.get("summary")
    if isinstance(summary, dict):
        summary["content_item_count"] = sum(int(item.get("content_item_count") or 0) for item in manifest.get("classes", []) or [])
    return manifest


def _classops_status_summary() -> dict:
    return classops_ai.build_status_summary(
        bot.gs.get_classops_ledger(),
        bot.gs.get_classops_students,
        now=datetime.now(bot.SGT),
        logger=bot.logger,
    )


def _classops_refresh_running() -> bool:
    return bool(_CLASSOPS_REFRESH_FUTURE and not _CLASSOPS_REFRESH_FUTURE.done())


def _schedule_classops_manifest_refresh() -> bool:
    global _CLASSOPS_REFRESH_FUTURE
    if _classops_refresh_running():
        return False

    future = _HOME_EXECUTOR.submit(dropbox.scan_classops_manifest, True)
    _CLASSOPS_REFRESH_FUTURE = future

    def _done(done_future):
        try:
            done_future.result()
            bot.logger.info("Background ClassOps Dropbox manifest refresh completed.")
        except Exception as exc:
            bot.logger.warning(f"Background ClassOps Dropbox manifest refresh failed: {exc}")

    future.add_done_callback(_done)
    return True


def _classops_extract_lesson_material(lesson: dict) -> dict:
    item = dict(lesson or {})
    path = str(item.get("path") or "").strip()
    if not path:
        return item
    filename = Path(path).name
    caption = " ".join(
        part for part in (
            "ClassOps lesson reflection worksheet",
            str(item.get("title") or ""),
            str(item.get("date") or ""),
        )
        if part
    )
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    mime_by_ext = {
        "pdf": "application/pdf",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }
    mime = mime_by_ext.get(ext, "")
    if not mime:
        item["source_note"] = "Selected file type is not extractable yet; worksheet uses lesson metadata."
        return item
    try:
        content = dropbox.download_file(path)
        kind, index_note, excerpt = docs.extract_supported_document(content, mime, filename, caption=caption)
    except Exception as exc:
        item["source_note"] = f"Could not extract selected lesson file: {exc}"
        return item
    item.update({
        "document_kind": kind,
        "index_note": index_note,
        "excerpt": excerpt[:9000],
        "source_note": f"{kind}: {index_note}",
    })
    return item


@app.post("/api/classops/dropbox/scan")
def classops_dropbox_scan(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    if not dropbox.configured():
        raise HTTPException(status_code=400, detail="Dropbox ClassOps env vars are not configured.")
    try:
        return dropbox.scan_classops_manifest(force_refresh=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Dropbox ClassOps scan failed: {exc}") from exc


@app.get("/api/classops/dashboard")
def classops_dashboard(
    x_hira_token: Optional[str] = Header(default=None),
    refresh: bool = False,
    background: bool = False,
):
    _require_token(x_hira_token)
    if not dropbox.configured():
        raise HTTPException(status_code=400, detail="Dropbox ClassOps env vars are not configured.")
    try:
        if background:
            manifest = dropbox.scan_classops_manifest(allow_stale=True)
            cache = manifest.get("cache", {}) if isinstance(manifest.get("cache"), dict) else {}
            should_refresh = bool(cache.get("hit") and (refresh or cache.get("stale")))
            queued = _schedule_classops_manifest_refresh() if should_refresh else False
        else:
            manifest = dropbox.scan_classops_manifest(force_refresh=refresh)
            queued = False
        dashboard = _classops_enrich_with_students(manifest)
        cache = dashboard.setdefault("cache", {})
        cache["refresh_queued"] = bool(queued)
        cache["refreshing"] = _classops_refresh_running()
        return dashboard
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps dashboard unavailable: {exc}") from exc


@app.get("/api/classops/students")
def classops_students(class_name: str, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        students = bot.gs.get_classops_students(class_name, include_scores=True)
        return {
            "ok": True,
            "class_name": class_name,
            "students": students,
            "report": _classops_student_report(class_name, students),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps student list unavailable: {exc}") from exc


@app.post("/api/classops/assignment")
def classops_assignment(
    req: ClassOpsAssignmentRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        assignment = bot.gs.save_classops_assignment(
            class_name=req.class_name,
            lesson_date=req.lesson_date,
            topic=req.topic,
            folder=req.folder,
            source_path=req.source_path,
            assignment_title=req.assignment_title,
            collect_by=req.collect_by,
            absent=req.absent or [],
            submitted=req.submitted or [],
            non_submitted=req.non_submitted or [],
            notes=req.notes,
        )
        _record_web_action(
            "classops.assignment",
            "saved",
            subject=f"{req.class_name}: {req.assignment_title}",
            date_value=req.collect_by or req.lesson_date,
            result=f"Tracked ClassOps assignment #{assignment.get('id', '')}",
            client_id=x_hira_client,
            metadata={"assignment_id": str(assignment.get("id", "")), "class_name": req.class_name},
        )
        students = bot.gs.get_classops_students(req.class_name, include_scores=True)
        return {
            "ok": True,
            "assignment": assignment,
            "report": _classops_student_report(req.class_name, students),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps assignment save failed: {exc}") from exc


@app.post("/api/classops/content-override")
def classops_content_override(
    req: ClassOpsContentOverrideRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        override = bot.gs.save_classops_content_override(
            req.path,
            title=req.title,
            hidden=req.hidden,
            no_submission_needed=req.no_submission_needed,
            purpose_id=req.purpose_id,
        )
        _record_web_action(
            "classops.content_override",
            "saved",
            subject=req.title or req.path,
            result=f"{'Hid' if req.hidden else 'Updated'} ClassOps content override",
            client_id=x_hira_client,
            metadata={
                "path": req.path,
                "hidden": str(bool(req.hidden)),
                "no_submission_needed": str(bool(req.no_submission_needed)),
                "purpose_id": req.purpose_id or "",
            },
        )
        return {"ok": True, "override": override}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps content update failed: {exc}") from exc


@app.post("/api/classops/assignment/no-submission-needed")
def classops_no_submission_needed(
    req: ClassOpsNoSubmissionNeededRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        deleted = bot.gs.delete_classops_assignment(class_name=req.class_name, source_path=req.source_path)
        override = bot.gs.save_classops_content_override(req.source_path, no_submission_needed=True)
        _record_web_action(
            "classops.assignment",
            "no_submission_needed",
            subject=f"{req.class_name}: {req.assignment_title or req.source_path}",
            result=f"Marked ClassOps item as no submission needed; removed {deleted.get('deleted_count', 0)} tracked records",
            client_id=x_hira_client,
            metadata={
                "class_name": req.class_name,
                "source_path": req.source_path,
                "deleted_count": str(deleted.get("deleted_count", 0)),
            },
        )
        students = bot.gs.get_classops_students(req.class_name, include_scores=True)
        return {
            "ok": True,
            "deleted": deleted,
            "override": override,
            "report": _classops_student_report(req.class_name, students),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps no-submission-needed update failed: {exc}") from exc


@app.post("/api/classops/reflection-worksheet")
def classops_reflection_worksheet(req: ClassOpsReflectionRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        ledger = bot.gs.get_classops_ledger()
        students = bot.gs.get_classops_students(req.class_name, include_scores=True)
        report = _classops_student_report(req.class_name, students, ledger)
        lesson = _classops_extract_lesson_material(req.lesson or {})
        worksheet = classops_ai.build_lesson_reflection_worksheet(req.class_name, lesson, report)
        return {"ok": True, "worksheet": worksheet}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps reflection worksheet failed: {exc}") from exc


@app.get("/api/classops/dropbox/file-link")
def classops_dropbox_file_link(path: str, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    if not dropbox.configured():
        raise HTTPException(status_code=400, detail="Dropbox ClassOps env vars are not configured.")
    try:
        return {"ok": True, **dropbox.get_file_link(path)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Dropbox file link unavailable: {exc}") from exc


@app.get("/api/classops/status")
def classops_status(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        return _classops_status_summary()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps status unavailable: {exc}") from exc


@app.post("/api/notifications/subscribe")
def notifications_subscribe(
    req: PushSubscribeRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        ok = bot.gs.save_web_push_subscription(
            _client_key(x_hira_client),
            req.subscription,
            metadata={
                "display_mode": req.display_mode,
                "app_version": req.app_version,
                "user_agent": req.user_agent,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not save notification subscription: {exc}") from exc
    if not ok:
        raise HTTPException(status_code=400, detail="Invalid notification subscription")
    return {"ok": True}


@app.post("/api/notifications/seen")
def notifications_seen(
    req: NotificationSeenRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        client_key = _client_key(x_hira_client)
        for notification_id in req.ids:
            item = bot.gs.get_app_notification(notification_id)
            if item:
                bot._record_notification_outcome(
                    "seen",
                    notification_id=item.get("id", ""),
                    source=item.get("source", ""),
                    kind=item.get("kind", ""),
                    client_id=client_key,
                    title=item.get("title", ""),
                )
        marked = bot.gs.mark_app_notifications_seen(_client_key(x_hira_client), req.ids)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not update notifications: {exc}") from exc
    return {"ok": True, "marked": marked}


@app.post("/api/notifications/archive")
def notifications_archive(
    req: NotificationSeenRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        client_key = _client_key(x_hira_client)
        for notification_id in req.ids:
            item = bot.gs.get_app_notification(notification_id)
            if item:
                bot._record_notification_outcome(
                    "dismissed",
                    notification_id=item.get("id", ""),
                    source=item.get("source", ""),
                    kind=item.get("kind", ""),
                    client_id=client_key,
                    title=item.get("title", ""),
                )
        archived = bot.gs.archive_app_notifications(req.ids)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not dismiss notifications: {exc}") from exc
    return {"ok": True, "archived": archived}


@app.post("/api/notifications/action")
def notifications_action(
    req: NotificationActionRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    action = str(req.action or "").strip().lower()
    if action not in {"done", "snooze", "useful", "not_useful", "not_now"}:
        raise HTTPException(status_code=400, detail="Unsupported notification action")
    try:
        item = bot.gs.get_app_notification(req.id)
        if not item:
            raise HTTPException(status_code=404, detail=f"Notification #{req.id} not found")
        client_key = _client_key(x_hira_client)
        source = str(item.get("source", "") or "").strip()
        kind = str(item.get("kind", "") or "").strip()
        title = str(item.get("title", "") or "").strip()
        body = str(item.get("body", "") or "").strip()
        result = {}

        if action == "done":
            task_match = re.search(r"(?:^|:)task(?:_reminder)?:[^:]*:(\w[\w-]*)$", source)
            if not task_match:
                task_match = re.search(r"^task_reminder:[^:]+:(\w[\w-]*)$", source)
            followup_match = re.search(r"^followup:(\w[\w-]*)$", source)
            checkin_match = re.search(r"^checkin:(\w[\w-]*)$", source)
            prayer_key = bot._prayer_key_from_notification_source(source)
            nudge_id = _nudge_id_from_source(source)
            if task_match:
                ok, synced_marking = bot.complete_reminder_by_id(task_match.group(1))
                result = {"completed": bool(ok), "synced_marking": synced_marking}
            elif followup_match:
                ok = bot.gs.complete_followup(followup_match.group(1))
                result = {"completed": bool(ok)}
            elif checkin_match:
                ok = bot.gs.complete_checkin_today(checkin_match.group(1))
                result = {"completed": bool(ok)}
            elif prayer_key:
                ok = bot.mark_prayer_prompt_done(prayer_key)
                result = {"completed": bool(ok), "prayer_key": prayer_key}
            elif nudge_id:
                ok = bot.gs.cancel_nudge(nudge_id)
                already_cleared = False
                if not ok:
                    try:
                        already_cleared = any(
                            str(nudge.get("id", "")) == nudge_id
                            and str(nudge.get("status", "")).strip().lower() in {"sent", "cancelled"}
                            for nudge in bot.gs.get_nudges(include_sent=True)
                        )
                    except Exception:
                        already_cleared = False
                result = {"completed": bool(ok or already_cleared), "nudge_id": nudge_id, "nudge_cancelled": bool(ok)}
            else:
                result = {"completed": False, "reason": "No linked task, follow-up, check-in, or nudge"}
            bot._record_notification_outcome(
                "done",
                notification_id=item.get("id", ""),
                source=source,
                kind=kind,
                client_id=client_key,
                title=title,
            )
            archived_ids = _archive_completed_notification(req.id, source)
            if result.get("completed"):
                ledger_meta = {"notification_id": str(req.id), "source": source, "kind": kind}
                if archived_ids:
                    ledger_meta["archived_notification_ids"] = archived_ids
                if task_match:
                    ledger_meta["reminder_id"] = task_match.group(1)
                elif followup_match:
                    ledger_meta["followup_id"] = followup_match.group(1)
                elif checkin_match:
                    ledger_meta["checkin_id"] = checkin_match.group(1)
                elif prayer_key:
                    ledger_meta["prayer_key"] = prayer_key
                elif nudge_id:
                    ledger_meta["nudge_id"] = nudge_id
                _record_web_action(
                    "notification.done",
                    "done",
                    subject=title or body or f"Notification #{req.id}",
                    result=f"Completed from notification #{req.id}",
                    client_id=x_hira_client,
                    metadata=ledger_meta,
                )
        elif action == "snooze":
            minutes = max(5, min(1440, int(req.snooze_minutes or 30)))
            send_at = (datetime.now(bot.SGT) + bot.timedelta(minutes=minutes)).isoformat()
            message = body or title or "H.I.R.A reminder"
            nudge = bot.gs.add_nudge(message, send_at)
            bot._record_notification_outcome(
                "snoozed",
                notification_id=item.get("id", ""),
                source=source,
                kind=kind,
                rating=str(minutes),
                client_id=client_key,
                title=title,
            )
            bot.gs.archive_app_notifications([req.id])
            result = {"snoozed_until": send_at, "nudge_id": nudge.get("id", "")}
            _record_web_action(
                "notification.snooze",
                "snoozed",
                subject=title or message,
                date_value=send_at,
                result=f"Snoozed notification #{req.id} for {minutes} minutes",
                client_id=x_hira_client,
                metadata={
                    "notification_id": str(req.id),
                    "source": source,
                    "kind": kind,
                    "nudge_id": str(nudge.get("id", "")),
                },
            )
        else:
            rating = "useful" if action == "useful" else action
            bot.gs.add_insight_feedback("notification", req.id, rating)
            bot._record_notification_outcome(
                rating,
                notification_id=item.get("id", ""),
                source=source,
                kind=kind,
                rating=rating,
                client_id=client_key,
                title=title,
            )
            if rating in {"not_useful", "not_now"}:
                bot.gs.archive_app_notifications([req.id])
            result = {"rating": rating}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not apply notification action: {exc}") from exc
    return {"ok": True, "action": action, **result}


@app.post("/api/insights/feedback")
def insight_feedback(
    req: InsightFeedbackRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        feedback = bot.gs.add_insight_feedback(req.kind, req.target, req.rating, req.note)
        if req.kind == "notification":
            item = bot.gs.get_app_notification(req.target)
            if item:
                bot._record_notification_outcome(
                    req.rating,
                    notification_id=item.get("id", ""),
                    source=item.get("source", ""),
                    kind=item.get("kind", ""),
                    rating=req.rating,
                    client_id=_client_key(x_hira_client),
                    title=item.get("title", ""),
                )
                if str(req.rating or "").strip() in bot.NOTIFICATION_NEGATIVE_ACTIONS:
                    bot.gs.archive_app_notifications([req.target])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not save feedback: {exc}") from exc
    return {"ok": True, "count": len(feedback)}


@app.get("/api/taste")
def taste_profile(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return bot.taste_calibration_prompt()


@app.post("/api/taste")
def taste_profile_save(
    req: TasteProfileRequest,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        profile = bot.save_taste_profile(req.answers)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not save taste profile: {exc}") from exc
    return {"ok": True, "profile": profile}


@app.get("/api/files")
def files(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return {"text": _safe_text(lambda: bot.build_files_index(), "File memory unavailable until Google is connected.")}


@app.post("/api/gmail")
def gmail(req: GmailRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    req.max_items = max(1, min(20, req.max_items))
    account = bot._normalise_gmail_account(req.account)
    if not bot.gs.gmail_ok(account):
        raise HTTPException(status_code=400, detail=f"{bot.gs.gmail_label(account).title()} is not connected")
    try:
        messages = bot.gs.list_gmail_messages(req.query, req.max_items, account=account)
        if account == "work":
            bot.record_work_gmail_success("pwa_gmail", messages_scanned=len(messages))
    except Exception as exc:
        bot.logger.warning("PWA Gmail fetch failed for account=%s query=%r: %s", account, req.query, exc)
        raise _gmail_http_error(exc, account) from exc
    return {"account": account, "messages": messages}


@app.post("/api/gmail/draft")
def gmail_draft(
    req: DraftRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    account = bot._normalise_gmail_account(req.account)
    if not bot.gs.gmail_ok(account):
        raise HTTPException(status_code=400, detail=f"{bot.gs.gmail_label(account).title()} is not connected")
    try:
        draft = bot.gs.create_gmail_draft(req.to, req.subject, req.body, req.cc, account=account)
        if account == "work":
            bot.record_work_gmail_success("pwa_gmail_draft")
    except Exception as exc:
        bot.logger.warning("PWA Gmail draft failed for account=%s to=%r: %s", account, req.to, exc)
        raise _gmail_http_error(exc, account) from exc
    draft_id = str(draft.get("id", "") or "")
    _record_web_action(
        "gmail.draft",
        "saved",
        subject=req.subject,
        result=f"Created {bot.gs.gmail_label(account)} Gmail draft: {draft_id}",
        client_id=x_hira_client,
        metadata={"draft_id": draft_id, "account": account, "to": req.to},
    )
    return {"account": account, "draft_id": draft.get("id", "")}


@app.post("/api/upload")
async def upload_document(
    request: Request,
    file: UploadFile = File(...),
    note: str = Form(""),
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    if not await _UPLOAD_RATE_LIMITER.is_allowed(_request_ip(request)):
        raise HTTPException(status_code=429, detail="Too many uploads. Wait a minute and try again.")
    mime = (file.content_type or "").lower()
    filename = file.filename or ""
    is_document = _is_supported_document(mime, filename)
    max_bytes = _MAX_DOCUMENT_BYTES if is_document else _MAX_UPLOAD_BYTES
    file_size = getattr(file, "size", None)
    if file_size is not None and file_size > max_bytes:
        label = "Document" if is_document else "Upload"
        raise HTTPException(status_code=413, detail=f"{label} is too large. Limit is {max_bytes // (1024 * 1024)} MB.")
    async with _UPLOAD_SEMAPHORE:
        try:
            return await _process_upload_document(file, note)
        finally:
            gc.collect()
            bot._log_memory("after pwa upload")


@app.post("/api/upload/jobs")
async def create_upload_job(
    request: Request,
    file: UploadFile = File(...),
    note: str = Form(""),
    request_id: str = Form(""),
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    client_key = _client_key(x_hira_client)
    existing_job_id = _get_upload_job_id_for_request(client_key, request_id)
    if existing_job_id:
        existing_job = _get_upload_job(existing_job_id)
        if existing_job and existing_job.get("status") != "missing":
            return _upload_job_public(existing_job)
    if not await _UPLOAD_RATE_LIMITER.is_allowed(_request_ip(request)):
        raise HTTPException(status_code=429, detail="Too many uploads. Wait a minute and try again.")
    mime = (file.content_type or "").lower()
    filename = file.filename or ""
    is_document = _is_supported_document(mime, filename)
    max_bytes = _MAX_DOCUMENT_BYTES if is_document else _MAX_UPLOAD_BYTES
    file_size = getattr(file, "size", None)
    if file_size is not None and file_size > max_bytes:
        label = "Document" if is_document else "Upload"
        raise HTTPException(status_code=413, detail=f"{label} is too large. Limit is {max_bytes // (1024 * 1024)} MB.")
    job_id = uuid.uuid4().hex
    tmp_path, total = await _spool_upload_to_temp(file, max_bytes)
    _set_upload_job(job_id, {
        "status": "queued",
        "filename": filename,
        "mime": mime,
        "note": note,
        "request_id": request_id,
        "client_key": client_key,
        "size": total,
    })
    _set_upload_request_job(client_key, request_id, job_id)
    try:
        await _enqueue_upload_job({
            "job_id": job_id,
            "tmp_path": tmp_path,
            "mime": mime,
            "filename": filename,
            "note": note,
        })
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise
    return _upload_job_public(_get_upload_job(job_id) or {"job_id": job_id, "status": "queued"})


@app.get("/api/upload/jobs/{job_id}")
def get_upload_job(
    job_id: str,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    job = _get_upload_job(job_id)
    if job and job.get("status") != "missing":
        owner = str(job.get("client_key", "") or "").strip()
        if owner and owner != _client_key(x_hira_client):
            return _upload_job_public({"job_id": job_id, "status": "missing", "error": "Upload job not found or expired."})
    return _upload_job_public(job or {"job_id": job_id, "status": "missing"})


async def _run_upload_job(job_id: str, tmp_path: str, mime: str, filename: str, note: str):
    _set_upload_job(job_id, {"status": "running"})
    try:
        async with _UPLOAD_SEMAPHORE:
            result = await _process_upload_path(tmp_path, mime, filename, note)
        _set_upload_job(job_id, {"status": "done", **result})
    except Exception as exc:
        bot.logger.exception(f"Upload job {job_id} failed: {exc}")
        _set_upload_job(job_id, {"status": "error", "error": str(exc)})
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        gc.collect()
        bot._log_memory(f"after upload job {job_id}")


async def _process_upload_document(file: UploadFile, note: str = ""):
    mime = (file.content_type or "").lower()
    filename = file.filename or ""
    is_document = _is_supported_document(mime, filename)
    max_bytes = _MAX_DOCUMENT_BYTES if is_document else _MAX_UPLOAD_BYTES

    if is_document:
        suffix = Path(filename).suffix or ".upload"
        tmp_path = ""
        total = 0
        try:
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp_path = tmp.name
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail=f"Document is too large. Limit is {max_bytes // (1024 * 1024)} MB.",
                        )
                    tmp.write(chunk)
            if not total:
                raise HTTPException(status_code=400, detail="Empty file")
            kind, index_note, excerpt = docs.extract_supported_document_path(
                tmp_path,
                mime,
                filename=filename,
                caption=note,
            )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
        return await _analyse_document_excerpt(kind, index_note, excerpt, note)

    data = await _read_upload_bytes(file, max_bytes)

    if mime.startswith("image/") or filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
        return await _analyse_image_bytes(data, mime, filename, note)

    if mime.startswith("audio/") or filename.lower().endswith((".ogg", ".m4a", ".mp3", ".wav")):
        if not os.environ.get("OPENAI_API_KEY", "").strip():
            raise HTTPException(status_code=400, detail="Voice notes need OPENAI_API_KEY configured first")
        try:
            from openai import OpenAI

            suffix = Path(filename).suffix or ".ogg"
            with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
                tmp.write(data)
                tmp.flush()
                client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
                with open(tmp.name, "rb") as audio:
                    transcript = client.audio.transcriptions.create(
                        model=os.environ.get("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe"),
                        file=audio,
                    )
            text = getattr(transcript, "text", str(transcript)).strip()
            if not text:
                raise HTTPException(status_code=400, detail="I could not transcribe that voice note")
            reply_text = await bot._run_agentic_chat(
                [{"role": "user", "content": text}],
                max_tokens=1600,
                direct_user_text=text,
            )
            return {"reply": reply_text, "index": f"Voice note transcribed: {text}"}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not process voice note: {exc}") from exc

    raise HTTPException(status_code=400, detail=f"Unsupported document type: {mime or filename}")


async def _process_upload_path(tmp_path: str, mime: str, filename: str, note: str):
    is_document = _is_supported_document(mime, filename)
    if is_document:
        try:
            kind, index_note, excerpt = docs.extract_supported_document_path(
                tmp_path,
                mime,
                filename=filename,
                caption=note,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return await _analyse_document_excerpt(kind, index_note, excerpt, note)

    name = (filename or "").lower()
    if mime.startswith("image/") or name.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
        with open(tmp_path, "rb") as fh:
            data = fh.read()
        return await _analyse_image_bytes(data, mime, filename, note)

    if mime.startswith("audio/") or name.endswith((".ogg", ".m4a", ".mp3", ".wav")):
        if not os.environ.get("OPENAI_API_KEY", "").strip():
            raise HTTPException(status_code=400, detail="Voice notes need OPENAI_API_KEY configured first")
        try:
            from openai import OpenAI

            client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            with open(tmp_path, "rb") as audio:
                transcript = client.audio.transcriptions.create(
                    model=os.environ.get("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe"),
                    file=audio,
                )
            text = getattr(transcript, "text", str(transcript)).strip()
            if not text:
                raise HTTPException(status_code=400, detail="I could not transcribe that voice note")
            reply_text = await bot._run_agentic_chat(
                [{"role": "user", "content": text}],
                max_tokens=1600,
                direct_user_text=text,
            )
            return {"reply": reply_text, "index": f"Voice note transcribed: {text}"}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not process voice note: {exc}") from exc

    raise HTTPException(status_code=400, detail=f"Unsupported document type: {mime or filename}")


async def _analyse_document_excerpt(kind: str, index_note: str, excerpt: str, note: str):
    if not excerpt.strip():
        return {
            "reply": (
                f"{index_note}\n\n"
                "I could not extract searchable text. Send an OCR/searchable export or the relevant page screenshots."
            )
        }

    prompt = (
        f"{bot.DOCUMENT_ANALYSIS_INSTRUCTION}\n\n"
        f"Document type: {kind}\n"
        f"User note: {note or 'Analyse this document for useful school/work actions.'}\n\n"
        f"Document index: {index_note}\n\n"
        f"Extracted relevant text:\n{excerpt}"
    )
    reply_text = await bot._run_agentic_chat(
        [{"role": "user", "content": prompt}],
        max_tokens=2500,
        tools=[bot.CONTEXT_TOOL, bot.CALENDAR_TOOL, bot.REMINDER_TOOL, bot.MEMORY_TOOL],
        direct_user_text=note,
    )
    return {"reply": reply_text, "index": index_note}
