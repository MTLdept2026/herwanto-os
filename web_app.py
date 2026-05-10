from __future__ import annotations

import base64
import asyncio
import gc
import json
import os
import re
import secrets
import subprocess
import tempfile
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta
import ipaddress
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
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

PWA_APP_VERSION = "20260510-classops-37"
PWA_SERVICE_WORKER_CACHE = "hira-os-v108"

try:
    _HOME_EXECUTOR_WORKERS = int(os.environ.get("HIRA_HOME_WORKERS", "4"))
except ValueError:
    _HOME_EXECUTOR_WORKERS = 4
_HOME_EXECUTOR_WORKERS = max(1, min(4, _HOME_EXECUTOR_WORKERS))
_HOME_EXECUTOR = ThreadPoolExecutor(max_workers=_HOME_EXECUTOR_WORKERS)
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
    content_length = request.headers.get("content-length")
    if content_length and content_length.isdigit() and int(content_length) > _MAX_REQUEST_BYTES:
        return JSONResponse(
            {"detail": f"Request is too large. Limit is {_MAX_REQUEST_BYTES // (1024 * 1024)} MB."},
            status_code=413,
        )
    if request.url.path.startswith("/api/"):
        expected = _expected_web_token()
        if not expected:
            return JSONResponse(
                {"detail": "HIRA_WEB_TOKEN is not configured. Set it in Railway environment variables."},
                status_code=503,
            )
        if not _token_matches(request.headers.get("x-hira-token"), expected):
            if not await _AUTH_RATE_LIMITER.is_allowed(_request_ip(request)):
                return JSONResponse(
                    {"detail": "Too many invalid token attempts. Try again in a minute."},
                    status_code=429,
                    headers={"Retry-After": "60"},
                )
            return JSONResponse({"detail": "Invalid H.I.R.A web token"}, status_code=401)
    is_static_path = request.url.path in _STATIC_PATHS or request.url.path.startswith("/static/")
    if _memory_pressure_high() and not is_static_path:
        gc.collect()
        return JSONResponse(
            {"detail": "H.I.R.A is under memory pressure. Try again in a moment."},
            status_code=503,
            headers={"Retry-After": "20"},
        )
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
    return response


async def _web_daily_briefing_loop(hour: int, minute: int, sender, source: str):
    last_attempt_date = None
    while True:
        try:
            now = datetime.now(bot.SGT)
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            grace_until = target + bot.timedelta(minutes=bot.DAILY_JOB_GRACE_MINUTES)
            today_key = now.strftime("%Y-%m-%d")
            if target <= now <= grace_until and today_key != last_attempt_date:
                bot.logger.info(f"Web scheduler running {source} for {today_key}")
                last_attempt_date = today_key
                await sender(context=None, source=source)
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


def recover_missed_push_notifications(limit: int | None = None) -> dict:
    current = datetime.now(bot.SGT)
    max_age = current - bot.timedelta(hours=_WEB_PUSH_RECOVERY_MAX_AGE_HOURS)
    try:
        queued = bot.gs.get_app_notifications(include_archived=False)
        delivery_log = bot.gs.get_web_push_delivery_log()
    except Exception as exc:
        bot.logger.warning(f"Web push recovery storage error: {exc}")
        return {"attempted": 0, "sent": 0, "error": str(exc)}

    attempted = 0
    sent_total = 0
    skipped = 0
    for item in reversed(queued):
        if attempted >= max(1, int(limit or _WEB_PUSH_RECOVERY_LIMIT)):
            break
        source = _notification_push_source(item)
        kind = str(item.get("kind", "") or "").strip()
        title = str(item.get("title", "H.I.R.A") or "H.I.R.A").strip()
        body = str(item.get("body", "") or "").strip()
        if not source or not body:
            skipped += 1
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


async def _web_push_recovery_loop():
    while True:
        try:
            result = recover_missed_push_notifications()
            if result.get("attempted"):
                bot.logger.info(
                    "Web push recovery attempted=%s sent=%s skipped=%s",
                    result.get("attempted", 0),
                    result.get("sent", 0),
                    result.get("skipped", 0),
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
            _web_daily_briefing_loop(morning_hour, morning_minute, bot.send_morning_briefing_once, "web_morning_briefing")
        ))
    if evening_enabled:
        evening_hour, evening_minute = bot.EVENING_BRIEFING_TIME
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(
            _web_daily_briefing_loop(evening_hour, evening_minute, bot.send_evening_briefing_once, "web_evening_briefing")
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


class ChatRequest(BaseModel):
    message: str
    location: Optional[DeviceLocation] = None


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


class ClassOpsAssignmentRequest(BaseModel):
    class_name: str
    lesson_date: str = ""
    topic: str = ""
    folder: str = ""
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


_UPLOAD_JOBS: OrderedDict[str, dict] = OrderedDict()
_MAX_LOCAL_UPLOAD_JOBS = _env_int("HIRA_WEB_MAX_LOCAL_UPLOAD_JOBS", 100)
_WEB_WORKING_MEMORY: OrderedDict[str, dict] = OrderedDict()
_MAX_LOCAL_WORKING_MEMORIES = _env_int("HIRA_WEB_MAX_LOCAL_WORKING_MEMORIES", 100)


def _history_key(client_id: str | None) -> str:
    clean = (client_id or "").strip()
    return f"pwa:{clean}" if clean else "pwa"


def _working_memory_storage_key(history_key: str) -> str:
    return f"workmem:{history_key}"


def _safe_text(builder, fallback: str) -> str:
    try:
        return builder()
    except Exception:
        return fallback


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
    r"this|that|it|its|they|them|there|then|again|earlier|"
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
        "pronouns or vague references. Resolve words like this/that/it/again/that day against the newest "
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
    r"\b(remind|reminder|nudge|ping|notify|calendar|schedule|add|delete|remove|mark|done|follow[- ]?up|briefing)\b",
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
    if not _ACTION_KEYWORD_RE.search(clean):
        return ""
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


def _update_working_memory(history_key: str, history: list, message: str) -> dict:
    memory = _load_working_memory(history_key)
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
    pending_action = _pending_action_from_text(message) or str(memory.get("pending_action", "") or "")
    updated = {
        **memory,
        "current_subject": current_subject,
        "latest_correction": latest_correction,
        "pending_action": pending_action,
        "competing_subjects": competing[-3:],
        "updated_at": datetime.now(bot.SGT).isoformat(),
    }
    _save_working_memory(history_key, updated)
    return updated


def _working_memory_context(memory: dict) -> str:
    subject = memory.get("current_subject")
    pending = memory.get("pending_action")
    correction = memory.get("latest_correction")
    competing = memory.get("competing_subjects") or []
    if not any([subject, pending, correction, competing]):
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
    connected_count = sum(1 for value in services.values() if value)
    disconnected_count = max(0, len(services) - connected_count)
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
        f"{connected_count}/{len(services) or 0} services connected",
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


def _parallel_home_data(days: int) -> dict:
    jobs = {
        "agenda": lambda: bot.build_agenda(days),
        "agenda_structured": lambda: bot.build_agenda_structured(days),
        "daily_load": lambda: bot.build_daily_load(days),
        "digest": bot.build_curated_digest_snapshot,
        "proactive": lambda: bot.build_proactive_v2_snapshot(days=days),
        "tasks": lambda: bot.build_task_brief(days),
        "tasks_structured": lambda: bot.build_task_structured(days),
        "islamic": lambda: bot.build_islamic_brief(),
        "prayers": bot.prayer_notification_status,
        "files": bot.build_files_index,
        "services": _service_status,
        "marking": _marking_summary,
        "classops": _classops_status_summary,
    }
    fallbacks = {
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
    }
    futures = {key: _HOME_EXECUTOR.submit(builder) for key, builder in jobs.items()}
    wait(futures.values(), timeout=20)
    results = {}
    for key, future in futures.items():
        if not future.done():
            future.cancel()
            results[key] = fallbacks[key]
            continue
        try:
            results[key] = future.result()
        except Exception:
            results[key] = fallbacks[key]
    results["intelligence"] = _home_intelligence(results, days)
    return results


def _service_status() -> dict:
    return {
        "google": bot.google_ok(),
        "calendar": bot.google_ok(),
        "work_drive": bot.google_ok(),
        "personal_gmail": bot.gs.gmail_ok("personal"),
        "personal_gmail2": bot.gs.gmail_ok("personal2"),
        "work_gmail": bot.gs.gmail_ok("work"),
        "dropbox": dropbox.configured(),
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
        "connected": True,
    }


def _expected_web_token() -> str:
    return os.environ.get("HIRA_WEB_TOKEN", "").strip()


def _token_matches(candidate: str | None, expected: str) -> bool:
    candidate = str(candidate or "")
    return bool(expected) and secrets.compare_digest(candidate, expected)


def _require_token(x_hira_token: Optional[str] = Header(default=None)):
    expected = _expected_web_token()
    if not expected:
        raise HTTPException(status_code=503, detail="HIRA_WEB_TOKEN is not configured. Set it in Railway environment variables.")
    if not _token_matches(x_hira_token, expected):
        raise HTTPException(status_code=401, detail="Invalid H.I.R.A web token")


# ─── Rate limiting ────────────────────────────────────────────────────────────
import time as _time
from collections import defaultdict as _defaultdict


class _SlidingWindowRateLimiter:
    """In-memory sliding window rate limiter keyed by IP address."""

    def __init__(self, max_requests: int, window_seconds: int = 60):
        self._max = max_requests
        self._window = window_seconds
        self._buckets: dict[str, list[float]] = _defaultdict(list)
        self._lock = asyncio.Lock()

    async def is_allowed(self, key: str) -> bool:
        async with self._lock:
            now = _time.monotonic()
            cutoff = now - self._window
            bucket = self._buckets[key]
            # Drop timestamps outside the window
            while bucket and bucket[0] < cutoff:
                bucket.pop(0)
            if len(bucket) >= self._max:
                return False
            bucket.append(now)
            return True

    async def prune(self):
        """Remove stale buckets — call periodically to avoid memory growth."""
        async with self._lock:
            cutoff = _time.monotonic() - self._window
            stale = [k for k, v in self._buckets.items() if not v or v[-1] < cutoff]
            for k in stale:
                del self._buckets[k]


_CHAT_RATE_LIMITER   = _SlidingWindowRateLimiter(max_requests=_env_int("HIRA_CHAT_RATE_LIMIT", 20), window_seconds=60)
_UPLOAD_RATE_LIMITER = _SlidingWindowRateLimiter(max_requests=_env_int("HIRA_UPLOAD_RATE_LIMIT", 10), window_seconds=60)
_AUTH_RATE_LIMITER   = _SlidingWindowRateLimiter(max_requests=_AUTH_RATE_LIMIT, window_seconds=60)


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


def _client_key(client_id: str | None) -> str:
    clean = (client_id or "").strip()
    return clean or "pwa"


def _upload_job_key(job_id: str) -> str:
    return f"hira:upload_job:{job_id}"


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


def _briefing_replay_slot(message: str) -> str:
    clean = " ".join((message or "").lower().split())
    if not clean:
        return ""
    wants_digest = re.search(r"\b(digest|briefing|brief|roundup)\b", clean)
    wants_live = re.search(r"\b(right now|live|current|fresh|now)\b", clean)
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


def _live_briefing_slot(message: str) -> str:
    clean = " ".join((message or "").lower().split())
    if not clean:
        return ""
    if not re.search(r"\b(brief me|briefing|brief)\b", clean):
        return ""
    if not re.search(r"\b(right now|live|current|fresh|now)\b", clean):
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


def _quick_sse_response(reply: str, history_key: str, history: list, route_name: str = "quick", tool_name: str = ""):
    history.append({"role": "assistant", "content": reply})
    bot.save_history(history_key, history[-bot.MAX_TURNS:])

    async def events():
        def sse(payload: dict) -> str:
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        try:
            yield sse({"type": "route", "name": route_name})
            if tool_name:
                yield sse({"type": "tool", "name": tool_name})
            yield sse({"type": "text", "text": reply})
            yield sse({"type": "done", "text": reply})
            yield sse({"type": "saved"})
        finally:
            _CHAT_SEMAPHORE.release()
            gc.collect()
            bot._log_memory(f"after pwa {route_name}")

    return StreamingResponse(events(), media_type="text/event-stream")


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
    history = bot.get_history(history_key)
    working_memory = _update_working_memory(history_key, history, message)
    working_summary = _working_memory_summary(working_memory)
    bot.absorb_taste_hint(message)
    user_content = message
    if bot.re.search(r"\b(?:personal|personal\s*2|second(?:ary)?|other\s+personal|work|moe|school)\s+(?:gmail|email|emails|mail|inbox)\b", message, bot.re.I):
        account_hint, _ = bot._extract_gmail_account_from_text(message)
        user_content = f"{message}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
    user_content = (
        f"{user_content}"
        f"{_working_memory_context(working_memory)}"
        f"{_recent_turn_grounding_context(history, message)}"
        f"{bot.intent_lens_hint(message)}"
        f"{bot.source_discipline_hint(message)}"
    )
    location_context = _device_location_context(location)
    if location_context:
        user_content = f"{user_content}{location_context}"
    history.append({"role": "user", "content": user_content})
    history = history[-bot.MAX_TURNS:]

    live_briefing_slot = _live_briefing_slot(message)
    if live_briefing_slot:
        reply = _live_briefing_text(live_briefing_slot)
        return _quick_sse_response(reply, history_key, history, route_name="live_briefing", tool_name=f"{live_briefing_slot}_briefing")

    briefing_slot = _briefing_replay_slot(message)
    if briefing_slot:
        reply = _briefing_replay_text(briefing_slot)
        return _quick_sse_response(reply, history_key, history, route_name="briefing_replay", tool_name=f"{briefing_slot}_briefing")

    quick_checkin_reply = ""
    if bot.google_ok() and bot._is_affirmative(message):
        try:
            completed = []
            for checkin in bot.gs.awaiting_checkins():
                if bot.gs.complete_checkin_today(checkin["id"]):
                    completed.append(checkin["name"])
            if completed:
                quick_checkin_reply = (
                    f"Marked done for today: {', '.join(completed)}. "
                    "I’ll leave you in peace until tomorrow."
                )
        except Exception as exc:
            bot.logger.warning(f"PWA check-in affirmation error: {exc}")

    if quick_checkin_reply:
        history.append({"role": "assistant", "content": quick_checkin_reply})
        bot.save_history(history_key, history[-bot.MAX_TURNS:])

        async def quick_checkin_events():
            def sse(payload: dict) -> str:
                return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            try:
                yield sse({"type": "route", "name": "quick"})
                yield sse({"type": "text", "text": quick_checkin_reply})
                yield sse({"type": "done", "text": quick_checkin_reply})
                yield sse({"type": "saved"})
            finally:
                _CHAT_SEMAPHORE.release()
                gc.collect()
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
        bot.save_history(history_key, history[-bot.MAX_TURNS:])

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
                gc.collect()
                bot._log_memory("after pwa delayed digest schedule")

        return StreamingResponse(delayed_digest_events(), media_type="text/event-stream")

    async def events():
        reply_parts: list[str] = []
        final_text = ""
        started = time.perf_counter()
        phase_started = started

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
            return payload

        try:
            if working_summary:
                yield sse({"type": "understood", **working_summary})
            quick = await bot.should_route_quick_pwa_chat(list(history[:-1]), message)
            yield sse(timing("route"))
            yield sse({"type": "route", "name": "quick" if quick else "agentic"})
            recent_context = "\n".join(
                str(item.get("content", ""))[:600]
                for item in history[-6:-1]
                if isinstance(item.get("content"), str)
            )
            tools = [] if quick else bot.pwa_tools_for_message(message, recent_context=recent_context)
            if not quick:
                yield sse({"type": "tools", "count": len(tools), "names": [tool["name"] for tool in tools]})
            stream = (
                bot.stream_quick_pwa_reply(list(history[:-1]), message)
                if quick
                else bot.stream_agentic_claude(list(history), max_tokens=_CHAT_MAX_TOKENS, tools=tools)
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
                yield sse(event)

            reply_text = final_text or "".join(reply_parts).strip() or "Done."
            history.append({"role": "assistant", "content": reply_text})
            bot.save_history(history_key, history[-bot.MAX_TURNS:])
            try:
                recorded = bot.record_chat_learning_event(message, reply_text, source="pwa")
                if recorded:
                    yield sse({"type": "learning", "count": len(recorded), "kinds": [item["type"] for item in recorded]})
            except Exception as exc:
                bot.logger.warning(f"PWA learning event failed: {exc}")
            yield sse(timing("saved"))
            yield sse({"type": "saved"})
        except Exception as exc:
            bot.logger.exception(f"PWA chat failed: {exc}")
            yield sse({
                "type": "error",
                "message": "H.I.R.A hit a backend snag. Try again in a moment.",
            })
        finally:
            _CHAT_SEMAPHORE.release()
            gc.collect()
            bot._log_memory("after pwa chat")

    return StreamingResponse(events(), media_type="text/event-stream")


@app.post("/api/chat/reset")
def reset_chat(
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    bot.save_history(_history_key(x_hira_client), [])
    return {"ok": True}


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
def task_done(task_id: str, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        ok, synced_marking = bot.complete_reminder_by_id(task_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not complete task: {exc}") from exc
    if not ok:
        raise HTTPException(status_code=404, detail=f"Task #{task_id} not found")
    return {"ok": True, "synced_marking": synced_marking}


@app.get("/api/notifications")
def notifications(
    limit: int = 12,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
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
    today = today or datetime.now(bot.SGT).date()
    record = _classops_record_for(ledger, class_name)
    assignments = [item for item in record.get("assignments", []) if isinstance(item, dict)]
    roster = []
    for index, student in enumerate(students or [], start=1):
        name = str(student.get("name") or "").strip()
        if not name:
            continue
        roster.append({
            "no": str(student.get("no") or index).strip(),
            "class": str(student.get("class") or class_name).strip(),
            "name": name,
            "source": str(student.get("source") or "").strip(),
            "submitted_count": 0,
            "missing_count": 0,
            "absent_count": 0,
            "catchup_count": 0,
            "risk_score": 0,
            "risk_reasons": [],
            "timing_patterns": {},
            "status": "clear",
        })
    roster_by_key = {_classops_name_key(item["name"]): item for item in roster}
    student_events = {key: {"missing": [], "absent": []} for key in roster_by_key}
    unmatched = {"absent": [], "submitted": [], "non_submitted": []}
    assignment_summaries = []
    for assignment in assignments:
        due_date = _classops_parse_date(assignment.get("collect_by", ""))
        timing_context = _classops_timing_context(due_date)
        event = _classops_make_event(assignment, timing_context)
        submitted = {_classops_name_key(name) for name in assignment.get("submitted", []) if _classops_name_key(name)}
        non_submitted = {_classops_name_key(name) for name in assignment.get("non_submitted", []) if _classops_name_key(name)}
        absent = {_classops_name_key(name) for name in assignment.get("absent", []) if _classops_name_key(name)}
        for raw in assignment.get("submitted", []) or []:
            key = _classops_name_key(raw)
            if key and key not in roster_by_key:
                unmatched["submitted"].append({"assignment_id": assignment.get("id", ""), "name": str(raw)})
        for raw in assignment.get("non_submitted", []) or []:
            key = _classops_name_key(raw)
            if key and key not in roster_by_key:
                unmatched["non_submitted"].append({"assignment_id": assignment.get("id", ""), "name": str(raw)})
        for raw in assignment.get("absent", []) or []:
            key = _classops_name_key(raw)
            if key and key not in roster_by_key:
                unmatched["absent"].append({"assignment_id": assignment.get("id", ""), "name": str(raw)})
        submitted_total = 0
        missing_total = 0
        absent_total = 0
        for student in roster:
            key = _classops_name_key(student["name"])
            if key in absent:
                absent_total += 1
                student["absent_count"] += 1
                student["catchup_count"] += 1
                student_events[key]["absent"].append(event)
            elif non_submitted:
                if key in non_submitted:
                    missing_total += 1
                    student["missing_count"] += 1
                    student_events[key]["missing"].append(event)
                else:
                    submitted_total += 1
                    student["submitted_count"] += 1
            elif key in submitted:
                submitted_total += 1
                student["submitted_count"] += 1
            else:
                missing_total += 1
                student["missing_count"] += 1
                student_events[key]["missing"].append(event)
        assignment_summaries.append({
            **assignment,
            "roster_count": len(roster),
            "submitted_count": submitted_total,
            "missing_count": missing_total,
            "absent_count": absent_total,
            "timing_context": timing_context,
        })
    for student in roster:
        key = _classops_name_key(student["name"])
        events = student_events.get(key, {"missing": [], "absent": []})
        overdue = 0
        timing_patterns: dict[str, int] = {}
        for event in events.get("missing", []):
            due = _classops_parse_date(event.get("collect_by", ""))
            if due and due < today:
                overdue += 1
            for timing in event.get("timing_context", []):
                timing_key = timing.get("key", "")
                if timing_key:
                    timing_patterns[timing_key] = timing_patterns.get(timing_key, 0) + 1
        reasons = []
        if student["missing_count"] >= 2:
            reasons.append(f"Repeated non-submission across {student['missing_count']} tracked assignments")
        elif student["missing_count"] == 1:
            reasons.append("One open non-submission")
        if overdue:
            reasons.append(f"{overdue} overdue item{'s' if overdue != 1 else ''}")
        if student["catchup_count"] >= 1:
            reasons.append(f"{student['catchup_count']} absence catch-up item{'s' if student['catchup_count'] != 1 else ''}")
        if timing_patterns.get("after_weekend", 0) >= 2:
            reasons.append("Pattern appears after weekends")
        if timing_patterns.get("after_public_holiday", 0) >= 1:
            reasons.append("Watch after school/public holiday")
        student["timing_patterns"] = timing_patterns
        student["risk_reasons"] = reasons
        student["risk_score"] = student["missing_count"] * 3 + overdue * 2 + student["catchup_count"]
        if student["missing_count"] >= 2:
            student["status"] = "follow up"
        elif student["catchup_count"] >= 1:
            student["status"] = "catch up"
        elif student["missing_count"] == 1:
            student["status"] = "watch"
    concerns = [student for student in roster if student["status"] != "clear"]
    insights = _classops_build_insights(class_name, roster, assignments, today)
    return {
        "class_name": class_name,
        "roster_count": len(roster),
        "assignment_count": len(assignments),
        "concern_count": len(concerns),
        "insight_count": len(insights),
        "students": roster,
        "concerns": concerns,
        "assignments": assignment_summaries[-12:],
        "insights": insights,
        "unmatched": unmatched,
    }


def _classops_enrich_with_students(manifest: dict) -> dict:
    ledger = bot.gs.get_classops_ledger()
    _classops_apply_content_overrides(manifest, ledger)
    student_errors = {}
    for class_item in manifest.get("classes", []) or []:
        class_name = str(class_item.get("class") or "").strip()
        if not class_name:
            continue
        try:
            students = bot.gs.get_classops_students(class_name)
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
            }
        else:
            normalised[key] = {"title": str(value or "").strip(), "hidden": False}
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
            filtered.append(next_item)
        class_item["content_items"] = filtered
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


@app.post("/api/classops/dropbox/scan")
def classops_dropbox_scan(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    if not dropbox.configured():
        raise HTTPException(status_code=400, detail="Dropbox ClassOps env vars are not configured.")
    try:
        return dropbox.scan_classops_manifest()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Dropbox ClassOps scan failed: {exc}") from exc


@app.get("/api/classops/dashboard")
def classops_dashboard(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    if not dropbox.configured():
        raise HTTPException(status_code=400, detail="Dropbox ClassOps env vars are not configured.")
    try:
        return _classops_enrich_with_students(dropbox.scan_classops_manifest())
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps dashboard unavailable: {exc}") from exc


@app.get("/api/classops/students")
def classops_students(class_name: str, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        students = bot.gs.get_classops_students(class_name)
        return {
            "ok": True,
            "class_name": class_name,
            "students": students,
            "report": _classops_student_report(class_name, students),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps student list unavailable: {exc}") from exc


@app.post("/api/classops/assignment")
def classops_assignment(req: ClassOpsAssignmentRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        assignment = bot.gs.save_classops_assignment(
            class_name=req.class_name,
            lesson_date=req.lesson_date,
            topic=req.topic,
            folder=req.folder,
            assignment_title=req.assignment_title,
            collect_by=req.collect_by,
            absent=req.absent or [],
            submitted=req.submitted or [],
            non_submitted=req.non_submitted or [],
            notes=req.notes,
        )
        students = bot.gs.get_classops_students(req.class_name)
        return {
            "ok": True,
            "assignment": assignment,
            "report": _classops_student_report(req.class_name, students),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps assignment save failed: {exc}") from exc


@app.post("/api/classops/content-override")
def classops_content_override(req: ClassOpsContentOverrideRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        override = bot.gs.save_classops_content_override(req.path, title=req.title, hidden=req.hidden)
        return {"ok": True, "override": override}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ClassOps content update failed: {exc}") from exc


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
            if task_match:
                ok, synced_marking = bot.complete_reminder_by_id(task_match.group(1))
                result = {"completed": bool(ok), "synced_marking": synced_marking}
            elif followup_match:
                ok = bot.gs.complete_followup(followup_match.group(1))
                result = {"completed": bool(ok)}
            elif checkin_match:
                ok = bot.gs.complete_checkin_today(checkin_match.group(1))
                result = {"completed": bool(ok)}
            else:
                result = {"completed": False, "reason": "No linked task, follow-up, or check-in"}
            bot._record_notification_outcome(
                "done",
                notification_id=item.get("id", ""),
                source=source,
                kind=kind,
                client_id=client_key,
                title=title,
            )
            bot.gs.archive_app_notifications([req.id])
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
    except Exception as exc:
        bot.logger.warning("PWA Gmail fetch failed for account=%s query=%r: %s", account, req.query, exc)
        raise _gmail_http_error(exc, account) from exc
    return {"account": account, "messages": messages}


@app.post("/api/gmail/draft")
def gmail_draft(req: DraftRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    account = bot._normalise_gmail_account(req.account)
    if not bot.gs.gmail_ok(account):
        raise HTTPException(status_code=400, detail=f"{bot.gs.gmail_label(account).title()} is not connected")
    try:
        draft = bot.gs.create_gmail_draft(req.to, req.subject, req.body, req.cc, account=account)
    except Exception as exc:
        bot.logger.warning("PWA Gmail draft failed for account=%s to=%r: %s", account, req.to, exc)
        raise _gmail_http_error(exc, account) from exc
    return {"account": account, "draft_id": draft.get("id", "")}


@app.post("/api/upload")
async def upload_document(
    request: Request,
    file: UploadFile = File(...),
    note: str = "",
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
    if file_size and file_size > max_bytes:
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
    note: str = "",
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
    if file_size and file_size > max_bytes:
        label = "Document" if is_document else "Upload"
        raise HTTPException(status_code=413, detail=f"{label} is too large. Limit is {max_bytes // (1024 * 1024)} MB.")
    job_id = uuid.uuid4().hex
    tmp_path, total = await _spool_upload_to_temp(file, max_bytes)
    _set_upload_job(job_id, {
        "status": "queued",
        "filename": filename,
        "mime": mime,
        "note": note,
        "size": total,
    })
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
def get_upload_job(job_id: str, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return _upload_job_public(_get_upload_job(job_id) or {"job_id": job_id, "status": "missing"})


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

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(data) > max_bytes:
        raise HTTPException(status_code=413, detail=f"Upload is too large. Limit is {max_bytes // (1024 * 1024)} MB.")

    if mime.startswith("image/") or filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
        encoded = base64.b64encode(data).decode()
        reply_text = await bot._run_agentic_claude(
            [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": mime or "image/png", "data": encoded}},
                {"type": "text", "text": f"{bot.MEDIA_SCHEDULE_INSTRUCTION}\n\nUser note: {note or 'Extract useful schedule items, actions, dates, and reminders from this image.'}"}
            ]}],
            max_tokens=2200,
            tools=[bot.CONTEXT_TOOL, bot.CALENDAR_TOOL, bot.REMINDER_TOOL, bot.MEMORY_TOOL],
        )
        return {"reply": reply_text, "index": f"Image analysed: {filename or 'uploaded image'}"}

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
            reply_text = await bot._run_agentic_claude(
                [{"role": "user", "content": text}],
                max_tokens=1600,
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
            encoded = base64.b64encode(fh.read()).decode()
        reply_text = await bot._run_agentic_claude(
            [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": mime or "image/png", "data": encoded}},
                {"type": "text", "text": f"{bot.MEDIA_SCHEDULE_INSTRUCTION}\n\nUser note: {note or 'Extract useful schedule items, actions, dates, and reminders from this image.'}"}
            ]}],
            max_tokens=2200,
            tools=[bot.CONTEXT_TOOL, bot.CALENDAR_TOOL, bot.REMINDER_TOOL, bot.MEMORY_TOOL],
        )
        return {"reply": reply_text, "index": f"Image analysed: {filename or 'uploaded image'}"}

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
            reply_text = await bot._run_agentic_claude(
                [{"role": "user", "content": text}],
                max_tokens=1600,
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
    reply_text = await bot._run_agentic_claude(
        [{"role": "user", "content": prompt}],
        max_tokens=2500,
        tools=[bot.CONTEXT_TOOL, bot.CALENDAR_TOOL, bot.REMINDER_TOOL, bot.MEMORY_TOOL],
    )
    return {"reply": reply_text, "index": index_note}
