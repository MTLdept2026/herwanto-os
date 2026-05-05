from __future__ import annotations

import base64
import asyncio
import gc
import json
import os
import secrets
import tempfile
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
import ipaddress
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import JSONResponse

import bot
import document_service as docs


APP_DIR = Path(__file__).resolve().parent
PWA_DIR = APP_DIR / "pwa"

app = FastAPI(title="H.I.R.A OS")
app.mount("/static", StaticFiles(directory=str(PWA_DIR)), name="static")

try:
    _HOME_EXECUTOR_WORKERS = int(os.environ.get("HIRA_HOME_WORKERS", "4"))
except ValueError:
    _HOME_EXECUTOR_WORKERS = 4
_HOME_EXECUTOR_WORKERS = max(1, min(4, _HOME_EXECUTOR_WORKERS))
_HOME_EXECUTOR = ThreadPoolExecutor(max_workers=_HOME_EXECUTOR_WORKERS)
_WEB_SCHEDULER_TASKS: list[asyncio.Task] = []
_WEB_MEMORY_WATCHDOG_TASK: asyncio.Task | None = None


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
_STATIC_PATHS = {
    "/",
    "/growth",
    "/hira-growth",
    "/healthz",
    "/manifest.webmanifest",
    "/service-worker.js",
    "/app.js",
    "/styles.css",
    "/hira-growth.css",
    "/hira-growth.js",
    "/hira-growth-data.json",
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
        status = 401
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
        "/service-worker.js",
        "/app.js",
        "/styles.css",
        "/hira-growth.css",
        "/hira-growth.js",
        "/hira-growth-data.json",
        "/static/app.js",
        "/static/styles.css",
        "/static/hira-growth.css",
        "/static/hira-growth.js",
        "/static/hira-growth-data.json",
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


@app.on_event("startup")
async def start_web_scheduler():
    global _WEB_SCHEDULER_TASKS, _WEB_MEMORY_WATCHDOG_TASK, _UPLOAD_QUEUE, _UPLOAD_QUEUE_TASKS
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
    global _WEB_SCHEDULER_TASKS, _WEB_MEMORY_WATCHDOG_TASK, _UPLOAD_QUEUE_TASKS
    for task in _WEB_SCHEDULER_TASKS:
        task.cancel()
    _WEB_SCHEDULER_TASKS = []
    for task in _UPLOAD_QUEUE_TASKS:
        task.cancel()
    _UPLOAD_QUEUE_TASKS = []
    if _WEB_MEMORY_WATCHDOG_TASK is not None:
        _WEB_MEMORY_WATCHDOG_TASK.cancel()
        _WEB_MEMORY_WATCHDOG_TASK = None


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


class PushSubscribeRequest(BaseModel):
    subscription: dict


class InsightFeedbackRequest(BaseModel):
    kind: str = "insight"
    target: str
    rating: str
    note: str = ""


class TasteProfileRequest(BaseModel):
    answers: dict


_UPLOAD_JOBS: OrderedDict[str, dict] = OrderedDict()
_MAX_LOCAL_UPLOAD_JOBS = _env_int("HIRA_WEB_MAX_LOCAL_UPLOAD_JOBS", 100)


def _history_key(client_id: str | None) -> str:
    clean = (client_id or "").strip()
    return f"pwa:{clean}" if clean else "pwa"


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


def _parallel_home_data(days: int) -> dict:
    jobs = {
        "agenda": lambda: bot.build_agenda(days),
        "daily_load": lambda: bot.build_daily_load(days),
        "digest": bot.build_curated_digest_snapshot,
        "proactive": lambda: bot.build_proactive_v2_snapshot(days=days),
        "tasks": lambda: bot.build_task_brief(days),
        "islamic": lambda: bot.build_islamic_brief(),
        "files": bot.build_files_index,
        "services": _service_status,
        "marking": _marking_summary,
    }
    fallbacks = {
        "agenda": "Agenda unavailable right now.",
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
        "islamic": "Islamic rhythm unavailable right now.",
        "files": "File memory unavailable until Google is connected.",
        "services": {
            "google": False,
            "calendar": False,
            "work_drive": False,
            "personal_gmail": False,
        },
        "marking": {
            "active_stacks": 0,
            "total_scripts": 0,
            "marked_scripts": 0,
            "unmarked_scripts": 0,
            "connected": False,
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
    return results


def _service_status() -> dict:
    return {
        "google": bot.google_ok(),
        "calendar": bot.google_ok(),
        "work_drive": bot.google_ok(),
        "personal_gmail": bot.gs.gmail_ok("personal"),
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
    bot.absorb_taste_hint(message)
    if bot.is_removed_work_gmail_request(message):
        reply = bot.WORK_GMAIL_REMOVED_MESSAGE
        history.append({"role": "user", "content": message})
        history.append({"role": "assistant", "content": reply})
        bot.save_history(history_key, history[-bot.MAX_TURNS:])

        async def removed_work_gmail_events():
            def sse(payload: dict) -> str:
                return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            try:
                yield sse({"type": "route", "name": "quick"})
                yield sse({"type": "text", "text": reply})
                yield sse({"type": "done", "text": reply})
                yield sse({"type": "saved"})
            finally:
                _CHAT_SEMAPHORE.release()

        return StreamingResponse(removed_work_gmail_events(), media_type="text/event-stream")
    user_content = message
    if bot.re.search(r"\bpersonal\s+(?:gmail|email|emails|mail)\b", message, bot.re.I):
        account_hint, _ = bot._extract_gmail_account_from_text(message)
        user_content = f"{message}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
    user_content = f"{user_content}{bot.intent_lens_hint(message)}{bot.source_discipline_hint(message)}"
    location_context = _device_location_context(location)
    if location_context:
        user_content = f"{user_content}{location_context}"
    history.append({"role": "user", "content": user_content})
    history = history[-bot.MAX_TURNS:]

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


@app.get("/api/notifications/health")
def notifications_health(
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
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
    client_key = _client_key(x_hira_client)
    current_subscription = bot.gs.get_web_push_subscription(client_key)
    delivery_log = bot.gs.get_web_push_delivery_log()
    outcome_summary = bot.gs.get_notification_outcome_summary(days=14)
    stale_threshold = datetime.now(bot.SGT) - bot.timedelta(days=30)
    stale_subscriptions = 0
    for item in subscriptions:
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
        "stale_subscription_count": stale_subscriptions,
        "subscription_error": subscription_error,
        "queued_notification_count": len(queued),
        "queue_error": queue_error,
        "current_client_id": client_key,
        "current_client_subscribed": bool(current_subscription),
        "current_client_last_seen": current_subscription.get("last_seen", "") if current_subscription else "",
        "recent_delivery_log": delivery_log[-5:],
        "outcome_actions": outcome_summary.get("actions", {}),
        "prayers": bot.prayer_notification_status(),
    }


@app.get("/api/admin/status")
def admin_status(x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    base = _health_details()
    try:
        runtime = bot.build_runtime_status()
    except Exception as exc:
        runtime = {"error": str(exc)}
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


@app.post("/api/notifications/subscribe")
def notifications_subscribe(
    req: PushSubscribeRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        ok = bot.gs.save_web_push_subscription(_client_key(x_hira_client), req.subscription)
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
    if account != "personal":
        raise HTTPException(status_code=410, detail=bot.WORK_GMAIL_REMOVED_MESSAGE)
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
    if account != "personal":
        raise HTTPException(status_code=410, detail=bot.WORK_GMAIL_REMOVED_MESSAGE)
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
