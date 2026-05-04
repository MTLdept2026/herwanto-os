from __future__ import annotations

import os
import io
import asyncio
import gc
import json
import base64
import logging
import re
import resource
import tempfile
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta, time as dt_time, date
from difflib import SequenceMatcher
import pytz

from anthropic import Anthropic, AsyncAnthropic

import google_services as gs
import timetable as tt
import search_service as ss
import sports_service as sports
import weather_service as ws
import artifact_service as artifacts
import pdf_service as pdfs
import document_service as docs
import islamic_service as isl

# ─── SETUP ───────────────────────────────────────────────────────────────────

SGT = pytz.timezone("Asia/Singapore")
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)

def _daily_time_from_env(key: str, default_hour: int, default_minute: int = 0) -> tuple[int, int]:
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default_hour, default_minute
    try:
        hour_text, minute_text = raw.replace(".", ":").split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass
    logger.warning(f"Invalid {key}={raw!r}; using {default_hour:02d}:{default_minute:02d} SGT")
    return default_hour, default_minute


MORNING_BRIEFING_TIME = _daily_time_from_env("HIRA_MORNING_BRIEFING_TIME", 6, 45)
EVENING_BRIEFING_TIME = _daily_time_from_env("HIRA_EVENING_BRIEFING_TIME", 21, 0)
try:
    DAILY_JOB_GRACE_MINUTES = max(1, int(os.environ.get("HIRA_DAILY_JOB_GRACE_MINUTES", "20") or 20))
except ValueError:
    logger.warning("Invalid HIRA_DAILY_JOB_GRACE_MINUTES; using 20 minutes")
    DAILY_JOB_GRACE_MINUTES = 20

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
if not ANTHROPIC_API_KEY:
    logger.warning("ANTHROPIC_API_KEY is not set; Claude calls will fail until it is configured.")
claude = Anthropic(api_key=ANTHROPIC_API_KEY or "missing-key")
async_claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY or "missing-key")
_SYSTEM_PROMPT_CACHE = {"key": None, "value": None}

# ─── TELEGRAM USER AUTHORIZATION ─────────────────────────────────────────────
_raw_allowed = os.environ.get("HIRA_ALLOWED_USER_IDS", "").strip()
TELEGRAM_OPEN_DEV_MODE = os.environ.get("HIRA_TELEGRAM_OPEN_DEV_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
ALLOWED_TELEGRAM_USER_IDS: set[int] = set()
if _raw_allowed:
    for _uid in _raw_allowed.split(","):
        _uid = _uid.strip()
        if _uid.isdigit():
            ALLOWED_TELEGRAM_USER_IDS.add(int(_uid))
if not ALLOWED_TELEGRAM_USER_IDS and TELEGRAM_OPEN_DEV_MODE:
    logger.warning(
        "HIRA_TELEGRAM_OPEN_DEV_MODE is enabled and HIRA_ALLOWED_USER_IDS is not set — "
        "the Telegram bot accepts messages from ANY user."
    )
elif not ALLOWED_TELEGRAM_USER_IDS:
    logger.error("HIRA_ALLOWED_USER_IDS is not set; Telegram bot startup will fail closed.")

async def _is_authorized(update) -> bool:
    """Return True if the sender is an allowed user, or explicit open dev mode is enabled."""
    if not ALLOWED_TELEGRAM_USER_IDS:
        return TELEGRAM_OPEN_DEV_MODE
    user = update.effective_user
    if user and user.id in ALLOWED_TELEGRAM_USER_IDS:
        return True
    logger.warning(f"Unauthorized Telegram access attempt from user_id={user.id if user else 'unknown'}")
    try:
        await update.message.reply_text("Unauthorized.")
    except Exception:
        pass
    return False
AGENTIC_MODEL = os.environ.get("HIRA_AGENTIC_MODEL", "claude-sonnet-4-6").strip() or "claude-sonnet-4-6"
DEEP_MODEL = os.environ.get("HIRA_DEEP_MODEL", AGENTIC_MODEL).strip() or AGENTIC_MODEL
QUICK_MODEL = os.environ.get("HIRA_QUICK_MODEL", "claude-haiku-4-5-20251001").strip() or "claude-haiku-4-5-20251001"
ROUTER_MODEL = os.environ.get("HIRA_ROUTER_MODEL", QUICK_MODEL).strip() or QUICK_MODEL
STRUCTURED_MODEL = os.environ.get("HIRA_STRUCTURED_MODEL", QUICK_MODEL).strip() or QUICK_MODEL

WORK_DRIVE_REFERENCES = [
    (
        "Work Google Drive: 2026 MTL folder "
        "(https://drive.google.com/drive/folders/1VPlciwlR1_wVsUyr4_KovCfx6xVbLkJC) "
        "is Herwanto's main 2026 Mother Tongue Languages work folder. "
        "Top-level areas include Oral Exam 2026, 2025/2026 GCE/Internal Exam Result Analysis, "
        "2026 MTL Intensive Programme, 2026 Work Review, Prize Presentation, "
        "National Exam Deployment, MTL Classlist by Teachers, MTL Calendar, "
        "Non-MTL/Exempted/Third Language Matters, MTL PD, MTL Meeting & PLT, "
        "MTL Workplan, Hari Raya Celebration, MTF & Cultural Camp, Assessment Matters, "
        "SOW & Resources, CNY Celebration, and Teachers Deployment."
    ),
    (
        "Important MTL Drive folder: 2026 ASSESSMENT MATTERS "
        "(https://drive.google.com/drive/folders/11sCM0e2FMwX159ru6dbFxM-Xt_S6hKTT). "
        "Use this for assessment planning, exam/WA overview, rubrics, and assessment-related work. "
        "Known contents include 2026 ASSESSMENT PLAN _MTL OVERVIEW.xlsx and "
        "2026 ML&TL AA PROJECT SEC 3."
    ),
    (
        "Important MTL Drive folder: 2026 MTL MEETING & PLT "
        "(https://drive.google.com/drive/folders/1i7XO_ZVk4rEuHITs98tMt0cubjWO41qz). "
        "Use this for department meetings, PLT decks, WA1 result discussions, and ML/TL PLT project work. "
        "Known contents include 2026 ML&TL PLT Project, PLT decks #01-#10, "
        "2026 MTL RESULTS - WA1, TAKE-AWAY FORM MTL SHARING, and the 2026/2025 MTL Dept Meeting deck."
    )
]

# ─── REDIS MEMORY (falls back to in-memory if Redis not configured) ──────────

_redis = None

def _get_redis():
    global _redis
    if _redis is None:
        url = os.environ.get("REDIS_URL")
        if url:
            try:
                import redis
                _redis = redis.from_url(url, decode_responses=True)
                _redis.ping()
                logger.info("Redis connected")
            except Exception as e:
                logger.warning(f"Redis unavailable: {e}")
                _redis = False
    return _redis if _redis else None

def redis_required() -> bool:
    return os.environ.get("HIRA_REQUIRE_REDIS", "").strip().lower() in {"1", "true", "yes", "on"}

def require_redis_for_service(service: str) -> bool:
    if not redis_required():
        return True
    if _get_redis() is not None:
        return True
    message = f"Redis is required for {service}; set REDIS_URL or disable HIRA_REQUIRE_REDIS."
    logger.error(message)
    return False


def _message_text_for_routing(messages: list[dict]) -> str:
    if not messages:
        return ""
    content = messages[-1].get("content", "") if isinstance(messages[-1], dict) else ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
        return " ".join(parts)
    return ""


def _agentic_model_for_messages(messages: list[dict]) -> str:
    text = _message_text_for_routing(messages).lower()
    if DEEP_MODEL != AGENTIC_MODEL and re.search(
        r"\b(code|debug|architecture|refactor|proposal|strategy|analyse|analyze|"
        r"document|pdf|slides?|deck|lesson plan|rubric|business|contract|legal|"
        r"medical|religious ruling|fatwa|research|compare|decision)\b",
        text,
    ):
        return DEEP_MODEL
    return AGENTIC_MODEL


_mem_histories = OrderedDict()
MAX_TURNS = 20
try:
    MAX_IN_MEMORY_HISTORIES = int(os.environ.get("HIRA_MAX_IN_MEMORY_HISTORIES", "100"))
except ValueError:
    MAX_IN_MEMORY_HISTORIES = 100
_BREAK_AWARE_SLOT_CACHE = {}
_PRAYER_PROMPT_FALLBACK_KEYS = set()
_LAST_MEMORY_LOG = None
_FORGET_CONFIRM_PENDING: dict[int, datetime] = {}  # user_id → timestamp when /forget all was first requested
_FORGET_CONFIRM_TTL_SECONDS = 60

def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default))))
    except ValueError:
        return default

JOB_INTERVALS = {
    "proactive_nudges": _env_int("HIRA_PROACTIVE_NUDGE_INTERVAL", 60, 30),
    "proactive_intelligence": _env_int("HIRA_PROACTIVE_INTELLIGENCE_INTERVAL", 21600, 1800),
    "daily_checkins": _env_int("HIRA_DAILY_CHECKIN_INTERVAL", 300, 60),
    "followups": _env_int("HIRA_FOLLOWUP_INTERVAL", 3600, 300),
}
MEMORY_DISPLAY_CATEGORIES = (
    "profile",
    "preferences",
    "people",
    "places",
    "teaching",
    "business",
    "projects",
    "sports",
    "files",
    "templates",
    "constraints",
    "recent_summaries",
    "topic_profiles",
    "correction_ledger",
    "self_reflections",
    "source_notes",
)

def get_history(user_id):
    r = _get_redis()
    if r:
        data = r.get(f"hist:{user_id}")
        return json.loads(data) if data else []
    key = str(user_id)
    history = _mem_histories.get(key, [])
    if key in _mem_histories:
        _mem_histories.move_to_end(key)
    return list(history)

def save_history(user_id, history):
    r = _get_redis()
    if r:
        r.setex(f"hist:{user_id}", 86400 * 7, json.dumps(history))
    else:
        key = str(user_id)
        _mem_histories[key] = history[-MAX_TURNS:]
        _mem_histories.move_to_end(key)
        while len(_mem_histories) > MAX_IN_MEMORY_HISTORIES:
            _mem_histories.popitem(last=False)


def build_runtime_status() -> dict:
    limit = _memory_limit_mb()
    rss = _rss_mb()
    redis = _get_redis()
    memory_counts = {}
    project_count = None
    notification_count = None
    subscription_count = None
    outcome_summary = {}
    delivery_log = []
    memory_error = ""
    google_connected = google_ok()
    def safe_config(key: str) -> str:
        if not google_connected:
            return ""
        try:
            return gs.get_config(key) or ""
        except Exception:
            return ""

    if google_connected:
        try:
            memory = gs.get_memory()
            memory_counts = {category: len(memory.get(category, [])) for category in MEMORY_DISPLAY_CATEGORIES}
        except Exception as exc:
            memory_error = str(exc)
        try:
            project_count = len(gs.get_projects())
        except Exception:
            project_count = None
        try:
            notification_count = len(gs.get_app_notifications(include_archived=False))
        except Exception:
            notification_count = None
        try:
            subscription_count = len(gs.get_web_push_subscriptions())
        except Exception:
            subscription_count = None
        try:
            outcome_summary = gs.get_notification_outcome_summary(days=14).get("actions", {})
        except Exception:
            outcome_summary = {}
        try:
            delivery_log = gs.get_web_push_delivery_log()[-5:]
        except Exception:
            delivery_log = []
    return {
        "memory": {
            "rss_mb": round(rss, 1),
            "limit_mb": round(limit, 1) if limit else None,
            "ratio": round(rss / limit, 3) if limit else None,
            "in_memory_histories": len(_mem_histories),
            "max_in_memory_histories": MAX_IN_MEMORY_HISTORIES,
            "break_slot_cache": len(_BREAK_AWARE_SLOT_CACHE),
        },
        "integrations": {
            "redis_connected": redis is not None,
            "redis_required": redis_required(),
            "google_connected": google_connected,
            "personal_gmail": gs.gmail_ok("personal"),
            "work_gmail": gs.gmail_ok("work"),
            "search_enabled": ss.search_enabled(),
        },
        "models": {
            "agentic": AGENTIC_MODEL,
            "deep": DEEP_MODEL,
            "quick": QUICK_MODEL,
            "router": ROUTER_MODEL,
            "structured": STRUCTURED_MODEL,
        },
        "memory_buckets": memory_counts,
        "memory_error": memory_error,
        "projects": {"count": project_count},
        "notifications": {
            "queued_count": notification_count,
            "subscription_count": subscription_count,
            "outcome_actions": outcome_summary,
            "recent_delivery_log": delivery_log,
        },
        "jobs": {
            "intervals_seconds": dict(JOB_INTERVALS),
            "last_morning_briefing": safe_config(MORNING_BRIEFING_SENT_KEY),
            "last_evening_briefing": safe_config(EVENING_BRIEFING_SENT_KEY),
        },
    }


VOLATILE_FACT_PATTERN = re.compile(
    r"\b("
    r"latest|current|today|tomorrow|now|live|result|results|score|scores|standings|table|"
    r"lineup|line-up|starting xi|fixture|fixtures|schedule|injury|injuries|transfer|rumou?r|"
    r"price|prices|cost|weather|forecast|prayer|khutbah|news|headline|headlines|"
    r"f1|formula 1|liverpool|lfc|epl|premier league"
    r")\b",
    re.I,
)

def source_discipline_for_text(text: str) -> dict:
    clean = " ".join((text or "").split())
    lowered = clean.lower()
    if not clean:
        return {"confidence": "unknown", "needs_live_check": False, "reason": "empty input", "recommended_tools": []}

    tools: list[str] = []
    if re.search(r"https?://\S+", clean):
        tools.append("fetch_url")
    if re.search(r"\b(liverpool|lfc|epl|premier league|anfield|salah|wirtz|isak|transfer|rumou?r|lineup|starting xi)\b", lowered):
        tools.append("get_liverpool_brief")
    if re.search(r"\b(f1|formula 1|grand prix|mercedes|russell|antonelli|hamilton|qualifying|driver standings|constructor standings)\b", lowered):
        tools.append("get_f1_brief")
    if re.search(r"\b(weather|forecast|rain|temperature|haze|psi|pm2\.5)\b", lowered):
        tools.append("get_nea_weather")
    if re.search(r"\b(prayer|solat|salah|subuh|fajr|zohor|asar|maghrib|isyak|khutbah|sermon)\b", lowered):
        tools.extend(["get_muis_prayer_times", "get_muis_friday_khutbah"])
    if VOLATILE_FACT_PATTERN.search(clean):
        tools.append("get_latest_news")
    if re.search(r"\b(search|web|website|article|page|latest|current|news|headline|rumou?r)\b", lowered):
        tools.append("web_search")

    deduped_tools = list(dict.fromkeys(tools))
    needs_live = bool(deduped_tools) or bool(VOLATILE_FACT_PATTERN.search(clean))
    confidence = "needs_live_source" if needs_live else "memory_ok"
    reason = (
        "Question appears time-sensitive, source-sensitive, or about a volatile fact."
        if needs_live else
        "Question appears stable enough for memory or general reasoning."
    )
    return {
        "confidence": confidence,
        "needs_live_check": needs_live,
        "reason": reason,
        "recommended_tools": deduped_tools,
    }

def source_discipline_hint(text: str) -> str:
    discipline = source_discipline_for_text(text)
    if not discipline["needs_live_check"]:
        return ""
    tools = ", ".join(discipline["recommended_tools"]) or "available source tools"
    return (
        "\n\n[Source discipline: confidence=needs_live_source. "
        f"Reason: {discipline['reason']} Recommended tools: {tools}. "
        "Do not rely only on memory; cite or summarise source-backed findings and label rumours/live facts clearly.]"
    )

def build_memory_review(limit: int = 5) -> dict:
    limit = max(1, min(int(limit or 5), 20))
    if not google_ok():
        return {"ok": False, "error": "Google memory is not connected.", "buckets": {}}
    memory = gs.get_memory()
    buckets = {}
    total = 0
    for category in MEMORY_DISPLAY_CATEGORIES:
        items = list(memory.get(category, []))
        total += len(items)
        buckets[category] = {
            "count": len(items),
            "recent": items[-limit:],
            "needs_review": len(items) > 80 or category in {"correction_ledger", "self_reflections", "source_notes"} and len(items) > 40,
        }
    return {
        "ok": True,
        "total_items": total,
        "bucket_count": len(buckets),
        "buckets": buckets,
        "review_rules": [
            "Promote repeated corrections into stable preferences or constraints.",
            "Keep source_notes marked live_check as research leads, not permanent truth.",
            "Prune duplicate file/source notes when a newer summary supersedes them.",
        ],
    }


def _memory_item_text(item) -> str:
    if isinstance(item, dict):
        parts = []
        for key in ("correction", "learned", "next_behavior", "topic", "insight", "summary", "text", "stable_context", "preferred_angle"):
            value = item.get(key)
            if value:
                parts.append(str(value))
        if not parts:
            parts = [json.dumps(item, ensure_ascii=False)]
        return " | ".join(parts)
    return str(item)


def _intent_tokens(text: str) -> set[str]:
    stop = {
        "the", "and", "for", "you", "your", "what", "when", "where", "how", "can", "could",
        "would", "should", "this", "that", "with", "from", "have", "need", "want", "like",
        "hira", "please", "pls", "about", "into", "just", "really", "today", "tomorrow",
    }
    return {
        token for token in re.findall(r"[a-z0-9][a-z0-9_.@-]*", (text or "").lower())
        if len(token) > 2 and token not in stop
    }


def retrieve_relevant_memory(text: str, limit: int = 6) -> list[dict]:
    if not google_ok():
        return []
    tokens = _intent_tokens(text)
    if not tokens and len((text or "").strip()) < 8:
        return []
    try:
        memory = gs.get_memory()
    except Exception as exc:
        logger.debug(f"Relevant memory retrieval failed: {exc}")
        return []

    priority = {
        "correction_ledger": 12,
        "constraints": 10,
        "self_reflections": 8,
        "preferences": 7,
        "topic_profiles": 7,
        "recent_summaries": 5,
        "projects": 5,
        "teaching": 4,
        "business": 4,
        "sports": 4,
        "source_notes": 4,
        "people": 3,
        "places": 2,
        "files": 1,
        "templates": 1,
        "profile": 1,
    }
    scored = []
    for category in MEMORY_DISPLAY_CATEGORIES:
        items = list(memory.get(category, []))
        for recency, item in enumerate(items[-60:]):
            rendered = _clip_memory_text(_memory_item_text(item), 420)
            lower = rendered.lower()
            overlap = sum(1 for token in tokens if token in lower)
            intent_bonus = 0
            if category in {"correction_ledger", "constraints", "self_reflections", "preferences"} and re.search(
                r"\b(prefer|remember|correct|wrong|mistake|style|upgrade|improve|mean|meant|context)\b",
                text or "",
                re.I,
            ):
                intent_bonus = 4
            if category in {"topic_profiles", "sports", "source_notes"} and re.search(
                r"\b(latest|current|track|follow|interest|liverpool|lfc|f1|news|rumou?r)\b",
                text or "",
                re.I,
            ):
                intent_bonus = 4
            score = priority.get(category, 0) + (overlap * 5) + intent_bonus + min(3, recency // 12)
            if score >= 8 and (overlap or intent_bonus or category in {"constraints", "correction_ledger"}):
                scored.append({
                    "category": category,
                    "text": rendered,
                    "score": score,
                })
    scored.sort(key=lambda item: (item["score"], item["category"]), reverse=True)
    return scored[: max(1, min(int(limit or 6), 12))]


def infer_intent_lens(text: str) -> dict:
    clean = " ".join((text or "").split())
    lower = clean.lower()
    if re.search(r"\b(class|lesson|student|marking|school|mtl|nbss|worksheet|assessment)\b", lower):
        hat = "educator"
    elif re.search(r"\b(code|debug|app|ios|android|react|vite|capacitor|deploy|github|api)\b", lower):
        hat = "developer"
    elif re.search(r"\b(gameplan|ruh|rūḥ|business|client|pricing|market|product|pitch)\b", lower):
        hat = "entrepreneur"
    else:
        hat = "personal"

    if re.search(r"\b(what do i do|priority|prioritise|prioritize|overwhelmed|packed|busy|workload|focus)\b", lower):
        likely = "prioritise the next practical move"
    elif re.search(r"\b(upgrade|improve|better|review|architecture|self)\b", lower):
        likely = "improve H.I.R.A's own behaviour or architecture"
    elif re.search(r"\b(latest|current|news|rumou?r|today|now)\b", lower):
        likely = "answer with live-source discipline"
    elif re.search(r"\b(remember|prefer|style|mean|meant|correction|wrong)\b", lower):
        likely = "apply or store durable personal context"
    else:
        likely = "answer the direct request while preserving context"

    return {
        "hat": hat,
        "likely_intent": likely,
        "stance": "be concise, use relevant memory, and distinguish literal wording from likely intent",
    }


def intent_lens_hint(text: str) -> str:
    lens = infer_intent_lens(text)
    memories = retrieve_relevant_memory(text, limit=5)
    lines = [
        "\n\n[Intent lens:",
        f"- Inferred hat: {lens['hat']}.",
        f"- Likely intent: {lens['likely_intent']}.",
        f"- Response stance: {lens['stance']}.",
    ]
    if memories:
        lines.append("- Relevant memory to consider:")
        for item in memories:
            lines.append(f"  - {item['category']}: {item['text']}")
    else:
        lines.append("- No highly relevant stored memory found; do not pretend otherwise.")
    lines.append("Use this lens quietly. Do not mention it unless Herwanto asks how you reasoned.]")
    return "\n".join(lines)

def _rss_mb() -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB, macOS reports bytes.
    if rss > 10_000_000:
        return rss / (1024 * 1024)
    return rss / 1024

def _memory_limit_mb() -> float | None:
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = fh.read().strip()
        except Exception:
            continue
        if not raw or raw == "max":
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if value > 0 and value < 10**15:
            return value / (1024 * 1024)
    return None

def _log_memory(label: str, force: bool = False) -> None:
    global _LAST_MEMORY_LOG
    now = datetime.now(SGT)
    if not force and _LAST_MEMORY_LOG and now - _LAST_MEMORY_LOG < timedelta(minutes=10):
        return
    _LAST_MEMORY_LOG = now
    limit = _memory_limit_mb()
    limit_text = f" / limit {limit:.0f} MB" if limit else ""
    logger.info(
        "Memory %s: rss %.1f MB%s, in-memory histories=%s, break-slot cache=%s",
        label,
        _rss_mb(),
        limit_text,
        len(_mem_histories),
        len(_BREAK_AWARE_SLOT_CACHE),
    )

def _finish_background_job(name: str) -> None:
    gc.collect()
    _log_memory(f"after {name}")


def _acquire_job_lock(name: str, ttl_seconds: int = 120) -> bool:
    r = _get_redis()
    if not r:
        return True
    try:
        return bool(r.set(f"hira:job_lock:{name}", os.uname().nodename, nx=True, ex=ttl_seconds))
    except Exception as exc:
        logger.warning(f"Job lock unavailable for {name}: {exc}")
        return True

# ─── SYSTEM PROMPT ───────────────────────────────────────────────────────────

def SYSTEM_PROMPT():
    now = datetime.now(SGT)
    date_ctx = now.strftime("Today is %A, %-d %B %Y. Current time in Singapore: %H:%M SGT.")
    memory_ctx = (
        "\n\nPersistent school calendar memory:\n" + tt.format_school_calendar_memory() +
        "\n\nPersistent timetable memory:\n" + tt.format_timetable_memory()
    )
    if WORK_DRIVE_REFERENCES:
        memory_ctx += "\n\nKnown work Drive references:\n" + "\n".join(f"- {item}" for item in WORK_DRIVE_REFERENCES)
    if google_ok():
        try:
            memory = gs.get_memory()
            # Priority-weighted limits: high-signal buckets are never clipped to 8.
            # Corrections and constraints are critical — always shown in full (up to cap).
            # Low-signal buckets like files and summaries are trimmed more aggressively.
            MEMORY_PROMPT_LIMITS: dict[str, int | None] = {
                "correction_ledger":  20,   # always show all corrections — HIRA's primary learning signal
                "constraints":        None,  # always show all — immutable rules Herwanto has set
                "self_reflections":   15,   # HIRA's own learning journal
                "preferences":        12,
                "topic_profiles":     10,
                "people":             10,
                "teaching":           10,
                "profile":             8,
                "business":            8,
                "projects":            8,
                "sports":              8,
                "places":              6,
                "source_notes":        6,
                "recent_summaries":    5,
                "templates":           5,
                "files":               4,   # low-priority in prompt; accessible via tool
            }
            memory_lines = []
            for category in MEMORY_DISPLAY_CATEGORIES:
                items = memory.get(category, [])
                if not items:
                    continue
                limit = MEMORY_PROMPT_LIMITS.get(category, 8)
                shown = items if limit is None else items[-limit:]
                prefix = "[HIGH PRIORITY] " if category in ("correction_ledger", "constraints") else ""
                memory_lines.append(f"{prefix}{category.title()}: " + "; ".join(shown))
            if memory_lines:
                memory_ctx += "\n\nStored memory:\n" + "\n".join(memory_lines)
        except Exception:
            pass
        try:
            official_week = tt.get_school_week_info(now.date())
            if official_week:
                week_label = tt.week_type_label(official_week["week_type"])
                memory_ctx += (
                    f"\n\nTimetable reference: this is {week_label} week, "
                    f"{official_week['term']} Week {official_week['week_number']}."
                )
            else:
                ref_date = gs.get_config("week_ref_date")
                ref_type = gs.get_config("week_ref_type")
                if not ref_date or not ref_type:
                    raise ValueError("No timetable reference set")
                wt = tt.get_week_type(ref_date, ref_type, now.date())
                week_label = tt.week_type_label(wt)
                school_week_number = gs.get_config("school_week_number")
                week_number = f", school week {school_week_number}" if school_week_number else ""
                memory_ctx += f"\n\nTimetable reference: this is {week_label} week{week_number}."
        except Exception:
            pass

    return f"""{date_ctx}{memory_ctx}

You are Herwanto's personal AI assistant. Your name is H.I.R.A — Herwanto Interface for Responsive Assistance.
You are Singapore-based, calm under pressure, quick with useful judgment, and quietly warm.
You feel like a capable chief-of-staff in his pocket: practical, observant, wickedly witty when the moment allows, and never needy.

Personality:
- Speak like a trusted colleague who knows his life, not a generic chatbot.
- Default vibe: concise, grounded, encouraging, lightly informal, and sharp without being cruel.
- If he asks your name, answer naturally: "I'm H.I.R.A — Herwanto Interface for Responsive Assistance."
- Be decisive when the path is clear; ask only when a missing detail blocks action.
- Have a wicked sense of humour and good wit: dry, clever, quick, and occasionally cheeky.
- Use humour like seasoning, not gravy. Never force jokes, emojis, hype, or motivational fluff.
- Do not make jokes when the user is upset, dealing with a serious issue, asking for BM accuracy, or needs exact code/business judgement.
- Never be mean-spirited, insulting, crude, or sarcastic at the user's expense. Punch up at chaos, bureaucracy, vague requirements, and bad error messages.
- Protect his attention: summarise, prioritise, and make the next action obvious.
- Notice patterns across school, CCA, projects, deadlines, and personal preferences.
- Use the intent lens and relevant memory context to infer what Herwanto likely means, not only the literal words.
- When he is stressed or overloaded, steady the room first, then give a short practical plan.
- When he is building something, be direct and product-minded.
- When he is teaching, be precise, culturally aware, and DBP-clean for Bahasa Melayu.
- When he is doing business, be commercially honest and Singapore-market aware.
- You may say "I" naturally, but do not pretend to have a human body, private life, or feelings outside the assistant role.

Herwanto wears three hats:

1. EDUCATOR — Bahasa Melayu teacher at Naval Base Secondary School (NBSS). Form teacher of 1 Flagship. Teaches ML to Sec 1, 2, 3, and 4 groups. Runs the school Football CCA. Use DBP conventions for all BM content.
   Current live MTL groups include Sec 1 ML G2 / 1G2, Sec 2 2G3 ML, Sec 3 3G3 ML, and Sec 4 BML/4T BML-style classlists under CG Herwanto.

2. APP DEVELOPER — Solo developer. Stack: React + Vite, Capacitor, Netlify, GitHub, Python. Active projects: GamePlan (sports CCA website service for Singapore schools) and Ruh (Islamic spiritual app, currently in App Store review). When he pastes code, debug immediately without preamble.

3. ENTREPRENEUR — Building GamePlan and Ruh as commercial products. Singapore market focus.

Rules:
- Be concise. No filler, no preamble.
- Natural language is the main interface. Slash commands are shortcuts, not required. If the user asks in plain English/Singlish, infer the intent and use tools directly.
- Prefer doing the requested action or lookup over explaining which command to use. Only mention a slash command if the user asks how to do it manually or the action is blocked.
- After tool results, answer in natural language with a brief useful summary. Do not dump raw tool output unless the user asks for raw output.
- When Herwanto corrects you, repair the answer before reassuring him: first state the corrected fact with source-backed details if the fact is live/current, then apologise briefly, then say the prevention rule. Never respond to a correction with only "noted" or "won't happen again".
- If you say you will check, pull, verify, look up, or get the actual result/details, you must call the relevant tool in that same turn before answering. Do not end with "give me a moment" as the final answer.
- For data lookups, use the user's words as intent: "last 5 emails" means latest 5 Gmail messages; "what's on today" means schedule/context; "anything due" means reminders/tasks; "who do I owe replies/follow-ups to" means Gmail/follow-up/task context as relevant.
- For timetable or lesson lookups, use get_timetable. TIMETABLE in timetable.py is the source of truth for lessons; Google Calendar is only for events/appointments.
- For availability planning ("best slots", "free slots", "when can I schedule", "after school", "not during CCA day"), call find_available_training_slots before suggesting times. Do not suggest a slot until timetable lessons and Google Calendar conflicts have been checked.
- For MTL classlists, student names, scores, marks, WA/weighted assessment, FA/formative assessment, prelim, EOY, assessment columns, progress analysis, or who is in Herwanto's classes, call get_mtl_classlists or analyze_mtl_scores as appropriate. His classlist tabs in the 2026 MTL classlist sheets include CG HERWANTO or CG HERWANTO/CG KADIR.
- Score-sheet layouts: Sec 1 uses WA1 (40) and WA1 %, plus PreWA2 (20), WA2, WA3, EOY. Sec 2 uses WA1, Pra/Prg-WA2 mock/pre-WA columns, then actual WA2, WA3, EOY; Pra/Prg-WA means pre-WA/mock tests, not the actual WA2 result. Sec 3 uses WA1 (20), WA1 %, then WA2, WA3, EOY. Sec 4 FA layout uses component columns and a total followed by %, e.g. FA1 15/30/45/% and FA2 10/25/35/%; compare the % columns for progress unless the user asks for raw marks.
- Infer his hat from context — never ask.
- For code: fix first, explain if needed.
- For BM: proper DBP spelling and grammar always.
- For business: give a direct recommendation.
- Singapore English and local context always apply.
- When he asks to add, schedule, or create a calendar event, create it directly if the date and time are clear. If details are incomplete, ask only for the missing detail.
- When he asks to cancel, remove, delete, or mark a calendar event as done, call delete_calendar_event_by_text with the event description.
- Never offer to generate .ics files. Use Google Calendar directly.
- The current date and time is already provided at the top of this prompt — always use it for any date/time reasoning.
- Never guess weekdays. If you mention a date with a weekday, derive the weekday from the actual calendar date. For 2026, 1 May is Friday, not Thursday.
- For appointments and dated commitments, treat Google Calendar/tool output as authoritative. Copy exact dates from tool results; never shift an appointment by memory, assumption, or weekday inference. Known correction: the HDB appointment is on 6 May, not 7 May, unless a newer calendar result says otherwise.
- Religion, prayers, solat times, Islamic rulings, halal/haram questions, and worship guidance require extra care: verify with credible sources before giving factual claims. For Singapore practice, prefer MUIS or official Singapore mosque/source data. If no credible source/tool result is available, say clearly that H.I.R.A cannot verify it right now.
- Never guess prayer times. For Singapore prayer-time questions, call get_muis_prayer_times and answer from MUIS data. If the tool fails, say H.I.R.A cannot verify the exact time right now; do not invent an approximate time, do not say "around", and do not ask Herwanto to check another app as the primary answer.
- For Friday khutbah/sermon questions, call get_muis_friday_khutbah and answer from MUIS data. Keep it to a practical heads-up: title, date, key message, and what to carry into Jumu'ah.
- Never invent mosque or place locations. If a place location affects the answer and you do not have a verified source/tool result, say what you know and what is unverified. Be especially careful with Singapore masjid names that sound similar.
- Known mosque correction: Masjid Al-Muttaqin is at 5140 Ang Mo Kio Ave 6, Singapore 569844, not Kovan.
- For journey-time estimates, use the current device location context when it is provided. If it is not provided, use only explicit user-provided origin/destination or stable stored memory, and label any estimate as rough.
- You have tools: create_calendar_event, delete_calendar_event_by_text, find_available_training_slots, add_reminder, add_marking_task, update_marking_progress, reset_marking_load, get_marking_brief, create_proactive_nudge, create_daily_checkin, create_break_aware_daily_checkin, create_followup, complete_task_by_text, get_task_brief, get_timetable, get_mtl_classlists, analyze_mtl_scores, update_mtl_class_score, fill_mtl_percentage_scores, get_gmail_brief, create_gmail_draft, create_document_artifact, create_slide_deck_artifact, remember_artifact_template, get_assistant_context, remember_user_info, create_topic_profile, remember_source_insight, update_project_status, get_nea_weather, get_muis_prayer_times, get_muis_friday_khutbah, get_latest_news, get_liverpool_brief, get_f1_brief, web_search, and fetch_url. Use them proactively.
- When the user mentions an event, match, duty, or appointment at a specific time — call create_calendar_event immediately without asking.
- When the user mentions a task, deadline, or something to prepare/submit/complete — call add_reminder immediately without asking.
- When the user mentions marking scripts, papers, compositions, kefahaman, karangan, worksheets, or a marking stack, use marking tools instead of ordinary reminders: add_marking_task for a new stack, update_marking_progress when he says how many scripts are marked, reset_marking_load when he asks to reset/clear the marking load or board, and get_marking_brief when he asks what marking is outstanding. Marking tasks are mission-critical and must persist even at 0 outstanding; only complete one when he explicitly says that marking stack is done, completed, can be closed, reset, or cleared.
- When the user asks you to nudge, ping, check in, remind him at a specific time, or initiate a chat later — call create_proactive_nudge. Use this for time-specific heads-ups, not ordinary all-day deadlines.
- When the user asks for a recurring daily ping/check-in until he replies yes/done — call create_daily_checkin.
- When the user asks for daily reminders/check-ins to adapt around his schedule, breaks, timetable, lessons, or calendar, call create_break_aware_daily_checkin. This is especially appropriate for selawat, salawat, selawat ke atas Nabi, istighfar, zikr/dhikr, and similar habits he wants during free pockets of the day.
- Islamic practice is first-class context: use MUIS Singapore prayer times, Hijri context, fasting windows, and his timetable/calendar to help him protect prayer time. If a prayer enters during a lesson, advise praying as soon as the lesson ends.
- When the user asks about Subuh/Fajr, Syuruk, Zohor/Zuhur/Zuhr/Dhuhr, Asar/Asr, Maghrib, Isyak/Isha, prayer times, solat, salah, or whether there is time to pray, call get_muis_prayer_times before answering.
- When the user asks about khutbah, Friday sermon, Jumu'ah sermon, or what the sermon is about, call get_muis_friday_khutbah before answering.
- For religious advice beyond basic scheduling, be humble and source-aware. Distinguish verified source-backed information from practical planning suggestions, and suggest checking an asatizah/MUIS source for rulings when needed.
- When the user sends a screenshot, image, or PDF, inspect it for schedule items first: duties, appointments, matches, trainings, meetings, event timings, reporting times, deadlines, submissions, or preparation tasks.
- For screenshots/PDFs/images: create calendar events for items with a clear date and time, add reminders for dated tasks/deadlines, then summarise what you added and what still needs clarification.
- Uploaded PDFs/images are saved as file memory after processing. When the user later refers to a previously uploaded file, use Stored memory / Files first; do not ask for a re-upload unless the stored summary lacks the exact detail needed.
- When the user asks about his day, week, workload, priorities, deadlines, or project status — call get_assistant_context before answering.
- When the user asks for his MTL classlists, class names, student names, students in a group, scores, marks, assessment results, or whether a named student is in his class — call get_mtl_classlists before answering. Set include_scores=true for score/mark/result questions.
- When the user asks for score analysis, progress, mean, median, pass rate, underperforming students, strongest students, most improved, or drastic drops — call analyze_mtl_scores. Treat 0 as an attempted paper with zero marks. Treat AB as absent, VR as valid reason, and MC as medical certificate; these status codes are non-scoring and should be excluded from mean/median/pass-rate calculations but counted separately. For Sec 1G2, 2G3, and 3G3 aliases, resolve them to ML G2, 2G3 ML, and 3G3 ML if needed.
- When the user asks to calculate and enter a score/mark/result into an MTL classlist sheet, calculate only from the numbers he gives or sheet values retrieved with include_scores=true, then call update_mtl_class_score. Do not guess a student or column; if the tool reports ambiguity, ask for the missing class/student/column detail.
- When the user asks to fill percentage columns in an MTL classlist, call fill_mtl_percentage_scores. Use class_query and assessment_query if the user provides them; otherwise the tool will ask for specificity when multiple % columns match.
- When the user asks about latest news, current events, headlines, football, Liverpool/LFC, F1, AI, Singapore education, apps, Apple, Nothing OS, or his shortlisted topics — call get_latest_news before answering. Prefer get_liverpool_brief for Liverpool/LFC questions and get_f1_brief for Formula 1 questions because they gather structured source slices.
- For sports corrections and follow-ups, preserve the conversation subject. If Herwanto corrects a Liverpool/F1 claim, or then asks "what was the match like?", "the result?", "recap?", "home or away?", or "actual details?", use the relevant structured sports brief before answering even if the follow-up omits the team name.
- After giving verified Liverpool or F1 scores/match details, add a short supporter-read: infer Herwanto's likely mood from his known loyalties and the result. Keep it humble ("I imagine", "this probably lands as") and concrete, not melodramatic. For Liverpool losses, acknowledge frustration, defensive/selection worries, or rivalry pain before any tactical note. For wins, share the lift and what he will probably enjoy. Do not let mood-reading replace the verified facts.
- Liverpool FC is a first-class interest. Herwanto supports Liverpool. Track the current squad/line-ups, Premier League standing, progress in every competition Liverpool are still in, injuries/suspensions, fixtures/results, and transfer news/rumours. As of the 2025-26 squad context, Liverpool are managed by Arne Slot and the first-team group includes Alisson, Giorgi Mamardashvili, Freddie Woodman, Virgil van Dijk, Ibrahima Konate, Joe Gomez, Milos Kerkez, Conor Bradley, Andy Robertson, Jeremie Frimpong, Giovanni Leoni, Wataru Endo, Florian Wirtz, Dominik Szoboszlai, Alexis Mac Allister, Curtis Jones, Ryan Gravenberch, Trey Nyoni, Alexander Isak, Mohamed Salah, Federico Chiesa, Cody Gakpo, Hugo Ekitike, and Rio Ngumoha. If Herwanto mentions Wirtz, Isak, or "lfc big match", assume Liverpool context and do not correct him back to old clubs without first checking current sources. For current starting XIs, matchday line-ups, EPL table position, points, goal difference, form, Champions League/FA Cup/Carabao Cup progress, injuries, contract situations, departures, signings, or rumours, always use get_latest_news/web_search/fetch_url and cite the source. Clearly label transfer items as confirmed, reported, or rumour/speculation.
- F1 is a first-class interest. Herwanto supports Mercedes, especially Kimi Antonelli and George Russell; Lewis Hamilton is still one of his favourites even at Ferrari. As of the 2026 season, the official F1 line-up is: Mercedes — George Russell, Kimi Antonelli; Ferrari — Charles Leclerc, Lewis Hamilton; McLaren — Lando Norris, Oscar Piastri; Red Bull Racing — Max Verstappen, Isack Hadjar; Racing Bulls — Liam Lawson, Arvid Lindblad; Williams — Carlos Sainz, Alexander Albon; Aston Martin — Fernando Alonso, Lance Stroll; Haas — Esteban Ocon, Oliver Bearman; Alpine — Pierre Gasly, Franco Colapinto; Audi — Nico Hulkenberg, Gabriel Bortoleto; Cadillac — Sergio Perez, Valtteri Bottas. For live F1 results, championship standings, race-weekend timings, current team stats, driver stats, rumours, penalties, or upgrades, use get_latest_news/web_search/fetch_url and cite what you found instead of relying on memory.
- When the user pastes a web link or asks you to read/check a URL, call fetch_url. If fetch_url fails or the page is paywalled/dynamic, say what failed and use web_search/get_latest_news for corroborating public sources where available.
- After web_search, fetch_url, get_latest_news, get_liverpool_brief, or get_f1_brief reveals useful knowledge H.I.R.A should retain, call remember_source_insight. Store stable background as durability=stable. Store current standings, line-ups, results, transfer rumours, prices, schedules, and laws/rules that may change as durability=live_check or rumour, so future answers know to verify again.
- When the user asks about weather, temperature, high/low temp, hot/cold conditions, rain, forecast, haze, PSI, air quality, umbrella, or whether it will rain in Singapore — call get_nea_weather before answering. If no area is specified, use Yishun. Weather answers must include available temperature, humidity, PSI/PM2.5 air quality, 2-hour nowcast, and 24-hour forecast details.
- When the user says "remember", "note that", or gives stable preferences/facts about himself — call remember_user_info.
- Treat Correction_Ledger memory as high-priority. If it conflicts with older memory, follow the correction and do not repeat the old mistake.
- Treat Self_Reflections memory as your own learning journal: use it to improve future behaviour, source discipline, and follow-through.
- Treat Source_Notes as source-backed research memory. Use them as leads and context, but live-check anything marked live_check, rumour, temporary, or time-sensitive.
- When the user says they have a new interest, are getting into a topic, want H.I.R.A to track/learn/follow something, or asks to build a beginner map for a new topic — call create_topic_profile. Store what to track, preferred angle, which facts should be live-checked, and stable background context. Do not store volatile standings/results/prices as permanent facts; mark those as live_facts.
- When the user gives a project progress update — call update_project_status.
- When the user asks to follow up with someone later, call create_followup.
- When the user asks to follow up based on an email, Gmail, inbox, or a recent message, call get_gmail_brief first and use the returned sender, subject, date, snippet/body excerpt to create the follow-up. Do not ask him to paste email details unless Gmail is not connected or the matching email cannot be found.
- When the user asks to mark a reminder/task/follow-up done, call complete_task_by_text or complete_followup_by_text.
- When the user asks what to do now or how to prioritise tasks, call get_task_brief.
- When the user asks about Gmail, emails, inbox, unread email, latest/last/recent mail, email summaries, or drafting an email, use get_gmail_brief or create_gmail_draft when Gmail is connected. For "last/latest/recent emails", leave the Gmail query empty and set max_items. Use is:unread only when he explicitly asks for unread mail. If he asks for a number, set max_items to that number.
- If he says work/MOE/school email, use account="work". If he says personal Gmail/email, use account="personal". If unspecified, default to personal.
- When the user asks you to create a document, worksheet, letter, report, lesson plan, handout, memo, proposal, or meeting notes, call create_document_artifact.
- When the user asks you to create slides, a deck, PowerPoint, PPTX, presentation, pitch deck, briefing deck, or lesson slides, call create_slide_deck_artifact.
- When the user gives a reusable document/deck style, format, template preference, rubric format, NBSS worksheet format, GamePlan pitch style, or Rūḥ deck style, call remember_artifact_template.
- After using a tool, confirm briefly and naturally. Do not ask "shall I add this?" — just do it.
"""

def CACHED_SYSTEM_PROMPT():
    now = datetime.now(SGT)
    key = (now.strftime("%Y-%m-%d %H:%M"), google_ok())
    if _SYSTEM_PROMPT_CACHE["key"] != key:
        _SYSTEM_PROMPT_CACHE["key"] = key
        _SYSTEM_PROMPT_CACHE["value"] = SYSTEM_PROMPT()
    return _SYSTEM_PROMPT_CACHE["value"]

# Claude tool definition for web search
SEARCH_TOOL = {
    "name": "web_search",
    "description": "Search the web for current information — news, prices, events, recent developments. Use when the question needs up-to-date information.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "The search query"}
        },
        "required": ["query"]
    }
}

FETCH_URL_TOOL = {
    "name": "fetch_url",
    "description": "Fetch and read a pasted http(s) URL. Use when the user gives a link, asks to read/check a website, or needs page-specific browsing rather than general search.",
    "input_schema": {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "The full http(s) URL to fetch and read."},
            "max_chars": {"type": "integer", "description": "Maximum readable characters to return, usually 6000."},
        },
        "required": ["url"]
    }
}

SOURCE_NOTE_TOOL = {
    "name": "remember_source_insight",
    "description": "Save a useful source-backed insight after web search, news, or URL reading. Store stable background facts as stable; store standings, prices, line-ups, rumours, schedules, and other changing facts as live_check.",
    "input_schema": {
        "type": "object",
        "properties": {
            "topic": {"type": "string", "description": "Topic this source teaches H.I.R.A about."},
            "source": {"type": "string", "description": "Source name or publication, if known."},
            "source_url": {"type": "string", "description": "URL for the source, if available."},
            "insight": {"type": "string", "description": "Concise useful learning from the source."},
            "durability": {"type": "string", "description": "stable, live_check, rumour, or temporary."},
            "confidence": {"type": "string", "description": "source-backed, reported, official, rumour, or uncertain."},
        },
        "required": ["topic", "insight"]
    }
}

CALENDAR_TOOL = {
    "name": "create_calendar_event",
    "description": "Create a Google Calendar event. Use this automatically when the user mentions attending, scheduling, or having something at a specific date and time — matches, duties, meetings, trainings, appointments.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title":       {"type": "string", "description": "Event title"},
            "date":        {"type": "string", "description": "YYYY-MM-DD"},
            "start_time":  {"type": "string", "description": "HH:MM in 24hr"},
            "end_time":    {"type": "string", "description": "HH:MM in 24hr"},
            "location":    {"type": "string", "description": "Location if mentioned, else empty"},
            "description": {"type": "string", "description": "Extra notes if any, else empty"}
        },
        "required": ["title", "date", "start_time", "end_time"]
    }
}

DELETE_CALENDAR_TOOL = {
    "name": "delete_calendar_event_by_text",
    "description": "Delete the closest matching Google Calendar event by natural language text. Use when Herwanto says an event/meeting/duty/appointment is done, cancelled, or should be removed.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Event text to match, e.g. CCA duty, meeting with VP, lab booking"},
            "days_back": {"type": "integer", "description": "Days back to search, default 7"},
            "days_ahead": {"type": "integer", "description": "Days ahead to search, default 30"}
        },
        "required": ["query"]
    }
}

AVAILABILITY_SLOT_TOOL = {
    "name": "find_available_training_slots",
    "description": "Find checked after-school availability slots by combining timetable lessons and Google Calendar events. Use before suggesting best slots for trainings, meetings, Sahibba, CCA-adjacent activities, or any request that asks when something can fit.",
    "input_schema": {
        "type": "object",
        "properties": {
            "days": {"type": "integer", "description": "Days to scan ahead, default 7, max 14"},
            "duration_minutes": {"type": "integer", "description": "Required slot duration in minutes, default 60"},
            "window_start": {"type": "string", "description": "Earliest time in HH:MM, default 14:00"},
            "window_end": {"type": "string", "description": "Latest time in HH:MM, default 18:30"},
            "avoid_keywords": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Calendar keywords to avoid entirely on that day, e.g. CCA, football"
            },
            "purpose": {"type": "string", "description": "Short reason for the slot search, e.g. Sahibba training"}
        }
    }
}

REMINDER_TOOL = {
    "name": "add_reminder",
    "description": "Add a task or deadline reminder. Use this automatically when the user mentions something they need to do, prepare, submit, mark, or complete by a certain date.",
    "input_schema": {
        "type": "object",
        "properties": {
            "description": {"type": "string", "description": "What needs to be done"},
            "due_date":    {"type": "string", "description": "YYYY-MM-DD"},
            "category":    {"type": "string", "description": "Teaching, CCA, GamePlan, Ruh, or Personal"}
        },
        "required": ["description", "due_date"]
    }
}

NUDGE_TOOL = {
    "name": "create_proactive_nudge",
    "description": "Schedule H.I.R.A to initiate a Telegram chat at a specific date and time with a short message or heads-up.",
    "input_schema": {
        "type": "object",
        "properties": {
            "message": {"type": "string", "description": "Message H.I.R.A should send later"},
            "send_at": {"type": "string", "description": "ISO datetime in Asia/Singapore, e.g. 2026-05-01T16:30:00+08:00"}
        },
        "required": ["message", "send_at"]
    }
}

DAILY_CHECKIN_TOOL = {
    "name": "create_daily_checkin",
    "description": "Create a recurring daily check-in. H.I.R.A pings at configured times until Herwanto replies affirmatively, then stops for that day.",
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Short habit/check-in name"},
            "question": {"type": "string", "description": "Question H.I.R.A should ask"},
            "times": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Daily times in HH:MM 24-hour Singapore time"
            }
        },
        "required": ["name", "question", "times"]
    }
}

BREAK_AWARE_CHECKIN_TOOL = {
    "name": "create_break_aware_daily_checkin",
    "description": "Create a recurring daily check-in that recalculates today's reminder times from Herwanto's lessons and Google Calendar, then pings during free breaks.",
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Short habit/check-in name"},
            "question": {"type": "string", "description": "Question H.I.R.A should ask"},
            "target_count": {
                "type": "integer",
                "description": "How many break-time reminders to aim for each day, usually 2 to 4"
            },
            "window_start": {
                "type": "string",
                "description": "Earliest reminder time in HH:MM Singapore time, default 08:00"
            },
            "window_end": {
                "type": "string",
                "description": "Latest reminder time in HH:MM Singapore time, default 21:30"
            },
            "min_break_minutes": {
                "type": "integer",
                "description": "Smallest free window worth using, default 20"
            }
        },
        "required": ["name", "question"]
    }
}

CONTEXT_TOOL = {
    "name": "get_assistant_context",
    "description": "Get Herwanto's current assistant context: timetable, calendar events, reminders, projects, and stored memory. Use before answering questions about schedule, priorities, deadlines, workload, or project state.",
    "input_schema": {
        "type": "object",
        "properties": {
            "days": {"type": "integer", "description": "Number of days to look ahead, from 1 to 14"}
        }
    }
}

TIMETABLE_TOOL = {
    "name": "get_timetable",
    "description": "Get Herwanto's NBSS timetable lessons from timetable.py, the source of truth. Use for lesson/timetable questions, including specific day and odd/even week lookups.",
    "input_schema": {
        "type": "object",
        "properties": {
            "day": {
                "type": "string",
                "description": "Optional day name: Monday, Tuesday, Wednesday, Thursday, Friday, or short forms Mon-Fri. Leave blank for today."
            },
            "week_type": {
                "type": "string",
                "description": "Optional week type: odd, even, O, E, or current. Leave blank/current to use the current school week."
            }
        }
    }
}

CLASSLIST_TOOL = {
    "name": "get_mtl_classlists",
    "description": "Get Herwanto's live 2026 MTL classlists from Google Sheets. Use when he asks about his classes, classlists, student names, score columns, marks, results, who is in a class, MTL groups, or students under CG Herwanto.",
    "input_schema": {
        "type": "object",
        "properties": {
            "class_query": {
                "type": "string",
                "description": "Optional class/group/name filter, such as S1, 1 Flagship, 2G3 ML, 4NT BML, or a student name."
            },
            "include_students": {
                "type": "boolean",
                "description": "Whether to include student names. Use true unless only a high-level count is requested."
            },
            "include_scores": {
                "type": "boolean",
                "description": "Whether to include non-roster columns such as marks, assessment results, remarks, or score fields. Use true for scores/marks/results questions."
            }
        }
    }
}

UPDATE_CLASS_SCORE_TOOL = {
    "name": "update_mtl_class_score",
    "description": "Write a calculated or provided score/mark/result into Herwanto's live MTL classlist Google Sheet for one clearly matched student and one clearly matched column. Use after reading scores when needed. Do not use if the student or column is ambiguous.",
    "input_schema": {
        "type": "object",
        "properties": {
            "class_query": {
                "type": "string",
                "description": "Class/group filter, e.g. 2G3, 3G3, S1, 4NT BML. Include it whenever known."
            },
            "student_query": {
                "type": "string",
                "description": "Student name or register number to match exactly enough."
            },
            "score_column": {
                "type": "string",
                "description": "Target sheet column/header to update, e.g. WA1, Oral, Karangan, Total, Overall."
            },
            "score_value": {
                "type": "string",
                "description": "Value to write. Can be a number, percentage, text, or Sheets formula. Calculate first if the user gives a formula/instructions."
            }
        },
        "required": ["student_query", "score_column", "score_value"]
    }
}

FILL_PERCENTAGE_SCORES_TOOL = {
    "name": "fill_mtl_percentage_scores",
    "description": "Bulk-fill percentage columns in Herwanto's MTL classlist sheet. Use for WA, FA, prelim, and EOY tables where a raw total column is immediately followed by a % column, e.g. FA2 35 then %. It converts numeric scores including 0 to rounded percentages and copies non-scoring status codes: AB absent, VR valid reason, MC medical certificate.",
    "input_schema": {
        "type": "object",
        "properties": {
            "class_query": {
                "type": "string",
                "description": "Class/group filter, e.g. S4-AN, 4NT BML, 2G3. Strongly recommended."
            },
            "assessment_query": {
                "type": "string",
                "description": "Assessment group above the percentage column, e.g. FA1 or FA2. Strongly recommended when multiple % columns exist."
            },
            "only_blank": {
                "type": "boolean",
                "description": "If true, fill only empty percentage cells and leave existing values unchanged. Default true."
            }
        }
    }
}

ANALYZE_MTL_SCORES_TOOL = {
    "name": "analyze_mtl_scores",
    "description": "Analyse Herwanto's MTL classlist scores from Google Sheets for WA, FA, prelim, and EOY results. Computes mean, median, range, standard deviation, pass/distinction counts, watchlist/underperforming students, strongest students, most improved, and drastic drops across score columns. Treat 0 as a real attempted score; treat AB absent, VR valid reason, and MC medical certificate as non-scoring statuses counted separately.",
    "input_schema": {
        "type": "object",
        "properties": {
            "class_query": {
                "type": "string",
                "description": "Optional class/group filter, e.g. S4-AN, 2G3, 4NT BML."
            },
            "assessment_query": {
                "type": "string",
                "description": "Optional assessment or column filter, e.g. FA1, FA2, %, WA1, WA2, Oral, Prelim, EOY, Overall."
            },
            "compare_from": {
                "type": "string",
                "description": "Optional starting assessment/column for progress comparison, e.g. FA1 %."
            },
            "compare_to": {
                "type": "string",
                "description": "Optional ending assessment/column for progress comparison, e.g. FA2 %."
            }
        }
    }
}

MEMORY_TOOL = {
    "name": "remember_user_info",
    "description": "Persist stable information about Herwanto: preferences, personal profile facts, important people, places, or project context.",
    "input_schema": {
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "description": "One of: profile, preferences, people, places, teaching, business, projects, sports, files, templates, constraints, recent_summaries"
            },
            "text": {"type": "string", "description": "Concise memory to store"}
        },
        "required": ["category", "text"]
    }
}

TOPIC_PROFILE_TOOL = {
    "name": "create_topic_profile",
    "description": "Create or update a structured profile for a new or growing interest H.I.R.A should learn, track, and explain through Herwanto's preferred lens. Use when he says new interest, getting into, picked up, follow/track this, deep dive, beginner map, or wants H.I.R.A to remember an interest.",
    "input_schema": {
        "type": "object",
        "properties": {
            "topic": {"type": "string", "description": "Topic name, e.g. MotoGP, Japanese city pop, espresso, mechanical keyboards."},
            "category": {"type": "string", "description": "Broad bucket such as sports, music, tech, culture, business, teaching, personal, or faith."},
            "why": {"type": "string", "description": "Why Herwanto cares or what drew him in, if known."},
            "track": {
                "type": "array",
                "items": {"type": "string"},
                "description": "People, teams, subtopics, events, concepts, or questions to keep an eye on."
            },
            "preferred_angle": {"type": "string", "description": "How to explain it to him, e.g. beginner-friendly, tactical, historical, product-minded, Singapore context."},
            "preferred_sources": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Sources or source types to prefer if the user names any."
            },
            "live_facts": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Facts that must be web-checked each time, e.g. standings, results, prices, lineups, latest releases."
            },
            "stable_context": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Stable background to learn, e.g. rules, history, terminology, key people, beginner map."
            },
            "update_cadence": {"type": "string", "description": "Optional cadence, e.g. race weekends, weekly, when I ask, major news only."}
        },
        "required": ["topic"]
    }
}

WEEK_TYPE_TOOL = {
    "name": "set_current_school_week",
    "description": "Persist the current school timetable week type. Use when Herwanto says this/today/current week is odd/even, or gives a school week number like 'week 6'.",
    "input_schema": {
        "type": "object",
        "properties": {
            "week_type": {
                "type": "string",
                "description": "odd or even"
            },
            "week_number": {
                "type": "integer",
                "description": "Optional school week number if mentioned"
            }
        },
        "required": ["week_type"]
    }
}

PROJECT_TOOL = {
    "name": "update_project_status",
    "description": "Create or update a tracked project status in Google Sheets when Herwanto gives a project progress update or milestone.",
    "input_schema": {
        "type": "object",
        "properties": {
            "project": {"type": "string", "description": "Project name"},
            "status": {"type": "string", "description": "Current status"},
            "milestone": {"type": "string", "description": "Next milestone, if any"},
            "milestone_date": {"type": "string", "description": "YYYY-MM-DD if known, else empty"},
            "notes": {"type": "string", "description": "Important notes, blockers, or context"}
        },
        "required": ["project", "status"]
    }
}

NEWS_TOOL = {
    "name": "get_latest_news",
    "description": "Fetch current Google News headlines. Use for latest news, current events, and Herwanto's shortlisted news topics.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional specific topic to search. Leave empty to use Herwanto's shortlisted topics."
            },
            "max_items": {
                "type": "integer",
                "description": "Number of headlines per topic, usually 2 to 5."
            }
        }
    }
}

LIVERPOOL_BRIEF_TOOL = {
    "name": "get_liverpool_brief",
    "description": "Fetch a structured current Liverpool FC brief: Premier League standing/form, fixtures/results/line-ups, competition progress, injuries/suspensions, and transfer news/rumours. Use for Liverpool, LFC, Anfield, EPL/Premier League table, Wirtz, Isak, Salah, team news, and transfer questions.",
    "input_schema": {
        "type": "object",
        "properties": {
            "focus": {
                "type": "string",
                "description": "Optional focus, e.g. upcoming match, transfer rumours, injuries, EPL table, or a player name."
            },
            "max_items": {
                "type": "integer",
                "description": "Number of source items per section, usually 2 or 3."
            }
        }
    }
}

F1_BRIEF_TOOL = {
    "name": "get_f1_brief",
    "description": "Fetch a structured current Formula 1 brief: standings, latest race results, Mercedes/Russell/Antonelli focus, Hamilton watch, and team news/upgrades. Use for F1, Formula 1, Grand Prix, Mercedes, Kimi Antonelli, George Russell, Lewis Hamilton, qualifying, race results, and championship questions.",
    "input_schema": {
        "type": "object",
        "properties": {
            "focus": {
                "type": "string",
                "description": "Optional focus, e.g. latest race, standings, Mercedes, Hamilton, or a Grand Prix name."
            },
            "max_items": {
                "type": "integer",
                "description": "Number of source items per section, usually 2 or 3."
            }
        }
    }
}

WEATHER_TOOL = {
    "name": "get_nea_weather",
    "description": "Fetch latest Singapore weather from NEA/MSS via data.gov.sg, including 2-hour nowcast, temperature, humidity, PSI/PM2.5 air quality, and forecast details. Use for weather, temperature, high/low temp, hot/cold conditions, rain, forecast, haze, PSI, air quality, umbrella, or whether it will rain. If no area is specified, use Yishun.",
    "input_schema": {
        "type": "object",
        "properties": {
            "area": {
                "type": "string",
                "description": "Singapore area/town for the 2-hour forecast, e.g. Yishun, Woodlands, City, Tampines. Use Yishun if unspecified."
            },
            "include_24h": {
                "type": "boolean",
                "description": "Include the 24-hour general forecast and regional periods. Default true."
            },
            "include_4day": {
                "type": "boolean",
                "description": "Include the 4-day outlook when the user asks for this week, coming days, or outlook. Default false."
            }
        }
    }
}

PRAYER_TIME_TOOL = {
    "name": "get_muis_prayer_times",
    "description": "Get exact MUIS Singapore prayer times for a date. Use for Subuh/Fajr, Syuruk, Zohor/Zuhur/Zuhr/Dhuhr, Asar/Asr, Maghrib, Isyak/Isha, solat/salah timing, or prayer planning. Never guess prayer times when this tool is available.",
    "input_schema": {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "description": "YYYY-MM-DD in Singapore time. Leave blank for today."
            },
            "prayer": {
                "type": "string",
                "description": "Optional prayer name: subuh/fajr, syuruk, zohor/zuhur/zuhr/dhuhr, asar/asr, maghrib, isyak/isha."
            }
        }
    }
}

KHUTBAH_TOOL = {
    "name": "get_muis_friday_khutbah",
    "description": "Get the latest MUIS Singapore Friday khutbah/sermon heads-up for Jumu'ah, including title, date, official summary, key points, and source link.",
    "input_schema": {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "description": "YYYY-MM-DD in Singapore time. Leave blank for today/latest available khutbah."
            },
            "language": {
                "type": "string",
                "description": "English, Malay/Jawi, or Tamil. Default English."
            }
        }
    }
}

DOCUMENT_ARTIFACT_TOOL = {
    "name": "create_document_artifact",
    "description": "Create a downloadable DOCX document and, when Google Drive is available, an editable Google Docs link.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Document title"},
            "instructions": {"type": "string", "description": "What the document should contain"},
            "doc_type": {"type": "string", "description": "worksheet, letter, report, lesson_plan, memo, proposal, notes, or general"},
            "audience": {"type": "string", "description": "Intended audience, e.g. Sec 3 ML, parents, school leaders"},
            "language": {"type": "string", "description": "Language to use, e.g. English, Bahasa Melayu"},
        },
        "required": ["title", "instructions"]
    }
}

SLIDE_ARTIFACT_TOOL = {
    "name": "create_slide_deck_artifact",
    "description": "Create a downloadable PPTX slide deck and, when Google Drive is available, an editable Google Slides link.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Deck title"},
            "instructions": {"type": "string", "description": "What the deck should contain"},
            "audience": {"type": "string", "description": "Intended audience"},
            "slide_count": {"type": "integer", "description": "Approximate number of slides, usually 5 to 12"},
            "language": {"type": "string", "description": "Language to use, e.g. English, Bahasa Melayu"},
        },
        "required": ["title", "instructions"]
    }
}

TEMPLATE_MEMORY_TOOL = {
    "name": "remember_artifact_template",
    "description": "Persist reusable document/deck style and template preferences for future generated artifacts.",
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Template or style name"},
            "notes": {"type": "string", "description": "Reusable formatting, tone, structure, and audience notes"}
        },
        "required": ["name", "notes"]
    }
}

FOLLOWUP_TOOL = {
    "name": "create_followup",
    "description": "Track a person/topic to follow up by a due date.",
    "input_schema": {
        "type": "object",
        "properties": {
            "person": {"type": "string", "description": "Person or organisation to follow up with"},
            "topic": {"type": "string", "description": "What to follow up about"},
            "due_date": {"type": "string", "description": "YYYY-MM-DD"},
            "channel": {"type": "string", "description": "Email, WhatsApp, Telegram, call, or blank"},
            "notes": {"type": "string", "description": "Optional context"}
        },
        "required": ["topic", "due_date"]
    }
}

COMPLETE_TASK_TOOL = {
    "name": "complete_task_by_text",
    "description": "Mark the closest matching reminder/task done from natural text.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Reminder text to match"}
        },
        "required": ["query"]
    }
}

COMPLETE_FOLLOWUP_TOOL = {
    "name": "complete_followup_by_text",
    "description": "Mark the closest matching follow-up done from natural text.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Follow-up text or id to match"}
        },
        "required": ["query"]
    }
}

ADD_MARKING_TOOL = {
    "name": "add_marking_task",
    "description": "Add a marking stack/task for scripts or papers. Use when Herwanto says to add something to marking tasks, e.g. 'add 1 stack of kefahaman 2G3'.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Short marking task title, e.g. Kefahaman 2G3"},
            "total_scripts": {"type": "integer", "description": "Total scripts if known; use 0 if unknown"},
            "stack_count": {"type": "integer", "description": "Number of stacks, default 1"},
            "collected_date": {"type": "string", "description": "YYYY-MM-DD date the stack was collected from students; default today if not stated"},
            "notes": {"type": "string", "description": "Optional notes"}
        },
        "required": ["title"]
    }
}

UPDATE_MARKING_TOOL = {
    "name": "update_marking_progress",
    "description": "Update scripts marked for an active marking task. Use marked_count to set the current total, or increment when the user says 'more' or 'another'. Only set done=true when the user explicitly says the marking stack is done, completed, or can be closed.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Marking task text or id to match"},
            "marked_count": {"type": "integer", "description": "Current total scripts marked, if the user gives the new total"},
            "increment": {"type": "integer", "description": "Additional scripts marked, if the user says more/another"},
            "done": {"type": "boolean", "description": "True if the marking stack is complete"}
        },
        "required": ["query"]
    }
}

MARKING_BRIEF_TOOL = {
    "name": "get_marking_brief",
    "description": "Get active marking stacks with marked and outstanding script counts.",
    "input_schema": {
        "type": "object",
        "properties": {}
    }
}

RESET_MARKING_TOOL = {
    "name": "reset_marking_load",
    "description": "Clear all active marking stacks from the marking-load board. Use when Herwanto asks to reset or clear marking load, outstanding marking, or the marking board.",
    "input_schema": {
        "type": "object",
        "properties": {}
    }
}

TASK_BRIEF_TOOL = {
    "name": "get_task_brief",
    "description": "Get prioritised reminders/tasks with due dates and next actions.",
    "input_schema": {
        "type": "object",
        "properties": {
            "days": {"type": "integer", "description": "Days ahead to include, default 7"}
        }
    }
}

GMAIL_BRIEF_TOOL = {
    "name": "get_gmail_brief",
    "description": "Get Gmail messages from natural language email queries. Use for inbox, latest emails, last N emails, recent mail, unread mail, sender/subject searches, or email summaries.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Gmail search query. Leave empty for latest inbox messages. Use is:unread only if the user asks for unread mail."},
            "max_items": {"type": "integer", "description": "Maximum messages, default 10"},
            "account": {"type": "string", "description": "personal or work. Use work for MOE/school email."}
        }
    }
}

GMAIL_DRAFT_TOOL = {
    "name": "create_gmail_draft",
    "description": "Create a Gmail draft when Gmail is connected.",
    "input_schema": {
        "type": "object",
        "properties": {
            "to": {"type": "string", "description": "Recipient email"},
            "subject": {"type": "string", "description": "Email subject"},
            "body": {"type": "string", "description": "Email body"},
            "cc": {"type": "string", "description": "Optional cc"},
            "account": {"type": "string", "description": "personal or work. Use work for MOE/school email."}
        },
        "required": ["to", "subject", "body"]
    }
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def google_ok():
    return bool(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") and os.environ.get("GOOGLE_SHEET_ID"))

async def reply(update, text, **kwargs):
    if len(text) > 4000:
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            await update.message.reply_text(chunk, **kwargs)
    else:
        await update.message.reply_text(text, **kwargs)

def _get_week_config():
    if not google_ok():
        return None, None
    try:
        return gs.get_config("week_ref_date"), gs.get_config("week_ref_type")
    except Exception:
        return None, None

def _normalise_week_type(value: str) -> str | None:
    value = (value or "").strip().lower()
    if value in ("odd", "o"):
        return "odd"
    if value in ("even", "e"):
        return "even"
    return None

def _school_week_from_text(text: str) -> tuple[str | None, int | None]:
    clean = " ".join((text or "").lower().split())

    number_match = re.search(r"\b(?:week|wk)\s*(\d{1,2})\b", clean)
    if number_match:
        context_words = (
            "today", "this week", "current week", "remember", "note that",
            "timetable", "refer to", "school week"
        )
        if clean.strip() == number_match.group(0) or any(word in clean for word in context_words):
            week_number = int(number_match.group(1))
            return ("odd" if week_number % 2 else "even"), week_number

    type_patterns = [
        r"\b(odd|even)\s+week\b",
        r"\bthis\s+week\s+is\s+(?:an?\s+)?(odd|even)\b",
        r"\bcurrent\s+week\s+is\s+(?:an?\s+)?(odd|even)\b",
        r"\btoday\s+is\s+(?:an?\s+)?(odd|even)\b",
    ]
    for pattern in type_patterns:
        match = re.search(pattern, clean)
        if match:
            return match.group(1), None

    return None, None

def _set_current_school_week(week_type: str, week_number: int | None = None) -> str:
    if not google_ok():
        raise RuntimeError("Google not connected.")
    wt = _normalise_week_type(week_type)
    if not wt:
        raise ValueError("Week type must be odd or even.")
    today = datetime.now(SGT).date()
    monday = (today - timedelta(days=today.weekday())).isoformat()
    gs.set_config("week_ref_date", monday)
    gs.set_config("week_ref_type", wt)
    if week_number:
        gs.set_config("school_week_number", str(week_number))
    return monday

def _normalise_timetable_day(value: str | None) -> str | None:
    clean = (value or "").strip().lower()
    if not clean:
        return None
    days = {
        "monday": "Mon", "mon": "Mon",
        "tuesday": "Tue", "tue": "Tue", "tues": "Tue",
        "wednesday": "Wed", "wed": "Wed",
        "thursday": "Thu", "thu": "Thu", "thur": "Thu", "thurs": "Thu",
        "friday": "Fri", "fri": "Fri",
    }
    return days.get(clean)

def _timetable_for_lookup(day: str | None = "", week_type: str | None = "") -> str:
    today = datetime.now(SGT).date()
    day_name = _normalise_timetable_day(day) or tt.DAY_MAP.get(today.weekday())
    if not day_name:
        return "No timetabled lessons — free day."

    wt = _normalise_week_type(week_type or "")
    if wt:
        wt_code = "O" if wt == "odd" else "E"
        wt_label = tt.week_type_label(wt_code)
        lessons = tt.TIMETABLE.get((day_name, wt_code), [])
        return f"{day_name} {wt_label} week timetable:\n{tt.format_lessons(lessons)}"

    lessons, wt_label = _lessons_for_date(today)
    if _normalise_timetable_day(day):
        official_week = tt.get_school_week_info(today)
        if official_week:
            wt_code = official_week["week_type"]
        else:
            ref_date, ref_type = _get_week_config()
            if not ref_date or not ref_type:
                return "Week type is not set. Use /setweek odd or /setweek even first."
            wt_code = tt.get_week_type(ref_date, ref_type, today)
        lessons = tt.TIMETABLE.get((day_name, wt_code), [])
        wt_label = tt.week_type_label(wt_code)
    return f"{day_name} {wt_label} week timetable:\n{tt.format_lessons(lessons)}"

def _lessons_for_date(target):
    official_week = tt.get_school_week_info(target)
    if official_week:
        day_name = tt.DAY_MAP.get(target.weekday())
        if not day_name or official_week["is_school_holiday"]:
            return [], ""
        lessons = tt.TIMETABLE.get((day_name, official_week["week_type"]), [])
        return lessons, tt.week_type_label(official_week["week_type"])

    ref_date, ref_type = _get_week_config()
    if not ref_date or not ref_type:
        return [], ""
    lessons = tt.get_lessons(target, ref_date, ref_type)
    wt = tt.get_week_type(ref_date, ref_type, target)
    return lessons, tt.week_type_label(wt)

def _school_week_label(target: date) -> str:
    official_week = tt.get_school_week_info(target)
    if official_week:
        return f"{official_week['term']} Week {official_week['week_number']}"
    if google_ok():
        try:
            stored_week_number = gs.get_config("school_week_number")
            if stored_week_number:
                return f"school week {stored_week_number}"
        except Exception:
            pass
    return ""

def _week_display(wt_label: str, target: date) -> str:
    school_week = _school_week_label(target)
    return f"{wt_label} week, {school_week}" if school_week else f"{wt_label} week"

def _agenda_week_display(target: date) -> str:
    official_week = tt.get_school_week_info(target)
    if not official_week:
        return ""
    base = f"{official_week['term']} Week {official_week['week_number']}"
    if target.weekday() > 4:
        return f"Weekend, {base}"
    if official_week["is_school_holiday"]:
        return f"School holiday, {base}"
    return f"{tt.week_type_label(official_week['week_type'])} week, {base}"

def _format_memory(memory: dict) -> str:
    lines = [
        "*Persistent School Calendar*",
        f"- {tt.format_school_calendar_memory()}",
        "",
        "*Persistent Timetable*",
        f"- {tt.format_timetable_memory()}",
        "",
    ]
    for category in MEMORY_DISPLAY_CATEGORIES:
        items = memory.get(category, [])
        if not items:
            continue
        lines.append(f"*{category.title()}*")
        for item in items:
            lines.append(f"- {item}")
        lines.append("")
    return "\n".join(lines).strip()

def _clip_memory_text(value: str, limit: int = 1200) -> str:
    clean = " ".join((value or "").split())
    if len(clean) <= limit:
        return clean
    return clean[:limit - 3].rstrip() + "..."

def _looks_like_correction(text: str) -> bool:
    clean = " ".join((text or "").lower().split())
    if not clean:
        return False
    return bool(re.search(
        r"\b("
        r"actually|correction|correct that|not quite|not really|wrong|incorrect|mistake|"
        r"facts straight|fact check|source check|verify that|check that|"
        r"you got|you said|you missed|you forgot|don't say|dont say|should know|"
        r"needs to know|lost when it comes to|out of the loop|still a fav|still a favourite|"
        r"still a favorite"
        r")\b",
        clean,
    ))

def _looks_reflection_worthy(text: str) -> bool:
    clean = " ".join((text or "").lower().split())
    if not clean:
        return False
    return _looks_like_correction(clean) or bool(re.search(
        r"\b("
        r"new interest|new topic|getting into|picked up|deep dive|track this|follow this|"
        r"learn this|remember|upgrade|improve|architecture|brain|self aware|self-aware|"
        r"knowledge base|web|browse|live update|current|latest|lineups?|standings|rumou?rs?"
        r")\b",
        clean,
    ))

def record_chat_learning_event(user_text: str, assistant_text: str = "", source: str = "chat") -> list[dict]:
    if not google_ok():
        return []
    user_summary = _clip_memory_text(user_text, 360)
    assistant_summary = _clip_memory_text(assistant_text, 260)
    if not user_summary or not _looks_reflection_worthy(user_summary):
        return []

    now = datetime.now(SGT).strftime("%Y-%m-%d %H:%M SGT")
    recorded: list[dict] = []
    try:
        if _looks_like_correction(user_summary):
            correction = gs.add_correction({
                "date": now,
                "source": source,
                "correction": user_summary,
                "assistant_response": assistant_summary,
                "priority": "high",
            })
            recorded.append({"type": "correction", "entry": correction})
    except Exception as exc:
        logger.warning(f"Could not record correction learning event: {exc}")

    try:
        behavior = (
            "Before answering similar future requests, check correction_ledger and use live/source-backed tools "
            "when the fact may have changed."
        )
        if re.search(r"\b(new interest|new topic|getting into|picked up|deep dive|track this|follow this|learn this)\b", user_summary, re.I):
            behavior = "Build or update a topic profile, separate stable context from live facts, and offer a beginner map when useful."
        elif re.search(r"\b(upgrade|improve|architecture|brain|self aware|self-aware|knowledge base)\b", user_summary, re.I):
            behavior = "Record what changed, update the growth log when the upgrade is meaningful, and preserve the lesson for future design decisions."
        reflection = gs.add_self_reflection({
            "date": now,
            "source": source,
            "trigger": user_summary,
            "learned": f"Herwanto signalled this was important: {user_summary}",
            "next_behavior": behavior,
        })
        recorded.append({"type": "self_reflection", "entry": reflection})
    except Exception as exc:
        logger.warning(f"Could not record self-reflection learning event: {exc}")
    return recorded

async def _detect_implicit_correction(user_text: str, history: list[dict]) -> None:
    """Use the router model to check if the user is implicitly correcting HIRA's previous response.
    If a correction is detected, it is logged to the correction ledger automatically.
    Only runs when there is a preceding assistant turn to compare against."""
    if not google_ok():
        return
    if len(history) < 2:
        return
    # Find the most recent assistant turn
    prev_assistant = next(
        (m["content"] for m in reversed(history) if m.get("role") == "assistant" and isinstance(m.get("content"), str)),
        None,
    )
    if not prev_assistant:
        return
    # Don't double-fire for messages already caught by explicit keyword detection
    if _looks_like_correction(user_text):
        return
    # Quick guard: skip very short or clearly non-corrective messages
    clean = " ".join(user_text.split())
    if len(clean) < 8 or _obvious_quick_chat(clean):
        return
    try:
        prompt = (
            "You are checking if the user message implicitly contradicts or corrects something in the assistant's previous response.\n\n"
            f"Previous assistant response (excerpt): {prev_assistant[:400]}\n\n"
            f"User message: {clean[:300]}\n\n"
            "Does the user message correct a specific fact, date, name, number, or claim in the assistant response?\n"
            "Reply with ONLY: CORRECTION: <one-sentence description of what was corrected>\n"
            "Or if no correction: NO"
        )
        resp = await async_claude.messages.create(
            model=ROUTER_MODEL,
            max_tokens=60,
            messages=[{"role": "user", "content": prompt}],
        )
        verdict = (resp.content[0].text or "").strip()
        if verdict.upper().startswith("CORRECTION:"):
            correction_text = verdict[len("CORRECTION:"):].strip()
            now_str = datetime.now(SGT).strftime("%Y-%m-%d %H:%M SGT")
            gs.add_correction({
                "date": now_str,
                "source": "implicit_detection",
                "correction": correction_text,
                "assistant_response": prev_assistant[:200],
                "priority": "medium",
            })
            gs.add_self_reflection({
                "date": now_str,
                "source": "implicit_detection",
                "trigger": f"User implicitly corrected: {clean[:200]}",
                "learned": correction_text,
                "next_behavior": "Verify this fact type with live tools before stating it as truth.",
            })
            logger.info(f"Implicit correction detected and logged: {correction_text[:100]}")
    except Exception as exc:
        logger.debug(f"Implicit correction detection failed: {exc}")


def _summarise_conversation_to_memory(history: list[dict]) -> None:
    """Summarise a conversation into recent_summaries memory when it ends (/clear or long session).
    Only fires if the conversation has meaningful depth (6+ turns with substantive content)."""
    if not google_ok():
        return
    user_turns = [m for m in history if m.get("role") == "user"]
    if len(user_turns) < 3:
        return
    # Only summarise if any turn has real content (not just one-word replies)
    substantive = [m for m in user_turns if len(str(m.get("content", "")).split()) > 4]
    if len(substantive) < 2:
        return
    try:
        # Use a short window of recent turns — we don't need the full history
        window = history[-12:]
        prompt_messages = [m for m in window if isinstance(m.get("content"), str)]
        if not prompt_messages:
            return
        summary_resp = claude.messages.create(
            model=QUICK_MODEL,
            max_tokens=100,
            messages=prompt_messages + [
                {"role": "user", "content": (
                    "In one sentence (max 30 words), summarise what was decided, created, or learned in this conversation. "
                    "Focus on outcomes and actions, not the chat mechanics. No preamble."
                )}
            ],
        )
        summary = (summary_resp.content[0].text or "").strip()
        if summary:
            date_str = datetime.now(SGT).strftime("%Y-%m-%d")
            gs.add_memory("recent_summaries", f"{date_str}: {summary}")
            logger.info(f"Conversation summary saved to recent_summaries: {summary[:80]}")
    except Exception as exc:
        logger.warning(f"Conversation summarisation failed: {exc}")


def _artifact_template_context() -> str:
    if not google_ok():
        return ""
    try:
        templates = gs.get_memory().get("templates", [])
    except Exception:
        return ""
    if not templates:
        return ""
    return "\n".join(f"- {item}" for item in templates[-12:])

def _remember_uploaded_file(kind: str, file_id: str, caption: str, extracted_summary: str, filename: str = "", mime_type: str = ""):
    if not google_ok():
        return
    received = datetime.now(SGT).strftime("%Y-%m-%d %H:%M SGT")
    label = filename or kind
    parts = [
        f"{label} received {received}",
        f"type={mime_type or kind}",
        f"telegram_file_id={file_id}",
    ]
    if caption:
        parts.append(f"caption={_clip_memory_text(caption, 240)}")
    if extracted_summary:
        parts.append(f"extracted={_clip_memory_text(extracted_summary, 900)}")
    gs.add_memory("files", " | ".join(parts))

def build_context_snapshot(days: int = 7) -> str:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(SGT)
    today = now.date()
    end_date = today + timedelta(days=days)
    lines = [f"Assistant context as of {now.strftime('%A, %-d %B %Y, %H:%M SGT')}"]

    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"\nToday's lessons ({_week_display(wt_label, today)}):")
        lines.append(tt.format_lessons(lessons).replace("*", ""))

    if not google_ok():
        lines.append("\nGoogle is not connected.")
        return "\n".join(lines)

    try:
        events = gs.get_events_for_days(days)
        lines.append(f"\nCalendar, next {days} days:")
        lines.append(gs.format_events(events, show_date=True).replace("*", ""))
    except Exception as e:
        lines.append(f"\nCalendar unavailable: {e}")

    try:
        reminders = gs.get_reminders()
        active = [r for r in reminders if r["due"] <= end_date.isoformat()]
        lines.append(f"\nReminders due by {end_date.isoformat()}:")
        if active:
            for r in sorted(active, key=lambda x: x["due"]):
                status = "OVERDUE" if r["due"] < today.isoformat() else "due"
                lines.append(f"- [{r['id']}] {r['due']} ({status}) {r['description']} / {r['category']}")
        else:
            lines.append("None.")
    except Exception as e:
        lines.append(f"\nReminders unavailable: {e}")

    try:
        followups = gs.get_followups()
        lines.append("\nFollow-ups:")
        if followups:
            for f in sorted(followups, key=lambda item: item["due_date"])[:10]:
                lines.append(f"- [{f['id']}] {f['due_date']} {f['person']} {f['topic']}".strip())
        else:
            lines.append("None.")
    except Exception as e:
        lines.append(f"\nFollow-ups unavailable: {e}")

    try:
        marking = gs.get_marking_tasks()
        lines.append("\nOutstanding marking:")
        if marking:
            for task in marking:
                lines.append(f"- {_format_marking_task(task).replace('*', '').replace('`', '')}")
        else:
            lines.append("None.")
    except Exception as e:
        lines.append(f"\nMarking unavailable: {e}")

    try:
        projects = gs.get_projects()
        lines.append("\nProjects:")
        if projects:
            for p in projects:
                milestone = f"; next {p['next_milestone']} ({p['milestone_date']})" if p["next_milestone"] else ""
                notes = f"; {p['notes']}" if p["notes"] else ""
                lines.append(f"- {p['project']}: {p['status']}{milestone}{notes}")
        else:
            lines.append("None tracked.")
    except Exception as e:
        lines.append(f"\nProjects unavailable: {e}")

    try:
        memory = gs.get_memory()
        formatted = _format_memory(memory).replace("*", "")
        lines.append("\nStored memory:")
        lines.append(formatted)
    except Exception as e:
        lines.append(f"\nMemory unavailable: {e}")

    return "\n".join(lines)

def build_artifact_index() -> str:
    if not google_ok():
        return "Google is not connected."
    try:
        memory = gs.get_memory()
        files = memory.get("files", [])
        if not files:
            return "No generated or uploaded files remembered yet."
        lines = ["*Artifact library*"]
        for item in files[-20:]:
            lines.append(f"- {item}")
        return "\n".join(lines)
    except Exception as e:
        return f"Artifact library unavailable: {e}"

def build_agenda(days: int = 7) -> str:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(SGT)
    today = now.date()
    lines = [f"*Agenda*\n_{now.strftime('%A, %-d %B %Y, %H:%M SGT')}_\n"]

    lessons, wt_label = _lessons_for_date(today)
    if wt_label and today.weekday() < 5:
        lines.append(f"*Today at school ({_week_display(wt_label, today)})*")
        lines.append(tt.format_lessons(lessons))
        lines.append("")

    if not google_ok():
        lines.append("_Google not connected._")
        return "\n".join(lines)

    events = gs.get_events_for_days(days)
    reminders = gs.get_reminders()
    end_date = today + timedelta(days=days)
    overdue = [r for r in reminders if r["due"] < today.isoformat()]
    due_window = [r for r in reminders if today.isoformat() <= r["due"] <= end_date.isoformat()]

    lines.append(f"*Calendar - next {days} days*")
    lines.append(gs.format_events(events, show_date=True))
    lines.append("")

    if overdue:
        lines.append("*Overdue*")
        for r in sorted(overdue, key=lambda x: x["due"]):
            lines.append(f"- `[{r['id']}]` {r['due']} - {r['description']} _{r['category']}_")
        lines.append("")

    lines.append(f"*Due by {(end_date).strftime('%-d %b')}*")
    if due_window:
        for r in sorted(due_window, key=lambda x: x["due"]):
            lines.append(f"- `[{r['id']}]` {r['due']} - {r['description']} _{r['category']}_")
    else:
        lines.append("Nothing due in this window.")
    lines.append("")

    return "\n".join(lines).strip()

def _event_to_agenda_item(event: dict) -> dict:
    raw_start = event.get("start", {}).get("dateTime", event.get("start", {}).get("date", ""))
    raw_end = event.get("end", {}).get("dateTime", event.get("end", {}).get("date", ""))
    all_day = "T" not in raw_start
    if all_day:
        start_dt = datetime.fromisoformat(raw_start).date()
        time_text = "All day"
    else:
        start_dt = datetime.fromisoformat(raw_start).astimezone(SGT)
        time_text = start_dt.strftime("%H:%M")
        if raw_end:
            try:
                end_dt = datetime.fromisoformat(raw_end).astimezone(SGT)
                time_text = f"{time_text}-{end_dt.strftime('%H:%M')}"
            except Exception:
                pass
    return {
        "date": start_dt.date().isoformat() if isinstance(start_dt, datetime) else start_dt.isoformat(),
        "time": time_text,
        "title": event.get("summary", "(No title)"),
        "meta": event.get("location", ""),
        "kind": "event",
    }

def build_agenda_structured(days: int = 7) -> dict:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(SGT)
    today = now.date()
    end_date = today + timedelta(days=days - 1)
    day_map = {}

    for offset in range(days):
        target = today + timedelta(days=offset)
        lessons, wt_label = _lessons_for_date(target)
        day_map[target.isoformat()] = {
            "date": target.isoformat(),
            "label": target.strftime("%A, %-d %B"),
            "week": _agenda_week_display(target),
            "lessons": [
                {
                    "time": f"{lesson['start']}-{lesson['end']}",
                    "subject": lesson["subject"],
                    "title": lesson["description"],
                    "room": lesson["room"] if lesson["room"] != "-" else "",
                    "kind": "lesson",
                }
                for lesson in lessons
            ],
            "events": [],
            "due": [],
        }

    if google_ok():
        try:
            for event in gs.get_events_for_days(days):
                item = _event_to_agenda_item(event)
                if item["date"] in day_map:
                    day_map[item["date"]]["events"].append(item)
        except Exception:
            pass
        try:
            reminders = gs.get_reminders()
            for reminder in reminders:
                due = reminder.get("due", "")
                if today.isoformat() <= due <= end_date.isoformat() and due in day_map:
                    day_map[due]["due"].append({
                        "id": reminder.get("id", ""),
                        "title": reminder.get("description", ""),
                        "category": reminder.get("category", ""),
                        "kind": "due",
                    })
        except Exception:
            pass

    return {
        "generated_at": now.strftime("%A, %-d %B %Y, %H:%M SGT"),
        "days": list(day_map.values()),
        "services": {"google": google_ok()},
    }

def _daily_load_tone(score: int) -> str:
    if score >= 82:
        return "red"
    if score >= 58:
        return "orange"
    if score >= 34:
        return "yellow"
    return "green"

def _daily_load_label(tone: str) -> str:
    return {
        "red": "Full day",
        "orange": "Packed",
        "yellow": "Steady",
        "green": "Pretty chill",
    }.get(tone, "Steady")

def _daily_load_note(today_load: dict) -> str:
    tone = today_load.get("tone", "green")
    lessons = int(today_load.get("lessons", 0) or 0)
    events = int(today_load.get("events", 0) or 0)
    due = int(today_load.get("due", 0) or 0)
    marking = int(today_load.get("marking_scripts", 0) or 0)
    if tone == "red":
        return "Heavy day. Keep decisions simple and protect the first clear pocket."
    if tone == "orange":
        return "Packed but movable. Triage the must-dos before the day starts pulling."
    if tone == "yellow":
        return "Steady day. Keep the middle of the day clear enough to breathe."
    if due or marking:
        return "Steady load. Clear one due item early so the day feels less sticky."
    if lessons >= 4 or events:
        return "Teaching rhythm day. Keep the small admin from breeding in corners."
    return "Pretty chill on paper. Good day to move one thing forward quietly."

def _workload_score(lessons: int, events: int, due: int, marking_scripts: int = 0) -> int:
    return min(100, (lessons * 12) + (events * 10) + (due * 9) + min(24, marking_scripts * 2))

def _daily_load_item(target, today, lesson_count: int, event_count: int, due_count: int, marking_scripts: int = 0) -> dict:
    score = _workload_score(lesson_count, event_count, due_count, marking_scripts)
    tone = _daily_load_tone(score)
    return {
        "date": target.isoformat(),
        "label": "Today" if target == today else target.strftime("%a"),
        "day_number": target.strftime("%-d/%-m"),
        "score": score,
        "tone": tone,
        "load": _daily_load_label(tone),
        "lessons": lesson_count,
        "events": event_count,
        "due": due_count,
        "marking_scripts": marking_scripts,
    }

def _weekday_neighbors(today, direction: int, count: int = 5) -> list:
    out = []
    cursor = today
    while len(out) < count:
        cursor = cursor + timedelta(days=direction)
        if cursor.weekday() < 5:
            out.append(cursor)
    if direction < 0:
        out.reverse()
    return out

def _load_days_for_dates(dates: list, today) -> list:
    if not dates:
        return []
    event_counts = {target.isoformat(): 0 for target in dates}
    due_counts = {target.isoformat(): 0 for target in dates}
    if google_ok():
        try:
            start = SGT.localize(datetime.combine(min(dates), datetime.min.time()))
            end = SGT.localize(datetime.combine(max(dates) + timedelta(days=1), datetime.min.time()))
            for event in gs.get_events_between(start, end):
                item = _event_to_agenda_item(event)
                if item["date"] in event_counts:
                    event_counts[item["date"]] += 1
        except Exception:
            pass
        try:
            date_set = set(due_counts.keys())
            for reminder in gs.get_reminders():
                due = reminder.get("due", "")
                if due in date_set:
                    due_counts[due] += 1
        except Exception:
            pass

    load_days = []
    for target in dates:
        lessons, _ = _lessons_for_date(target)
        key = target.isoformat()
        load_days.append(_daily_load_item(
            target,
            today,
            len(lessons),
            event_counts.get(key, 0),
            due_counts.get(key, 0),
            0,
        ))
    return load_days

def _rest_load_note(previous_week: list, next_week: list) -> str:
    upcoming = max(next_week, key=lambda item: item.get("score", 0), default=None)
    previous_scores = [int(item.get("score", 0) or 0) for item in previous_week]
    previous_average = round(sum(previous_scores) / len(previous_scores)) if previous_scores else 0
    if not upcoming:
        return "No workload pattern yet. Keep rest steady and check back once the week fills in."
    day = upcoming.get("label", "next week")
    score = int(upcoming.get("score", 0) or 0)
    if score >= 82:
        return f"Peak ahead: {day} looks full. Plan recovery the night before and keep that morning friction-free."
    if score >= 58:
        return f"{day} is the next packed day. Put one lighter pocket before it and one after it."
    if previous_average >= 58 and score < 58:
        return "Next week eases up compared with last week. Good window for recovery, admin cleanup, and one deeper task."
    return "No major spike ahead. Keep sleep and breaks regular so the quiet days actually restore you."

def build_daily_load(days: int = 7) -> dict:
    days = max(1, min(int(days or 7), 14))
    agenda = build_agenda_structured(days)
    today = datetime.now(SGT).date()
    marking_scripts = 0
    if google_ok():
        try:
            marking_scripts = sum(
                max(0, int(task.get("total_scripts") or 0) - int(task.get("marked_count") or 0))
                for task in gs.get_marking_tasks()
            )
        except Exception:
            marking_scripts = 0

    load_days = []
    for index, day in enumerate(agenda.get("days", [])):
        lesson_count = len(day.get("lessons", []))
        event_count = len(day.get("events", []))
        due_count = len(day.get("due", []))
        scripts_today = marking_scripts if index == 0 else 0
        date_obj = datetime.fromisoformat(day["date"]).date()
        load_days.append(_daily_load_item(date_obj, today, lesson_count, event_count, due_count, scripts_today))

    today_load = load_days[0] if load_days else {
        "score": 0,
        "tone": "green",
        "load": "Pretty chill",
        "lessons": 0,
        "events": 0,
        "due": 0,
        "marking_scripts": 0,
    }
    previous_week = _load_days_for_dates(_weekday_neighbors(today, -1), today)
    next_week = _load_days_for_dates(_weekday_neighbors(today, 1), today)
    return {
        "today": today_load,
        "days": load_days,
        "note": _daily_load_note(today_load),
        "previous_week": previous_week,
        "next_week": next_week,
        "rest_note": _rest_load_note(previous_week, next_week),
    }


def _proactive_seen_key(now: datetime | None = None) -> str:
    current = now or datetime.now(SGT)
    return f"proactive_intelligence_seen:{current.strftime('%Y-%m')}"


def _get_proactive_seen(now: datetime | None = None) -> dict:
    try:
        raw = gs.get_config(_proactive_seen_key(now)) or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _mark_proactive_seen(insight_id: str, now: datetime | None = None):
    current = now or datetime.now(SGT)
    seen = _get_proactive_seen(current)
    seen[str(insight_id)] = current.isoformat()
    try:
        gs.set_config(_proactive_seen_key(current), json.dumps(seen, ensure_ascii=False))
    except Exception as exc:
        logger.warning(f"Could not persist proactive intelligence seen state: {exc}")


def _task_due_days(task: dict, today: date) -> int:
    try:
        return (date.fromisoformat(task.get("due", "")) - today).days
    except Exception:
        return 999


def build_proactive_intelligence_insights(days: int = 7, now: datetime | None = None) -> list[dict]:
    """Build high-signal proactive suggestions from workload, due items, marking, and follow-ups."""
    if not google_ok():
        return []
    current = now or datetime.now(SGT)
    today = current.date()
    insights: list[dict] = []

    try:
        load = build_daily_load(days)
    except Exception as exc:
        logger.warning(f"Proactive load scan failed: {exc}")
        load = {}

    today_load = load.get("today", {}) if isinstance(load, dict) else {}
    next_days = load.get("days", []) if isinstance(load, dict) else []
    marking_scripts = int(today_load.get("marking_scripts", 0) or 0)
    today_score = int(today_load.get("score", 0) or 0)

    try:
        tasks = build_task_structured(days).get("items", [])
    except Exception as exc:
        logger.warning(f"Proactive task scan failed: {exc}")
        tasks = []
    urgent_tasks = [task for task in tasks if _task_due_days(task, today) <= 2]

    try:
        followups = gs.get_followups()
    except Exception:
        followups = []
    due_followups = [item for item in followups if item.get("due_date", "9999-12-31") <= (today + timedelta(days=1)).isoformat()]

    if today_score >= 58 and (urgent_tasks or marking_scripts):
        task_bits = []
        if urgent_tasks:
            task_bits.append(f"{len(urgent_tasks)} due item(s) within 48 hours")
        if marking_scripts:
            task_bits.append(f"{marking_scripts} unmarked script(s)")
        insights.append({
            "id": f"{today.isoformat()}:packed_due_marking",
            "kind": "workload",
            "priority": "high" if today_score >= 82 else "medium",
            "title": "Workload pinch point",
            "body": (
                f"Today looks {str(today_load.get('load', 'packed')).lower()} and has "
                f"{', '.join(task_bits)}. Clear one small due item or marking slice before the day gets noisy."
            ),
        })

    future_spike = max(next_days[1: min(len(next_days), days)], key=lambda item: int(item.get("score", 0) or 0), default=None)
    if future_spike and int(future_spike.get("score", 0) or 0) >= 70:
        spike_date = future_spike.get("date", "")
        before_spike = []
        for task in tasks:
            due = task.get("due", "")
            if due and due <= spike_date and _task_due_days(task, today) >= 0:
                before_spike.append(task)
        if before_spike:
            first = before_spike[0]
            insights.append({
                "id": f"{spike_date}:pre_spike_task:{first.get('id', 'x')}",
                "kind": "planning",
                "priority": "medium",
                "title": "Pre-empt the packed day",
                "body": (
                    f"{future_spike.get('label', spike_date)} is shaping up as {str(future_spike.get('load', 'packed')).lower()}. "
                    f"Move this before then if possible: {first.get('description', 'one due item')}."
                ),
            })

    if due_followups:
        followup = sorted(due_followups, key=lambda item: item.get("due_date", ""))[0]
        person = f"{followup.get('person')} - " if followup.get("person") else ""
        insights.append({
            "id": f"{today.isoformat()}:followup:{followup.get('id', 'x')}",
            "kind": "followup",
            "priority": "medium",
            "title": "Follow-up due",
            "body": f"{person}{followup.get('topic', 'A follow-up')} is due by {followup.get('due_date', today.isoformat())}.",
        })

    if not insights and current.hour < 11 and today_score <= 34:
        insights.append({
            "id": f"{today.isoformat()}:quiet_window",
            "kind": "opportunity",
            "priority": "low",
            "title": "Quiet window",
            "body": "Today looks lighter on paper. Good window to move one deeper project before admin expands to fill the room.",
        })

    order = {"high": 0, "medium": 1, "low": 2}
    unique = {}
    for insight in insights:
        unique.setdefault(insight["id"], insight)
    return sorted(unique.values(), key=lambda item: (order.get(item.get("priority", "medium"), 1), item["id"]))[:3]


def due_proactive_intelligence(now: datetime | None = None) -> list[dict]:
    current = now or datetime.now(SGT)
    if current.hour < _env_int("HIRA_PROACTIVE_INTELLIGENCE_START_HOUR", 6, 0):
        return []
    if current.hour > _env_int("HIRA_PROACTIVE_INTELLIGENCE_END_HOUR", 21, 0):
        return []
    seen = _get_proactive_seen(current)
    items = [item for item in build_proactive_intelligence_insights(now=current) if item["id"] not in seen]
    ranked = []
    for item in items:
        source = f"proactive_intelligence:{item['id']}"
        if _should_suppress_notification(source, "update", now=current):
            continue
        ranked.append((item, _notification_feedback_bias(source, "update", now=current)))
    ranked.sort(
        key=lambda pair: (
            {"high": 0, "medium": 1, "low": 2}.get(pair[0].get("priority", "medium"), 1),
            -pair[1],
            pair[0]["id"],
        )
    )
    return [item for item, _score in ranked]


def _priority_rank(priority: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(str(priority or "").strip().lower(), 1)


def _task_candidate_score(task: dict, today: date) -> int:
    score = 40
    days_due = _task_due_days(task, today)
    if days_due < 0:
        score += 34
    elif days_due == 0:
        score += 28
    elif days_due == 1:
        score += 22
    elif days_due <= 3:
        score += 14
    score += _priority_rank(task.get("priority", "")) * 6
    if task.get("next_action"):
        score += 4
    if task.get("effort", "") == "high":
        score += 2
    return min(100, score)


def _make_proactive_candidate(
    family: str,
    source: str,
    kind: str,
    title: str,
    body: str,
    score: int,
    priority: str = "medium",
    why: str = "",
    action_hint: str = "",
    event_date: str = "",
    metadata: dict | None = None,
) -> dict:
    source_clean = str(source or "").strip()
    feedback_bias = _notification_feedback_bias(source_clean, kind)
    final_score = max(0, min(100, int(score or 0) + feedback_bias))
    suppressed = _should_suppress_notification(source_clean, kind)
    return {
        "id": source_clean or f"{family}:{title}",
        "family": str(family or "").strip() or "general",
        "source": source_clean,
        "kind": str(kind or "update").strip() or "update",
        "title": str(title or "H.I.R.A").strip() or "H.I.R.A",
        "body": str(body or "").strip(),
        "score": final_score,
        "base_score": int(score or 0),
        "feedback_bias": feedback_bias,
        "priority": str(priority or "medium").strip() or "medium",
        "why": str(why or "").strip(),
        "action_hint": str(action_hint or "").strip(),
        "event_date": str(event_date or "").strip(),
        "suppressed": bool(suppressed),
        "metadata": dict(metadata or {}),
    }


def _dedupe_proactive_candidates(candidates: list[dict]) -> list[dict]:
    kept = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        key = str(candidate.get("source") or candidate.get("id") or "").strip()
        if not key:
            continue
        existing = kept.get(key)
        if not existing or int(candidate.get("score", 0) or 0) > int(existing.get("score", 0) or 0):
            kept[key] = candidate
    return list(kept.values())


def build_proactive_v2_queue(now: datetime | None = None, days: int = 7, families: set[str] | None = None) -> list[dict]:
    current = now or datetime.now(SGT)
    today = current.date()
    allowed = {str(item).strip() for item in families} if families else None
    candidates: list[dict] = []

    def allow(family: str) -> bool:
        return allowed is None or family in allowed

    if google_ok():
        try:
            if allow("nudge"):
                for nudge in gs.due_nudges(current):
                    candidates.append(_make_proactive_candidate(
                        "nudge",
                        f"nudge:{nudge['id']}",
                        "reminder",
                        "H.I.R.A nudge",
                        nudge.get("message", ""),
                        96,
                        priority="high",
                        why="Due now from a scheduled nudge.",
                        action_hint="Act now or snooze externally.",
                        event_date=str(nudge.get("send_at", "")),
                        metadata={"nudge_id": str(nudge.get("id", ""))},
                    ))
        except Exception as exc:
            logger.warning(f"Proactive v2 nudge scan failed: {exc}")

        try:
            if allow("checkin"):
                for checkin in gs.due_checkins(current) + _due_break_aware_checkins(current):
                    slot = str(checkin.get("due_slot", ""))
                    score = 78 if checkin.get("schedule_aware") else 72
                    why = f"Check-in window due{f' at {slot}' if slot else ''}."
                    candidates.append(_make_proactive_candidate(
                        "checkin",
                        f"checkin:{checkin['id']}",
                        "reminder",
                        "H.I.R.A check-in",
                        checkin.get("question", ""),
                        score,
                        priority="medium",
                        why=why,
                        action_hint="Reply yes/done once completed.",
                        metadata={"checkin_id": str(checkin.get("id", "")), "due_slot": slot},
                    ))
        except Exception as exc:
            logger.warning(f"Proactive v2 check-in scan failed: {exc}")

        try:
            if allow("followup"):
                for followup in gs.due_followups(today.isoformat()):
                    days_late = 0
                    try:
                        days_late = max(0, (today - date.fromisoformat(followup.get("due_date", today.isoformat()))).days)
                    except Exception:
                        days_late = 0
                    score = min(95, 76 + min(12, days_late * 4))
                    candidates.append(_make_proactive_candidate(
                        "followup",
                        f"followup:{followup['id']}",
                        "reminder",
                        "H.I.R.A follow-up",
                        _format_followup(followup),
                        score,
                        priority="high" if days_late else "medium",
                        why="Follow-up is due and still open.",
                        action_hint=f"Use /donefollowup {followup['id']} when settled.",
                        event_date=str(followup.get("due_date", "")),
                        metadata={"followup_id": str(followup.get("id", ""))},
                    ))
        except Exception as exc:
            logger.warning(f"Proactive v2 follow-up scan failed: {exc}")

        try:
            if allow("prayer"):
                due = _prayer_reminder_due(current)
                if due:
                    text = f"{due['label']} entered at {due['time']}. {due['note']}"
                    candidates.append(_make_proactive_candidate(
                        "prayer",
                        f"prayer:{due['key']}",
                        "reminder",
                        f"{due['label']} prayer",
                        text,
                        98,
                        priority="high",
                        why="Prayer time just entered.",
                        action_hint="Pray at the next available pocket.",
                        metadata={"prayer_key": str(due.get("key", ""))},
                    ))
        except Exception as exc:
            logger.warning(f"Proactive v2 prayer scan failed: {exc}")

        try:
            if allow("friday_khutbah"):
                khutbah_text = _friday_khutbah_heads_up_due(current)
                if khutbah_text:
                    candidates.append(_make_proactive_candidate(
                        "friday_khutbah",
                        "friday_khutbah",
                        "update",
                        "Friday khutbah",
                        khutbah_text,
                        82,
                        priority="medium",
                        why="Friday sermon heads-up is due.",
                        action_hint="Carry one point into Jumu'ah.",
                    ))
        except Exception as exc:
            logger.warning(f"Proactive v2 khutbah scan failed: {exc}")

        try:
            if allow("friday_checkin") and current.weekday() == 4 and current.hour >= 12:
                projects = gs.get_projects()
                if projects:
                    score = 74
                    body = "Friday is a good checkpoint for projects that could drift over the weekend."
                    candidates.append(_make_proactive_candidate(
                        "friday_checkin",
                        "friday_checkin",
                        "update",
                        "Weekly project check-in",
                        body,
                        score,
                        priority="medium",
                        why="Friday review window is open.",
                        action_hint="Review project statuses and next milestones.",
                    ))
        except Exception as exc:
            logger.warning(f"Proactive v2 Friday project scan failed: {exc}")

        try:
            if allow("weekly_planning") and current.weekday() in {6, 0}:
                score = 70 if current.weekday() == 6 else 62
                why = "Weekly planning window is open." if current.weekday() == 6 else "Week start planning is still useful now."
                candidates.append(_make_proactive_candidate(
                    "weekly_planning",
                    "weekly_planning",
                    "update",
                    "Weekly plan",
                    "Look ahead across calendar, reminders, follow-ups, and marking before the week gets noisy.",
                    score,
                    priority="medium",
                    why=why,
                    action_hint="Open the weekly plan and front-load one hard thing.",
                ))
        except Exception as exc:
            logger.warning(f"Proactive v2 weekly planning scan failed: {exc}")

    try:
        if allow("digest") and 5 <= current.hour <= 11:
            for entry in build_curated_digest_entries(now=current, limit=3, fetch_limit=4, record=False):
                item = entry.get("item", {}) if isinstance(entry.get("item"), dict) else {}
                title = str(item.get("title", "")).strip()
                if not title:
                    continue
                label = str(entry.get("label", "")).strip()
                source = str(item.get("source", "")).strip()
                why = str(entry.get("why", "")).strip() or "shortlist relevance"
                meta = " · ".join(part for part in [label, source] if part)
                body = title if not meta else f"{title}\n\n{meta}"
                candidates.append(_make_proactive_candidate(
                    "digest",
                    f"digest:{entry.get('key', '')}",
                    "update",
                    "Morning digest",
                    body,
                    max(58, int(entry.get("score", 0) or 0)),
                    priority="medium",
                    why=f"Fresh digest pick with {why}.",
                    action_hint="Open the full digest for the rest of the shortlist.",
                    metadata={"digest_key": str(entry.get("key", ""))},
                ))
        if allow("intelligence"):
            for insight in build_proactive_intelligence_insights(days=days, now=current):
                score = {"high": 84, "medium": 72, "low": 58}.get(insight.get("priority", "medium"), 72)
                candidates.append(_make_proactive_candidate(
                    "intelligence",
                    f"proactive_intelligence:{insight['id']}",
                    "update",
                    insight.get("title", "H.I.R.A heads-up"),
                    insight.get("body", ""),
                    score,
                    priority=insight.get("priority", "medium"),
                    why="Detected from workload, due items, marking, or follow-ups.",
                    action_hint="Use this to pre-empt friction before the day crowds in.",
                    metadata={"insight_id": str(insight.get("id", ""))},
                ))
    except Exception as exc:
        logger.warning(f"Proactive v2 intelligence scan failed: {exc}")

    try:
        if allow("task"):
            structured = build_task_structured(days)
            for task in structured.get("items", [])[:5]:
                score = _task_candidate_score(task, today)
                if score < 58:
                    continue
                due = task.get("due", "")
                why = "Task is overdue." if task.get("overdue") else f"Task due by {due or 'soon'}."
                candidates.append(_make_proactive_candidate(
                    "task",
                    f"task:{task.get('id', '')}",
                    "update",
                    task.get("description", "Task"),
                    task.get("next_action", "") or f"{due} {task.get('category', '')}".strip(),
                    score,
                    priority=task.get("priority", "") or ("high" if task.get("overdue") else "medium"),
                    why=why,
                    action_hint=task.get("next_action", "") or "Clear one concrete next action.",
                    event_date=due,
                    metadata={"task_id": str(task.get("id", "")), "due": due},
                ))
    except Exception as exc:
        logger.warning(f"Proactive v2 task scan failed: {exc}")

    queue = _dedupe_proactive_candidates(candidates)
    queue.sort(
        key=lambda item: (
            1 if item.get("suppressed") else 0,
            -int(item.get("score", 0) or 0),
            -_priority_rank(item.get("priority", "medium")),
            str(item.get("source", "")),
        )
    )
    return queue


def build_proactive_v2_snapshot(now: datetime | None = None, days: int = 7, limit: int = 3) -> dict:
    current = now or datetime.now(SGT)
    queue = build_proactive_v2_queue(now=current, days=days)
    ready = [item for item in queue if not item.get("suppressed")]
    suppressed = [item for item in queue if item.get("suppressed")]
    changed = []
    for item in ready[: max(3, limit)]:
        bias = int(item.get("feedback_bias", 0) or 0)
        if bias:
            changed.append(f"{item.get('title', 'Item')} is being {'boosted' if bias > 0 else 'downranked'} by recent feedback.")
    if suppressed:
        changed.append(f"{len(suppressed)} signal(s) suppressed because similar items were dismissed or marked not now.")
    return {
        "generated_at": current.strftime("%A, %-d %B %Y, %H:%M SGT"),
        "top": ready[: max(1, int(limit or 3))],
        "queue_count": len(queue),
        "ready_count": len(ready),
        "suppressed_count": len(suppressed),
        "changed": changed[:3],
    }


async def _dispatch_proactive_candidates(context, candidates: list[dict], limit: int = 3) -> int:
    sent_count = 0
    for candidate in candidates:
        if candidate.get("suppressed"):
            continue
        if sent_count >= max(1, int(limit or 3)):
            break
        title = str(candidate.get("title", "H.I.R.A")).strip() or "H.I.R.A"
        body = str(candidate.get("body", "")).strip()
        if not body:
            continue
        kind = str(candidate.get("kind", "update")).strip() or "update"
        source = str(candidate.get("source", "")).strip()
        family = str(candidate.get("family", "")).strip()
        if family == "checkin":
            telegram_text = f"*{title}*\n\n{body}\n\nReply `yes`, `done`, or `alhamdulillah` once it is done and I’ll stop asking for today."
        elif family == "followup":
            followup_id = candidate.get("metadata", {}).get("followup_id", "")
            telegram_text = f"*{title}*\n\n{body}\n\nUse `/donefollowup {followup_id}` when settled." if followup_id else f"*{title}*\n\n{body}"
        else:
            telegram_text = f"*{title}*\n\n{body}"
        if context is not None:
            await _send_telegram_notification(context, telegram_text)
        queued = _queue_app_notification(kind, title, body, source=source)
        if not queued:
            continue
        if family == "nudge":
            gs.mark_nudge_sent(candidate.get("metadata", {}).get("nudge_id", ""))
        elif family == "checkin":
            gs.mark_checkin_prompted(
                candidate.get("metadata", {}).get("checkin_id", ""),
                candidate.get("metadata", {}).get("due_slot", ""),
                datetime.now(SGT),
            )
        elif family == "followup":
            gs.mark_followup_prompted(candidate.get("metadata", {}).get("followup_id", ""), datetime.now(SGT).strftime("%Y-%m-%d"))
        elif family == "intelligence":
            _mark_proactive_seen(candidate.get("metadata", {}).get("insight_id", ""), datetime.now(SGT))
        sent_count += 1
    return sent_count

def _news_topics():
    if google_ok():
        try:
            return [(t["label"], t["query"]) for t in gs.get_news_topics()]
        except Exception:
            pass
    return [(label, query) for label, query in ss.DIGEST_TOPICS]


REQUIRED_DIGEST_LABEL_TERMS = ("f1", "liverpool")


def _is_required_digest_label(label: str) -> bool:
    clean = str(label or "").lower()
    return any(term in clean for term in REQUIRED_DIGEST_LABEL_TERMS)


def _digest_label_lens(label: str) -> tuple[str, int]:
    clean = str(label or "").lower()
    if any(term in clean for term in ("education", "moe", "sg education")):
        return ("school relevance", 10)
    if any(term in clean for term in ("ai", "developer", "app dev", "android", "ios", "macos", "design", "ui/ux", "nothing")):
        return ("product/build relevance", 8)
    if any(term in clean for term in ("liverpool", "f1")):
        return ("personal interest", 6)
    if any(term in clean for term in ("islam", "sg news")):
        return ("daily life relevance", 5)
    return ("shortlist relevance", 4)


def _curated_digest_score(label: str, item: dict, slot_index: int = 0) -> tuple[int, str]:
    lens, lens_bonus = _digest_label_lens(label)
    score = 50 + ss.news_quality_score(item) + lens_bonus
    title_text = str(item.get("title", "")).lower()
    if any(term in title_text for term in ("today", "new", "launch", "update", "policy", "developer", "release", "security")):
        score += 4
    score -= max(0, int(slot_index or 0)) * 2
    return min(100, score), lens


def build_curated_digest_entries(now: datetime | None = None, limit: int = 4, fetch_limit: int = 4, record: bool = False) -> list[dict]:
    current = now or datetime.now(SGT)
    seen = _recent_news_digest_keys(current)
    candidates = []
    seen_candidates = []
    used_keys = set()
    topics = _news_topics()
    for label, query in topics:
        items = ss.google_news(query, max_items=max(2, int(fetch_limit or 4)))
        for index, item in enumerate(items):
            key = ss.news_item_key(item)
            if not key or key in used_keys:
                continue
            score, lens = _curated_digest_score(label, item, slot_index=index)
            entry = {
                "label": label,
                "item": item,
                "key": key,
                "score": score,
                "why": lens,
            }
            if key in seen:
                seen_candidates.append(entry)
                used_keys.add(key)
                continue
            candidates.append(entry)
            used_keys.add(key)
    candidates.sort(key=lambda entry: (-int(entry.get("score", 0) or 0), str(entry.get("label", "")), str(entry.get("key", ""))))
    seen_candidates.sort(key=lambda entry: (-int(entry.get("score", 0) or 0), str(entry.get("label", "")), str(entry.get("key", ""))))

    chosen = []
    chosen_labels = set()

    for entry in candidates + seen_candidates:
        label = str(entry.get("label", "")).strip()
        label_key = label.lower()
        if not _is_required_digest_label(label) or label_key in chosen_labels:
            continue
        chosen.append(entry)
        chosen_labels.add(label_key)
        if len(chosen) >= max(1, int(limit or 4)):
            break

    for label, _query in topics:
        label_text = str(label or "").strip()
        label_key = label_text.lower()
        if not _is_required_digest_label(label_text) or label_key in chosen_labels:
            continue
        chosen.append({
            "label": label_text,
            "item": {
                "title": f"{label_text} check: no fresh headline returned by Google News RSS",
                "source": "H.I.R.A",
                "url": "",
            },
            "key": f"required-digest:{label_key}:{current.strftime('%Y-%m-%d')}",
            "score": 54,
            "why": "personal interest",
        })
        chosen_labels.add(label_key)
        if len(chosen) >= max(1, int(limit or 4)):
            break

    for entry in candidates:
        label = str(entry.get("label", "")).strip().lower()
        if label in chosen_labels:
            continue
        chosen.append(entry)
        chosen_labels.add(label)
        if len(chosen) >= max(1, int(limit or 4)):
            break

    if len(chosen) < max(1, int(limit or 4)):
        for entry in candidates:
            if entry in chosen:
                continue
            chosen.append(entry)
            if len(chosen) >= max(1, int(limit or 4)):
                break

    if not chosen:
        fallback = ss.pick_fresh_morning_digest_entries(
            topics=_news_topics(),
            seen_keys=seen,
            max_items_per_topic=1,
            fetch_limit=1,
        )
        chosen = [
            {**entry, "score": 55, "why": "shortlist relevance"}
            for entry in fallback[: max(1, int(limit or 4))]
        ]

    if record and chosen:
        _remember_news_digest_entries(chosen, now=current)
    return chosen[: max(1, int(limit or 4))]


def format_curated_digest(entries: list[dict]) -> str:
    lines = []
    for entry in entries or []:
        label = str(entry.get("label", "")).strip()
        item = entry.get("item") if isinstance(entry.get("item"), dict) else {}
        title = str(item.get("title", "")).strip()
        source = str(item.get("source", "")).strip()
        why = str(entry.get("why", "")).strip()
        if not title:
            continue
        meta = " · ".join(part for part in [label, source] if part)
        lines.append(f"- *{title}*{f' ({meta})' if meta else ''}")
        if why:
            lines.append(f"  Why it matters: {why}.")
    return "\n".join(lines).strip()


def build_curated_digest_snapshot(now: datetime | None = None, limit: int = 4) -> dict:
    current = now or datetime.now(SGT)
    entries = build_curated_digest_entries(now=current, limit=limit, fetch_limit=4, record=False)
    return {
        "generated_at": current.strftime("%A, %-d %B %Y, %H:%M SGT"),
        "items": [
            {
                "label": entry.get("label", ""),
                "title": entry.get("item", {}).get("title", ""),
                "source": entry.get("item", {}).get("source", ""),
                "url": entry.get("item", {}).get("url", ""),
                "why": entry.get("why", ""),
                "score": entry.get("score", 0),
            }
            for entry in entries
        ],
    }

def build_news_digest(query: str = "", max_items: int = 2) -> str:
    max_items = max(1, min(int(max_items or 2), 5))
    if query.strip():
        items = ss.google_news(query.strip(), max_items=max_items)
        return f"*News: {query.strip()}*\n\n{ss.format_news_items(items)}"

    curated = format_curated_digest(build_curated_digest_entries(limit=max_items, fetch_limit=4, record=False))
    return f"*Latest from your shortlist*\n\n{curated or 'No news found.'}"


TASTE_CALIBRATION_QUESTIONS = [
    {
        "id": "quality_bar",
        "question": "When H.I.R.A shows you news or analysis, what makes something worth your time?",
        "hint": "For example: original reporting, practical school relevance, product strategy, technical depth, spiritual usefulness.",
    },
    {
        "id": "sources",
        "question": "Which sources or styles should H.I.R.A trust, and which should it quietly avoid?",
        "hint": "Name outlets, newsletters, channels, or patterns like press releases, listicles, rumours, generic AI SEO posts.",
    },
    {
        "id": "design_taste",
        "question": "What product/design taste should H.I.R.A learn from you?",
        "hint": "Think Nothing OS, Apple, Japanese minimalism, school ops dashboards, tactical football visuals, or anything else.",
    },
    {
        "id": "business_lens",
        "question": "When connecting dots for GamePlan or Rūḥ, what commercial lens should H.I.R.A use?",
        "hint": "Examples: school leader buyer psychology, App Store risk, Singapore parent/teacher adoption, pricing, demo value.",
    },
    {
        "id": "islamic_content_tone",
        "question": "For Islamic reminders and reflections, what tone feels right?",
        "hint": "Gentle, scholarly, practical, Malay/Singapore context, short dalil, reflective, no preachiness, etc.",
    },
]


def taste_calibration_prompt() -> dict:
    profile = gs.get_taste_profile() if google_ok() else {}
    return {
        "profile": profile,
        "questions": TASTE_CALIBRATION_QUESTIONS,
    }


def save_taste_profile(answers: dict) -> dict:
    if not google_ok():
        raise ValueError("Google is not connected")
    current = gs.get_taste_profile()
    sources_text = str(answers.get("sources", "") or "").strip()
    next_profile = {
        **current,
        "quality_bar": answers.get("quality_bar", current.get("quality_bar", "")),
        "design_taste": answers.get("design_taste", current.get("design_taste", "")),
        "business_lens": answers.get("business_lens", current.get("business_lens", "")),
        "islamic_content_tone": answers.get("islamic_content_tone", current.get("islamic_content_tone", "")),
    }
    if sources_text:
        avoid_markers = ("avoid:", "ignore:", "skip:")
        lower = sources_text.lower()
        if any(marker in lower for marker in avoid_markers):
            trusted, _, avoid = sources_text.partition(";")
            next_profile["sources_to_trust"] = trusted.replace("trust:", "").strip()
            next_profile["sources_to_avoid"] = avoid.replace("avoid:", "").replace("ignore:", "").replace("skip:", "").strip()
        else:
            next_profile["sources_to_trust"] = sources_text
    return gs.set_taste_profile(next_profile)


def absorb_taste_hint(text: str) -> bool:
    if not google_ok():
        return False
    clean = str(text or "").strip()
    if len(clean) < 12:
        return False
    lower = clean.lower()
    taste_markers = (
        "i like", "i prefer", "my taste", "my style", "i hate", "i dislike",
        "avoid", "don't show me", "dont show me", "quality bar", "feels premium",
        "too noisy", "too cluttered", "not my vibe", "my vibe",
    )
    if not any(marker in lower for marker in taste_markers):
        return False
    try:
        profile = gs.get_taste_profile()
        field = "quality_bar"
        if any(marker in lower for marker in ("design", "ui", "font", "style", "aesthetic", "vibe", "premium", "cluttered")):
            field = "design_taste"
        if any(marker in lower for marker in ("source", "article", "news", "digest", "show me", "don't show me", "dont show me")):
            field = "sources_to_avoid" if any(marker in lower for marker in ("avoid", "don't", "dont", "hate", "dislike")) else "sources_to_trust"
        existing = profile.get(field, "")
        existing_text = ", ".join(existing) if isinstance(existing, list) else str(existing or "").strip()
        hint = clean[:240]
        if hint.lower() in existing_text.lower():
            return False
        profile[field] = f"{existing_text}\n- {hint}".strip() if existing_text else f"- {hint}"
        gs.set_taste_profile(profile)
        return True
    except Exception as exc:
        logger.warning(f"Taste hint capture failed: {exc}")
        return False


def _insight(key: str, title: str, body: str, score: int, reason: str, actions: list[str] | None = None) -> dict:
    return {
        "key": key,
        "title": title,
        "body": body,
        "score": max(0, min(100, int(score))),
        "reason": reason,
        "actions": actions or [],
    }


def build_project_radar(projects: list[dict] | None = None, topic_labels: list[str] | None = None) -> list[dict]:
    projects = projects or []
    topic_labels = topic_labels or [label.lower() for label, _ in _news_topics()]
    project_names = {str(p.get("project", "")).lower(): p for p in projects if p.get("project")}
    radar = []
    if "gameplan" in project_names:
        if any("education" in label or "sg" in label for label in topic_labels):
            radar.append(_insight(
                "radar:gameplan:sg-education",
                "SG education may shape GamePlan positioning",
                "Watch for school policy, CCA, admin workload, and student development angles that sharpen the pitch.",
                74,
                "GamePlan sells into Singapore schools, and your watchlist includes SG education.",
                ["Open latest SG Education headlines", "Draft one pitch-deck implication"],
            ))
        if any("f1" in label or "liverpool" in label for label in topic_labels):
            radar.append(_insight(
                "radar:gameplan:sports-models",
                "Sports news may contain reusable coaching models",
                "Training setups, tactical language, or elite team routines can become Football CCA/GamePlan examples.",
                62,
                "You track sports and run Football CCA while building GamePlan.",
                ["Capture coaching example"],
            ))
    if any(name in project_names for name in ("ruh", "rūḥ")):
        if any("islam" in label for label in topic_labels):
            radar.append(_insight(
                "radar:ruh:islam-content",
                "Islam reading may feed Rūḥ content taste",
                "Look for gentle, useful reflections that match your preferred Islamic reminder tone.",
                70,
                "Rūḥ is active and Islamic content is now first-class in H.I.R.A.",
                ["Save as Rūḥ content idea"],
            ))
        if any("ios" in label or "developer" in label for label in topic_labels):
            radar.append(_insight(
                "radar:ruh:developer-risk",
                "Developer updates may affect Rūḥ shipping risk",
                "App Store, iOS, or framework changes can matter before they become blockers.",
                68,
                "Rūḥ is in the iOS/App Store lane and you track developer updates.",
                ["Check release risk"],
            ))
    if any("nothing" in label for label in topic_labels):
        radar.append(_insight(
            "radar:taste:nothing",
            "Nothing updates are product taste signals",
            "Scan them for UI tone, launch language, and product restraint, not only gadget news.",
            55,
            "You explicitly care about Nothing products/OS and product aesthetics.",
            ["Save product taste note"],
        ))
    return radar


def build_anticipatory_insight_items(days: int = 2, limit: int = 6) -> list[dict]:
    now = datetime.now(SGT)
    today = now.date()
    insights = []

    projects = []
    reminders = []
    if google_ok():
        try:
            projects = gs.get_projects()
        except Exception:
            projects = []
        try:
            reminders = gs.enriched_reminders()
        except Exception:
            reminders = []

    if google_ok():
        try:
            events = gs.get_tomorrow_events()
            for event in events[:3]:
                title = event.get("summary", "")
                text = " ".join([title, event.get("location", ""), event.get("description", "")]).lower()
                related = [
                    p.get("project", "")
                    for p in projects
                    if p.get("project") and p.get("project", "").lower() in text
                ]
                if related:
                    insights.append(_insight(
                        f"calendar-project:{title}",
                        "Cross-check",
                        f"Tomorrow's calendar has {title}; it appears connected to {', '.join(related)}.",
                        76,
                        "Calendar language overlaps with an active project.",
                        ["Open project radar"],
                    ))
        except Exception:
            pass

    urgent = []
    for task in reminders:
        due = task.get("due", "9999-12-31")
        if due <= (today + timedelta(days=max(1, min(days, 7)))).isoformat():
            urgent.append(task)
    for task in sorted(urgent, key=lambda item: _task_priority_score(item, today))[:2]:
        next_action = f" Next move: {task['next_action']}." if task.get("next_action") else ""
        insights.append(_insight(
            f"task:{task.get('id', task.get('description', ''))}",
            "Do-not-miss",
            f"{task.get('description', '')} is due {task.get('due', '')}.{next_action}",
            88 if task.get("due", "") <= today.isoformat() else 72,
            "Near-term task with due-date pressure.",
            ["Mark done", "Create follow-up"],
        ))

    topic_labels = [label.lower() for label, _ in _news_topics()]
    insights.extend(build_project_radar(projects, topic_labels))

    if not insights:
        return []

    deduped = []
    seen = set()
    feedback = gs.get_insight_feedback() if google_ok() else []
    penalties = {item.get("target", "") for item in feedback[-30:] if item.get("rating") in {"not_now", "not_useful"}}
    for insight in sorted(insights, key=lambda item: item["score"], reverse=True):
        key = insight["key"]
        if key in seen:
            continue
        if key in penalties:
            insight = {**insight, "score": max(0, insight["score"] - 18)}
            if insight["score"] < 45:
                continue
        seen.add(key)
        deduped.append(insight)
        if len(deduped) >= limit:
            break
    return deduped


def build_anticipatory_insights(days: int = 2, limit: int = 6) -> str:
    items = build_anticipatory_insight_items(days, limit)
    if not items:
        return "No anticipatory signals yet. Add projects, tasks, or upcoming context and I will start connecting the dots."
    lines = ["*Anticipatory signals*\n"]
    for item in items:
        actions = f" Actions: {', '.join(item['actions'])}." if item.get("actions") else ""
        lines.append(f"- *{item['title']}* ({item['score']}/100): {item['body']} _Why: {item['reason']}._{actions}")
    return "\n".join(lines)

def _parse_iso_sgt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return SGT.localize(dt)
    return dt.astimezone(SGT)

def _format_nudge(nudge: dict) -> str:
    try:
        send_at = _parse_iso_sgt(nudge["send_at"])
        when = send_at.strftime("%a %-d %b, %H:%M")
    except Exception:
        when = nudge.get("send_at", "")
    return f"`[{nudge['id']}]` *{when}* - {nudge['message']}"

def _create_nudge(message: str, send_at: str) -> dict:
    send_dt = _parse_iso_sgt(send_at)
    if send_dt <= datetime.now(SGT):
        raise ValueError("Nudge time must be in the future.")
    return gs.add_nudge(message, send_dt.isoformat())

AFFIRMATIVE_REPLIES = {
    "yes", "y", "done", "did", "completed", "complete", "settled", "ok",
    "okay", "yup", "yep", "yes done", "done already", "alhamdulillah",
    "alhamdulillah done", "dah", "sudah", "dah buat", "sudah buat"
}

def _is_affirmative(text: str) -> bool:
    clean = " ".join(text.lower().replace(".", " ").replace("!", " ").split())
    return clean in AFFIRMATIVE_REPLIES or clean.startswith("yes ") or clean.startswith("done ")

def _parse_checkin_times(raw: str) -> list:
    times = []
    for part in raw.replace(";", ",").split(","):
        value = part.strip()
        if not value:
            continue
        parsed = datetime.strptime(value, "%H:%M")
        times.append(parsed.strftime("%H:%M"))
    return sorted(set(times))

def _format_checkin(checkin: dict) -> str:
    status = "done today" if checkin.get("last_completed_date") == datetime.now(SGT).strftime("%Y-%m-%d") else "active"
    if checkin.get("schedule_aware"):
        timing = (
            f"around breaks ({checkin.get('target_count', 3)}x, "
            f"{checkin.get('window_start', '08:00')}-{checkin.get('window_end', '21:30')})"
        )
    else:
        timing = f"at {', '.join(checkin['times'])}"
    return f"`[{checkin['id']}]` *{checkin['name']}* {timing} - {status}\n{checkin['question']}"

def _score_text_match(query: str, value: str) -> float:
    query = " ".join((query or "").lower().split())
    value = " ".join((value or "").lower().split())
    if not query or not value:
        return 0
    ratio = SequenceMatcher(None, query, value).ratio()
    contains = 0.35 if query in value or value in query else 0
    query_words = set(query.split())
    value_words = set(value.split())
    overlap = len(query_words & value_words) / max(1, len(query_words | value_words))
    return ratio + contains + overlap

def _find_best_reminder(query: str):
    reminders = gs.get_reminders()
    if not reminders:
        return None, 0
    scored = sorted(
        ((r, _score_text_match(query, f"{r['description']} {r['category']} {r['due']}")) for r in reminders),
        key=lambda item: item[1],
        reverse=True,
    )
    return scored[0]

def _find_best_followup(query: str):
    followups = gs.get_followups()
    if not followups:
        return None, 0
    scored = sorted(
        ((f, _score_text_match(query, f"{f['person']} {f['topic']} {f['due_date']} {f['channel']}")) for f in followups),
        key=lambda item: item[1],
        reverse=True,
    )
    return scored[0]

def _event_text(event: dict) -> str:
    start = event.get("start", {})
    end = event.get("end", {})
    return " ".join([
        event.get("summary", ""),
        event.get("location", ""),
        event.get("description", ""),
        start.get("dateTime", start.get("date", "")),
        end.get("dateTime", end.get("date", "")),
    ])

def _looks_like_duplicate_delete_query(query: str) -> bool:
    text = " ".join((query or "").lower().split())
    if not text:
        return False
    return bool(re.search(r"\b(duplicate|dup|copy|instance)\b", text)) or bool(
        re.search(r"\bremove\s+(?:1|one)\b", text)
    )

def _normalized_event_summary(event: dict) -> str:
    summary = re.sub(r"[^a-z0-9]+", " ", (event.get("summary", "") or "").lower())
    return " ".join(summary.split())

def _event_duplicate_key(event: dict) -> tuple[str, str, str]:
    start = event.get("start", {}).get("dateTime", event.get("start", {}).get("date", ""))
    end = event.get("end", {}).get("dateTime", event.get("end", {}).get("date", ""))
    return (_normalized_event_summary(event), start, end)

def _duplicate_event_groups(events: list[dict]) -> list[list[dict]]:
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for event in events:
        key = _event_duplicate_key(event)
        if key[0] and key[1]:
            grouped[key].append(event)
    return [group for group in grouped.values() if len(group) >= 2]

def _duplicate_group_text(group: list[dict]) -> str:
    if not group:
        return ""
    event = group[0]
    return f"{event.get('summary', '')} {_event_when_text(event)}"

def _event_when_text(event: dict) -> str:
    raw_start = event.get("start", {}).get("dateTime", event.get("start", {}).get("date", ""))
    raw_end = event.get("end", {}).get("dateTime", event.get("end", {}).get("date", ""))
    try:
        if "T" in raw_start:
            start_dt = datetime.fromisoformat(raw_start).astimezone(SGT)
            end_dt = datetime.fromisoformat(raw_end).astimezone(SGT) if raw_end else None
            end_text = f"–{end_dt.strftime('%H:%M')}" if end_dt else ""
            return f"{start_dt.strftime('%a %-d %b %H:%M')}{end_text}"
        return datetime.fromisoformat(raw_start).strftime("%a %-d %b")
    except Exception:
        return raw_start

def _find_best_calendar_event(query: str, days_back: int = 7, days_ahead: int = 30):
    now = datetime.now(SGT)
    start = now - timedelta(days=max(0, int(days_back or 7)))
    end = now + timedelta(days=max(1, int(days_ahead or 30)))
    events = gs.get_events_between(start, end)
    if not events:
        return None, 0
    scored = sorted(
        ((event, _score_text_match(query, _event_text(event))) for event in events),
        key=lambda item: item[1],
        reverse=True,
    )
    return scored[0]

def _resolve_calendar_event_for_deletion(query: str, days_back: int = 7, days_ahead: int = 30):
    now = datetime.now(SGT)
    start = now - timedelta(days=max(0, int(days_back or 7)))
    end = now + timedelta(days=max(1, int(days_ahead or 30)))
    events = gs.get_events_between(start, end)
    if not events:
        return None, 0

    if _looks_like_duplicate_delete_query(query):
        duplicate_groups = _duplicate_event_groups(events)
        if len(duplicate_groups) == 1:
            group = duplicate_groups[0]
            return group[0], max(0.8, _score_text_match(query, _duplicate_group_text(group)))
        if duplicate_groups:
            scored_groups = sorted(
                ((group, _score_text_match(query, _duplicate_group_text(group))) for group in duplicate_groups),
                key=lambda item: item[1],
                reverse=True,
            )
            best_group, best_score = scored_groups[0]
            next_score = scored_groups[1][1] if len(scored_groups) > 1 else 0
            if best_score >= 0.45 and best_score >= next_score + 0.12:
                return best_group[0], best_score
            return None, 0

    scored = sorted(
        ((event, _score_text_match(query, _event_text(event))) for event in events),
        key=lambda item: item[1],
        reverse=True,
    )
    return scored[0]

def _find_best_marking_task(query: str):
    tasks = gs.get_marking_tasks()
    if not tasks:
        return None, 0
    if str(query).strip().isdigit():
        for task in tasks:
            if str(task["id"]) == str(query).strip():
                return task, 1.0
    scored = sorted(
        ((task, _score_text_match(query, f"{task['title']} {task.get('notes', '')}")) for task in tasks),
        key=lambda item: item[1],
        reverse=True,
    )
    return scored[0]

def _is_marking_reminder(reminder: dict) -> bool:
    text = f"{reminder.get('description', '')} {reminder.get('category', '')}".lower()
    return bool(re.search(
        r"\b(marking|marked|scripts?|papers?|compositions?|kefahaman|karangan|worksheets?|worksheet)\b",
        text,
    ))

def complete_reminder_by_id(reminder_id: str) -> tuple[bool, dict | None]:
    reminders = gs.get_reminders(include_done=True)
    reminder = next((item for item in reminders if str(item.get("id")) == str(reminder_id)), None)
    ok = gs.mark_done(reminder_id)
    synced_marking = None
    if ok and reminder and _is_marking_reminder(reminder):
        query = f"{reminder.get('description', '')} {reminder.get('category', '')}"
        marking, score = _find_best_marking_task(query)
        if marking and score >= 0.35:
            synced_marking = gs.update_marking_progress(marking["id"], done=True)
    return ok, synced_marking

def _marking_counts(task: dict) -> tuple[int, int, int | None]:
    total = int(task.get("total_scripts") or 0)
    marked = int(task.get("marked_count") or 0)
    outstanding = max(0, total - marked) if total else None
    return total, marked, outstanding

def _days_old_text(days_old: int) -> str:
    if days_old == 0:
        return "today"
    if days_old == 1:
        return "1 day ago"
    return f"{days_old} days ago"

def _format_marking_task(task: dict) -> str:
    total, marked, outstanding = _marking_counts(task)
    stack = "stack" if int(task.get("stack_count") or 1) == 1 else "stacks"
    prefix = task["title"]
    collected = task.get("collected_date", "")
    age = ""
    if collected:
        try:
            days_old = (datetime.now(SGT).date() - date.fromisoformat(collected)).days
            age = f" Collected {collected} ({_days_old_text(days_old)})."
        except Exception:
            age = f" Collected {collected}."
    if total:
        return f"{prefix}: {marked} of {total} scripts marked, {outstanding} outstanding.{age}"
    return f"{prefix}: {marked} scripts marked so far. Total scripts not set yet ({task.get('stack_count', 1)} {stack}).{age}"

def build_marking_brief() -> str:
    if not google_ok():
        return "Google is not connected."
    tasks = gs.get_marking_tasks()
    if not tasks:
        return "No active marking stacks."
    lines = ["*Outstanding marking*"]
    for task in tasks:
        lines.append(f"- {_format_marking_task(task)}")
    return "\n".join(lines)

def _task_priority_score(task: dict, today: date) -> tuple:
    priority_order = {"urgent": 0, "high": 1, "medium": 2, "low": 3}
    due = task.get("due", "9999-12-31")
    try:
        due_days = (date.fromisoformat(due) - today).days
    except Exception:
        due_days = 999
    priority = priority_order.get(task.get("priority", "medium"), 2)
    effort = {"quick": 0, "small": 1, "medium": 2, "deep": 3, "large": 3}.get(task.get("effort", "medium"), 2)
    return (priority, due_days, effort, task.get("description", ""))

def build_task_brief(days: int = 7) -> str:
    today = datetime.now(SGT).date()
    end_date = today + timedelta(days=max(1, min(int(days or 7), 30)))
    tasks = [
        task for task in gs.enriched_reminders()
        if task.get("due", "9999-12-31") <= end_date.isoformat()
    ]
    if not tasks:
        return "No active tasks in that window."
    lines = [f"*Task brief* - now to {end_date.strftime('%-d %b')}\n"]
    for task in sorted(tasks, key=lambda item: _task_priority_score(item, today))[:12]:
        next_action = f"\n  Next: {task['next_action']}" if task.get("next_action") else ""
        overdue = " OVERDUE" if task["due"] < today.isoformat() else ""
        lines.append(
            f"- `[{task['id']}]` {task['due']}{overdue} - {task['description']}{next_action}"
        )
    return "\n".join(lines)


def build_task_structured(days: int = 7) -> dict:
    today = datetime.now(SGT).date()
    window = max(1, min(int(days or 7), 30))
    end_date = today + timedelta(days=window)
    tasks = [
        task for task in gs.enriched_reminders()
        if task.get("due", "9999-12-31") <= end_date.isoformat()
    ]
    items = []
    for task in sorted(tasks, key=lambda item: _task_priority_score(item, today))[:30]:
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
        "generated_at": datetime.now(SGT).strftime("%A, %-d %B %Y, %H:%M SGT"),
        "end_date": end_date.isoformat(),
        "items": items,
    }

def _format_followup(followup: dict) -> str:
    person = f"{followup['person']} - " if followup.get("person") else ""
    channel = f" via {followup['channel']}" if followup.get("channel") else ""
    notes = f"\n  {followup['notes']}" if followup.get("notes") else ""
    return f"`[{followup['id']}]` {followup['due_date']} - {person}{followup['topic']}{channel}{notes}"

def build_files_index() -> str:
    lines = ["*File memory*\n"]
    for item in WORK_DRIVE_REFERENCES:
        lines.append(f"- {item}")
    if not google_ok():
        return "\n".join(lines) if len(lines) > 1 else "Google is not connected."
    try:
        files = gs.get_memory().get("files", [])
        for item in files[-25:]:
            lines.append(f"- {item}")
        return "\n".join(lines) if len(lines) > 1 else "No files remembered yet."
    except Exception as e:
        if len(lines) > 1:
            lines.append(f"- Live file memory unavailable: {e}")
            return "\n".join(lines)
        return f"File memory unavailable: {e}"

def _hm_to_minutes(value: str) -> int:
    parsed = datetime.strptime(value.strip(), "%H:%M")
    return parsed.hour * 60 + parsed.minute

def _minutes_to_hm(value: int) -> str:
    value = max(0, min(23 * 60 + 59, int(value)))
    return f"{value // 60:02d}:{value % 60:02d}"

def _merge_busy_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    intervals = sorted((s, e) for s, e in intervals if e > s)
    if not intervals:
        return []
    merged = [intervals[0]]
    for start, end in intervals[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged

def _event_busy_intervals_for_date(target: date) -> list[tuple[int, int]]:
    intervals = []
    start_of_day = SGT.localize(datetime.combine(target, dt_time.min))
    end_of_day = start_of_day + timedelta(days=1)
    for event in gs._fetch_events(start_of_day, end_of_day):
        start_raw = event.get("start", {}).get("dateTime")
        end_raw = event.get("end", {}).get("dateTime")
        if not start_raw or not end_raw:
            continue
        try:
            start_dt = datetime.fromisoformat(start_raw).astimezone(SGT)
            end_dt = datetime.fromisoformat(end_raw).astimezone(SGT)
        except Exception:
            continue
        if start_dt.date() > target or end_dt.date() < target:
            continue
        start_min = max(0, start_dt.hour * 60 + start_dt.minute)
        end_min = min(24 * 60, end_dt.hour * 60 + end_dt.minute)
        intervals.append((start_min, end_min))
    return intervals


def _calendar_events_for_date(target: date) -> list[dict]:
    start_of_day = SGT.localize(datetime.combine(target, dt_time.min))
    end_of_day = start_of_day + timedelta(days=1)
    try:
        return gs._fetch_events(start_of_day, end_of_day)
    except Exception as exc:
        logger.warning(f"Calendar availability read failed: {exc}")
        return []


def _event_minutes(event: dict) -> tuple[int, int] | None:
    start_raw = event.get("start", {}).get("dateTime")
    end_raw = event.get("end", {}).get("dateTime")
    if not start_raw or not end_raw:
        return None
    try:
        start_dt = datetime.fromisoformat(start_raw).astimezone(SGT)
        end_dt = datetime.fromisoformat(end_raw).astimezone(SGT)
    except Exception:
        return None
    return max(0, start_dt.hour * 60 + start_dt.minute), min(24 * 60, end_dt.hour * 60 + end_dt.minute)


def find_available_training_slots(
    days: int = 7,
    duration_minutes: int = 60,
    window_start: str = "14:00",
    window_end: str = "18:30",
    avoid_keywords: list[str] | None = None,
    purpose: str = "training",
) -> str:
    if not google_ok():
        return "Google Calendar is not connected, so I cannot safely suggest availability slots."
    today = datetime.now(SGT).date()
    scan_days = max(1, min(int(days or 7), 14))
    duration = max(30, min(int(duration_minutes or 60), 180))
    start_min = _hm_to_minutes(window_start or "14:00")
    end_min = _hm_to_minutes(window_end or "18:30")
    if end_min <= start_min:
        end_min = start_min + duration
    avoid = [str(item).strip().lower() for item in (avoid_keywords or ["cca", "football"]) if str(item).strip()]

    slots = []
    skipped = []
    for offset in range(scan_days):
        target = today + timedelta(days=offset)
        if target.weekday() >= 5:
            continue
        lessons, wt_label = _lessons_for_date(target)
        events = _calendar_events_for_date(target)
        event_text = " ".join(_event_text(event) for event in events).lower()
        if avoid and any(keyword in event_text for keyword in avoid):
            skipped.append(f"{target.strftime('%a %-d %b')}: avoided due to {', '.join(k for k in avoid if k in event_text) or 'blocked keyword'}")
            continue

        busy = []
        reasons = []
        for lesson in lessons:
            try:
                lesson_start = _hm_to_minutes(lesson["start"])
                lesson_end = _hm_to_minutes(lesson["end"])
            except Exception:
                continue
            busy.append((lesson_start, lesson_end))
            if lesson_end > start_min:
                reasons.append(f"lesson until {lesson['end']}")
        for event in events:
            minutes = _event_minutes(event)
            if not minutes:
                continue
            busy.append(minutes)
            if minutes[1] > start_min and minutes[0] < end_min:
                reasons.append(f"{event.get('summary', 'calendar event')} {_minutes_to_hm(minutes[0])}-{_minutes_to_hm(minutes[1])}")

        busy = _merge_busy_intervals([
            (max(start_min, start), min(end_min, end))
            for start, end in busy
            if end > start_min and start < end_min
        ])
        cursor = start_min
        for busy_start, busy_end in busy:
            if busy_start - cursor >= duration:
                slots.append((target, cursor, busy_start, wt_label, reasons))
            cursor = max(cursor, busy_end)
        if end_min - cursor >= duration:
            slots.append((target, cursor, end_min, wt_label, reasons))

    if not slots:
        skip_note = "\n".join(f"- {item}" for item in skipped[:5])
        return (
            f"No clear after-school {purpose or 'training'} slots found in the next {scan_days} days "
            f"between {_minutes_to_hm(start_min)} and {_minutes_to_hm(end_min)} after checking timetable and calendar."
            + (f"\nAvoided days:\n{skip_note}" if skip_note else "")
        )

    ranked = sorted(slots, key=lambda item: (item[0], item[2] - item[1], -item[1]))[:5]
    lines = [
        f"Checked timetable + Google Calendar for after-school {purpose or 'training'} slots.",
        f"Constraints: {duration} minutes, {_minutes_to_hm(start_min)}-{_minutes_to_hm(end_min)}, avoid days with {', '.join(avoid) or 'no extra keywords'}.",
        "",
        "Best options:",
    ]
    for target, slot_start, slot_end, wt_label, reasons in ranked:
        reason = "; ".join(dict.fromkeys(reasons[-3:])) if reasons else "no lesson/calendar conflict in this window"
        week = f", {wt_label} week" if wt_label else ""
        lines.append(f"- {target.strftime('%a %-d %b')}{week}: {_minutes_to_hm(slot_start)}-{_minutes_to_hm(slot_end)} ({reason})")
    if skipped:
        lines.append("\nAvoided:")
        lines.extend(f"- {item}" for item in skipped[:5])
    return "\n".join(lines)


def _lesson_busy_intervals_for_date(target: date) -> list[tuple[int, int, str]]:
    intervals = []
    lessons, _ = _lessons_for_date(target)
    for lesson in lessons:
        try:
            start = _hm_to_minutes(lesson["start"])
            end = _hm_to_minutes(lesson["end"])
        except Exception:
            continue
        label = f"{lesson.get('subject', '')} {lesson.get('description', '')}".strip()
        intervals.append((start, end, label))
    return intervals


def _prayer_plan_for_date(target: date) -> list[dict]:
    plan = []
    lesson_busy = _lesson_busy_intervals_for_date(target)

    for prayer in isl.prayer_schedule(target):
        minute = _hm_to_minutes(prayer["time"])
        note = "Pray as soon as it enters."
        blocked_until = None
        blocker = ""
        for start, end, label in lesson_busy:
            if start <= minute < end:
                blocked_until = end
                blocker = label or "lesson"
                break
        if blocked_until is not None:
            note = f"During {blocker}; pray as soon as it ends around {_minutes_to_hm(blocked_until)}."
        plan.append({**prayer, "note": note, "blocked_until": blocked_until, "blocker": blocker})
    return plan


def build_islamic_brief(target: date | None = None) -> str:
    target = target or datetime.now(SGT).date()
    try:
        prayer_line = isl.format_prayer_times(target)
        hijri = isl.hijri_context(target)
        reflection = isl.daily_reflection(target)
        fasting = isl.is_sunnah_fasting_day(target)
        lines = [f"*Islamic rhythm* - {hijri}", prayer_line]
        if fasting:
            lines.append(f"*Fasting:* {fasting}")
        lines.append(f"*Reflection:* {reflection['text']} _({reflection['ref']})_")
        if target.weekday() == 4:
            try:
                khutbah = isl.latest_khutbah(target)
                if khutbah.get("date") == target.isoformat():
                    lines.append(f"*Jumu'ah khutbah:* {khutbah['title']} - {khutbah.get('summary', '').strip()}")
            except Exception as exc:
                logger.info("MUIS khutbah brief unavailable: %s", exc)
        zohor_asar = [item for item in _prayer_plan_for_date(target) if item["key"] in {"zohor", "asar", "maghrib"}]
        for item in zohor_asar:
            if item.get("blocked_until") is not None:
                lines.append(f"- {item['label']} {item['time']}: {item['note']}")
        return "\n".join(lines)
    except Exception as exc:
        return f"Islamic rhythm unavailable: {exc}"


def _normalise_prayer_key(value: str = "") -> str:
    clean = re.sub(r"[^a-z]", "", (value or "").lower())
    aliases = {
        "fajr": "subuh",
        "subuh": "subuh",
        "syuruk": "syuruk",
        "sunrise": "syuruk",
        "zohor": "zohor",
        "zuhur": "zohor",
        "zuhr": "zohor",
        "dhuhr": "zohor",
        "asar": "asar",
        "asr": "asar",
        "maghrib": "maghrib",
        "isyak": "isyak",
        "isha": "isyak",
        "ishak": "isyak",
    }
    return aliases.get(clean, "")


def build_muis_prayer_time_brief(target_text: str = "", prayer: str = "") -> str:
    today = datetime.now(SGT).date()
    target = today
    clean_date = (target_text or "").strip().lower()
    if clean_date:
        if clean_date == "today":
            target = today
        elif clean_date == "tomorrow":
            target = today + timedelta(days=1)
        else:
            target = date.fromisoformat(clean_date)

    record = isl.get_prayer_times(target)
    source = "MUIS Singapore prayer timetable"
    day = record.get("day") or target.strftime("%a")
    key = _normalise_prayer_key(prayer)
    if key:
        if not record.get(key):
            raise ValueError(f"No MUIS time found for {prayer} on {target.isoformat()}")
        label = isl.PRAYER_LABELS.get(key, key.title())
        return f"{source}: {target.isoformat()} ({day}) - {label} {record[key]}"

    parts = [f"{isl.PRAYER_LABELS[key]} {record[key]}" for key in isl.PRAYER_KEYS if record.get(key)]
    return f"{source}: {target.isoformat()} ({day}) - " + " · ".join(parts)


def _parse_optional_date(target_text: str = "") -> date:
    today = datetime.now(SGT).date()
    clean_date = (target_text or "").strip().lower()
    if not clean_date or clean_date == "today":
        return today
    if clean_date == "tomorrow":
        return today + timedelta(days=1)
    return date.fromisoformat(clean_date)


def build_muis_khutbah_brief(target_text: str = "", language: str = "English") -> str:
    target = _parse_optional_date(target_text)
    khutbah = isl.latest_khutbah(target, language or "English")
    date_label = date.fromisoformat(khutbah["date"]).strftime("%-d %B %Y")
    lines = [
        f"*Friday khutbah heads-up* - MUIS, {date_label}",
        f"*{khutbah['title']}*",
    ]
    if khutbah.get("summary"):
        lines.append(khutbah["summary"])
    points = khutbah.get("key_points") or []
    if points:
        lines.append("*Listen for:*")
        lines.extend(f"- {point}" for point in points[:4])
    takeaway = points[0] if points else khutbah.get("summary", "")
    if takeaway:
        lines.append(f"*Before Jumu'ah:* carry this question in: how does this apply to my choices today?")
    if khutbah.get("url"):
        lines.append(f"Source: {khutbah['url']}")
    if khutbah.get("pdf_url"):
        lines.append(f"PDF: {khutbah['pdf_url']}")
    return "\n".join(lines)


def _prayer_reminder_due(now: datetime) -> dict | None:
    now = now.astimezone(SGT)
    today_key = now.strftime("%Y-%m-%d")
    now_minute = now.hour * 60 + now.minute
    window_minutes = _env_int("HIRA_PRAYER_REMINDER_WINDOW_MINUTES", 20, 3)
    try:
        plan = _prayer_plan_for_date(now.date())
    except Exception as exc:
        logger.warning("Prayer reminder plan unavailable: %s", exc)
        return None

    for item in plan:
        key = f"prayer_prompt:{today_key}:{item['key']}"
        try:
            already_prompted = bool(gs.get_config(key))
        except Exception as exc:
            logger.warning("Prayer prompt config read unavailable: %s", exc)
            already_prompted = key in _PRAYER_PROMPT_FALLBACK_KEYS
        if already_prompted:
            continue
        prayer_minute = _hm_to_minutes(item["time"])
        due_minute = item.get("blocked_until") or prayer_minute
        if 0 <= now_minute - due_minute <= window_minutes:
            try:
                gs.set_config(key, now.strftime("%H:%M"))
            except Exception as exc:
                logger.warning("Prayer prompt config write unavailable: %s", exc)
                _PRAYER_PROMPT_FALLBACK_KEYS.add(key)
            return item
    return None


def prayer_notification_status(now: datetime | None = None) -> dict:
    now = (now or datetime.now(SGT)).astimezone(SGT)
    try:
        plan = _prayer_plan_for_date(now.date())
    except Exception as exc:
        return {"ok": False, "error": str(exc), "today": now.date().isoformat(), "prayers": []}
    today_key = now.strftime("%Y-%m-%d")
    prayers = []
    for item in plan:
        key = f"prayer_prompt:{today_key}:{item['key']}"
        prompted_at = ""
        try:
            prompted_at = gs.get_config(key) or ""
        except Exception:
            prompted_at = ""
        due_minute = item.get("blocked_until") or _hm_to_minutes(item["time"])
        prayers.append({
            "key": item["key"],
            "label": item["label"],
            "time": item["time"],
            "due_time": _minutes_to_hm(due_minute),
            "prompted_at": prompted_at,
            "prompted": bool(prompted_at),
        })
    return {
        "ok": True,
        "today": today_key,
        "now": now.strftime("%H:%M"),
        "window_minutes": _env_int("HIRA_PRAYER_REMINDER_WINDOW_MINUTES", 20, 3),
        "prayers": prayers,
    }


def _friday_khutbah_heads_up_due(now: datetime) -> str | None:
    now = now.astimezone(SGT)
    if now.weekday() != 4:
        return None
    today_key = now.strftime("%Y-%m-%d")
    key = f"khutbah_heads_up:{today_key}"
    if gs.get_config(key):
        return None
    try:
        text = build_muis_khutbah_brief(today_key)
    except Exception as exc:
        logger.warning("MUIS khutbah heads-up unavailable: %s", exc)
        return None
    gs.set_config(key, now.strftime("%H:%M"))
    return text


def _break_aware_slots(checkin: dict, target: date) -> list[str]:
    window_start = _hm_to_minutes(checkin.get("window_start", "08:00"))
    window_end = _hm_to_minutes(checkin.get("window_end", "21:30"))
    if window_end <= window_start:
        window_end = window_start + 60

    busy = []
    lessons, _ = _lessons_for_date(target)
    for lesson in lessons:
        busy.append((_hm_to_minutes(lesson["start"]), _hm_to_minutes(lesson["end"])))

    try:
        busy.extend(_event_busy_intervals_for_date(target))
    except Exception as e:
        logger.warning(f"Break-aware calendar read failed: {e}")

    busy = _merge_busy_intervals([
        (max(window_start, start), min(window_end, end))
        for start, end in busy
        if end > window_start and start < window_end
    ])

    free = []
    cursor = window_start
    for start, end in busy:
        if start > cursor:
            free.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < window_end:
        free.append((cursor, window_end))

    min_break = max(5, int(checkin.get("min_break_minutes", 20) or 20))
    free = [(start, end) for start, end in free if end - start >= min_break]
    if not free:
        return []

    target_count = max(1, min(int(checkin.get("target_count", 3) or 3), 8))
    chosen = sorted(free, key=lambda slot: (slot[1] - slot[0], -slot[0]), reverse=True)[:target_count]
    slots = []
    for start, end in sorted(chosen):
        reminder_minute = start + min(10, max(2, (end - start) // 4))
        slots.append(_minutes_to_hm(min(reminder_minute, end - 1)))
    return sorted(set(slots))

def _due_break_aware_checkins(now: datetime) -> list[dict]:
    today = now.strftime("%Y-%m-%d")
    now_hm = now.strftime("%H:%M")
    due = []
    cache_prefix = f"{today}:"
    for key in list(_BREAK_AWARE_SLOT_CACHE):
        if not key.startswith(cache_prefix):
            _BREAK_AWARE_SLOT_CACHE.pop(key, None)
    for checkin in gs.get_checkins(include_inactive=True):
        if (
            not checkin["active"]
            or not checkin.get("schedule_aware")
            or checkin.get("last_completed_date") == today
        ):
            continue
        sent_slots = checkin.get("sent_slots") if isinstance(checkin.get("sent_slots"), dict) else {}
        today_slots = sent_slots.get(today, [])
        cache_key = (
            f"{today}:{checkin['id']}:{checkin.get('window_start')}:{checkin.get('window_end')}:"
            f"{checkin.get('target_count')}:{checkin.get('min_break_minutes')}"
        )
        cached = _BREAK_AWARE_SLOT_CACHE.get(cache_key)
        if cached and now - cached["created"] < timedelta(minutes=15):
            slots = cached["slots"]
        else:
            slots = _break_aware_slots(checkin, now.date())
            _BREAK_AWARE_SLOT_CACHE[cache_key] = {"created": now, "slots": slots}
        for slot in slots:
            if slot <= now_hm and slot not in today_slots:
                due.append({**checkin, "due_slot": slot})
                break
    return due

def _json_from_claude_text(raw: str):
    clean = raw.strip().replace("```json", "").replace("```", "").strip()
    start = min((clean.find(c) for c in ["{", "["] if c in clean), default=0)
    return json.loads(clean[start:])

def _generate_document_spec(title: str, instructions: str, doc_type: str = "general", audience: str = "", language: str = "") -> dict:
    template_context = _artifact_template_context()
    prompt = f"""Create a structured DOCX content spec for H.I.R.A to render.

Title: {title}
Document type: {doc_type or "general"}
Audience: {audience or "Herwanto"}
Language: {language or "Use the user's requested language; for Bahasa Melayu, use DBP conventions."}
Instructions: {instructions}

Reusable template/style memory:
{template_context or "- None stored yet."}

Return ONLY valid JSON in this exact shape:
{{
  "title": "document title",
  "subtitle": "optional subtitle",
  "author": "optional author/context",
  "sections": [
    {{"heading": "section heading", "body": "paragraph text", "bullets": ["optional bullet"]}}
  ]
}}

Rules:
- Make it ready to use, not a rough outline.
- For worksheets, include student-facing instructions, exercises, and an answer key section.
- For lesson plans, include objectives, materials, flow, checks for understanding, differentiation, and exit ticket.
- Keep section body text concise but complete.
- Return ONLY JSON."""
    resp = claude.messages.create(
        model=DEEP_MODEL,
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}]
    )
    return _json_from_claude_text(resp.content[0].text)

def _generate_slide_spec(title: str, instructions: str, audience: str = "", slide_count: int = 8, language: str = "") -> dict:
    template_context = _artifact_template_context()
    slide_count = max(3, min(int(slide_count or 8), 15))
    prompt = f"""Create a structured PPTX content spec for H.I.R.A to render.

Title: {title}
Audience: {audience or "Herwanto"}
Approximate slide count: {slide_count}
Language: {language or "Use the user's requested language; for Bahasa Melayu, use DBP conventions."}
Instructions: {instructions}

Reusable template/style memory:
{template_context or "- None stored yet."}

Return ONLY valid JSON in this exact shape:
{{
  "title": "deck title",
  "subtitle": "optional subtitle",
  "audience": "audience",
  "slides": [
    {{"title": "slide title", "bullets": ["short bullet"], "notes": "optional presenter notes"}}
  ]
}}

Rules:
- Make the deck presentation-ready, not a rough outline.
- Use concise slide bullets and put details in notes.
- Include a strong opening and useful closing/action slide.
- Return ONLY JSON."""
    resp = claude.messages.create(
        model=DEEP_MODEL,
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}]
    )
    return _json_from_claude_text(resp.content[0].text)

def _upload_artifact_if_possible(path: str, convert_to: str, category: str = "General") -> dict | None:
    if not google_ok():
        return None
    try:
        return gs.upload_artifact(path, convert_to=convert_to, category=category)
    except Exception as e:
        logger.warning(f"Drive artifact upload failed: {e}")
        return None

def _artifact_result_text(kind: str, path, drive_file: dict | None = None) -> str:
    text = f"Created {kind}: `{path.name}`"
    if drive_file and drive_file.get("webViewLink"):
        text += f"\nEditable Google link: {drive_file['webViewLink']}"
    return text

def _forced_tool_for_text(text: str, tools: list[dict]) -> str | None:
    available = {tool["name"] for tool in tools}
    clean = " ".join((text or "").lower().split())
    if not clean:
        return None

    def has_any(words):
        return any(word in clean for word in words)

    gmail_intent = has_any([
        "email", "emails", "gmail", "inbox", "unread mail", "unread email",
        "last mail", "latest mail", "recent mail", "message"
    ])

    action_intent = has_any([
        "add ", "create ", "schedule ", "book ", "put ", "set up ",
        "remind me", "nudge me", "ping me", "check in", "check-in",
        "draft ", "write ", "compose ", "reply ", "send ",
    ])
    completion_intent = has_any([
        "done", "completed", "complete ", "mark as done", "mark done",
        "cancel ", "delete ", "remove ",
    ])
    if (
        "get_gmail_brief" in available
        and gmail_intent
        and has_any(["follow up", "follow-up", "chase", "note", "remember", "remind"])
        and not has_any(["draft", "write", "compose", "reply", "send "])
    ):
        return "get_gmail_brief"
    if (
        "find_available_training_slots" in available
        and has_any([
            "best slot", "best slots", "free slot", "free slots", "available slot", "available slots",
            "availability", "when can", "when should", "fit in", "slot to", "slots to",
            "after school", "not during", "avoid cca", "sahibba training", "training after school"
        ])
    ):
        return "find_available_training_slots"
    if (
        "get_assistant_context" in available
        and has_any(["appointment", "hdb", "calendar"])
        and has_any(["when", "date", "what day", "which day", "schedule", "confirm"])
        and not action_intent
    ):
        return "get_assistant_context"
    if (
        "reset_marking_load" in available
        and has_any(["marking", "scripts", "script", "papers", "paper", "unmarked", "marked"])
        and has_any(["reset", "clear", "clear all", "wipe"])
    ):
        return "reset_marking_load"
    if "fill_mtl_percentage_scores" in available and has_any([
        "converted score", "converted scores",
        "fill percentage", "input percentage", "under the percentage", "under %",
        "calculate percentage", "convert to percentage"
    ]):
        return "fill_mtl_percentage_scores"

    if "create_topic_profile" in available and has_any([
        "new interest", "new topic", "getting into", "got into", "picked up",
        "i'm into", "im into", "i am into", "deep dive", "beginner map",
        "track this", "follow this", "learn this", "teach me about"
    ]):
        return "create_topic_profile"

    if "analyze_mtl_scores" in available and has_any([
        "analyse", "analyze", "analysis", "mean", "median", "average", "pass rate",
        "distinction", "underperforming", "under-performing", "weak", "watchlist",
        "most improved", "improved", "improvement", "drop", "dropped", "drastic drop",
        "progress", "standard deviation", "std dev"
    ]):
        return "analyze_mtl_scores"

    if action_intent or completion_intent:
        return None

    if "update_mtl_class_score" in available and has_any([
        "enter score", "input score", "update score", "key in", "key-in", "record score",
        "write score", "put score", "set score", "enter mark", "input mark", "update mark",
        "record mark", "write mark", "set mark"
    ]):
        return "update_mtl_class_score"

    if "get_mtl_classlists" in available and has_any([
        "classlist", "class list", "student", "students", "names", "name list",
        "my classes", "mtl group", "grouping", "1 flagship", "2g3", "3g3", "4nt",
        "score", "scores", "mark", "marks", "results", "result", "wa1", "wa2", "exam", "assessment",
        "fa1", "fa2", "weighted assessment", "formative assessment", "prelim", "eoy"
    ]):
        return "get_mtl_classlists"

    if "get_timetable" in available and has_any([
        "timetable", "lesson", "lessons", "periods", "classes"
    ]):
        if not has_any(["calendar event", "calendar events", "appointment", "appointments"]):
            return "get_timetable"

    if "get_gmail_brief" in available and gmail_intent:
        if not has_any(["draft", "write", "compose", "reply"]):
            return "get_gmail_brief"

    if "get_nea_weather" in available and has_any([
        "weather", "forecast", "temperature", " temp", "temp ", "high temp",
        "low temp", "hot", "cold", "rain", "raining", "rainy", "showers",
        "thunder", "storm", "umbrella", "haze", "psi", "pm2.5",
        "air quality", "nea", "mss"
    ]):
        return "get_nea_weather"

    if "get_muis_friday_khutbah" in available and has_any([
        "khutbah", "sermon", "friday sermon", "jumuah sermon", "jumu'ah sermon",
        "jumaah sermon", "jumaat sermon"
    ]):
        return "get_muis_friday_khutbah"

    if "get_muis_prayer_times" in available and has_any([
        "prayer", "prayers", "pray", "solat", "salah", "subuh", "fajr",
        "syuruk", "zohor", "zuhur", "zuhr", "dhuhr", "asar", "asr",
        "maghrib", "isyak", "isha", "muis"
    ]):
        return "get_muis_prayer_times"

    if "fetch_url" in available and re.search(r"https?://\S+", text or "", re.I):
        return "fetch_url"

    if "get_f1_brief" in available and has_any([
        "f1", "formula 1", "grand prix", "qualifying", "race result",
        "standings", "driver standings", "constructor standings", "lineup",
        "line-up", "teams", "drivers", "mercedes", "ferrari", "mclaren",
        "red bull", "kimi", "antonelli", "russell", "hamilton"
    ]):
        return "get_f1_brief"

    if "get_liverpool_brief" in available and has_any([
        "liverpool", "lfc", "anfield", "ynwa", "premier league", "epl",
        "arne slot", "salah", "van dijk", "alisson", "isak", "wirtz",
        "mac allister", "szoboszlai", "gakpo", "chiesa", "ekitike",
        "lineup", "line-up", "starting xi", "team news", "injury",
        "injuries", "standings", "table", "fixtures", "results",
        "champions league", "fa cup", "carabao", "transfer", "rumour",
        "rumor", "signing"
    ]):
        return "get_liverpool_brief"

    if (
        "update_project_status" in available
        and has_any(["gameplan", "ruh", "rūḥ", "app", "apps", "project", "client", "demo"])
        and has_any([
            " is ", " got ", " now ", "currently", "status", "progress", "milestone",
            "approved", "rejected", "submitted", "review", "launched", "shipped",
            "released", "blocked", "done", "completed",
        ])
        and not has_any(["what", "show", "check", "list", "how", "when", "why", "?"])
    ):
        return "update_project_status"

    if "get_assistant_context" in available and has_any([
        "today", "tomorrow", "schedule", "calendar", "agenda", "my day",
        "my week", "what's on", "whats on"
    ]):
        return "get_assistant_context"

    if "get_task_brief" in available and has_any([
        "tasks", "task", "due", "deadline", "deadlines", "prioritise",
        "prioritize", "what should i do", "focus on"
    ]):
        return "get_task_brief"

    return None

def _normalise_gmail_account(value: str = "") -> str:
    clean = " ".join((value or "").lower().split())
    if clean in ("work", "moe", "school", "work gmail", "moe gmail", "school gmail"):
        return "work"
    return "personal"

def _extract_gmail_account_from_text(text: str) -> tuple[str, str]:
    clean = " ".join((text or "").split())
    lowered = clean.lower()
    account = "personal"
    for pattern in [
        r"\bwork\s+gmail\b",
        r"\bwork\s+emails?\b",
        r"\bmoe\s+gmail\b",
        r"\bmoe\s+emails?\b",
        r"\bschool\s+gmail\b",
        r"\bschool\s+emails?\b",
    ]:
        if re.search(pattern, lowered):
            account = "work"
            clean = re.sub(pattern, "", clean, flags=re.I).strip()
            break
    for pattern in [r"\bpersonal\s+gmail\b", r"\bpersonal\s+emails?\b"]:
        if re.search(pattern, lowered):
            account = "personal"
            clean = re.sub(pattern, "", clean, flags=re.I).strip()
            break
    return account, " ".join(clean.split())


def _forced_tool_for_current_turn(messages: list[dict], tools: list[dict]) -> str | None:
    if not messages:
        return None
    last_message = messages[-1]
    if last_message.get("role") != "user":
        return None
    content = last_message.get("content")
    if not isinstance(content, str):
        return None
    forced = _forced_tool_for_text(content, tools)
    if forced:
        return forced
    available = {tool["name"] for tool in tools}
    clean = " ".join(content.lower().split())
    recent_context = "\n".join(
        str(item.get("content", ""))[:600]
        for item in messages[-6:-1]
        if isinstance(item.get("content"), str)
    ).lower()
    if (
        "get_liverpool_brief" in available
        and re.search(r"\b(match|result|recap|score|home|away|host(?:ed)?|fixture|game|details?|what was it like)\b", clean)
        and re.search(r"\b(liverpool|lfc|man utd|man united|manchester united|premier league|epl)\b", recent_context)
    ):
        return "get_liverpool_brief"
    if (
        "get_f1_brief" in available
        and re.search(r"\b(race|result|recap|score|standings|qualifying|sprint|grand prix|details?|what was it like)\b", clean)
        and re.search(r"\b(f1|formula 1|grand prix|mercedes|ferrari|mclaren|red bull|hamilton|russell|antonelli)\b", recent_context)
    ):
        return "get_f1_brief"
    return None


async def _run_forced_weather_fallback(tool_choice: str | None) -> str | None:
    if tool_choice != "get_nea_weather":
        return None
    return await _execute_tool("get_nea_weather", {
        "area": "Yishun",
        "include_24h": True,
        "include_4day": False,
    })


_MONTH_LOOKUP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

_WEEKDAY_LOOKUP = {
    "mon": 0, "monday": 0,
    "tue": 1, "tues": 1, "tuesday": 1,
    "wed": 2, "wednesday": 2,
    "thu": 3, "thur": 3, "thurs": 3, "thursday": 3,
    "fri": 4, "friday": 4,
    "sat": 5, "saturday": 5,
    "sun": 6, "sunday": 6,
}

_WEEKDAY_SHORT = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_WEEKDAY_LONG = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _weekday_for(day: str, month: str, year: str = "") -> int | None:
    try:
        yr = int(year) if year else datetime.now(SGT).year
        return date(yr, _MONTH_LOOKUP[month.lower().rstrip(".")], int(day)).weekday()
    except Exception:
        return None


def _matching_weekday_label(original: str, weekday: int) -> str:
    return _WEEKDAY_LONG[weekday] if len(original) > 3 else _WEEKDAY_SHORT[weekday]


def _correct_weekday_date_mismatches(text: str) -> str:
    """Correct obvious weekday/date mismatches in model prose before delivery."""
    if not text:
        return text
    month_pattern = r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
    weekday_pattern = r"Mon(?:day)?|Tue(?:s(?:day)?)?|Wed(?:nesday)?|Thu(?:r(?:s(?:day)?)?|rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?"

    def fix_weekday_first(match):
        weekday_text, day, month, year = match.group("weekday", "day", "month", "year")
        actual = _weekday_for(day, month, year or "")
        expected = _WEEKDAY_LOOKUP.get(weekday_text.lower())
        if actual is None or expected == actual:
            return match.group(0)
        return match.group(0).replace(weekday_text, _matching_weekday_label(weekday_text, actual), 1)

    def fix_date_first(match):
        day, month, year, weekday_text = match.group("day", "month", "year", "weekday")
        actual = _weekday_for(day, month, year or "")
        expected = _WEEKDAY_LOOKUP.get(weekday_text.lower())
        if actual is None or expected == actual:
            return match.group(0)
        return match.group(0).replace(weekday_text, _matching_weekday_label(weekday_text, actual), 1)

    text = re.sub(
        rf"\b(?P<weekday>{weekday_pattern})\s+(?P<day>\d{{1,2}})\s+(?P<month>{month_pattern})(?:\s+(?P<year>20\d{{2}}))?\b",
        fix_weekday_first,
        text,
        flags=re.I,
    )
    text = re.sub(
        rf"\b(?P<day>\d{{1,2}})\s+(?P<month>{month_pattern})(?:\s+(?P<year>20\d{{2}}))?\s*(?P<sep>[|,\-–—:])\s*(?P<weekday>{weekday_pattern})\b",
        fix_date_first,
        text,
        flags=re.I,
    )
    return text


def build_briefing(record_news_digest: bool = False):
    now = datetime.now(SGT)
    today = now.date()
    lines = [f"Good morning, Herwanto!\n_{now.strftime('%A, %-d %B %Y')}_\n"]

    # Timetable
    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"*Today's lessons ({_week_display(wt_label, today)}):*")
        lines.append(tt.format_lessons(lessons))
    elif google_ok():
        lines.append("_Timetable: use /setweek to activate_")
    lines.append("")

    lines.append(build_islamic_brief(today))
    lines.append("")

    if google_ok():
        try:
            events = gs.get_today_events()
            formatted = gs.format_events(events)
            if "Nothing" not in formatted:
                lines.append("*Calendar:*")
                lines.append(formatted)
                lines.append("")
        except Exception as e:
            lines.append(f"_(Calendar error: {e})_\n")

        try:
            reminders = gs.get_reminders()
            today_str = today.strftime("%Y-%m-%d")
            week_str  = (today + timedelta(days=7)).strftime("%Y-%m-%d")
            overdue   = [r for r in reminders if r["due"] < today_str]
            due_today = [r for r in reminders if r["due"] == today_str]
            upcoming  = [r for r in reminders if today_str < r["due"] <= week_str]
            if overdue:
                lines.append("*Overdue:*")
                for r in overdue:
                    lines.append(f"- {r['due']} - {r['description']} ({r['category']})")
                lines.append("")
            if due_today:
                lines.append("*Due today:*")
                for r in due_today:
                    lines.append(f"- {r['description']} ({r['category']})")
                lines.append("")
            if upcoming:
                lines.append("*Due this week:*")
                for r in sorted(upcoming, key=lambda x: x["due"]):
                    lines.append(f"- {r['due']} - {r['description']}")
                lines.append("")
            if not overdue and not due_today and not upcoming:
                lines.append("No deadlines this week.")
        except Exception as e:
            lines.append(f"_(Reminders error: {e})_")
        try:
            marking = gs.get_marking_tasks()
            if marking:
                lines.append("")
                lines.append(build_marking_brief())
        except Exception:
            pass

    # Morning news digest
    try:
        digest = _fresh_morning_digest(now=now, record=record_news_digest)
        if digest:
            lines.append("")
            lines.append("*Morning digest:*")
            lines.append(digest)
    except Exception:
        pass

    lines.append("\nHave a productive day!")
    return "\n".join(lines)

def build_evening_briefing():
    now = datetime.now(SGT)
    today = now.date()
    tomorrow = now.date() + timedelta(days=1)
    lines = [f"*Evening roundup*\n_{now.strftime('%A, %-d %B %Y, %H:%M SGT')}_\n"]

    lines.append("*Today in review*")
    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lesson_count = len(lessons)
        lines.append(f"- Lessons: {lesson_count} block{'s' if lesson_count != 1 else ''} ({_week_display(wt_label, today)})")
        if lessons:
            lines.append(tt.format_lessons(lessons))
    else:
        lines.append("- Lessons: no active timetable reference.")
    if google_ok():
        try:
            events = gs.get_today_events()
            event_count = len(events)
            lines.append(f"- Calendar: {event_count} item{'s' if event_count != 1 else ''} today")
            formatted_today = gs.format_events(events)
            if "Nothing" not in formatted_today:
                lines.append(formatted_today)
        except Exception as e:
            lines.append(f"- Calendar unavailable: {e}")
        try:
            reminders = gs.get_reminders()
            today_str = today.strftime("%Y-%m-%d")
            due_today = [r for r in reminders if r["due"] == today_str]
            overdue = [r for r in reminders if r["due"] < today_str]
            lines.append(f"- Tasks: {len(due_today)} due today, {len(overdue)} overdue")
        except Exception:
            pass
    lines.append("")

    tomorrow_lessons, tomorrow_wt_label = _lessons_for_date(tomorrow)
    lines.append(f"*Tomorrow prep ({tomorrow.strftime('%A, %-d %B')})*")
    lines.append(build_islamic_brief(tomorrow))
    lines.append("")
    if tomorrow_wt_label:
        lines.append(f"Lessons ({_week_display(tomorrow_wt_label, tomorrow)}):")
        lines.append(tt.format_lessons(tomorrow_lessons))
    else:
        lines.append("No timetable reference for tomorrow.")
    lines.append("")

    if google_ok():
        try:
            events = gs.get_tomorrow_events()
            lines.append("*Tomorrow calendar:*")
            lines.append(gs.format_events(events))
            lines.append("")
        except Exception as e:
            lines.append(f"Calendar unavailable: {e}\n")
        try:
            lines.append(build_task_brief(days=3))
            marking = build_marking_brief()
            if "No active" not in marking:
                lines.append("")
                lines.append(marking)
        except Exception:
            pass

    lines.append("\nTake note of anything that needs packing, charging, printing, replying, or mentally parking before sleep.")
    return "\n".join(lines).strip()

def build_weekly_plan():
    now = datetime.now(SGT)
    lines = [f"*Weekly plan*\n_{now.strftime('%A, %-d %B %Y')}_\n"]
    if google_ok():
        try:
            lines.append("*Calendar - next 7 days*")
            lines.append(gs.format_events(gs.get_week_events(), show_date=True))
            lines.append("")
        except Exception as e:
            lines.append(f"Calendar unavailable: {e}\n")
        try:
            lines.append(build_task_brief(days=7))
            lines.append("")
        except Exception:
            pass
        try:
            marking = build_marking_brief()
            if "No active" not in marking:
                lines.append(marking)
                lines.append("")
        except Exception:
            pass
        try:
            followups = gs.get_followups()
            if followups:
                lines.append("*Follow-ups*")
                for followup in sorted(followups, key=lambda f: f["due_date"])[:10]:
                    lines.append(f"- {_format_followup(followup)}")
                lines.append("")
        except Exception:
            pass
        try:
            projects = gs.get_projects()
            if projects:
                lines.append("*Projects*")
                for p in projects:
                    lines.append(_format_project_line(p, include_updated=False))
        except Exception:
            pass
    return "\n".join(lines).strip()

def _format_project_line(p: dict, include_updated: bool = True) -> str:
    details = []
    if p.get("next_milestone"):
        milestone = f"Next: {p['next_milestone']}"
        if p.get("milestone_date"):
            milestone += f" ({p['milestone_date']})"
        details.append(milestone)
    if p.get("notes"):
        details.append(f"Notes: {p['notes']}")
    detail_text = f" {' '.join(details)}" if details else ""
    updated = f" _(updated {p['last_update']})_" if include_updated and p.get("last_update") else ""
    return f"- *{p.get('project', '')}* - {p.get('status', '')}.{detail_text}{updated}"

# ─── COMMANDS ────────────────────────────────────────────────────────────────

async def start(update, context):
    chat_id = str(update.effective_chat.id)
    if google_ok():
        try:
            gs.set_config("chat_id", chat_id)
        except Exception as e:
            logger.warning(f"Could not store chat_id: {e}")
    redis_status = "Redis connected" if _get_redis() else "Redis not connected (in-memory only)"
    search_status = "Web search enabled" if ss.search_enabled() else "Web search disabled (add TAVILY_API_KEY)"
    g = "Google connected" if google_ok() else "Google not connected"
    await reply(update,
        f"Assalamualaikum Herwanto\n\n"
        f"{g}\n{redis_status}\n{search_status}\n\n"
        f"*School timetable*\n/lessons /setweek odd|even\n\n"
        f"*Calendar*\n/today /tomorrow /week\n/addcal [natural language]\n\n"
        f"*Reminders*\n/due /remind /done\n\n"
        f"*Proactive nudges*\n/nudge /nudges /cancelnudge\n\n"
        f"*Daily check-ins*\n/checkin /checkins /cancelcheckin\n"
        f"`/checkin Name | breaks | Question` for schedule-aware pings\n\n"
        f"*Artifacts*\n/doc /slides /template /templates /artifacts\n\n"
        f"*Pro assistant*\n/tasks /taskmeta /donetask /followup /followups /files /classlists /evening /weekly\n\n"
        f"*Gmail*\n/gmail /gmaildraft (optional setup)\n\n"
        f"*Assistant*\n/agenda [days] /remember /memory /forget all\n\n"
        f"*Projects*\n/projects /update\n\n"
        f"*Search*\n/search [query]\n\n"
        f"*Weather*\n/weather [area]\n\n"
        f"*News*\n/news [topic] /watch /watchlist /unwatch\n\n"
        f"*Briefing*\n/briefing (auto 7am SGT)\n\n"
        f"/clear - reset AI chat\nOr just talk to me.",
        parse_mode="Markdown")

async def lessons_cmd(update, context):
    today = datetime.now(SGT).date()
    if today.weekday() > 4:
        await update.message.reply_text("Weekend - no lessons!")
        return
    ref_date, ref_type = _get_week_config()
    if not ref_date and not tt.get_school_week_info(today):
        await update.message.reply_text("Week type not set. Use /setweek odd or /setweek even first.")
        return
    lessons, wt_label = _lessons_for_date(today)
    day_str = datetime.now(SGT).strftime("%A, %-d %B")
    await reply(update, f"*{day_str} ({_week_display(wt_label, today)})*\n\n{tt.format_lessons(lessons)}", parse_mode="Markdown")

async def setweek_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    arg = context.args[0].lower() if context.args else ""
    if arg not in ("odd", "even", "o", "e"):
        await update.message.reply_text("Usage: /setweek odd or /setweek even")
        return
    try:
        wt = "odd" if arg in ("odd", "o") else "even"
        _set_current_school_week(wt)
        await update.message.reply_text(f"This week is *{wt.upper()}* week.", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def today_cmd(update, context):
    today = datetime.now(SGT).date()
    day_str = datetime.now(SGT).strftime("%A, %-d %B %Y")
    lines = [f"*{day_str}*\n"]
    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"*Lessons ({_week_display(wt_label, today)}):*")
        lines.append(tt.format_lessons(lessons))
    else:
        lines.append("_Timetable: use /setweek to activate_")
    lines.append("")
    if google_ok():
        try:
            events = gs.get_today_events()
            formatted = gs.format_events(events)
            if "Nothing" not in formatted:
                lines.append("*Calendar:*")
                lines.append(formatted)
        except Exception as e:
            lines.append(f"_(Calendar error: {e})_")
    await reply(update, "\n".join(lines), parse_mode="Markdown")

async def tomorrow_cmd(update, context):
    tomorrow = datetime.now(SGT).date() + timedelta(days=1)
    day_str = (datetime.now(SGT) + timedelta(days=1)).strftime("%A, %-d %B %Y")
    lines = [f"*{day_str}*\n"]
    lessons, wt_label = _lessons_for_date(tomorrow)
    if wt_label:
        lines.append(f"*Lessons ({_week_display(wt_label, tomorrow)}):*")
        lines.append(tt.format_lessons(lessons))
    else:
        lines.append("_Timetable: use /setweek to activate_")
    lines.append("")
    if google_ok():
        try:
            events = gs.get_tomorrow_events()
            formatted = gs.format_events(events)
            if "Nothing" not in formatted:
                lines.append("*Calendar:*")
                lines.append(formatted)
        except Exception as e:
            lines.append(f"_(Calendar error: {e})_")
    await reply(update, "\n".join(lines), parse_mode="Markdown")

async def week_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        events = gs.get_week_events()
        await reply(update, f"*Next 7 days*\n\n{gs.format_events(events, show_date=True)}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Calendar error: {e}")

async def due_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        reminders = gs.get_reminders()
        if not reminders:
            await update.message.reply_text("Nothing due. All clear.")
            return
        lines = ["*All reminders*\n"]
        for r in sorted(reminders, key=lambda x: x["due"]):
            lines.append(f"`[{r['id']}]` *{r['due']}* - {r['description']} _{r['category']}_")
        lines.append("\n/done <id> to mark complete")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def remind_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    args_text = " ".join(context.args)
    parts = [p.strip() for p in args_text.split("|")]
    if len(parts) < 2:
        await reply(update, "Usage: `/remind Description | YYYY-MM-DD | Category`", parse_mode="Markdown")
        return
    try:
        rid = gs.add_reminder(parts[0], parts[1], parts[2] if len(parts) > 2 else "General")
        await update.message.reply_text(f"Reminder #{rid} added: {parts[0]} by {parts[1]}")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def done_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /done <id>")
        return
    try:
        rid = context.args[0]
        ok = gs.mark_done(rid)
        await update.message.reply_text(f"#{rid} done!" if ok else f"#{rid} not found.")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def nudge_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    if not text:
        await reply(update,
            "Usage: `/nudge Message | YYYY-MM-DD HH:MM`\n"
            "Or: `/nudge Check on GamePlan proposal tomorrow 16:30`",
            parse_mode="Markdown")
        return

    try:
        if "|" in text:
            message, when_text = [p.strip() for p in text.split("|", 1)]
            send_dt = SGT.localize(datetime.strptime(when_text, "%Y-%m-%d %H:%M"))
            nudge = _create_nudge(message, send_dt.isoformat())
        else:
            now = datetime.now(SGT)
            parse_prompt = f"""Today is {now.strftime('%Y-%m-%d')} and current time is {now.strftime('%H:%M')} SGT.
Extract a proactive nudge from this text and return ONLY valid JSON.
Text: "{text}"

Return exactly:
{{"message":"message to send later","send_at":"YYYY-MM-DDTHH:MM:SS+08:00"}}

Rules: Use Asia/Singapore time. If no year is mentioned, use 2026. If the time/date is unclear, return {{"error":"missing date/time"}}. Return ONLY JSON."""
            parse_resp = claude.messages.create(
                model=STRUCTURED_MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": parse_prompt}]
            )
            raw = parse_resp.content[0].text.strip().replace("```json", "").replace("```", "").strip()
            start = min((raw.find(c) for c in ["{", "["] if c in raw), default=0)
            data = json.loads(raw[start:])
            if "error" in data:
                await update.message.reply_text("I need a clear date and time for that nudge.")
                return
            nudge = _create_nudge(data["message"], data["send_at"])

        await reply(update, f"Nudge scheduled:\n{_format_nudge(nudge)}", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"nudge error: {e}")
        await update.message.reply_text(f"Could not schedule nudge: {e}")

async def nudges_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        nudges = sorted(gs.get_nudges(), key=lambda n: n["send_at"])
        if not nudges:
            await update.message.reply_text("No pending nudges.")
            return
        lines = ["*Pending nudges*\n"]
        for nudge in nudges:
            lines.append(_format_nudge(nudge))
        lines.append("\n/cancelnudge <id> to cancel")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Nudges error: {e}")

async def cancelnudge_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /cancelnudge <id>")
        return
    try:
        nudge_id = context.args[0]
        ok = gs.cancel_nudge(nudge_id)
        await update.message.reply_text(f"Nudge #{nudge_id} cancelled." if ok else f"Nudge #{nudge_id} not found.")
    except Exception as e:
        await update.message.reply_text(f"Nudges error: {e}")

async def checkin_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    if not text or "|" not in text:
        await reply(update,
            "Usage: `/checkin Name | HH:MM, HH:MM | Question`\n"
            "Smart breaks: `/checkin Name | breaks | Question`\n"
            "Example: `/checkin Istigfar & Salawat | breaks | Have you done your istigfar and salawat today?`",
            parse_mode="Markdown")
        return
    try:
        parts = [p.strip() for p in text.split("|")]
        if len(parts) < 3:
            await update.message.reply_text("I need a name, time(s), and question.")
            return
        timing = parts[1].strip().lower()
        if timing in ("break", "breaks", "smart", "schedule", "schedule-aware", "calendar"):
            checkin = gs.add_checkin(parts[0], parts[2], [], schedule_aware=True)
        else:
            times = _parse_checkin_times(parts[1])
            checkin = gs.add_checkin(parts[0], parts[2], times)
        await reply(update, f"Daily check-in added:\n{_format_checkin(checkin)}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Check-in error: {e}")

async def checkins_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        checkins = gs.get_checkins()
        if not checkins:
            await update.message.reply_text("No active daily check-ins.")
            return
        lines = ["*Daily check-ins*\n"]
        for checkin in checkins:
            lines.append(_format_checkin(checkin))
            lines.append("")
        lines.append("/cancelcheckin <id> to stop one")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Check-in error: {e}")

async def cancelcheckin_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /cancelcheckin <id>")
        return
    try:
        checkin_id = context.args[0]
        ok = gs.cancel_checkin(checkin_id)
        await update.message.reply_text(f"Check-in #{checkin_id} cancelled." if ok else f"Check-in #{checkin_id} not found.")
    except Exception as e:
        await update.message.reply_text(f"Check-in error: {e}")

async def projects_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        projs = gs.get_projects()
        if not projs:
            await update.message.reply_text("No projects. Use /update to add one.")
            return
        lines = ["*Projects*\n"]
        for p in projs:
            lines.append(f"*{p['project']}* - {p['status']}")
            if p["next_milestone"]:
                lines.append(f"  Next: {p['next_milestone']} ({p['milestone_date']})")
            if p["notes"]:
                lines.append(f"  {p['notes']}")
            lines.append(f"  _{p['last_update']}_\n")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def update_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    args_text = " ".join(context.args)
    parts = [p.strip() for p in args_text.split("|")]
    if len(parts) < 2:
        await reply(update, "Usage: `/update Project | Status | Milestone | Date | Notes`", parse_mode="Markdown")
        return
    try:
        gs.update_project(parts[0], parts[1],
            parts[2] if len(parts) > 2 else "",
            parts[3] if len(parts) > 3 else "",
            parts[4] if len(parts) > 4 else "")
        await update.message.reply_text(f"{parts[0]} updated.")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def doc_cmd(update, context):
    text = " ".join(context.args).strip()
    if not text or "|" not in text:
        await reply(update,
            "Usage: `/doc Title | What to create`\n"
            "Example: `/doc Peribahasa Sec 3 Worksheet | 20-minute BM worksheet with answers`",
            parse_mode="Markdown")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        title, instructions = [p.strip() for p in text.split("|", 1)]
        spec = _generate_document_spec(title, instructions)
        path = artifacts.render_docx(spec)
        drive_file = _upload_artifact_if_possible(str(path), "doc", category="Documents")
        if google_ok():
            link_note = f" | google_doc={drive_file.get('webViewLink', '')}" if drive_file else ""
            gs.add_memory("files", f"Generated DOCX {path.name} on {datetime.now(SGT).strftime('%Y-%m-%d %H:%M SGT')} | prompt={_clip_memory_text(instructions, 240)}{link_note}")
        with path.open("rb") as artifact_file:
            await update.message.reply_document(
                document=artifact_file,
                filename=path.name,
                caption=_artifact_result_text("DOCX", path, drive_file),
                parse_mode="Markdown",
            )
    except Exception as e:
        logger.error(f"doc generation error: {e}")
        await update.message.reply_text(f"Could not create document: {e}")

async def slides_cmd(update, context):
    text = " ".join(context.args).strip()
    if not text or "|" not in text:
        await reply(update,
            "Usage: `/slides Title | What to create`\n"
            "Example: `/slides GamePlan Pitch | 8-slide deck for a Singapore school leader`",
            parse_mode="Markdown")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        title, instructions = [p.strip() for p in text.split("|", 1)]
        spec = _generate_slide_spec(title, instructions)
        path = artifacts.render_pptx(spec)
        drive_file = _upload_artifact_if_possible(str(path), "slides", category="Slides")
        if google_ok():
            link_note = f" | google_slides={drive_file.get('webViewLink', '')}" if drive_file else ""
            gs.add_memory("files", f"Generated PPTX {path.name} on {datetime.now(SGT).strftime('%Y-%m-%d %H:%M SGT')} | prompt={_clip_memory_text(instructions, 240)}{link_note}")
        with path.open("rb") as artifact_file:
            await update.message.reply_document(
                document=artifact_file,
                filename=path.name,
                caption=_artifact_result_text("PPTX", path, drive_file),
                parse_mode="Markdown",
            )
    except Exception as e:
        logger.error(f"slides generation error: {e}")
        await update.message.reply_text(f"Could not create slides: {e}")

async def template_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    if not text or "|" not in text:
        await reply(update,
            "Usage: `/template Name | Reusable style notes`\n"
            "Example: `/template NBSS BM Worksheet | Title, objectives, short practice, answer key, DBP BM`",
            parse_mode="Markdown")
        return
    try:
        name, notes = [p.strip() for p in text.split("|", 1)]
        gs.add_memory("templates", f"{name}: {notes}")
        await update.message.reply_text(f"Template remembered: {name}")
    except Exception as e:
        await update.message.reply_text(f"Template memory error: {e}")

async def templates_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        templates = gs.get_memory().get("templates", [])
        if not templates:
            await update.message.reply_text("No artifact templates remembered yet.")
            return
        lines = ["*Artifact templates*\n"]
        for item in templates:
            lines.append(f"- {item}")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Template memory error: {e}")

async def artifacts_cmd(update, context):
    await reply(update, build_artifact_index(), parse_mode="Markdown")

async def files_cmd(update, context):
    await reply(update, build_files_index(), parse_mode="Markdown")

async def classlists_cmd(update, context):
    query = " ".join(context.args).strip()
    try:
        await reply(update, gs.format_mtl_classlists(class_query=query), parse_mode=None)
    except Exception as e:
        await reply(update, f"Classlist lookup failed: {e}")

async def tasks_cmd(update, context):
    days = 7
    if context.args:
        try:
            days = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Usage: /tasks [days]")
            return
    try:
        await reply(update, build_task_brief(days), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Task brief error: {e}")

async def marking_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    if not text:
        await reply(update, build_marking_brief(), parse_mode="Markdown")
        return
    parts = [p.strip() for p in text.split("|")]
    title = parts[0]
    if not title:
        await reply(
            update,
            "Tell me naturally, like: “Add Kefahaman 2G3 to marking, 34 scripts, collected today.”",
            parse_mode="Markdown",
        )
        return
    try:
        total_scripts = int(parts[1]) if len(parts) > 1 and parts[1] else 0
        collected_date = parts[2] if len(parts) > 2 else ""
        notes = parts[3] if len(parts) > 3 else ""
        task = gs.add_marking_task(title, total_scripts=total_scripts, collected_date=collected_date, notes=notes)
        await reply(update, f"Added to your marking tracker. {_format_marking_task(task)}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Marking task error: {e}")

async def marked_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 2:
        await reply(update, "Tell me naturally, like: “I’ve marked 12 scripts for Kefahaman 2G3.”", parse_mode="Markdown")
        return
    try:
        task, score = _find_best_marking_task(parts[0])
        if not task or score < 0.35:
            await update.message.reply_text("I could not confidently match that to an active marking stack.")
            return
        value = parts[1].lower()
        done = value in ("done", "complete", "completed", "settled")
        marked_count = None if done else int(parts[1])
        updated = gs.update_marking_progress(task["id"], marked_count=marked_count, done=done)
        await reply(update, f"Updated. {_format_marking_task(updated)}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Marking update error: {e}")

async def taskmeta_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 2:
        await reply(update,
            "Usage: `/taskmeta id | priority | effort | next action`\n"
            "Priority: urgent/high/medium/low. Effort: quick/small/medium/deep.",
            parse_mode="Markdown")
        return
    try:
        meta = gs.update_task_metadata(
            parts[0],
            priority=parts[1] if len(parts) > 1 else "",
            effort=parts[2] if len(parts) > 2 else "",
            next_action=parts[3] if len(parts) > 3 else "",
        )
        await update.message.reply_text(f"Task #{parts[0]} updated: {meta}")
    except Exception as e:
        await update.message.reply_text(f"Task metadata error: {e}")

async def done_text_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    query = " ".join(context.args).strip()
    if not query:
        await reply(update, "Usage: `/donetask reminder text`", parse_mode="Markdown")
        return
    try:
        reminder, score = _find_best_reminder(query)
        if not reminder or score < 0.35:
            await update.message.reply_text("I could not confidently match that to an active reminder.")
            return
        gs.mark_done(reminder["id"])
        await update.message.reply_text(f"Marked done: [{reminder['id']}] {reminder['description']}")
    except Exception as e:
        await update.message.reply_text(f"Could not mark task done: {e}")

async def followup_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 3:
        await reply(update,
            "Usage: `/followup Person | Topic | YYYY-MM-DD | Channel | Notes`",
            parse_mode="Markdown")
        return
    try:
        followup = gs.add_followup(
            parts[0],
            parts[1],
            parts[2],
            parts[3] if len(parts) > 3 else "",
            parts[4] if len(parts) > 4 else "",
        )
        await reply(update, f"Follow-up added:\n{_format_followup(followup)}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Follow-up error: {e}")

async def followups_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        followups = gs.get_followups()
        if not followups:
            await update.message.reply_text("No open follow-ups.")
            return
        lines = ["*Follow-ups*\n"]
        for followup in sorted(followups, key=lambda f: f["due_date"]):
            lines.append(_format_followup(followup))
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Follow-up error: {e}")

async def donefollowup_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    query = " ".join(context.args).strip()
    if not query:
        await reply(update, "Usage: `/donefollowup id or text`", parse_mode="Markdown")
        return
    try:
        if query.isdigit():
            ok = gs.complete_followup(query)
            await update.message.reply_text(f"Follow-up #{query} done." if ok else f"Follow-up #{query} not found.")
            return
        followup, score = _find_best_followup(query)
        if not followup or score < 0.35:
            await update.message.reply_text("I could not confidently match that to an open follow-up.")
            return
        gs.complete_followup(followup["id"])
        await update.message.reply_text(f"Follow-up done: {followup['topic']}")
    except Exception as e:
        await update.message.reply_text(f"Could not mark follow-up done: {e}")

async def evening_cmd(update, context):
    await reply(update, build_evening_briefing(), parse_mode="Markdown")

async def weekly_cmd(update, context):
    await reply(update, build_weekly_plan(), parse_mode="Markdown")

async def gmail_cmd(update, context):
    account = "personal"
    args = list(context.args)
    if args and args[0].lower() in ("personal", "work", "moe", "school"):
        account = _normalise_gmail_account(args.pop(0))
    query = " ".join(args).strip()
    if not gs.gmail_ok(account):
        await update.message.reply_text(f"{gs.gmail_label(account).title()} not connected.")
        return
    try:
        messages = gs.list_gmail_messages(query=query, max_results=10, account=account)
        if not messages:
            detail = f" for `{query}`" if query else ""
            await reply(update, f"No {gs.gmail_label(account)} messages found{detail}.", parse_mode="Markdown")
            return
        title = query if query else "latest messages"
        lines = [f"*{gs.gmail_label(account).title()}: {title}*\n"]
        for msg in messages:
            lines.append(f"- *{msg['subject']}*")
            lines.append(f"  From: {msg['from']}")
            lines.append(f"  {msg['snippet']}")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Gmail error: {e}")

async def gmaildraft_cmd(update, context):
    account = "personal"
    args = list(context.args)
    if args and args[0].lower() in ("personal", "work", "moe", "school"):
        account = _normalise_gmail_account(args.pop(0))
    if not gs.gmail_ok(account):
        await update.message.reply_text(f"{gs.gmail_label(account).title()} not connected.")
        return
    text = " ".join(args).strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 3:
        await reply(update, "Usage: `/gmaildraft [personal|work] to | subject | body | cc`", parse_mode="Markdown")
        return
    try:
        draft = gs.create_gmail_draft(parts[0], parts[1], parts[2], parts[3] if len(parts) > 3 else "", account=account)
        await update.message.reply_text(f"{gs.gmail_label(account).title()} draft created: {draft.get('id', '')}")
    except Exception as e:
        await update.message.reply_text(f"Gmail draft error: {e}")

async def briefing_cmd(update, context):
    await reply(update, build_briefing(), parse_mode="Markdown")

async def prayers_cmd(update, context):
    await reply(update, build_islamic_brief(), parse_mode="Markdown")

async def khutbah_cmd(update, context):
    target = context.args[0] if context.args else ""
    await reply(update, build_muis_khutbah_brief(target), parse_mode="Markdown")

async def agenda_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    days = 7
    if context.args:
        try:
            days = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Usage: /agenda [days]")
            return
    try:
        await reply(update, build_agenda(days), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Agenda error: {e}")

async def remember_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    if not text:
        await reply(update,
            "Usage: `/remember preferences | Keep replies very concise`\n"
            "Categories: profile, preferences, people, places, teaching, business, projects, sports, files, templates, constraints, recent_summaries, topic_profiles",
            parse_mode="Markdown")
        return
    if "|" in text:
        category, memory_text = [p.strip() for p in text.split("|", 1)]
    else:
        category, memory_text = "profile", text
    try:
        inferred_week_type, inferred_week_number = _school_week_from_text(memory_text)
        if inferred_week_type:
            _set_current_school_week(inferred_week_type, inferred_week_number)
        gs.add_memory(category, memory_text)
        if inferred_week_type:
            number_note = f" Week {inferred_week_number}," if inferred_week_number else ""
            await update.message.reply_text(
                f"Remembered: {memory_text}\n{number_note} this week is now *{inferred_week_type.upper()}* week for the timetable.",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(f"Remembered: {memory_text}")
    except Exception as e:
        await update.message.reply_text(f"Memory error: {e}")

async def memory_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        await reply(update, f"*Assistant memory*\n\n{_format_memory(gs.get_memory())}", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Memory error: {e}")

async def forget_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    confirm = " ".join(context.args).strip().lower()
    user_id = update.effective_user.id
    now = datetime.now(SGT)

    if confirm == "confirm":
        # Second step — check if a pending request exists and is still within TTL
        pending_at = _FORGET_CONFIRM_PENDING.get(user_id)
        if not pending_at or (now - pending_at).total_seconds() > _FORGET_CONFIRM_TTL_SECONDS:
            _FORGET_CONFIRM_PENDING.pop(user_id, None)
            await reply(update, "Confirmation expired. Run `/forget all` again to start over.", parse_mode="Markdown")
            return
        _FORGET_CONFIRM_PENDING.pop(user_id, None)
        try:
            gs.clear_memory()
            await update.message.reply_text("Assistant memory cleared. All buckets wiped.")
        except Exception as e:
            await update.message.reply_text(f"Memory error: {e}")
        return

    if confirm != "all":
        await reply(update, "Usage: `/forget all` — then confirm within 60 seconds with `/forget confirm`.", parse_mode="Markdown")
        return

    # First step — register the pending request and ask for confirmation
    _FORGET_CONFIRM_PENDING[user_id] = now
    await reply(
        update,
        "⚠️ This will wipe *all* stored memory — corrections, preferences, profiles, files, everything.\n\n"
        "Run `/forget confirm` within 60 seconds to proceed, or do nothing to cancel.",
        parse_mode="Markdown",
    )

async def search_cmd(update, context):
    """Manual web search command."""
    query = " ".join(context.args)
    if not query:
        await update.message.reply_text("Usage: /search [query]")
        return
    if not ss.search_enabled():
        await update.message.reply_text("Web search not enabled. Add TAVILY_API_KEY to Railway variables.\nSign up free at tavily.com")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    results = ss.web_search(query, max_results=5)
    if not results:
        await update.message.reply_text("No results found.")
        return
    lines = [f"*Search: {query}*\n"]
    for r in results:
        lines.append(f"*{r['title']}*")
        lines.append(f"{r['description'][:150]}")
        lines.append(f"_{r['url']}_\n")
    await reply(update, "\n".join(lines), parse_mode="Markdown")

async def news_cmd(update, context):
    """Show latest news for a query, or the shortlisted topics."""
    query = " ".join(context.args).strip()
    try:
        await reply(update, build_news_digest(query, max_items=2 if not query else 5), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"News error: {e}")

async def weather_cmd(update, context):
    """Show latest NEA weather for a Singapore area."""
    area = " ".join(context.args).strip() or "Yishun"
    include_4day = bool(re.search(r"\b(week|days|outlook|4|four)\b", area.lower()))
    try:
        await reply(update, ws.build_weather_brief(area, include_24h=True, include_4day=include_4day), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Weather error: {e}")

async def watch_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    text = " ".join(context.args).strip()
    if not text:
        await reply(update,
            "Usage: `/watch Label | search query`\nExample: `/watch Apple AI | Apple artificial intelligence`",
            parse_mode="Markdown")
        return
    if "|" in text:
        label, query = [p.strip() for p in text.split("|", 1)]
    else:
        label, query = text, text
    try:
        gs.add_news_topic(label, query)
        await update.message.reply_text(f"Added to news shortlist: {label}")
    except Exception as e:
        await update.message.reply_text(f"Watchlist error: {e}")

async def watchlist_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    try:
        topics = gs.get_news_topics()
        lines = ["*News shortlist*\n"]
        for topic in topics:
            lines.append(f"- *{topic['label']}* — `{topic['query']}`")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Watchlist error: {e}")

async def unwatch_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    label = " ".join(context.args).strip()
    if not label:
        await reply(update, "Usage: `/unwatch Label`", parse_mode="Markdown")
        return
    try:
        before = len(gs.get_news_topics())
        topics = gs.remove_news_topic(label)
        after = len(topics)
        msg = f"Removed from news shortlist: {label}" if after < before else f"Not found: {label}"
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"Watchlist error: {e}")

async def clear_cmd(update, context):
    history = get_history(update.effective_user.id)
    try:
        _summarise_conversation_to_memory(history)
    except Exception:
        pass
    save_history(update.effective_user.id, [])
    await update.message.reply_text("Cleared.")

async def addcal_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    user_text = " ".join(context.args)
    if not user_text:
        await reply(update,
            "Usage: `/addcal [natural language]`\nExample: `/addcal CCA duty 7 May 3-6pm`",
            parse_mode="Markdown")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    today_str = datetime.now(SGT).strftime("%Y-%m-%d")
    parse_prompt = f"""Today is {today_str} (Singapore time).
Extract calendar event details from this text and return ONLY valid JSON, nothing else.
Text: "{user_text}"

Return exactly:
{{"title":"event title","date":"YYYY-MM-DD","start_time":"HH:MM","end_time":"HH:MM","location":"location or empty","description":"notes or empty"}}

Rules: the current year is 2026 — ALWAYS use 2026 if no year is mentioned, 24hr time, add 1hr if no end time specified. Return ONLY the JSON."""
    try:
        parse_resp = claude.messages.create(
            model=STRUCTURED_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": parse_prompt}]
        )
        raw = parse_resp.content[0].text.strip()
        # Strip markdown fences
        raw = raw.replace("```json","").replace("```","").strip()
        # Strip anything before first { or [
        start = min((raw.find(c) for c in ["{","["] if c in raw), default=0)
        raw = raw[start:]
        event_data = json.loads(raw)
        # Handle if Claude returned a list instead of a dict
        if isinstance(event_data, list):
            event_data = event_data[0]
        start_dt = SGT.localize(datetime.strptime(f"{event_data['date']} {event_data['start_time']}", "%Y-%m-%d %H:%M"))
        end_dt   = SGT.localize(datetime.strptime(f"{event_data['date']} {event_data['end_time']}",   "%Y-%m-%d %H:%M"))
        gs.create_event(event_data["title"], start_dt, end_dt,
                        event_data.get("location",""), event_data.get("description",""))
        loc = f"\n📍 {event_data['location']}" if event_data.get("location") else ""
        await update.message.reply_text(
            f"Added to calendar:\n\n*{event_data['title']}*\n"
            f"📅 {event_data['date']}\n🕐 {event_data['start_time']} – {event_data['end_time']}{loc}",
            parse_mode="Markdown")
    except Exception as e:
        logger.error(f"addcal error: {e}")
        await update.message.reply_text(f"Could not create event: {e}")

# ─── MEDIA HANDLERS ──────────────────────────────────────────────────────────

MEDIA_SCHEDULE_INSTRUCTION = """
Inspect this screenshot/document carefully. Priority:
1. Extract schedule/calendar items: duties, meetings, appointments, matches, trainings, reporting times, event timings, venue changes.
2. Extract tasks/deadlines: things to submit, prepare, mark, complete, or follow up.
3. If a calendar item has a clear date and start time, call create_calendar_event. Use a sensible one-hour duration if no end time is shown.
4. If a task has a due date, call add_reminder.
5. If the date/time is ambiguous, do not invent it. Ask for the exact missing detail.
6. After tools run, reply with a concise summary: added to calendar, added as reminders, unclear items.
"""

def _core_tools():
    tools = [
        CONTEXT_TOOL,
        CALENDAR_TOOL,
        DELETE_CALENDAR_TOOL,
        AVAILABILITY_SLOT_TOOL,
        REMINDER_TOOL,
        NUDGE_TOOL,
        DAILY_CHECKIN_TOOL,
        BREAK_AWARE_CHECKIN_TOOL,
        DOCUMENT_ARTIFACT_TOOL,
        SLIDE_ARTIFACT_TOOL,
        TEMPLATE_MEMORY_TOOL,
        TOPIC_PROFILE_TOOL,
        FOLLOWUP_TOOL,
        COMPLETE_TASK_TOOL,
        COMPLETE_FOLLOWUP_TOOL,
        ADD_MARKING_TOOL,
        UPDATE_MARKING_TOOL,
        RESET_MARKING_TOOL,
        MARKING_BRIEF_TOOL,
        TASK_BRIEF_TOOL,
        TIMETABLE_TOOL,
        CLASSLIST_TOOL,
        ANALYZE_MTL_SCORES_TOOL,
        UPDATE_CLASS_SCORE_TOOL,
        FILL_PERCENTAGE_SCORES_TOOL,
        GMAIL_BRIEF_TOOL,
        GMAIL_DRAFT_TOOL,
        MEMORY_TOOL,
        SOURCE_NOTE_TOOL,
        WEEK_TYPE_TOOL,
        PROJECT_TOOL,
        WEATHER_TOOL,
        PRAYER_TIME_TOOL,
        KHUTBAH_TOOL,
        NEWS_TOOL,
        LIVERPOOL_BRIEF_TOOL,
        F1_BRIEF_TOOL,
        FETCH_URL_TOOL,
    ]
    if ss.search_enabled():
        tools.append(SEARCH_TOOL)
    return tools

def pwa_tools_for_message(text: str, recent_context: str = "") -> list[dict]:
    text = (text or "").lower()
    context = (recent_context or "").lower()
    combined = f"{context}\n{text}"
    tools: list[dict] = []

    def add(*items):
        for item in items:
            if item not in tools:
                tools.append(item)

    if re.search(r"\b(gmail|email|emails|mail|inbox|unread|draft|reply)\b", text):
        add(GMAIL_BRIEF_TOOL, GMAIL_DRAFT_TOOL)
    if re.search(r"\b(timetable|lesson|period|odd week|even week|school week)\b", text):
        add(TIMETABLE_TOOL, WEEK_TYPE_TOOL)
    if re.search(r"\b(classlist|class list|students?|names?|my classes|mtl group|grouping|1 flagship|2g3|3g3|4nt|4nt bml|scores?|marks?|results?|wa1|wa2|fa1|fa2|prelim|eoy|weighted assessment|formative assessment|exam|assessment|percentage|percent|%|analyse|analyze|analysis|mean|median|average|pass rate|underperforming|watchlist|most improved|progress|drop|dropped)\b", text):
        add(CLASSLIST_TOOL, ANALYZE_MTL_SCORES_TOOL, UPDATE_CLASS_SCORE_TOOL, FILL_PERCENTAGE_SCORES_TOOL, TIMETABLE_TOOL)
    if re.search(r"\b(calendar|schedule|agenda|today|tomorrow|week|meeting|event|appointment|duty|training|match|cca|what'?s on)\b", text):
        add(CONTEXT_TOOL, CALENDAR_TOOL, DELETE_CALENDAR_TOOL, AVAILABILITY_SLOT_TOOL, REMINDER_TOOL, TIMETABLE_TOOL)
    if re.search(r"\b(task|tasks|due|deadline|remind|reminder|prepare|submit|complete|done|priority|prioritise|prioritize|focus)\b", text):
        add(CONTEXT_TOOL, TASK_BRIEF_TOOL, REMINDER_TOOL, COMPLETE_TASK_TOOL)
    if re.search(r"\b(marking|scripts?|papers?|compositions?|kefahaman|karangan|worksheets?|marked|unmarked)\b", text):
        add(ADD_MARKING_TOOL, UPDATE_MARKING_TOOL, RESET_MARKING_TOOL, MARKING_BRIEF_TOOL)
    if re.search(r"\b(nudge|ping|check[- ]?in|check in|selawat|salawat|istighfar|zikir|zikr|dhikr)\b", text):
        add(NUDGE_TOOL, DAILY_CHECKIN_TOOL, BREAK_AWARE_CHECKIN_TOOL)
    if re.search(r"\b(follow[- ]?up|follow up|owe replies|chase)\b", text):
        add(FOLLOWUP_TOOL, COMPLETE_FOLLOWUP_TOOL, GMAIL_BRIEF_TOOL, TASK_BRIEF_TOOL)
    sports_followup = (
        re.search(r"\b(match|result|results|recap|details?|score|home|away|host(?:ed)?|fixture|game|what was it like)\b", text)
        and re.search(r"\b(football|f1|formula 1|liverpool|lfc|man utd|man united|manchester united|premier league|epl|grand prix|mercedes|ferrari|mclaren|red bull)\b", combined)
    )
    correction_followup = _looks_like_correction(text) and re.search(
        r"\b(football|f1|formula 1|liverpool|lfc|man utd|man united|manchester united|premier league|epl|grand prix|mercedes|ferrari|mclaren|red bull)\b",
        combined,
    )
    if re.search(r"\b(news|latest|current|headline|headlines|search|web|football|f1|liverpool|lfc|anfield|ynwa|premier league|epl|champions league|fa cup|carabao|transfer|rumou?r|salah|van dijk|alisson|isak|wirtz|mac allister|szoboszlai|gakpo|chiesa|ekitike|apple|ai|singapore education|nothing os)\b", text) or sports_followup or correction_followup:
        add(NEWS_TOOL, SOURCE_NOTE_TOOL)
        if re.search(r"\b(liverpool|lfc|anfield|ynwa|premier league|epl|champions league|fa cup|carabao|transfer|rumou?r|salah|van dijk|alisson|isak|wirtz|mac allister|szoboszlai|gakpo|chiesa|ekitike|man utd|man united|manchester united)\b", combined):
            add(LIVERPOOL_BRIEF_TOOL)
        if re.search(r"\b(f1|formula 1|grand prix|qualifying|driver standings|constructor standings|mercedes|ferrari|mclaren|red bull|kimi|antonelli|russell|hamilton)\b", combined):
            add(F1_BRIEF_TOOL)
        if ss.search_enabled():
            add(SEARCH_TOOL)
    if re.search(r"https?://\S+|\b(link|url|website|webpage|article|page)\b", text):
        add(FETCH_URL_TOOL, SOURCE_NOTE_TOOL)
        if ss.search_enabled():
            add(SEARCH_TOOL)
    if re.search(r"\b(weather|forecast|temperature|temp|hot|cold|rain|raining|rainy|shower|showers|thunder|storm|umbrella|haze|psi|pm2\.5|air quality|nea|mss)\b", text):
        add(WEATHER_TOOL)
    if re.search(r"\b(prayer|prayers|pray|solat|salah|subuh|fajr|syuruk|zohor|zuhur|zuhr|dhuhr|asar|asr|maghrib|isyak|isha|muis|religion|religious|islam|islamic|halal|haram|fatwa|zakat|puasa|fasting|ramadan|qibla|wudhu|wudu|ablution)\b", text):
        add(PRAYER_TIME_TOOL, KHUTBAH_TOOL, CONTEXT_TOOL)
        if ss.search_enabled():
            add(SEARCH_TOOL)
    if re.search(r"\b(khutbah|sermon|friday sermon|jumu'?ah sermon|jumaah sermon|jumaat sermon)\b", text):
        add(KHUTBAH_TOOL, CONTEXT_TOOL)
    if re.search(r"\b(location|where|journey|travel|route|directions|commute|drive|driving|mrt|bus|walk|walking|masjid|mosque)\b", text):
        add(CONTEXT_TOOL, WEATHER_TOOL)
        if ss.search_enabled():
            add(SEARCH_TOOL)
    if re.search(r"\b(document|docx|worksheet|letter|report|lesson plan|handout|memo|proposal|meeting notes)\b", text):
        add(DOCUMENT_ARTIFACT_TOOL, TEMPLATE_MEMORY_TOOL)
    if re.search(r"\b(slide|slides|deck|ppt|pptx|powerpoint|presentation|pitch)\b", text):
        add(SLIDE_ARTIFACT_TOOL, TEMPLATE_MEMORY_TOOL)
    if re.search(r"\b(new interest|new topic|getting into|got into|picked up|i'?m into|i am into|deep dive|beginner map|track this|follow this|learn this|teach me about)\b", text):
        add(TOPIC_PROFILE_TOOL, MEMORY_TOOL)
    if re.search(r"\b(remember|note that|preference|prefer|template|style|project status|milestone)\b", text):
        add(MEMORY_TOOL, PROJECT_TOOL, TEMPLATE_MEMORY_TOOL, TOPIC_PROFILE_TOOL)
    if re.search(r"\b(gameplan|ruh|rūḥ|app|apps|project|projects|product|products|app store|play store|review|approved|rejected|submitted|launched|shipped|released|blocked|progress|status|milestone|client|demo)\b", text):
        add(CONTEXT_TOOL, PROJECT_TOOL, MEMORY_TOOL)

    return tools or _core_tools()

async def _run_agentic_claude(messages, max_tokens=2048, tools=None):
    tools = tools or _core_tools()
    reply_text = ""
    max_iterations = 8

    for _ in range(max_iterations):
        forced_tool = _forced_tool_for_current_turn(messages, tools)
        tool_choice = {"type": "tool", "name": forced_tool} if forced_tool else None
        kwargs = {}
        if tool_choice:
            kwargs["tool_choice"] = tool_choice
        resp = claude.messages.create(
            model=_agentic_model_for_messages(messages),
            max_tokens=max_tokens,
            system=CACHED_SYSTEM_PROMPT(),
            tools=tools,
            messages=messages,
            **kwargs,
        )

        if resp.stop_reason != "tool_use":
            segment = await _run_forced_weather_fallback(forced_tool)
            if not segment:
                segment = next((b.text for b in resp.content if b.type == "text"), "")
            reply_text = f"{reply_text}{segment}"
            if resp.stop_reason == "max_tokens":
                messages.append({"role": "assistant", "content": resp.content})
                messages.append({
                    "role": "user",
                    "content": "Continue exactly where you stopped. Finish the response completely without restarting or summarising earlier text.",
                })
                logger.warning("Claude hit max_tokens in non-streaming chat; requesting continuation.")
                continue
            break

        messages.append({"role": "assistant", "content": resp.content})
        tool_blocks = [block for block in resp.content if block.type == "tool_use"]

        async def run_tool(block):
            logger.info(f"Tool call: {block.name} {block.input}")
            result = await _execute_tool(block.name, block.input)
            return {
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result
            }

        tool_results = await asyncio.gather(*(run_tool(block) for block in tool_blocks))
        messages.append({"role": "user", "content": tool_results})

    return _correct_weekday_date_mismatches(reply_text or "Done.")

def _looks_tool_heavy(text: str) -> bool:
    return bool(re.search(
        r"\b(calendar|schedule|meeting|event|remind|nudge|task|due|marking|scripts?|"
        r"email|gmail|inbox|draft|reply|timetable|lesson|news|latest|search|remember|"
        r"classlist|class list|students?|my classes|mtl group|grouping|"
        r"prayer|prayers|pray|solat|salah|subuh|fajr|syuruk|zohor|zuhur|zuhr|dhuhr|asar|asr|maghrib|isyak|isha|muis|religion|religious|islam|islamic|halal|haram|fatwa|zakat|puasa|fasting|ramadan|qibla|wudhu|wudu|ablution|"
        r"location|where|journey|travel|route|directions|commute|drive|driving|mrt|bus|walk|walking|masjid|mosque|"
        r"weather|forecast|temperature|temp|hot|cold|rain|raining|rainy|shower|showers|thunder|storm|umbrella|"
        r"haze|psi|pm2\.5|air quality|nea|mss|project|projects|gameplan|ruh|rūḥ|apps?|app store|"
        r"milestone|launched|shipped|released|approved|rejected|submitted|blocked|"
        r"football|f1|liverpool|lfc|anfield|ynwa|premier league|epl|champions league|fa cup|carabao|"
        r"match|recap|score|home|away|host(?:ed)?|line-?up|starting xi|standings|table|fixtures?|results?|transfers?|rumou?rs?|injur(?:y|ies)|"
        r"salah|van dijk|alisson|isak|wirtz|mac allister|szoboszlai|gakpo|chiesa|ekitike|"
        r"facts straight|fact check|source check|actual details?|actual result|"
        r"document|worksheet|slides?|ppt|deck|follow\s*up|done|complete)\b",
        text,
        re.I,
    ))

def _obvious_quick_chat(text: str) -> bool:
    clean = re.sub(r"[^\w\s']", "", text.lower()).strip()
    if not clean:
        return False
    if clean in {
        "ok", "okay", "k", "kk", "yes", "yep", "yeah", "no", "nope", "nah",
        "thanks", "thank you", "thx", "ty", "cool", "great", "nice", "noted",
        "got it", "understood", "sure", "alright", "morning", "hi", "hello", "hey",
    }:
        return True
    return len(clean.split()) <= 5 and bool(re.search(r"\b(thanks?|ok(?:ay)?|yes|no|hi|hello|hey)\b", clean))

async def should_route_quick_pwa_chat(messages: list[dict], message: str) -> bool:
    text = (message or "").strip()
    if not text or len(text) > 120 or _looks_tool_heavy(text):
        return False
    recent_context = "\n".join(
        str(item.get("content", ""))[:500]
        for item in messages[-4:]
        if isinstance(item.get("content"), str)
    ).lower()
    if (
        re.search(r"\b(match|result|recap|score|home|away|host(?:ed)?|fixture|game|details?)\b", text, re.I)
        and re.search(r"\b(liverpool|lfc|man utd|man united|manchester united|f1|formula 1|grand prix)\b", recent_context)
    ):
        return False
    if _obvious_quick_chat(text):
        return True
    try:
        prompt = (
            "Classify whether this chat message can be answered as lightweight small talk "
            "without tools, private data, calendar, Gmail, tasks, files, or web lookup. "
            "Reply with only QUICK or FULL.\n\n"
            f"Message: {text}"
        )
        resp = await async_claude.messages.create(
            model=ROUTER_MODEL,
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}],
        )
        verdict = (resp.content[0].text or "").strip().upper()
        return verdict.startswith("QUICK")
    except Exception as exc:
        logger.warning(f"Quick-route classifier failed: {exc}")
        return False

async def stream_quick_pwa_reply(messages: list[dict], message: str):
    context = messages[-6:]
    prompt_messages = context + [{"role": "user", "content": message}]
    lens = intent_lens_hint(message)
    try:
        async with async_claude.messages.stream(
            model=QUICK_MODEL,
            max_tokens=220,
            system=(
                "You are H.I.R.A, Herwanto's concise personal assistant. "
                "Answer lightweight chat naturally in one or two short sentences. "
                "Do not use tools or pretend to have checked live data."
                f"{lens}"
            ),
            messages=prompt_messages,
        ) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and getattr(event.delta, "type", None) == "text_delta":
                    yield {"type": "text", "text": event.delta.text}
    except Exception as exc:
        logger.error(f"Quick PWA reply failed: {exc}")
        raise

async def stream_agentic_claude(messages, max_tokens=650, tools=None):
    tools = tools or _core_tools()
    reply_text = ""
    max_iterations = 8

    for _ in range(max_iterations):
        forced_tool = _forced_tool_for_current_turn(messages, tools)
        tool_choice = {"type": "tool", "name": forced_tool} if forced_tool else None
        kwargs = {}
        if tool_choice:
            kwargs["tool_choice"] = tool_choice
        resp = None
        text_parts = []
        async with async_claude.messages.stream(
            model=_agentic_model_for_messages(messages),
            max_tokens=max_tokens,
            system=CACHED_SYSTEM_PROMPT(),
            tools=tools,
            messages=messages,
            **kwargs,
        ) as stream:
            async for event in stream:
                if event.type == "content_block_delta" and getattr(event.delta, "type", None) == "text_delta":
                    text = event.delta.text
                    text_parts.append(text)
                    yield {"type": "text", "text": text}
            resp = await stream.get_final_message()

        if resp.stop_reason != "tool_use":
            fallback_text = await _run_forced_weather_fallback(forced_tool)
            if fallback_text:
                reply_text = f"{reply_text}{fallback_text}"
                yield {"type": "replace", "text": reply_text}
            else:
                segment = "".join(text_parts) or next((b.text for b in resp.content if b.type == "text"), "")
                reply_text = f"{reply_text}{segment}"
            if resp.stop_reason == "max_tokens":
                messages.append({"role": "assistant", "content": resp.content})
                messages.append({
                    "role": "user",
                    "content": "Continue exactly where you stopped. Finish the response completely without restarting or summarising earlier text.",
                })
                logger.warning("Claude hit max_tokens in streaming chat; requesting continuation.")
                yield {"type": "continuation", "reason": "max_tokens"}
                continue
            break

        messages.append({"role": "assistant", "content": resp.content})
        tool_blocks = [block for block in resp.content if block.type == "tool_use"]
        for block in tool_blocks:
            yield {"type": "tool", "name": block.name}

        async def run_tool(block):
            logger.info(f"Tool call: {block.name} {block.input}")
            result = await _execute_tool(block.name, block.input)
            return {
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result,
            }

        tool_results = await asyncio.gather(*(run_tool(block) for block in tool_blocks))
        messages.append({"role": "user", "content": tool_results})

    corrected = _correct_weekday_date_mismatches(reply_text or "Done.")
    if corrected != (reply_text or "Done."):
        yield {"type": "replace", "text": corrected}
    yield {"type": "done", "text": corrected}

async def handle_photo(update, context):
    """Extract schedule data from photos/screenshots and send to Claude vision."""
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        image_data = base64.b64encode(buf.getvalue()).decode()
        caption = update.message.caption or "Extract any schedule items, calendar events, reminders, and useful context from this screenshot/photo."
        reply_text = await _run_agentic_claude(
            [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": image_data}},
                {"type": "text", "text": f"{MEDIA_SCHEDULE_INSTRUCTION}\n\nUser note: {caption}"}
            ]}],
            max_tokens=2048,
            tools=[CONTEXT_TOOL, CALENDAR_TOOL, REMINDER_TOOL]
        )
        try:
            _remember_uploaded_file("photo", photo.file_id, caption, reply_text, mime_type="image/jpeg")
        except Exception as e:
            logger.warning(f"Could not store photo memory: {e}")
        await reply(update, reply_text)
    except Exception as e:
        logger.error(f"Photo error: {e}")
        await update.message.reply_text(f"Could not process photo: {e}")

DOCUMENT_ANALYSIS_INSTRUCTION = """
You are analysing extracted text from a potentially large uploaded work document.
It may be a PDF, Word document, or PowerPoint deck. Work only from the excerpts given.

Priority:
1. Identify pages/sections relevant to Herwanto, Muhammad Herwanto Johari, MTL, Malay/Bahasa Melayu, and timetable entries.
2. Extract useful details for school work: timetable entries, classes, days, periods, rooms, odd/even week notes, meeting/event details, deadlines, instructions, rubrics, tasks, and follow-ups.
3. Extract dated calendar items or actionable deadlines only if the excerpt contains clear dates/times.
4. If the excerpts are insufficient, say exactly what is missing and which page clues were found.
5. Do not pretend to have read pages not included in the excerpt.
6. Reply with a concise summary and page references.
"""

async def _process_office_document(update, doc, file_bytes: bytes, caption: str):
    try:
        kind, index_note, excerpt = docs.extract_supported_document(
            file_bytes,
            doc.mime_type or "",
            filename=doc.file_name or "",
            caption=caption,
        )
    except Exception as e:
        logger.error(f"Document extraction error: {e}")
        await update.message.reply_text(f"Could not read that document locally: {e}")
        return

    if not excerpt.strip():
        summary = (
            f"{index_note}\n\n"
            "I could not extract searchable text from it. If this is a scanned PDF or image-based deck, send the relevant page/screenshot or an OCR/searchable export."
        )
        _remember_uploaded_file(
            kind.lower(),
            doc.file_id,
            caption,
            summary,
            filename=doc.file_name or "",
            mime_type=doc.mime_type or kind.lower(),
        )
        await reply(update, summary)
        return

    await reply(update, f"Got the {kind}. {index_note}\nAnalysing the most relevant parts now...")
    prompt = (
        f"{DOCUMENT_ANALYSIS_INSTRUCTION}\n\n"
        f"Document type: {kind}\n"
        f"User note: {caption}\n\n"
        f"Document index: {index_note}\n\n"
        f"Extracted relevant text:\n{excerpt}"
    )
    try:
        reply_text = await _run_agentic_claude(
            [{"role": "user", "content": prompt}],
            max_tokens=3000,
            tools=[CONTEXT_TOOL, CALENDAR_TOOL, REMINDER_TOOL, MEMORY_TOOL]
        )
    except Exception as e:
        logger.error(f"Document Claude analysis error: {e}")
        reply_text = (
            f"I extracted the document text, but the AI analysis step failed: {e}\n\n"
            f"{index_note}\nTry asking me a narrower question like: “find Herwanto in this timetable”."
        )

    try:
        _remember_uploaded_file(
            kind.lower(),
            doc.file_id,
            caption,
            f"{index_note} Analysis: {reply_text}",
            filename=doc.file_name or "",
            mime_type=doc.mime_type or kind.lower(),
        )
    except Exception as e:
        logger.warning(f"Could not store document memory: {e}")
    await reply(update, reply_text)

async def handle_document(update, context):
    """Handle PDFs/images and extract schedule data when present."""
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    doc = update.message.document
    caption = update.message.caption or "Extract any schedule items, calendar events, reminders, deadlines, and action items relevant to my work."
    try:
        file = await context.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        raw_bytes = buf.getvalue()

        office_mimes = {
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        }
        filename = (doc.file_name or "").lower()
        if doc.mime_type in office_mimes or filename.endswith((".pdf", ".docx", ".pptx")):
            await _process_office_document(update, doc, raw_bytes, caption)
            return
        elif doc.mime_type and doc.mime_type.startswith("image/"):
            file_data = base64.b64encode(raw_bytes).decode()
            content_block = {
                "type": "image",
                "source": {"type": "base64", "media_type": doc.mime_type, "data": file_data}
            }
        else:
            await update.message.reply_text(
                f"File type `{doc.mime_type}` not supported yet.\nSend PDFs, DOCX, PPTX, or images.",
                parse_mode="Markdown")
            return

        reply_text = await _run_agentic_claude(
            [{"role": "user", "content": [
                content_block,
                {"type": "text", "text": f"{MEDIA_SCHEDULE_INSTRUCTION}\n\nUser note: {caption}"}
            ]}],
            max_tokens=2048,
            tools=[CONTEXT_TOOL, CALENDAR_TOOL, REMINDER_TOOL]
        )
        try:
            _remember_uploaded_file(
                "document",
                doc.file_id,
                caption,
                reply_text,
                filename=doc.file_name or "",
                mime_type=doc.mime_type or "",
            )
        except Exception as e:
            logger.warning(f"Could not store document memory: {e}")
        await reply(update, reply_text)
    except Exception as e:
        logger.error(f"Document error: {e}")
        await update.message.reply_text(f"Could not process document: {e}")

# ─── AI CHAT (with web search tool use) ──────────────────────────────────────

async def _execute_tool(name: str, inp: dict) -> str:
    """Execute a tool call and return result string."""
    if name == "web_search":
        results = ss.web_search(inp.get("query", ""), max_results=5)
        return ss.format_results(results)

    elif name == "fetch_url":
        result = ss.fetch_url(inp.get("url", ""), max_chars=inp.get("max_chars", 6000))
        return ss.format_url_fetch(result)

    elif name == "remember_source_insight":
        try:
            entry = dict(inp)
            entry["date"] = datetime.now(SGT).strftime("%Y-%m-%d %H:%M SGT")
            note = gs.add_source_note(entry)
            durability = note.get("durability") or "stable"
            source = f" from {note['source']}" if note.get("source") else ""
            return f"Stored source note for {note['topic']}{source} ({durability})."
        except Exception as e:
            return f"Failed to store source note: {e}"

    elif name == "get_assistant_context":
        try:
            return build_context_snapshot(inp.get("days", 7))
        except Exception as e:
            return f"Failed to get assistant context: {e}"

    elif name == "get_timetable":
        try:
            return _timetable_for_lookup(inp.get("day", ""), inp.get("week_type", ""))
        except Exception as e:
            return f"Failed to get timetable: {e}"

    elif name == "find_available_training_slots":
        try:
            return find_available_training_slots(
                days=inp.get("days", 7),
                duration_minutes=inp.get("duration_minutes", 60),
                window_start=inp.get("window_start", "14:00"),
                window_end=inp.get("window_end", "18:30"),
                avoid_keywords=inp.get("avoid_keywords") or ["cca", "football"],
                purpose=inp.get("purpose", "training"),
            )
        except Exception as e:
            return f"Failed to find available slots: {e}"

    elif name == "get_mtl_classlists":
        try:
            return gs.format_mtl_classlists(
                teacher_query="HERWANTO",
                class_query=inp.get("class_query", ""),
                include_students=inp.get("include_students", True),
                include_scores=inp.get("include_scores", False),
            )
        except Exception as e:
            return f"Failed to get MTL classlists: {e}"

    elif name == "analyze_mtl_scores":
        try:
            return gs.format_mtl_score_analysis(
                teacher_query="HERWANTO",
                class_query=inp.get("class_query", ""),
                assessment_query=inp.get("assessment_query", ""),
                compare_from=inp.get("compare_from", ""),
                compare_to=inp.get("compare_to", ""),
            )
        except Exception as e:
            return f"Failed to analyse MTL scores: {e}"

    elif name == "update_mtl_class_score":
        try:
            result = gs.update_mtl_class_score(
                teacher_query="HERWANTO",
                class_query=inp.get("class_query", ""),
                student_query=inp.get("student_query", ""),
                score_column=inp.get("score_column", ""),
                score_value=inp.get("score_value", ""),
            )
            cls = f" [{result['class']}]" if result.get("class") else ""
            return (
                "Updated MTL classlist score: "
                f"{result['student']}{cls} - {result['column']} = {result['value']} "
                f"in {result['spreadsheet_title']} / {result['sheet_title']} ({result['range']})."
            )
        except Exception as e:
            return f"Failed to update MTL classlist score: {e}"

    elif name == "fill_mtl_percentage_scores":
        try:
            result = gs.fill_mtl_percentage_scores(
                teacher_query="HERWANTO",
                class_query=inp.get("class_query", ""),
                assessment_query=inp.get("assessment_query", ""),
                only_blank=inp.get("only_blank", True),
            )
            targets = ", ".join(result.get("targets") or []) or "matched percentage columns"
            return (
                f"Filled {result['updated_cells']} percentage cells for {targets}. "
                f"Converted {result['filled_numbers']} numeric scores and copied "
                f"{result['copied_codes']} status codes. Skipped {result['skipped']} cells."
            )
        except Exception as e:
            return f"Failed to fill MTL percentage scores: {e}"

    elif name == "remember_user_info":
        try:
            inferred_week_type, inferred_week_number = _school_week_from_text(inp.get("text", ""))
            week_note = ""
            if inferred_week_type:
                _set_current_school_week(inferred_week_type, inferred_week_number)
                week_note = f" Also set current timetable week to {inferred_week_type.upper()}."
            gs.add_memory(inp.get("category", "profile"), inp["text"])
            return f"Remembered under {inp.get('category', 'profile')}: {inp['text']}.{week_note}"
        except Exception as e:
            return f"Failed to remember: {e}"

    elif name == "create_topic_profile":
        try:
            profile = gs.add_topic_profile(inp)
            track = ", ".join(profile.get("track") or []) or "general developments"
            live = ", ".join(profile.get("live_facts") or []) or "latest/current facts"
            return (
                f"Created topic profile: {profile['topic']} ({profile.get('category') or 'interests'}). "
                f"Track: {track}. Live-check: {live}."
            )
        except Exception as e:
            return f"Failed to create topic profile: {e}"

    elif name == "set_current_school_week":
        try:
            monday = _set_current_school_week(inp["week_type"], inp.get("week_number"))
            number_note = f" School week {inp['week_number']}." if inp.get("week_number") else ""
            return f"Set timetable reference: week starting {monday} is {inp['week_type'].upper()}.{number_note}"
        except Exception as e:
            return f"Failed to set school week: {e}"

    elif name == "get_latest_news":
        try:
            return build_news_digest(inp.get("query", ""), inp.get("max_items", 2))
        except Exception as e:
            return f"Failed to fetch news: {e}"

    elif name == "get_liverpool_brief":
        try:
            return sports.build_liverpool_brief(inp.get("focus", ""), inp.get("max_items", 3))
        except Exception as e:
            return f"Failed to fetch Liverpool brief: {e}"

    elif name == "get_f1_brief":
        try:
            return sports.build_f1_brief(inp.get("focus", ""), inp.get("max_items", 3))
        except Exception as e:
            return f"Failed to fetch F1 brief: {e}"

    elif name == "get_nea_weather":
        try:
            return ws.build_weather_brief(
                inp.get("area", "Yishun"),
                include_24h=inp.get("include_24h", True),
                include_4day=inp.get("include_4day", False),
            )
        except Exception as e:
            return f"Failed to fetch NEA weather: {e}"

    elif name == "get_muis_prayer_times":
        try:
            return build_muis_prayer_time_brief(inp.get("date", ""), inp.get("prayer", ""))
        except Exception as e:
            return f"Failed to fetch MUIS prayer times: {e}"

    elif name == "get_muis_friday_khutbah":
        try:
            return build_muis_khutbah_brief(inp.get("date", ""), inp.get("language", "English"))
        except Exception as e:
            return f"Failed to fetch MUIS Friday khutbah: {e}"

    elif name == "create_calendar_event":
        try:
            start_dt = SGT.localize(datetime.strptime(f"{inp['date']} {inp['start_time']}", "%Y-%m-%d %H:%M"))
            end_dt   = SGT.localize(datetime.strptime(f"{inp['date']} {inp['end_time']}",   "%Y-%m-%d %H:%M"))
            gs.create_event(inp["title"], start_dt, end_dt,
                            inp.get("location", ""), inp.get("description", ""))
            return f"Created: {inp['title']} on {inp['date']} {inp['start_time']}–{inp['end_time']}"
        except Exception as e:
            return f"Failed to create event: {e}"

    elif name == "delete_calendar_event_by_text":
        try:
            event, score = _resolve_calendar_event_for_deletion(
                inp["query"],
                days_back=inp.get("days_back", 7),
                days_ahead=inp.get("days_ahead", 30),
            )
            if not event or score < 0.45:
                return "No confident calendar event match found. Ask with the event title/date so I do not delete the wrong thing."
            gs.delete_event(event["id"], event.get("_calendar_id", ""))
            return f"Deleted calendar event: {event.get('summary', '(No title)')} ({_event_when_text(event)})"
        except Exception as e:
            return f"Failed to delete calendar event: {e}"

    elif name == "add_reminder":
        try:
            rid = gs.add_reminder(inp["description"], inp["due_date"], inp.get("category", "General"))
            return f"Added reminder #{rid}: {inp['description']} by {inp['due_date']}"
        except Exception as e:
            return f"Failed to add reminder: {e}"

    elif name == "create_proactive_nudge":
        try:
            nudge = _create_nudge(inp["message"], inp["send_at"])
            return f"Scheduled nudge #{nudge['id']} for {nudge['send_at']}: {nudge['message']}"
        except Exception as e:
            return f"Failed to schedule nudge: {e}"

    elif name == "add_marking_task":
        try:
            task = gs.add_marking_task(
                inp["title"],
                total_scripts=inp.get("total_scripts", 0),
                stack_count=inp.get("stack_count", 1),
                collected_date=inp.get("collected_date", ""),
                notes=inp.get("notes", ""),
            )
            return f"Added to your marking tracker. {_format_marking_task(task)}"
        except Exception as e:
            return f"Failed to add marking stack: {e}"

    elif name == "update_marking_progress":
        try:
            task, score = _find_best_marking_task(inp["query"])
            if not task or score < 0.35:
                return "No confident marking stack match found."
            updated = gs.update_marking_progress(
                task["id"],
                marked_count=inp.get("marked_count"),
                increment=inp.get("increment", 0),
                done=bool(inp.get("done", False)),
            )
            return f"Updated. {_format_marking_task(updated)}"
        except Exception as e:
            return f"Failed to update marking: {e}"

    elif name == "reset_marking_load":
        try:
            result = gs.reset_marking_tasks()
            count = int(result.get("cleared_count") or 0)
            if count:
                return f"Marking load reset to zero - cleared {count} active stack{'s' if count != 1 else ''} from the board."
            return "Marking load is already at zero - no active stacks on the board."
        except Exception as e:
            return f"Failed to reset marking load: {e}"

    elif name == "get_marking_brief":
        try:
            return build_marking_brief()
        except Exception as e:
            return f"Failed to get marking brief: {e}"

    elif name == "create_daily_checkin":
        try:
            times = _parse_checkin_times(",".join(inp.get("times", [])))
            checkin = gs.add_checkin(inp["name"], inp["question"], times)
            return f"Created daily check-in #{checkin['id']}: {checkin['name']} at {', '.join(checkin['times'])}"
        except Exception as e:
            return f"Failed to create daily check-in: {e}"

    elif name == "create_break_aware_daily_checkin":
        try:
            checkin = gs.add_checkin(
                inp["name"],
                inp["question"],
                [],
                schedule_aware=True,
                target_count=inp.get("target_count", 3),
                window_start=inp.get("window_start", "08:00"),
                window_end=inp.get("window_end", "21:30"),
                min_break_minutes=inp.get("min_break_minutes", 20),
            )
            slots = _break_aware_slots(checkin, datetime.now(SGT).date())
            today_note = f" Today's planned slots: {', '.join(slots)}." if slots else " No clear break slots found for today yet."
            return f"Created break-aware daily check-in #{checkin['id']}: {checkin['name']}.{today_note}"
        except Exception as e:
            return f"Failed to create break-aware daily check-in: {e}"

    elif name == "create_document_artifact":
        try:
            spec = _generate_document_spec(
                inp["title"],
                inp["instructions"],
                inp.get("doc_type", "general"),
                inp.get("audience", ""),
                inp.get("language", ""),
            )
            path = artifacts.render_docx(spec)
            drive_file = _upload_artifact_if_possible(str(path), "doc", category="Documents")
            if google_ok():
                link_note = f" | google_doc={drive_file.get('webViewLink', '')}" if drive_file else ""
                gs.add_memory("files", f"Generated DOCX {path.name} on {datetime.now(SGT).strftime('%Y-%m-%d %H:%M SGT')} | prompt={_clip_memory_text(inp['instructions'], 240)}{link_note}")
            return _artifact_result_text("DOCX", path, drive_file)
        except Exception as e:
            return f"Failed to create document artifact: {e}"

    elif name == "create_slide_deck_artifact":
        try:
            spec = _generate_slide_spec(
                inp["title"],
                inp["instructions"],
                inp.get("audience", ""),
                inp.get("slide_count", 8),
                inp.get("language", ""),
            )
            path = artifacts.render_pptx(spec)
            drive_file = _upload_artifact_if_possible(str(path), "slides", category="Slides")
            if google_ok():
                link_note = f" | google_slides={drive_file.get('webViewLink', '')}" if drive_file else ""
                gs.add_memory("files", f"Generated PPTX {path.name} on {datetime.now(SGT).strftime('%Y-%m-%d %H:%M SGT')} | prompt={_clip_memory_text(inp['instructions'], 240)}{link_note}")
            return _artifact_result_text("PPTX", path, drive_file)
        except Exception as e:
            return f"Failed to create slide deck artifact: {e}"

    elif name == "remember_artifact_template":
        try:
            gs.add_memory("templates", f"{inp['name']}: {inp['notes']}")
            return f"Remembered artifact template: {inp['name']}"
        except Exception as e:
            return f"Failed to remember artifact template: {e}"

    elif name == "create_followup":
        try:
            followup = gs.add_followup(
                inp.get("person", ""),
                inp["topic"],
                inp["due_date"],
                inp.get("channel", ""),
                inp.get("notes", ""),
            )
            return f"Created follow-up: {_format_followup(followup)}"
        except Exception as e:
            return f"Failed to create follow-up: {e}"

    elif name == "complete_task_by_text":
        try:
            reminder, score = _find_best_reminder(inp["query"])
            if not reminder or score < 0.35:
                return "No confident reminder match found."
            ok, synced_marking = complete_reminder_by_id(reminder["id"])
            if not ok:
                return "No reminder found."
            marking_note = f" Also completed marking stack: {synced_marking['title']}." if synced_marking else ""
            return f"Marked reminder #{reminder['id']} done: {reminder['description']}.{marking_note}"
        except Exception as e:
            return f"Failed to mark task done: {e}"

    elif name == "complete_followup_by_text":
        try:
            query = inp["query"]
            if str(query).strip().isdigit():
                ok = gs.complete_followup(query)
                return f"Marked follow-up #{query} done." if ok else "No follow-up found."
            followup, score = _find_best_followup(query)
            if not followup or score < 0.35:
                return "No confident follow-up match found."
            gs.complete_followup(followup["id"])
            return f"Marked follow-up #{followup['id']} done: {followup['topic']}"
        except Exception as e:
            return f"Failed to mark follow-up done: {e}"

    elif name == "get_task_brief":
        try:
            return build_task_brief(inp.get("days", 7))
        except Exception as e:
            return f"Failed to get task brief: {e}"

    elif name == "get_gmail_brief":
        try:
            account = _normalise_gmail_account(inp.get("account", "personal"))
            if not gs.gmail_ok(account):
                return f"{gs.gmail_label(account).title()} is not connected."
            messages = gs.list_gmail_messages(
                inp.get("query", ""),
                inp.get("max_items", 10),
                account=account,
            )
            if not messages:
                return f"No {gs.gmail_label(account)} messages found."
            lines = []
            for msg in messages:
                body = (msg.get("body") or "").strip()
                excerpt = body or msg.get("snippet", "")
                lines.append(
                    f"- {msg['subject']} | From: {msg['from']} | Date: {msg.get('date', '')} | "
                    f"Snippet: {msg.get('snippet', '')} | Body excerpt: {excerpt[:1200]}"
                )
            return "\n".join(lines)
        except Exception as e:
            return f"Failed to get Gmail brief: {e}"

    elif name == "create_gmail_draft":
        try:
            account = _normalise_gmail_account(inp.get("account", "personal"))
            if not gs.gmail_ok(account):
                return f"{gs.gmail_label(account).title()} is not connected."
            draft = gs.create_gmail_draft(
                inp["to"],
                inp["subject"],
                inp["body"],
                inp.get("cc", ""),
                account=account,
            )
            return f"Created {gs.gmail_label(account)} draft: {draft.get('id', '')}"
        except Exception as e:
            return f"Failed to create Gmail draft: {e}"

    elif name == "update_project_status":
        try:
            gs.update_project(
                inp["project"],
                inp["status"],
                inp.get("milestone", ""),
                inp.get("milestone_date", ""),
                inp.get("notes", ""),
            )
            return f"Updated project: {inp['project']} - {inp['status']}"
        except Exception as e:
            return f"Failed to update project: {e}"

    return "Unknown tool."


async def handle_message(update, context):
    await _process_user_text(update, context, update.message.text)

async def _process_user_text(update, context, text: str):
    user_id = update.effective_user.id
    absorb_taste_hint(text)
    inferred_week_type, inferred_week_number = _school_week_from_text(text)
    if inferred_week_type and google_ok():
        try:
            _set_current_school_week(inferred_week_type, inferred_week_number)
            number_note = f" Week {inferred_week_number}," if inferred_week_number else ""
            await update.message.reply_text(
                f"Locked in.{number_note} this week is *{inferred_week_type.upper()}* week for the timetable.",
                parse_mode="Markdown"
            )
            return
        except Exception as e:
            logger.warning(f"School week update error: {e}")

    if google_ok() and _is_affirmative(text):
        try:
            awaiting = gs.awaiting_checkins()
            if awaiting:
                completed = []
                for checkin in awaiting:
                    if gs.complete_checkin_today(checkin["id"]):
                        completed.append(checkin["name"])
                if completed:
                    await update.message.reply_text(
                        f"Marked done for today: {', '.join(completed)}. I’ll leave you in peace until tomorrow."
                    )
                    return
        except Exception as e:
            logger.warning(f"Check-in affirmation error: {e}")

    if google_ok():
        done_match = re.match(r"^\s*(?:done with|mark done|mark .* done|completed|settled)\s+(.+)$", text, re.I)
        if done_match:
            query = done_match.group(1).strip()
            try:
                reminder, r_score = _find_best_reminder(query)
                followup, f_score = _find_best_followup(query)
                marking, m_score = _find_best_marking_task(query)
                event, e_score = _find_best_calendar_event(query)
                if marking and m_score >= max(0.35, r_score, f_score, e_score):
                    updated = gs.update_marking_progress(marking["id"], done=True)
                    await update.message.reply_text(f"Completed marking: {updated['title']}")
                    return
                if reminder and r_score >= max(0.35, f_score, e_score):
                    _, synced_marking = complete_reminder_by_id(reminder["id"])
                    marking_note = f" Also completed marking: {synced_marking['title']}." if synced_marking else ""
                    await update.message.reply_text(f"Marked done: [{reminder['id']}] {reminder['description']}.{marking_note}")
                    return
                if followup and f_score >= max(0.35, e_score):
                    gs.complete_followup(followup["id"])
                    await update.message.reply_text(f"Follow-up done: {followup['topic']}")
                    return
                if event and e_score >= 0.5:
                    gs.delete_event(event["id"], event.get("_calendar_id", ""))
                    await update.message.reply_text(f"Removed from calendar: {event.get('summary', '(No title)')} ({_event_when_text(event)})")
                    return
            except Exception as e:
                logger.warning(f"Natural done handling error: {e}")

        calendar_delete_match = re.match(
            r"^\s*(?:cancel|delete|remove)\s+(?:the\s+)?(?:calendar\s+)?(?:event\s+)?(.+?)(?:\s+from\s+(?:my\s+)?calendar)?\s*$",
            text,
            re.I,
        )
        if calendar_delete_match:
            query = calendar_delete_match.group(1).strip()
            try:
                event, score = _resolve_calendar_event_for_deletion(query)
                if not event or score < 0.45:
                    await update.message.reply_text("I could not confidently match that to a calendar event. Give me the event title or date so I do not delete the wrong thing.")
                    return
                gs.delete_event(event["id"], event.get("_calendar_id", ""))
                await update.message.reply_text(f"Removed from calendar: {event.get('summary', '(No title)')} ({_event_when_text(event)})")
                return
            except Exception as e:
                logger.warning(f"Natural calendar delete error: {e}")

    history = get_history(user_id)
    user_content = text
    if re.search(r"\b(?:work|moe|school|personal)\s+(?:gmail|email|emails|mail)\b", text, re.I):
        account_hint, _ = _extract_gmail_account_from_text(text)
        user_content = f"{text}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
    user_content = f"{user_content}{intent_lens_hint(text)}{source_discipline_hint(text)}"
    history.append({"role": "user", "content": user_content})
    if len(history) > MAX_TURNS:
        # Summarise the about-to-be-dropped portion before trimming
        try:
            _summarise_conversation_to_memory(history[:-MAX_TURNS + 2])
        except Exception:
            pass
        history = history[-MAX_TURNS:]

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    try:
        messages = list(history)
        reply_text = await _run_agentic_claude(messages, max_tokens=1024)

        # Save only user message + final reply to persistent history
        history.append({"role": "assistant", "content": reply_text})
        save_history(user_id, history)
        record_chat_learning_event(text, reply_text, source="telegram")
        # Detect implicit corrections in background — does not block the reply
        asyncio.create_task(_detect_implicit_correction(text, history))
        await reply(update, reply_text)

    except Exception as e:
        logger.error(f"Claude error: {e}")
        await update.message.reply_text("Error. Try again.")

def _openai_ok() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY", "").strip())

async def handle_voice(update, context):
    if not _openai_ok():
        await update.message.reply_text("Voice notes need OPENAI_API_KEY configured first.")
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        from openai import OpenAI

        voice = update.message.voice
        file = await context.bot.get_file(voice.file_id)
        with tempfile.NamedTemporaryFile(suffix=".ogg") as tmp:
            await file.download_to_drive(custom_path=tmp.name)
            client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
            with open(tmp.name, "rb") as audio:
                transcript = client.audio.transcriptions.create(
                    model=os.environ.get("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe"),
                    file=audio,
                )
        text = getattr(transcript, "text", str(transcript)).strip()
        if not text:
            await update.message.reply_text("I could not transcribe that voice note.")
            return
        await reply(update, f"Transcribed: {text}")
        await _process_user_text(update, context, text)
    except Exception as e:
        logger.error(f"Voice note error: {e}")
        await update.message.reply_text(f"Could not process voice note: {e}")

# ─── SCHEDULED JOBS ──────────────────────────────────────────────────────────

def _queue_app_notification(kind: str, title: str, body: str, source: str = ""):
    if _should_suppress_notification(source, kind):
        logger.info(f"Notification suppressed by preference memory for source={source or kind}")
        return None
    try:
        item = gs.enqueue_app_notification(kind, title, body, source=source)
        if item.get("_duplicate"):
            logger.info(f"Notification already active for source={source or kind}")
            return None
        if item.get("id"):
            _record_notification_outcome(
                "queued",
                notification_id=item.get("id", ""),
                source=source,
                kind=kind,
                title=title,
            )
        if not _quiet_hours_active() or kind in {"urgent"}:
            sent = gs.send_web_push_notification(
                title,
                body,
                data={"id": item.get("id", ""), "kind": kind, "source": source},
            )
            _record_notification_outcome(
                "pushed" if sent else "push_missed",
                notification_id=item.get("id", ""),
                source=source,
                kind=kind,
                title=title,
            )
            if sent == 0:
                logger.warning(f"No web push delivery confirmed for notification source={source or kind}")
        return item
    except Exception as e:
        logger.warning(f"App notification queue error: {e}")
        return None


def _quiet_hours_active(now: datetime | None = None) -> bool:
    now = (now or datetime.now(SGT)).astimezone(SGT)
    start = int(os.environ.get("HIRA_QUIET_START_HOUR", "23") or 23)
    end = int(os.environ.get("HIRA_QUIET_END_HOUR", "5") or 5)
    return now.hour >= start or now.hour < end


MORNING_BRIEFING_SENT_KEY = "last_morning_briefing_date"
EVENING_BRIEFING_SENT_KEY = "last_evening_briefing_date"
NEWS_DIGEST_HISTORY_KEY = "news_digest_history"
NEWS_DIGEST_FRESHNESS_HOURS = 48
NOTIFICATION_NEGATIVE_ACTIONS = {"dismissed", "not_now"}
NOTIFICATION_COOLDOWN_HOURS = {
    "checkin": 8,
    "followup": 18,
    "proactive_intelligence": 24,
    "friday_checkin": 36,
    "weekly_planning": 24,
    "friday_khutbah": 24,
    "web_friday_khutbah": 24,
}


def _notification_source_group(source: str, kind: str = "") -> str:
    clean_source = str(source or "").strip()
    if clean_source:
        return clean_source.split(":", 1)[0]
    return str(kind or "notice").strip() or "notice"


def _record_notification_outcome(
    action: str,
    notification_id: str = "",
    source: str = "",
    kind: str = "",
    rating: str = "",
    client_id: str = "",
    title: str = "",
):
    try:
        gs.add_notification_outcome(
            action=action,
            notification_id=notification_id,
            source=source,
            kind=kind,
            rating=rating,
            client_id=client_id,
            title=title,
        )
    except Exception as exc:
        logger.warning(f"Could not persist notification outcome: {exc}")


def _notification_feedback_bias(source: str, kind: str, now: datetime | None = None, days: int = 30) -> int:
    current = now or datetime.now(SGT)
    threshold = current - timedelta(days=max(1, int(days or 30)))
    exact = str(source or "").strip()
    group = _notification_source_group(source, kind)
    score = 0
    try:
        outcomes = gs.get_notification_outcomes()
    except Exception:
        return 0
    for item in outcomes:
        try:
            created = datetime.fromisoformat(item.get("created", ""))
        except Exception:
            continue
        if created < threshold:
            continue
        action = str(item.get("action", "")).strip()
        item_source = str(item.get("source", "")).strip()
        item_group = str(item.get("group", "")).strip() or _notification_source_group(item_source, item.get("kind", ""))
        if item_source != exact and item_group != group:
            continue
        weight = 2 if item_source == exact else 1
        if action == "useful":
            score += 2 * weight
        elif action in NOTIFICATION_NEGATIVE_ACTIONS:
            score -= 2 * weight
    return score


def _should_suppress_notification(source: str, kind: str, now: datetime | None = None) -> bool:
    current = now or datetime.now(SGT)
    exact = str(source or "").strip()
    group = _notification_source_group(source, kind)
    cooldown_hours = NOTIFICATION_COOLDOWN_HOURS.get(group)
    if not cooldown_hours or group in {"briefing", "morning_briefing", "evening_briefing", "prayer", "web_prayer", "nudge"}:
        return False
    threshold = current - timedelta(hours=cooldown_hours)
    try:
        outcomes = gs.get_notification_outcomes()
    except Exception:
        return False
    for item in reversed(outcomes):
        try:
            created = datetime.fromisoformat(item.get("created", ""))
        except Exception:
            continue
        if created < threshold:
            break
        action = str(item.get("action", "")).strip()
        if action not in NOTIFICATION_NEGATIVE_ACTIONS:
            continue
        item_source = str(item.get("source", "")).strip()
        item_group = str(item.get("group", "")).strip() or _notification_source_group(item_source, item.get("kind", ""))
        if item_source == exact or item_group == group:
            return True
    return False


def _get_news_digest_history() -> list[dict]:
    try:
        raw = gs.get_config(NEWS_DIGEST_HISTORY_KEY) or "[]"
        history = json.loads(raw)
    except Exception:
        return []
    if not isinstance(history, list):
        return []
    clean = []
    for item in history[-240:]:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        shown_at = str(item.get("shown_at", "")).strip()
        title = str(item.get("title", "")).strip()
        if not key or not shown_at:
            continue
        clean.append({"key": key, "shown_at": shown_at, "title": title})
    return clean


def _recent_news_digest_keys(now: datetime | None = None, hours: int = NEWS_DIGEST_FRESHNESS_HOURS) -> set[str]:
    current = now or datetime.now(SGT)
    threshold = current - timedelta(hours=max(1, int(hours or NEWS_DIGEST_FRESHNESS_HOURS)))
    keys = set()
    for item in _get_news_digest_history():
        try:
            shown_at = datetime.fromisoformat(item["shown_at"])
        except Exception:
            continue
        if shown_at >= threshold:
            keys.add(item["key"])
    return keys


def _remember_news_digest_entries(entries: list[dict], now: datetime | None = None):
    if not entries:
        return
    current = now or datetime.now(SGT)
    history = _get_news_digest_history()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        item = entry.get("item") if isinstance(entry.get("item"), dict) else {}
        key = str(entry.get("key", "")).strip()
        title = str(item.get("title", "")).strip()
        if not key:
            continue
        history.append({"key": key, "title": title, "shown_at": current.isoformat()})
    history = history[-240:]
    try:
        gs.set_config(NEWS_DIGEST_HISTORY_KEY, json.dumps(history, ensure_ascii=False))
    except Exception as exc:
        logger.warning(f"Could not persist news digest history: {exc}")


def _fresh_morning_digest(now: datetime | None = None, record: bool = False) -> str:
    current = now or datetime.now(SGT)
    entries = build_curated_digest_entries(now=current, limit=4, fetch_limit=4, record=record)
    return format_curated_digest(entries)


async def send_morning_briefing_once(context=None, force: bool = False, source: str = "morning_briefing") -> bool:
    if not force and not _acquire_job_lock(source, 900):
        return False
    if not google_ok():
        logger.warning("Morning briefing skipped: Google services are not connected")
        return False
    today_key = datetime.now(SGT).strftime("%Y-%m-%d")
    try:
        if not force and gs.get_config(MORNING_BRIEFING_SENT_KEY) == today_key:
            return False
        text = build_briefing(record_news_digest=True)
        if context is not None:
            await _send_telegram_notification(context, text)
        _queue_app_notification("briefing", "Morning briefing", text, source=f"{source}:{today_key}")
        gs.set_config(MORNING_BRIEFING_SENT_KEY, today_key)
        return True
    except Exception as e:
        logger.error(f"Morning briefing error: {e}")
        return False


async def send_evening_briefing_once(context=None, force: bool = False, source: str = "evening_briefing") -> bool:
    if not force and not _acquire_job_lock(source, 900):
        return False
    if not google_ok():
        logger.warning("Evening briefing skipped: Google services are not connected")
        return False
    today_key = datetime.now(SGT).strftime("%Y-%m-%d")
    try:
        if not force and gs.get_config(EVENING_BRIEFING_SENT_KEY) == today_key:
            return False
        text = build_evening_briefing()
        if context is not None:
            await _send_telegram_notification(context, text)
        _queue_app_notification("briefing", "Evening roundup", text, source=f"{source}:{today_key}")
        gs.set_config(EVENING_BRIEFING_SENT_KEY, today_key)
        return True
    except Exception as e:
        logger.error(f"Evening briefing error: {e}")
        return False


async def _send_telegram_notification(context, text: str):
    try:
        if context is None:
            return False
        if _quiet_hours_active():
            return False
        chat_id = gs.get_config("chat_id")
        if not chat_id:
            return False
        await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
        return True
    except Exception as e:
        logger.warning(f"Telegram notification error: {e}")
        return False


async def morning_briefing_job(context):
    await send_morning_briefing_once(context, source="morning_briefing")

async def friday_checkin_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("friday_checkin", 900):
        return
    try:
        sent = await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=datetime.now(SGT), families={"friday_checkin"}), limit=1)
        if sent:
            return
        projs = gs.get_projects()
        lines = ["*Weekly project check-in*\n"]
        for p in projs:
            lines.append(_format_project_line(p, include_updated=True))
        if not projs:
            lines.append("No projects tracked yet.")
        lines.append("\nTell H.I.R.A naturally or use /update to log progress.")
        text = "\n".join(lines)
        await _send_telegram_notification(context, text)
        _queue_app_notification("update", "Weekly project check-in", text, source="friday_checkin")
    except Exception as e:
        logger.error(f"Friday check-in error: {e}")

async def evening_briefing_job(context):
    await send_evening_briefing_once(context, source="evening_briefing")

async def weekly_planning_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("weekly_planning", 900):
        return
    try:
        sent = await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=datetime.now(SGT), families={"weekly_planning"}), limit=1)
        if sent:
            return
        text = build_weekly_plan()
        await _send_telegram_notification(context, text)
        _queue_app_notification("update", "Weekly plan", text, source="weekly_planning")
    except Exception as e:
        logger.error(f"Weekly planning error: {e}")

async def proactive_nudges_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("proactive_nudges", max(60, JOB_INTERVALS["proactive_nudges"] - 5)):
        return
    try:
        _log_memory("before proactive_nudges")
        now = datetime.now(SGT)
        await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=now, families={"nudge"}), limit=6)
    except Exception as e:
        logger.error(f"Proactive nudge error: {e}")
    finally:
        _finish_background_job("proactive_nudges")


async def proactive_intelligence_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("proactive_intelligence", max(900, JOB_INTERVALS["proactive_intelligence"] - 30)):
        return
    try:
        _log_memory("before proactive_intelligence")
        now = datetime.now(SGT)
        await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=now, days=7, families={"intelligence"}), limit=3)
    except Exception as e:
        logger.error(f"Proactive intelligence error: {e}")
    finally:
        _finish_background_job("proactive_intelligence")

async def daily_checkins_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("daily_checkins", max(60, JOB_INTERVALS["daily_checkins"] - 5)):
        return
    try:
        _log_memory("before daily_checkins")
        now = datetime.now(SGT)
        await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=now, families={"checkin"}), limit=6)
    except Exception as e:
        logger.error(f"Daily check-in error: {e}")
    finally:
        _finish_background_job("daily_checkins")


async def prayer_reminders_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("prayer_reminders", 55):
        return
    try:
        await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=datetime.now(SGT), families={"prayer"}), limit=2)
    except Exception as e:
        logger.error(f"Prayer reminder error: {e}")


async def friday_khutbah_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("friday_khutbah", 900):
        return
    try:
        await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=datetime.now(SGT), families={"friday_khutbah"}), limit=1)
    except Exception as e:
        logger.error(f"Friday khutbah heads-up error: {e}")


async def followups_job(context):
    if not google_ok():
        return
    if not _acquire_job_lock("followups", max(300, JOB_INTERVALS["followups"] - 5)):
        return
    try:
        _log_memory("before followups")
        await _dispatch_proactive_candidates(context, build_proactive_v2_queue(now=datetime.now(SGT), families={"followup"}), limit=6)
    except Exception as e:
        logger.error(f"Follow-up job error: {e}")
    finally:
        _finish_background_job("followups")


async def memory_consolidation_job(context=None):
    """Weekly job: ask Claude to review corrections + reflections, promote durable
    learnings into preferences/constraints, and prune stale entries.
    Runs Friday night at 22:30 SGT. Safe to call manually for a forced pass."""
    if not google_ok():
        return
    if not _acquire_job_lock("memory_consolidation", 3600):
        return
    try:
        _log_memory("before memory_consolidation")
        memory = gs.get_memory()
        corrections = memory.get("correction_ledger", [])
        reflections = memory.get("self_reflections", [])
        if not corrections and not reflections:
            logger.info("Memory consolidation: nothing to consolidate.")
            return

        now_str = datetime.now(SGT).strftime("%Y-%m-%d %H:%M SGT")
        prompt = f"""You are H.I.R.A doing a weekly self-improvement pass on your own memory.

Today: {now_str}

Review these corrections (things Herwanto has explicitly corrected you on) and reflections (things you noted about your own behaviour):

CORRECTIONS ({len(corrections)} items):
{json.dumps(corrections[-30:], ensure_ascii=False, indent=2)}

SELF-REFLECTIONS ({len(reflections)} items):
{json.dumps(reflections[-30:], ensure_ascii=False, indent=2)}

Your tasks:
1. Identify any correction or reflection that represents a STABLE, DURABLE rule (not a one-off fact) — e.g. "always call get_muis_prayer_times before giving prayer times", "Herwanto prefers direct answers without preamble", "never guess weekdays".
2. For each durable rule, produce a concise preference or constraint to add to memory.
3. Identify any corrections/reflections that are now stale, superseded, or already captured as a preference — mark them for pruning.

Return ONLY valid JSON:
{{
  "promote": [
    {{"category": "preferences|constraints", "text": "concise rule to remember"}}
  ],
  "growth_note": "One sentence summary of what H.I.R.A learned this cycle.",
  "prune_count": 0
}}

Rules: be conservative — only promote genuinely durable rules. Return ONLY JSON."""

        resp = claude.messages.create(
            model=QUICK_MODEL,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        result = _json_from_claude_text(resp.content[0].text)
        promoted = result.get("promote") or []
        growth_note = result.get("growth_note", "")

        for item in promoted:
            category = item.get("category", "preferences")
            text = item.get("text", "").strip()
            if text and category in MEMORY_DISPLAY_CATEGORIES:
                gs.add_memory(category, f"[consolidated {datetime.now(SGT).strftime('%Y-%m-%d')}] {text}")

        if growth_note:
            gs.add_self_reflection({
                "date": now_str,
                "source": "memory_consolidation_job",
                "trigger": "Weekly self-improvement pass",
                "learned": growth_note,
                "next_behavior": "Apply promoted preferences and constraints in future answers.",
            })

        logger.info(
            f"Memory consolidation complete: {len(promoted)} rule(s) promoted. Note: {growth_note}"
        )
        if context and promoted:
            summary = "\n".join(f"- [{item['category']}] {item['text']}" for item in promoted)
            await _send_telegram_notification(
                context,
                f"*H.I.R.A self-improvement pass*\n\n{len(promoted)} durable rule(s) promoted to memory:\n{summary}\n\n_{growth_note}_"
            )

    except Exception as e:
        logger.error(f"Memory consolidation error: {e}")
    finally:
        _finish_background_job("memory_consolidation")


async def _pwa_worker_daily_loop(name: str, hour: int, minute: int, job, days: tuple[int, ...] | None = None):
    last_attempt_date = None
    while True:
        try:
            now = datetime.now(SGT)
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            grace_until = target + timedelta(minutes=DAILY_JOB_GRACE_MINUTES)
            today_key = now.strftime("%Y-%m-%d")
            if (
                target <= now <= grace_until
                and today_key != last_attempt_date
                and (days is None or now.weekday() in days)
            ):
                logger.info(f"PWA worker running daily job {name} for {today_key}")
                last_attempt_date = today_key
                await job(None)
            if now >= grace_until:
                target = target + timedelta(days=1)
            await asyncio.sleep(max(60, min(1800, (target - now).total_seconds())))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"PWA worker daily job {name} error: {e}")
            await asyncio.sleep(300)


async def _pwa_worker_repeating_loop(name: str, interval: int, first: int, job):
    await asyncio.sleep(first)
    while True:
        try:
            await job(None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"PWA worker repeating job {name} error: {e}")
        await asyncio.sleep(interval)


async def run_pwa_notification_worker():
    logger.info("H.I.R.A PWA notification worker running.")
    _log_memory("pwa_worker startup", force=True)
    morning_hour, morning_minute = MORNING_BRIEFING_TIME
    evening_hour, evening_minute = EVENING_BRIEFING_TIME
    tasks = [
        asyncio.create_task(_pwa_worker_daily_loop("morning_briefing", morning_hour, morning_minute, morning_briefing_job)),
        asyncio.create_task(_pwa_worker_daily_loop("evening_briefing", evening_hour, evening_minute, evening_briefing_job)),
        asyncio.create_task(_pwa_worker_daily_loop("weekly_planning", 19, 30, weekly_planning_job, days=(6,))),
        asyncio.create_task(_pwa_worker_daily_loop("friday_khutbah", 10, 30, friday_khutbah_job, days=(4,))),
        asyncio.create_task(_pwa_worker_daily_loop("friday_checkin", 17, 0, friday_checkin_job, days=(4,))),
        asyncio.create_task(_pwa_worker_daily_loop("memory_consolidation", 22, 30, memory_consolidation_job, days=(4,))),
        asyncio.create_task(_pwa_worker_repeating_loop(
            "proactive_nudges",
            JOB_INTERVALS["proactive_nudges"],
            10,
            proactive_nudges_job,
        )),
        asyncio.create_task(_pwa_worker_repeating_loop(
            "proactive_intelligence",
            JOB_INTERVALS["proactive_intelligence"],
            15,
            proactive_intelligence_job,
        )),
        asyncio.create_task(_pwa_worker_repeating_loop(
            "daily_checkins",
            JOB_INTERVALS["daily_checkins"],
            20,
            daily_checkins_job,
        )),
        asyncio.create_task(_pwa_worker_repeating_loop("prayer_reminders", 60, 30, prayer_reminders_job)),
        asyncio.create_task(_pwa_worker_repeating_loop(
            "followups",
            JOB_INTERVALS["followups"],
            40,
            followups_job,
        )),
    ]
    await asyncio.gather(*tasks)


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    from telegram.ext import Application, CommandHandler, MessageHandler, filters

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN not set")
    if not ALLOWED_TELEGRAM_USER_IDS and not TELEGRAM_OPEN_DEV_MODE:
        raise ValueError("HIRA_ALLOWED_USER_IDS must be set unless HIRA_TELEGRAM_OPEN_DEV_MODE=1")
    app = Application.builder().token(token).build()

    # ── Authorization filter ──────────────────────────────────────────────────
    # If HIRA_ALLOWED_USER_IDS is configured, restrict all handlers to those
    # user IDs only. Open access requires explicit HIRA_TELEGRAM_OPEN_DEV_MODE.
    if ALLOWED_TELEGRAM_USER_IDS:
        _auth = filters.User(user_id=list(ALLOWED_TELEGRAM_USER_IDS))
        async def _unauthorized_handler(update, context):
            try:
                await update.message.reply_text("Unauthorized.")
            except Exception:
                pass
        # Catch-all for unauthorized users (must be added last, lowest priority)
        _unauth_handler_obj = MessageHandler(~_auth & filters.ALL, _unauthorized_handler)
    else:
        _auth = filters.ALL
        _unauth_handler_obj = None

    def _cmd(name, fn):
        return CommandHandler(name, fn, filters=_auth)

    app.add_handler(_cmd("start",    start))
    app.add_handler(_cmd("lessons",  lessons_cmd))
    app.add_handler(_cmd("setweek",  setweek_cmd))
    app.add_handler(_cmd("today",    today_cmd))
    app.add_handler(_cmd("tomorrow", tomorrow_cmd))
    app.add_handler(_cmd("week",     week_cmd))
    app.add_handler(_cmd("due",      due_cmd))
    app.add_handler(_cmd("remind",   remind_cmd))
    app.add_handler(_cmd("done",     done_cmd))
    app.add_handler(_cmd("nudge",    nudge_cmd))
    app.add_handler(_cmd("nudges",   nudges_cmd))
    app.add_handler(_cmd("cancelnudge", cancelnudge_cmd))
    app.add_handler(_cmd("checkin",  checkin_cmd))
    app.add_handler(_cmd("checkins", checkins_cmd))
    app.add_handler(_cmd("cancelcheckin", cancelcheckin_cmd))
    app.add_handler(_cmd("projects", projects_cmd))
    app.add_handler(_cmd("update",   update_cmd))
    app.add_handler(_cmd("doc",      doc_cmd))
    app.add_handler(_cmd("slides",   slides_cmd))
    app.add_handler(_cmd("template", template_cmd))
    app.add_handler(_cmd("templates", templates_cmd))
    app.add_handler(_cmd("artifacts", artifacts_cmd))
    app.add_handler(_cmd("files",    files_cmd))
    app.add_handler(_cmd("classlists", classlists_cmd))
    app.add_handler(_cmd("tasks",    tasks_cmd))
    app.add_handler(_cmd("marking",  marking_cmd))
    app.add_handler(_cmd("marked",   marked_cmd))
    app.add_handler(_cmd("taskmeta", taskmeta_cmd))
    app.add_handler(_cmd("donetask", done_text_cmd))
    app.add_handler(_cmd("followup", followup_cmd))
    app.add_handler(_cmd("followups", followups_cmd))
    app.add_handler(_cmd("donefollowup", donefollowup_cmd))
    app.add_handler(_cmd("evening",  evening_cmd))
    app.add_handler(_cmd("weekly",   weekly_cmd))
    app.add_handler(_cmd("gmail",    gmail_cmd))
    app.add_handler(_cmd("gmaildraft", gmaildraft_cmd))
    app.add_handler(_cmd("briefing", briefing_cmd))
    app.add_handler(_cmd("prayers",  prayers_cmd))
    app.add_handler(_cmd("khutbah",  khutbah_cmd))
    app.add_handler(_cmd("agenda",   agenda_cmd))
    app.add_handler(_cmd("remember", remember_cmd))
    app.add_handler(_cmd("memory",   memory_cmd))
    app.add_handler(_cmd("forget",   forget_cmd))
    app.add_handler(_cmd("search",   search_cmd))
    app.add_handler(_cmd("weather",  weather_cmd))
    app.add_handler(_cmd("news",     news_cmd))
    app.add_handler(_cmd("watch",    watch_cmd))
    app.add_handler(_cmd("watchlist", watchlist_cmd))
    app.add_handler(_cmd("unwatch",  unwatch_cmd))
    app.add_handler(_cmd("addcal",   addcal_cmd))
    app.add_handler(_cmd("clear",    clear_cmd))
    app.add_handler(MessageHandler(_auth & filters.PHOTO,        handle_photo))
    app.add_handler(MessageHandler(_auth & filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(_auth & filters.VOICE,        handle_voice))
    app.add_handler(MessageHandler(_auth & filters.TEXT & ~filters.COMMAND, handle_message))
    if _unauth_handler_obj:
        app.add_handler(_unauth_handler_obj)
    jq = app.job_queue
    morning_hour, morning_minute = MORNING_BRIEFING_TIME
    evening_hour, evening_minute = EVENING_BRIEFING_TIME
    jq.run_daily(morning_briefing_job, time=dt_time(morning_hour, morning_minute, 0, tzinfo=SGT), name="morning_briefing")
    jq.run_daily(evening_briefing_job, time=dt_time(evening_hour, evening_minute, 0, tzinfo=SGT), name="evening_briefing")
    jq.run_daily(weekly_planning_job, time=dt_time(19, 30, 0, tzinfo=SGT), days=(6,), name="weekly_planning")
    jq.run_daily(friday_khutbah_job, time=dt_time(10, 30, 0, tzinfo=SGT), days=(4,), name="friday_khutbah")
    jq.run_daily(friday_checkin_job,   time=dt_time(17, 0, 0, tzinfo=SGT), days=(4,), name="friday_checkin")
    jq.run_daily(memory_consolidation_job, time=dt_time(22, 30, 0, tzinfo=SGT), days=(4,), name="memory_consolidation")
    jq.run_repeating(
        proactive_nudges_job,
        interval=JOB_INTERVALS["proactive_nudges"],
        first=10,
        name="proactive_nudges",
        job_kwargs={"coalesce": True, "max_instances": 1},
    )
    jq.run_repeating(
        proactive_intelligence_job,
        interval=JOB_INTERVALS["proactive_intelligence"],
        first=15,
        name="proactive_intelligence",
        job_kwargs={"coalesce": True, "max_instances": 1},
    )
    jq.run_repeating(
        daily_checkins_job,
        interval=JOB_INTERVALS["daily_checkins"],
        first=20,
        name="daily_checkins",
        job_kwargs={"coalesce": True, "max_instances": 1},
    )
    jq.run_repeating(
        prayer_reminders_job,
        interval=60,
        first=30,
        name="prayer_reminders",
        job_kwargs={"coalesce": True, "max_instances": 1},
    )
    jq.run_repeating(
        followups_job,
        interval=JOB_INTERVALS["followups"],
        first=40,
        name="followups",
        job_kwargs={"coalesce": True, "max_instances": 1},
    )
    logger.info("Herwanto OS running — all systems active.")
    _log_memory("startup", force=True)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
