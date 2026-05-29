from __future__ import annotations

"""
Google Calendar (read) + Google Sheets (read/write) via Service Account.
Postgres is the preferred durable store for H.I.R.A-owned state when
DATABASE_URL is configured; Sheets remains the external/read-only source and
temporary fallback during migration.
"""

import os
import json
import base64
import csv
import io
import logging
import re
import tempfile
import uuid
import pytz
import threading
import statistics
import time
from functools import lru_cache
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

try:
    import postgres_storage as pg_storage
except Exception:  # pragma: no cover - import failure is reported at use-site
    pg_storage = None

SGT = pytz.timezone('Asia/Singapore')
logger = logging.getLogger(__name__)
NUDGE_STALE_HOURS = 24
FOLLOWUP_STALE_DAYS = 30

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
]

SHEETS_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
]

GMAIL_SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.compose',
]

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
DEFAULT_CCA_SCHEDULE_URL = "https://docs.google.com/spreadsheets/d/1L5FGME5itmc3vknwL0xSsIrz4qJ3n6z1YfxffgeB3nU/edit?usp=sharing"
CCA_SCHEDULE_URL = os.environ.get("CCA_SCHEDULE_URL", DEFAULT_CCA_SCHEDULE_URL)
CCA_SCHEDULE_SPREADSHEET_ID = os.environ.get("CCA_SCHEDULE_SPREADSHEET_ID", "1L5FGME5itmc3vknwL0xSsIrz4qJ3n6z1YfxffgeB3nU")
CCA_SCHEDULE_GID = os.environ.get("CCA_SCHEDULE_GID", "1961438111")
CCA_SCHEDULE_RESOURCE_KEY = os.environ.get("CCA_SCHEDULE_RESOURCE_KEY", "")
DEFAULT_CLASSLIST_SHEET_IDS = [
    "1wK1YTRzyjQ5a976D_Z4q886sZofTQnpXT4kMtf1LjKk",  # 2026 S1 MTL CLASSLIST
    "1kHvPV58jdk9mNRuQSpS4bWy93UsSvYFT-CPw0rimkvs",  # 2026 S2 MTL CLASSLIST
    "1yJHYp7VDDstnGIFITHOByLbV0oKy-ZEXy6ifmtPL1qg",  # 2026 S3 MTL CLASSLIST
    "1sKTqAgllMFy0Fq4nLBpF2vxdGpzn3ByMGKCq-NuLSDI",  # 2026 S4 MTL CLASSLIST
]
_thread_local = threading.local()
_redis_client = None
_config_cache_lock = threading.RLock()
_config_cache = {
    "expires_at": 0.0,
    "values": None,
    "row_numbers": None,
    "row_count": 0,
    "stale_after_error": False,
}
_memory_cache = {
    "expires_at": 0.0,
    "value": None,
    "source": "",
}
_classlist_cache_lock = threading.RLock()
_classlist_cache: dict[str, dict] = {}
_REDIS_NUDGE_KEY = "hira:proactive_nudges:fallback"
_REDIS_ASSISTANT_MEMORY_KEY = "hira:assistant_memory:fallback"
_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY = "hira:web_push_subscriptions:fallback"
_REDIS_WEB_PUSH_DELIVERY_LOG_KEY = "hira:web_push_delivery_log:fallback"
_app_notifications_mutation_lock = threading.RLock()
_nudges_mutation_lock = threading.RLock()
_reminders_mutation_lock = threading.RLock()


class _StorageMutationLock:
    def __init__(self, name: str, local_lock: threading.RLock, timeout_seconds: float = 5.0):
        self.name = name
        self.local_lock = local_lock
        self.timeout_seconds = timeout_seconds
        self.redis = None
        self.token = uuid.uuid4().hex
        self.redis_key = f"hira:lock:{name}"

    def __enter__(self):
        self.local_lock.acquire()
        try:
            self.redis = _get_redis()
        except Exception:
            self.redis = None
        if not self.redis:
            return self
        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            try:
                if self.redis.set(self.redis_key, self.token, nx=True, ex=max(6, int(self.timeout_seconds) + 3)):
                    return self
            except Exception as exc:
                logger.warning(f"Could not acquire Redis lock {self.name}: {exc}")
                self.redis = None
                return self
            time.sleep(0.05)
        logger.warning(f"Timed out waiting for Redis lock {self.name}; continuing with local lock only.")
        self.redis = None
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.redis and self.redis.get(self.redis_key) == self.token:
                self.redis.delete(self.redis_key)
        except Exception as release_exc:
            logger.warning(f"Could not release Redis lock {self.name}: {release_exc}")
        finally:
            self.local_lock.release()


def _storage_mutation_lock(name: str, local_lock: threading.RLock) -> _StorageMutationLock:
    return _StorageMutationLock(name, local_lock)

def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return default


_CONFIG_CACHE_TTL_SECONDS = max(0, _int_env("HIRA_CONFIG_CACHE_TTL_SECONDS", 300))
_MEMORY_CACHE_TTL_SECONDS = max(0, _int_env("HIRA_MEMORY_CACHE_TTL_SECONDS", 300))
_CLASSLIST_CACHE_TTL_SECONDS = max(0, _int_env("HIRA_CLASSLIST_CACHE_TTL_SECONDS", 120))
_CLASSLIST_GRID_FIELDS = "properties(title),sheets(properties(sheetId,title),data(rowData(values(formattedValue,effectiveValue))))"
APP_NOTIFICATION_PUSH_BODY_LIMIT = max(240, min(1800, _int_env("HIRA_WEB_PUSH_BODY_LIMIT", 650)))
APP_NOTIFICATION_SHEET = "AppNotifications"
APP_NOTIFICATION_HEADERS = ["id", "kind", "title", "body", "created", "source", "seen_by", "archived"]
MEMORY_LOG_SHEET = "MemoryLog"
MEMORY_LOG_HEADERS = ["created", "category", "text", "source"]
SCORE_STATUS_LABELS = {
    "AB": "absent",
    "VR": "valid reason",
    "MC": "medical certificate",
}


@lru_cache(maxsize=4)
def _service_account_info(raw: str) -> dict:
    return json.loads(base64.b64decode(raw).decode("utf-8"))

def _split_ids(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _configured_calendar_ids() -> list[str]:
    """Read configured calendar IDs, preserving write-calendar first."""
    ids = _split_ids(os.environ.get("GOOGLE_CALENDAR_IDS", ""))
    if not ids:
        ids = _split_ids(os.environ.get("GOOGLE_CALENDAR_ID", "")) or ["primary"]

    deduped = []
    seen = set()
    for cal_id in ids:
        if cal_id not in seen:
            deduped.append(cal_id)
            seen.add(cal_id)
    return deduped


CALENDAR_IDS = _configured_calendar_ids()


def _creds(scopes=None, subject: str = ""):
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    sa_info = _service_account_info(raw)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes or SCOPES)
    if subject:
        creds = creds.with_subject(subject)
    return creds


def _service_account_email() -> str:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        return ""
    try:
        return str(_service_account_info(raw).get("client_email", "") or "").strip()
    except Exception:
        return ""


def _oauth_value(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def _work_google_oauth_configured() -> bool:
    return bool(
        _oauth_value("GOOGLE_WORK_SHEETS_REFRESH_TOKEN")
        and _oauth_value(
            "GOOGLE_WORK_SHEETS_CLIENT_ID",
            "GOOGLE_WORK_GMAIL_CLIENT_ID",
            "GOOGLE_SHEETS_CLIENT_ID",
            "GOOGLE_USER_CLIENT_ID",
            "GOOGLE_GMAIL_CLIENT_ID",
        )
        and _oauth_value(
            "GOOGLE_WORK_SHEETS_CLIENT_SECRET",
            "GOOGLE_WORK_GMAIL_CLIENT_SECRET",
            "GOOGLE_SHEETS_CLIENT_SECRET",
            "GOOGLE_USER_CLIENT_SECRET",
            "GOOGLE_GMAIL_CLIENT_SECRET",
        )
    )


def _personal_google_oauth_configured() -> bool:
    return bool(
        _oauth_value("GOOGLE_SHEETS_REFRESH_TOKEN", "GOOGLE_USER_REFRESH_TOKEN")
        and _oauth_value("GOOGLE_SHEETS_CLIENT_ID", "GOOGLE_USER_CLIENT_ID", "GOOGLE_GMAIL_CLIENT_ID")
        and _oauth_value("GOOGLE_SHEETS_CLIENT_SECRET", "GOOGLE_USER_CLIENT_SECRET", "GOOGLE_GMAIL_CLIENT_SECRET")
    )


def _user_google_oauth_configured() -> bool:
    return bool(
        _work_google_oauth_configured()
        or _personal_google_oauth_configured()
    )


def _user_google_creds(scopes=None, account: str = "personal"):
    account = (account or "personal").strip().lower()
    if account == "work":
        if not _work_google_oauth_configured():
            raise EnvironmentError("Work Google Sheets OAuth credentials are not configured")
        refresh_token = _oauth_value("GOOGLE_WORK_SHEETS_REFRESH_TOKEN")
        client_id = _oauth_value(
            "GOOGLE_WORK_SHEETS_CLIENT_ID",
            "GOOGLE_WORK_GMAIL_CLIENT_ID",
            "GOOGLE_SHEETS_CLIENT_ID",
            "GOOGLE_USER_CLIENT_ID",
            "GOOGLE_GMAIL_CLIENT_ID",
        )
        client_secret = _oauth_value(
            "GOOGLE_WORK_SHEETS_CLIENT_SECRET",
            "GOOGLE_WORK_GMAIL_CLIENT_SECRET",
            "GOOGLE_SHEETS_CLIENT_SECRET",
            "GOOGLE_USER_CLIENT_SECRET",
            "GOOGLE_GMAIL_CLIENT_SECRET",
        )
    else:
        if not _personal_google_oauth_configured():
            raise EnvironmentError("Personal Google Sheets OAuth credentials are not configured")
        refresh_token = _oauth_value("GOOGLE_SHEETS_REFRESH_TOKEN", "GOOGLE_USER_REFRESH_TOKEN")
        client_id = _oauth_value("GOOGLE_SHEETS_CLIENT_ID", "GOOGLE_USER_CLIENT_ID", "GOOGLE_GMAIL_CLIENT_ID")
        client_secret = _oauth_value("GOOGLE_SHEETS_CLIENT_SECRET", "GOOGLE_USER_CLIENT_SECRET", "GOOGLE_GMAIL_CLIENT_SECRET")
    if not (refresh_token and client_id and client_secret):
        raise EnvironmentError("Google user OAuth credentials are not configured")
    return Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes or SHEETS_SCOPES,
    )


def _sheets_auth_mode(account: str = "app") -> str:
    account = (account or "app").strip().lower()
    if account == "work":
        if _work_google_oauth_configured():
            return "work_user_oauth"
        if _personal_google_oauth_configured():
            return "user_oauth"
        return "service_account"
    if _personal_google_oauth_configured():
        return "user_oauth"
    return "service_account"


def _work_sheets_auth_mode() -> str:
    if _work_google_oauth_configured():
        return "work_user_oauth"
    return _sheets_auth_mode("app")


def sheets_access_identity(account: str = "app") -> dict:
    mode = _work_sheets_auth_mode() if (account or "").strip().lower() == "work" else _sheets_auth_mode("app")
    if mode == "service_account":
        identity = _service_account_email()
    elif mode == "work_user_oauth":
        identity = "work Google Sheets OAuth user"
    else:
        identity = "personal Google Sheets OAuth user"
    return {"mode": mode, "identity": identity}


def google_sheets_configured() -> bool:
    return bool(os.environ.get("GOOGLE_SHEET_ID", "") and (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "") or _user_google_oauth_configured()))


def _thread_cached_service(key: tuple, builder):
    cache = getattr(_thread_local, "google_services", None)
    if cache is None:
        cache = {}
        _thread_local.google_services = cache
    if key not in cache:
        cache[key] = builder()
    return cache[key]


def _build_service(name: str, version: str, credentials):
    return build(name, version, credentials=credentials, cache_discovery=False)


def _cal():
    return _thread_cached_service(("calendar",), lambda: _build_service("calendar", "v3", _creds()))


def _sheets(account: str = "app"):
    account = (account or "app").strip().lower()
    mode = _work_sheets_auth_mode() if account == "work" else _sheets_auth_mode("app")
    if mode == "work_user_oauth":
        creds_builder = lambda scopes: _user_google_creds(scopes, account="work")
    elif mode == "user_oauth":
        creds_builder = lambda scopes: _user_google_creds(scopes, account="personal")
    else:
        creds_builder = _creds
    return _thread_cached_service(("sheets", account, mode), lambda: _build_service("sheets", "v4", creds_builder(SHEETS_SCOPES)))


def _work_sheets():
    return _sheets("work")


def _drive():
    return _thread_cached_service(("drive",), lambda: _build_service("drive", "v3", _creds()))


def _gmail(account: str = "personal"):
    account = (account or "personal").strip().lower()

    def build_gmail():
        if account in ("work", "moe", "school"):
            refresh_token = os.environ.get("GOOGLE_WORK_GMAIL_REFRESH_TOKEN", "").strip()
            client_id = os.environ.get("GOOGLE_WORK_GMAIL_CLIENT_ID", "").strip()
            client_secret = os.environ.get("GOOGLE_WORK_GMAIL_CLIENT_SECRET", "").strip()
            if not (refresh_token and client_id and client_secret):
                # Reuse the same OAuth client app if only the work refresh token differs.
                client_id = client_id or os.environ.get("GOOGLE_GMAIL_CLIENT_ID", "").strip()
                client_secret = client_secret or os.environ.get("GOOGLE_GMAIL_CLIENT_SECRET", "").strip()
        else:
            suffix = "2" if account in ("personal2", "secondary", "second") else ""
            refresh_token = os.environ.get(f"GOOGLE_GMAIL{suffix}_REFRESH_TOKEN", "").strip()
            client_id = os.environ.get(f"GOOGLE_GMAIL{suffix}_CLIENT_ID", "").strip() or os.environ.get("GOOGLE_GMAIL_CLIENT_ID", "").strip()
            client_secret = os.environ.get(f"GOOGLE_GMAIL{suffix}_CLIENT_SECRET", "").strip() or os.environ.get("GOOGLE_GMAIL_CLIENT_SECRET", "").strip()

        if refresh_token and client_id and client_secret:
            creds = Credentials(
                None,
                refresh_token=refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=client_id,
                client_secret=client_secret,
                scopes=GMAIL_SCOPES,
            )
            return _build_service("gmail", "v1", creds)

        if account in ("work", "moe", "school"):
            user_key = "GOOGLE_WORK_GMAIL_USER"
        elif account in ("personal2", "secondary", "second"):
            user_key = "GOOGLE_GMAIL2_USER"
        else:
            user_key = "GOOGLE_GMAIL_USER"
        user = os.environ.get(user_key, "").strip()
        if not user:
            raise EnvironmentError(f"{account.title()} Gmail not configured.")
        return _build_service("gmail", "v1", _creds(GMAIL_SCOPES, subject=user))

    return _thread_cached_service(("gmail", account), build_gmail)


# ─── CALENDAR ────────────────────────────────────────────────────────────────

def _fetch_events(start: datetime, end: datetime):
    """Fetch from all configured calendars, merged and sorted by start time."""
    service = _cal()
    all_events = []
    for cal_id in CALENDAR_IDS:
        try:
            result = service.events().list(
                calendarId=cal_id,
                timeMin=start.isoformat(),
                timeMax=end.isoformat(),
                singleEvents=True,
                orderBy="startTime",
            ).execute()
            for event in result.get("items", []):
                event["_calendar_id"] = cal_id
                all_events.append(event)
        except Exception:
            pass  # skip calendars not yet shared
    all_events.sort(key=lambda e: e["start"].get("dateTime", e["start"].get("date", "")))
    return all_events


def get_today_events():
    now = datetime.now(SGT)
    return _fetch_events(
        now.replace(hour=0, minute=0, second=0, microsecond=0),
        now.replace(hour=23, minute=59, second=59, microsecond=0),
    )


def get_tomorrow_events():
    now = datetime.now(SGT) + timedelta(days=1)
    return _fetch_events(
        now.replace(hour=0, minute=0, second=0, microsecond=0),
        now.replace(hour=23, minute=59, second=59, microsecond=0),
    )


def get_week_events():
    now = datetime.now(SGT)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=7)).replace(hour=23, minute=59, second=59)
    return _fetch_events(start, end)


def get_events_for_days(days: int = 7):
    """Fetch events from today through the next N days."""
    now = datetime.now(SGT)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = (start + timedelta(days=max(1, days))).replace(hour=23, minute=59, second=59)
    return _fetch_events(start, end)


def format_events(events, show_date=False):
    if not events:
        return "Nothing scheduled."
    lines = []
    for e in events:
        raw_start = e["start"].get("dateTime", e["start"].get("date", ""))
        summary = e.get("summary", "(No title)")
        if "T" in raw_start:
            dt = datetime.fromisoformat(raw_start).astimezone(SGT)
            time_str = dt.strftime("%H:%M")
            date_str = dt.strftime("%a %-d %b")
        else:
            time_str = "All day"
            dt = datetime.fromisoformat(raw_start)
            date_str = dt.strftime("%a %-d %b")
        prefix = f"{date_str} " if show_date else ""
        lines.append(f"• {prefix}{time_str} — {summary}")

    return "\n".join(lines)


# ─── SHEETS: MTL CLASSLISTS ─────────────────────────────────────────────────

def _configured_classlist_sheet_ids() -> list[str]:
    ids = _split_ids(os.environ.get("GOOGLE_CLASSLIST_SHEET_IDS", ""))
    return ids or list(DEFAULT_CLASSLIST_SHEET_IDS)


def _is_google_rate_limit_error(exc: Exception) -> bool:
    if not isinstance(exc, HttpError):
        return False
    status = getattr(exc.resp, "status", None)
    if status in {429, 500, 502, 503, 504}:
        return True
    content = getattr(exc, "content", b"") or b""
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="ignore")
    return any(token in str(content).lower() for token in ("ratelimit", "rate limit", "quota", "user_rate_limit_exceeded"))


def _execute_with_rate_limit_retry(request, *, attempts: int = 3):
    last_error = None
    for attempt in range(max(1, attempts)):
        try:
            return request.execute()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts - 1 or not _is_google_rate_limit_error(exc):
                raise
            time.sleep(0.8 * (2 ** attempt))
    raise last_error


def _read_classlist_book(spreadsheet_id: str) -> dict:
    return _execute_with_rate_limit_retry(
        _work_sheets().spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            includeGridData=True,
            fields=_CLASSLIST_GRID_FIELDS,
        )
    )


def _classlist_book(spreadsheet_id: str, force: bool = False) -> dict:
    if _CLASSLIST_CACHE_TTL_SECONDS <= 0:
        return _read_classlist_book(spreadsheet_id)
    with _classlist_cache_lock:
        now = time.monotonic()
        cached = _classlist_cache.get(spreadsheet_id)
        if not force and cached and now < float(cached.get("expires_at", 0.0) or 0.0):
            return cached["book"]
        try:
            book = _read_classlist_book(spreadsheet_id)
        except Exception:
            if cached:
                logger.warning("Using stale classlist cache for %s after Sheets read failed.", spreadsheet_id)
                return cached["book"]
            raise
        _classlist_cache[spreadsheet_id] = {
            "book": book,
            "expires_at": time.monotonic() + _CLASSLIST_CACHE_TTL_SECONDS,
        }
        return book


def _invalidate_classlist_cache(spreadsheet_id: str = ""):
    if _CLASSLIST_CACHE_TTL_SECONDS <= 0:
        return
    with _classlist_cache_lock:
        if spreadsheet_id:
            _classlist_cache.pop(spreadsheet_id, None)
        else:
            _classlist_cache.clear()


def _norm_cell(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def _class_query_variants(value: str) -> set[str]:
    norm = _norm_cell(value)
    variants = {norm} if norm else set()
    compact = norm.replace(" ", "")
    match = re.match(r"SEC(?:ONDARY)?([1-4])G([1-9])", compact)
    if match:
        level, group = match.groups()
        variants.update({
            f"{level}G{group}",
            f"{level} G{group}",
            f"G{group}",
            f"ML G{group}",
            f"{level}G{group} ML",
            f"{level} G{group} ML",
        })
    match = re.match(r"([1-4])G([1-9])", compact)
    if match:
        level, group = match.groups()
        variants.update({
            f"{level}G{group}",
            f"{level} G{group}",
            f"G{group}",
            f"ML G{group}",
            f"{level}G{group} ML",
            f"{level} G{group} ML",
        })
    return {_norm_cell(item) for item in variants if _norm_cell(item)}


def _class_query_matches(class_query: str, *values: str) -> bool:
    variants = _class_query_variants(class_query)
    if not variants:
        return True
    haystack = _norm_cell(" ".join(str(value or "") for value in values))
    return any(variant in haystack for variant in variants)


def _cell_value(cell: dict) -> str:
    if not isinstance(cell, dict):
        return ""
    value = cell.get("formattedValue")
    if value is not None:
        return str(value).strip()
    effective = cell.get("effectiveValue") or {}
    for key in ("stringValue", "numberValue", "boolValue"):
        if key in effective:
            return str(effective[key]).strip()
    return ""


def _row_values(row: dict) -> list[str]:
    return [_cell_value(cell) for cell in row.get("values", [])]


def _column_letter(index: int) -> str:
    value = index + 1
    letters = []
    while value:
        value, remainder = divmod(value - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _quote_sheet_name(title: str) -> str:
    return "'" + str(title or "").replace("'", "''") + "'"


def _sheet_title_for_gid(spreadsheet_id: str, gid: str = "") -> tuple[str, str]:
    book = _sheets().spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=False,
        fields="properties(title),sheets(properties(sheetId,title))",
    ).execute()
    spreadsheet_title = book.get("properties", {}).get("title", spreadsheet_id)
    clean_gid = str(gid or "").strip()
    sheets = book.get("sheets", []) or []
    if clean_gid:
        for sheet in sheets:
            props = sheet.get("properties", {})
            if str(props.get("sheetId", "")) == clean_gid:
                return str(props.get("title", "") or ""), spreadsheet_title
    first_title = sheets[0].get("properties", {}).get("title", "") if sheets else ""
    return str(first_title or ""), spreadsheet_title


def _date_tokens_for_sheet(target: date) -> set[str]:
    month_full = target.strftime("%B").lower()
    month_short = target.strftime("%b").lower()
    return {
        target.isoformat(),
        target.strftime("%d/%m/%Y").lstrip("0").replace("/0", "/"),
        target.strftime("%d/%m").lstrip("0").replace("/0", "/"),
        target.strftime("%-d %B").lower() if os.name != "nt" else f"{target.day} {month_full}",
        target.strftime("%-d %b").lower() if os.name != "nt" else f"{target.day} {month_short}",
        target.strftime("%A").lower(),
        target.strftime("%a").lower(),
    }


def _date_selection_tokens_for_sheet(target: date, week_label: str = "") -> set[str]:
    month_full = target.strftime("%B").lower()
    month_short = target.strftime("%b").lower()
    tokens = set(_date_tokens_for_sheet(target))
    tokens.update({month_full, month_short})
    label = _norm_cell(week_label)
    if label:
        tokens.add(label)
        match = re.search(r"\bterm\s*(\d+)\s+week\s*(\d+)\b", label)
        if match:
            term, week = match.groups()
            tokens.update({
                f"term {term} week {week}",
                f"t{term} week {week}",
                f"t{term}w{week}",
                f"week {week}",
            })
    return {token for token in tokens if token}


def _week_label_tokens(week_label: str = "") -> set[str]:
    label = _norm_cell(week_label)
    if not label:
        return set()
    tokens = {label}
    match = re.search(r"\bterm\s*(\d+)\s+week\s*(\d+)\b", label)
    if match:
        term, week = match.groups()
        tokens.update({
            f"term {term} week {week}",
            f"t{term} week {week}",
            f"t{term}w{week}",
            f"week {week}",
        })
    return tokens


def _row_text(row: list[str]) -> str:
    return " ".join(str(cell or "").strip() for cell in row if str(cell or "").strip()).lower()


def _row_has_any(row: list[str], tokens: set[str]) -> bool:
    text = _row_text(row)
    return any(token and token in text for token in tokens)


def _sheet_values(spreadsheet_id: str, sheet_title: str, cell_range: str = "A1:Z220", resource_key: str = "") -> list[list[str]]:
    quoted = _quote_sheet_name(sheet_title)
    request = _sheets().spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{quoted}!{cell_range}",
    )
    if resource_key and hasattr(request, "headers"):
        request.headers.update(_resource_key_headers(spreadsheet_id, resource_key))
    result = request.execute()
    return [[str(cell).strip() for cell in row] for row in result.get("values", [])]


def _public_sheet_csv_values(spreadsheet_id: str, gid: str = "", resource_key: str = "", timeout: int = 12) -> list[list[str]]:
    if not spreadsheet_id:
        return []
    params = {"format": "csv"}
    if str(gid or "").strip():
        params["gid"] = str(gid).strip()
    if str(resource_key or "").strip():
        params["resourcekey"] = str(resource_key).strip()
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
    response = requests.get(url, params=params, headers=_resource_key_headers(spreadsheet_id, resource_key), timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"public CSV export returned HTTP {response.status_code}")
    text = response.text or ""
    if "ServiceLogin" in response.url or "Sign in" in text[:500]:
        raise RuntimeError("public CSV export requires sign-in")
    rows = []
    for row in csv.reader(io.StringIO(text)):
        cleaned = [str(cell).strip() for cell in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


def _parse_google_sheet_source(value: str) -> dict:
    text = str(value or "").strip()
    if not text:
        return {}
    result: dict[str, str] = {}
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", text)
    if match:
        result["spreadsheet_id"] = match.group(1)
    elif re.fullmatch(r"[a-zA-Z0-9_-]{20,}", text):
        result["spreadsheet_id"] = text
    gid_match = re.search(r"[?&#]gid=([0-9]+)", text)
    if gid_match:
        result["gid"] = gid_match.group(1)
    resource_match = re.search(r"[?&#]resourcekey=([^&#]+)", text)
    if resource_match:
        result["resource_key"] = resource_match.group(1)
    return result


def _resource_key_headers(spreadsheet_id: str, resource_key: str = "") -> dict:
    clean_key = str(resource_key or "").strip()
    clean_id = str(spreadsheet_id or "").strip()
    if not (clean_id and clean_key):
        return {}
    return {"X-Goog-Drive-Resource-Keys": f"{clean_id}/{clean_key}"}


def _cca_schedule_public_snapshot(
    source: dict,
    target: date,
    week_label: str = "",
    user_name: str = "Herwanto",
    error: str = "",
) -> dict:
    rows = _public_sheet_csv_values(source["spreadsheet_id"], source.get("gid", ""), source.get("resource_key", ""))
    tokens = _date_tokens_for_sheet(target)
    name_tokens = {str(user_name or "Herwanto").lower(), "herwanto"}
    date_rows = [row for row in rows if _row_has_any(row, tokens)]
    candidate_rows = date_rows or rows
    assigned_rows = [
        row for row in candidate_rows
        if any(token and token in _row_text(row) for token in name_tokens)
    ]
    return {
        **source,
        "spreadsheet_title": source["spreadsheet_id"],
        "date": target.isoformat(),
        "week_label": week_label,
        "selected_tab": f"public CSV gid {source.get('gid', '')}",
        "selected_gid": source.get("gid", ""),
        "selection_score": 0,
        "selection_reasons": ["public CSV fallback after Sheets API access failed"],
        "day_specific_rows_found": bool(date_rows),
        "assigned": bool(assigned_rows),
        "assigned_rows": assigned_rows[:12],
        "row_count": len(rows),
        "access_warning": error,
    }


def _cca_source_config() -> dict:
    source_url = get_config("cca_schedule_url") or CCA_SCHEDULE_URL
    parsed = _parse_google_sheet_source(source_url)
    spreadsheet_id = parsed.get("spreadsheet_id") or get_config("cca_schedule_spreadsheet_id") or CCA_SCHEDULE_SPREADSHEET_ID
    gid = parsed.get("gid") or get_config("cca_schedule_gid") or CCA_SCHEDULE_GID
    resource_key = parsed.get("resource_key") or get_config("cca_schedule_resource_key") or CCA_SCHEDULE_RESOURCE_KEY
    resource_suffix = f"&resourcekey={resource_key}" if resource_key else ""
    return {
        "spreadsheet_id": spreadsheet_id,
        "gid": str(gid or ""),
        "resource_key": str(resource_key or ""),
        "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={gid}{resource_suffix}#gid={gid}",
    }


def set_cca_schedule_source(
    spreadsheet_id: str = CCA_SCHEDULE_SPREADSHEET_ID,
    gid: str = CCA_SCHEDULE_GID,
    resource_key: str = "",
) -> dict:
    parsed = _parse_google_sheet_source(spreadsheet_id)
    clean_id = (parsed.get("spreadsheet_id") or str(spreadsheet_id or "")).strip()
    clean_gid = (parsed.get("gid") or str(gid or "")).strip()
    clean_resource_key = (parsed.get("resource_key") or str(resource_key or "")).strip()
    if not clean_id:
        raise ValueError("CCA schedule spreadsheet id is required.")
    set_config("cca_schedule_spreadsheet_id", clean_id)
    if clean_gid:
        set_config("cca_schedule_gid", clean_gid)
    if clean_resource_key:
        set_config("cca_schedule_resource_key", clean_resource_key)
    return _cca_source_config()


def get_cca_schedule_snapshot(target_date: date | str | None = None, week_label: str = "", user_name: str = "Herwanto") -> dict:
    if isinstance(target_date, date):
        target = target_date
    elif target_date:
        target = date.fromisoformat(str(target_date)[:10])
    else:
        target = datetime.now(SGT).date()
    source = _cca_source_config()
    spreadsheet_id = source["spreadsheet_id"]
    gid = source["gid"]
    auth = sheets_access_identity()
    try:
        request = _sheets().spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            includeGridData=False,
            fields="properties(title),sheets(properties(sheetId,title))",
        )
        if source.get("resource_key") and hasattr(request, "headers"):
            request.headers.update(_resource_key_headers(spreadsheet_id, source.get("resource_key", "")))
        book = request.execute()
    except HttpError as exc:
        status = getattr(exc.resp, "status", None)
        error = f"Sheets API access failed with HTTP {status or 'unknown'}"
        try:
            return _cca_schedule_public_snapshot(source, target, week_label, user_name, error=error)
        except Exception as fallback_exc:
            return {
                **source,
                "spreadsheet_title": spreadsheet_id,
                "date": target.isoformat(),
                "week_label": week_label,
                "assigned": False,
                "access_blocked": True,
                "auth_mode": auth.get("mode", ""),
                "auth_identity": auth.get("identity", ""),
                "error": (
                    f"{error} for {auth.get('mode', 'unknown auth')} ({auth.get('identity', 'unknown identity')}); "
                    f"public CSV fallback also failed: {fallback_exc}. "
                    f"Resource key present: {'yes' if source.get('resource_key') else 'no'}. "
                    "Official CCA duty status is unverified. Do not ask Herwanto to manually check the sheet; report the access block and use only calendar evidence as non-roster context."
                ),
            }
    spreadsheet_title = book.get("properties", {}).get("title", spreadsheet_id)
    tokens = _date_tokens_for_sheet(target)
    selection_tokens = _date_selection_tokens_for_sheet(target, week_label)
    week_tokens = _week_label_tokens(week_label)
    candidates = []
    for sheet in book.get("sheets", []) or []:
        props = sheet.get("properties", {})
        title = str(props.get("title", "") or "")
        sheet_id = str(props.get("sheetId", "") or "")
        title_text = title.lower()
        rows = []
        try:
            rows = _sheet_values(spreadsheet_id, title, resource_key=source.get("resource_key", ""))
        except Exception:
            rows = []
        top_text = " ".join(_row_text(row) for row in rows[:30])
        score = 0
        reasons = []
        if sheet_id == gid:
            score += 1
            reasons.append("configured gid")
        if any(token and token in title_text for token in selection_tokens):
            score += 12
            reasons.append("date/week/month in tab title")
        if any(token and token in top_text for token in tokens):
            score += 8
            reasons.append("date/day in tab rows")
        if any(token and token in title_text for token in week_tokens):
            score += 8
            reasons.append("week label in tab title")
        if any(token and token in top_text for token in week_tokens):
            score += 5
            reasons.append("week label in tab rows")
        candidates.append({
            "sheet_id": sheet_id,
            "sheet_title": title,
            "rows": rows,
            "score": score,
            "reasons": reasons,
        })

    if not candidates:
        return {**source, "spreadsheet_title": spreadsheet_title, "date": target.isoformat(), "assigned": False, "error": "No tabs found."}
    selected = max(candidates, key=lambda item: item["score"])
    if selected["score"] <= 0:
        selected = next((item for item in candidates if item["sheet_id"] == gid), selected)

    name_tokens = {str(user_name or "Herwanto").lower(), "herwanto"}
    date_rows = [row for row in selected["rows"] if _row_has_any(row, tokens)]
    candidate_rows = date_rows or selected["rows"]
    assigned_rows = [
        row for row in candidate_rows
        if any(token and token in _row_text(row) for token in name_tokens)
    ]
    return {
        **source,
        "spreadsheet_title": spreadsheet_title,
        "date": target.isoformat(),
        "week_label": week_label,
        "auth_mode": auth.get("mode", ""),
        "auth_identity": auth.get("identity", ""),
        "selected_tab": selected["sheet_title"],
        "selected_gid": selected["sheet_id"],
        "selection_score": selected["score"],
        "selection_reasons": selected["reasons"],
        "day_specific_rows_found": bool(date_rows),
        "assigned": bool(assigned_rows),
        "assigned_rows": assigned_rows[:12],
        "row_count": len(selected["rows"]),
    }


def format_cca_schedule_snapshot(snapshot: dict) -> str:
    if snapshot.get("error"):
        return f"CCA schedule unavailable: {snapshot['error']}"
    lines = [
        f"CCA schedule source: {snapshot.get('url', '')}",
        f"Date: {snapshot.get('date', '')}",
        f"Sheets auth: {snapshot.get('auth_mode', '')} {snapshot.get('auth_identity', '')}".strip(),
        f"Selected tab: {snapshot.get('selected_tab', '')} ({', '.join(snapshot.get('selection_reasons') or ['fallback'])})",
        f"Herwanto on schedule: {'YES' if snapshot.get('assigned') else 'NO'}",
    ]
    if snapshot.get("access_warning"):
        lines.append(f"Access warning: {snapshot.get('access_warning')}")
    if not snapshot.get("assigned"):
        lines.append("Hard stop: if Herwanto is not on this day's schedule, do not prompt him and do not add a CCA duty/event to his calendar.")
        return "\n".join(lines)
    lines.append("Matching row(s):")
    for row in snapshot.get("assigned_rows", [])[:8]:
        lines.append("- " + " | ".join(str(cell) for cell in row if str(cell).strip()))
    return "\n".join(lines)


def _first_after_label(rows: list[list[str]], label: str, max_rows: int = 12) -> str:
    needle = _norm_cell(label)
    for row in rows[:max_rows]:
        for idx, cell in enumerate(row):
            if needle in _norm_cell(cell):
                for candidate in row[idx + 1:]:
                    if str(candidate).strip():
                        return str(candidate).strip()
    return ""


def _teacher_matches(rows: list[list[str]], sheet_title: str, teacher_query: str) -> bool:
    needles = [_norm_cell(part) for part in re.split(r"[,/|]+", teacher_query or "HERWANTO") if part.strip()]
    if not needles:
        needles = ["HERWANTO"]
    haystacks = [_norm_cell(sheet_title)]
    haystacks.extend(_norm_cell(" ".join(row)) for row in rows[:14])
    return any(any(needle in hay for hay in haystacks) for needle in needles)


def _find_classlist_header(rows: list[list[str]]) -> tuple[int, int, int, int | None] | None:
    for row_idx, row in enumerate(rows[:30]):
        norms = [_norm_cell(cell) for cell in row]
        name_col = next(
            (
                idx for idx, cell in enumerate(norms)
                if cell in {"FULL NAME", "NAMA", "NAME"} or cell.endswith(" FULL NAME")
            ),
            None,
        )
        class_col = next((idx for idx, cell in enumerate(norms) if cell == "CLASS"), None)
        no_col = next((idx for idx, cell in enumerate(norms) if cell in {"NO", "NO."}), None)
        if name_col is not None and (class_col is not None or no_col is not None):
            return row_idx, name_col, class_col if class_col is not None else -1, no_col
    return None


def _extract_students(rows: list[list[str]]) -> list[dict]:
    header = _find_classlist_header(rows)
    if not header:
        return []
    header_idx, name_col, class_col, no_col = header
    students = []
    blank_run = 0
    for row in rows[header_idx + 1:]:
        name = row[name_col].strip() if name_col < len(row) else ""
        class_name = row[class_col].strip() if class_col >= 0 and class_col < len(row) else ""
        number = row[no_col].strip() if no_col is not None and no_col < len(row) else ""
        if not name and not class_name:
            blank_run += 1
            if blank_run >= 6 and students:
                break
            continue
        blank_run = 0
        if not name or _norm_cell(name) in {"FULL NAME", "NAME", "NAMA"}:
            continue
        if _norm_cell(name).startswith("TEACHER NAME"):
            continue
        students.append({
            "no": number,
            "class": class_name,
            "name": name.replace("*", "").strip(),
        })
    return students


def _extract_students_with_fields(rows: list[list[str]]) -> tuple[list[dict], list[str]]:
    header = _find_classlist_header(rows)
    if not header:
        return [], []
    header_idx, name_col, class_col, no_col = header
    headers = [str(cell or "").strip() for cell in rows[header_idx]]
    protected = {name_col, class_col, no_col}
    students = []
    blank_run = 0
    for row_offset, row in enumerate(rows[header_idx + 1:], start=header_idx + 1):
        name = row[name_col].strip() if name_col < len(row) else ""
        class_name = row[class_col].strip() if class_col >= 0 and class_col < len(row) else ""
        number = row[no_col].strip() if no_col is not None and no_col < len(row) else ""
        if not name and not class_name:
            blank_run += 1
            if blank_run >= 6 and students:
                break
            continue
        blank_run = 0
        if not name or _norm_cell(name) in {"FULL NAME", "NAME", "NAMA"}:
            continue
        if _norm_cell(name).startswith("TEACHER NAME"):
            continue
        fields = {}
        for idx, header_label in enumerate(headers):
            if idx in protected or not header_label.strip():
                continue
            value = row[idx].strip() if idx < len(row) else ""
            if value:
                fields[header_label.strip()] = value
        students.append({
            "row_index": row_offset,
            "no": number,
            "class": class_name,
            "name": name.replace("*", "").strip(),
            "fields": fields,
        })
    return students, headers


def get_mtl_classlists(
    teacher_query: str = "HERWANTO",
    class_query: str = "",
    include_students: bool = True,
    include_scores: bool = False,
) -> list[dict]:
    class_filter = _norm_cell(class_query)
    lists = []
    for spreadsheet_id in _configured_classlist_sheet_ids():
        book = _classlist_book(spreadsheet_id)
        title = book.get("properties", {}).get("title", spreadsheet_id)
        for sheet in book.get("sheets", []):
            props = sheet.get("properties", {})
            sheet_title = props.get("title", "")
            sheet_id = props.get("sheetId")
            row_data = []
            for data in sheet.get("data", []):
                row_data.extend(data.get("rowData", []))
            rows = [_row_values(row) for row in row_data]
            if not _teacher_matches(rows, sheet_title, teacher_query):
                continue
            grouping = _first_after_label(rows, "GROUPING")
            venue = _first_after_label(rows, "VENUE")
            teacher = _first_after_label(rows, "TEACHER NAME") or teacher_query
            if include_scores:
                students, headers = _extract_students_with_fields(rows)
            else:
                students = _extract_students(rows)
                headers = []
            if class_filter:
                students = [
                    student for student in students
                    if _class_query_matches(class_query, student.get("class", ""), student.get("name", ""), grouping)
                ]
                if not students and not _class_query_matches(class_query, grouping, sheet_title):
                    continue
            lists.append({
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_title": title,
                "sheet_title": sheet_title,
                "teacher": teacher,
                "grouping": grouping,
                "venue": venue,
                "student_count": len(students),
                "columns": headers if include_scores else [],
                "students": students if include_students else [],
            })
    return lists


def format_mtl_classlists(
    teacher_query: str = "HERWANTO",
    class_query: str = "",
    include_students: bool = True,
    include_scores: bool = False,
) -> str:
    lists = get_mtl_classlists(teacher_query, class_query, include_students, include_scores)
    if not lists:
        return "No matching MTL classlist tabs found. Check that the classlist sheets are shared with H.I.R.A's Google service account."
    lines = ["MTL classlists from 2026 MTL CLASSLIST BY TEACHERS:"]
    for item in lists:
        label = item["grouping"] or item["sheet_title"] or item["spreadsheet_title"]
        teacher = item["teacher"]
        venue = f", venue {item['venue']}" if item.get("venue") else ""
        lines.append(f"\n{label} - {item['spreadsheet_title']} ({teacher}{venue})")
        if not include_students:
            lines.append(f"- {item['student_count']} students")
            continue
        for student in item["students"]:
            prefix = f"{student['no']}. " if student.get("no") else "- "
            cls = f" [{student['class']}]" if student.get("class") else ""
            fields = student.get("fields") or {}
            score_text = ""
            if include_scores and fields:
                score_text = " | " + "; ".join(f"{key}: {value}" for key, value in fields.items())
            lines.append(f"{prefix}{student['name']}{cls}{score_text}")
    return "\n".join(lines)


def _matching_header_columns(headers: list[str], query: str, protected_cols: set[int]) -> list[int]:
    needle = _norm_cell(query)
    if not needle:
        return []
    exact = [
        idx for idx, header in enumerate(headers)
        if idx not in protected_cols and _norm_cell(header) == needle
    ]
    if exact:
        return exact
    return [
        idx for idx, header in enumerate(headers)
        if idx not in protected_cols
        and header.strip()
        and (needle in _norm_cell(header) or _norm_cell(header) in needle)
    ]


def _number_from_header(value: str) -> float | None:
    text = str(value or "")
    match = re.search(r"\(\s*(\d+(?:\.\d+)?)\s*\)", text)
    if not match:
        match = re.search(r"/\s*(\d+(?:\.\d+)?)\b", text)
    if not match:
        numbers = re.findall(r"\d+(?:\.\d+)?", text)
        if not numbers:
            return None
        candidate = numbers[-1]
        try:
            # Avoid treating assessment labels such as WA2 or Pra-WA A2 as max marks.
            if float(candidate) <= 4:
                return None
        except ValueError:
            return None
        match = re.search(rf"{re.escape(candidate)}(?!.*\d)", text)
    if not match:
        return None
    try:
        return float(match.group(1) if match.lastindex else match.group(0))
    except ValueError:
        return None


def _number_from_cell(value: str) -> float | None:
    text = str(value or "").strip()
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _format_percentage(value: float) -> str:
    rounded = round(value)
    return str(int(rounded))


def _percentage_header_for_source(header: str, assessment: str = "") -> str:
    source = str(header or "").strip()
    label = re.sub(r"\s*\(\s*\d+(?:\.\d+)?\s*\)\s*$", "", source).strip()
    if not label:
        label = str(assessment or "").strip()
    return f"{label or 'Percentage'} (100%)"


def _is_percentage_header(value: str) -> bool:
    text = str(value or "")
    norm = _norm_cell(text)
    return "%" in text or norm in {"PERCENT", "PERCENTAGE"}


def _assessment_label_for_column(rows: list[list[str]], header_idx: int, col_idx: int) -> str:
    if header_idx <= 0:
        return ""
    upper = rows[header_idx - 1] if header_idx - 1 < len(rows) else []
    for idx in range(min(col_idx, len(upper) - 1), -1, -1):
        label = str(upper[idx] or "").strip()
        if label:
            return label
    return ""


def _student_identity_text(student: dict) -> str:
    return " ".join(
        str(part or "")
        for part in (student.get("no"), student.get("class"), student.get("name"))
    )


def _score_sheet_matches(teacher_query: str, class_query: str) -> list[dict]:
    class_filter = _norm_cell(class_query)
    matches = []
    for spreadsheet_id in _configured_classlist_sheet_ids():
        book = _classlist_book(spreadsheet_id)
        spreadsheet_title = book.get("properties", {}).get("title", spreadsheet_id)
        for sheet in book.get("sheets", []):
            props = sheet.get("properties", {})
            sheet_title = props.get("title", "")
            sheet_id = props.get("sheetId")
            row_data = []
            for data in sheet.get("data", []):
                row_data.extend(data.get("rowData", []))
            rows = [_row_values(row) for row in row_data]
            if not _teacher_matches(rows, sheet_title, teacher_query):
                continue
            header = _find_classlist_header(rows)
            if not header:
                continue
            header_idx, name_col, class_col, no_col = header
            grouping = _first_after_label(rows, "GROUPING")
            if class_filter and not _class_query_matches(class_query, grouping, sheet_title):
                row_has_class = any(
                    class_col >= 0
                    and class_col < len(row)
                    and _class_query_matches(class_query, row[class_col])
                    for row in rows[header_idx + 1:]
                )
                if not row_has_class:
                    continue
            headers = [str(cell or "").strip() for cell in rows[header_idx]]
            students, _ = _extract_students_with_fields(rows)
            if class_filter:
                students = [
                    student for student in students
                    if _class_query_matches(class_query, grouping, student.get("class", ""), student.get("name", ""))
                ]
            matches.append({
                "spreadsheet_id": spreadsheet_id,
                "spreadsheet_title": spreadsheet_title,
                "sheet_title": sheet_title,
                "sheet_id": sheet_id,
                "grouping": grouping,
                "headers": headers,
                "header_idx": header_idx,
                "name_col": name_col,
                "class_col": class_col,
                "no_col": no_col,
                "students": students,
                "rows": rows,
            })
    return matches


def _score_column_label(rows: list[list[str]], header_idx: int, headers: list[str], col_idx: int) -> str:
    header = headers[col_idx] if col_idx < len(headers) else ""
    assessment = _assessment_label_for_column(rows, header_idx, col_idx)
    if assessment and header:
        return f"{assessment} {header}".strip()
    if _is_pre_wa_label(header):
        same_header_count = sum(1 for item in headers[:col_idx + 1] if _norm_cell(item) == _norm_cell(header))
        if same_header_count > 1:
            return f"{header} {same_header_count}".strip()
    if header and sum(1 for item in headers if _norm_cell(item) == _norm_cell(header)) > 1:
        same_header_count = sum(1 for item in headers[:col_idx + 1] if _norm_cell(item) == _norm_cell(header))
        return f"{header} {same_header_count}".strip()
    return header or assessment or _column_letter(col_idx)


def _is_pre_wa_label(value: str) -> bool:
    return bool(re.search(r"\b(?:PRA|PRE|PRG)[-\s]*W\s*A\b|\b(?:PRA|PRE|PRG)[-\s]*WA\d+\b", str(value or ""), re.I))


def _score_query_parts(column_query: str) -> dict:
    raw = str(column_query or "")
    norm = _norm_cell(raw)
    wants_percent = bool(re.search(r"%|\bpercent(?:age)?\b", raw, re.I))
    wants_pre_wa = _is_pre_wa_label(raw) or bool(re.search(r"\bmock\s*(?:test|wa)?\b|\bpre\s*wa\b", raw, re.I))
    assessment_terms = re.findall(r"\b(?:FA|WA)\s*\d+\b|\bPRELIM\b|\bEOY\b", raw, re.I)
    assessment_norms = [_norm_cell(term) for term in assessment_terms]
    remainder = norm
    for term in assessment_norms:
        remainder = re.sub(rf"\b{re.escape(term)}\b", " ", remainder).strip()
    remainder = re.sub(r"\bPERCENT(?:AGE)?\b", " ", remainder).strip()
    if wants_pre_wa:
        remainder = re.sub(r"\b(?:PRA|PRE|PRG)\s*W\s*A\d?\b|\bMOCK\s*(?:TEST|WA)?\b", " ", remainder).strip()
    return {
        "norm": norm,
        "wants_percent": wants_percent,
        "wants_pre_wa": wants_pre_wa,
        "assessment_norms": assessment_norms,
        "remainder": " ".join(remainder.split()),
    }


def _classlist_permission_message(exc: HttpError, spreadsheet_ids: list[str], targets: list[str]) -> str:
    email = _service_account_email()
    target_text = ", ".join(targets) if targets else "the matching MTL classlist"
    links = []
    for spreadsheet_id in dict.fromkeys(spreadsheet_ids):
        if spreadsheet_id:
            links.append(f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")
    link_text = f" Target sheet: {'; '.join(links)}." if links else ""
    auth_mode = _work_sheets_auth_mode()
    if auth_mode == "work_user_oauth":
        account_text = (
            " H.I.R.A is using the work Google account OAuth path for Sheets. Confirm that this work account "
            "can edit the sheet and that GOOGLE_WORK_SHEETS_REFRESH_TOKEN was granted the Google Sheets scope."
        )
    elif auth_mode == "user_oauth":
        account_text = (
            " H.I.R.A is using Google user OAuth for Sheets. Confirm that OAuth account can edit the sheet "
            "and that the refresh token was granted the Google Sheets scope."
        )
    else:
        service_account_text = (
            f" Fallback option: share it with {email} as Editor."
            if email
            else " Fallback option: share it with H.I.R.A's Google service account as Editor."
        )
        account_text = (
            " H.I.R.A is currently falling back to the service account because work Google Sheets OAuth is not configured. "
            "To edit as Herwanto's work Google account, configure GOOGLE_WORK_SHEETS_REFRESH_TOKEN with Google Sheets scope "
            "and make sure that work account can edit the sheet."
            f"{service_account_text}"
        )
    protected_hint = " If Editor access is already enabled, check whether the tab or WA/% columns are protected for only selected users."
    return (
        f"Google Sheets denied write access while filling {target_text}."
        f"{account_text}{protected_hint}{link_text}"
    )


def _score_columns_for_sheet(sheet: dict, column_query: str = "", prefer_percent: bool = False) -> list[dict]:
    query = _score_query_parts(column_query)
    protected = {sheet["name_col"], sheet["class_col"], sheet["no_col"]}
    columns = []
    for idx, header in enumerate(sheet["headers"]):
        if idx in protected or not str(header or "").strip():
            continue
        label = _score_column_label(sheet["rows"], sheet["header_idx"], sheet["headers"], idx)
        assessment = _assessment_label_for_column(sheet["rows"], sheet["header_idx"], idx)
        header_norm = _norm_cell(header)
        label_norm = _norm_cell(label)
        assessment_norm = _norm_cell(assessment)
        is_percent = _is_percentage_header(header)
        is_pre_wa = _is_pre_wa_label(label)
        if query["wants_percent"] and not is_percent:
            continue
        if not query["wants_pre_wa"] and is_pre_wa and query["assessment_norms"]:
            continue
        if query["wants_pre_wa"] and not is_pre_wa:
            continue
        if query["assessment_norms"] and not all(term in assessment_norm or term in label_norm for term in query["assessment_norms"]):
            continue
        if query["remainder"] and query["remainder"] not in label_norm and query["remainder"] not in header_norm:
            continue
        sample_values = []
        numeric_count = 0
        status_count = 0
        for student in sheet["students"]:
            row = sheet["rows"][student["row_index"]] if student["row_index"] < len(sheet["rows"]) else []
            value = row[idx].strip() if idx < len(row) else ""
            if not value:
                continue
            sample_values.append(value)
            if _number_from_cell(value) is not None:
                numeric_count += 1
            elif _norm_cell(value) in SCORE_STATUS_LABELS:
                status_count += 1
        derived_percent_from_index = None
        max_score = _number_from_header(header)
        if is_percent and not numeric_count and idx > 0:
            source_header = sheet["headers"][idx - 1] if idx - 1 < len(sheet["headers"]) else ""
            source_max_score = _number_from_header(source_header)
            source_numeric_count = 0
            source_status_count = 0
            if source_max_score:
                for student in sheet["students"]:
                    row = sheet["rows"][student["row_index"]] if student["row_index"] < len(sheet["rows"]) else []
                    source_value = row[idx - 1].strip() if idx - 1 < len(row) else ""
                    if _number_from_cell(source_value) is not None:
                        source_numeric_count += 1
                    elif _norm_cell(source_value) in SCORE_STATUS_LABELS:
                        source_status_count += 1
            if source_numeric_count or source_status_count:
                derived_percent_from_index = idx - 1
                max_score = source_max_score
                numeric_count = source_numeric_count
                status_count = source_status_count
        if not numeric_count and not status_count:
            continue
        columns.append({
            "index": idx,
            "header": header,
            "label": label,
            "assessment": assessment,
            "is_percent": is_percent,
            "is_pre_wa": is_pre_wa,
            "max_score": max_score,
            "derived_percent_from_index": derived_percent_from_index,
            "numeric_count": numeric_count,
            "status_count": status_count,
        })
    if prefer_percent and any(column["is_percent"] for column in columns):
        return [column for column in columns if column["is_percent"]]
    return columns


def _contiguous_row_ranges(row_indexes: list[int]) -> list[tuple[int, int]]:
    rows = sorted({int(row) for row in row_indexes})
    if not rows:
        return []
    ranges = []
    start = prev = rows[0]
    for row in rows[1:]:
        if row == prev + 1:
            prev = row
            continue
        ranges.append((start, prev + 1))
        start = prev = row
    ranges.append((start, prev + 1))
    return ranges


def apply_mtl_failure_highlighting(
    class_query: str = "",
    assessment_query: str = "",
    failure_threshold: float = 50,
    teacher_query: str = "HERWANTO",
) -> dict:
    sheets = _score_sheet_matches(teacher_query, class_query)
    if not sheets:
        raise ValueError("No matching MTL score sheets found.")
    if len(sheets) > 1 and not _norm_cell(class_query):
        options = [f"{item['grouping'] or item['sheet_title']} / {item['spreadsheet_title']}" for item in sheets[:8]]
        raise ValueError("More than one classlist sheet matched. Specify the class/group: " + "; ".join(options))

    try:
        threshold = float(failure_threshold)
    except (TypeError, ValueError):
        raise ValueError("Failure threshold must be a number.") from None

    targets = []
    requests_by_sheet: dict[str, list[dict]] = {}
    target_descriptions = []
    for sheet in sheets:
        sheet_id = sheet.get("sheet_id")
        if sheet_id is None:
            raise ValueError(f"Cannot format {sheet['sheet_title']} because the sheet id is missing.")
        columns = [
            column for column in _score_columns_for_sheet(sheet, assessment_query, prefer_percent=True)
            if column.get("is_percent")
        ]
        if not columns:
            continue
        if len(columns) > 1 and not assessment_query:
            options = [f"{sheet['grouping'] or sheet['sheet_title']} {column['label']}" for column in columns[:8]]
            raise ValueError("More than one percentage column matched. Specify the assessment: " + "; ".join(options))

        row_ranges = _contiguous_row_ranges([student["row_index"] for student in sheet["students"]])
        if not row_ranges:
            continue
        for column in columns:
            ranges = [
                {
                    "sheetId": int(sheet_id),
                    "startRowIndex": start,
                    "endRowIndex": end,
                    "startColumnIndex": int(column["index"]),
                    "endColumnIndex": int(column["index"]) + 1,
                }
                for start, end in row_ranges
            ]
            requests_by_sheet.setdefault(sheet["spreadsheet_id"], []).append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": ranges,
                        "booleanRule": {
                            "condition": {
                                "type": "NUMBER_LESS",
                                "values": [{"userEnteredValue": f"{threshold:g}"}],
                            },
                            "format": {
                                "backgroundColorStyle": {
                                    "rgbColor": {"red": 0.86, "green": 0.0, "blue": 0.12}
                                },
                                "textFormat": {
                                    "foregroundColorStyle": {
                                        "rgbColor": {"red": 1, "green": 1, "blue": 1}
                                    },
                                    "bold": True,
                                },
                            },
                        },
                    },
                    "index": 0,
                }
            })
            target = {
                "spreadsheet_id": sheet["spreadsheet_id"],
                "spreadsheet_title": sheet["spreadsheet_title"],
                "sheet_title": sheet["sheet_title"],
                "grouping": sheet["grouping"],
                "column": column["label"],
                "threshold": threshold,
                "student_rows": len(sheet["students"]),
            }
            targets.append(target)
            target_descriptions.append(f"{sheet['grouping'] or sheet['sheet_title']} {column['label']}")

    if not targets:
        raise ValueError("No matching percentage columns found for failure highlighting.")

    service = _work_sheets()
    for spreadsheet_id, requests in requests_by_sheet.items():
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()
        except HttpError as exc:
            if getattr(exc.resp, "status", None) == 403:
                raise PermissionError(_classlist_permission_message(exc, [spreadsheet_id], target_descriptions)) from exc
            raise
        _invalidate_classlist_cache(spreadsheet_id)

    return {"highlighted_columns": len(targets), "threshold": threshold, "targets": targets}


def format_mtl_failure_highlighting(
    class_query: str = "",
    assessment_query: str = "",
    failure_threshold: float = 50,
    teacher_query: str = "HERWANTO",
) -> str:
    result = apply_mtl_failure_highlighting(class_query, assessment_query, failure_threshold, teacher_query)
    target_text = "; ".join(
        f"{target['column']} in {target['spreadsheet_title']} / {target['sheet_title']}"
        for target in result["targets"]
    )
    return f"Applied red failure highlighting below {result['threshold']:g}% to {target_text}."


def _stats_for_values(values: list[dict]) -> dict:
    scores = [item["score"] for item in values]
    if not scores:
        return {}
    ordered = sorted(scores)
    mean = statistics.fmean(scores)
    return {
        "count": len(scores),
        "mean": round(mean, 1),
        "median": round(statistics.median(scores), 1),
        "min": round(min(scores), 1),
        "max": round(max(scores), 1),
        "q1": round(ordered[len(ordered) // 4], 1),
        "q3": round(ordered[(len(ordered) * 3) // 4], 1),
        "std_dev": round(statistics.pstdev(scores), 1) if len(scores) > 1 else 0,
        "pass_count": sum(1 for score in scores if score >= 50),
        "distinction_count": sum(1 for score in scores if score >= 75),
        "below_50_count": sum(1 for score in scores if score < 50),
    }


def _score_from_row(row: list[str], column: dict) -> tuple[float | None, str]:
    col_idx = int(column["index"])
    raw = row[col_idx].strip() if col_idx < len(row) else ""
    source_idx = column.get("derived_percent_from_index")
    if source_idx is not None:
        source_raw = row[source_idx].strip() if source_idx < len(row) else ""
        source_score = _number_from_cell(source_raw)
        max_score = column.get("max_score")
        if source_score is not None and max_score:
            return round((source_score / float(max_score)) * 100, 1), source_raw
        return None, source_raw
    return _number_from_cell(raw), raw


def _student_score_values(sheet: dict, column: dict) -> tuple[list[dict], dict]:
    values = []
    statuses = {}
    for student in sheet["students"]:
        row = sheet["rows"][student["row_index"]] if student["row_index"] < len(sheet["rows"]) else []
        score, raw = _score_from_row(row, column)
        if score is not None:
            values.append({
                "student": student,
                "score": score,
                "raw": raw,
            })
            continue
        norm = _norm_cell(raw)
        if norm:
            statuses[norm] = statuses.get(norm, 0) + 1
    return values, statuses


def _student_label(student: dict) -> str:
    number = f"{student.get('no')}. " if student.get("no") else ""
    class_name = f" [{student.get('class')}]" if student.get("class") else ""
    return f"{number}{student.get('name', '')}{class_name}".strip()


def analyze_mtl_scores(
    class_query: str = "",
    assessment_query: str = "",
    compare_from: str = "",
    compare_to: str = "",
    teacher_query: str = "HERWANTO",
) -> dict:
    sheets = _score_sheet_matches(teacher_query, class_query)
    if not sheets:
        raise ValueError("No matching MTL score sheets found.")

    analyses = []
    for sheet in sheets:
        query = assessment_query or compare_to or compare_from
        columns = _score_columns_for_sheet(sheet, query, prefer_percent=bool(query))
        query_parts = _score_query_parts(query)
        if not columns and query and not query_parts["assessment_norms"] and not query_parts["wants_percent"] and not query_parts["wants_pre_wa"]:
            columns = _score_columns_for_sheet(sheet, "")
        if not columns:
            continue
        sheet_analysis = {
            "spreadsheet_title": sheet["spreadsheet_title"],
            "sheet_title": sheet["sheet_title"],
            "grouping": sheet["grouping"],
            "student_count": len(sheet["students"]),
            "columns": [],
            "progress": [],
        }
        for column in columns[:12]:
            values, statuses = _student_score_values(sheet, column)
            stats = _stats_for_values(values)
            if not stats:
                continue
            sorted_values = sorted(values, key=lambda item: item["score"])
            mean = stats["mean"]
            underperforming = [
                {
                    "student": _student_label(item["student"]),
                    "score": item["score"],
                    "reason": "below 50" if item["score"] < 50 else "10+ points below mean",
                }
                for item in sorted_values
                if item["score"] < 50 or item["score"] <= mean - 10
            ][:8]
            top = [
                {"student": _student_label(item["student"]), "score": item["score"]}
                for item in sorted(values, key=lambda item: item["score"], reverse=True)[:5]
            ]
            sheet_analysis["columns"].append({
                "label": column["label"],
                "computed_percent": bool(column.get("derived_percent_from_index") is not None),
                "stats": stats,
                "statuses": statuses,
                "underperforming": underperforming,
                "top": top,
            })

        progress_pairs = []
        from_cols = _score_columns_for_sheet(sheet, compare_from, prefer_percent=True) if compare_from else []
        to_cols = _score_columns_for_sheet(sheet, compare_to, prefer_percent=True) if compare_to else []
        if from_cols and to_cols:
            progress_pairs = [(from_cols[0], to_cols[0])]
        elif len(columns) >= 2:
            numeric_columns = [col for col in columns if col["numeric_count"]]
            if len(numeric_columns) >= 2:
                progress_pairs = [(numeric_columns[-2], numeric_columns[-1])]
        for start_col, end_col in progress_pairs[:1]:
            changes = []
            for student in sheet["students"]:
                row = sheet["rows"][student["row_index"]] if student["row_index"] < len(sheet["rows"]) else []
                start, _ = _score_from_row(row, start_col)
                end, _ = _score_from_row(row, end_col)
                if start is None or end is None:
                    continue
                changes.append({
                    "student": _student_label(student),
                    "from": start,
                    "to": end,
                    "change": round(end - start, 1),
                })
            if changes:
                sheet_analysis["progress"].append({
                    "from_label": start_col["label"],
                    "to_label": end_col["label"],
                    "most_improved": sorted(changes, key=lambda item: item["change"], reverse=True)[:5],
                    "drastic_drops": sorted(changes, key=lambda item: item["change"])[:5],
                })
        analyses.append(sheet_analysis)

    if not analyses:
        raise ValueError("No numeric score columns matched that request.")
    return {"analyses": analyses}


def format_mtl_score_analysis(
    class_query: str = "",
    assessment_query: str = "",
    compare_from: str = "",
    compare_to: str = "",
    teacher_query: str = "HERWANTO",
) -> str:
    result = analyze_mtl_scores(class_query, assessment_query, compare_from, compare_to, teacher_query)
    lines = ["MTL score analysis:"]
    for analysis in result["analyses"]:
        label = analysis["grouping"] or analysis["sheet_title"]
        lines.append(f"\n{label} - {analysis['spreadsheet_title']} ({analysis['student_count']} students)")
        for column in analysis["columns"][:6]:
            stats = column["stats"]
            pass_rate = round((stats["pass_count"] / stats["count"]) * 100) if stats["count"] else 0
            distinction_rate = round((stats["distinction_count"] / stats["count"]) * 100) if stats["count"] else 0
            lines.append(
                f"- {column['label']}: mean {stats['mean']}, median {stats['median']}, "
                f"range {stats['min']}-{stats['max']}, SD {stats['std_dev']}, "
                f"pass {stats['pass_count']}/{stats['count']} ({pass_rate}%), "
                f"distinction {stats['distinction_count']}/{stats['count']} ({distinction_rate}%)."
            )
            if column["label"].endswith(" %"):
                lines.append("  This is the percentage column for that assessment, not the raw component columns.")
            if column["statuses"]:
                status_text = ", ".join(
                    f"{key} ({SCORE_STATUS_LABELS.get(key, 'non-scoring status')}): {value}"
                    for key, value in sorted(column["statuses"].items())
                )
                lines.append(f"  Status/non-numeric, excluded from score statistics: {status_text}.")
            if column["underperforming"]:
                lines.append("  Underperforming / watchlist:")
                for item in column["underperforming"][:6]:
                    lines.append(f"  - {item['student']}: {item['score']} ({item['reason']})")
            if column["top"]:
                top_text = "; ".join(f"{item['student']} {item['score']}" for item in column["top"][:3])
                lines.append(f"  Strongest: {top_text}.")
        for progress in analysis["progress"]:
            lines.append(f"  Progress: {progress['from_label']} -> {progress['to_label']}")
            improved = "; ".join(
                f"{item['student']} {item['change']:+g}" for item in progress["most_improved"][:5]
            )
            drops = "; ".join(
                f"{item['student']} {item['change']:+g}" for item in progress["drastic_drops"][:5]
            )
            if improved:
                lines.append(f"  Most improved: {improved}.")
            if drops:
                lines.append(f"  Drastic drops: {drops}.")
    return "\n".join(lines)


def _trend_score_from_row(row: list[str], column: dict) -> tuple[float | None, str]:
    score, raw = _score_from_row(row, column)
    if score is None:
        return None, raw
    if column.get("is_percent"):
        return round(score, 1), raw
    max_score = column.get("max_score")
    if max_score:
        return round((score / float(max_score)) * 100, 1), raw
    return round(score, 1), raw


def _trend_columns_for_sheet(sheet: dict, column_query: str = "") -> list[dict]:
    columns = _score_columns_for_sheet(sheet, column_query, prefer_percent=False)
    if not columns:
        return []
    percent_source_indexes = {
        column.get("derived_percent_from_index", column["index"] - 1)
        for column in columns
        if column.get("is_percent")
    }
    trend_columns = []
    for column in columns:
        if column.get("is_percent"):
            trend_columns.append(column)
            continue
        if column["index"] in percent_source_indexes:
            continue
        if column.get("max_score"):
            trend_columns.append(column)
    return sorted(trend_columns, key=lambda item: item["index"])


def _trend_insight(recent: float | None, change: float | None) -> str:
    if recent is None:
        return "No current score"
    if recent < 50 and change is not None and change <= -10:
        return "Immediate attention: below 50 and declining"
    if recent < 50:
        return "Attention: below 50"
    if change is not None and change <= -10:
        return "Check-in: significant regression"
    if change is not None and change >= 10 and recent >= 75:
        return "Affirmation: strong improvement into high band"
    if change is not None and change >= 10:
        return "Affirmation: strong improvement"
    if recent >= 75 and (change is None or change >= 0):
        return "Affirmation: sustaining strong performance"
    if change is not None and change >= 5:
        return "Encourage: improving"
    if change is not None and change <= -5:
        return "Monitor: mild regression"
    return "Steady"


def _safe_sheet_title(value: str) -> str:
    title = re.sub(r"[\[\]\*\?/\\:]", " ", str(value or "")).strip()
    title = re.sub(r"\s+", " ", title)
    return title[:90] or "HIRA Analysis"


def _spreadsheet_sheet_ids(spreadsheet_id: str) -> tuple[str, dict[str, int]]:
    book = _work_sheets().spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=False,
        fields="properties(title),sheets(properties(sheetId,title))",
    ).execute()
    title = book.get("properties", {}).get("title", spreadsheet_id)
    ids = {}
    for sheet in book.get("sheets", []) or []:
        props = sheet.get("properties", {})
        ids[str(props.get("title", "") or "")] = int(props.get("sheetId"))
    return title, ids


def _build_trend_report_values(sheet: dict, columns: list[dict]) -> tuple[list[list], dict]:
    value_sets = []
    summary_rows = []
    for column in columns:
        values = []
        statuses = {}
        for student in sheet["students"]:
            row = sheet["rows"][student["row_index"]] if student["row_index"] < len(sheet["rows"]) else []
            score, raw = _trend_score_from_row(row, column)
            if score is not None:
                values.append({"student": student, "score": score})
            else:
                norm = _norm_cell(raw)
                if norm:
                    statuses[norm] = statuses.get(norm, 0) + 1
        stats = _stats_for_values(values)
        if not stats:
            continue
        value_sets.append((column, values, statuses, stats))
        summary_rows.append([
            column["label"],
            stats["count"],
            stats["mean"],
            stats["median"],
            round((stats["pass_count"] / stats["count"]) * 100, 1) if stats["count"] else 0,
            round((stats["distinction_count"] / stats["count"]) * 100, 1) if stats["count"] else 0,
            stats["below_50_count"],
            ", ".join(f"{key}:{value}" for key, value in sorted(statuses.items())),
        ])

    labels = [item[0]["label"] for item in value_sets]
    score_by_student = {}
    for column, values, _statuses, _stats in value_sets:
        for item in values:
            key = item["student"]["row_index"]
            score_by_student.setdefault(key, {})[column["index"]] = item["score"]

    student_rows = []
    for student in sheet["students"]:
        score_map = score_by_student.get(student["row_index"], {})
        scores = [score_map.get(column["index"]) for column, _values, _statuses, _stats in value_sets]
        numeric_scores = [score for score in scores if score is not None]
        if not numeric_scores:
            continue
        first = numeric_scores[0]
        recent = numeric_scores[-1]
        change = round(recent - first, 1) if len(numeric_scores) >= 2 else None
        student_rows.append([
            student.get("no", ""),
            student.get("class", ""),
            student.get("name", ""),
            *["" if score is None else score for score in scores],
            "" if change is None else change,
            _trend_insight(recent, change),
        ])

    change_index = 3 + len(labels)
    students_with_change = [row for row in student_rows if row[change_index] != ""]
    improved = sorted(
        [row for row in students_with_change if row[change_index] > 0],
        key=lambda row: row[change_index],
        reverse=True,
    )[:10]
    regressed = sorted(
        [row for row in students_with_change if row[change_index] < 0],
        key=lambda row: row[change_index],
    )[:10]
    attention = [
        row for row in student_rows
        if "Attention" in str(row[-1]) or "Immediate attention" in str(row[-1]) or "Check-in" in str(row[-1])
    ][:12]
    affirmation = [row for row in student_rows if "Affirmation" in str(row[-1])][:12]

    rows: list[list] = []
    rows.append([f"H.I.R.A score trend report: {sheet['grouping'] or sheet['sheet_title']}"])
    rows.append([f"Generated {datetime.now(SGT).strftime('%Y-%m-%d %H:%M')} SGT", "Scores normalised to percentage where raw max marks are available."])
    rows.append([])
    rows.append(["Assessment", "Count", "Mean %", "Median %", "Pass %", "Distinction %", "Below 50", "Statuses"])
    rows.extend(summary_rows)
    rows.append([])
    rows.append(["Most improved", "Change"])
    rows.extend([[row[2], row[change_index]] for row in improved[:8]])
    rows.append([])
    rows.append(["Most regressed", "Change"])
    rows.extend([[row[2], row[change_index]] for row in regressed[:8]])
    rows.append([])
    rows.append(["Needs attention", "Latest insight"])
    rows.extend([[row[2], row[-1]] for row in attention[:8]])
    rows.append([])
    rows.append(["Affirmation candidates", "Latest insight"])
    rows.extend([[row[2], row[-1]] for row in affirmation[:8]])
    rows.append([])
    student_table_row = len(rows)
    rows.append(["No", "Class", "Student", *labels, "Change", "Insight"])
    rows.extend(student_rows)

    return rows, {
        "summary_start_row": 3,
        "summary_count": len(summary_rows),
        "student_table_row": student_table_row,
        "student_count": len(student_rows),
        "assessment_count": len(labels),
        "most_improved": [{"student": row[2], "change": row[change_index]} for row in improved[:5]],
        "most_regressed": [{"student": row[2], "change": row[change_index]} for row in regressed[:5]],
        "attention_count": len(attention),
        "affirmation_count": len(affirmation),
    }


def _trend_chart_requests(report_sheet_id: int, meta: dict) -> list[dict]:
    requests = []
    summary_start = meta["summary_start_row"] + 1
    summary_end = summary_start + meta["summary_count"]
    if meta["summary_count"] >= 1:
        requests.append({
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Mean by Assessment",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "NO_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Assessment"},
                                {"position": "LEFT_AXIS", "title": "Mean %"},
                            ],
                            "domains": [{
                                "domain": {"sourceRange": {"sources": [{
                                    "sheetId": report_sheet_id,
                                    "startRowIndex": summary_start,
                                    "endRowIndex": summary_end,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 1,
                                }]}}
                            }],
                            "series": [{
                                "series": {"sourceRange": {"sources": [{
                                    "sheetId": report_sheet_id,
                                    "startRowIndex": summary_start,
                                    "endRowIndex": summary_end,
                                    "startColumnIndex": 2,
                                    "endColumnIndex": 3,
                                }]}},
                                "targetAxis": "LEFT_AXIS",
                            }],
                            "headerCount": 0,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": report_sheet_id, "rowIndex": 2, "columnIndex": 9},
                            "widthPixels": 620,
                            "heightPixels": 360,
                        }
                    },
                }
            }
        })
    student_start = meta["student_table_row"] + 1
    student_end = min(student_start + meta["student_count"], student_start + 20)
    change_col = 3 + meta["assessment_count"]
    if meta["student_count"] >= 1 and meta["assessment_count"] >= 2:
        requests.append({
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Student Change (first to latest)",
                        "basicChart": {
                            "chartType": "COLUMN",
                            "legendPosition": "NO_LEGEND",
                            "axis": [
                                {"position": "BOTTOM_AXIS", "title": "Change"},
                                {"position": "LEFT_AXIS", "title": "Student"},
                            ],
                            "domains": [{
                                "domain": {"sourceRange": {"sources": [{
                                    "sheetId": report_sheet_id,
                                    "startRowIndex": student_start,
                                    "endRowIndex": student_end,
                                    "startColumnIndex": 2,
                                    "endColumnIndex": 3,
                                }]}}
                            }],
                            "series": [{
                                "series": {"sourceRange": {"sources": [{
                                    "sheetId": report_sheet_id,
                                    "startRowIndex": student_start,
                                    "endRowIndex": student_end,
                                    "startColumnIndex": change_col,
                                    "endColumnIndex": change_col + 1,
                                }]}},
                                "targetAxis": "LEFT_AXIS",
                            }],
                            "headerCount": 0,
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": report_sheet_id, "rowIndex": 21, "columnIndex": 9},
                            "widthPixels": 620,
                            "heightPixels": 380,
                        }
                    },
                }
            }
        })
    return requests


def generate_mtl_score_trend_report(
    class_query: str = "",
    assessment_query: str = "",
    teacher_query: str = "HERWANTO",
) -> dict:
    sheets = _score_sheet_matches(teacher_query, class_query)
    if not sheets:
        raise ValueError("No matching MTL score sheets found.")
    if len(sheets) > 1 and not _norm_cell(class_query):
        options = [f"{item['grouping'] or item['sheet_title']} / {item['spreadsheet_title']}" for item in sheets[:8]]
        raise ValueError("More than one classlist sheet matched. Specify the class/group: " + "; ".join(options))

    reports = []
    service = _work_sheets()
    for sheet in sheets:
        columns = _trend_columns_for_sheet(sheet, assessment_query)
        if len(columns) < 2:
            raise ValueError("Need at least two matching score columns to build a trend report.")
        rows, meta = _build_trend_report_values(sheet, columns)
        if meta["assessment_count"] < 2 or meta["student_count"] < 1:
            raise ValueError("Need at least two populated score columns and one student score to build a trend report.")
        report_title = _safe_sheet_title(f"HIRA Trends - {sheet['grouping'] or sheet['sheet_title']}")
        _spreadsheet_title, existing_sheet_ids = _spreadsheet_sheet_ids(sheet["spreadsheet_id"])
        report_sheet_id = int(uuid.uuid4().int % 900000000) + 1000000
        requests = []
        if report_title in existing_sheet_ids:
            requests.append({"deleteSheet": {"sheetId": existing_sheet_ids[report_title]}})
        requests.append({
            "addSheet": {
                "properties": {
                    "sheetId": report_sheet_id,
                    "title": report_title,
                    "gridProperties": {"rowCount": 220, "columnCount": 18},
                }
            }
        })
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet["spreadsheet_id"],
            body={"requests": requests},
        ).execute()

        service.spreadsheets().values().update(
            spreadsheetId=sheet["spreadsheet_id"],
            range=f"{_quote_sheet_name(report_title)}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": rows},
        ).execute()
        chart_requests = _trend_chart_requests(report_sheet_id, meta)
        if chart_requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet["spreadsheet_id"],
                body={"requests": chart_requests},
            ).execute()
        reports.append({
            "spreadsheet_id": sheet["spreadsheet_id"],
            "spreadsheet_title": sheet["spreadsheet_title"],
            "sheet_title": report_title,
            "source_sheet_title": sheet["sheet_title"],
            "grouping": sheet["grouping"],
            "assessments": [column["label"] for column in columns],
            "student_count": meta["student_count"],
            "attention_count": meta["attention_count"],
            "affirmation_count": meta["affirmation_count"],
            "most_improved": meta["most_improved"],
            "most_regressed": meta["most_regressed"],
            "url": f"https://docs.google.com/spreadsheets/d/{sheet['spreadsheet_id']}/edit#gid={report_sheet_id}",
        })
        _invalidate_classlist_cache(sheet["spreadsheet_id"])
    return {"reports": reports}


def format_mtl_score_trend_report(
    class_query: str = "",
    assessment_query: str = "",
    teacher_query: str = "HERWANTO",
) -> str:
    result = generate_mtl_score_trend_report(class_query, assessment_query, teacher_query)
    lines = ["Created MTL trend report:"]
    for report in result["reports"]:
        lines.append(
            f"- {report['sheet_title']} in {report['spreadsheet_title']} "
            f"({report['student_count']} students, {len(report['assessments'])} assessments)."
        )
        lines.append(f"  Link: {report['url']}")
        if report["most_improved"]:
            improved = "; ".join(f"{item['student']} +{item['change']}" for item in report["most_improved"][:3])
            lines.append(f"  Most improved: {improved}.")
        if report["most_regressed"]:
            regressed = "; ".join(f"{item['student']} {item['change']}" for item in report["most_regressed"][:3])
            lines.append(f"  Most regressed: {regressed}.")
        lines.append(
            f"  Flagged {report['attention_count']} for attention and "
            f"{report['affirmation_count']} for affirmation."
        )
    return "\n".join(lines)


def update_mtl_class_score(
    class_query: str,
    student_query: str,
    score_column: str,
    score_value: str,
    teacher_query: str = "HERWANTO",
) -> dict:
    if not str(student_query or "").strip():
        raise ValueError("student_query is required.")
    if not str(score_column or "").strip():
        raise ValueError("score_column is required.")

    service = _work_sheets()
    class_filter = _norm_cell(class_query)
    student_filter = _norm_cell(student_query)
    matches = []
    for spreadsheet_id in _configured_classlist_sheet_ids():
        book = _classlist_book(spreadsheet_id)
        spreadsheet_title = book.get("properties", {}).get("title", spreadsheet_id)
        for sheet in book.get("sheets", []):
            props = sheet.get("properties", {})
            sheet_title = props.get("title", "")
            sheet_id = props.get("sheetId")
            row_data = []
            for data in sheet.get("data", []):
                row_data.extend(data.get("rowData", []))
            rows = [_row_values(row) for row in row_data]
            if not _teacher_matches(rows, sheet_title, teacher_query):
                continue
            header = _find_classlist_header(rows)
            if not header:
                continue
            header_idx, name_col, class_col, no_col = header
            grouping = _first_after_label(rows, "GROUPING")
            if class_filter and not _class_query_matches(class_query, grouping, sheet_title):
                row_has_class = any(
                    class_col >= 0
                    and class_col < len(row)
                    and _class_query_matches(class_query, row[class_col])
                    for row in rows[header_idx + 1:]
                )
                if not row_has_class:
                    continue
            headers = [str(cell or "").strip() for cell in rows[header_idx]]
            protected = {name_col, class_col, no_col}
            candidate_cols = _matching_header_columns(headers, score_column, protected)
            students, _ = _extract_students_with_fields(rows)
            for student in students:
                if class_filter and not _class_query_matches(class_query, grouping, student.get("class", "")):
                    continue
                if student_filter in _norm_cell(_student_identity_text(student)):
                    matches.append({
                        "spreadsheet_id": spreadsheet_id,
                        "spreadsheet_title": spreadsheet_title,
                        "sheet_title": sheet_title,
                        "grouping": grouping,
                        "student": student,
                        "candidate_cols": candidate_cols,
                        "headers": headers,
                        "protected_cols": protected,
                    })

    if not matches:
        raise ValueError("No matching student found in Herwanto's MTL classlist sheets.")
    if len(matches) > 1:
        options = [
            f"{item['student'].get('no')}. {item['student'].get('name')} [{item['student'].get('class') or item['grouping']}]"
            for item in matches[:8]
        ]
        raise ValueError("More than one matching student found. Be more specific: " + "; ".join(options))

    target = matches[0]
    candidate_cols = target["candidate_cols"]
    if not candidate_cols:
        available = [
            header for idx, header in enumerate(target["headers"])
            if header.strip() and idx not in target["protected_cols"]
        ]
        raise ValueError("No matching score column found. Available columns include: " + ", ".join(available[:18]))
    if len(candidate_cols) > 1:
        options = [target["headers"][idx] for idx in candidate_cols]
        raise ValueError("More than one matching score column found. Be more specific: " + ", ".join(options))

    col_idx = candidate_cols[0]
    row_number = int(target["student"]["row_index"]) + 1
    cell_range = f"{_quote_sheet_name(target['sheet_title'])}!{_column_letter(col_idx)}{row_number}"
    service.spreadsheets().values().update(
        spreadsheetId=target["spreadsheet_id"],
        range=cell_range,
        valueInputOption="USER_ENTERED",
        body={"values": [[str(score_value)]]},
    ).execute()
    _invalidate_classlist_cache(target["spreadsheet_id"])
    return {
        "spreadsheet_title": target["spreadsheet_title"],
        "sheet_title": target["sheet_title"],
        "grouping": target["grouping"],
        "student": target["student"]["name"],
        "class": target["student"].get("class", ""),
        "column": target["headers"][col_idx],
        "value": str(score_value),
        "range": cell_range,
    }


def fill_mtl_percentage_scores(
    class_query: str,
    assessment_query: str = "",
    teacher_query: str = "HERWANTO",
    only_blank: bool = True,
) -> dict:
    service = _work_sheets()
    class_filter = _norm_cell(class_query)
    assessment_filter = _norm_cell(assessment_query)
    assessment_parts = _score_query_parts(assessment_query)
    if assessment_parts["assessment_norms"]:
        assessment_filter = " ".join(assessment_parts["assessment_norms"])
    sheet_matches = []
    for spreadsheet_id in _configured_classlist_sheet_ids():
        book = _classlist_book(spreadsheet_id)
        spreadsheet_title = book.get("properties", {}).get("title", spreadsheet_id)
        for sheet in book.get("sheets", []):
            props = sheet.get("properties", {})
            sheet_title = props.get("title", "")
            sheet_id = props.get("sheetId")
            row_data = []
            for data in sheet.get("data", []):
                row_data.extend(data.get("rowData", []))
            rows = [_row_values(row) for row in row_data]
            if not _teacher_matches(rows, sheet_title, teacher_query):
                continue
            header = _find_classlist_header(rows)
            if not header:
                continue
            header_idx, name_col, class_col, no_col = header
            grouping = _first_after_label(rows, "GROUPING")
            if class_filter and not _class_query_matches(class_query, grouping, sheet_title):
                row_has_class = any(
                    class_col >= 0
                    and class_col < len(row)
                    and _class_query_matches(class_query, row[class_col])
                    for row in rows[header_idx + 1:]
                )
                if not row_has_class:
                    continue
            headers = [str(cell or "").strip() for cell in rows[header_idx]]
            percent_cols = []
            raw_score_cols = []
            for idx, header_label in enumerate(headers):
                assessment_label = _assessment_label_for_column(rows, header_idx, idx)
                if _is_percentage_header(header_label):
                    source_col = idx - 1
                    if source_col < 0:
                        continue
                    source_header = headers[source_col] if source_col < len(headers) else ""
                    max_score = _number_from_header(source_header)
                    if not max_score:
                        continue
                    filter_haystack = _norm_cell(" ".join([assessment_label, header_label, source_header]))
                    if assessment_filter and assessment_filter not in filter_haystack:
                        continue
                    percent_cols.append({
                        "percent_col": idx,
                        "source_col": source_col,
                        "max_score": max_score,
                        "assessment": assessment_label or header_label,
                        "header": header_label,
                        "create_column": False,
                    })
                    continue
                if idx in {name_col, class_col, no_col}:
                    continue
                max_score = _number_from_header(header_label)
                if not max_score:
                    continue
                filter_haystack = _norm_cell(" ".join([assessment_label, header_label]))
                if assessment_filter and assessment_filter not in filter_haystack:
                    continue
                raw_score_cols.append({
                    "percent_col": idx + 1,
                    "source_col": idx,
                    "max_score": max_score,
                    "assessment": assessment_label or header_label,
                    "header": header_label,
                    "create_column": True,
                    "insert_column": not (
                        idx + 1 < len(headers)
                        and not str(headers[idx + 1] or "").strip()
                    ),
                })
            existing_sources = {col["source_col"] for col in percent_cols}
            create_cols = [
                col for col in raw_score_cols
                if col["source_col"] not in existing_sources
                and not (
                    col["source_col"] + 1 < len(headers)
                    and _is_percentage_header(headers[col["source_col"] + 1])
                )
            ]
            if not percent_cols:
                percent_cols = create_cols
            if percent_cols:
                sheet_matches.append({
                    "spreadsheet_id": spreadsheet_id,
                    "spreadsheet_title": spreadsheet_title,
                    "sheet_title": sheet_title,
                    "sheet_id": sheet_id,
                    "rows": rows,
                    "header_idx": header_idx,
                    "name_col": name_col,
                    "class_col": class_col,
                    "grouping": grouping,
                    "percent_cols": percent_cols,
                })

    if not sheet_matches:
        raise ValueError("No matching percentage columns found in Herwanto's MTL classlist sheets.")
    if len(sheet_matches) > 1 and not class_filter:
        options = [f"{item['grouping'] or item['sheet_title']} / {item['spreadsheet_title']}" for item in sheet_matches[:8]]
        raise ValueError("More than one classlist sheet matched. Specify the class/group: " + "; ".join(options))
    if sum(len(item["percent_cols"]) for item in sheet_matches) > 1 and not assessment_filter:
        options = []
        for item in sheet_matches:
            for col in item["percent_cols"]:
                options.append(f"{item['grouping'] or item['sheet_title']} {col['assessment'] or '%'}")
        raise ValueError("More than one percentage column matched. Specify the assessment, e.g. FA1 or FA2: " + "; ".join(options[:8]))
    for item in sheet_matches:
        created_in_sheet = [col for col in item["percent_cols"] if col.get("create_column")]
        if len(created_in_sheet) > 1:
            options = [f"{item['grouping'] or item['sheet_title']} {col['header']}" for col in created_in_sheet]
            raise ValueError("More than one raw score column needs a new percentage column. Specify the assessment: " + "; ".join(options[:8]))

    updates = []
    insert_requests_by_sheet = {}
    changed = 0
    created_columns = 0
    skipped = 0
    copied_codes = 0
    filled_numbers = 0
    target_descriptions = []
    for item in sheet_matches:
        rows = item["rows"]
        class_col = item["class_col"]
        for col_info in item["percent_cols"]:
            if col_info.get("create_column"):
                if col_info.get("insert_column") and item.get("sheet_id") is None:
                    raise ValueError(f"Cannot create a percentage column in {item['sheet_title']} because the sheet id is missing.")
                if col_info.get("insert_column"):
                    insert_requests_by_sheet.setdefault(item["spreadsheet_id"], []).append({
                        "insertDimension": {
                            "range": {
                                "sheetId": item["sheet_id"],
                                "dimension": "COLUMNS",
                                "startIndex": col_info["percent_col"],
                                "endIndex": col_info["percent_col"] + 1,
                            },
                            "inheritFromBefore": True,
                        }
                    })
                header_range = (
                    f"{_quote_sheet_name(item['sheet_title'])}!"
                    f"{_column_letter(col_info['percent_col'])}{item['header_idx'] + 1}"
                )
                updates.append({
                    "range": header_range,
                    "values": [[_percentage_header_for_source(col_info.get("header", ""), col_info.get("assessment", ""))]],
                    "spreadsheet_id": item["spreadsheet_id"],
                })
                created_columns += 1
            for row_idx, row in enumerate(rows[item["header_idx"] + 1:], start=item["header_idx"] + 1):
                name = row[item["name_col"]].strip() if item["name_col"] < len(row) else ""
                class_name = row[class_col].strip() if class_col >= 0 and class_col < len(row) else ""
                if not name and not class_name:
                    break
                if class_filter and not _class_query_matches(class_query, item["grouping"], class_name):
                    skipped += 1
                    continue
                current = "" if col_info.get("insert_column") else (row[col_info["percent_col"]].strip() if col_info["percent_col"] < len(row) else "")
                source = row[col_info["source_col"]].strip() if col_info["source_col"] < len(row) else ""
                if only_blank and current:
                    skipped += 1
                    continue
                number = _number_from_cell(source)
                if number is not None:
                    next_value = _format_percentage((number / col_info["max_score"]) * 100)
                    filled_numbers += 1
                elif _norm_cell(source) in SCORE_STATUS_LABELS:
                    next_value = source.upper()
                    copied_codes += 1
                else:
                    skipped += 1
                    continue
                cell_range = (
                    f"{_quote_sheet_name(item['sheet_title'])}!"
                    f"{_column_letter(col_info['percent_col'])}{row_idx + 1}"
                )
                updates.append({
                    "range": cell_range,
                    "values": [[next_value]],
                    "spreadsheet_id": item["spreadsheet_id"],
                })
                changed += 1
            target_descriptions.append(f"{item['grouping'] or item['sheet_title']} {col_info['assessment']}")

    for spreadsheet_id, requests in insert_requests_by_sheet.items():
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()
        except HttpError as exc:
            if getattr(exc.resp, "status", None) == 403:
                raise PermissionError(_classlist_permission_message(exc, [spreadsheet_id], target_descriptions)) from exc
            raise

    updates_by_sheet = {}
    for update in updates:
        updates_by_sheet.setdefault(update["spreadsheet_id"], []).append({
            "range": update["range"],
            "values": update["values"],
        })
    for spreadsheet_id, data in updates_by_sheet.items():
        try:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": data},
            ).execute()
        except HttpError as exc:
            if getattr(exc.resp, "status", None) == 403:
                raise PermissionError(_classlist_permission_message(exc, [spreadsheet_id], target_descriptions)) from exc
            raise
        _invalidate_classlist_cache(spreadsheet_id)

    return {
        "updated_cells": changed,
        "created_columns": created_columns,
        "filled_numbers": filled_numbers,
        "copied_codes": copied_codes,
        "skipped": skipped,
        "targets": target_descriptions,
    }


# ─── CALENDAR WRITE ──────────────────────────────────────────────────────────

def create_event(title: str, start_dt: datetime, end_dt: datetime,
                 location: str = "", description: str = "") -> dict:
    """Create a calendar event. Uses first calendar ID (your personal calendar)."""
    cal_id = CALENDAR_IDS[0] if CALENDAR_IDS else "primary"
    event = {
        "summary": title,
        "start": {"dateTime": start_dt.isoformat(), "timeZone": "Asia/Singapore"},
        "end":   {"dateTime": end_dt.isoformat(),   "timeZone": "Asia/Singapore"},
    }
    if location:
        event["location"] = location
    if description:
        event["description"] = description
    result = _cal().events().insert(calendarId=cal_id, body=event).execute()
    return result


def create_all_day_event(title: str, start_date: date | str, end_date: date | str,
                         location: str = "", description: str = "") -> dict:
    """Create an all-day calendar event, inclusive of end_date."""
    cal_id = CALENDAR_IDS[0] if CALENDAR_IDS else "primary"
    start = date.fromisoformat(str(start_date)[:10]) if not isinstance(start_date, date) else start_date
    end = date.fromisoformat(str(end_date)[:10]) if not isinstance(end_date, date) else end_date
    event = {
        "summary": title,
        "start": {"date": start.isoformat()},
        "end": {"date": (end + timedelta(days=1)).isoformat()},
    }
    if location:
        event["location"] = location
    if description:
        event["description"] = description
    result = _cal().events().insert(calendarId=cal_id, body=event).execute()
    return result


def get_events_between(start: datetime, end: datetime):
    return _fetch_events(start, end)


def delete_event(event_id: str, calendar_id: str = "") -> bool:
    cal_id = calendar_id or (CALENDAR_IDS[0] if CALENDAR_IDS else "primary")
    _cal().events().delete(calendarId=cal_id, eventId=event_id).execute()
    return True


# ─── DRIVE ARTIFACTS ────────────────────────────────────────────────────────

def _drive_query_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _ensure_drive_folder(name: str, parent_id: str = "") -> str:
    service = _drive()
    escaped = _drive_query_escape(name)
    query = (
        f"name = '{escaped}' and mimeType = 'application/vnd.google-apps.folder' "
        "and trashed = false"
    )
    if parent_id:
        query += f" and '{parent_id}' in parents"
    result = service.files().list(q=query, fields="files(id,name)", pageSize=1).execute()
    files = result.get("files", [])
    if files:
        return files[0]["id"]

    body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    folder = service.files().create(body=body, fields="id").execute()
    return folder["id"]


def artifact_folder_id(category: str = "General") -> str:
    root_id = os.environ.get("GOOGLE_ARTIFACT_FOLDER_ID", "").strip()
    if not root_id:
        root_id = get_config("artifact_root_folder_id") or ""
    if not root_id:
        root_id = _ensure_drive_folder("H.I.R.A")
        set_config("artifact_root_folder_id", root_id)
    category_name = (category or "General").strip().title()
    cache_key = f"artifact_folder_{category_name.lower().replace(' ', '_')}"
    folder_id = get_config(cache_key) or ""
    if not folder_id:
        folder_id = _ensure_drive_folder(category_name, parent_id=root_id)
        set_config(cache_key, folder_id)
    return folder_id

def upload_artifact(path: str, convert_to: str = "", category: str = "General") -> dict:
    """Upload a generated file to Drive, optionally converting to Docs/Slides."""
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(path)

    ext = source.suffix.lower()
    source_mime = {
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".pdf": "application/pdf",
    }.get(ext, "application/octet-stream")

    target_mime = ""
    if convert_to == "doc":
        target_mime = "application/vnd.google-apps.document"
    elif convert_to == "slides":
        target_mime = "application/vnd.google-apps.presentation"

    body = {"name": source.stem}
    try:
        body["parents"] = [artifact_folder_id(category)]
    except Exception:
        pass
    if target_mime:
        body["mimeType"] = target_mime

    media = MediaFileUpload(str(source), mimetype=source_mime, resumable=False)
    uploaded = _drive().files().create(
        body=body,
        media_body=media,
        fields="id,name,mimeType,webViewLink",
    ).execute()
    share_email = os.environ.get("GOOGLE_ARTIFACT_SHARE_EMAIL", "").strip()
    if share_email:
        role = os.environ.get("GOOGLE_ARTIFACT_SHARE_ROLE", "writer").strip() or "writer"
        try:
            _drive().permissions().create(
                fileId=uploaded["id"],
                body={"type": "user", "role": role, "emailAddress": share_email},
                fields="id",
                sendNotificationEmail=False,
            ).execute()
        except Exception:
            pass
    return uploaded


# ─── SHEETS: REMINDERS ───────────────────────────────────────────────────────
# Sheet structure: id | description | due_date | category | done | created

def _raw_reminders():
    r = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Reminders!A2:F"
    ).execute()
    return r.get("values", [])


def get_reminders(include_done=False):
    rows = _raw_reminders()
    out = []
    for row in rows:
        if len(row) < 5:
            continue
        rid, desc, due, cat, done = row[0], row[1], row[2], row[3], row[4]
        is_done = done.upper() == "TRUE"
        if is_done and not include_done:
            continue
        out.append({"id": rid, "description": desc, "due": due, "category": cat, "done": is_done})
    return out


def add_reminder(description: str, due_date: str, category: str = "General") -> int:
    with _storage_mutation_lock("reminders", _reminders_mutation_lock):
        rows = _raw_reminders()
        numeric_ids = []
        for row in rows:
            try:
                numeric_ids.append(int(str(row[0]).strip()))
            except Exception:
                continue
        next_id = (max(numeric_ids) + 1) if numeric_ids else 1
        today = datetime.now(SGT).strftime("%Y-%m-%d")
        _sheets().spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Reminders!A:F",
            valueInputOption="USER_ENTERED",
            body={"values": [[str(next_id), description, due_date, category, "FALSE", today]]},
        ).execute()
        return next_id


def mark_done(reminder_id: str) -> bool:
    rows = _raw_reminders()
    for i, row in enumerate(rows):
        if row and str(row[0]) == str(reminder_id):
            row_num = i + 2
            _sheets().spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"Reminders!E{row_num}",
                valueInputOption="RAW",
                body={"values": [["TRUE"]]},
            ).execute()
            return True
    return False


def mark_not_done(reminder_id: str) -> bool:
    rows = _raw_reminders()
    for i, row in enumerate(rows):
        if row and str(row[0]) == str(reminder_id):
            row_num = i + 2
            _sheets().spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"Reminders!E{row_num}",
                valueInputOption="RAW",
                body={"values": [["FALSE"]]},
            ).execute()
            return True
    return False


def get_task_metadata() -> dict:
    raw = get_config("task_metadata")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def set_task_metadata(metadata: dict):
    set_config("task_metadata", json.dumps(metadata, ensure_ascii=False))


def update_task_metadata(
    reminder_id: str,
    priority: str = "",
    effort: str = "",
    next_action: str = "",
    status: str = "",
) -> dict:
    metadata = get_task_metadata()
    item = metadata.get(str(reminder_id), {})
    if priority:
        item["priority"] = priority.strip().lower()
    if effort:
        item["effort"] = effort.strip().lower()
    if next_action:
        item["next_action"] = next_action.strip()
    if status:
        item["status"] = status.strip().lower()
    metadata[str(reminder_id)] = item
    set_task_metadata(metadata)
    return item


def enriched_reminders(include_done=False) -> list:
    metadata = get_task_metadata()
    out = []
    for reminder in get_reminders(include_done=include_done):
        meta = metadata.get(str(reminder["id"]), {})
        out.append({**reminder, **meta})
    return out


# ─── CONFIG: MARKING TASKS ────────────────────────────────────────────────────

def get_marking_tasks(include_done=False) -> list:
    raw = get_config("marking_tasks")
    if not raw:
        return []
    try:
        tasks = json.loads(raw)
    except Exception:
        return []
    if not isinstance(tasks, list):
        return []
    out = []
    for task in tasks:
        if not isinstance(task, dict):
            continue
        normalised = {
            "id": str(task.get("id", "")),
            "title": task.get("title", ""),
            "total_scripts": int(task.get("total_scripts") or 0),
            "marked_count": int(task.get("marked_count") or 0),
            "stack_count": int(task.get("stack_count") or 1),
            "notes": task.get("notes", ""),
            "collected_date": task.get("collected_date", task.get("created", "")),
            "created": task.get("created", ""),
            "done": bool(task.get("done", False)),
            "completed_at": task.get("completed_at", ""),
        }
        if normalised["done"] and not include_done:
            continue
        out.append(normalised)
    return out


def set_marking_tasks(tasks: list):
    set_config("marking_tasks", json.dumps(tasks, ensure_ascii=False))


def reset_marking_tasks() -> dict:
    tasks = get_marking_tasks(include_done=True)
    completed_at = datetime.now(SGT).strftime("%Y-%m-%d")
    cleared = []
    for task in tasks:
        if task.get("done"):
            continue
        task["done"] = True
        task["completed_at"] = completed_at
        cleared.append({
            "id": str(task.get("id", "")),
            "title": task.get("title", ""),
            "total_scripts": int(task.get("total_scripts") or 0),
            "marked_count": int(task.get("marked_count") or 0),
        })
    if cleared:
        set_marking_tasks(tasks)
    return {"cleared_count": len(cleared), "cleared": cleared}


def add_marking_task(
    title: str,
    total_scripts: int = 0,
    stack_count: int = 1,
    notes: str = "",
    collected_date: str = "",
) -> dict:
    tasks = get_marking_tasks(include_done=True)
    next_id = max([int(t["id"]) for t in tasks if str(t.get("id", "")).isdigit()] or [0]) + 1
    total_scripts = max(0, int(total_scripts or 0))
    task = {
        "id": str(next_id),
        "title": title.strip(),
        "total_scripts": total_scripts,
        "marked_count": 0,
        "stack_count": max(1, int(stack_count or 1)),
        "notes": notes.strip(),
        "collected_date": collected_date.strip() or datetime.now(SGT).strftime("%Y-%m-%d"),
        "created": datetime.now(SGT).strftime("%Y-%m-%d"),
        "done": False,
        "completed_at": "",
    }
    tasks.append(task)
    set_marking_tasks(tasks)
    return task


def update_marking_progress(
    task_id: str,
    marked_count=None,
    increment: int = 0,
    done: bool = False,
):
    tasks = get_marking_tasks(include_done=True)
    updated = None
    for task in tasks:
        if str(task.get("id")) != str(task_id):
            continue
        current = int(task.get("marked_count") or 0)
        total = int(task.get("total_scripts") or 0)
        if marked_count is not None:
            current = max(0, int(marked_count))
        if increment:
            current = max(0, current + int(increment))
        if total:
            current = min(current, total)
        task["marked_count"] = current
        if done:
            task["done"] = True
            task["completed_at"] = datetime.now(SGT).strftime("%Y-%m-%d")
        updated = task
        break
    if updated:
        set_marking_tasks(tasks)
    return updated


# ─── CONFIG: CLASSOPS LEDGER ─────────────────────────────────────────────────

CLASSOPS_LEDGER_KEY = "classops_ledger"
NAVAL_BASE_2026_FORM_CLASSES = {
    "AN": "Anchor",
    "BE": "Beacon",
    "CO": "Compass",
    "DA": "Danforth",
    "EX": "Expedition",
    "FL": "Flagship",
    "GA": "Garrison",
    "HA": "Harbour",
}


def _normalise_classops_names(values) -> list[str]:
    names = []
    seen = set()
    for value in values or []:
        name = str(value or "").replace("*", "").strip()
        if not name:
            continue
        key = _norm_cell(name)
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


def get_classops_ledger() -> dict:
    raw = get_config(CLASSOPS_LEDGER_KEY)
    if not raw:
        return {"classes": {}}
    try:
        ledger = json.loads(raw)
    except Exception:
        return {"classes": {}}
    if not isinstance(ledger, dict):
        return {"classes": {}}
    classes = ledger.get("classes")
    if not isinstance(classes, dict):
        ledger["classes"] = {}
    return ledger


def set_classops_ledger(ledger: dict):
    if not isinstance(ledger, dict):
        ledger = {"classes": {}}
    if not isinstance(ledger.get("classes"), dict):
        ledger["classes"] = {}
    set_config(CLASSOPS_LEDGER_KEY, json.dumps(ledger, ensure_ascii=False))


def get_classops_content_overrides() -> dict:
    ledger = get_classops_ledger()
    overrides = ledger.get("content_overrides")
    if not isinstance(overrides, dict):
        legacy = ledger.get("content_title_overrides")
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
    return normalised


def save_classops_content_override(
    path: str,
    title: str | None = None,
    hidden: bool | None = None,
    no_submission_needed: bool | None = None,
    purpose_id: str | None = None,
) -> dict:
    clean_path = str(path or "").strip()
    if not clean_path:
        raise ValueError("Content item path is required.")
    ledger = get_classops_ledger()
    overrides = ledger.setdefault("content_overrides", {})
    current = overrides.get(clean_path)
    if not isinstance(current, dict):
        current = {"title": str(current or "").strip(), "hidden": False}
    if title is not None:
        current["title"] = str(title or "").strip()
    if hidden is not None:
        current["hidden"] = bool(hidden)
    if no_submission_needed is not None:
        current["no_submission_needed"] = bool(no_submission_needed)
    if purpose_id is not None:
        current["purpose_id"] = str(purpose_id or "").strip()
    overrides[clean_path] = current
    set_classops_ledger(ledger)
    return {"path": clean_path, **current}


def delete_classops_assignment(
    class_name: str,
    source_path: str = "",
    assignment_id: str = "",
) -> dict:
    ledger = get_classops_ledger()
    classes = ledger.setdefault("classes", {})
    class_key = str(class_name or "").strip().upper()
    clean_source_path = str(source_path or "").strip()
    clean_assignment_id = str(assignment_id or "").strip()
    if not class_key:
        raise ValueError("Class name is required.")
    if not clean_source_path and not clean_assignment_id:
        raise ValueError("Source path or assignment id is required.")

    record = classes.get(class_key)
    if not isinstance(record, dict):
        return {"deleted": False, "deleted_count": 0, "class_name": class_key, "assignments": []}

    assignments = [item for item in record.get("assignments", []) if isinstance(item, dict)]
    removed = []
    kept = []
    for assignment in assignments:
        source_matches = clean_source_path and str(assignment.get("source_path") or "").strip() == clean_source_path
        id_matches = clean_assignment_id and str(assignment.get("id") or "").strip() == clean_assignment_id
        if source_matches or id_matches:
            removed.append(assignment)
        else:
            kept.append(assignment)

    if not removed:
        return {
            "deleted": False,
            "deleted_count": 0,
            "class_name": class_key,
            "assignments": assignments,
        }

    record["assignments"] = kept
    set_classops_ledger(ledger)
    return {
        "deleted": True,
        "deleted_count": len(removed),
        "class_name": class_key,
        "assignment": removed[0],
        "assignments": kept,
    }


def _classops_canonical_class(value: str) -> str:
    compact = _norm_cell(value).replace(" ", "")
    if not compact:
        return ""
    for pattern in (r"SEC(?:ONDARY)?([1-4])([A-Z]{1,3}\d?)", r"([1-4])([A-Z]{1,3}\d?)"):
        match = re.match(pattern, compact)
        if match:
            return "".join(match.groups())
    return compact


def _classops_class_matches(class_name: str, value: str) -> bool:
    query = _classops_canonical_class(class_name)
    candidate = _classops_canonical_class(value)
    raw_candidate = _norm_cell(value).replace(" ", "")
    if not query or not candidate:
        return False
    return candidate == query or raw_candidate.startswith(query) or query in raw_candidate


def _classops_sheet_matches(class_name: str, *values: str) -> bool:
    return any(_classops_class_matches(class_name, value) for value in values)


def get_classops_students(class_name: str, include_scores: bool = False) -> list[dict]:
    lists = get_mtl_classlists(
        teacher_query="HERWANTO",
        class_query="",
        include_students=True,
        include_scores=include_scores,
    )
    students = []
    seen = set()
    for item in lists:
        source = item.get("grouping") or item.get("sheet_title") or item.get("spreadsheet_title") or ""
        item_students = item.get("students") or []
        has_student_classes = any(str(student.get("class") or "").strip() for student in item_students)
        sheet_matches_class = _classops_sheet_matches(class_name, item.get("grouping", ""), item.get("sheet_title", ""))
        if has_student_classes:
            exact_students = [
                student for student in item_students
                if _classops_class_matches(class_name, str(student.get("class") or "").strip())
            ]
            if exact_students:
                candidate_students = exact_students
            elif sheet_matches_class:
                candidate_students = item_students
            else:
                continue
        elif sheet_matches_class:
            candidate_students = item_students
        else:
            continue
        for student in candidate_students:
            name = str(student.get("name") or "").replace("*", "").strip()
            if not name:
                continue
            key = _norm_cell(f"{student.get('class', '')} {name}")
            if key in seen:
                continue
            seen.add(key)
            row = {
                "no": str(student.get("no") or "").strip(),
                "class": str(student.get("class") or class_name or "").strip(),
                "name": name,
                "source": source,
            }
            if include_scores:
                row["fields"] = student.get("fields") if isinstance(student.get("fields"), dict) else {}
            students.append(row)
    return students


def save_classops_assignment(
    class_name: str,
    lesson_date: str,
    topic: str = "",
    folder: str = "",
    assignment_title: str = "",
    collect_by: str = "",
    source_path: str = "",
    absent=None,
    submitted=None,
    non_submitted=None,
    notes: str = "",
) -> dict:
    ledger = get_classops_ledger()
    classes = ledger.setdefault("classes", {})
    class_key = str(class_name or "").strip().upper()
    if not class_key:
        raise ValueError("Class name is required.")
    if not str(assignment_title or "").strip():
        raise ValueError("Assignment title is required.")
    record = classes.setdefault(class_key, {"lessons": [], "assignments": []})
    now = datetime.now(SGT).isoformat()
    clean_source_path = str(source_path or "").strip()
    assignments = record.setdefault("assignments", [])
    existing = None
    if clean_source_path:
        existing = next(
            (
                item for item in assignments
                if isinstance(item, dict) and str(item.get("source_path") or "").strip() == clean_source_path
            ),
            None,
        )
    if existing is None:
        match_key = "|".join([
            str(lesson_date or "").strip(),
            str(folder or "").strip(),
            str(assignment_title or "").strip().lower(),
        ])
        existing = next(
            (
                item for item in assignments
                if isinstance(item, dict)
                and not str(item.get("source_path") or "").strip()
                and "|".join([
                    str(item.get("lesson_date") or "").strip(),
                    str(item.get("folder") or "").strip(),
                    str(item.get("assignment_title") or "").strip().lower(),
                ]) == match_key
            ),
            None,
        )
    assignment_id = str(existing.get("id") or uuid.uuid4()) if isinstance(existing, dict) else str(uuid.uuid4())
    assignment = {
        "id": assignment_id,
        "class_name": class_key,
        "lesson_date": str(lesson_date or "").strip(),
        "topic": str(topic or "").strip(),
        "folder": str(folder or "").strip(),
        "source_path": clean_source_path,
        "tracking_mode": "non_submission_list",
        "assignment_title": str(assignment_title or "").strip(),
        "collect_by": str(collect_by or "").strip(),
        "absent": _normalise_classops_names(absent),
        "submitted": _normalise_classops_names(submitted),
        "non_submitted": _normalise_classops_names(non_submitted),
        "notes": str(notes or "").strip(),
        "created_at": str(existing.get("created_at") or now) if isinstance(existing, dict) else now,
        "updated_at": now,
    }
    if isinstance(existing, dict):
        existing.clear()
        existing.update(assignment)
    else:
        assignments.append(assignment)
    if clean_source_path:
        overrides = ledger.setdefault("content_overrides", {})
        current = overrides.get(clean_source_path)
        if not isinstance(current, dict):
            current = {"title": str(current or "").strip(), "hidden": False}
        current["no_submission_needed"] = False
        overrides[clean_source_path] = current
    lesson_key = f"{assignment['lesson_date']}|{assignment['folder']}"
    lessons = record.setdefault("lessons", [])
    if not any(f"{lesson.get('date', '')}|{lesson.get('folder', '')}" == lesson_key for lesson in lessons):
        lessons.append({
            "date": assignment["lesson_date"],
            "topic": assignment["topic"],
            "folder": assignment["folder"],
            "created_at": now,
        })
    set_classops_ledger(ledger)
    return assignment


# ─── SHEETS: PROJECTS ────────────────────────────────────────────────────────
# Sheet structure: project | status | last_update | next_milestone | milestone_date | notes

def get_projects():
    r = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Projects!A2:F"
    ).execute()
    rows = r.get("values", [])
    out = []
    for row in rows:
        if not row:
            continue
        out.append({
            "project":        row[0] if len(row) > 0 else "",
            "status":         row[1] if len(row) > 1 else "",
            "last_update":    row[2] if len(row) > 2 else "",
            "next_milestone": row[3] if len(row) > 3 else "",
            "milestone_date": row[4] if len(row) > 4 else "",
            "notes":          row[5] if len(row) > 5 else "",
        })
    return out


def update_project(project: str, status: str, milestone="", milestone_date="", notes=""):
    r = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Projects!A2:A"
    ).execute()
    rows = r.get("values", [])
    today = datetime.now(SGT).strftime("%Y-%m-%d")
    new_row = [project, status, today, milestone, milestone_date, notes]

    for i, row in enumerate(rows):
        if row and row[0].lower() == project.lower():
            _sheets().spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"Projects!A{i + 2}:F{i + 2}",
                valueInputOption="USER_ENTERED",
                body={"values": [new_row]},
            ).execute()
            return
    # New project — append
    _sheets().spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="Projects!A:F",
        valueInputOption="USER_ENTERED",
        body={"values": [new_row]},
    ).execute()


# ─── SHEETS: CONFIG ──────────────────────────────────────────────────────────
# Sheet structure: key | value

def _read_config_sheet() -> tuple[dict, dict, int]:
    r = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Config!A2:B"
    ).execute()
    values = {}
    row_numbers = {}
    rows = r.get("values", [])
    for index, row in enumerate(rows):
        if row and row[0] not in values:
            values[row[0]] = row[1] if len(row) > 1 else None
            row_numbers[row[0]] = index + 2
    return values, row_numbers, len(rows)


def _config_cache_valid(now: float | None = None) -> bool:
    values = _config_cache.get("values")
    if values is None:
        return False
    if _CONFIG_CACHE_TTL_SECONDS <= 0:
        return False
    return (now if now is not None else time.monotonic()) < float(_config_cache.get("expires_at", 0.0) or 0.0)


def _load_config_cache(force: bool = False) -> tuple[dict, dict, int]:
    with _config_cache_lock:
        now = time.monotonic()
        if not force and _config_cache_valid(now):
            return (
                dict(_config_cache.get("values") or {}),
                dict(_config_cache.get("row_numbers") or {}),
                int(_config_cache.get("row_count") or 0),
            )
        try:
            values, row_numbers, row_count = _read_config_sheet()
        except Exception:
            stale_values = _config_cache.get("values")
            if stale_values is not None:
                logger.warning("Using stale Config cache after Sheets read failed.")
                _config_cache["stale_after_error"] = True
                return (
                    dict(stale_values or {}),
                    dict(_config_cache.get("row_numbers") or {}),
                    int(_config_cache.get("row_count") or 0),
                )
            raise
        _config_cache["values"] = values
        _config_cache["row_numbers"] = row_numbers
        _config_cache["row_count"] = row_count
        _config_cache["expires_at"] = time.monotonic() + _CONFIG_CACHE_TTL_SECONDS
        _config_cache["stale_after_error"] = False
        return dict(values), dict(row_numbers), row_count


def _remember_config_cache_value(key: str, value: str, row_number: int | None = None):
    if _CONFIG_CACHE_TTL_SECONDS <= 0:
        return
    with _config_cache_lock:
        values = dict(_config_cache.get("values") or {})
        row_numbers = dict(_config_cache.get("row_numbers") or {})
        values[key] = value
        if row_number is not None:
            row_numbers[key] = row_number
        _config_cache["values"] = values
        _config_cache["row_numbers"] = row_numbers
        _config_cache["row_count"] = max(int(_config_cache.get("row_count") or 0), (row_number or 2) - 1)
        _config_cache["expires_at"] = time.monotonic() + _CONFIG_CACHE_TTL_SECONDS
        _config_cache["stale_after_error"] = False


def invalidate_config_cache():
    with _config_cache_lock:
        _config_cache["expires_at"] = 0.0
        _config_cache["values"] = None
        _config_cache["row_numbers"] = None
        _config_cache["row_count"] = 0
        _config_cache["stale_after_error"] = False
        _memory_cache["expires_at"] = 0.0
        _memory_cache["value"] = None
        _memory_cache["source"] = ""


def _get_sheets_config(key: str):
    values, _, _ = _load_config_cache()
    return values.get(key)


def get_config(key: str):
    if _postgres_available():
        try:
            value = pg_storage.get_config(key)
            if value is not None:
                return value
        except Exception as exc:
            logger.warning(f"Postgres config read failed for {key}; using Sheets fallback: {exc}")
            return _get_sheets_config(key)
        value = _get_sheets_config(key)
        if value is not None:
            try:
                pg_storage.set_config(key, value)
            except Exception as exc:
                logger.warning(f"Could not backfill Postgres config key {key}: {exc}")
        return value
    return _get_sheets_config(key)


def _set_sheets_config(key: str, value: str):
    # If we already know the Config row for an existing key, write directly.
    # This avoids a fresh read-before-write during active chats, which can hit
    # Google Sheets' per-user read quota and make memory saves fail even though
    # the target row is already known from cache.
    with _config_cache_lock:
        cached_values = _config_cache.get("values")
        cached_rows = dict(_config_cache.get("row_numbers") or {})
        row_number = cached_rows.get(key) if cached_values is not None else None
    if row_number:
        _sheets().spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"Config!B{row_number}",
            valueInputOption="RAW",
            body={"values": [[value]]},
        ).execute()
        _remember_config_cache_value(key, value, row_number)
        return
    _, row_numbers, row_count = _load_config_cache()
    row_number = row_numbers.get(key)
    if row_number:
        _sheets().spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"Config!B{row_number}",
            valueInputOption="RAW",
            body={"values": [[value]]},
        ).execute()
        _remember_config_cache_value(key, value, row_number)
        return
    with _config_cache_lock:
        if _config_cache.get("stale_after_error"):
            raise RuntimeError(f"Cannot append Config key {key!r} while Sheets Config read is unavailable.")
    _sheets().spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="Config!A:B",
        valueInputOption="RAW",
        body={"values": [[key, value]]},
    ).execute()
    _remember_config_cache_value(key, value, row_count + 2)


def set_config(key: str, value: str):
    if _postgres_available():
        try:
            pg_storage.set_config(key, value)
            _remember_config_cache_value(key, value)
            return
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres config write failed for {key}: {exc}") from exc
    _set_sheets_config(key, value)


# ─── SHEETS: APP NOTIFICATIONS ───────────────────────────────────────────────
# Stored row-by-row so full briefings/digests can appear in the app.

_app_notifications_sheet_ready = False

def _ensure_app_notifications_sheet():
    global _app_notifications_sheet_ready
    if _app_notifications_sheet_ready:
        return
    try:
        _sheets().spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{APP_NOTIFICATION_SHEET}!A1:H1",
        ).execute()
        _app_notifications_sheet_ready = True
        return
    except HttpError as exc:
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status not in (400, 404):
            raise

    _sheets().spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": APP_NOTIFICATION_SHEET}}}]},
    ).execute()
    _sheets().spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{APP_NOTIFICATION_SHEET}!A1:H1",
        valueInputOption="RAW",
        body={"values": [APP_NOTIFICATION_HEADERS]},
    ).execute()
    _app_notifications_sheet_ready = True


def _legacy_config_app_notifications(include_archived=False) -> list:
    raw = get_config("app_notifications")
    if not raw:
        return []
    try:
        notifications = json.loads(raw)
    except Exception:
        return []
    clean = []
    for item in notifications:
        if not isinstance(item, dict):
            continue
        archived = bool(item.get("archived", False))
        if archived and not include_archived:
            continue
        seen_by = item.get("seen_by", [])
        clean.append({
            "id": str(item.get("id", "")).strip(),
            "kind": str(item.get("kind", "notice") or "notice").strip(),
            "title": str(item.get("title", "H.I.R.A") or "H.I.R.A").strip(),
            "body": str(item.get("body", "") or "").strip(),
            "created": str(item.get("created", "") or "").strip(),
            "source": str(item.get("source", "") or "").strip(),
            "seen_by": [str(client) for client in seen_by if str(client).strip()] if isinstance(seen_by, list) else [],
            "archived": archived,
        })
    return [item for item in clean if item["id"] and item["body"]]


def _set_legacy_config_app_notifications(notifications: list):
    set_config("app_notifications", json.dumps(notifications[-80:], ensure_ascii=False))


def _app_notification_rows() -> list[list[str]]:
    _ensure_app_notifications_sheet()
    result = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{APP_NOTIFICATION_SHEET}!A2:H",
    ).execute()
    return result.get("values", [])


def _normalise_app_notification_row(row: list[str]) -> dict | None:
    padded = [*row, *[""] * (8 - len(row))]
    nid, kind, title, body, created, source, seen_by_raw, archived_raw = padded[:8]
    try:
        seen_by = json.loads(seen_by_raw) if seen_by_raw else []
    except Exception:
        seen_by = []
    if not isinstance(seen_by, list):
        seen_by = []
    item = {
        "id": str(nid).strip(),
        "kind": str(kind or "notice").strip(),
        "title": str(title or "H.I.R.A").strip(),
        "body": str(body or "").strip(),
        "created": str(created or "").strip(),
        "source": str(source or "").strip(),
        "seen_by": [str(client) for client in seen_by if str(client).strip()],
        "archived": str(archived_raw or "").strip().upper() == "TRUE",
    }
    if not item["id"] or not item["body"]:
        return None
    return item


def _merge_app_notifications(primary: list, legacy: list) -> list:
    merged = []
    seen = set()
    for source in (primary, legacy):
        for item in source:
            nid = str(item.get("id", "")).strip() if isinstance(item, dict) else ""
            if not nid or nid in seen:
                continue
            seen.add(nid)
            merged.append(item)
    return merged


def get_app_notifications(include_archived=False) -> list:
    if _postgres_available():
        try:
            clean = [_normalise_app_notification_row([
                item.get("id", ""),
                item.get("kind", ""),
                item.get("title", ""),
                item.get("body", ""),
                item.get("created", ""),
                item.get("source", ""),
                json.dumps(item.get("seen_by", []), ensure_ascii=False),
                "TRUE" if item.get("archived") else "FALSE",
            ]) for item in pg_storage.load_app_notifications(include_archived=True)]
            clean = [item for item in clean if item]
            if clean:
                return clean if include_archived else [item for item in clean if not item["archived"]]
            if pg_storage.get_config("notification_state_initialized") == "1":
                return []
            try:
                rows = _app_notification_rows()
                sheet_clean = [_normalise_app_notification_row(row) for row in rows]
                sheet_clean = [item for item in sheet_clean if item]
            except Exception:
                sheet_clean = []
            legacy = _legacy_config_app_notifications(include_archived=True)
            merged = _merge_app_notifications(sheet_clean, legacy)
            if merged:
                pg_storage.import_app_notifications(merged)
                return merged if include_archived else [item for item in merged if not item["archived"]]
            return []
        except Exception as exc:
            logger.warning(f"Postgres app notifications unavailable; using Sheets fallback: {exc}")
    try:
        rows = _app_notification_rows()
    except Exception:
        return _legacy_config_app_notifications(include_archived=include_archived)

    clean = [_normalise_app_notification_row(row) for row in rows]
    clean = [item for item in clean if item]
    legacy = _legacy_config_app_notifications(include_archived=True)
    if legacy:
        merged = _merge_app_notifications(clean, legacy)
        if len(merged) != len(clean):
            set_app_notifications(merged)
        clean = merged
    if not include_archived:
        clean = [item for item in clean if not item["archived"]]
    return clean


def set_app_notifications(notifications: list):
    if _postgres_available():
        try:
            pg_storage.set_app_notifications(notifications[-80:])
            return
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres notification write failed: {exc}") from exc
    try:
        _ensure_app_notifications_sheet()
    except Exception:
        _set_legacy_config_app_notifications(notifications)
        return
    kept = notifications[-80:]
    values = []
    for item in kept:
        if not isinstance(item, dict):
            continue
        values.append([
            str(item.get("id", "")).strip(),
            str(item.get("kind", "notice") or "notice").strip(),
            str(item.get("title", "H.I.R.A") or "H.I.R.A").strip(),
            str(item.get("body", "") or "").strip(),
            str(item.get("created", "") or "").strip(),
            str(item.get("source", "") or "").strip(),
            json.dumps(item.get("seen_by", [])[-20:] if isinstance(item.get("seen_by", []), list) else [], ensure_ascii=False),
            "TRUE" if item.get("archived") else "FALSE",
        ])
    try:
        _sheets().spreadsheets().values().clear(
            spreadsheetId=SHEET_ID,
            range=f"{APP_NOTIFICATION_SHEET}!A2:H",
            body={},
        ).execute()
        if values:
            _sheets().spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{APP_NOTIFICATION_SHEET}!A2:H",
                valueInputOption="RAW",
                body={"values": values},
            ).execute()
    except Exception:
        _set_legacy_config_app_notifications(notifications)

def _compact_notification_body(body: str) -> str:
    clean = str(body or "").strip()
    if len(clean) <= APP_NOTIFICATION_PUSH_BODY_LIMIT:
        return clean
    return clean[: APP_NOTIFICATION_PUSH_BODY_LIMIT - 80].rstrip() + "\n\n[Open H.I.R.A for the full context.]"


def enqueue_app_notification(kind: str, title: str, body: str, source: str = "") -> dict:
    clean_kind = (kind or "notice").strip()
    clean_title = (title or "H.I.R.A").strip()
    clean_body = (body or "").strip()
    clean_source = (source or "").strip()
    if _postgres_available():
        try:
            return pg_storage.enqueue_app_notification(clean_kind, clean_title, clean_body, clean_source)
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres notification enqueue failed: {exc}") from exc
    with _storage_mutation_lock("app_notifications", _app_notifications_mutation_lock):
        notifications = get_app_notifications(include_archived=True)
        now = datetime.now(SGT).isoformat()
        if not clean_body:
            return {
                "id": "",
                "kind": clean_kind,
                "title": clean_title,
                "body": clean_body,
                "created": now,
                "source": clean_source,
                "seen_by": [],
                "archived": False,
            }
        for item in reversed(notifications):
            if item.get("archived"):
                continue
            if clean_source and str(item.get("source", "")).strip() == clean_source:
                item.update({
                    "kind": clean_kind,
                    "title": clean_title,
                    "body": clean_body,
                    "created": now,
                })
                item["_duplicate"] = True
                set_app_notifications(notifications)
                return item
            if (
                not clean_source
                and str(item.get("kind", "")).strip() == clean_kind
                and str(item.get("title", "")).strip() == clean_title
                and str(item.get("body", "")).strip() == clean_body
            ):
                item["_duplicate"] = True
                return item
        next_id = 1
        numeric_ids = [int(item["id"]) for item in notifications if str(item.get("id", "")).isdigit()]
        if numeric_ids:
            next_id = max(numeric_ids) + 1
        item = {
            "id": str(next_id),
            "kind": clean_kind,
            "title": clean_title,
            "body": clean_body,
            "created": now,
            "source": clean_source,
            "seen_by": [],
            "archived": False,
        }
        notifications.append(item)
        set_app_notifications(notifications)
        return item


def unseen_app_notifications(client_id: str, limit: int = 12) -> list:
    client_id = str(client_id or "default").strip() or "default"
    notifications = get_app_notifications(include_archived=False)
    unseen = [item for item in notifications if client_id not in item.get("seen_by", [])]
    return unseen[-max(1, int(limit or 12)):]


def mark_app_notifications_seen(client_id: str, notification_ids: list[str]) -> int:
    if _postgres_available():
        try:
            return pg_storage.mark_app_notifications_seen(client_id, notification_ids)
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres notification seen update failed: {exc}") from exc
    with _storage_mutation_lock("app_notifications", _app_notifications_mutation_lock):
        client_id = str(client_id or "default").strip() or "default"
        ids = {str(item_id) for item_id in notification_ids}
        if not ids:
            return 0
        notifications = get_app_notifications(include_archived=True)
        changed = 0
        for item in notifications:
            if str(item.get("id")) not in ids:
                continue
            seen_by = item.get("seen_by") if isinstance(item.get("seen_by"), list) else []
            if client_id not in seen_by:
                seen_by.append(client_id)
                item["seen_by"] = seen_by[-20:]
                changed += 1
        if changed:
            set_app_notifications(notifications)
        return changed


def archive_app_notifications(notification_ids: list[str]) -> int:
    if _postgres_available():
        try:
            return pg_storage.archive_app_notifications(notification_ids)
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres notification archive failed: {exc}") from exc
    with _storage_mutation_lock("app_notifications", _app_notifications_mutation_lock):
        ids = {str(item_id) for item_id in notification_ids}
        if not ids:
            return 0
        notifications = get_app_notifications(include_archived=True)
        changed = 0
        for item in notifications:
            if str(item.get("id")) in ids and not item.get("archived"):
                item["archived"] = True
                changed += 1
        if changed:
            set_app_notifications(notifications)
        return changed


def get_app_notification(notification_id: str) -> dict | None:
    target = str(notification_id or "").strip()
    if not target:
        return None
    for item in get_app_notifications(include_archived=True):
        if str(item.get("id", "")).strip() == target:
            return item
    return None


def _notification_outcome_group(source: str, kind: str = "") -> str:
    clean_source = str(source or "").strip()
    if clean_source:
        return clean_source.split(":", 1)[0]
    return str(kind or "notice").strip() or "notice"


def get_notification_outcomes() -> list:
    raw = get_config("notification_outcomes")
    if not raw:
        return []
    try:
        entries = json.loads(raw)
    except Exception:
        return []
    if not isinstance(entries, list):
        return []
    clean = []
    for item in entries[-400:]:
        if not isinstance(item, dict):
            continue
        clean.append({
            "created": str(item.get("created", "")).strip(),
            "notification_id": str(item.get("notification_id", "")).strip(),
            "source": str(item.get("source", "")).strip(),
            "group": str(item.get("group", "")).strip(),
            "kind": str(item.get("kind", "")).strip(),
            "action": str(item.get("action", "")).strip(),
            "rating": str(item.get("rating", "")).strip(),
            "client_id": str(item.get("client_id", "")).strip(),
            "title": str(item.get("title", "")).strip(),
        })
    return clean


def set_notification_outcomes(entries: list):
    kept = list(entries[-160:])
    payload = json.dumps(kept, ensure_ascii=False)
    while kept and len(payload) > 45000:
        kept = kept[20:]
        payload = json.dumps(kept, ensure_ascii=False)
    set_config("notification_outcomes", payload)


def add_notification_outcome(
    action: str,
    notification_id: str = "",
    source: str = "",
    kind: str = "",
    rating: str = "",
    client_id: str = "",
    title: str = "",
) -> list:
    entries = get_notification_outcomes()
    item = {
        "created": datetime.now(SGT).isoformat(),
        "notification_id": str(notification_id or "").strip()[:80],
        "source": str(source or "").strip()[:180],
        "group": _notification_outcome_group(source, kind)[:80],
        "kind": str(kind or "").strip()[:40],
        "action": str(action or "").strip()[:40],
        "rating": str(rating or "").strip()[:40],
        "client_id": str(client_id or "").strip()[:80],
        "title": str(title or "").strip()[:160],
    }
    if not item["action"]:
        return entries
    entries.append(item)
    set_notification_outcomes(entries)
    return entries


def get_notification_outcome_summary(days: int = 14) -> dict:
    current = datetime.now(SGT)
    threshold = current - timedelta(days=max(1, int(days or 14)))
    summary = {
        "actions": {},
        "groups": {},
        "sources": {},
        "recent": [],
    }
    for item in get_notification_outcomes():
        try:
            created = datetime.fromisoformat(item.get("created", ""))
        except Exception:
            continue
        if created < threshold:
            continue
        action = item.get("action", "") or "unknown"
        group = item.get("group", "") or _notification_outcome_group(item.get("source", ""), item.get("kind", ""))
        source = item.get("source", "") or group
        summary["actions"][action] = summary["actions"].get(action, 0) + 1
        group_bucket = summary["groups"].setdefault(group, {"count": 0, "negative": 0, "positive": 0})
        group_bucket["count"] += 1
        if action in {"dismissed", "not_now", "not_useful"}:
            group_bucket["negative"] += 1
        if action == "useful":
            group_bucket["positive"] += 1
        source_bucket = summary["sources"].setdefault(source, {"count": 0, "negative": 0, "positive": 0})
        source_bucket["count"] += 1
        if action in {"dismissed", "not_now", "not_useful"}:
            source_bucket["negative"] += 1
        if action == "useful":
            source_bucket["positive"] += 1
        summary["recent"].append(item)
    summary["recent"] = summary["recent"][-30:]
    return summary


def get_insight_feedback() -> list:
    raw = get_config("insight_feedback")
    if not raw:
        return []
    try:
        feedback = json.loads(raw)
        return feedback if isinstance(feedback, list) else []
    except Exception:
        return []


def add_insight_feedback(kind: str, target: str, rating: str, note: str = "") -> list:
    item = {
        "created": datetime.now(SGT).isoformat(),
        "kind": str(kind or "insight").strip(),
        "target": str(target or "").strip()[:240],
        "rating": str(rating or "").strip()[:40],
        "note": str(note or "").strip()[:500],
    }
    feedback = get_insight_feedback()
    feedback.append(item)
    feedback = feedback[-120:]
    set_config("insight_feedback", json.dumps(feedback, ensure_ascii=False))
    return feedback


def _clean_action_ledger_item(item: dict) -> dict:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    return {
        "id": str(item.get("id", "")).strip(),
        "created": str(item.get("created", "")).strip(),
        "action": str(item.get("action", "")).strip()[:80],
        "status": str(item.get("status", "")).strip()[:40],
        "subject": str(item.get("subject", "")).strip()[:240],
        "date": str(item.get("date", "")).strip()[:120],
        "result": str(item.get("result", "")).strip()[:900],
        "source": str(item.get("source", "")).strip()[:80],
        "client_id": str(item.get("client_id", "")).strip()[:120],
        "undo_status": str(item.get("undo_status", "")).strip()[:40],
        "undo_result": str(item.get("undo_result", "")).strip()[:500],
        "reviewed": bool(item.get("reviewed", False)),
        "metadata": {
            str(key)[:80]: str(value).strip()[:240]
            for key, value in metadata.items()
            if str(key).strip() and str(value).strip()
        },
    }


def get_action_ledger(include_reviewed: bool = True) -> list[dict]:
    raw = get_config("action_ledger")
    if not raw:
        return []
    try:
        entries = json.loads(raw)
    except Exception:
        return []
    if not isinstance(entries, list):
        return []
    clean = [_clean_action_ledger_item(item) for item in entries[-250:] if isinstance(item, dict)]
    clean = [item for item in clean if item["id"] and item["action"]]
    if not include_reviewed:
        clean = [item for item in clean if not item.get("reviewed")]
    return clean


def set_action_ledger(entries: list[dict]):
    clean = [_clean_action_ledger_item(item) for item in entries if isinstance(item, dict)]
    set_config("action_ledger", json.dumps(clean[-250:], ensure_ascii=False))


def add_action_ledger(
    action: str,
    status: str,
    subject: str = "",
    date: str = "",
    result: str = "",
    source: str = "assistant",
    client_id: str = "",
    metadata: dict | None = None,
) -> dict:
    entries = get_action_ledger(include_reviewed=True)
    existing_ids = [int(item["id"]) for item in entries if str(item.get("id", "")).isdigit()]
    entry = _clean_action_ledger_item({
        "id": str((max(existing_ids) + 1) if existing_ids else 1),
        "created": datetime.now(SGT).isoformat(),
        "action": action,
        "status": status,
        "subject": subject,
        "date": date,
        "result": result,
        "source": source,
        "client_id": client_id,
        "metadata": metadata or {},
        "undo_status": "",
        "undo_result": "",
        "reviewed": False,
    })
    entries.append(entry)
    set_action_ledger(entries)
    return entry


def update_action_ledger_entry(entry_id: str, updates: dict) -> dict | None:
    entries = get_action_ledger(include_reviewed=True)
    changed = None
    for item in entries:
        if str(item.get("id", "")) != str(entry_id):
            continue
        item.update(updates or {})
        changed = _clean_action_ledger_item(item)
        item.clear()
        item.update(changed)
        break
    if changed:
        set_action_ledger(entries)
    return changed


DEFAULT_TASTE_PROFILE = {
    "sources_to_trust": [],
    "sources_to_avoid": [],
    "quality_bar": "",
    "preferred_depth": "",
    "design_taste": "",
    "business_lens": "",
    "islamic_content_tone": "",
    "conversation_directness": "",
    "conversation_humour": "",
    "conversation_judgement": "",
    "conversation_briefness": "",
    "conversation_followthrough": "",
}


def get_taste_profile() -> dict:
    raw = get_config("taste_profile")
    profile = dict(DEFAULT_TASTE_PROFILE)
    if not raw:
        return profile
    try:
        stored = json.loads(raw)
    except Exception:
        return profile
    if isinstance(stored, dict):
        for key in profile:
            value = stored.get(key)
            if isinstance(profile[key], list):
                profile[key] = [str(item).strip() for item in value if str(item).strip()] if isinstance(value, list) else []
            else:
                profile[key] = str(value or "").strip()
    return profile


def set_taste_profile(profile: dict) -> dict:
    clean = dict(DEFAULT_TASTE_PROFILE)
    for key in clean:
        value = profile.get(key) if isinstance(profile, dict) else None
        if isinstance(clean[key], list):
            if isinstance(value, str):
                clean[key] = [part.strip() for part in value.split(",") if part.strip()]
            elif isinstance(value, list):
                clean[key] = [str(item).strip() for item in value if str(item).strip()]
        else:
            clean[key] = str(value or "").strip()[:1000]
    set_config("taste_profile", json.dumps(clean, ensure_ascii=False))
    return clean


def _redis_json_get(key: str, default):
    r = _get_redis()
    if not r:
        return default
    try:
        raw = r.get(key)
        if not raw:
            return default
        return json.loads(raw)
    except Exception as exc:
        logger.warning(f"Could not read Redis fallback {key}: {exc}")
        return default


def _redis_json_set(key: str, value, ttl_seconds: int = 60 * 60 * 24 * 14) -> bool:
    r = _get_redis()
    if not r:
        return False
    try:
        r.set(key, json.dumps(value, ensure_ascii=False), ex=ttl_seconds)
        return True
    except Exception as exc:
        logger.warning(f"Could not write Redis fallback {key}: {exc}")
        return False


def _clean_web_push_subscriptions(subscriptions) -> list:
    clean = []
    seen = set()
    for item in subscriptions or []:
        if not isinstance(item, dict):
            continue
        client_id = str(item.get("client_id", "")).strip()
        subscription = item.get("subscription")
        endpoint = subscription.get("endpoint") if isinstance(subscription, dict) else ""
        if not client_id or not endpoint or endpoint in seen:
            continue
        seen.add(endpoint)
        clean.append({
            "client_id": client_id,
            "subscription": subscription,
            "created": str(item.get("created", "")).strip(),
            "last_seen": str(item.get("last_seen", "")).strip(),
            "display_mode": str(item.get("display_mode", "")).strip() or "unknown",
            "app_version": str(item.get("app_version", "")).strip(),
            "user_agent": str(item.get("user_agent", "")).strip()[:180],
        })
    return clean


def _clean_web_push_delivery_log(entries) -> list:
    clean = []
    for item in (entries or [])[-80:]:
        if not isinstance(item, dict):
            continue
        clean.append({
            "created": str(item.get("created", "")).strip(),
            "source": str(item.get("source", "")).strip(),
            "kind": str(item.get("kind", "")).strip(),
            "title": str(item.get("title", "")).strip(),
            "attempted": int(item.get("attempted", 0) or 0),
            "sent": int(item.get("sent", 0) or 0),
            "expired": int(item.get("expired", 0) or 0),
            "errors": item.get("errors", {}) if isinstance(item.get("errors"), dict) else {},
            "last_error": str(item.get("last_error", "")).strip()[:300],
            "payload_bytes": int(item.get("payload_bytes", 0) or 0),
        })
    return clean


def get_web_push_subscriptions() -> list:
    if _postgres_available():
        try:
            clean = _clean_web_push_subscriptions(pg_storage.load_web_push_subscriptions())
            if clean:
                _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, clean)
                return clean
            if pg_storage.get_config("web_push_subscriptions_initialized") == "1":
                return []
            raw = _get_sheets_config("web_push_subscriptions")
            if raw:
                try:
                    clean = _clean_web_push_subscriptions(json.loads(raw))
                except Exception:
                    clean = []
                if clean:
                    pg_storage.import_web_push_subscriptions(clean[-30:])
                    _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, clean[-30:])
                    return clean[-30:]
            return _clean_web_push_subscriptions(_redis_json_get(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, []))
        except Exception as exc:
            fallback = _clean_web_push_subscriptions(_redis_json_get(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, []))
            if fallback:
                logger.warning(f"Using Redis web push subscriptions after Postgres read failed: {exc}")
                return fallback
            logger.warning(f"Postgres web push subscriptions unavailable; using Sheets fallback: {exc}")
    try:
        raw = get_config("web_push_subscriptions")
    except Exception as exc:
        fallback = _clean_web_push_subscriptions(_redis_json_get(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, []))
        if fallback:
            logger.warning(f"Using Redis web push subscriptions after Sheets read failed: {exc}")
            return fallback
        raise
    if not raw:
        return _clean_web_push_subscriptions(_redis_json_get(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, []))
    try:
        subscriptions = json.loads(raw)
    except Exception:
        subscriptions = []
    clean = _clean_web_push_subscriptions(subscriptions)
    if clean:
        _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, clean)
    return clean


def get_web_push_subscription(client_id: str) -> dict | None:
    target = str(client_id or "").strip()
    if not target:
        return None
    for item in get_web_push_subscriptions():
        if item.get("client_id") == target:
            return item
    return None


def set_web_push_subscriptions(subscriptions: list):
    clean = _clean_web_push_subscriptions(subscriptions)[-30:]
    _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, clean)
    if _postgres_available():
        try:
            pg_storage.set_web_push_subscriptions(clean)
            return
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres web push subscription write failed: {exc}") from exc
    set_config("web_push_subscriptions", json.dumps(clean, ensure_ascii=False))


def save_web_push_subscription(client_id: str, subscription: dict, metadata: dict | None = None) -> bool:
    client_id = str(client_id or "").strip()
    endpoint = subscription.get("endpoint") if isinstance(subscription, dict) else ""
    if not client_id or not endpoint:
        return False
    metadata = metadata or {}
    display_mode = str(metadata.get("display_mode", "") or "").strip() or "unknown"
    app_version = str(metadata.get("app_version", "") or "").strip()
    user_agent = str(metadata.get("user_agent", "") or "").strip()[:180]
    now = datetime.now(SGT).isoformat()
    new_item = {
        "client_id": client_id,
        "subscription": subscription,
        "created": now,
        "last_seen": now,
        "display_mode": display_mode,
        "app_version": app_version,
        "user_agent": user_agent,
    }
    if _postgres_available():
        try:
            pg_storage.upsert_web_push_subscription(new_item)
            clean = get_web_push_subscriptions()
            if clean:
                _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, clean)
            return True
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres web push subscription write failed: {exc}") from exc
    subscriptions = get_web_push_subscriptions()
    updated = False
    for item in subscriptions:
        if item["subscription"].get("endpoint") == endpoint or item["client_id"] == client_id:
            item["client_id"] = client_id
            item["subscription"] = subscription
            item["last_seen"] = now
            item["display_mode"] = display_mode
            item["app_version"] = app_version
            item["user_agent"] = user_agent
            updated = True
            break
    if not updated:
        subscriptions.append({
            "client_id": client_id,
            "subscription": subscription,
            "created": now,
            "last_seen": now,
            "display_mode": display_mode,
            "app_version": app_version,
            "user_agent": user_agent,
        })
    set_web_push_subscriptions(subscriptions)
    return True


def _preferred_web_push_subscriptions(subscriptions: list) -> list:
    standalone = [
        item for item in subscriptions
        if str(item.get("display_mode", "")).strip().lower() in {"standalone", "fullscreen"}
    ]
    if not standalone:
        return subscriptions
    cutoff = datetime.now(SGT) - timedelta(days=7)

    def recently_seen(item: dict) -> bool:
        raw = str(item.get("last_seen", "") or "").strip()
        if not raw:
            return True
        try:
            seen = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if seen.tzinfo is None:
                seen = SGT.localize(seen) if hasattr(SGT, "localize") else seen.replace(tzinfo=SGT)
            return seen.astimezone(SGT) >= cutoff
        except Exception:
            return True

    fresh_standalone = [item for item in standalone if recently_seen(item)]
    if fresh_standalone:
        return fresh_standalone
    fresh_any = [item for item in subscriptions if recently_seen(item)]
    return fresh_any or subscriptions


def get_web_push_delivery_log() -> list:
    if _postgres_available():
        try:
            clean = _clean_web_push_delivery_log(pg_storage.load_web_push_delivery_log(limit=80))
            if clean:
                _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, clean)
                return clean
            if pg_storage.get_config("web_push_delivery_log_initialized") == "1":
                return []
            raw = _get_sheets_config("web_push_delivery_log")
            if raw:
                try:
                    clean = _clean_web_push_delivery_log(json.loads(raw))
                except Exception:
                    clean = []
                if clean:
                    pg_storage.import_web_push_delivery_log(clean)
                    _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, clean)
                    return clean
            return _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
        except Exception as exc:
            fallback = _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
            if fallback:
                logger.warning(f"Using Redis web push delivery log after Postgres read failed: {exc}")
                return fallback
            logger.warning(f"Postgres web push delivery log unavailable; using Sheets fallback: {exc}")
    try:
        raw = get_config("web_push_delivery_log")
    except Exception as exc:
        fallback = _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
        if fallback:
            logger.warning(f"Using Redis web push delivery log after Sheets read failed: {exc}")
            return fallback
        raise
    if not raw:
        return _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
    try:
        entries = json.loads(raw)
    except Exception:
        entries = []
    if not isinstance(entries, list):
        entries = []
    clean = _clean_web_push_delivery_log(entries)
    if clean:
        _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, clean)
    return clean


def set_web_push_delivery_log(entries: list):
    clean = _clean_web_push_delivery_log(entries)
    _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, clean)
    if _postgres_available():
        try:
            pg_storage.set_web_push_delivery_log(clean[-80:])
            return
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres web push delivery write failed: {exc}") from exc
    set_config("web_push_delivery_log", json.dumps(clean[-80:], ensure_ascii=False))


def add_web_push_delivery_log(
    source: str,
    kind: str,
    title: str,
    attempted: int,
    sent: int,
    expired: int = 0,
    errors: dict | None = None,
    last_error: str = "",
    payload_bytes: int = 0,
) -> list:
    entry = {
        "created": datetime.now(SGT).isoformat(),
        "source": str(source or "").strip()[:240],
        "kind": str(kind or "").strip()[:40],
        "title": str(title or "").strip()[:240],
        "attempted": int(attempted or 0),
        "sent": int(sent or 0),
        "expired": int(expired or 0),
        "errors": errors or {},
        "last_error": str(last_error or "").strip()[:300],
        "payload_bytes": int(payload_bytes or 0),
    }
    if _postgres_available():
        try:
            pg_storage.append_web_push_delivery_log(entry)
            entries = _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
            entries.append(entry)
            _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, _clean_web_push_delivery_log(entries))
            return get_web_push_delivery_log()
        except Exception as exc:
            logger.warning(f"Web push delivery log persisted to Redis only after Postgres append failed: {exc}")
            entries = _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
            entries.append(entry)
            _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, _clean_web_push_delivery_log(entries))
            return entries
    try:
        entries = get_web_push_delivery_log()
    except Exception as exc:
        logger.warning(f"Using Redis-only web push delivery log after Sheets read failed: {exc}")
        entries = _clean_web_push_delivery_log(_redis_json_get(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, []))
    entries.append(entry)
    try:
        set_web_push_delivery_log(entries)
    except Exception as exc:
        logger.warning(f"Web push delivery log persisted to Redis only after Sheets write failed: {exc}")
        _redis_json_set(_REDIS_WEB_PUSH_DELIVERY_LOG_KEY, _clean_web_push_delivery_log(entries))
    return entries


def _web_push_error_label(exc: Exception) -> str:
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    if status_code:
        return f"http_{status_code}"
    return exc.__class__.__name__


def _web_push_error_detail(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    text = str(getattr(response, "text", "") or "").strip()
    if text:
        text = re.sub(r"\s+", " ", text)
        return f"{status_code}: {text}"[:300] if status_code else text[:300]
    return str(exc or exc.__class__.__name__).strip()[:300]


def send_web_push_notification(title: str, body: str, data: dict | None = None) -> int:
    private_key = os.environ.get("HIRA_WEB_PUSH_PRIVATE_KEY", "").strip()
    subject = os.environ.get("HIRA_WEB_PUSH_SUBJECT", "mailto:hira@example.com").strip()
    payload_data = data or {}
    if not private_key:
        add_web_push_delivery_log(
            source=str(payload_data.get("source", "")).strip(),
            kind=str(payload_data.get("kind", "")).strip(),
            title=title,
            attempted=0,
            sent=0,
            errors={"missing_private_key": 1},
            last_error="HIRA_WEB_PUSH_PRIVATE_KEY is not set for this service.",
        )
        return 0
    try:
        from pywebpush import WebPushException, webpush
    except Exception as exc:
        add_web_push_delivery_log(
            source=str(payload_data.get("source", "")).strip(),
            kind=str(payload_data.get("kind", "")).strip(),
            title=title,
            attempted=0,
            sent=0,
            errors={"pywebpush_import_failed": 1},
            last_error=str(exc or "pywebpush import failed")[:300],
        )
        return 0

    payload = json.dumps({
        "title": title or "H.I.R.A",
        "body": _compact_notification_body(body),
        "icon": "/static/icon.svg",
        "badge": "/static/icon.svg",
        "data": payload_data,
    }, ensure_ascii=False)
    payload_bytes = len(payload.encode("utf-8"))

    key_file = None
    key_for_webpush = private_key
    try:
        if "BEGIN" in private_key:
            key_file = tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False)
            key_file.write(private_key.replace("\\n", "\n"))
            key_file.close()
            key_for_webpush = key_file.name
        else:
            decoded = base64.b64decode(private_key)
            if decoded.startswith(b"-----BEGIN"):
                key_file = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False)
                key_file.write(decoded)
                key_file.close()
                key_for_webpush = key_file.name
    except Exception:
        key_for_webpush = private_key

    sent = 0
    all_subscriptions = get_web_push_subscriptions()
    subscriptions = _preferred_web_push_subscriptions(all_subscriptions)
    expired_client_ids = []
    expired_endpoints = []
    expired = 0
    errors = {}
    last_error = ""
    try:
        for item in subscriptions:
            try:
                webpush(
                    subscription_info=item["subscription"],
                    data=payload,
                    vapid_private_key=key_for_webpush,
                    vapid_claims={"sub": subject},
                )
                sent += 1
            except WebPushException as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                label = _web_push_error_label(exc)
                errors[label] = errors.get(label, 0) + 1
                last_error = _web_push_error_detail(exc) or last_error
                logger.warning(
                    "Web push delivery failed for client_id=%s status=%s error=%s",
                    item.get("client_id", ""),
                    status_code or "",
                    last_error,
                )
                if status_code in (404, 410):
                    expired += 1
                    expired_client_ids.append(str(item.get("client_id", "") or ""))
                    expired_endpoints.append(str((item.get("subscription") or {}).get("endpoint", "") or ""))
            except Exception as exc:
                label = _web_push_error_label(exc)
                errors[label] = errors.get(label, 0) + 1
                last_error = _web_push_error_detail(exc) or last_error
                logger.warning(
                    "Web push delivery error for client_id=%s error=%s",
                    item.get("client_id", ""),
                    last_error,
                )
    finally:
        if key_file:
            try:
                os.unlink(key_file.name)
            except Exception:
                pass
    if expired_client_ids or expired_endpoints:
        if _postgres_available():
            try:
                pg_storage.delete_web_push_subscriptions(expired_client_ids, expired_endpoints)
                clean = get_web_push_subscriptions()
                _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, clean)
            except Exception as exc:
                logger.warning(f"Expired web push subscriptions persisted to Redis only after Postgres delete failed: {exc}")
        expired_endpoint_set = {endpoint for endpoint in expired_endpoints if endpoint}
        expired_client_set = {client_id for client_id in expired_client_ids if client_id}
        remaining = [
            item for item in all_subscriptions
            if str(item.get("client_id", "") or "") not in expired_client_set
            and str((item.get("subscription") or {}).get("endpoint", "") or "") not in expired_endpoint_set
        ]
        if not _postgres_available():
            set_web_push_subscriptions(remaining)
        else:
            _redis_json_set(_REDIS_WEB_PUSH_SUBSCRIPTIONS_KEY, _clean_web_push_subscriptions(remaining))
    add_web_push_delivery_log(
        source=str(payload_data.get("source", "")).strip(),
        kind=str(payload_data.get("kind", "")).strip(),
        title=title,
        attempted=len(subscriptions),
        sent=sent,
        expired=expired,
        errors=errors,
        last_error=last_error,
        payload_bytes=payload_bytes,
    )
    return sent


# ─── SHEETS: ASSISTANT MEMORY ────────────────────────────────────────────────
# Stored in Config as one JSON blob to avoid requiring another sheet tab.

DEFAULT_MEMORY = {
    "profile": [],
    "preferences": [],
    "people": [],
    "places": [],
    "teaching": [],
    "business": [],
    "projects": [],
    "sports": [],
    "files": [],
    "templates": [],
    "constraints": [],
    "recent_summaries": [],
    "conversation_episodes": [],
    "topic_profiles": [],
    "correction_ledger": [],
    "self_reflections": [],
    "source_notes": [],
}


def _normalise_memory_category(category: str) -> str:
    category = (category or "profile").lower().strip()
    aliases = {
        "preference": "preferences",
        "person": "people",
        "place": "places",
        "school": "teaching",
        "classes": "teaching",
        "class": "teaching",
        "business": "business",
        "company": "business",
        "project": "projects",
        "sport": "sports",
        "sports": "sports",
        "football": "sports",
        "f1": "sports",
        "formula 1": "sports",
        "liverpool": "sports",
        "lfc": "sports",
        "file": "files",
        "document": "files",
        "attachment": "files",
        "upload": "files",
        "template": "templates",
        "style": "templates",
        "constraint": "constraints",
        "rule": "constraints",
        "summary": "recent_summaries",
        "recent": "recent_summaries",
        "conversation": "conversation_episodes",
        "conversations": "conversation_episodes",
        "episode": "conversation_episodes",
        "episodes": "conversation_episodes",
        "topic": "topic_profiles",
        "topic_profile": "topic_profiles",
        "interest": "topic_profiles",
        "interests": "topic_profiles",
        "correction": "correction_ledger",
        "corrections": "correction_ledger",
        "mistake": "correction_ledger",
        "mistakes": "correction_ledger",
        "reflection": "self_reflections",
        "reflections": "self_reflections",
        "learning": "self_reflections",
        "source": "source_notes",
        "sources": "source_notes",
        "source_note": "source_notes",
        "source_notes": "source_notes",
        "knowledge": "source_notes",
    }
    category = aliases.get(category, category)
    return category if category in DEFAULT_MEMORY else "profile"


def _copy_memory(memory: dict) -> dict:
    return {key: list(memory.get(key, [])) for key in DEFAULT_MEMORY}


def _postgres_available() -> bool:
    return bool(pg_storage and pg_storage.enabled())


def memory_storage_status() -> dict:
    if pg_storage and pg_storage.enabled():
        return pg_storage.storage_status()
    if google_sheets_configured():
        return {"enabled": False, "connected": True, "source": "sheets"}
    return {"enabled": False, "connected": False, "source": "unavailable"}


def _memory_cache_valid(now: float | None = None, source: str = "") -> bool:
    if _MEMORY_CACHE_TTL_SECONDS <= 0:
        return False
    if _memory_cache.get("value") is None:
        return False
    if source and _memory_cache.get("source") != source:
        return False
    return (now if now is not None else time.monotonic()) < float(_memory_cache.get("expires_at", 0.0) or 0.0)


def _remember_memory_cache(memory: dict, source: str = ""):
    if _MEMORY_CACHE_TTL_SECONDS <= 0:
        return
    with _config_cache_lock:
        _memory_cache["value"] = _copy_memory(memory)
        _memory_cache["expires_at"] = time.monotonic() + _MEMORY_CACHE_TTL_SECONDS
        _memory_cache["source"] = source


def _merge_memory_dict(base: dict, extra: dict) -> dict:
    merged = _copy_memory(base)
    for category in DEFAULT_MEMORY:
        for item in extra.get(category, []) if isinstance(extra, dict) else []:
            text = str(item).strip()
            if text and text not in merged[category]:
                merged[category].append(text)
    return merged


def _redis_memory_fallback() -> dict:
    raw = _redis_json_get(_REDIS_ASSISTANT_MEMORY_KEY, {})
    if not isinstance(raw, dict):
        raw = {}
    memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
    for key, value in raw.items():
        category = _normalise_memory_category(key)
        if isinstance(value, list):
            memory[category].extend(str(item).strip() for item in value if str(item).strip())
    return memory


def _set_redis_memory_fallback(memory: dict) -> bool:
    clean = _copy_memory(memory)
    return _redis_json_set(_REDIS_ASSISTANT_MEMORY_KEY, clean, ttl_seconds=60 * 60 * 24 * 365)


def _is_sheets_read_quota_error(exc: Exception) -> bool:
    text = str(exc)
    return (
        "RATE_LIMIT_EXCEEDED" in text
        or ("Quota exceeded" in text and "Read requests" in text)
        or "ReadRequestsPerMinutePerUser" in text
        or "readrequests" in text.lower()
    )


def _is_sheets_access_error(exc: Exception) -> bool:
    text = str(exc)
    return (
        _is_sheets_read_quota_error(exc)
        or "The caller does not have permission" in text
        or "does not have permission" in text
        or "PERMISSION_DENIED" in text
        or "HttpError 403" in text
        or "returned \"The caller does not have permission\"" in text
    )


def _ensure_memory_log_sheet():
    try:
        _sheets().spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": MEMORY_LOG_SHEET}}}]},
        ).execute()
    except Exception as exc:
        if "already exists" not in str(exc).lower():
            raise
    _sheets().spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{MEMORY_LOG_SHEET}!A1:D1",
        valueInputOption="RAW",
        body={"values": [MEMORY_LOG_HEADERS]},
    ).execute()


def _append_memory_log(category: str, text: str, source: str = "fallback") -> None:
    row = [datetime.now(SGT).strftime("%Y-%m-%d %H:%M:%S %Z"), category, text, source]
    try:
        _sheets().spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{MEMORY_LOG_SHEET}!A:D",
            valueInputOption="RAW",
            body={"values": [row]},
        ).execute()
    except Exception as exc:
        if "Unable to parse range" not in str(exc) and "not found" not in str(exc).lower():
            raise
        _ensure_memory_log_sheet()
        _sheets().spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{MEMORY_LOG_SHEET}!A:D",
            valueInputOption="RAW",
            body={"values": [row]},
        ).execute()


def _merge_memory_log(memory: dict) -> dict:
    try:
        r = _sheets().spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{MEMORY_LOG_SHEET}!A2:D",
        ).execute()
    except Exception as exc:
        logger.debug(f"MemoryLog merge skipped: {exc}")
        return memory
    for row in r.get("values", []) or []:
        if len(row) < 3:
            continue
        category = _normalise_memory_category(row[1])
        item = str(row[2] or "").strip()
        if item and item not in memory[category]:
            memory[category].append(item)
    return memory


def _get_sheets_memory() -> dict:
    with _config_cache_lock:
        if _memory_cache_valid(source="sheets"):
            return _copy_memory(_memory_cache.get("value") or {})
    try:
        raw = get_config("assistant_memory")
    except Exception as exc:
        fallback = _redis_memory_fallback()
        if any(fallback.get(category) for category in DEFAULT_MEMORY):
            logger.warning(f"Using Redis assistant memory after Sheets read failed: {exc}")
            _remember_memory_cache(fallback, source="redis_fallback")
            return fallback
        raise
    if not raw:
        memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
        memory = _merge_memory_log(memory)
        memory = _merge_memory_dict(memory, _redis_memory_fallback())
        _remember_memory_cache(memory, source="sheets")
        return memory
    try:
        data = json.loads(raw)
    except Exception:
        memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
        memory = _merge_memory_log(memory)
        memory = _merge_memory_dict(memory, _redis_memory_fallback())
        _remember_memory_cache(memory, source="sheets")
        return memory

    memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
    for key, value in data.items():
        if isinstance(value, list):
            memory[key] = [str(item) for item in value if str(item).strip()]
    memory = _merge_memory_log(memory)
    memory = _merge_memory_dict(memory, _redis_memory_fallback())
    _remember_memory_cache(memory, source="sheets")
    return memory


def get_memory() -> dict:
    if _postgres_available():
        with _config_cache_lock:
            if _memory_cache_valid(source="postgres"):
                return _copy_memory(_memory_cache.get("value") or {})
        try:
            memory, initialized = pg_storage.load_memory(DEFAULT_MEMORY)
            if initialized:
                _remember_memory_cache(memory, source="postgres")
                return memory
            sheets_memory = _get_sheets_memory()
            pg_storage.import_memory(sheets_memory, DEFAULT_MEMORY, source="sheets_auto_import")
            imported_memory, _ = pg_storage.load_memory(DEFAULT_MEMORY)
            _remember_memory_cache(imported_memory, source="postgres")
            return imported_memory
        except Exception as exc:
            logger.warning(f"Postgres memory unavailable; using Sheets/Redis fallback: {exc}")
    return _get_sheets_memory()


def import_sheet_memory_to_postgres() -> dict:
    if not _postgres_available():
        raise RuntimeError("Postgres storage is not enabled. Set DATABASE_URL first.")
    memory = _get_sheets_memory()
    result = pg_storage.import_memory(memory, DEFAULT_MEMORY, source="sheets_manual_import")
    imported_memory, _ = pg_storage.load_memory(DEFAULT_MEMORY)
    _remember_memory_cache(imported_memory, source="postgres")
    return result


def set_memory(memory: dict):
    clean = {}
    for key in DEFAULT_MEMORY:
        values = memory.get(key, [])
        clean[key] = [str(item).strip() for item in values if str(item).strip()]
    if _postgres_available():
        try:
            saved = pg_storage.set_memory(clean, DEFAULT_MEMORY)
            _set_redis_memory_fallback(saved)
            _remember_memory_cache(saved, source="postgres")
            return
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres memory write failed: {exc}") from exc
    redis_saved = _set_redis_memory_fallback(clean)
    try:
        set_config("assistant_memory", json.dumps(clean, ensure_ascii=False))
    except Exception as exc:
        if redis_saved and _is_sheets_access_error(exc):
            logger.warning(f"Assistant memory persisted to Redis fallback after Sheets write failed: {exc}")
        else:
            raise
    _remember_memory_cache(clean, source="sheets")


def add_memory(category: str, text: str) -> dict:
    item = text.strip()
    category = _normalise_memory_category(category)
    if _postgres_available():
        try:
            memory = pg_storage.add_memory(category, item, DEFAULT_MEMORY)
            _set_redis_memory_fallback(memory)
            _remember_memory_cache(memory, source="postgres")
            return memory
        except Exception as exc:
            raise RuntimeError(f"storage unavailable: Postgres memory write failed: {exc}") from exc
    try:
        memory = get_memory()
    except Exception as exc:
        if not item or not _is_sheets_read_quota_error(exc):
            raise
        try:
            _append_memory_log(category, item, source="read_quota_fallback")
        except Exception as append_exc:
            raise RuntimeError(f"{exc}; MemoryLog fallback also failed: {append_exc}") from exc
        with _config_cache_lock:
            cached = _copy_memory(_memory_cache.get("value") or {})
        memory = cached or {k: list(v) for k, v in DEFAULT_MEMORY.items()}
        if item not in memory[category]:
            memory[category].append(item)
        _remember_memory_cache(memory, source="sheets")
        return memory
    if item and item not in memory[category]:
        memory[category].append(item)
    set_memory(memory)
    return memory


def _append_memory_json(category: str, payload: dict, limit: int = 80) -> dict:
    memory = get_memory()
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    bucket = memory.get(category, [])
    if encoded not in bucket:
        bucket.append(encoded)
    memory[category] = bucket[-limit:]
    set_memory(memory)
    return payload


def add_correction(entry: dict) -> dict:
    clean = {
        "date": str(entry.get("date", "")).strip(),
        "source": str(entry.get("source", "")).strip(),
        "correction": str(entry.get("correction", "")).strip(),
        "assistant_response": str(entry.get("assistant_response", "")).strip(),
        "priority": str(entry.get("priority", "") or "high").strip(),
    }
    if not clean["correction"]:
        raise ValueError("Correction entry needs correction text")
    return _append_memory_json("correction_ledger", clean, limit=80)


def add_self_reflection(entry: dict) -> dict:
    clean = {
        "date": str(entry.get("date", "")).strip(),
        "source": str(entry.get("source", "")).strip(),
        "trigger": str(entry.get("trigger", "")).strip(),
        "learned": str(entry.get("learned", "")).strip(),
        "next_behavior": str(entry.get("next_behavior", "")).strip(),
    }
    if not clean["learned"]:
        raise ValueError("Self-reflection entry needs learned text")
    return _append_memory_json("self_reflections", clean, limit=120)


def add_source_note(entry: dict) -> dict:
    clean = {
        "date": str(entry.get("date", "")).strip(),
        "topic": str(entry.get("topic", "")).strip(),
        "source": str(entry.get("source", "")).strip(),
        "source_url": str(entry.get("source_url", "")).strip(),
        "insight": str(entry.get("insight", "")).strip(),
        "durability": str(entry.get("durability", "") or "stable").strip(),
        "confidence": str(entry.get("confidence", "") or "source-backed").strip(),
    }
    if not clean["topic"] or not clean["insight"]:
        raise ValueError("Source note needs topic and insight")
    return _append_memory_json("source_notes", clean, limit=120)


def add_topic_profile(profile: dict) -> dict:
    memory = get_memory()
    topic = str(profile.get("topic", "")).strip()
    if not topic:
        raise ValueError("Topic profile needs a topic name")
    existing = memory.get("topic_profiles", [])
    previous = {}
    for item in existing:
        try:
            parsed = json.loads(item)
        except Exception:
            parsed = {}
        if str(parsed.get("topic", "")).strip().lower() == topic.lower():
            previous = parsed
            break
    now = datetime.now(SGT).isoformat()
    clean = {
        "topic": topic,
        "category": str(profile.get("category", "") or "interests").strip(),
        "kind": str(profile.get("kind", previous.get("kind", "")) or "").strip(),
        "source_signal": str(profile.get("source_signal", previous.get("source_signal", "")) or "").strip(),
        "why": str(profile.get("why", "")).strip(),
        "track": [str(item).strip() for item in profile.get("track", []) if str(item).strip()],
        "preferred_angle": str(profile.get("preferred_angle", "")).strip(),
        "preferred_sources": [str(item).strip() for item in profile.get("preferred_sources", []) if str(item).strip()],
        "live_facts": [str(item).strip() for item in profile.get("live_facts", []) if str(item).strip()],
        "stable_context": [str(item).strip() for item in profile.get("stable_context", []) if str(item).strip()],
        "update_cadence": str(profile.get("update_cadence", "")).strip(),
        "interest_phase": str(profile.get("interest_phase", previous.get("interest_phase", "")) or "").strip(),
        "created": str(profile.get("created", previous.get("created", "")) or now).strip(),
        "updated": now,
    }
    encoded = json.dumps(clean, ensure_ascii=False, sort_keys=True)
    next_profiles = []
    replaced = False
    for item in existing:
        try:
            parsed = json.loads(item)
        except Exception:
            parsed = {}
        if str(parsed.get("topic", "")).strip().lower() == topic.lower():
            next_profiles.append(encoded)
            replaced = True
        else:
            next_profiles.append(item)
    if not replaced:
        next_profiles.append(encoded)
    memory["topic_profiles"] = next_profiles[-40:]
    set_memory(memory)
    return clean


def clear_memory() -> dict:
    memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
    set_memory(memory)
    return memory


# ─── SHEETS: NEWS WATCHLIST ─────────────────────────────────────────────────
# Stored in Config so users can shortlist topics without adding another tab.

DEFAULT_NEWS_TOPICS = [
    {"label": "Liverpool / EPL", "query": "Liverpool FC latest result match report fixture line-up injury transfer"},
    {"label": "F1 / Mercedes", "query": "Mercedes F1 George Russell Kimi Antonelli Lewis Hamilton latest result qualifying upgrade"},
    {"label": "AI Tools", "query": "OpenAI Codex Kimi Moonshot Gemini AI tools latest model release"},
    {"label": "Teenage Engineering", "query": "Teenage Engineering OP-XY OP-1 Field Pocket Operator product review firmware update"},
    {"label": "Android", "query": "Android OS Google Pixel Google Play app ecosystem security update"},
    {"label": "iOS", "query": "iOS iPhone Apple developer App Store TestFlight policy update"},
    {"label": "Solo Dev", "query": "solo developer iOS Android React Vite Capacitor Railway Netlify GitHub update"},
    {"label": "Islam", "query": "Islam Muslim spirituality Singapore"},
    {"label": "SG Education", "query": "Singapore education MOE"},
    {"label": "SG News", "query": "Singapore news today"},
    {"label": "Design / UI/UX", "query": "UI UX design"},
    {"label": "macOS", "query": "macOS Apple MacBook Xcode developer update"},
    {"label": "Nothing Products", "query": "Nothing Phone Nothing Ear CMF product review launch upcoming plans"},
    {"label": "Nothing OS", "query": "Nothing OS Nothing Phone Android update beta features"},
]

PINNED_NEWS_TOPIC_LABELS = {
    "f1 / mercedes",
    "ai tools",
    "teenage engineering",
    "android",
    "ios",
    "solo dev",
    "islam",
    "sg education",
    "sg news",
    "nothing products",
    "nothing os",
}


def get_news_topics() -> list:
    raw = get_config("news_topics")
    if not raw:
        return [dict(topic) for topic in DEFAULT_NEWS_TOPICS]
    try:
        topics = json.loads(raw)
    except Exception:
        return [dict(topic) for topic in DEFAULT_NEWS_TOPICS]

    clean = []
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        label = str(topic.get("label", "")).strip()
        query = str(topic.get("query", "")).strip()
        if label and query:
            clean.append({"label": label, "query": query})
    if not clean:
        return [dict(topic) for topic in DEFAULT_NEWS_TOPICS]
    seen = {topic["label"].lower() for topic in clean}
    for topic in DEFAULT_NEWS_TOPICS:
        if topic["label"].lower() in PINNED_NEWS_TOPIC_LABELS and topic["label"].lower() not in seen:
            clean.append(dict(topic))
            seen.add(topic["label"].lower())
    return clean


def set_news_topics(topics: list):
    clean = []
    seen = set()
    for topic in topics:
        label = str(topic.get("label", "")).strip()
        query = str(topic.get("query", "")).strip()
        if not label or not query:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        clean.append({"label": label, "query": query})
    set_config("news_topics", json.dumps(clean, ensure_ascii=False))


def add_news_topic(label: str, query: str = "") -> list:
    label = label.strip()
    query = (query or label).strip()
    topics = [t for t in get_news_topics() if t["label"].lower() != label.lower()]
    topics.append({"label": label, "query": query})
    set_news_topics(topics)
    return topics


def remove_news_topic(label: str) -> list:
    label = label.strip().lower()
    topics = [t for t in get_news_topics() if t["label"].lower() != label]
    set_news_topics(topics)
    return topics


def _get_redis():
    global _redis_client
    if _redis_client is None:
        url = os.environ.get("REDIS_URL", "").strip()
        if not url:
            _redis_client = False
        else:
            try:
                import redis

                client = redis.from_url(url, decode_responses=True)
                client.ping()
                _redis_client = client
            except Exception as exc:
                logger.warning(f"Redis unavailable for Sheets fallback storage: {exc}")
                _redis_client = False
    return _redis_client if _redis_client else None


def _normalise_nudges(nudges: list, include_sent=False) -> list:
    clean = []
    for nudge in nudges:
        if not isinstance(nudge, dict):
            continue
        status = str(nudge.get("status", "pending")).strip().lower() or "pending"
        if status != "pending" and not include_sent:
            continue
        clean.append({
            "id": str(nudge.get("id", "")),
            "message": str(nudge.get("message", "")).strip(),
            "send_at": str(nudge.get("send_at", "")).strip(),
            "status": status,
            "created": str(nudge.get("created", "")).strip(),
            "sent_at": str(nudge.get("sent_at", "")).strip(),
        })
    return [n for n in clean if n["id"] and n["message"] and n["send_at"]]


def _redis_nudges(include_sent=False) -> list:
    r = _get_redis()
    if not r:
        return []
    try:
        raw = r.get(_REDIS_NUDGE_KEY) or "[]"
        data = json.loads(raw)
        return _normalise_nudges(data if isinstance(data, list) else [], include_sent=include_sent)
    except Exception as exc:
        logger.warning(f"Could not read Redis nudge fallback: {exc}")
        return []


def _set_redis_nudges(nudges: list):
    r = _get_redis()
    if not r:
        return False
    try:
        r.set(_REDIS_NUDGE_KEY, json.dumps(nudges[-120:], ensure_ascii=False), ex=60 * 60 * 24 * 14)
        return True
    except Exception as exc:
        logger.warning(f"Could not persist Redis nudge fallback: {exc}")
        return False


def _merge_nudges(primary: list, fallback: list, include_sent=False) -> list:
    merged = []
    seen = set()
    for source in (primary, fallback):
        for nudge in source:
            nid = str(nudge.get("id", "")).strip()
            if not nid or nid in seen:
                continue
            seen.add(nid)
            merged.append(nudge)
    return _normalise_nudges(merged, include_sent=include_sent)


def _fallback_nudge_id(existing: list) -> str:
    existing_ids = {str(item.get("id", "")) for item in existing if isinstance(item, dict)}
    base = f"r-{int(time.time() * 1000)}"
    if base not in existing_ids:
        return base
    suffix = 1
    while f"{base}-{suffix}" in existing_ids:
        suffix += 1
    return f"{base}-{suffix}"


# ─── SHEETS: PROACTIVE NUDGES ───────────────────────────────────────────────
# Stored in Config as JSON so H.I.R.A can initiate chats at specific times.

def _sheet_nudges(include_sent=False) -> list:
    raw = get_config("proactive_nudges")
    if not raw:
        return []
    try:
        nudges = json.loads(raw)
    except Exception:
        return []
    return _normalise_nudges(nudges if isinstance(nudges, list) else [], include_sent=include_sent)


def get_nudges(include_sent=False) -> list:
    redis_items = _redis_nudges(include_sent=include_sent)
    try:
        sheet_items = _sheet_nudges(include_sent=include_sent)
    except Exception as exc:
        logger.warning(f"Sheets nudge read failed; using Redis fallback only: {exc}")
        return redis_items
    return _merge_nudges(sheet_items, redis_items, include_sent=include_sent)


def set_nudges(nudges: list):
    set_config("proactive_nudges", json.dumps(nudges, ensure_ascii=False))


def add_nudge(message: str, send_at: str) -> dict:
    with _storage_mutation_lock("proactive_nudges", _nudges_mutation_lock):
        now = datetime.now(SGT)
        try:
            sheet_nudges = _sheet_nudges(include_sent=True)
            next_id = 1
            numeric_ids = [int(n["id"]) for n in sheet_nudges if str(n.get("id", "")).isdigit()]
            if numeric_ids:
                next_id = max(numeric_ids) + 1
            nudge = {
                "id": str(next_id),
                "message": message.strip(),
                "send_at": send_at.strip(),
                "status": "pending",
                "created": now.isoformat(),
                "sent_at": "",
            }
            set_nudges(sheet_nudges + [nudge])
            return nudge
        except Exception as exc:
            logger.warning(f"Sheets nudge write failed; queueing Redis fallback nudge: {exc}")

        fallback_nudges = _redis_nudges(include_sent=True)
        nudge = {
            "id": _fallback_nudge_id(fallback_nudges),
            "message": message.strip(),
            "send_at": send_at.strip(),
            "status": "pending",
            "created": now.isoformat(),
            "sent_at": "",
        }
        if not _set_redis_nudges(fallback_nudges + [nudge]):
            raise RuntimeError("Sheets and Redis fallback are both unavailable for proactive nudges.")
        return nudge


def cancel_nudge(nudge_id: str) -> bool:
    with _storage_mutation_lock("proactive_nudges", _nudges_mutation_lock):
        nudges = get_nudges(include_sent=True)
        changed = False
        for nudge in nudges:
            if str(nudge.get("id")) == str(nudge_id) and nudge.get("status") != "sent":
                nudge["status"] = "cancelled"
                changed = True
        if changed:
            try:
                redis_items = [n for n in nudges if str(n.get("id", "")).startswith("r-")]
                if str(nudge_id).startswith("r-"):
                    _set_redis_nudges(redis_items)
                else:
                    set_nudges([n for n in nudges if not str(n.get("id", "")).startswith("r-")])
                    if redis_items:
                        _set_redis_nudges(redis_items)
            except Exception:
                _set_redis_nudges([n for n in nudges if str(n.get("id", "")).startswith("r-")] or nudges)
        return changed


def _parse_nudge_send_at(value: str) -> datetime | None:
    try:
        send_at = datetime.fromisoformat(str(value or "").strip())
    except Exception:
        return None
    if send_at.tzinfo is None:
        return SGT.localize(send_at)
    return send_at.astimezone(SGT)


def expire_stale_nudges(now: datetime | None = None, max_age_hours: int = NUDGE_STALE_HOURS) -> int:
    current = now or datetime.now(SGT)
    if current.tzinfo is None:
        current = SGT.localize(current)
    else:
        current = current.astimezone(SGT)
    cutoff = current - timedelta(hours=max(1, int(max_age_hours or NUDGE_STALE_HOURS)))
    with _storage_mutation_lock("proactive_nudges", _nudges_mutation_lock):
        nudges = get_nudges(include_sent=True)
        expired = 0
        for nudge in nudges:
            if str(nudge.get("status", "")).strip().lower() != "pending":
                continue
            send_at = _parse_nudge_send_at(nudge.get("send_at", ""))
            if not send_at or send_at >= cutoff:
                continue
            nudge["status"] = "expired"
            nudge["sent_at"] = current.isoformat()
            expired += 1
        if not expired:
            return 0
        sheet_items = [n for n in nudges if not str(n.get("id", "")).startswith("r-")]
        redis_items = [n for n in nudges if str(n.get("id", "")).startswith("r-")]
        try:
            set_nudges(sheet_items)
        except Exception as exc:
            logger.warning(f"Could not expire stale Sheets nudges; preserving state in Redis fallback: {exc}")
            _set_redis_nudges(nudges)
            return expired
        if redis_items:
            _set_redis_nudges(redis_items)
        return expired


def due_nudges(now: datetime) -> list:
    current = now
    if current.tzinfo is None:
        current = SGT.localize(current)
    else:
        current = current.astimezone(SGT)
    cutoff = current - timedelta(hours=NUDGE_STALE_HOURS)
    due = []
    for nudge in get_nudges(include_sent=True):
        if nudge.get("status") != "pending":
            continue
        send_at = _parse_nudge_send_at(nudge.get("send_at", ""))
        if not send_at:
            continue
        if send_at < cutoff:
            continue
        if send_at <= current:
            due.append(nudge)
    return due


def mark_nudge_sent(nudge_id: str):
    with _storage_mutation_lock("proactive_nudges", _nudges_mutation_lock):
        nudges = get_nudges(include_sent=True)
        now = datetime.now(SGT).isoformat()
        for nudge in nudges:
            if str(nudge.get("id")) == str(nudge_id):
                nudge["status"] = "sent"
                nudge["sent_at"] = now
                break
        sheet_items = [n for n in nudges if not str(n.get("id", "")).startswith("r-")]
        redis_items = [n for n in nudges if str(n.get("id", "")).startswith("r-")]
        if str(nudge_id).startswith("r-"):
            _set_redis_nudges(redis_items)
            return
        try:
            set_nudges(sheet_items)
        except Exception as exc:
            logger.warning(f"Could not mark Sheets nudge sent; preserving state in Redis fallback: {exc}")
            _set_redis_nudges(nudges)
        if redis_items:
            _set_redis_nudges(redis_items)


# ─── SHEETS: DAILY CHECK-INS ────────────────────────────────────────────────
# Recurring habits H.I.R.A can ask about daily until marked done for the day.

def get_checkins(include_inactive=False) -> list:
    raw = get_config("daily_checkins")
    if not raw:
        return []
    try:
        checkins = json.loads(raw)
    except Exception:
        return []

    clean = []
    for checkin in checkins:
        if not isinstance(checkin, dict):
            continue
        active = bool(checkin.get("active", True))
        if not active and not include_inactive:
            continue
        times = checkin.get("times", [])
        if not isinstance(times, list):
            times = []
        clean.append({
            "id": str(checkin.get("id", "")),
            "name": str(checkin.get("name", "")).strip(),
            "question": str(checkin.get("question", "")).strip(),
            "times": [str(t).strip() for t in times if str(t).strip()],
            "schedule_aware": bool(checkin.get("schedule_aware", False)),
            "target_count": int(checkin.get("target_count", 3) or 3),
            "window_start": str(checkin.get("window_start", "08:00") or "08:00").strip(),
            "window_end": str(checkin.get("window_end", "21:30") or "21:30").strip(),
            "min_break_minutes": int(checkin.get("min_break_minutes", 20) or 20),
            "active": active,
            "created": str(checkin.get("created", "")).strip(),
            "last_completed_date": str(checkin.get("last_completed_date", "")).strip(),
            "last_prompt_date": str(checkin.get("last_prompt_date", "")).strip(),
            "sent_slots": checkin.get("sent_slots", {}),
            "awaiting_reply": bool(checkin.get("awaiting_reply", False)),
        })
    return [
        c for c in clean
        if c["id"] and c["name"] and c["question"] and (c["times"] or c["schedule_aware"])
    ]


def set_checkins(checkins: list):
    set_config("daily_checkins", json.dumps(checkins, ensure_ascii=False))


def add_checkin(
    name: str,
    question: str,
    times: list,
    schedule_aware: bool = False,
    target_count: int = 3,
    window_start: str = "08:00",
    window_end: str = "21:30",
    min_break_minutes: int = 20,
) -> dict:
    cleaned_name = name.strip()
    cleaned_question = question.strip()
    cleaned_times = [str(t).strip() for t in times if str(t).strip()]
    checkins = get_checkins(include_inactive=True)
    for checkin in checkins:
        if str(checkin.get("name", "")).strip().lower() != cleaned_name.lower():
            continue
        checkin["name"] = cleaned_name
        checkin["question"] = cleaned_question
        checkin["times"] = cleaned_times
        checkin["schedule_aware"] = bool(schedule_aware)
        checkin["target_count"] = int(target_count or 3)
        checkin["window_start"] = str(window_start or "08:00").strip()
        checkin["window_end"] = str(window_end or "21:30").strip()
        checkin["min_break_minutes"] = int(min_break_minutes or 20)
        checkin["active"] = True
        checkin["sent_slots"] = {}
        checkin["awaiting_reply"] = False
        set_checkins(checkins)
        return checkin

    numeric_ids = [int(c["id"]) for c in checkins if str(c.get("id", "")).isdigit()]
    next_id = max(numeric_ids) + 1 if numeric_ids else 1
    checkin = {
        "id": str(next_id),
        "name": cleaned_name,
        "question": cleaned_question,
        "times": cleaned_times,
        "schedule_aware": bool(schedule_aware),
        "target_count": int(target_count or 3),
        "window_start": str(window_start or "08:00").strip(),
        "window_end": str(window_end or "21:30").strip(),
        "min_break_minutes": int(min_break_minutes or 20),
        "active": True,
        "created": datetime.now(SGT).isoformat(),
        "last_completed_date": "",
        "last_prompt_date": "",
        "sent_slots": {},
        "awaiting_reply": False,
    }
    checkins.append(checkin)
    set_checkins(checkins)
    return checkin


def cancel_checkin(checkin_id: str) -> bool:
    checkins = get_checkins(include_inactive=True)
    changed = False
    for checkin in checkins:
        if str(checkin.get("id")) == str(checkin_id):
            checkin["active"] = False
            checkin["awaiting_reply"] = False
            changed = True
    if changed:
        set_checkins(checkins)
    return changed


def due_checkins(now: datetime) -> list:
    today = now.strftime("%Y-%m-%d")
    now_hm = now.strftime("%H:%M")
    due = []
    for checkin in get_checkins(include_inactive=True):
        if (
            not checkin["active"]
            or checkin.get("schedule_aware")
            or checkin.get("last_completed_date") == today
        ):
            continue
        sent_slots = checkin.get("sent_slots") if isinstance(checkin.get("sent_slots"), dict) else {}
        today_slots = sent_slots.get(today, [])
        for slot in checkin["times"]:
            if slot <= now_hm and slot not in today_slots:
                due.append({**checkin, "due_slot": slot})
                break
    return due


def mark_checkin_prompted(checkin_id: str, slot: str, now: datetime):
    checkins = get_checkins(include_inactive=True)
    today = now.strftime("%Y-%m-%d")
    for checkin in checkins:
        if str(checkin.get("id")) != str(checkin_id):
            continue
        sent_slots = checkin.get("sent_slots") if isinstance(checkin.get("sent_slots"), dict) else {}
        today_slots = sent_slots.get(today, [])
        if slot not in today_slots:
            today_slots.append(slot)
        sent_slots[today] = sorted(today_slots)
        checkin["sent_slots"] = sent_slots
        checkin["last_prompt_date"] = today
        checkin["awaiting_reply"] = True
        break
    set_checkins(checkins)


def awaiting_checkins() -> list:
    today = datetime.now(SGT).strftime("%Y-%m-%d")
    return [
        c for c in get_checkins()
        if c.get("awaiting_reply") and c.get("last_completed_date") != today
    ]


def complete_checkin_today(checkin_id: str) -> bool:
    checkins = get_checkins(include_inactive=True)
    today = datetime.now(SGT).strftime("%Y-%m-%d")
    changed = False
    for checkin in checkins:
        if str(checkin.get("id")) == str(checkin_id):
            checkin["last_completed_date"] = today
            checkin["awaiting_reply"] = False
            changed = True
    if changed:
        set_checkins(checkins)
    return changed


# ─── SHEETS: FOLLOW-UPS ─────────────────────────────────────────────────────
# Stored in Config as JSON so H.I.R.A can track people/topic/date/status.

def get_followups(include_done=False) -> list:
    raw = get_config("followups")
    if not raw:
        return []
    try:
        followups = json.loads(raw)
    except Exception:
        return []
    clean = []
    for item in followups:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", "open")).strip().lower() or "open"
        if status == "done" and not include_done:
            continue
        clean.append({
            "id": str(item.get("id", "")),
            "person": str(item.get("person", "")).strip(),
            "topic": str(item.get("topic", "")).strip(),
            "due_date": str(item.get("due_date", "")).strip(),
            "channel": str(item.get("channel", "")).strip(),
            "status": status,
            "created": str(item.get("created", "")).strip(),
            "last_prompted": str(item.get("last_prompted", "")).strip(),
            "notes": str(item.get("notes", "")).strip(),
        })
    return [f for f in clean if f["id"] and f["topic"] and f["due_date"]]


def set_followups(followups: list):
    set_config("followups", json.dumps(followups, ensure_ascii=False))


def add_followup(person: str, topic: str, due_date: str, channel: str = "", notes: str = "") -> dict:
    followups = get_followups(include_done=True)
    numeric_ids = [int(f["id"]) for f in followups if str(f.get("id", "")).isdigit()]
    followup = {
        "id": str(max(numeric_ids) + 1 if numeric_ids else 1),
        "person": person.strip(),
        "topic": topic.strip(),
        "due_date": due_date.strip(),
        "channel": channel.strip(),
        "status": "open",
        "created": datetime.now(SGT).isoformat(),
        "last_prompted": "",
        "notes": notes.strip(),
    }
    followups.append(followup)
    set_followups(followups)
    return followup


def due_followups(today: str) -> list:
    try:
        current = date.fromisoformat(str(today or "")[:10])
    except Exception:
        return []
    stale_before = current - timedelta(days=FOLLOWUP_STALE_DAYS)
    due = []
    for followup in get_followups():
        try:
            due_date = date.fromisoformat(str(followup.get("due_date", ""))[:10])
        except Exception:
            continue
        if due_date < stale_before:
            continue
        if due_date <= current and followup.get("last_prompted") != today:
            due.append(followup)
    return due


def mark_followup_prompted(followup_id: str, today: str):
    followups = get_followups(include_done=True)
    for followup in followups:
        if str(followup.get("id")) == str(followup_id):
            followup["last_prompted"] = today
            break
    set_followups(followups)


def complete_followup(followup_id: str) -> bool:
    followups = get_followups(include_done=True)
    changed = False
    for followup in followups:
        if str(followup.get("id")) == str(followup_id):
            followup["status"] = "done"
            changed = True
            break
    if changed:
        set_followups(followups)
    return changed


# ─── GMAIL ──────────────────────────────────────────────────────────────────

def gmail_ok(account: str = "personal") -> bool:
    account = (account or "personal").strip().lower()
    if account in ("work", "moe", "school"):
        has_work_oauth = bool(os.environ.get("GOOGLE_WORK_GMAIL_REFRESH_TOKEN", "").strip()) and (
            all(
                os.environ.get(key, "").strip()
                for key in ("GOOGLE_WORK_GMAIL_CLIENT_ID", "GOOGLE_WORK_GMAIL_CLIENT_SECRET")
            )
            or all(
                os.environ.get(key, "").strip()
                for key in ("GOOGLE_GMAIL_CLIENT_ID", "GOOGLE_GMAIL_CLIENT_SECRET")
            )
        )
        return has_work_oauth or bool(os.environ.get("GOOGLE_WORK_GMAIL_USER", "").strip())
    if account in ("personal2", "secondary", "second"):
        has_oauth = bool(os.environ.get("GOOGLE_GMAIL2_REFRESH_TOKEN", "").strip()) and (
            all(
                os.environ.get(key, "").strip()
                for key in ("GOOGLE_GMAIL2_CLIENT_ID", "GOOGLE_GMAIL2_CLIENT_SECRET")
            )
            or all(
                os.environ.get(key, "").strip()
                for key in ("GOOGLE_GMAIL_CLIENT_ID", "GOOGLE_GMAIL_CLIENT_SECRET")
            )
        )
        return has_oauth or bool(os.environ.get("GOOGLE_GMAIL2_USER", "").strip())

    has_oauth = all(
        os.environ.get(key, "").strip()
        for key in ("GOOGLE_GMAIL_CLIENT_ID", "GOOGLE_GMAIL_CLIENT_SECRET", "GOOGLE_GMAIL_REFRESH_TOKEN")
    )
    return has_oauth or bool(os.environ.get("GOOGLE_GMAIL_USER", "").strip())


def gmail_label(account: str = "personal") -> str:
    account = (account or "personal").strip().lower()
    if account in ("work", "moe", "school"):
        return "work Gmail"
    if account in ("personal2", "secondary", "second"):
        return "personal Gmail 2"
    return "personal Gmail"


def _decode_gmail_part_body(part: dict) -> str:
    data = part.get("body", {}).get("data", "")
    if not data:
        return ""
    padded = data + "=" * (-len(data) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode()).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _gmail_body_text(payload: dict) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    def walk(part: dict):
        mime_type = (part.get("mimeType") or "").lower()
        if mime_type == "text/plain":
            plain_parts.append(_decode_gmail_part_body(part))
        elif mime_type == "text/html":
            html_parts.append(_decode_gmail_part_body(part))
        for child in part.get("parts", []) or []:
            walk(child)

    walk(payload or {})
    text = "\n".join(item for item in plain_parts if item.strip())
    if not text.strip() and html_parts:
        html = "\n".join(html_parts)
        html = re.sub(r"(?is)<(script|style).*?</\1>", " ", html)
        html = re.sub(r"(?i)<br\s*/?>", "\n", html)
        html = re.sub(r"(?i)</p\s*>", "\n", html)
        text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _gmail_body_limit(value: int | str | None = None) -> int:
    try:
        limit = int(value) if value is not None else 12000
    except Exception:
        limit = 12000
    return max(1000, min(limit, 50000))


def list_gmail_messages(query: str = "", max_results: int = 10, account: str = "personal", body_limit: int | str | None = None) -> list:
    service = _gmail(account)
    body_limit = _gmail_body_limit(body_limit)
    kwargs = {
        "userId": "me",
        "maxResults": max(1, min(int(max_results or 10), 25)),
        "fields": "messages/id,nextPageToken",
    }
    if query.strip():
        kwargs["q"] = query.strip()
    result = service.users().messages().list(**kwargs).execute()
    messages = []
    for item in result.get("messages", []):
        msg = service.users().messages().get(
            userId="me",
            id=item["id"],
            format="full",
            metadataHeaders=["From", "Subject", "Date"],
            fields="id,threadId,snippet,payload/mimeType,payload/body/data,payload/headers/name,payload/headers/value,payload/parts",
        ).execute()
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        body_text = _gmail_body_text(msg.get("payload", {}))
        body = body_text[:body_limit]
        messages.append({
            "id": msg.get("id", ""),
            "thread_id": msg.get("threadId", ""),
            "from": headers.get("from", ""),
            "subject": headers.get("subject", ""),
            "date": headers.get("date", ""),
            "snippet": msg.get("snippet", ""),
            "body": body,
            "body_chars": len(body_text),
            "body_truncated": len(body_text) > len(body),
        })
    return messages


def create_gmail_draft(to: str, subject: str, body: str, cc: str = "", account: str = "personal") -> dict:
    service = _gmail(account)
    message = EmailMessage()
    message["To"] = to
    if cc:
        message["Cc"] = cc
    message["Subject"] = subject
    message.set_content(body)
    encoded = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    return service.users().drafts().create(
        userId="me",
        body={"message": {"raw": encoded}},
    ).execute()
