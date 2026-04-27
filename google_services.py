"""
Google Calendar (read) + Google Sheets (read/write) via Service Account.
Sheets acts as persistent storage for reminders, projects, and config.
"""

import os
import json
import base64
import pytz
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

SGT = pytz.timezone('Asia/Singapore')

SCOPES = [
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
]

GMAIL_SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.compose',
]

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")

# Support multiple calendars via GOOGLE_CALENDAR_IDS (comma-separated)
# Falls back to GOOGLE_CALENDAR_ID, then "primary"
_cal_ids_raw = os.environ.get("GOOGLE_CALENDAR_IDS", "") or os.environ.get("GOOGLE_CALENDAR_ID", "primary")
CALENDAR_IDS = [c.strip() for c in _cal_ids_raw.split(",") if c.strip()]


def _creds(scopes=None, subject: str = ""):
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    sa_info = json.loads(base64.b64decode(raw).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes or SCOPES)
    if subject:
        creds = creds.with_subject(subject)
    return creds


def _cal():
    return build("calendar", "v3", credentials=_creds())


def _sheets():
    return build("sheets", "v4", credentials=_creds())


def _drive():
    return build("drive", "v3", credentials=_creds())


def _gmail():
    refresh_token = os.environ.get("GOOGLE_GMAIL_REFRESH_TOKEN", "").strip()
    client_id = os.environ.get("GOOGLE_GMAIL_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_GMAIL_CLIENT_SECRET", "").strip()
    if refresh_token and client_id and client_secret:
        creds = Credentials(
            None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=GMAIL_SCOPES,
        )
        return build("gmail", "v1", credentials=creds)

    user = os.environ.get("GOOGLE_GMAIL_USER", "").strip()
    if not user:
        raise EnvironmentError("Gmail not configured. Set personal Gmail OAuth vars or GOOGLE_GMAIL_USER for Workspace delegation.")
    return build("gmail", "v1", credentials=_creds(GMAIL_SCOPES, subject=user))


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
            all_events.extend(result.get("items", []))
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
        root_id = _ensure_drive_folder("Hira")
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
    rows = _raw_reminders()
    next_id = len(rows) + 1
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

def get_config(key: str):
    r = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Config!A2:B"
    ).execute()
    for row in r.get("values", []):
        if row and row[0] == key:
            return row[1] if len(row) > 1 else None
    return None


def set_config(key: str, value: str):
    r = _sheets().spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Config!A2:B"
    ).execute()
    rows = r.get("values", [])
    for i, row in enumerate(rows):
        if row and row[0] == key:
            _sheets().spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"Config!B{i + 2}",
                valueInputOption="RAW",
                body={"values": [[value]]},
            ).execute()
            return
    _sheets().spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="Config!A:B",
        valueInputOption="RAW",
        body={"values": [[key, value]]},
    ).execute()


# ─── SHEETS: ASSISTANT MEMORY ────────────────────────────────────────────────
# Stored in Config as one JSON blob to avoid requiring another sheet tab.

DEFAULT_MEMORY = {
    "profile": [],
    "preferences": [],
    "people": [],
    "places": [],
    "projects": [],
    "files": [],
    "templates": [],
}


def get_memory() -> dict:
    raw = get_config("assistant_memory")
    if not raw:
        return {k: list(v) for k, v in DEFAULT_MEMORY.items()}
    try:
        data = json.loads(raw)
    except Exception:
        return {k: list(v) for k, v in DEFAULT_MEMORY.items()}

    memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
    for key, value in data.items():
        if isinstance(value, list):
            memory[key] = [str(item) for item in value if str(item).strip()]
    return memory


def set_memory(memory: dict):
    clean = {}
    for key in DEFAULT_MEMORY:
        values = memory.get(key, [])
        clean[key] = [str(item).strip() for item in values if str(item).strip()]
    set_config("assistant_memory", json.dumps(clean, ensure_ascii=False))


def add_memory(category: str, text: str) -> dict:
    category = (category or "profile").lower().strip()
    aliases = {
        "preference": "preferences",
        "person": "people",
        "place": "places",
        "project": "projects",
        "file": "files",
        "document": "files",
        "attachment": "files",
        "upload": "files",
        "template": "templates",
        "style": "templates",
    }
    category = aliases.get(category, category)
    if category not in DEFAULT_MEMORY:
        category = "profile"
    memory = get_memory()
    item = text.strip()
    if item and item not in memory[category]:
        memory[category].append(item)
    set_memory(memory)
    return memory


def clear_memory() -> dict:
    memory = {k: list(v) for k, v in DEFAULT_MEMORY.items()}
    set_memory(memory)
    return memory


# ─── SHEETS: NEWS WATCHLIST ─────────────────────────────────────────────────
# Stored in Config so users can shortlist topics without adding another tab.

DEFAULT_NEWS_TOPICS = [
    {"label": "Liverpool / EPL", "query": "Liverpool FC Premier League"},
    {"label": "F1", "query": "Formula 1"},
    {"label": "AI", "query": "Claude Gemini Codex AI"},
    {"label": "Islam", "query": "Islam Muslim"},
    {"label": "SG Education", "query": "Singapore education MOE"},
    {"label": "Current Affairs", "query": "Singapore news today"},
    {"label": "Design / UI/UX", "query": "UI UX design"},
    {"label": "App Dev", "query": "iOS Android app development"},
    {"label": "macOS", "query": "macOS Apple"},
    {"label": "Nothing OS", "query": "Nothing Phone Android"},
]


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
    return clean or [dict(topic) for topic in DEFAULT_NEWS_TOPICS]


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


# ─── SHEETS: PROACTIVE NUDGES ───────────────────────────────────────────────
# Stored in Config as JSON so Hira can initiate chats at specific times.

def get_nudges(include_sent=False) -> list:
    raw = get_config("proactive_nudges")
    if not raw:
        return []
    try:
        nudges = json.loads(raw)
    except Exception:
        return []

    clean = []
    for nudge in nudges:
        if not isinstance(nudge, dict):
            continue
        status = nudge.get("status", "pending")
        if status == "sent" and not include_sent:
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


def set_nudges(nudges: list):
    set_config("proactive_nudges", json.dumps(nudges, ensure_ascii=False))


def add_nudge(message: str, send_at: str) -> dict:
    nudges = get_nudges(include_sent=True)
    now = datetime.now(SGT)
    next_id = 1
    numeric_ids = [int(n["id"]) for n in nudges if str(n.get("id", "")).isdigit()]
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
    nudges.append(nudge)
    set_nudges(nudges)
    return nudge


def cancel_nudge(nudge_id: str) -> bool:
    nudges = get_nudges(include_sent=True)
    changed = False
    for nudge in nudges:
        if str(nudge.get("id")) == str(nudge_id) and nudge.get("status") != "sent":
            nudge["status"] = "cancelled"
            changed = True
    if changed:
        set_nudges(nudges)
    return changed


def due_nudges(now: datetime) -> list:
    due = []
    for nudge in get_nudges(include_sent=True):
        if nudge.get("status") != "pending":
            continue
        try:
            send_at = datetime.fromisoformat(nudge["send_at"])
            if send_at.tzinfo is None:
                send_at = SGT.localize(send_at)
            else:
                send_at = send_at.astimezone(SGT)
        except Exception:
            continue
        if send_at <= now:
            due.append(nudge)
    return due


def mark_nudge_sent(nudge_id: str):
    nudges = get_nudges(include_sent=True)
    now = datetime.now(SGT).isoformat()
    for nudge in nudges:
        if str(nudge.get("id")) == str(nudge_id):
            nudge["status"] = "sent"
            nudge["sent_at"] = now
            break
    set_nudges(nudges)


# ─── SHEETS: DAILY CHECK-INS ────────────────────────────────────────────────
# Recurring habits Hira can ask about daily until marked done for the day.

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
# Stored in Config as JSON so Hira can track people/topic/date/status.

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
    due = []
    for followup in get_followups():
        if followup["due_date"] <= today and followup.get("last_prompted") != today:
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

def gmail_ok() -> bool:
    has_oauth = all(
        os.environ.get(key, "").strip()
        for key in ("GOOGLE_GMAIL_CLIENT_ID", "GOOGLE_GMAIL_CLIENT_SECRET", "GOOGLE_GMAIL_REFRESH_TOKEN")
    )
    return has_oauth or bool(os.environ.get("GOOGLE_GMAIL_USER", "").strip())


def list_gmail_messages(query: str = "is:unread newer_than:7d", max_results: int = 10) -> list:
    service = _gmail()
    result = service.users().messages().list(
        userId="me",
        q=query,
        maxResults=max(1, min(int(max_results or 10), 25)),
    ).execute()
    messages = []
    for item in result.get("messages", []):
        msg = service.users().messages().get(
            userId="me",
            id=item["id"],
            format="metadata",
            metadataHeaders=["From", "Subject", "Date"],
        ).execute()
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        messages.append({
            "id": msg.get("id", ""),
            "thread_id": msg.get("threadId", ""),
            "from": headers.get("from", ""),
            "subject": headers.get("subject", ""),
            "date": headers.get("date", ""),
            "snippet": msg.get("snippet", ""),
        })
    return messages


def create_gmail_draft(to: str, subject: str, body: str, cc: str = "") -> dict:
    service = _gmail()
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
