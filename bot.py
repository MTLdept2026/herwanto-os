from __future__ import annotations

import os
import io
import json
import base64
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, time as dt_time, date
import pytz

from anthropic import Anthropic
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, CallbackContext,
)

import google_services as gs
import timetable as tt
import search_service as ss
import artifact_service as artifacts
import canva_service as canva

# ─── SETUP ───────────────────────────────────────────────────────────────────

SGT = pytz.timezone("Asia/Singapore")
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

claude = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

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

_mem_histories = defaultdict(list)
MAX_TURNS = 20

def get_history(user_id):
    r = _get_redis()
    if r:
        data = r.get(f"hist:{user_id}")
        return json.loads(data) if data else []
    return list(_mem_histories[user_id])

def save_history(user_id, history):
    r = _get_redis()
    if r:
        r.setex(f"hist:{user_id}", 86400 * 7, json.dumps(history))
    else:
        _mem_histories[user_id] = history

# ─── SYSTEM PROMPT ───────────────────────────────────────────────────────────

def SYSTEM_PROMPT():
    now = datetime.now(SGT)
    date_ctx = now.strftime("Today is %A, %-d %B %Y. Current time in Singapore: %H:%M SGT.")
    memory_ctx = (
        "\n\nPersistent school calendar memory:\n" + tt.format_school_calendar_memory() +
        "\n\nPersistent timetable memory:\n" + tt.format_timetable_memory()
    )
    if google_ok():
        try:
            memory = gs.get_memory()
            memory_lines = []
            for category, items in memory.items():
                if items:
                    memory_lines.append(f"{category.title()}: " + "; ".join(items[:8]))
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

You are Herwanto's personal AI assistant. Your name is Hira.
You are Singapore-based, calm under pressure, quick with useful judgment, and quietly warm.
You feel like a capable chief-of-staff in his pocket: practical, observant, wickedly witty when the moment allows, and never needy.

Personality:
- Speak like a trusted colleague who knows his life, not a generic chatbot.
- Default vibe: concise, grounded, encouraging, lightly informal, and sharp without being cruel.
- If he asks your name, answer naturally: "I'm Hira — your personal assistant."
- Be decisive when the path is clear; ask only when a missing detail blocks action.
- Have a wicked sense of humour and good wit: dry, clever, quick, and occasionally cheeky.
- Use humour like seasoning, not gravy. Never force jokes, emojis, hype, or motivational fluff.
- Do not make jokes when the user is upset, dealing with a serious issue, asking for BM accuracy, or needs exact code/business judgement.
- Never be mean-spirited, insulting, crude, or sarcastic at the user's expense. Punch up at chaos, bureaucracy, vague requirements, and bad error messages.
- Protect his attention: summarise, prioritise, and make the next action obvious.
- Notice patterns across school, CCA, projects, deadlines, and personal preferences.
- When he is stressed or overloaded, steady the room first, then give a short practical plan.
- When he is building something, be direct and product-minded.
- When he is teaching, be precise, culturally aware, and DBP-clean for Bahasa Melayu.
- When he is doing business, be commercially honest and Singapore-market aware.
- You may say "I" naturally, but do not pretend to have a human body, private life, or feelings outside the assistant role.

Herwanto wears three hats:

1. EDUCATOR — Bahasa Melayu teacher at Naval Base Secondary School (NBSS). Form teacher of 1 Flagship. Teaches ML to Sec 1, 2, 3, and 4 groups. Runs the school Football CCA. Use DBP conventions for all BM content.

2. APP DEVELOPER — Solo developer. Stack: React + Vite, Capacitor, Netlify, GitHub, Python. Active projects: GamePlan (sports CCA website service for Singapore schools) and Ruh (Islamic spiritual app, currently in App Store review). When he pastes code, debug immediately without preamble.

3. ENTREPRENEUR — Building GamePlan and Ruh as commercial products. Singapore market focus.

Rules:
- Be concise. No filler, no preamble.
- Infer his hat from context — never ask.
- For code: fix first, explain if needed.
- For BM: proper DBP spelling and grammar always.
- For business: give a direct recommendation.
- Singapore English and local context always apply.
- When he asks to add, schedule, or create a calendar event, create it directly if the date and time are clear. If details are incomplete, ask only for the missing detail.
- Never offer to generate .ics files. Use Google Calendar directly.
- The current date and time is already provided at the top of this prompt — always use it for any date/time reasoning.
- You have tools: create_calendar_event, add_reminder, create_proactive_nudge, create_daily_checkin, create_break_aware_daily_checkin, create_document_artifact, create_slide_deck_artifact, remember_artifact_template, get_assistant_context, remember_user_info, update_project_status, get_latest_news, and web_search. Use them proactively.
- When the user mentions an event, match, duty, or appointment at a specific time — call create_calendar_event immediately without asking.
- When the user mentions a task, deadline, or something to prepare/submit/complete — call add_reminder immediately without asking.
- When the user asks you to nudge, ping, check in, remind him at a specific time, or initiate a chat later — call create_proactive_nudge. Use this for time-specific heads-ups, not ordinary all-day deadlines.
- When the user asks for a recurring daily ping/check-in until he replies yes/done — call create_daily_checkin.
- When the user asks for daily reminders/check-ins to adapt around his schedule, breaks, timetable, lessons, or calendar, call create_break_aware_daily_checkin. This is especially appropriate for selawat, salawat, selawat ke atas Nabi, istighfar, zikr/dhikr, and similar habits he wants during free pockets of the day.
- When the user sends a screenshot, image, or PDF, inspect it for schedule items first: duties, appointments, matches, trainings, meetings, event timings, reporting times, deadlines, submissions, or preparation tasks.
- For screenshots/PDFs/images: create calendar events for items with a clear date and time, add reminders for dated tasks/deadlines, then summarise what you added and what still needs clarification.
- Uploaded PDFs/images are saved as file memory after processing. When the user later refers to a previously uploaded file, use Stored memory / Files first; do not ask for a re-upload unless the stored summary lacks the exact detail needed.
- When the user asks about his day, week, workload, priorities, deadlines, or project status — call get_assistant_context before answering.
- When the user asks about latest news, current events, headlines, football, F1, AI, Singapore education, apps, Apple, Nothing OS, or his shortlisted topics — call get_latest_news before answering.
- When the user says "remember", "note that", or gives stable preferences/facts about himself — call remember_user_info.
- When the user gives a project progress update — call update_project_status.
- When the user asks you to create a document, worksheet, letter, report, lesson plan, handout, memo, proposal, or meeting notes, call create_document_artifact.
- When the user asks you to create slides, a deck, PowerPoint, PPTX, presentation, pitch deck, briefing deck, or lesson slides, call create_slide_deck_artifact.
- When the user gives a reusable document/deck style, format, template preference, rubric format, NBSS worksheet format, GamePlan pitch style, or Rūḥ deck style, call remember_artifact_template.
- When the user explicitly asks for Canva, call create_canva_design if they want a new Canva Doc/presentation/whiteboard/email. If Canva is not connected, say the Canva access token is missing.
- After using a tool, confirm briefly and naturally. Do not ask "shall I add this?" — just do it.
"""

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
    "description": "Schedule Hira to initiate a Telegram chat at a specific date and time with a short message or heads-up.",
    "input_schema": {
        "type": "object",
        "properties": {
            "message": {"type": "string", "description": "Message Hira should send later"},
            "send_at": {"type": "string", "description": "ISO datetime in Asia/Singapore, e.g. 2026-05-01T16:30:00+08:00"}
        },
        "required": ["message", "send_at"]
    }
}

DAILY_CHECKIN_TOOL = {
    "name": "create_daily_checkin",
    "description": "Create a recurring daily check-in. Hira pings at configured times until Herwanto replies affirmatively, then stops for that day.",
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Short habit/check-in name"},
            "question": {"type": "string", "description": "Question Hira should ask"},
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
            "question": {"type": "string", "description": "Question Hira should ask"},
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

MEMORY_TOOL = {
    "name": "remember_user_info",
    "description": "Persist stable information about Herwanto: preferences, personal profile facts, important people, places, or project context.",
    "input_schema": {
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "description": "One of: profile, preferences, people, places, projects, files, templates"
            },
            "text": {"type": "string", "description": "Concise memory to store"}
        },
        "required": ["category", "text"]
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

CANVA_CREATE_TOOL = {
    "name": "create_canva_design",
    "description": "Create a new editable Canva design shell when Canva is connected. Supports doc, presentation, whiteboard, and email.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Design title"},
            "design_type": {"type": "string", "description": "doc, presentation, whiteboard, or email"}
        },
        "required": ["title", "design_type"]
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

def _lessons_for_date(target):
    official_week = tt.get_school_week_info(target)
    if official_week:
        day_name = tt.DAY_MAP.get(target.weekday())
        lessons = [] if official_week["is_school_holiday"] else tt.TIMETABLE.get((day_name, official_week["week_type"]), []) if day_name else []
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

def _format_memory(memory: dict) -> str:
    lines = [
        "*Persistent School Calendar*",
        f"- {tt.format_school_calendar_memory()}",
        "",
        "*Persistent Timetable*",
        f"- {tt.format_timetable_memory()}",
        "",
    ]
    for category in ("profile", "preferences", "people", "places", "projects", "files", "templates"):
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
    if wt_label:
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

    try:
        projects = gs.get_projects()
        if projects:
            lines.append("*Project pulse*")
            for p in projects:
                next_bit = f" Next: {p['next_milestone']} ({p['milestone_date']})." if p["next_milestone"] else ""
                lines.append(f"- *{p['project']}* - {p['status']}.{next_bit}")
    except Exception:
        pass

    return "\n".join(lines).strip()

def _news_topics():
    if google_ok():
        try:
            return [(t["label"], t["query"]) for t in gs.get_news_topics()]
        except Exception:
            pass
    return [(label, query) for label, query in ss.DIGEST_TOPICS]

def build_news_digest(query: str = "", max_items: int = 2) -> str:
    max_items = max(1, min(int(max_items or 2), 5))
    if query.strip():
        items = ss.google_news(query.strip(), max_items=max_items)
        return f"*News: {query.strip()}*\n\n{ss.format_news_items(items)}"

    digest = ss.get_digest_for_topics(_news_topics(), max_items=max_items)
    return f"*Latest from your shortlist*\n\n{digest or 'No news found.'}"

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
    for checkin in gs.get_checkins(include_inactive=True):
        if (
            not checkin["active"]
            or not checkin.get("schedule_aware")
            or checkin.get("last_completed_date") == today
        ):
            continue
        sent_slots = checkin.get("sent_slots") if isinstance(checkin.get("sent_slots"), dict) else {}
        today_slots = sent_slots.get(today, [])
        for slot in _break_aware_slots(checkin, now.date()):
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
    prompt = f"""Create a structured DOCX content spec for Hira to render.

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
        model="claude-sonnet-4-6",
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}]
    )
    return _json_from_claude_text(resp.content[0].text)

def _generate_slide_spec(title: str, instructions: str, audience: str = "", slide_count: int = 8, language: str = "") -> dict:
    template_context = _artifact_template_context()
    slide_count = max(3, min(int(slide_count or 8), 15))
    prompt = f"""Create a structured PPTX content spec for Hira to render.

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
        model="claude-sonnet-4-6",
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}]
    )
    return _json_from_claude_text(resp.content[0].text)

def _upload_artifact_if_possible(path: str, convert_to: str) -> dict | None:
    if not google_ok():
        return None
    try:
        return gs.upload_artifact(path, convert_to=convert_to)
    except Exception as e:
        logger.warning(f"Drive artifact upload failed: {e}")
        return None

def _artifact_result_text(kind: str, path, drive_file: dict | None = None) -> str:
    text = f"Created {kind}: `{path.name}`"
    if drive_file and drive_file.get("webViewLink"):
        text += f"\nEditable Google link: {drive_file['webViewLink']}"
    return text

def build_briefing():
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

    # Morning news digest
    try:
        digest = ss.get_morning_digest()
        if digest:
            lines.append("")
            lines.append("*Morning digest:*")
            lines.append(digest)
    except Exception:
        pass

    lines.append("\nHave a productive day!")
    return "\n".join(lines)

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
        f"*Canva*\n/canva /canvasearch /canvaexport /canvaexportcheck\n\n"
        f"*Assistant*\n/agenda [days] /remember /memory /forget all\n\n"
        f"*Projects*\n/projects /update\n\n"
        f"*Search*\n/search [query]\n\n"
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
                model="claude-haiku-4-5-20251001",
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
        drive_file = _upload_artifact_if_possible(str(path), "doc")
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
        drive_file = _upload_artifact_if_possible(str(path), "slides")
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

async def canva_cmd(update, context):
    if not canva.canva_ok():
        await update.message.reply_text("Canva not connected. Add CANVA_ACCESS_TOKEN to Railway variables first.")
        return
    text = " ".join(context.args).strip()
    if not text or "|" not in text:
        await reply(update,
            "Usage: `/canva Title | doc|presentation|whiteboard|email`\n"
            "Example: `/canva GamePlan Pitch | presentation`",
            parse_mode="Markdown")
        return
    try:
        title, design_type = [p.strip() for p in text.split("|", 1)]
        result = canva.create_design(title, design_type)
        design = result.get("design", result)
        if google_ok():
            gs.add_memory("files", f"Created Canva {design_type} {design.get('title', title)} | id={design.get('id', '')} | edit={design.get('urls', {}).get('edit_url', '')}")
        await reply(update, f"Created Canva design:\n\n{canva.format_design(design)}", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Canva create error: {e}")
        await update.message.reply_text(f"Could not create Canva design: {e}")

async def canva_search_cmd(update, context):
    if not canva.canva_ok():
        await update.message.reply_text("Canva not connected. Add CANVA_ACCESS_TOKEN to Railway variables first.")
        return
    query = " ".join(context.args).strip()
    try:
        result = canva.list_designs(query)
        designs = result.get("items") or result.get("designs") or []
        if not designs:
            await update.message.reply_text("No Canva designs found.")
            return
        lines = ["*Canva designs*\n"]
        for item in designs[:10]:
            design = item.get("design", item)
            lines.append(canva.format_design(design))
            lines.append("")
        await reply(update, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Canva search error: {e}")
        await update.message.reply_text(f"Could not search Canva: {e}")

async def canva_export_cmd(update, context):
    if not canva.canva_ok():
        await update.message.reply_text("Canva not connected. Add CANVA_ACCESS_TOKEN to Railway variables first.")
        return
    text = " ".join(context.args).strip()
    if not text or "|" not in text:
        await reply(update,
            "Usage: `/canvaexport design_id | pdf|pptx|png|jpg`\n"
            "Then check it with `/canvaexportcheck job_id` if it is still processing.",
            parse_mode="Markdown")
        return
    try:
        design_id, file_type = [p.strip() for p in text.split("|", 1)]
        job = canva.create_export_job(design_id, file_type)
        await reply(update, canva.format_export(job.get("job", job)), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Canva export error: {e}")
        await update.message.reply_text(f"Could not start Canva export: {e}")

async def canva_export_check_cmd(update, context):
    if not canva.canva_ok():
        await update.message.reply_text("Canva not connected. Add CANVA_ACCESS_TOKEN to Railway variables first.")
        return
    job_id = " ".join(context.args).strip()
    if not job_id:
        await reply(update, "Usage: `/canvaexportcheck job_id`", parse_mode="Markdown")
        return
    try:
        job = canva.get_export_job(job_id)
        await reply(update, canva.format_export(job.get("job", job)), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Canva export check error: {e}")
        await update.message.reply_text(f"Could not check Canva export: {e}")

async def briefing_cmd(update, context):
    await reply(update, build_briefing(), parse_mode="Markdown")

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
            "Categories: profile, preferences, people, places, projects, files, templates",
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
    if confirm != "all":
        await reply(update, "Usage: `/forget all` clears stored assistant memory.", parse_mode="Markdown")
        return
    try:
        gs.clear_memory()
        await update.message.reply_text("Assistant memory cleared.")
    except Exception as e:
        await update.message.reply_text(f"Memory error: {e}")

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
            model="claude-haiku-4-5-20251001",
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
        REMINDER_TOOL,
        NUDGE_TOOL,
        DAILY_CHECKIN_TOOL,
        BREAK_AWARE_CHECKIN_TOOL,
        DOCUMENT_ARTIFACT_TOOL,
        SLIDE_ARTIFACT_TOOL,
        TEMPLATE_MEMORY_TOOL,
        CANVA_CREATE_TOOL,
        MEMORY_TOOL,
        WEEK_TYPE_TOOL,
        PROJECT_TOOL,
        NEWS_TOOL,
    ]
    if ss.search_enabled():
        tools.append(SEARCH_TOOL)
    return tools

async def _run_agentic_claude(messages, max_tokens=2048, tools=None):
    tools = tools or _core_tools()
    reply_text = ""
    max_iterations = 5

    for _ in range(max_iterations):
        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=max_tokens,
            system=SYSTEM_PROMPT(),
            tools=tools,
            messages=messages
        )

        if resp.stop_reason != "tool_use":
            reply_text = next((b.text for b in resp.content if b.type == "text"), "Done.")
            break

        messages.append({"role": "assistant", "content": resp.content})
        tool_results = []
        for block in resp.content:
            if block.type != "tool_use":
                continue
            logger.info(f"Tool call: {block.name} {block.input}")
            result = await _execute_tool(block.name, block.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result
            })
        messages.append({"role": "user", "content": tool_results})

    return reply_text or "Done."

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

async def handle_document(update, context):
    """Handle PDFs/images and extract schedule data when present."""
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    doc = update.message.document
    caption = update.message.caption or "Extract any schedule items, calendar events, reminders, deadlines, and action items relevant to my work."
    try:
        file = await context.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        file_data = base64.b64encode(buf.getvalue()).decode()

        if doc.mime_type == "application/pdf":
            content_block = {
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": file_data}
            }
        elif doc.mime_type and doc.mime_type.startswith("image/"):
            content_block = {
                "type": "image",
                "source": {"type": "base64", "media_type": doc.mime_type, "data": file_data}
            }
        else:
            await update.message.reply_text(
                f"File type `{doc.mime_type}` not supported yet.\nSend PDFs or images.",
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

    elif name == "get_assistant_context":
        try:
            return build_context_snapshot(inp.get("days", 7))
        except Exception as e:
            return f"Failed to get assistant context: {e}"

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

    elif name == "create_calendar_event":
        try:
            start_dt = SGT.localize(datetime.strptime(f"{inp['date']} {inp['start_time']}", "%Y-%m-%d %H:%M"))
            end_dt   = SGT.localize(datetime.strptime(f"{inp['date']} {inp['end_time']}",   "%Y-%m-%d %H:%M"))
            gs.create_event(inp["title"], start_dt, end_dt,
                            inp.get("location", ""), inp.get("description", ""))
            return f"Created: {inp['title']} on {inp['date']} {inp['start_time']}–{inp['end_time']}"
        except Exception as e:
            return f"Failed to create event: {e}"

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
            drive_file = _upload_artifact_if_possible(str(path), "doc")
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
            drive_file = _upload_artifact_if_possible(str(path), "slides")
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

    elif name == "create_canva_design":
        try:
            if not canva.canva_ok():
                return "Canva is not connected. Add CANVA_ACCESS_TOKEN to Railway variables first."
            result = canva.create_design(inp["title"], inp.get("design_type", "presentation"))
            design = result.get("design", result)
            if google_ok():
                gs.add_memory("files", f"Created Canva {inp.get('design_type', 'presentation')} {design.get('title', inp['title'])} | id={design.get('id', '')} | edit={design.get('urls', {}).get('edit_url', '')}")
            return f"Created Canva design:\n{canva.format_design(design)}"
        except Exception as e:
            return f"Failed to create Canva design: {e}"

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
    user_id = update.effective_user.id
    text = update.message.text

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

    history = get_history(user_id)
    history.append({"role": "user", "content": text})
    if len(history) > MAX_TURNS:
        history = history[-MAX_TURNS:]

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    try:
        messages = list(history)
        reply_text = await _run_agentic_claude(messages, max_tokens=1024)

        # Save only user message + final reply to persistent history
        history.append({"role": "assistant", "content": reply_text})
        save_history(user_id, history)
        await reply(update, reply_text)

    except Exception as e:
        logger.error(f"Claude error: {e}")
        await update.message.reply_text("Error. Try again.")

# ─── SCHEDULED JOBS ──────────────────────────────────────────────────────────

async def morning_briefing_job(context):
    if not google_ok():
        return
    try:
        chat_id = gs.get_config("chat_id")
        if not chat_id:
            return
        await context.bot.send_message(chat_id=int(chat_id), text=build_briefing(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Morning briefing error: {e}")

async def friday_checkin_job(context):
    if not google_ok():
        return
    try:
        chat_id = gs.get_config("chat_id")
        if not chat_id:
            return
        projs = gs.get_projects()
        lines = ["*Weekly project check-in*\n"]
        for p in projs:
            lines.append(f"- *{p['project']}* - {p['status']} _(updated {p['last_update']})_")
        if not projs:
            lines.append("No projects tracked yet.")
        lines.append("\nUse /update to log progress.")
        await context.bot.send_message(chat_id=int(chat_id), text="\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Friday check-in error: {e}")

async def proactive_nudges_job(context):
    if not google_ok():
        return
    try:
        chat_id = gs.get_config("chat_id")
        if not chat_id:
            return
        now = datetime.now(SGT)
        for nudge in gs.due_nudges(now):
            text = f"*Hira nudge*\n\n{nudge['message']}"
            await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
            gs.mark_nudge_sent(nudge["id"])
    except Exception as e:
        logger.error(f"Proactive nudge error: {e}")

async def daily_checkins_job(context):
    if not google_ok():
        return
    try:
        chat_id = gs.get_config("chat_id")
        if not chat_id:
            return
        now = datetime.now(SGT)
        due_checkins = gs.due_checkins(now) + _due_break_aware_checkins(now)
        for checkin in due_checkins:
            text = f"*Hira check-in*\n\n{checkin['question']}\n\nReply `yes`, `done`, or `alhamdulillah` once it is done and I’ll stop asking for today."
            await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
            gs.mark_checkin_prompted(checkin["id"], checkin["due_slot"], now)
    except Exception as e:
        logger.error(f"Daily check-in error: {e}")

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN not set")
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start",    start))
    app.add_handler(CommandHandler("lessons",  lessons_cmd))
    app.add_handler(CommandHandler("setweek",  setweek_cmd))
    app.add_handler(CommandHandler("today",    today_cmd))
    app.add_handler(CommandHandler("tomorrow", tomorrow_cmd))
    app.add_handler(CommandHandler("week",     week_cmd))
    app.add_handler(CommandHandler("due",      due_cmd))
    app.add_handler(CommandHandler("remind",   remind_cmd))
    app.add_handler(CommandHandler("done",     done_cmd))
    app.add_handler(CommandHandler("nudge",    nudge_cmd))
    app.add_handler(CommandHandler("nudges",   nudges_cmd))
    app.add_handler(CommandHandler("cancelnudge", cancelnudge_cmd))
    app.add_handler(CommandHandler("checkin",  checkin_cmd))
    app.add_handler(CommandHandler("checkins", checkins_cmd))
    app.add_handler(CommandHandler("cancelcheckin", cancelcheckin_cmd))
    app.add_handler(CommandHandler("projects", projects_cmd))
    app.add_handler(CommandHandler("update",   update_cmd))
    app.add_handler(CommandHandler("doc",      doc_cmd))
    app.add_handler(CommandHandler("slides",   slides_cmd))
    app.add_handler(CommandHandler("template", template_cmd))
    app.add_handler(CommandHandler("templates", templates_cmd))
    app.add_handler(CommandHandler("artifacts", artifacts_cmd))
    app.add_handler(CommandHandler("canva",   canva_cmd))
    app.add_handler(CommandHandler("canvasearch", canva_search_cmd))
    app.add_handler(CommandHandler("canvaexport", canva_export_cmd))
    app.add_handler(CommandHandler("canvaexportcheck", canva_export_check_cmd))
    app.add_handler(CommandHandler("briefing", briefing_cmd))
    app.add_handler(CommandHandler("agenda",   agenda_cmd))
    app.add_handler(CommandHandler("remember", remember_cmd))
    app.add_handler(CommandHandler("memory",   memory_cmd))
    app.add_handler(CommandHandler("forget",   forget_cmd))
    app.add_handler(CommandHandler("search",   search_cmd))
    app.add_handler(CommandHandler("news",     news_cmd))
    app.add_handler(CommandHandler("watch",    watch_cmd))
    app.add_handler(CommandHandler("watchlist", watchlist_cmd))
    app.add_handler(CommandHandler("unwatch",  unwatch_cmd))
    app.add_handler(CommandHandler("addcal",   addcal_cmd))
    app.add_handler(CommandHandler("clear",    clear_cmd))
    app.add_handler(MessageHandler(filters.PHOTO,        handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    jq = app.job_queue
    jq.run_daily(morning_briefing_job, time=dt_time(7, 0, 0, tzinfo=SGT), name="morning_briefing")
    jq.run_daily(friday_checkin_job,   time=dt_time(17, 0, 0, tzinfo=SGT), days=(4,), name="friday_checkin")
    jq.run_repeating(proactive_nudges_job, interval=60, first=10, name="proactive_nudges")
    jq.run_repeating(daily_checkins_job, interval=60, first=20, name="daily_checkins")
    logger.info("Herwanto OS running — all systems active.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
