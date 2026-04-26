import os
import io
import json
import base64
import logging
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
    memory_ctx = ""
    if google_ok():
        try:
            memory = gs.get_memory()
            memory_lines = []
            for category, items in memory.items():
                if items:
                    memory_lines.append(f"{category.title()}: " + "; ".join(items[:8]))
            if memory_lines:
                memory_ctx = "\n\nStored memory:\n" + "\n".join(memory_lines)
        except Exception:
            memory_ctx = ""

    return f"""{date_ctx}{memory_ctx}

You are Herwanto's personal AI assistant. Your name is Hira.
You are Singapore-based, calm under pressure, quick with useful judgment, and quietly warm.
You feel like a capable chief-of-staff in his pocket: practical, observant, a little witty when the moment allows, and never needy.

Personality:
- Speak like a trusted colleague who knows his life, not a generic chatbot.
- Default vibe: concise, grounded, encouraging, and lightly informal.
- Be decisive when the path is clear; ask only when a missing detail blocks action.
- Use gentle humour sparingly. Never force jokes, emojis, hype, or motivational fluff.
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
- You have tools: create_calendar_event, add_reminder, get_assistant_context, remember_user_info, update_project_status, and web_search. Use them proactively.
- When the user mentions an event, match, duty, or appointment at a specific time — call create_calendar_event immediately without asking.
- When the user mentions a task, deadline, or something to prepare/submit/complete — call add_reminder immediately without asking.
- When the user asks about his day, week, workload, priorities, deadlines, or project status — call get_assistant_context before answering.
- When the user says "remember", "note that", or gives stable preferences/facts about himself — call remember_user_info.
- When the user gives a project progress update — call update_project_status.
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
                "description": "One of: profile, preferences, people, places, projects"
            },
            "text": {"type": "string", "description": "Concise memory to store"}
        },
        "required": ["category", "text"]
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

def _lessons_for_date(target):
    ref_date, ref_type = _get_week_config()
    if not ref_date or not ref_type:
        return [], ""
    lessons = tt.get_lessons(target, ref_date, ref_type)
    wt = tt.get_week_type(ref_date, ref_type, target)
    return lessons, tt.week_type_label(wt)

def _format_memory(memory: dict) -> str:
    lines = []
    for category in ("profile", "preferences", "people", "places", "projects"):
        items = memory.get(category, [])
        if not items:
            continue
        lines.append(f"*{category.title()}*")
        for item in items:
            lines.append(f"- {item}")
        lines.append("")
    return "\n".join(lines).strip() or "No stored memory yet."

def build_context_snapshot(days: int = 7) -> str:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(SGT)
    today = now.date()
    end_date = today + timedelta(days=days)
    lines = [f"Assistant context as of {now.strftime('%A, %-d %B %Y, %H:%M SGT')}"]

    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"\nToday's lessons ({wt_label} week):")
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

def build_agenda(days: int = 7) -> str:
    days = max(1, min(int(days or 7), 14))
    now = datetime.now(SGT)
    today = now.date()
    lines = [f"*Agenda*\n_{now.strftime('%A, %-d %B %Y, %H:%M SGT')}_\n"]

    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"*Today at school ({wt_label} week)*")
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

def build_briefing():
    now = datetime.now(SGT)
    today = now.date()
    lines = [f"Good morning, Herwanto!\n_{now.strftime('%A, %-d %B %Y')}_\n"]

    # Timetable
    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"*Today's lessons ({wt_label} week):*")
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
        f"*Assistant*\n/agenda [days] /remember /memory /forget all\n\n"
        f"*Projects*\n/projects /update\n\n"
        f"*Search*\n/search [query]\n\n"
        f"*Briefing*\n/briefing (auto 7am SGT)\n\n"
        f"/clear - reset AI chat\nOr just talk to me.",
        parse_mode="Markdown")

async def lessons_cmd(update, context):
    today = datetime.now(SGT).date()
    if today.weekday() > 4:
        await update.message.reply_text("Weekend - no lessons!")
        return
    ref_date, ref_type = _get_week_config()
    if not ref_date:
        await update.message.reply_text("Week type not set. Use /setweek odd or /setweek even first.")
        return
    lessons, wt_label = _lessons_for_date(today)
    day_str = datetime.now(SGT).strftime("%A, %-d %B")
    await reply(update, f"*{day_str} ({wt_label} week)*\n\n{tt.format_lessons(lessons)}", parse_mode="Markdown")

async def setweek_cmd(update, context):
    if not google_ok():
        await update.message.reply_text("Google not connected.")
        return
    arg = context.args[0].lower() if context.args else ""
    if arg not in ("odd", "even", "o", "e"):
        await update.message.reply_text("Usage: /setweek odd or /setweek even")
        return
    wt = "odd" if arg in ("odd", "o") else "even"
    today = datetime.now(SGT).date()
    monday = (today - timedelta(days=today.weekday())).isoformat()
    try:
        gs.set_config("week_ref_date", monday)
        gs.set_config("week_ref_type", wt)
        await update.message.reply_text(f"This week is *{wt.upper()}* week.", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

async def today_cmd(update, context):
    today = datetime.now(SGT).date()
    day_str = datetime.now(SGT).strftime("%A, %-d %B %Y")
    lines = [f"*{day_str}*\n"]
    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"*Lessons ({wt_label} week):*")
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
        lines.append(f"*Lessons ({wt_label} week):*")
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
            "Categories: profile, preferences, people, places, projects",
            parse_mode="Markdown")
        return
    if "|" in text:
        category, memory_text = [p.strip() for p in text.split("|", 1)]
    else:
        category, memory_text = "profile", text
    try:
        gs.add_memory(category, memory_text)
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

async def handle_photo(update, context):
    """Send photo to Claude vision."""
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        buf = io.BytesIO()
        await file.download_to_memory(buf)
        image_data = base64.b64encode(buf.getvalue()).decode()
        caption = update.message.caption or "What is this? Help me with whatever is relevant to my work as an educator, developer, or entrepreneur."
        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT(),
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": image_data}},
                {"type": "text", "text": caption}
            ]}]
        )
        await reply(update, resp.content[0].text)
    except Exception as e:
        logger.error(f"Photo error: {e}")
        await update.message.reply_text(f"Could not process photo: {e}")

async def handle_document(update, context):
    """Handle PDF and other documents — send to Claude for analysis."""
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    doc = update.message.document
    caption = update.message.caption or "Summarise this document. Extract any key dates, deadlines, or action items relevant to my work."
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

        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=SYSTEM_PROMPT(),
            messages=[{"role": "user", "content": [
                content_block,
                {"type": "text", "text": caption}
            ]}]
        )
        await reply(update, resp.content[0].text)
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
            gs.add_memory(inp.get("category", "profile"), inp["text"])
            return f"Remembered under {inp.get('category', 'profile')}: {inp['text']}"
        except Exception as e:
            return f"Failed to remember: {e}"

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

    history = get_history(user_id)
    history.append({"role": "user", "content": text})
    if len(history) > MAX_TURNS:
        history = history[-MAX_TURNS:]

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    try:
        # Always include core assistant tools; add search if key is set
        tools = [CONTEXT_TOOL, CALENDAR_TOOL, REMINDER_TOOL, MEMORY_TOOL, PROJECT_TOOL]
        if ss.search_enabled():
            tools.append(SEARCH_TOOL)

        # Agentic loop — Claude may call multiple tools before giving a final reply
        messages = list(history)
        reply_text = ""
        max_iterations = 5

        for _ in range(max_iterations):
            resp = claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1024,
                system=SYSTEM_PROMPT(),
                tools=tools,
                messages=messages
            )

            if resp.stop_reason != "tool_use":
                reply_text = next((b.text for b in resp.content if b.type == "text"), "Done.")
                break

            # Process all tool calls in this response
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
    app.add_handler(CommandHandler("projects", projects_cmd))
    app.add_handler(CommandHandler("update",   update_cmd))
    app.add_handler(CommandHandler("briefing", briefing_cmd))
    app.add_handler(CommandHandler("agenda",   agenda_cmd))
    app.add_handler(CommandHandler("remember", remember_cmd))
    app.add_handler(CommandHandler("memory",   memory_cmd))
    app.add_handler(CommandHandler("forget",   forget_cmd))
    app.add_handler(CommandHandler("search",   search_cmd))
    app.add_handler(CommandHandler("addcal",   addcal_cmd))
    app.add_handler(CommandHandler("clear",    clear_cmd))
    app.add_handler(MessageHandler(filters.PHOTO,        handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    jq = app.job_queue
    jq.run_daily(morning_briefing_job, time=dt_time(7, 0, 0, tzinfo=SGT), name="morning_briefing")
    jq.run_daily(friday_checkin_job,   time=dt_time(17, 0, 0, tzinfo=SGT), days=(4,), name="friday_checkin")
    logger.info("Herwanto OS running — all systems active.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
