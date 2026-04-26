import os
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

SGT = pytz.timezone("Asia/Singapore")
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

claude = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
histories = defaultdict(list)
MAX_TURNS = 20

SYSTEM_PROMPT = """You are Herwanto's personal AI assistant. Singapore-based. He wears three hats:

1. EDUCATOR — Bahasa Melayu teacher at Naval Base Secondary School (NBSS). Form teacher of 1 Flagship. Teaches ML to Sec 1, 2, 3, and 4 groups. Runs the school Football CCA. When the topic involves students, lessons, or pedagogy, respond as a specialist. Use DBP conventions for all BM content.

2. APP DEVELOPER — Solo developer. Stack: React + Vite, Capacitor, Netlify, GitHub, Python. Active projects: GamePlan (sports CCA website service for Singapore schools) and Ruh (Islamic spiritual app, currently in App Store review). When he pastes code, debug immediately without preamble.

3. ENTREPRENEUR — Building GamePlan and Ruh as commercial products. Help with marketing, pricing, business decisions, and growth strategy for the Singapore market.

Behaviour rules:
- Be concise. No filler, no preamble.
- Infer his hat from context - never ask.
- For code: fix first, explain if needed.
- For BM: proper DBP spelling and grammar always.
- For business: give a direct recommendation, not just options.
- Singapore English and local context always apply.
"""

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

def build_briefing():
    now = datetime.now(SGT)
    today = now.date()
    lines = [f"Good morning, Herwanto!\n_{now.strftime('%A, %-d %B %Y')}_\n"]

    lessons, wt_label = _lessons_for_date(today)
    if wt_label:
        lines.append(f"*Today's lessons ({wt_label} week):*")
        lines.append(tt.format_lessons(lessons))
    elif google_ok():
        lines.append("_Timetable: use /setweek odd or /setweek even to activate_")
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
            week_str = (today + timedelta(days=7)).strftime("%Y-%m-%d")
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

    lines.append("\nHave a productive day!")
    return "\n".join(lines)

async def start(update, context):
    chat_id = str(update.effective_chat.id)
    if google_ok():
        try:
            gs.set_config("chat_id", chat_id)
        except Exception as e:
            logger.warning(f"Could not store chat_id: {e}")
    g = "Google connected" if google_ok() else "Google not connected (see README)"
    await reply(update,
        f"Assalamualaikum Herwanto\n\n{g}\n\n"
        f"*School timetable*\n/lessons /setweek odd|even\n\n"
        f"*Calendar*\n/today /tomorrow /week\n\n"
        f"*Reminders*\n/due /remind /done\n\n"
        f"*Projects*\n/projects /update\n\n"
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
        await update.message.reply_text("Week type not set.\nUse /setweek odd or /setweek even first.", parse_mode="Markdown")
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
        await update.message.reply_text("Usage: /setweek odd or /setweek even", parse_mode="Markdown")
        return
    wt = "odd" if arg in ("odd", "o") else "even"
    today = datetime.now(SGT).date()
    monday = (today - timedelta(days=today.weekday())).isoformat()
    try:
        gs.set_config("week_ref_date", monday)
        gs.set_config("week_ref_type", wt)
        await update.message.reply_text(f"This week is *{wt.upper()}* week. Auto-calculating all future weeks from here.", parse_mode="Markdown")
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
        await reply(update,
            "Usage: `/remind Description | YYYY-MM-DD | Category`\nCategories: Teaching, CCA, GamePlan, Ruh, Personal",
            parse_mode="Markdown")
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
            await update.message.reply_text("No projects tracked. Use /update to add one.")
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
        await reply(update,
            "Usage: `/update Project | Status | Milestone | Date | Notes`",
            parse_mode="Markdown")
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

async def clear_cmd(update, context):
    histories[update.effective_user.id] = []
    await update.message.reply_text("Cleared.")

async def handle_message(update, context):
    user_id = update.effective_user.id
    text = update.message.text
    histories[user_id].append({"role": "user", "content": text})
    if len(histories[user_id]) > MAX_TURNS:
        histories[user_id] = histories[user_id][-MAX_TURNS:]
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=histories[user_id])
        assistant_text = resp.content[0].text
        histories[user_id].append({"role": "assistant", "content": assistant_text})
        await reply(update, assistant_text)
    except Exception as e:
        logger.error(f"Claude error: {e}")
        await update.message.reply_text("Error. Try again.")

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
    app.add_handler(CommandHandler("clear",    clear_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    jq = app.job_queue
    jq.run_daily(morning_briefing_job, time=dt_time(7, 0, 0, tzinfo=SGT), name="morning_briefing")
    jq.run_daily(friday_checkin_job,   time=dt_time(17, 0, 0, tzinfo=SGT), days=(4,), name="friday_checkin")
    logger.info("Herwanto OS running - timetable loaded, scheduler active.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
