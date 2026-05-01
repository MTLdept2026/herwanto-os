from __future__ import annotations

import base64
import asyncio
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import bot
import document_service as docs


APP_DIR = Path(__file__).resolve().parent
PWA_DIR = APP_DIR / "pwa"

app = FastAPI(title="H.I.R.A OS")
app.mount("/static", StaticFiles(directory=str(PWA_DIR)), name="static")

try:
    _HOME_EXECUTOR_WORKERS = int(os.environ.get("HIRA_HOME_WORKERS", "1"))
except ValueError:
    _HOME_EXECUTOR_WORKERS = 1
_HOME_EXECUTOR_WORKERS = max(1, min(2, _HOME_EXECUTOR_WORKERS))
_HOME_EXECUTOR = ThreadPoolExecutor(max_workers=_HOME_EXECUTOR_WORKERS)
_WEB_SCHEDULER_TASKS: list[asyncio.Task] = []


@app.middleware("http")
async def add_static_cache_headers(request: Request, call_next):
    response = await call_next(request)
    if request.url.path in {"/", "/service-worker.js", "/app.js", "/styles.css", "/static/app.js", "/static/styles.css"}:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


async def _web_daily_briefing_loop(hour: int, minute: int, sender, source: str):
    while True:
        try:
            now = datetime.now(bot.SGT)
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if now >= target:
                target = target + bot.timedelta(days=1)
            sleep_for = max(60, min(1800, (target - now).total_seconds()))
            await asyncio.sleep(sleep_for)
            now = datetime.now(bot.SGT)
            if now.hour == hour and now.minute == minute:
                await sender(context=None, source=source)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web {source} scheduler error: {exc}")
            await asyncio.sleep(300)


async def _web_prayer_reminder_loop():
    while True:
        try:
            due = bot._prayer_reminder_due(datetime.now(bot.SGT))
            if due:
                text = f"*Prayer reminder*\n\n{due['label']} entered at {due['time']}. {due['note']}"
                bot._queue_app_notification("reminder", f"{due['label']} prayer", text, source=f"web_prayer:{due['key']}")
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bot.logger.warning(f"Web prayer reminder scheduler error: {exc}")
            await asyncio.sleep(300)


@app.on_event("startup")
async def start_web_scheduler():
    global _WEB_SCHEDULER_TASKS
    enabled = os.environ.get("HIRA_WEB_MORNING_BRIEFING", "1").strip().lower() not in {"0", "false", "no", "off"}
    evening_enabled = os.environ.get("HIRA_WEB_EVENING_BRIEFING", "1").strip().lower() not in {"0", "false", "no", "off"}
    prayer_enabled = os.environ.get("HIRA_WEB_PRAYER_REMINDERS", "1").strip().lower() not in {"0", "false", "no", "off"}
    if _WEB_SCHEDULER_TASKS:
        return
    if enabled:
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(
            _web_daily_briefing_loop(7, 0, bot.send_morning_briefing_once, "web_morning_briefing")
        ))
    if evening_enabled:
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(
            _web_daily_briefing_loop(21, 0, bot.send_evening_briefing_once, "web_evening_briefing")
        ))
    if prayer_enabled:
        _WEB_SCHEDULER_TASKS.append(asyncio.create_task(_web_prayer_reminder_loop()))


@app.on_event("shutdown")
async def stop_web_scheduler():
    global _WEB_SCHEDULER_TASKS
    for task in _WEB_SCHEDULER_TASKS:
        task.cancel()
    _WEB_SCHEDULER_TASKS = []


class ChatRequest(BaseModel):
    message: str


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


def _history_key(client_id: str | None) -> str:
    clean = (client_id or "").strip()
    return f"pwa:{clean}" if clean else "pwa"


def _safe_text(builder, fallback: str) -> str:
    try:
        return builder()
    except Exception:
        return fallback


def _parallel_home_data(days: int) -> dict:
    jobs = {
        "agenda": lambda: bot.build_agenda(days),
        "daily_load": lambda: bot.build_daily_load(days),
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
        "tasks": "Task brief unavailable until Google is connected.",
        "islamic": "Islamic rhythm unavailable right now.",
        "files": "File memory unavailable until Google is connected.",
        "services": {
            "google": False,
            "calendar": False,
            "personal_gmail": False,
            "work_gmail": False,
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
    wait(futures.values(), timeout=12)
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
        "personal_gmail": bot.gs.gmail_ok("personal"),
        "work_gmail": bot.gs.gmail_ok("work"),
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


def _require_token(x_hira_token: Optional[str] = Header(default=None)):
    expected = os.environ.get("HIRA_WEB_TOKEN", "").strip()
    if expected and x_hira_token != expected:
        raise HTTPException(status_code=401, detail="Invalid H.I.R.A web token")


def _client_key(client_id: str | None) -> str:
    clean = (client_id or "").strip()
    return clean or "pwa"


@app.get("/")
def index():
    return FileResponse(PWA_DIR / "index.html")


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


@app.get("/api/home")
def home(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    now = datetime.now(bot.SGT)
    data = _parallel_home_data(days)
    return {
        "greeting": now.strftime("%A, %-d %B"),
        "time_label": now.strftime("%H:%M SGT"),
        **data,
    }


@app.post("/api/chat")
async def chat(
    req: ChatRequest,
    x_hira_token: Optional[str] = Header(default=None),
    x_hira_client: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")

    history_key = _history_key(x_hira_client)
    history = bot.get_history(history_key)
    bot.absorb_taste_hint(message)
    user_content = message
    if bot.re.search(r"\b(?:work|moe|school|personal)\s+(?:gmail|email|emails|mail)\b", message, bot.re.I):
        account_hint, _ = bot._extract_gmail_account_from_text(message)
        user_content = f"{message}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
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

            yield sse({"type": "route", "name": "quick"})
            yield sse({"type": "text", "text": quick_checkin_reply})
            yield sse({"type": "done", "text": quick_checkin_reply})
            yield sse({"type": "saved"})

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
            tools = [] if quick else bot.pwa_tools_for_message(message)
            if not quick:
                yield sse({"type": "tools", "count": len(tools), "names": [tool["name"] for tool in tools]})
            stream = (
                bot.stream_quick_pwa_reply(list(history[:-1]), message)
                if quick
                else bot.stream_agentic_claude(list(history), max_tokens=650, tools=tools)
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
            yield sse(timing("saved"))
            yield sse({"type": "saved"})
        except Exception as exc:
            bot.logger.exception(f"PWA chat failed: {exc}")
            yield sse({
                "type": "error",
                "message": "H.I.R.A hit a backend snag. Try again in a moment.",
            })

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
        marked = bot.gs.mark_app_notifications_seen(_client_key(x_hira_client), req.ids)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not update notifications: {exc}") from exc
    return {"ok": True, "marked": marked}


@app.post("/api/notifications/archive")
def notifications_archive(
    req: NotificationSeenRequest,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        archived = bot.gs.archive_app_notifications(req.ids)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not dismiss notifications: {exc}") from exc
    return {"ok": True, "archived": archived}


@app.post("/api/insights/feedback")
def insight_feedback(
    req: InsightFeedbackRequest,
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    try:
        feedback = bot.gs.add_insight_feedback(req.kind, req.target, req.rating, req.note)
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
    account = bot._normalise_gmail_account(req.account)
    if not bot.gs.gmail_ok(account):
        raise HTTPException(status_code=400, detail=f"{bot.gs.gmail_label(account).title()} is not connected")
    messages = bot.gs.list_gmail_messages(req.query, req.max_items, account=account)
    return {"account": account, "messages": messages}


@app.post("/api/gmail/draft")
def gmail_draft(req: DraftRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    account = bot._normalise_gmail_account(req.account)
    if not bot.gs.gmail_ok(account):
        raise HTTPException(status_code=400, detail=f"{bot.gs.gmail_label(account).title()} is not connected")
    draft = bot.gs.create_gmail_draft(req.to, req.subject, req.body, req.cc, account=account)
    return {"account": account, "draft_id": draft.get("id", "")}


@app.post("/api/upload")
async def upload_document(
    file: UploadFile = File(...),
    note: str = "",
    x_hira_token: Optional[str] = Header(default=None),
):
    _require_token(x_hira_token)
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")

    mime = (file.content_type or "").lower()
    filename = file.filename or ""

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

    try:
        kind, index_note, excerpt = docs.extract_supported_document(
            data,
            mime,
            filename=filename,
            caption=note,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

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
