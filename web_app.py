from __future__ import annotations

import base64
import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import bot
import document_service as docs


APP_DIR = Path(__file__).resolve().parent
PWA_DIR = APP_DIR / "pwa"

app = FastAPI(title="Hira OS")
app.mount("/static", StaticFiles(directory=str(PWA_DIR)), name="static")


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


def _history_key(client_id: str | None) -> str:
    clean = (client_id or "").strip()
    return f"pwa:{clean}" if clean else "pwa"


def _safe_text(builder, fallback: str) -> str:
    try:
        return builder()
    except Exception:
        return fallback


def _service_status() -> dict:
    return {
        "google": bot.google_ok(),
        "calendar": bot.google_ok(),
        "personal_gmail": bot.gs.gmail_ok("personal"),
        "work_gmail": bot.gs.gmail_ok("work"),
    }


def _marking_summary() -> dict:
    try:
        tasks = bot.gs.get_marking_tasks()
    except Exception:
        return {
            "active_stacks": 0,
            "total_scripts": 0,
            "marked_scripts": 0,
            "unmarked_scripts": 0,
            "connected": False,
        }

    total_scripts = sum(max(0, int(task.get("total_scripts") or 0)) for task in tasks)
    marked_scripts = sum(max(0, int(task.get("marked_count") or 0)) for task in tasks)
    marked_scripts = min(marked_scripts, total_scripts) if total_scripts else marked_scripts
    return {
        "active_stacks": len(tasks),
        "total_scripts": total_scripts,
        "marked_scripts": marked_scripts,
        "unmarked_scripts": max(0, total_scripts - marked_scripts),
        "connected": True,
    }


def _require_token(x_hira_token: Optional[str] = Header(default=None)):
    expected = os.environ.get("HIRA_WEB_TOKEN", "").strip()
    if expected and x_hira_token != expected:
        raise HTTPException(status_code=401, detail="Invalid Hira web token")


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


@app.get("/api/home")
def home(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    now = datetime.now(bot.SGT)
    return {
        "greeting": now.strftime("%A, %-d %B"),
        "time_label": now.strftime("%H:%M SGT"),
        "agenda": _safe_text(lambda: bot.build_agenda(days), "Agenda unavailable right now."),
        "tasks": _safe_text(lambda: bot.build_task_brief(days), "Task brief unavailable until Google is connected."),
        "files": _safe_text(lambda: bot.build_files_index(), "File memory unavailable until Google is connected."),
        "services": _service_status(),
        "marking": _marking_summary(),
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
    user_content = message
    if bot.re.search(r"\b(?:work|moe|school|personal)\s+(?:gmail|email|emails|mail)\b", message, bot.re.I):
        account_hint, _ = bot._extract_gmail_account_from_text(message)
        user_content = f"{message}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
    history.append({"role": "user", "content": user_content})
    history = history[-bot.MAX_TURNS:]

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
                "message": "Hira hit a backend snag. Try again in a moment.",
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


@app.post("/api/tasks/{task_id}/done")
def task_done(task_id: str, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    try:
        ok = bot.gs.mark_done(task_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not complete task: {exc}") from exc
    if not ok:
        raise HTTPException(status_code=404, detail=f"Task #{task_id} not found")
    return {"ok": True}


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
