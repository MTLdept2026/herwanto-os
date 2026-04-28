from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
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


def _require_token(x_hira_token: Optional[str] = Header(default=None)):
    expected = os.environ.get("HIRA_WEB_TOKEN", "").strip()
    if expected and x_hira_token != expected:
        raise HTTPException(status_code=401, detail="Invalid Hira web token")


@app.get("/")
def index():
    return FileResponse(PWA_DIR / "index.html")


@app.get("/manifest.webmanifest")
def manifest():
    return FileResponse(PWA_DIR / "manifest.webmanifest", media_type="application/manifest+json")


@app.get("/service-worker.js")
def service_worker():
    return FileResponse(PWA_DIR / "service-worker.js", media_type="application/javascript")


@app.post("/api/chat")
async def chat(req: ChatRequest, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")

    history_key = "pwa"
    history = bot.get_history(history_key)
    user_content = message
    if bot.re.search(r"\b(?:work|moe|school|personal)\s+(?:gmail|email|emails|mail)\b", message, bot.re.I):
        account_hint, _ = bot._extract_gmail_account_from_text(message)
        user_content = f"{message}\n\n[Email account hint: use account=\"{account_hint}\" for Gmail tools.]"
    history.append({"role": "user", "content": user_content})
    history = history[-bot.MAX_TURNS:]
    reply_text = await bot._run_agentic_claude(list(history), max_tokens=1200)
    history.append({"role": "assistant", "content": reply_text})
    bot.save_history(history_key, history[-bot.MAX_TURNS:])
    return {"reply": reply_text}


@app.get("/api/agenda")
def agenda(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return {"text": bot.build_agenda(days)}


@app.get("/api/tasks")
def tasks(days: int = 7, x_hira_token: Optional[str] = Header(default=None)):
    _require_token(x_hira_token)
    return {"text": bot.build_task_brief(days)}


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

    try:
        kind, index_note, excerpt = docs.extract_supported_document(
            data,
            file.content_type or "",
            filename=file.filename or "",
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
