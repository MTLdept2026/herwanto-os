from __future__ import annotations

import os
import copy
import json
import re
import threading
import time
import zipfile
from datetime import date, datetime
from html.parser import HTMLParser
from io import BytesIO
from urllib.parse import quote
from xml.etree import ElementTree as ET

import requests
import pytz


SGT = pytz.timezone("Asia/Singapore")
DROPBOX_API = "https://api.dropboxapi.com/2"
DROPBOX_CONTENT_API = "https://content.dropboxapi.com/2"
DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
_TOKEN_CACHE: dict = {}
_TITLE_CACHE: dict[str, str] = {}
_CLASSOPS_MANIFEST_CACHE: dict = {"manifest": None, "stored_at": 0.0}
_CLASSOPS_MANIFEST_LOCK = threading.Lock()
CLASSOPS_CLASSES = {"1G2", "2G3", "3G3", "4NT"}
CONTENT_EXTENSIONS = {
    ".html": "mini-site",
    ".htm": "mini-site",
    ".pdf": "pdf",
    ".docx": "worksheet/doc",
    ".doc": "worksheet/doc",
    ".pptx": "slides",
    ".ppt": "slides",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".mp4": "video",
}
COLLECT_TERMS = (
    "collect",
    "collection",
    "submit",
    "submission",
    "homework",
    "tugasan",
    "latihan",
    "worksheet",
    "lembaran",
    "karangan",
    "kefahaman",
)
REFERENCE_SKIP_TERMS = ("reference", "rujukan", "answer", "answers", "jawapan", "scheme", "skema", "teacher")
FILING_EXTENSIONS = {".html", ".htm", ".pdf", ".docx", ".doc", ".pptx", ".ppt"}
TITLE_INSPECT_EXTENSIONS = {".html", ".htm", ".docx"}
TITLE_MAX_BYTES = int(os.environ.get("DROPBOX_CLASSOPS_TITLE_MAX_BYTES", "2500000") or 2500000)
CONTENT_PURPOSES = {
    "lesson_page": {"label": "Lesson page", "tone": "lesson", "rank": 10},
    "submission_task": {"label": "Submission task", "tone": "task", "rank": 20},
    "worksheet": {"label": "Worksheet", "tone": "task", "rank": 30},
    "notes": {"label": "Notes", "tone": "resource", "rank": 40},
    "slides": {"label": "Slides", "tone": "resource", "rank": 50},
    "media": {"label": "Media", "tone": "resource", "rank": 60},
    "resource": {"label": "Resource", "tone": "resource", "rank": 90},
}


class _MiniSiteTitleParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._stack: list[str] = []
        self.title_parts: list[str] = []
        self.h1_parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        self._stack.append(str(tag or "").lower())

    def handle_endtag(self, tag):
        tag = str(tag or "").lower()
        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index] == tag:
                del self._stack[index:]
                return

    def handle_data(self, data):
        current = self._stack[-1] if self._stack else ""
        text = " ".join(str(data or "").split())
        if not text:
            return
        if current == "title":
            self.title_parts.append(text)
        elif current == "h1":
            self.h1_parts.append(text)

    @property
    def title(self) -> str:
        h1 = " ".join(self.h1_parts).strip()
        if h1:
            return h1
        return " ".join(self.title_parts).strip()


def configured() -> bool:
    return all(
        os.environ.get(key, "").strip()
        for key in ("DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN")
    )


def _inspect_titles_enabled() -> bool:
    return os.environ.get("DROPBOX_CLASSOPS_INSPECT_TITLES", "").strip().lower() in {"1", "true", "yes", "on"}


def _root_path() -> str:
    root = os.environ.get("DROPBOX_CLASSOPS_ROOT", "").strip()
    if not root or root == "/":
        return ""
    return root if root.startswith("/") else f"/{root}"


def _dropbox_path_from_relative(path: str) -> str:
    rel = str(path or "").strip().lstrip("/")
    root = _root_path().strip("/")
    parts = [part for part in (root, rel) if part]
    return "/" + "/".join(parts) if parts else ""


def _access_token() -> str:
    if not configured():
        raise RuntimeError("Dropbox ClassOps env vars are not configured.")
    now = time.time()
    cached = _TOKEN_CACHE.get("access_token", "")
    expires_at = float(_TOKEN_CACHE.get("expires_at", 0) or 0)
    if cached and expires_at - now > 120:
        return cached
    resp = requests.post(
        DROPBOX_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["DROPBOX_REFRESH_TOKEN"],
            "client_id": os.environ["DROPBOX_APP_KEY"],
            "client_secret": os.environ["DROPBOX_APP_SECRET"],
        },
        timeout=12,
    )
    resp.raise_for_status()
    data = resp.json()
    token = str(data.get("access_token", "")).strip()
    if not token:
        raise RuntimeError("Dropbox token refresh returned no access_token.")
    _TOKEN_CACHE["access_token"] = token
    _TOKEN_CACHE["expires_at"] = now + int(data.get("expires_in", 3600) or 3600)
    return token


def _post(endpoint: str, payload: dict) -> dict:
    resp = requests.post(
        f"{DROPBOX_API}{endpoint}",
        headers={
            "Authorization": f"Bearer {_access_token()}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def _download_file(path: str) -> bytes:
    resp = requests.post(
        f"{DROPBOX_CONTENT_API}/files/download",
        headers={
            "Authorization": f"Bearer {_access_token()}",
            "Dropbox-API-Arg": json.dumps({"path": path}),
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.content


def download_file(path: str) -> bytes:
    dropbox_path = _dropbox_path_from_relative(path)
    if not dropbox_path:
        raise ValueError("Dropbox file path is required.")
    return _download_file(dropbox_path)


def get_file_link(path: str) -> dict:
    dropbox_path = _dropbox_path_from_relative(path)
    if not dropbox_path:
        raise ValueError("Dropbox file path is required.")
    try:
        data = _post("/files/get_temporary_link", {"path": dropbox_path})
        link = str(data.get("link") or "").strip()
        if link:
            return {"url": link, "kind": "temporary_link", "path": path}
    except Exception:
        pass
    folder, _, filename = str(path or "").strip("/").rpartition("/")
    if filename:
        url = f"https://www.dropbox.com/home/{quote(folder, safe='/')}?preview={quote(filename)}" if folder else f"https://www.dropbox.com/home?preview={quote(filename)}"
    else:
        url = f"https://www.dropbox.com/home/{quote(folder or path)}"
    return {"url": url, "kind": "dropbox_web", "path": path}


def _classops_manifest_ttl_seconds() -> int:
    try:
        return max(0, int(os.environ.get("DROPBOX_CLASSOPS_MANIFEST_CACHE_SECONDS", "600") or 600))
    except ValueError:
        return 600


def clear_classops_manifest_cache():
    with _CLASSOPS_MANIFEST_LOCK:
        _CLASSOPS_MANIFEST_CACHE["manifest"] = None
        _CLASSOPS_MANIFEST_CACHE["stored_at"] = 0.0


def classops_manifest_cache_status() -> dict:
    with _CLASSOPS_MANIFEST_LOCK:
        stored_at = float(_CLASSOPS_MANIFEST_CACHE.get("stored_at", 0.0) or 0.0)
        has_manifest = isinstance(_CLASSOPS_MANIFEST_CACHE.get("manifest"), dict)
    age = max(0.0, time.time() - stored_at) if stored_at else 0.0
    ttl = _classops_manifest_ttl_seconds()
    return {
        "available": has_manifest,
        "stored_at": datetime.fromtimestamp(stored_at, SGT).isoformat() if stored_at else "",
        "age_seconds": round(age),
        "ttl_seconds": ttl,
        "fresh": bool(has_manifest and ttl > 0 and age <= ttl),
    }


def _cached_classops_manifest(max_age_seconds: int, allow_stale: bool = False) -> tuple[dict | None, bool]:
    with _CLASSOPS_MANIFEST_LOCK:
        manifest = _CLASSOPS_MANIFEST_CACHE.get("manifest")
        stored_at = float(_CLASSOPS_MANIFEST_CACHE.get("stored_at", 0.0) or 0.0)
    if not isinstance(manifest, dict) or not stored_at:
        return None, False
    age = max(0.0, time.time() - stored_at)
    fresh = max_age_seconds > 0 and age <= max_age_seconds
    if not fresh and not allow_stale:
        return None, False
    result = copy.deepcopy(manifest)
    result["cache"] = {
        "hit": True,
        "stale": not fresh,
        "stored_at": datetime.fromtimestamp(stored_at, SGT).isoformat(),
        "age_seconds": round(age),
        "ttl_seconds": max_age_seconds,
    }
    return result, True


def _store_classops_manifest(manifest: dict) -> dict:
    stored_at = time.time()
    clean = copy.deepcopy(manifest)
    clean.pop("cache", None)
    with _CLASSOPS_MANIFEST_LOCK:
        _CLASSOPS_MANIFEST_CACHE["manifest"] = clean
        _CLASSOPS_MANIFEST_CACHE["stored_at"] = stored_at
    result = copy.deepcopy(clean)
    result["cache"] = {
        "hit": False,
        "stale": False,
        "stored_at": datetime.fromtimestamp(stored_at, SGT).isoformat(),
        "age_seconds": 0,
        "ttl_seconds": _classops_manifest_ttl_seconds(),
    }
    return result


def _list_folder(path: str, recursive: bool = True, limit: int = 2000) -> list[dict]:
    data = _post(
        "/files/list_folder",
        {
            "path": path,
            "recursive": recursive,
            "include_media_info": False,
            "include_deleted": False,
            "include_has_explicit_shared_members": False,
            "limit": max(1, min(int(limit or 500), 2000)),
        },
    )
    entries = list(data.get("entries", []) or [])
    while data.get("has_more"):
        data = _post("/files/list_folder/continue", {"cursor": data.get("cursor", "")})
        entries.extend(data.get("entries", []) or [])
    return entries


def parse_classops_date_folder(name: str) -> dict:
    clean = str(name or "").strip()
    match = re.search(r"(?<!\d)(\d{1,2})[.\-_/ :](\d{1,2})[.\-_/ :](\d{2,4})(?!\d)", clean)
    if not match:
        return {"date": "", "label": clean, "matched": False}
    day, month, year = (int(part) for part in match.groups())
    if year < 100:
        year += 2000
    try:
        parsed = date(year, month, day)
    except ValueError:
        return {"date": "", "label": clean, "matched": False}
    label = (clean[: match.start()] + clean[match.end():]).strip(" -_./")
    return {
        "date": parsed.isoformat(),
        "label": label,
        "matched": True,
        "original": clean,
    }


def _date_info_from_folder_parts(folder_parts: list[str]) -> tuple[dict, int]:
    if not folder_parts:
        return {"date": "", "label": "", "matched": False}, 0
    first = str(folder_parts[0] or "").strip()
    direct = parse_classops_date_folder(first)
    if direct.get("matched"):
        return direct, 1
    if len(folder_parts) >= 3 and all(re.fullmatch(r"\d{1,4}", str(part or "").strip()) for part in folder_parts[:3]):
        nested = parse_classops_date_folder("/".join(folder_parts[:3]))
        if nested.get("matched"):
            return nested, 3
    return {"date": "", "label": first, "matched": False}, 0


def _folder_sort_key(folder: dict) -> tuple:
    date_value = folder.get("date") or "9999-12-31"
    return (date_value, str(folder.get("folder", "")).lower())


def _classops_sort_date(value: str) -> date | None:
    clean = str(value or "").strip()
    if not clean:
        return None
    try:
        return date.fromisoformat(clean[:10])
    except ValueError:
        pass
    parsed = parse_classops_date_folder(clean)
    if not parsed.get("matched"):
        return None
    try:
        return date.fromisoformat(str(parsed.get("date", "")))
    except ValueError:
        return None


def classops_content_sort_key(item: dict) -> tuple:
    parsed_date = _classops_sort_date(item.get("date", ""))
    return (
        0 if parsed_date else 1,
        -parsed_date.toordinal() if parsed_date else 0,
        str(item.get("folder", "") or "").lower(),
        int(item.get("purpose_rank") or 90),
        str(item.get("title", "") or item.get("name", "") or "").lower(),
        str(item.get("path", "") or "").lower(),
    )


def sort_classops_content_items(items: list[dict]) -> list[dict]:
    return sorted([dict(item) for item in items or []], key=classops_content_sort_key)


def _file_extension(name: str) -> str:
    clean = str(name or "").lower()
    if "." not in clean:
        return ""
    return clean[clean.rfind("."):]


def _file_kind(name: str) -> str:
    return CONTENT_EXTENSIONS.get(_file_extension(name), "file")


def _content_signal_text(*values: str) -> str:
    text = " ".join(str(value or "") for value in values)
    return " ".join(text.lower().replace("_", " ").replace("-", " ").split())


def infer_content_purpose(file_item: dict, collection: dict | None = None) -> dict:
    name = str(file_item.get("name") or "")
    title = str(file_item.get("filing_title") or "")
    kind = str(file_item.get("kind") or _file_kind(name))
    ext = _file_extension(name)
    signal = _content_signal_text(name, title)
    collection = collection if isinstance(collection, dict) else file_item.get("collection")
    is_collect = bool(collection.get("collect")) if isinstance(collection, dict) else False
    explicit_collection = any(term in signal for term in ("collect", "collection", "submit", "submission", "homework"))

    if is_collect and explicit_collection:
        purpose = "submission_task"
    elif ext in {".html", ".htm"} or any(term in signal for term in ("mini site", "minisite", "microsite")):
        purpose = "lesson_page"
    elif any(term in signal for term in ("note", "notes", "nota", "rujukan murid", "student notes", "handout", "bahan")):
        purpose = "notes"
    elif ext in {".doc", ".docx"}:
        purpose = "worksheet"
    elif kind == "slides" or ext == ".pdf" or any(term in signal for term in ("slides", "slide", "deck", "slaid")):
        purpose = "slides"
    elif any(term in signal for term in ("worksheet", "lembaran", "latihan", "tugasan", "karangan", "kefahaman")):
        purpose = "worksheet"
    elif kind in {"image", "video"}:
        purpose = "media"
    else:
        purpose = "resource"

    meta = CONTENT_PURPOSES[purpose]
    return {
        "id": purpose,
        "label": meta["label"],
        "tone": meta["tone"],
        "rank": meta["rank"],
        "trackable": purpose in {"submission_task", "worksheet"},
    }


def classops_content_purpose_from_id(purpose_id: str) -> dict:
    purpose = str(purpose_id or "").strip()
    meta = CONTENT_PURPOSES.get(purpose) or CONTENT_PURPOSES["resource"]
    return {
        "id": purpose if purpose in CONTENT_PURPOSES else "resource",
        "label": meta["label"],
        "tone": meta["tone"],
        "rank": meta["rank"],
        "trackable": (purpose if purpose in CONTENT_PURPOSES else "resource") in {"submission_task", "worksheet"},
    }


def _clean_title_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"\s+", " ", text.replace("\u00a0", " ")).strip(" \t\r\n-_|")
    return text


def _smart_title(value: str) -> str:
    text = _clean_title_text(value)
    if not text:
        return ""
    small = {"dan", "atau", "di", "ke", "dari", "daripada", "untuk", "yang", "dengan", "serta"}
    words = []
    for raw in text.split(" "):
        if not raw:
            continue
        if raw.isupper() and len(raw) <= 5:
            words.append(raw)
            continue
        lowered = raw.lower()
        if lowered in small and words:
            words.append(lowered)
            continue
        words.append(raw[:1].upper() + raw[1:].lower())
    return " ".join(words)


def infer_filing_title_from_filename(name: str) -> str:
    clean = re.sub(r"\.[A-Za-z0-9]{1,6}$", "", str(name or "")).strip()
    clean = re.sub(r"[_]+", " ", clean)
    clean = re.sub(r"\s+[-–—]\s+", " - ", clean)
    clean = re.sub(r"(?<=[a-z])[-–—](?=[a-z])", " ", clean)
    clean = re.sub(r"[-–—]", " - ", clean)
    clean = re.sub(r"\s+", " ", clean)
    clean = re.sub(r"(?<!\d)\d{1,2}[.\-_/ :]\d{1,2}[.\-_/ :]\d{2,4}(?!\d)", " ", clean)
    clean = re.sub(r"\b(?:collect|collection|submit|submission)\s+(?:next\s+(?:lesson|class)|by\s+\S+)\b", " ", clean, flags=re.I)
    clean = re.sub(r"\b(?:collect|collection|submit|submission)\b", " ", clean, flags=re.I)
    clean = re.sub(r"\b(?:1G2|2G3|3G3|4NT)\b", " ", clean, flags=re.I)
    clean = re.sub(r"\b(?:copy|final|updated|new)\b", " ", clean, flags=re.I)
    clean = re.sub(r"\s+", " ", clean).strip(" -_")
    return _smart_title(clean)


def _html_title(content: bytes) -> str:
    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            text = content.decode(encoding, errors="ignore")
            break
        except Exception:
            text = ""
    parser = _MiniSiteTitleParser()
    try:
        parser.feed(text[:200_000])
    except Exception:
        return ""
    title = _clean_title_text(parser.title)
    title = re.sub(r"\s*[|–—-]\s*(?:H\.?I\.?R\.?A|Canva|Google Docs?)\s*$", "", title, flags=re.I)
    return _smart_title(title)


def _docx_text_from_paragraph(paragraph) -> str:
    texts = []
    for node in paragraph.iter():
        if node.tag.endswith("}t") and node.text:
            texts.append(node.text)
    return _clean_title_text("".join(texts))


def _docx_title(content: bytes) -> str:
    try:
        with zipfile.ZipFile(BytesIO(content)) as docx:
            try:
                core = ET.fromstring(docx.read("docProps/core.xml"))
                for node in core.iter():
                    if node.tag.endswith("}title") and _clean_title_text(node.text or ""):
                        return _smart_title(node.text or "")
            except Exception:
                pass
            document = ET.fromstring(docx.read("word/document.xml"))
    except Exception:
        return ""
    first_paragraph = ""
    for paragraph in document.iter():
        if not paragraph.tag.endswith("}p"):
            continue
        text = _docx_text_from_paragraph(paragraph)
        if not text:
            continue
        if not first_paragraph:
            first_paragraph = text
        for node in paragraph.iter():
            if node.tag.endswith("}pStyle"):
                style = str(node.attrib.get("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}val", ""))
                if "Heading" in style or "Title" in style:
                    return _smart_title(text)
        if len(text) <= 90:
            return _smart_title(text)
    return _smart_title(first_paragraph)


def infer_filing_title(file_item: dict) -> str:
    name = str(file_item.get("name") or "")
    fallback = infer_filing_title_from_filename(name)
    ext = _file_extension(name)
    dropbox_path = str(file_item.get("dropbox_path") or "")
    size = int(file_item.get("size", 0) or 0)
    if not _inspect_titles_enabled() or ext not in TITLE_INSPECT_EXTENSIONS or not dropbox_path or size > TITLE_MAX_BYTES:
        return fallback
    cached = _TITLE_CACHE.get(dropbox_path)
    if cached:
        return cached
    try:
        content = _download_file(dropbox_path)
        title = _html_title(content) if ext in {".html", ".htm"} else _docx_title(content)
    except Exception:
        title = ""
    title = title or fallback
    if title:
        _TITLE_CACHE[dropbox_path] = title
    return title


def _is_filing_item(file_item: dict) -> bool:
    name = str(file_item.get("name") or "")
    ext = _file_extension(name)
    if ext not in FILING_EXTENSIONS:
        return False
    clean = " ".join(name.lower().replace("_", " ").replace("-", " ").split())
    return not any(term in clean for term in REFERENCE_SKIP_TERMS)


def infer_collection_hint(name: str) -> dict:
    clean = " ".join(str(name or "").lower().replace("_", " ").replace("-", " ").split())
    if not clean or any(term in clean for term in REFERENCE_SKIP_TERMS):
        return {"collect": False, "hint": "", "due": ""}
    due = ""
    if "next lesson" in clean or "next class" in clean:
        due = "next_lesson"
    date_match = parse_classops_date_folder(clean)
    if date_match.get("matched"):
        due = date_match.get("date", "")
    collect = any(term in clean for term in COLLECT_TERMS)
    hint = ""
    if collect:
        hint = "Collect"
        if due:
            hint += f" by {due.replace('_', ' ')}"
    return {"collect": collect, "hint": hint, "due": due}


def enrich_classops_manifest(manifest: dict) -> dict:
    current = dict(manifest or {})
    classes_out = []
    total_collection_candidates = 0
    total_content_items = 0
    total_lessons = 0
    total_undated_folders = 0
    for class_item in current.get("classes", []) or []:
        folders_out = []
        latest = None
        class_collection = []
        content_items = []
        undated_folders = []
        for folder in class_item.get("folders", []) or []:
            files_out = []
            candidates = []
            for file_item in folder.get("files", []) or []:
                next_file = dict(file_item)
                next_file["kind"] = _file_kind(next_file.get("name", ""))
                next_file["filing_title"] = infer_filing_title(next_file) if _is_filing_item(next_file) else ""
                collection = infer_collection_hint(next_file.get("name", ""))
                next_file["collection"] = collection
                purpose = infer_content_purpose(next_file, collection)
                next_file["purpose"] = purpose
                if collection.get("collect"):
                    candidates.append(next_file)
                if next_file.get("filing_title"):
                    content_items.append({
                        "title": next_file["filing_title"],
                        "date": folder.get("date", ""),
                        "folder": folder.get("folder", ""),
                        "path": next_file.get("path", ""),
                        "kind": next_file.get("kind", ""),
                        "purpose": purpose,
                        "purpose_id": purpose.get("id", "resource"),
                        "purpose_label": purpose.get("label", "Resource"),
                        "purpose_tone": purpose.get("tone", "resource"),
                        "purpose_rank": purpose.get("rank", 90),
                        "trackable": bool(purpose.get("trackable")),
                        "collection": collection,
                        "date_missing": not bool(folder.get("date")),
                    })
                files_out.append(next_file)
            next_folder = {
                **folder,
                "files": files_out,
                "collection_candidates": candidates,
                "resource_count": len(files_out),
            }
            folders_out.append(next_folder)
            if folder.get("date"):
                total_lessons += 1
                if not latest or str(folder.get("date", "")) > str(latest.get("date", "")):
                    latest = next_folder
            elif files_out:
                undated_folders.append({
                    "folder": next_folder.get("folder", ""),
                    "topic": next_folder.get("topic", ""),
                    "file_count": len(files_out),
                    "files": [file_item.get("name", "") for file_item in files_out[:5]],
                })
            class_collection.extend(candidates)
        folders_out = sorted(folders_out, key=_folder_sort_key)
        content_items = sort_classops_content_items(content_items)
        total_collection_candidates += len(class_collection)
        total_content_items += len(content_items)
        total_undated_folders += len(undated_folders)
        classes_out.append({
            **class_item,
            "folders": folders_out,
            "lesson_count": sum(1 for folder in folders_out if folder.get("date")),
            "latest_lesson": {
                "date": latest.get("date", "") if latest else "",
                "topic": latest.get("topic", "") if latest else "",
                "folder": latest.get("folder", "") if latest else "",
            },
            "collection_candidate_count": len(class_collection),
            "collection_candidates": class_collection[:12],
            "content_items": content_items,
            "content_item_count": len(content_items),
            "undated_folder_count": len(undated_folders),
            "undated_folders": undated_folders[:12],
        })
    current["classes"] = classes_out
    current["summary"] = {
        "class_count": len(classes_out),
        "lesson_count": total_lessons,
        "file_count": int(current.get("file_count", 0) or 0),
        "collection_candidate_count": total_collection_candidates,
        "content_item_count": total_content_items,
        "undated_folder_count": total_undated_folders,
    }
    return current


def _scan_classops_manifest_uncached() -> dict:
    root = _root_path()
    entries = _list_folder(root, recursive=True)
    folders = [item for item in entries if item.get(".tag") == "folder"]
    files = [item for item in entries if item.get(".tag") == "file"]
    class_map: dict[str, dict] = {}

    for item in files:
        rel = str(item.get("path_display", "") or item.get("path_lower", "") or "")
        if root and rel.lower().startswith(root.lower()):
            rel = rel[len(root):].lstrip("/")
        else:
            rel = rel.lstrip("/")
        parts = [part for part in rel.split("/") if part]
        raw_class = parts[0] if parts else "Unsorted"
        class_name = raw_class.upper()
        if CLASSOPS_CLASSES and class_name not in CLASSOPS_CLASSES:
            class_name = raw_class
        folder_parts = parts[1:-1]
        folder = "/".join(folder_parts) if folder_parts else ""
        date_info, date_part_count = _date_info_from_folder_parts(folder_parts)
        if date_part_count and not date_info.get("label"):
            date_info["label"] = " ".join(folder_parts[date_part_count:]).strip()
        bucket = class_map.setdefault(class_name, {"class": class_name, "folders": {}, "file_count": 0})
        bucket["file_count"] += 1
        folder_bucket = bucket["folders"].setdefault(
            folder or ".",
            {
                "folder": folder or ".",
                "date": date_info.get("date", ""),
                "topic": date_info.get("label", ""),
                "date_folder_matched": bool(date_info.get("matched")),
                "files": [],
            },
        )
        folder_bucket["files"].append({
            "name": item.get("name", ""),
            "path": rel,
            "dropbox_path": item.get("path_display", "") or item.get("path_lower", ""),
            "size": int(item.get("size", 0) or 0),
            "modified": item.get("server_modified", "") or item.get("client_modified", ""),
            "id": item.get("id", ""),
        })

    classes = []
    for bucket in class_map.values():
        folders_out = sorted(bucket["folders"].values(), key=_folder_sort_key)
        for folder in folders_out:
            folder["files"].sort(key=lambda value: value["name"].lower())
        classes.append({
            "class": bucket["class"],
            "file_count": bucket["file_count"],
            "folder_count": len(folders_out),
            "folders": folders_out,
        })
    classes.sort(key=lambda value: value["class"].lower())
    manifest = {
        "ok": True,
        "generated_at": datetime.now(SGT).isoformat(),
        "root": root or "/",
        "class_count": len(classes),
        "folder_count": len(folders),
        "file_count": len(files),
        "classes": classes,
    }
    return enrich_classops_manifest(manifest)


def scan_classops_manifest(force_refresh: bool = False, max_age_seconds: int | None = None, allow_stale: bool = False) -> dict:
    ttl = _classops_manifest_ttl_seconds() if max_age_seconds is None else max(0, int(max_age_seconds or 0))
    if not force_refresh:
        cached, _hit = _cached_classops_manifest(ttl, allow_stale=allow_stale)
        if cached is not None:
            return cached
    manifest = _scan_classops_manifest_uncached()
    return _store_classops_manifest(manifest)
