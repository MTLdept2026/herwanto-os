from __future__ import annotations

import os
import re
import time
from datetime import date, datetime

import requests
import pytz


SGT = pytz.timezone("Asia/Singapore")
DROPBOX_API = "https://api.dropboxapi.com/2"
DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
_TOKEN_CACHE: dict = {}
CLASSOPS_CLASSES = {"1G2", "2G3", "3G3", "4NT"}


def configured() -> bool:
    return all(
        os.environ.get(key, "").strip()
        for key in ("DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN")
    )


def _root_path() -> str:
    root = os.environ.get("DROPBOX_CLASSOPS_ROOT", "").strip()
    if not root or root == "/":
        return ""
    return root if root.startswith("/") else f"/{root}"


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


def _list_folder(path: str, recursive: bool = True, limit: int = 500) -> list[dict]:
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
    match = re.search(r"(?<!\d)(\d{1,2})[.\-_/ ](\d{1,2})[.\-_/ ](\d{2,4})(?!\d)", clean)
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


def _folder_sort_key(folder: dict) -> tuple:
    date_value = folder.get("date") or "9999-12-31"
    return (date_value, str(folder.get("folder", "")).lower())


def scan_classops_manifest() -> dict:
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
        date_info = parse_classops_date_folder(folder_parts[0] if folder_parts else "")
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
    return {
        "ok": True,
        "generated_at": datetime.now(SGT).isoformat(),
        "root": root or "/",
        "class_count": len(classes),
        "folder_count": len(folders),
        "file_count": len(files),
        "classes": classes,
    }
