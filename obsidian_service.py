from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_VAULT_PATH = Path.home() / "Documents" / "Obsidian Vault"
OBSIDIAN_CONFIG_PATH = Path.home() / "Library" / "Application Support" / "obsidian" / "obsidian.json"
DEFAULT_INBOX_NOTE = "Inbox.md"
DEFAULT_MAX_SCAN_CHARS = 200_000
DEFAULT_MAX_READ_CHARS = 12_000

SYSTEM_EXCLUDED_DIRS = {
    ".git",
    ".obsidian",
    ".trash",
    ".trashes",
    "__pycache__",
}

DEFAULT_EXCLUDE_TERMS = (
    "31 classops",
    "classops",
    "private",
    "student sensitive",
    "student-sensitive",
    "student data",
    "student records",
    "student notes",
    "student profile",
    "student profiles",
    "classlist",
    "class list",
    "marks",
    "scores",
    "results",
)


@dataclass(frozen=True)
class VaultNote:
    path: Path
    relative_path: str
    title: str
    modified: datetime
    size: int


def _normalise_text(value: str) -> str:
    clean = re.sub(r"[\s_\-]+", " ", str(value or "").casefold())
    return re.sub(r"\s+", " ", clean).strip()


def _configured_path_from_obsidian() -> Path | None:
    try:
        raw = json.loads(OBSIDIAN_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    vaults = raw.get("vaults")
    if not isinstance(vaults, dict):
        return None
    candidates = []
    for item in vaults.values():
        if not isinstance(item, dict) or not item.get("path"):
            continue
        candidates.append(item)
    if not candidates:
        return None
    candidates.sort(key=lambda item: (bool(item.get("open")), int(item.get("ts") or 0)), reverse=True)
    return Path(str(candidates[0].get("path", ""))).expanduser()


def vault_path() -> Path:
    configured = os.environ.get("HIRA_OBSIDIAN_VAULT_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    return _configured_path_from_obsidian() or DEFAULT_VAULT_PATH


def _exclude_terms() -> tuple[str, ...]:
    extra = tuple(
        _normalise_text(item)
        for item in os.environ.get("HIRA_OBSIDIAN_EXTRA_EXCLUDE_TERMS", "").split(",")
        if _normalise_text(item)
    )
    return tuple(_normalise_text(item) for item in DEFAULT_EXCLUDE_TERMS) + extra


def _safe_join(relative_or_absolute: str, vault: Path | None = None) -> Path:
    base = (vault or vault_path()).expanduser().resolve()
    raw = str(relative_or_absolute or "").strip()
    if not raw:
        raise ValueError("Note path is empty.")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    resolved = candidate.resolve()
    try:
        resolved.relative_to(base)
    except ValueError as exc:
        raise ValueError("Path must stay inside the configured Obsidian vault.") from exc
    return resolved


def exclusion_reason(path: str | Path, vault: Path | None = None) -> str:
    base = (vault or vault_path()).expanduser().resolve()
    try:
        path_obj = Path(path).expanduser()
        if path_obj.is_absolute():
            relative = path_obj.resolve().relative_to(base)
        else:
            relative = path_obj
    except Exception:
        relative = Path(str(path))
    parts = [str(part) for part in relative.parts]
    if any(part.casefold() in SYSTEM_EXCLUDED_DIRS or part.startswith(".") for part in parts):
        return "system Obsidian folder"
    text = _normalise_text(" ".join(parts))
    for term in _exclude_terms():
        if re.search(rf"\b{re.escape(term)}\b", text):
            if "31 classops" in term or "classops" in term:
                return "31 ClassOps exclusion"
            if "private" in term:
                return "private-notes exclusion"
            return "student-sensitive exclusion"
    return ""


def _assert_vault_readable(vault: Path) -> None:
    if not vault.exists():
        raise FileNotFoundError(f"Obsidian vault not found: {vault}")
    if not vault.is_dir():
        raise NotADirectoryError(f"Obsidian vault path is not a directory: {vault}")
    try:
        next(vault.iterdir(), None)
    except PermissionError as exc:
        raise PermissionError(
            f"Obsidian vault is not readable by this process: {vault}. "
            "Grant Documents/Desktop access to the H.I.R.A process or set HIRA_OBSIDIAN_VAULT_PATH."
        ) from exc


def _iter_markdown_files(vault: Path | None = None):
    base = (vault or vault_path()).expanduser().resolve()
    _assert_vault_readable(base)
    try:
        walker = os.walk(base, topdown=True)
        for root, dirs, files in walker:
            root_path = Path(root)
            safe_dirs = []
            for dirname in dirs:
                rel_dir = root_path.joinpath(dirname).relative_to(base)
                if not exclusion_reason(rel_dir, base):
                    safe_dirs.append(dirname)
            dirs[:] = safe_dirs
            for filename in files:
                if not filename.lower().endswith(".md"):
                    continue
                path = root_path / filename
                rel = path.relative_to(base)
                if exclusion_reason(rel, base):
                    continue
                yield path
    except PermissionError as exc:
        raise PermissionError(
            f"Obsidian vault is not readable by this process: {base}. "
            "Grant Documents/Desktop access to the H.I.R.A process or set HIRA_OBSIDIAN_VAULT_PATH."
        ) from exc


def _note_from_path(path: Path, vault: Path) -> VaultNote:
    stat = path.stat()
    return VaultNote(
        path=path,
        relative_path=str(path.relative_to(vault)),
        title=path.stem,
        modified=datetime.fromtimestamp(stat.st_mtime, ZoneInfo("Asia/Singapore")),
        size=stat.st_size,
    )


def _read_text(path: Path, max_chars: int | None = None) -> str:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        if max_chars is None:
            return handle.read()
        return handle.read(max(1, int(max_chars)))


def _query_terms(query: str) -> list[str]:
    clean = _normalise_text(query)
    terms = [term for term in re.findall(r"[a-z0-9]+", clean) if len(term) >= 2]
    return list(dict.fromkeys(terms))


def _score_note(query: str, terms: list[str], note: VaultNote, content: str) -> tuple[int, str]:
    query_clean = _normalise_text(query)
    path_text = _normalise_text(f"{note.relative_path} {note.title}")
    content_text = _normalise_text(content)
    score = 0
    if query_clean and query_clean in path_text:
        score += 18
    if query_clean and query_clean in content_text:
        score += 10
    for term in terms:
        if re.search(rf"\b{re.escape(term)}\b", path_text):
            score += 8
        score += min(5, len(re.findall(rf"\b{re.escape(term)}\b", content_text)))
    excerpt = ""
    haystack = content_text
    hit_index = -1
    for term in [query_clean] + terms:
        if not term:
            continue
        hit_index = haystack.find(term)
        if hit_index >= 0:
            break
    if hit_index >= 0:
        start = max(0, hit_index - 140)
        end = min(len(content_text), hit_index + 220)
        excerpt = content_text[start:end].strip()
    return score, excerpt


def search_vault(query: str, max_results: int = 8) -> dict:
    query = str(query or "").strip()
    if not query:
        return {"ok": False, "error": "Search query is empty.", "results": []}
    base = vault_path().expanduser().resolve()
    terms = _query_terms(query)
    scan_chars = int(os.environ.get("HIRA_OBSIDIAN_MAX_SCAN_CHARS", str(DEFAULT_MAX_SCAN_CHARS)) or DEFAULT_MAX_SCAN_CHARS)
    results = []
    for path in _iter_markdown_files(base):
        try:
            note = _note_from_path(path, base)
            content = _read_text(path, scan_chars)
        except (OSError, UnicodeError):
            continue
        score, excerpt = _score_note(query, terms, note, content)
        if score <= 0:
            continue
        results.append({
            "path": note.relative_path,
            "title": note.title,
            "modified": note.modified.strftime("%Y-%m-%d %H:%M SGT"),
            "size": note.size,
            "score": score,
            "excerpt": excerpt,
        })
    results.sort(key=lambda item: (item["score"], item["modified"]), reverse=True)
    limit = max(1, min(int(max_results or 8), 20))
    return {"ok": True, "vault_path": str(base), "query": query, "results": results[:limit]}


def _resolve_note_ref(note_ref: str, vault: Path | None = None) -> Path:
    base = (vault or vault_path()).expanduser().resolve()
    _assert_vault_readable(base)
    raw = str(note_ref or "").strip()
    if not raw:
        raise ValueError("Note reference is empty.")

    candidates = []
    direct = _safe_join(raw, base)
    candidates.append(direct)
    if direct.suffix.lower() != ".md":
        candidates.append(direct.with_suffix(".md"))
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            rel = candidate.relative_to(base)
            reason = exclusion_reason(rel, base)
            if reason:
                raise PermissionError(f"Note is excluded by vault safety policy: {reason}.")
            return candidate

    target = _normalise_text(Path(raw).stem or raw)
    exact = []
    partial = []
    for path in _iter_markdown_files(base):
        note = _note_from_path(path, base)
        title = _normalise_text(note.title)
        rel = _normalise_text(note.relative_path)
        if title == target or rel == target:
            exact.append(path)
        elif target in title or target in rel:
            partial.append(path)
    matches = exact or partial
    if not matches:
        raise FileNotFoundError(f"No readable note matched: {raw}")
    if len(matches) > 1:
        sample = ", ".join(str(path.relative_to(base)) for path in matches[:5])
        raise ValueError(f"Multiple readable notes matched. Be more specific: {sample}")
    return matches[0]


def read_note(note: str, max_chars: int = DEFAULT_MAX_READ_CHARS) -> dict:
    base = vault_path().expanduser().resolve()
    path = _resolve_note_ref(note, base)
    rel = str(path.relative_to(base))
    reason = exclusion_reason(rel, base)
    if reason:
        return {"ok": False, "error": f"Note is excluded by vault safety policy: {reason}.", "path": rel}
    limit = max(500, min(int(max_chars or DEFAULT_MAX_READ_CHARS), 50_000))
    content = _read_text(path, limit + 1)
    truncated = len(content) > limit
    if truncated:
        content = content[:limit]
    note_info = _note_from_path(path, base)
    return {
        "ok": True,
        "vault_path": str(base),
        "path": rel,
        "title": note_info.title,
        "modified": note_info.modified.strftime("%Y-%m-%d %H:%M SGT"),
        "truncated": truncated,
        "content": content,
    }


def list_recent_notes(limit: int = 10) -> dict:
    base = vault_path().expanduser().resolve()
    notes = []
    for path in _iter_markdown_files(base):
        try:
            note = _note_from_path(path, base)
        except OSError:
            continue
        notes.append({
            "path": note.relative_path,
            "title": note.title,
            "modified": note.modified.strftime("%Y-%m-%d %H:%M SGT"),
            "size": note.size,
        })
    notes.sort(key=lambda item: item["modified"], reverse=True)
    capped = max(1, min(int(limit or 10), 25))
    return {"ok": True, "vault_path": str(base), "notes": notes[:capped]}


def append_to_inbox(content: str, heading: str = "", source: str = "H.I.R.A", inbox_path: str = "") -> dict:
    body = str(content or "").strip()
    if not body:
        return {"ok": False, "error": "Inbox entry content is empty."}
    base = vault_path().expanduser().resolve()
    _assert_vault_readable(base)
    target_ref = str(inbox_path or os.environ.get("HIRA_OBSIDIAN_INBOX_NOTE", "") or DEFAULT_INBOX_NOTE).strip()
    target = _safe_join(target_ref, base)
    if target.suffix.lower() != ".md":
        target = target.with_suffix(".md")
    rel = target.relative_to(base)
    reason = exclusion_reason(rel, base)
    if reason:
        return {"ok": False, "error": f"Inbox note is excluded by vault safety policy: {reason}.", "path": str(rel)}
    now = datetime.now(ZoneInfo("Asia/Singapore"))
    title = str(heading or "Inbox capture").strip()
    source_text = str(source or "H.I.R.A").strip()
    entry = (
        f"\n\n## {now.strftime('%Y-%m-%d %H:%M SGT')} - {title}\n"
        f"Source: {source_text}\n\n"
        f"{body}\n"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(entry)
    return {
        "ok": True,
        "vault_path": str(base),
        "path": str(rel),
        "heading": title,
        "chars_appended": len(entry),
        "timestamp": now.strftime("%Y-%m-%d %H:%M SGT"),
    }
