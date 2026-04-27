from __future__ import annotations

import os
from typing import Any

import requests


BASE_URL = "https://api.canva.com/rest/v1"


def canva_ok() -> bool:
    return bool(os.environ.get("CANVA_ACCESS_TOKEN", "").strip())


def _headers() -> dict:
    token = os.environ.get("CANVA_ACCESS_TOKEN", "").strip()
    if not token:
        raise EnvironmentError("CANVA_ACCESS_TOKEN not set")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, **kwargs) -> dict:
    response = requests.request(
        method,
        f"{BASE_URL}{path}",
        headers=_headers(),
        timeout=30,
        **kwargs,
    )
    try:
        data = response.json()
    except Exception:
        data = {"message": response.text}
    if response.status_code >= 400:
        message = data.get("message") or data.get("error") or response.text
        raise RuntimeError(f"Canva API {response.status_code}: {message}")
    return data


def create_design(title: str, design_type: str = "presentation") -> dict:
    design_type = (design_type or "presentation").strip().lower()
    allowed = {"doc", "presentation", "whiteboard", "email"}
    if design_type not in allowed:
        raise ValueError(f"Canva design_type must be one of: {', '.join(sorted(allowed))}")
    body = {
        "type": "type_and_asset",
        "design_type": {
            "type": "preset",
            "name": design_type,
        },
        "title": title.strip() or "Hira Canva Design",
    }
    return _request("POST", "/designs", json=body)


def list_designs(query: str = "") -> dict:
    params: dict[str, Any] = {}
    if query.strip():
        params["query"] = query.strip()
    return _request("GET", "/designs", params=params)


def create_export_job(design_id: str, file_type: str = "pdf") -> dict:
    file_type = (file_type or "pdf").strip().lower()
    allowed = {"pdf", "jpg", "png", "gif", "pptx", "mp4", "html_bundle", "html_standalone"}
    if file_type not in allowed:
        raise ValueError(f"Canva export type must be one of: {', '.join(sorted(allowed))}")
    body = {
        "design_id": design_id.strip(),
        "format": {"type": file_type},
    }
    if file_type == "pdf":
        body["format"]["size"] = "a4"
    return _request("POST", "/exports", json=body)


def get_export_job(job_id: str) -> dict:
    return _request("GET", f"/exports/{job_id.strip()}")


def format_design(design: dict) -> str:
    urls = design.get("urls", {})
    parts = [
        f"*{design.get('title', 'Untitled Canva design')}*",
        f"ID: `{design.get('id', '')}`",
    ]
    if urls.get("edit_url"):
        parts.append(f"Edit: {urls['edit_url']}")
    if urls.get("view_url"):
        parts.append(f"View: {urls['view_url']}")
    return "\n".join(parts)


def format_export(job: dict) -> str:
    status = job.get("status", "")
    lines = [f"Export job `{job.get('id', '')}`: {status}"]
    for url in job.get("urls", []) or []:
        lines.append(url)
    if job.get("error"):
        lines.append(str(job["error"]))
    return "\n".join(lines)
