"""
Structured sports intelligence helpers for H.I.R.A.

These adapters keep football/F1 prompts away from generic one-shot search.
They gather several targeted current-news slices so the model can answer with
source-backed sections instead of relying on stale memory.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import re

import search_service as ss


def _clamp_items(value: int | None, default: int = 3) -> int:
    try:
        return max(1, min(5, int(value or default)))
    except (TypeError, ValueError):
        return default


def _format_items(label: str, query: str, max_items: int) -> list[str]:
    lines = [f"{label}"]
    items = ss.google_news(query, max_items=max_items)
    if not items:
        lines.append("- No recent Google News items found.")
    for item in items:
        meta = []
        if item.get("source"):
            meta.append(item["source"])
        if item.get("published"):
            meta.append(item["published"])
        suffix = f" ({' · '.join(meta)})" if meta else ""
        lines.append(f"- {item.get('title', '')}{suffix}")
        if item.get("url"):
            lines.append(f"  {item['url']}")
    return lines


def _format_search(query: str, max_items: int) -> list[str]:
    if not ss.search_enabled():
        return [
            "Targeted web search",
            "- Disabled: set TAVILY_API_KEY for broader live web results beyond Google News RSS.",
        ]
    results = ss.web_search(query, max_results=max_items)
    if not results:
        return ["Targeted web search", "- No Tavily results found."]
    lines = ["Targeted web search"]
    for result in results:
        lines.append(f"- {result.get('title', '')}")
        if result.get("description"):
            lines.append(f"  {result['description']}")
        if result.get("url"):
            lines.append(f"  {result['url']}")
    return lines


def _scoreline_candidates(text: str) -> list[str]:
    clean = " ".join(str(text or "").split())
    if not clean:
        return []
    patterns = [
        r"\b(?:Liverpool|LFC)\s+(?:FC\s+)?(\d{1,2})\s*[-–]\s*(\d{1,2})\s+(?:Chelsea|[A-Z][A-Za-z .'-]{2,30})\b",
        r"\b(?:Chelsea|[A-Z][A-Za-z .'-]{2,30})\s+(\d{1,2})\s*[-–]\s*(\d{1,2})\s+(?:Liverpool|LFC)\b",
        r"\b(\d{1,2})\s*[-–]\s*(\d{1,2})\s+(?:draw|win|defeat|loss)\b",
        r"\b(?:drawn|drew|draw)\s+(\d{1,2})\s*[-–]\s*(\d{1,2})\b",
    ]
    found = []
    seen = set()
    for pattern in patterns:
        for match in re.finditer(pattern, clean, re.I):
            candidate = match.group(0).strip(" .,:;")
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            found.append(candidate)
    return found[:4]


def _format_liverpool_result_probe(focus: str, max_items: int) -> list[str]:
    focus_text = (focus or "").strip()
    query_bits = ["Liverpool FC latest result full-time score match report"]
    if focus_text:
        query_bits.append(focus_text)
    if re.search(r"\bchelsea\b", focus_text, re.I):
        query_bits.insert(0, "Liverpool Chelsea result full-time score match report")
    if re.search(r"\b(yesterday|last night|9 may|09 may|2026-05-09)\b", focus_text, re.I):
        query_bits.insert(0, "Liverpool result 9 May 2026 full-time score")

    lines = [
        "Priority result probe",
        "Answer rule: if Herwanto asks for a result or score, state the confirmed full-time score first. Use the scoreline candidates below before any reaction, line-up, or programme-note detail. If no score appears here, run web_search with an exact score query before answering.",
    ]
    candidates = []
    sources = []
    for query in query_bits[:3]:
        for item in ss.google_news(query, max_items=max_items):
            haystack = f"{item.get('title', '')} {item.get('description', '')}"
            for score in _scoreline_candidates(haystack):
                candidates.append(score)
            if item.get("title"):
                sources.append(item)
        if ss.search_enabled():
            for result in ss.web_search(query, max_results=max_items):
                haystack = f"{result.get('title', '')} {result.get('description', '')}"
                for score in _scoreline_candidates(haystack):
                    candidates.append(score)
                if result.get("title"):
                    sources.append({
                        "title": result.get("title", ""),
                        "description": result.get("description", ""),
                        "url": result.get("url", ""),
                        "source": "web_search",
                    })

    unique_candidates = []
    seen_scores = set()
    for candidate in candidates:
        key = candidate.lower()
        if key in seen_scores:
            continue
        seen_scores.add(key)
        unique_candidates.append(candidate)

    if unique_candidates:
        lines.append("Detected scoreline candidates:")
        for candidate in unique_candidates[:3]:
            lines.append(f"- {candidate}")
    else:
        lines.append("- No scoreline candidate was detected in the first pass.")

    if sources:
        lines.append("Result-source leads:")
        for item in sources[: max_items + 2]:
            meta = []
            if item.get("source"):
                meta.append(item["source"])
            if item.get("published"):
                meta.append(item["published"])
            suffix = f" ({' · '.join(meta)})" if meta else ""
            lines.append(f"- {item.get('title', '')}{suffix}")
            if item.get("description"):
                lines.append(f"  {item['description']}")
            if item.get("url"):
                lines.append(f"  {item['url']}")
    else:
        lines.append("- No result-source leads returned.")
    return lines


def _format_sections(sections: list[tuple[str, str]], count: int) -> list[str]:
    lines: list[str] = []
    with ThreadPoolExecutor(max_workers=min(5, len(sections))) as executor:
        futures = [executor.submit(_format_items, label, query, count) for label, query in sections]
        for future in futures:
            lines.extend(future.result())
            lines.append("")
    return lines


def build_liverpool_brief(focus: str = "", max_items: int = 3) -> str:
    count = _clamp_items(max_items)
    focus_text = (focus or "current Liverpool FC status").strip()
    sections = [
        (
            "Premier League table / form",
            f"Liverpool FC Premier League table standings points goal difference form {focus_text}",
        ),
        (
            "Fixtures, results, and line-ups",
            f"Liverpool FC latest fixture result starting XI team news line-up {focus_text}",
        ),
        (
            "Competition progress",
            f"Liverpool FC Champions League FA Cup Carabao Cup progress fixtures results {focus_text}",
        ),
        (
            "Injuries and availability",
            f"Liverpool FC injury news suspensions availability team news {focus_text}",
        ),
        (
            "Transfers and rumours",
            f"Liverpool FC transfer news rumours confirmed signings departures {focus_text}",
        ),
    ]
    lines = [
        "Liverpool FC structured live brief",
        "Answer guidance: cite sources, separate confirmed news from reports/rumours, and do not rely on old-club memory for current Liverpool players. For result/score questions, answer with the full-time score first; never bury it under preview, line-up, or fan-reaction items.",
        "",
    ]
    lines.extend(_format_liverpool_result_probe(focus_text, count))
    lines.append("")
    lines.extend(_format_sections(sections, count))
    lines.extend(_format_search(f"Liverpool FC {focus_text}", count))
    return "\n".join(lines).strip()


def build_f1_brief(focus: str = "", max_items: int = 3) -> str:
    count = _clamp_items(max_items)
    focus_text = (focus or "current Formula 1 status").strip()
    sections = [
        (
            "Championship standings",
            f"Formula 1 current driver standings constructor standings points {focus_text}",
        ),
        (
            "Race weekend / latest results",
            f"Formula 1 latest race result qualifying sprint grand prix {focus_text}",
        ),
        (
            "Mercedes focus",
            f"Mercedes F1 George Russell Kimi Antonelli latest result qualifying race pace {focus_text}",
        ),
        (
            "Hamilton watch",
            f"Lewis Hamilton Ferrari latest result qualifying race news {focus_text}",
        ),
        (
            "Team news and upgrades",
            f"Formula 1 team news upgrades penalties driver lineup rumours {focus_text}",
        ),
    ]
    lines = [
        "Formula 1 structured live brief",
        "Answer guidance: cite sources, prioritise Mercedes/Russell/Antonelli while keeping Hamilton context, and distinguish confirmed reports from rumours.",
        "",
    ]
    lines.extend(_format_sections(sections, count))
    lines.extend(_format_search(f"Formula 1 {focus_text}", count))
    return "\n".join(lines).strip()
