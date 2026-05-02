"""
Structured sports intelligence helpers for H.I.R.A.

These adapters keep football/F1 prompts away from generic one-shot search.
They gather several targeted current-news slices so the model can answer with
source-backed sections instead of relying on stale memory.
"""

from __future__ import annotations

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
        "Answer guidance: cite sources, separate confirmed news from reports/rumours, and do not rely on old-club memory for current Liverpool players.",
        "",
    ]
    for label, query in sections:
        lines.extend(_format_items(label, query, count))
        lines.append("")
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
    for label, query in sections:
        lines.extend(_format_items(label, query, count))
        lines.append("")
    lines.extend(_format_search(f"Formula 1 {focus_text}", count))
    return "\n".join(lines).strip()
