"""
Structured sports intelligence helpers for H.I.R.A.

These adapters keep football/F1 prompts away from generic one-shot search.
They gather several targeted current-news slices so the model can answer with
source-backed sections instead of relying on stale memory.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import re

import requests

import search_service as ss


ESPN_SCOREBOARD_TIMEOUT = 6
FOTMOB_TIMEOUT = 5
ESPN_LIVERPOOL_LEAGUES = (
    ("eng.1", "Premier League"),
    ("uefa.champions", "UEFA Champions League"),
    ("eng.fa", "FA Cup"),
    ("eng.league_cup", "Carabao Cup"),
)
FOTMOB_LIVERPOOL_URL = "https://www.fotmob.com/teams/8650/overview/liverpool"


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


def _clip_after(label: str, text: str, stop_labels: tuple[str, ...] = (), max_chars: int = 900) -> str:
    start = text.lower().find(label.lower())
    if start < 0:
        return ""
    end = len(text)
    lowered = text.lower()
    for stop in stop_labels:
        stop_at = lowered.find(stop.lower(), start + len(label))
        if stop_at > start:
            end = min(end, stop_at)
    return " ".join(text[start:end].split())[:max_chars].strip()


def _fotmob_result_lines(text: str, limit: int = 5) -> list[str]:
    snippet = _clip_after("Recent results for Liverpool:", text, ("Upcoming fixtures", "Liverpool currently sits"), max_chars=1600)
    if not snippet:
        return []
    matches = re.findall(
        r"([A-Z][a-z]+ \d{1,2}, \d{4}:\s*(?:Premier League|Champions League|FA Cup|Carabao Cup)\s+-\s+[^.]+)",
        snippet,
    )
    if matches:
        return [f"- {item.strip()}" for item in matches[:limit]]
    return [f"- {snippet}"]


def _fetch_fotmob_team_text() -> dict:
    try:
        resp = requests.get(
            FOTMOB_LIVERPOOL_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"},
            timeout=FOTMOB_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except Exception as exc:
        return {"ok": False, "error": f"Could not fetch FotMob: {exc}"}

    parser = ss._ReadableHTMLParser()
    try:
        parser.feed(resp.text or "")
        text = parser.readable_text()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", resp.text or "")
    return {"ok": True, "text": " ".join(text.split())}


def _format_liverpool_fotmob_probe(max_items: int = 5) -> list[str]:
    lines = [
        "FotMob team-page probe",
        f"Source: {FOTMOB_LIVERPOOL_URL}",
        "Answer rule: use this before general news snippets for Liverpool form, latest listed results, upcoming fixtures, and table position. If the page shows a fixture but no final score, do not invent the final score.",
    ]
    result = _fetch_fotmob_team_text()
    if not result.get("ok"):
        lines.append(f"- FotMob fetch failed: {result.get('error') or 'unknown error'}")
        return lines

    text = result.get("text") or ""
    recent = _fotmob_result_lines(text, max_items)
    if recent:
        lines.append("Recent results from FotMob:")
        lines.extend(recent)
    else:
        score_candidates = _scoreline_candidates(text)
        if score_candidates:
            lines.append("Scoreline candidates found on FotMob page:")
            lines.extend(f"- {candidate}" for candidate in score_candidates[:3])
        else:
            lines.append("- No explicit recent-result line found in fetched FotMob text.")

    table = _clip_after("Liverpool currently sits", text, ("#  | Team", "Liverpool's squad", "Fixtures"), max_chars=420)
    if table:
        lines.append(f"Table note: {table}")

    fixtures = _clip_after("Upcoming fixtures for Liverpool:", text, ("Looking ahead", "Liverpool currently sits"), max_chars=700)
    if fixtures:
        lines.append(f"Upcoming fixtures: {fixtures}")
    return lines


def _source_contract_line(status: str, as_of: str, source: str, reason: str) -> str:
    return f"SOURCE CONTRACT: status={status}; as_of={as_of}; source={source}; reason={reason}"


def _published_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).date()
    except Exception:
        return None


def _parse_espn_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None


def _espn_scoreboard_url(league: str, start: date, end: date) -> str:
    dates = f"{start:%Y%m%d}-{end:%Y%m%d}"
    return f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/scoreboard?dates={dates}"


def _is_liverpool_competitor(competitor: dict) -> bool:
    team = competitor.get("team") or {}
    values = [
        team.get("id"),
        team.get("uid"),
        team.get("slug"),
        team.get("abbreviation"),
        team.get("displayName"),
        team.get("shortDisplayName"),
        team.get("name"),
        competitor.get("displayName"),
    ]
    text = " ".join(str(value or "").lower() for value in values)
    return "liverpool" in text or "lfc" in text or "team:364" in text or " 364 " in f" {text} "


def _normalise_espn_event(event: dict, league_label: str, source_url: str) -> dict | None:
    competitions = event.get("competitions") or []
    competition = competitions[0] if competitions else {}
    competitors = competition.get("competitors") or []
    if not any(_is_liverpool_competitor(item) for item in competitors):
        return None

    sides = []
    for item in competitors:
        team = item.get("team") or {}
        home_away = item.get("homeAway") or ""
        score = item.get("score")
        sides.append({
            "home_away": home_away,
            "name": team.get("displayName") or team.get("shortDisplayName") or team.get("name") or "Unknown",
            "score": str(score) if score not in (None, "") else "",
            "is_liverpool": _is_liverpool_competitor(item),
        })
    sides.sort(key=lambda item: 0 if item["home_away"] == "home" else 1)
    if len(sides) < 2:
        return None

    status = event.get("status") or {}
    status_type = status.get("type") or {}
    event_date = _parse_espn_datetime(event.get("date"))
    if not event_date:
        return None

    completed = bool(status_type.get("completed")) or str(status_type.get("state", "")).lower() == "post"
    scoreline = f"{sides[0]['name']} {sides[0]['score']}-{sides[1]['score']} {sides[1]['name']}"
    return {
        "date": event_date,
        "date_text": event_date.astimezone(timezone.utc).strftime("%Y-%m-%d"),
        "league": league_label,
        "status": status_type.get("description") or status_type.get("detail") or status.get("type", {}).get("name") or "",
        "completed": completed,
        "scoreline": scoreline if all(side["score"] for side in sides[:2]) else f"{sides[0]['name']} vs {sides[1]['name']}",
        "source_url": source_url,
    }


def _espn_liverpool_scoreboard_probe(focus: str = "", today: date | None = None) -> dict:
    """Fetch Liverpool fixtures/results from ESPN's public scoreboard API."""
    anchor = today or datetime.now(timezone.utc).date()
    start = anchor - timedelta(days=45)
    end = anchor + timedelta(days=14)
    events: list[dict] = []
    errors: list[str] = []

    def fetch_league(league_info: tuple[str, str]) -> tuple[list[dict], str]:
        league, label = league_info
        url = _espn_scoreboard_url(league, start, end)
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"},
                timeout=ESPN_SCOREBOARD_TIMEOUT,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            return [], f"{label}: {exc}"
        parsed_events = []
        for event in payload.get("events") or []:
            parsed = _normalise_espn_event(event, label, url)
            if parsed:
                parsed_events.append(parsed)
        return parsed_events, ""

    with ThreadPoolExecutor(max_workers=len(ESPN_LIVERPOOL_LEAGUES)) as executor:
        futures = [executor.submit(fetch_league, league_info) for league_info in ESPN_LIVERPOOL_LEAGUES]
        for future in futures:
            league_events, error = future.result()
            events.extend(league_events)
            if error:
                errors.append(error)

    events.sort(key=lambda item: item["date"])
    completed = [item for item in events if item["completed"] and item["date"].date() <= anchor]
    upcoming = [item for item in events if not item["completed"] and item["date"].date() >= anchor]
    return {
        "start": start,
        "end": end,
        "latest_completed": completed[-1] if completed else None,
        "next_fixture": upcoming[0] if upcoming else None,
        "events": events,
        "errors": errors,
    }


def _format_liverpool_scoreboard_probe(focus: str, today: date | None = None) -> list[str]:
    probe = _espn_liverpool_scoreboard_probe(focus, today=today)
    return _format_liverpool_scoreboard_probe_from_data(probe)


def _format_liverpool_scoreboard_probe_from_data(probe: dict) -> list[str]:
    lines = [
        "Authoritative scoreboard probe",
        f"Source: ESPN public scoreboard API, window {probe['start'].isoformat()} to {probe['end'].isoformat()}.",
        "Answer rule: treat the latest completed fixture here as stronger than old news snippets. If this probe is unavailable or empty, say the live scoreboard check failed instead of answering from memory.",
    ]
    latest = probe.get("latest_completed")
    if latest:
        lines.append(
            f"- Latest completed: {latest['scoreline']} | {latest['date_text']} | {latest['league']} | {latest['status']}"
        )
        lines.append(f"  {latest['source_url']}")
    else:
        lines.append("- No completed Liverpool fixture returned by the scoreboard probe.")
    next_fixture = probe.get("next_fixture")
    if next_fixture:
        lines.append(
            f"- Next listed fixture: {next_fixture['scoreline']} | {next_fixture['date_text']} | {next_fixture['league']} | {next_fixture['status']}"
        )
    if probe.get("errors"):
        lines.append("- Scoreboard warnings: " + " | ".join(probe["errors"][:2]))
    return lines


def _format_liverpool_result_probe(focus: str, max_items: int) -> list[str]:
    focus_text = (focus or "").strip()
    query_bits = ["Liverpool FC latest result full-time score match report"]
    if focus_text:
        query_bits.append(focus_text)
    if re.search(r"\bchelsea\b", focus_text, re.I):
        query_bits.insert(0, "Liverpool Chelsea result full-time score match report")
    if re.search(r"\b(yesterday|last night|9 may|09 may|2026-05-09)\b", focus_text, re.I):
        query_bits.insert(0, "Liverpool result 9 May 2026 full-time score")

    scoreboard_probe = _espn_liverpool_scoreboard_probe(focus_text)
    latest_scoreboard = scoreboard_probe.get("latest_completed")
    latest_date = latest_scoreboard["date"].date() if latest_scoreboard else None
    if latest_scoreboard:
        contract = _source_contract_line(
            "confirmed",
            latest_scoreboard["date_text"],
            "ESPN scoreboard + FotMob probe",
            "latest completed Liverpool fixture returned by scoreboard source",
        )
    else:
        contract = _source_contract_line(
            "unconfirmed",
            datetime.now(timezone.utc).date().isoformat(),
            "FotMob/ESPN/news probe",
            "no completed Liverpool fixture was returned by scoreboard source",
        )

    lines = [
        contract,
        "Priority result probe",
        "Answer rule: if Herwanto asks for a result or score, state the confirmed full-time score first. Use the scoreline candidates below before any reaction, line-up, or programme-note detail. If the source contract is not confirmed, say you could not verify the result instead of guessing.",
        "Staleness gate: do not call a news item the latest result if it predates the scoreboard latest completed fixture.",
    ]
    lines.extend(_format_liverpool_fotmob_probe(max_items + 2))
    lines.append("")
    lines.extend(_format_liverpool_scoreboard_probe_from_data(scoreboard_probe))
    lines.append("")
    candidates = []
    sources = []
    stale_sources = []
    for query in query_bits[:3]:
        for item in ss.google_news(query, max_items=max_items):
            haystack = f"{item.get('title', '')} {item.get('description', '')}"
            for score in _scoreline_candidates(haystack):
                candidates.append(score)
            if item.get("title"):
                published = _published_date(item.get("published", ""))
                if latest_date and published and published < latest_date:
                    stale_sources.append(item)
                else:
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
    if stale_sources:
        lines.append("Demoted stale result/news leads:")
        for item in stale_sources[: max_items + 2]:
            meta = []
            if item.get("source"):
                meta.append(item["source"])
            if item.get("published"):
                meta.append(item["published"])
            suffix = f" ({' · '.join(meta)})" if meta else ""
            lines.append(f"- {item.get('title', '')}{suffix}")
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
