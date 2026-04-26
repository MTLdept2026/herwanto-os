"""
Web search via Brave Search API (free tier: 2000 queries/month).
Sign up at: https://brave.com/search/api/
Set BRAVE_API_KEY in Railway environment variables.
"""

import os
import logging
import requests

logger = logging.getLogger(__name__)

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
BRAVE_URL = "https://api.search.brave.com/res/v1/web/search"

# Morning digest topics — 1 headline each
DIGEST_TOPICS = [
    ("⚽ Liverpool / EPL",  "Liverpool FC EPL latest news"),
    ("🏎️ F1",               "Formula 1 latest news"),
    ("🤖 AI",               "Claude Gemini Codex AI news today"),
    ("☪️  Islam",            "Islam Muslim world news today"),
    ("🇸🇬 SG Education",    "Singapore MOE education news"),
    ("🌍 Current Affairs",  "Singapore world current affairs today"),
    ("🎨 Design / UI/UX",  "UI UX design trends news"),
    ("📱 App Dev",          "iOS Android app development news"),
    ("🍎 macOS",            "macOS Apple software news"),
    ("📦 Nothing OS",       "Nothing Phone OS Android news"),
]


def search_enabled():
    return bool(BRAVE_API_KEY)


def web_search(query, max_results=5):
    """Search via Brave API. Returns list of {title, description, url}."""
    if not BRAVE_API_KEY:
        return []
    try:
        resp = requests.get(
            BRAVE_URL,
            headers={
                "Accept": "application/json",
                "X-Subscription-Token": BRAVE_API_KEY,
            },
            params={"q": query, "count": max_results},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        return [
            {
                "title":       r.get("title", ""),
                "description": r.get("description", ""),
                "url":         r.get("url", ""),
            }
            for r in data.get("web", {}).get("results", [])
        ]
    except Exception as e:
        logger.warning(f"Search error for '{query}': {e}")
        return []


def format_results(results):
    """Format search results as context string for Claude."""
    if not results:
        return "No results found."
    lines = []
    for r in results:
        lines.append(f"Title: {r['title']}")
        lines.append(f"Summary: {r['description'][:400]}")
        lines.append(f"URL: {r['url']}")
        lines.append("")
    return "\n".join(lines).strip()


def get_morning_digest():
    """
    Fetch one headline per topic for the morning briefing.
    Requires BRAVE_API_KEY. Returns empty string if not configured.
    """
    if not search_enabled():
        return ""
    lines = []
    for label, query in DIGEST_TOPICS:
        results = web_search(query, max_results=1)
        if results:
            headline = results[0]["title"][:85]
            lines.append(f"{label}: {headline}")
    return "\n".join(lines)
