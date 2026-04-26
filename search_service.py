"""
Morning digest: Google News RSS — completely free, no API key needed.
Web search tool (AI chat): Tavily API — free tier 1000/month.
  Sign up: https://tavily.com — set TAVILY_API_KEY in Railway if you want it.
  Without it, the bot works fine; AI chat just won't search the web.
"""

import os
import logging
import requests
import feedparser
from urllib.parse import quote

logger = logging.getLogger(__name__)

TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")

# Google News RSS — one per digest topic, no API key needed
# Format: news.google.com/rss/search?q=QUERY&hl=en-SG&gl=SG&ceid=SG:en
DIGEST_TOPICS = [
    ("⚽ Liverpool / EPL",  "Liverpool FC Premier League"),
    ("🏎️ F1",               "Formula 1"),
    ("🤖 AI",               "Claude Gemini Codex AI"),
    ("☪️  Islam",            "Islam Muslim"),
    ("🇸🇬 SG Education",    "Singapore education MOE"),
    ("🌍 Current Affairs",  "Singapore news today"),
    ("🎨 Design / UI/UX",  "UI UX design"),
    ("📱 App Dev",          "iOS Android app development"),
    ("🍎 macOS",            "macOS Apple"),
    ("📦 Nothing OS",       "Nothing Phone Android"),
]


def search_enabled():
    return bool(TAVILY_API_KEY)


# ─── WEB SEARCH (Tavily — optional) ─────────────────────────────────────────

def web_search(query, max_results=5):
    """Search via Tavily API. Returns [] if key not set."""
    if not TAVILY_API_KEY:
        return []
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={"api_key": TAVILY_API_KEY, "query": query, "max_results": max_results},
            timeout=8,
        )
        resp.raise_for_status()
        return [
            {"title": r.get("title",""), "description": r.get("content","")[:400], "url": r.get("url","")}
            for r in resp.json().get("results", [])
        ]
    except Exception as e:
        logger.warning(f"Tavily search error: {e}")
        return []


def format_results(results):
    if not results:
        return "No results found."
    lines = []
    for r in results:
        lines.append(f"Title: {r['title']}")
        lines.append(f"Summary: {r['description']}")
        lines.append(f"URL: {r['url']}")
        lines.append("")
    return "\n".join(lines).strip()


# ─── MORNING DIGEST (Google News RSS — always free) ──────────────────────────

def _google_news_headline(query, max_items=1):
    """Fetch latest headline(s) from Google News RSS for a given query."""
    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-SG&gl=SG&ceid=SG:en"
    try:
        feed = feedparser.parse(url)
        return [e.title for e in feed.entries[:max_items] if hasattr(e, "title")]
    except Exception as e:
        logger.warning(f"RSS error for '{query}': {e}")
        return []


def get_morning_digest():
    """
    Fetch one headline per topic using Google News RSS.
    No API key needed — always works.
    """
    lines = []
    for label, query in DIGEST_TOPICS:
        headlines = _google_news_headline(query, max_items=1)
        if headlines:
            lines.append(f"{label}: {headlines[0][:85]}")
    return "\n".join(lines)
