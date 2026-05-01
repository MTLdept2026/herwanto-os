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
    ("🤖 Android",          "Android OS Google Pixel app ecosystem"),
    ("🍎 iOS",              "iOS iPhone Apple developer"),
    ("🧑‍💻 Developer",       "iOS Android React Vite Capacitor developer updates"),
    ("☪️  Islam",            "Islam Muslim spirituality Singapore"),
    ("🇸🇬 SG Education",    "Singapore education MOE"),
    ("🇸🇬 SG News",         "Singapore news today"),
    ("🎨 Design / UI/UX",  "UI UX design"),
    ("📱 App Dev",          "iOS Android app development"),
    ("🍎 macOS",            "macOS Apple"),
    ("📦 Nothing Products", "Nothing Phone CMF earbuds product launch"),
    ("📦 Nothing OS",       "Nothing OS Nothing Phone Android update"),
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


def google_news(query, max_items=5):
    """Fetch latest Google News RSS items for a query."""
    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-SG&gl=SG&ceid=SG:en"
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[: max_items * 4]:
            items.append({
                "title": getattr(entry, "title", ""),
                "url": getattr(entry, "link", ""),
                "published": getattr(entry, "published", ""),
                "source": getattr(getattr(entry, "source", None), "title", ""),
            })
        return _rank_news_items(items)[:max_items]
    except Exception as e:
        logger.warning(f"RSS error for '{query}': {e}")
        return []


def format_news_items(items):
    if not items:
        return "No news found."
    lines = []
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
    return "\n".join(lines)


LOW_SIGNAL_NEWS_TERMS = (
    "sponsored", "press release", "pr newswire", "globenewswire", "accesswire",
    "rumour roundup", "rumor roundup", "leak suggests", "you won't believe",
    "top 10", "best deals", "buy now", "coupon", "price drop",
)

HIGH_SIGNAL_NEWS_TERMS = (
    "analysis", "explainer", "policy", "research", "report", "interview",
    "launches", "announces", "updates", "reform", "curriculum", "developer",
    "release notes", "security", "education", "ministry", "moe",
)


def _news_quality_score(item: dict) -> int:
    text = f"{item.get('title', '')} {item.get('source', '')}".lower()
    score = 10
    for term in LOW_SIGNAL_NEWS_TERMS:
        if term in text:
            score -= 8
    for term in HIGH_SIGNAL_NEWS_TERMS:
        if term in text:
            score += 4
    title = item.get("title", "")
    if len(title) < 35:
        score -= 2
    if "|" in title or title.count("-") > 2:
        score -= 1
    return score


def _rank_news_items(items: list[dict]) -> list[dict]:
    filtered = [item for item in items if _news_quality_score(item) > 0]
    ranked = filtered or items
    return sorted(ranked, key=_news_quality_score, reverse=True)


def get_digest_for_topics(topics, max_items=2):
    """Return latest headlines for a list of (label, query) topics."""
    lines = []
    for label, query in topics:
        items = google_news(query, max_items=max_items)
        if not items:
            continue
        lines.append(f"{label}:")
        for item in items:
            lines.append(f"- {item['title'][:110]}")
            if item.get("url"):
                lines.append(f"  {item['url']}")
    return "\n".join(lines)


def get_morning_digest(topics=None):
    """
    Fetch one headline per topic using Google News RSS.
    No API key needed — always works.
    """
    lines = []
    for label, query in (topics or DIGEST_TOPICS):
        items = google_news(query, max_items=1)
        if items:
            lines.append(f"{label}: {items[0]['title'][:85]}")
    return "\n".join(lines)
