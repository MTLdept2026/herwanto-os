"""
Morning digest: Google News RSS — completely free, no API key needed.
Web search tool (AI chat): Tavily API — free tier 1000/month.
  Sign up: https://tavily.com — set TAVILY_API_KEY in Railway if you want it.
  Without it, the bot works fine; AI chat just won't search the web.
"""

import os
import logging
import re
import hashlib
import requests
import feedparser
from html.parser import HTMLParser
from urllib.parse import quote, urlparse

logger = logging.getLogger(__name__)

TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
GOOGLE_NEWS_TIMEOUT = 8

# Google News RSS — one per digest topic, no API key needed
# Format: news.google.com/rss/search?q=QUERY&hl=en-SG&gl=SG&ceid=SG:en
DIGEST_TOPICS = [
    ("⚽ Liverpool / EPL",  "Liverpool FC Premier League standings fixtures transfers"),
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


class _ReadableHTMLParser(HTMLParser):
    """Small dependency-free extractor for ordinary article/webpage text."""

    SKIP_TAGS = {"script", "style", "noscript", "svg", "canvas", "iframe"}
    BLOCK_TAGS = {
        "article", "section", "main", "div", "p", "br", "li", "tr",
        "h1", "h2", "h3", "h4", "h5", "h6", "blockquote",
    }

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title = ""
        self._in_title = False
        self._skip_depth = 0
        self._parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1
            return
        if tag == "title":
            self._in_title = True
        if tag in self.BLOCK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in self.SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if tag == "title":
            self._in_title = False
        if tag in self.BLOCK_TAGS:
            self._parts.append("\n")

    def handle_data(self, data):
        if self._skip_depth:
            return
        text = " ".join((data or "").split())
        if not text:
            return
        if self._in_title:
            self.title = f"{self.title} {text}".strip()
        else:
            self._parts.append(text)

    def readable_text(self):
        text = " ".join("\n".join(self._parts).split())
        return re.sub(r"\s*\n\s*", "\n", text).strip()


def _looks_like_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def fetch_url(url: str, max_chars: int = 6000) -> dict:
    """Fetch and extract readable text from a URL. Does not require Tavily."""
    url = (url or "").strip()
    if not _looks_like_url(url):
        return {"ok": False, "error": "Invalid URL. Use a full http(s) link.", "url": url}

    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"
                )
            },
            timeout=10,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"URL fetch error for '{url}': {e}")
        return {"ok": False, "error": f"Could not fetch URL: {e}", "url": url}

    content_type = resp.headers.get("content-type", "")
    text = resp.text or ""
    title = ""
    if "html" in content_type.lower() or "<html" in text[:500].lower():
        parser = _ReadableHTMLParser()
        try:
            parser.feed(text)
            title = parser.title
            text = parser.readable_text()
        except Exception as e:
            logger.warning(f"HTML parse error for '{url}': {e}")
            text = re.sub(r"<[^>]+>", " ", text)

    text = " ".join(text.split())
    limit = max(1000, min(int(max_chars or 6000), 12000))
    truncated = len(text) > limit
    if truncated:
        text = text[:limit].rsplit(" ", 1)[0]
    return {
        "ok": True,
        "url": resp.url,
        "title": title,
        "content_type": content_type,
        "text": text,
        "truncated": truncated,
    }


def format_url_fetch(result: dict) -> str:
    if not result.get("ok"):
        return result.get("error") or "Could not fetch URL."
    lines = [f"URL: {result.get('url', '')}"]
    if result.get("title"):
        lines.append(f"Title: {result['title']}")
    if result.get("content_type"):
        lines.append(f"Content-Type: {result['content_type']}")
    if result.get("truncated"):
        lines.append("Note: Content was truncated to the first readable portion.")
    lines.append("")
    lines.append(result.get("text") or "No readable text found.")
    return "\n".join(lines).strip()


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

def _parse_google_news_rss(query: str):
    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-SG&gl=SG&ceid=SG:en"
    resp = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"},
        timeout=GOOGLE_NEWS_TIMEOUT,
    )
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def _google_news_headline(query, max_items=1):
    """Fetch latest headline(s) from Google News RSS for a given query."""
    try:
        feed = _parse_google_news_rss(query)
        return [e.title for e in feed.entries[:max_items] if hasattr(e, "title")]
    except Exception as e:
        logger.warning(f"RSS error for '{query}': {e}")
        return []


def google_news(query, max_items=5):
    """Fetch latest Google News RSS items for a query."""
    try:
        feed = _parse_google_news_rss(query)
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


def news_quality_score(item: dict) -> int:
    return _news_quality_score(item)


def news_item_key(item: dict) -> str:
    title = " ".join(str(item.get("title", "")).lower().split())
    url = str(item.get("url", "")).strip().lower()
    seed = url or title
    if not seed:
        return ""
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def pick_fresh_morning_digest_entries(topics=None, seen_keys=None, max_items_per_topic: int = 1, fetch_limit: int = 6):
    """
    Fetch morning digest candidates and skip items already shown recently.
    Returns a list of {"label", "item", "key"} objects.
    """
    seen = {str(key).strip() for key in (seen_keys or []) if str(key).strip()}
    chosen = []
    used_keys = set()
    per_topic = max(1, int(max_items_per_topic or 1))
    limit = max(per_topic, int(fetch_limit or 6))
    for label, query in (topics or DIGEST_TOPICS):
        items = google_news(query, max_items=limit)
        if not items:
            continue
        picked = 0
        for item in items:
            key = news_item_key(item)
            if not key or key in used_keys or key in seen:
                continue
            chosen.append({"label": label, "item": item, "key": key})
            used_keys.add(key)
            picked += 1
            if picked >= per_topic:
                break
    return chosen


def format_morning_digest_entries(entries) -> str:
    lines = []
    for entry in entries or []:
        label = str(entry.get("label", "")).strip()
        item = entry.get("item") if isinstance(entry, dict) else {}
        title = str((item or {}).get("title", "")).strip()
        if not label or not title:
            continue
        lines.append(f"{label}: {title}")
    return "\n".join(lines)


def get_digest_for_topics(topics, max_items=2):
    """Return latest headlines for a list of (label, query) topics."""
    lines = []
    for label, query in topics:
        items = google_news(query, max_items=max_items)
        if not items:
            continue
        lines.append(f"{label}:")
        for item in items:
            lines.append(f"- {item['title']}")
            if item.get("url"):
                lines.append(f"  {item['url']}")
    return "\n".join(lines)


def get_morning_digest(topics=None):
    """
    Fetch one headline per topic using Google News RSS.
    No API key needed — always works.
    """
    entries = pick_fresh_morning_digest_entries(topics=topics, max_items_per_topic=1, fetch_limit=1)
    return format_morning_digest_entries(entries)
