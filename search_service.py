"""
Morning digest: Google News RSS — completely free, no API key needed.
Web search tool (AI chat): Tavily API — free tier 1000/month.
  Sign up: https://tavily.com — set TAVILY_API_KEY in Railway if you want it.
  Without it, the bot works fine; AI chat just won't search the web.
"""

from __future__ import annotations

import os
import logging
import re
import hashlib
import ipaddress
import json
import requests
import feedparser
import socket
import html
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse
from requests.adapters import HTTPAdapter
from urllib3 import PoolManager
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.exceptions import NewConnectionError

logger = logging.getLogger(__name__)

GOOGLE_NEWS_TIMEOUT = 8
RSS_FEED_TIMEOUT = 4
WEB_SEARCH_TIMEOUT = 8
RESEARCH_WORKERS = 4
_GOOGLE_NEWS_LAST_ERROR = ""
_GOOGLE_NEWS_ERRORS_BY_QUERY: dict[str, str] = {}


def _env_int(name: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        value = max(minimum, int(os.environ.get(name, str(default)) or default))
    except ValueError:
        value = default
    if maximum is not None:
        value = min(value, maximum)
    return value


URL_FETCH_MAX_BYTES = _env_int("HIRA_FETCH_URL_MAX_BYTES", 1_500_000, minimum=64_000, maximum=8_000_000)
JINA_READER_BASE_URL = "https://r.jina.ai/"
JINA_READER_TIMEOUT = _env_int("HIRA_JINA_READER_TIMEOUT", 12, minimum=3, maximum=30)
JINA_SEARCH_BASE_URL = "https://s.jina.ai/"
JINA_SEARCH_TIMEOUT = _env_int("HIRA_JINA_SEARCH_TIMEOUT", 12, minimum=3, maximum=30)


@dataclass
class _PublicAddress:
    family: int
    socktype: int
    proto: int
    sockaddr: tuple

# Google News RSS — one per digest topic, no API key needed
# Format: news.google.com/rss/search?q=QUERY&hl=en-SG&gl=SG&ceid=SG:en
DIGEST_TOPICS = [
    ("⚽ Liverpool / EPL",  "Liverpool FC latest result match report fixture line-up injury transfer"),
    ("🏎️ F1 / Mercedes",    "Mercedes F1 George Russell Kimi Antonelli Lewis Hamilton latest result qualifying upgrade"),
    ("🤖 AI Tools",         "OpenAI Codex Kimi Moonshot Gemini AI tools latest model release agent"),
    ("🧠 Codex / Gemini / Kimi", "OpenAI Codex Gemini Kimi Moonshot coding agent model update"),
    ("🎛️ Teenage Engineering", "Teenage Engineering OP-XY OP-1 Field Pocket Operator product review firmware update"),
    ("🤖 Android",          "Android 17 Android OS Google Pixel Google Play app ecosystem security update beta features"),
    ("🍎 iOS",              "iOS 20 iPhone Apple developer App Store TestFlight policy update beta features"),
    ("🧑‍💻 Solo Dev",        "solo developer iOS Android React Vite Capacitor Railway Netlify GitHub update"),
    ("☪️  Islam",            "Islam Muslim spirituality Singapore MUIS khutbah Ramadan prayer"),
    ("🇸🇬 SG Education",    "Singapore education MOE"),
    ("🇸🇬 SG News",         "Singapore news today"),
    ("🎨 Design / UI/UX",  "UI UX design"),
    ("🍎 macOS",            "macOS Apple MacBook Xcode developer update"),
    ("📦 Nothing Products", "Nothing Phone Nothing Ear CMF product review launch upcoming plans"),
    ("📦 Nothing OS",       "Nothing OS Nothing Phone Android update beta features"),
]


def search_enabled():
    return os.environ.get("HIRA_DISABLE_WEB_SEARCH", "").strip().lower() not in {"1", "true", "yes"}


def jina_reader_fallback_enabled() -> bool:
    return os.environ.get("HIRA_JINA_READER_FALLBACK", "1").strip().lower() not in {"0", "false", "no", "off"}


def _tavily_api_key() -> str:
    return os.environ.get("TAVILY_API_KEY", "").strip()


def _jina_api_key() -> str:
    return os.environ.get("JINA_API_KEY", "").strip()


def tavily_configured() -> bool:
    return bool(_tavily_api_key())


def _news_error_key(query: str) -> str:
    return " ".join(str(query or "").lower().split())[:240]


def _record_google_news_error(query: str, exc: Exception) -> None:
    global _GOOGLE_NEWS_LAST_ERROR
    message = " ".join(str(exc or "unknown error").split())[:260]
    _GOOGLE_NEWS_LAST_ERROR = message
    key = _news_error_key(query)
    if key:
        _GOOGLE_NEWS_ERRORS_BY_QUERY[key] = message
        if len(_GOOGLE_NEWS_ERRORS_BY_QUERY) > 80:
            oldest = next(iter(_GOOGLE_NEWS_ERRORS_BY_QUERY))
            _GOOGLE_NEWS_ERRORS_BY_QUERY.pop(oldest, None)


def _clear_google_news_error(query: str) -> None:
    key = _news_error_key(query)
    if key:
        _GOOGLE_NEWS_ERRORS_BY_QUERY.pop(key, None)


def google_news_last_error(query: str = "") -> str:
    key = _news_error_key(query)
    if key and key in _GOOGLE_NEWS_ERRORS_BY_QUERY:
        return _GOOGLE_NEWS_ERRORS_BY_QUERY[key]
    return _GOOGLE_NEWS_LAST_ERROR


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
        return " ".join("\n".join(self._parts).split()).strip()


def _looks_like_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _host_is_blocked_name(hostname: str) -> bool:
    host = (hostname or "").strip().strip("[]").lower().rstrip(".")
    return (
        not host
        or host in {"localhost", "localhost.localdomain"}
        or host.endswith(".localhost")
        or host.endswith(".local")
        or host.endswith(".internal")
    )


def _ip_is_public(ip_text: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_text)
    except ValueError:
        return False
    return bool(ip.is_global)


def _resolve_public_host(hostname: str) -> tuple[bool, str]:
    host = (hostname or "").strip().strip("[]")
    if _host_is_blocked_name(host):
        return False, "Blocked local hostname"
    try:
        ip = ipaddress.ip_address(host)
        return (bool(ip.is_global), "Blocked non-public IP address")
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except Exception as exc:
        return False, f"Could not resolve hostname: {exc}"
    addresses = {info[4][0] for info in infos if info and info[4]}
    if not addresses:
        return False, "Hostname resolved to no addresses"
    blocked = [address for address in addresses if not _ip_is_public(address)]
    if blocked:
        return False, "Hostname resolves to a non-public address"
    return True, ""


def _resolve_public_addresses(host: str, port: int) -> list[_PublicAddress]:
    clean_host = (host or "").strip().strip("[]")
    if _host_is_blocked_name(clean_host):
        raise OSError("Blocked local hostname")
    try:
        infos = socket.getaddrinfo(clean_host, port, type=socket.SOCK_STREAM)
    except Exception as exc:
        raise OSError(f"Could not resolve hostname: {exc}") from exc

    addresses: list[_PublicAddress] = []
    blocked: list[str] = []
    for family, socktype, proto, _canonname, sockaddr in infos:
        ip_text = sockaddr[0] if sockaddr else ""
        if not _ip_is_public(ip_text):
            blocked.append(ip_text)
            continue
        addresses.append(_PublicAddress(family, socktype, proto, sockaddr))
    if blocked:
        raise OSError("Hostname resolves to a non-public address")
    if not addresses:
        raise OSError("Hostname resolved to no public addresses")
    return addresses


def _create_public_connection(
    address,
    timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
    source_address=None,
    socket_options=None,
):
    host, port = address
    last_error: Exception | None = None
    for candidate in _resolve_public_addresses(host, int(port)):
        sock = None
        try:
            sock = socket.socket(candidate.family, candidate.socktype, candidate.proto)
            if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                sock.settimeout(timeout)
            for opt in socket_options or ():
                sock.setsockopt(*opt)
            if source_address:
                sock.bind(source_address)
            sock.connect(candidate.sockaddr)
            return sock
        except Exception as exc:
            last_error = exc
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
    if last_error:
        raise last_error
    raise OSError("Could not connect to any public address")


class _PublicHTTPConnection(HTTPConnection):
    def _new_conn(self):
        try:
            return _create_public_connection(
                (self.host, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )
        except OSError as exc:
            raise NewConnectionError(self, f"Failed to establish a public connection: {exc}") from exc


class _PublicHTTPSConnection(HTTPSConnection):
    def _new_conn(self):
        try:
            return _create_public_connection(
                (self.host, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )
        except OSError as exc:
            raise NewConnectionError(self, f"Failed to establish a public connection: {exc}") from exc


class _PublicHTTPConnectionPool(HTTPConnectionPool):
    ConnectionCls = _PublicHTTPConnection


class _PublicHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = _PublicHTTPSConnection


class _PublicHTTPAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, **pool_kwargs)
        self.poolmanager.pool_classes_by_scheme = {
            "http": _PublicHTTPConnectionPool,
            "https": _PublicHTTPSConnectionPool,
        }


def _public_requests_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    adapter = _PublicHTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _validate_public_http_url(url: str) -> tuple[bool, str]:
    parsed = urlparse((url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False, "Invalid URL. Use a full public http(s) link."
    if parsed.username or parsed.password:
        return False, "URL credentials are not allowed."
    return _resolve_public_host(parsed.hostname or "")


def _get_public_url(url: str, timeout: int = 10, max_redirects: int = 5) -> requests.Response:
    current = (url or "").strip()
    session = _public_requests_session()
    try:
        for _ in range(max_redirects + 1):
            ok, reason = _validate_public_http_url(current)
            if not ok:
                raise ValueError(reason)
            resp = session.get(
                current,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"
                    )
                },
                timeout=timeout,
                allow_redirects=False,
                stream=True,
            )
            if not resp.is_redirect:
                if resp.status_code < 400:
                    resp._content = _read_limited_response(resp, URL_FETCH_MAX_BYTES)
                    resp._content_consumed = True
                else:
                    resp._content = b""
                    resp._content_consumed = True
                return resp
            location = resp.headers.get("location", "")
            if not location:
                resp._content = b""
                resp._content_consumed = True
                return resp
            resp.close()
            current = urljoin(current, location)
    finally:
        session.close()
    raise ValueError("Too many redirects")


def _read_limited_response(resp: requests.Response, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=32 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(f"URL response is too large. Limit is {max_bytes // 1024} KB.")
        chunks.append(chunk)
    return b"".join(chunks)


def _url_fetch_result(
    url: str,
    content_type: str,
    title: str,
    text: str,
    max_chars: int,
    reader_fallback: bool = False,
) -> dict:
    text = " ".join(str(text or "").split())
    limit = max(1000, min(int(max_chars or 6000), 12000))
    truncated = len(text) > limit
    if truncated:
        text = text[:limit].rsplit(" ", 1)[0]
    return {
        "ok": True,
        "url": url,
        "title": title,
        "content_type": content_type,
        "text": text,
        "truncated": truncated,
        "reader_fallback": reader_fallback,
    }


def _parse_jina_reader_text(raw: str) -> tuple[str, str]:
    lines = str(raw or "").splitlines()
    title = ""
    content_start = 0
    for index, line in enumerate(lines[:30]):
        stripped = line.strip()
        lower = stripped.lower()
        if lower.startswith("title:"):
            title = stripped.split(":", 1)[1].strip()
            content_start = max(content_start, index + 1)
        elif lower.startswith(("url source:", "published time:", "warning:")):
            content_start = max(content_start, index + 1)
        elif lower in {"markdown content:", "content:"}:
            content_start = index + 1
            break
    content = "\n".join(lines[content_start:]).strip() or str(raw or "").strip()
    return title, content


def _jina_reader_url(url: str) -> str:
    return f"{JINA_READER_BASE_URL}{url}"


def _fetch_url_via_jina_reader(url: str, max_chars: int = 6000) -> dict:
    resp = _get_public_url(_jina_reader_url(url), timeout=JINA_READER_TIMEOUT)
    resp.raise_for_status()
    title, text = _parse_jina_reader_text(resp.text or "")
    return _url_fetch_result(
        url,
        resp.headers.get("content-type", ""),
        title,
        text,
        max_chars,
        reader_fallback=True,
    )


def _fetch_url_with_jina_fallback(url: str, max_chars: int = 6000) -> dict:
    try:
        result = _fetch_url_via_jina_reader(url, max_chars=max_chars)
        if result.get("text") and not _jina_reader_text_unusable(url, result.get("text", "")):
            return result
        return {"ok": False, "error": "Jina Reader returned no readable text.", "url": url}
    except Exception as exc:
        logger.warning(f"Jina Reader fallback error for '{url}': {exc}")
        return {"ok": False, "error": f"Jina Reader fallback failed: {exc}", "url": url}


def _jina_reader_text_unusable(url: str, text: str) -> bool:
    clean = " ".join(str(text or "").split())
    domain = _domain_from_url(url)
    if domain in {"x.com", "twitter.com"} and "Continue with phone" in clean and "Email or username" in clean:
        return True
    return False


def _should_retry_with_jina_reader(url: str, text: str) -> bool:
    clean = " ".join(str(text or "").split())
    if not clean:
        return True
    domain = _domain_from_url(url)
    if domain in {"x.com", "twitter.com"} and (
        "Something went wrong, but don" in clean
        or "Try again Some privacy related extensions may cause issues" in clean
    ):
        return True
    return False


def fetch_url(url: str, max_chars: int = 6000) -> dict:
    """Fetch and extract readable text from a URL. Does not require Tavily."""
    url = (url or "").strip()
    ok, reason = _validate_public_http_url(url)
    if not ok:
        return {"ok": False, "error": reason, "url": url}

    try:
        resp = _get_public_url(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"URL fetch error for '{url}': {e}")
        if jina_reader_fallback_enabled():
            fallback = _fetch_url_with_jina_fallback(url, max_chars=max_chars)
            if fallback.get("ok"):
                return fallback
            error = f"Could not fetch URL: {e}. {fallback.get('error', '')}".strip()
            return {"ok": False, "error": error, "url": url}
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

    result = _url_fetch_result(resp.url, content_type, title, text, max_chars)
    if _should_retry_with_jina_reader(result.get("url") or url, result.get("text", "")) and jina_reader_fallback_enabled():
        fallback = _fetch_url_with_jina_fallback(result.get("url") or url, max_chars=max_chars)
        if fallback.get("ok"):
            return fallback
    return result


def format_url_fetch(result: dict) -> str:
    if not result.get("ok"):
        return result.get("error") or "Could not fetch URL."
    lines = [f"URL: {result.get('url', '')}"]
    if result.get("title"):
        lines.append(f"Title: {result['title']}")
    if result.get("content_type"):
        lines.append(f"Content-Type: {result['content_type']}")
    if result.get("reader_fallback"):
        lines.append(
            "Note: Direct fetch failed or had no readable text, so H.I.R.A used Jina Reader for this public page."
        )
    if result.get("truncated"):
        lines.append("Note: Content was truncated to the first readable portion.")
    lines.append("")
    lines.append(result.get("text") or "No readable text found.")
    return "\n".join(lines).strip()


# ─── WEB SEARCH (Tavily optional, no-key fallback) ──────────────────────────

class _SearchResultParser(HTMLParser):
    """Small parser for search result pages where links carry useful titles."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.results = []
        self._current_href = ""
        self._current_text = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        attr = dict(attrs)
        href = attr.get("href", "")
        if not href:
            return
        cls = attr.get("class", "")
        rel = attr.get("rel", "")
        if "result" in cls or "nofollow" in rel or href.startswith("/l/?"):
            self._current_href = href
            self._current_text = []

    def handle_data(self, data):
        if self._current_href:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a" or not self._current_href:
            return
        title = " ".join(" ".join(self._current_text).split())
        href = _clean_search_url(self._current_href)
        if title and href:
            self.results.append({"title": title, "description": "", "url": href})
        self._current_href = ""
        self._current_text = []


def _clean_search_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        target = parse_qs(parsed.query).get("uddg", [""])[0]
        return unquote(target)
    if value.startswith("//"):
        value = f"https:{value}"
    if value.startswith("/"):
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    if "duckduckgo.com" in parsed.netloc and parsed.path in {"", "/"}:
        return ""
    return value


def _dedupe_results(results: list[dict], max_results: int) -> list[dict]:
    seen = set()
    clean = []
    for item in results:
        url = str(item.get("url", "") or "").strip()
        title = str(item.get("title", "") or "").strip()
        if not url or not title:
            continue
        key = url.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        clean.append({
            "title": title,
            "description": str(item.get("description", "") or "").strip()[:500],
            "url": url,
            "source": str(item.get("source", "") or "").strip(),
        })
        if len(clean) >= max(1, int(max_results or 5)):
            break
    return clean


def _tavily_search(query: str, max_results: int = 5) -> list[dict]:
    api_key = _tavily_api_key()
    if not api_key:
        return []
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={"api_key": api_key, "query": query, "max_results": max_results},
            timeout=WEB_SEARCH_TIMEOUT,
        )
        resp.raise_for_status()
        return [
            {"title": r.get("title",""), "description": r.get("content","")[:400], "url": r.get("url","")}
            for r in resp.json().get("results", [])
        ]
    except Exception as e:
        logger.warning(f"Tavily search error: {e}")
        return []


def tavily_search(query: str, max_results: int = 5) -> list[dict]:
    """Search with Tavily only; use when fallbacks are too slow or too noisy."""
    clean = " ".join(str(query or "").split())
    if not clean or not search_enabled():
        return []
    return _dedupe_results(_tavily_search(clean, max_results=max_results), max_results)


def _duckduckgo_search(query: str, max_results: int = 5) -> list[dict]:
    try:
        resp = requests.get(
            "https://lite.duckduckgo.com/lite/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"},
            timeout=WEB_SEARCH_TIMEOUT,
        )
        resp.raise_for_status()
        parser = _SearchResultParser()
        parser.feed(resp.text or "")
        return _dedupe_results(parser.results, max_results)
    except Exception as e:
        logger.warning(f"DuckDuckGo search error for '{query}': {e}")
        return []


def _jina_search_items_from_json(payload) -> list[dict]:
    raw_items = payload.get("data") if isinstance(payload, dict) else payload
    if isinstance(raw_items, dict):
        raw_items = raw_items.get("results") or raw_items.get("items") or []
    if not isinstance(raw_items, list):
        return []
    items = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title", "") or raw.get("name", "") or "").strip()
        url = str(raw.get("url", "") or raw.get("link", "") or raw.get("source", "") or "").strip()
        description = str(
            raw.get("description", "")
            or raw.get("content", "")
            or raw.get("text", "")
            or raw.get("snippet", "")
            or ""
        ).strip()
        if title and url:
            items.append({
                "title": title,
                "description": description[:500],
                "url": url,
                "source": "Jina Search",
            })
    return items


def _jina_search_items_from_text(raw: str) -> list[dict]:
    items = []
    current: dict[str, str] = {}
    for line in str(raw or "").splitlines():
        clean = line.strip()
        if not clean:
            continue
        if clean.startswith("Title:"):
            if current.get("title") and current.get("url"):
                items.append(current)
            current = {"title": clean.removeprefix("Title:").strip()}
            continue
        if clean.startswith("URL Source:") or clean.startswith("URL:"):
            current["url"] = clean.split(":", 1)[1].strip()
            continue
        if clean.startswith("Description:") or clean.startswith("Snippet:") or clean.startswith("Content:"):
            current["description"] = clean.split(":", 1)[1].strip()[:500]
    if current.get("title") and current.get("url"):
        items.append(current)
    return [
        {
            "title": item.get("title", ""),
            "description": item.get("description", ""),
            "url": item.get("url", ""),
            "source": "Jina Search",
        }
        for item in items
    ]


def _jina_search(query: str, max_results: int = 5) -> list[dict]:
    api_key = _jina_api_key()
    clean = " ".join(str(query or "").split())
    if not api_key or not clean:
        return []
    try:
        resp = requests.get(
            JINA_SEARCH_BASE_URL + quote(clean, safe=""),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
                "User-Agent": "HIRA/1.0",
            },
            timeout=JINA_SEARCH_TIMEOUT,
        )
        resp.raise_for_status()
        try:
            items = _jina_search_items_from_json(resp.json())
        except ValueError:
            items = _jina_search_items_from_text(resp.text or "")
        return _dedupe_results(items, max_results)
    except Exception as e:
        logger.warning(f"Jina search error for '{query}': {e}")
        return []


def _decode_js_string(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except Exception:
        return html.unescape(str(value or "").replace('\\"', '"').replace("\\/", "/"))


def _brave_search_items_from_html(raw: str) -> list[dict]:
    items = []
    pattern = re.compile(
        r'title:"(?P<title>(?:\\.|[^"\\])*)",url:"(?P<url>https?://(?P<host>[^/"\\]+)[^"\\]*)"'
        r'.{0,2500}?description:(?:"(?P<description>(?:\\.|[^"\\])*)"|void 0)',
        re.S,
    )
    for match in pattern.finditer(str(raw or "")):
        title = _decode_js_string(match.group("title"))
        url = _decode_js_string(match.group("url"))
        description = _decode_js_string(match.group("description") or "")
        if title and url:
            items.append({
                "title": title,
                "description": description[:500],
                "url": url,
                "source": "Brave Search",
            })
    return items


def _brave_search(query: str, max_results: int = 5) -> list[dict]:
    clean = " ".join(str(query or "").split())
    if not clean:
        return []
    try:
        resp = requests.get(
            "https://search.brave.com/search",
            params={"q": clean},
            headers={"User-Agent": "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"},
            timeout=WEB_SEARCH_TIMEOUT,
        )
        resp.raise_for_status()
        return _dedupe_results(_brave_search_items_from_html(resp.text or ""), max_results)
    except Exception as e:
        logger.warning(f"Brave search error for '{query}': {e}")
        return []


def _google_news_search_results(query: str, max_results: int = 5) -> list[dict]:
    return [
        {
            "title": item.get("title", ""),
            "description": " · ".join(part for part in [item.get("source", ""), item.get("published", "")] if part),
            "url": item.get("url", ""),
            "source": "Google News",
        }
        for item in google_news(query, max_items=max_results)
    ]


def web_search(query, max_results=5):
    """Search the open web, using Tavily when configured and no-key fallbacks otherwise."""
    clean = " ".join(str(query or "").split())
    if not clean or not search_enabled():
        return []
    limit = max(1, min(int(max_results or 5), 10))
    results = []
    results.extend(_tavily_search(clean, max_results=limit))
    if len(results) < limit:
        results.extend(_duckduckgo_search(clean, max_results=limit * 2))
    if len(results) < limit:
        results.extend(_google_news_search_results(clean, max_results=limit))
    return _dedupe_results(results, limit)


def social_search(query: str, max_results: int = 5) -> list[dict]:
    """Search public social pages without falling back to Google News RSS."""
    clean = " ".join(str(query or "").split())
    if not clean or not search_enabled():
        return []
    limit = max(1, min(int(max_results or 5), 10))
    results = []
    results.extend(_tavily_search(clean, max_results=limit))
    if len(results) < limit:
        results.extend(_duckduckgo_search(clean, max_results=limit * 2))
    if len(results) < limit:
        results.extend(_brave_search(clean, max_results=limit * 2))
    if len(results) < limit:
        results.extend(_jina_search(clean, max_results=limit * 2))
    return _dedupe_results(results, limit)


def format_results(results):
    if not results:
        return "No results found."
    lines = []
    for r in results:
        source = f" ({r['source']})" if r.get("source") else ""
        lines.append(f"Title: {r['title']}{source}")
        if r.get("description"):
            lines.append(f"Summary: {r['description']}")
        lines.append(f"URL: {r['url']}")
        lines.append("")
    return "\n".join(lines).strip()


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def _research_query_variants(query: str, freshness: str = "latest") -> list[str]:
    clean = " ".join(str(query or "").split())
    if not clean:
        return []
    lowered = clean.lower()
    variants = [clean]
    if freshness in {"latest", "today", "recent"} and not re.search(r"\b(latest|today|recent|current|2026|news)\b", lowered):
        variants.append(f"{clean} latest")
    if not re.search(r"\b(official|site:)\b", lowered):
        variants.append(f"{clean} official")
    if re.search(r"\b(research|policy|education|teaching|curriculum|moe|developer|api|docs|framework|standard|law|rule|regulation)\b", lowered):
        variants.append(f"{clean} source official documentation")
    if re.search(r"\b(f1|formula 1|grand prix|liverpool|lfc|football|sports)\b", lowered):
        variants.append(f"{clean} official results schedule")
    return list(dict.fromkeys(variants))[:4]


def _source_rank(result: dict) -> int:
    domain = _domain_from_url(result.get("url", ""))
    title = str(result.get("title", "")).lower()
    score = 0
    if any(domain.endswith(suffix) for suffix in (".gov", ".edu", ".int")):
        score += 20
    if any(term in domain for term in ("gov.sg", "moe.gov.sg", "data.gov.sg", "formula1.com", "fia.com", "premierleague.com", "openai.com", "apple.com", "google.com", "muis.gov.sg")):
        score += 18
    if any(term in title for term in ("official", "documentation", "calendar", "schedule", "results", "release notes")):
        score += 8
    if any(term in domain for term in ("reddit.com", "facebook.com", "instagram.com", "tiktok.com", "pinterest.com")):
        score -= 10
    return score


TRUSTED_PRIMARY_DOMAINS = (
    "gov.sg", "moe.gov.sg", "data.gov.sg", "muis.gov.sg",
    "formula1.com", "fia.com", "premierleague.com",
    "openai.com", "platform.openai.com",
    "apple.com", "developer.apple.com",
    "google.com", "developers.google.com", "android.com",
)
REPUTABLE_NEWS_DOMAINS = (
    "reuters.com", "apnews.com", "bbc.com", "channelnewsasia.com", "cna.com",
    "straitstimes.com", "businesstimes.com.sg", "theguardian.com",
    "formula1.com", "skysports.com", "motorsport.com", "espn.com",
)
LOW_TRUST_DOMAINS = (
    "reddit.com", "facebook.com", "instagram.com", "tiktok.com",
    "pinterest.com", "quora.com", "medium.com",
)


def _source_type(domain: str) -> str:
    clean = (domain or "").lower()
    if any(clean.endswith(item) or item in clean for item in TRUSTED_PRIMARY_DOMAINS):
        return "official/primary"
    if clean.endswith(".gov") or clean.endswith(".edu") or ".gov." in clean or ".edu." in clean:
        return "official/primary"
    if any(item in clean for item in REPUTABLE_NEWS_DOMAINS):
        return "reputable news"
    if any(item in clean for item in LOW_TRUST_DOMAINS):
        return "community/low-trust"
    if clean:
        return "web source"
    return "unknown"


def _parse_source_date(value: str) -> datetime | None:
    clean = str(value or "").strip()
    if not clean:
        return None
    try:
        parsed = parsedate_to_datetime(clean)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(clean.replace(",", ""), fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _freshness_label(date_text: str, freshness: str = "latest", now: datetime | None = None) -> str:
    parsed = _parse_source_date(date_text)
    if not parsed:
        return "unknown date"
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (current.astimezone(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() / 3600)
    if freshness == "stable":
        return "dated source"
    if age_hours <= 36:
        return "fresh"
    if age_hours <= 14 * 24:
        return "recent"
    if age_hours <= 90 * 24:
        return "aging"
    return "stale"


def _source_grade(source: dict) -> str:
    source_type = source.get("source_type", "")
    fetched = bool(source.get("fetched"))
    freshness = source.get("freshness", "")
    if source_type == "official/primary" and fetched and freshness != "stale":
        return "A"
    if source_type in {"official/primary", "reputable news"} and (fetched or freshness in {"fresh", "recent"}):
        return "B"
    if source_type == "community/low-trust":
        return "D"
    return "C"


def _select_diverse_sources(ranked: list[dict], max_sources: int) -> list[dict]:
    limit = max(1, min(int(max_sources or 5), 8))
    selected = []
    used_domains = set()
    for item in ranked:
        domain = _domain_from_url(item.get("url", ""))
        if domain and domain in used_domains:
            continue
        selected.append(item)
        if domain:
            used_domains.add(domain)
        if len(selected) >= limit:
            return selected
    for item in ranked:
        if item in selected:
            continue
        selected.append(item)
        if len(selected) >= limit:
            break
    return selected


def _extract_source_date(text: str) -> str:
    sample = str(text or "")[:3000]
    patterns = (
        r"\b(?:published|updated|last updated|date)\s*[:\-]?\s*([A-Z][a-z]{2,8}\s+\d{1,2},?\s+20\d{2})",
        r"\b(\d{1,2}\s+[A-Z][a-z]{2,8}\s+20\d{2})\b",
        r"\b(20\d{2}-\d{2}-\d{2})\b",
        r"\b([A-Z][a-z]{2},\s+\d{1,2}\s+[A-Z][a-z]{2}\s+20\d{2})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, sample, re.I)
        if match:
            return match.group(1).strip()
    return ""


def _evidence_snippet(text: str, query: str, limit: int = 650) -> str:
    clean = " ".join(str(text or "").split())
    if not clean:
        return ""
    terms = [term for term in re.findall(r"[a-zA-Z0-9]{4,}", query.lower())[:8]]
    lower = clean.lower()
    start = 0
    for term in terms:
        index = lower.find(term)
        if index >= 0:
            start = max(0, index - 160)
            break
    snippet = clean[start:start + max(250, int(limit or 650))]
    return snippet.rsplit(" ", 1)[0].strip()


def web_research(query: str, max_sources: int = 5, fetch_pages: int = 3, freshness: str = "latest") -> dict:
    """
    Build a source pack for a research question: multiple searches, source ranking,
    and readable excerpts from top pages.
    """
    clean = " ".join(str(query or "").split())
    if not clean or not search_enabled():
        return {"ok": False, "query": clean, "error": "Web research is disabled or the query is empty.", "queries": [], "sources": []}

    queries = _research_query_variants(clean, freshness=freshness)
    pool = []
    per_query_limit = max(3, int(max_sources or 5))
    with ThreadPoolExecutor(max_workers=min(RESEARCH_WORKERS, max(1, len(queries)))) as executor:
        futures = {executor.submit(web_search, variant, per_query_limit): variant for variant in queries}
        for future in as_completed(futures):
            variant = futures[future]
            try:
                results = future.result()
            except Exception as exc:
                logger.warning(f"Research search variant failed for '{variant}': {exc}")
                results = []
            for result in results:
                item = dict(result)
                item["query"] = variant
                pool.append(item)

    ranked = sorted(_dedupe_results(pool, max(8, int(max_sources or 5) * 3)), key=_source_rank, reverse=True)
    selected = _select_diverse_sources(ranked, max_sources=max_sources)
    fetch_limit = max(0, min(int(fetch_pages or 3), len(selected)))
    base_sources = []
    for result in selected:
        source = {
            "title": result.get("title", ""),
            "url": result.get("url", ""),
            "domain": _domain_from_url(result.get("url", "")),
            "description": result.get("description", ""),
            "query": result.get("query", ""),
            "source_rank": _source_rank(result),
            "fetched": False,
            "date": _extract_source_date(result.get("description", "")),
            "evidence": result.get("description", ""),
        }
        base_sources.append(source)

    fetch_results = {}
    fetch_jobs = base_sources[:fetch_limit]
    if fetch_jobs:
        with ThreadPoolExecutor(max_workers=min(RESEARCH_WORKERS, len(fetch_jobs))) as executor:
            futures = {executor.submit(fetch_url, source.get("url", ""), 4500): idx for idx, source in enumerate(fetch_jobs)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    fetch_results[idx] = future.result()
                except Exception as exc:
                    fetch_results[idx] = {"ok": False, "error": str(exc)}

    sources = []
    for index, source in enumerate(base_sources):
        if index in fetch_results:
            fetched = fetch_results[index]
            source["fetched"] = bool(fetched.get("ok"))
            if fetched.get("ok"):
                source["resolved_url"] = fetched.get("url", "")
                source["page_title"] = fetched.get("title", "")
                source["date"] = _extract_source_date(fetched.get("text", "")) or source["date"]
                source["evidence"] = _evidence_snippet(fetched.get("text", ""), clean)
            else:
                source["fetch_error"] = fetched.get("error", "")
        source["id"] = f"S{index + 1}"
        source["source_type"] = _source_type(source.get("domain", ""))
        source["freshness"] = _freshness_label(source.get("date", ""), freshness=freshness)
        source["grade"] = _source_grade(source)
        source["citation"] = f"[{source['id']}]"
        sources.append(source)

    source_types = {source.get("source_type") for source in sources}
    quality = {
        "source_count": len(sources),
        "fetched_count": sum(1 for source in sources if source.get("fetched")),
        "official_count": sum(1 for source in sources if source.get("source_type") == "official/primary"),
        "fresh_or_recent_count": sum(1 for source in sources if source.get("freshness") in {"fresh", "recent"}),
        "domain_count": len({source.get("domain") for source in sources if source.get("domain")}),
        "has_low_trust": "community/low-trust" in source_types,
    }
    quality["confidence"] = (
        "strong" if quality["official_count"] and quality["fetched_count"] >= 2
        else "moderate" if quality["fetched_count"] or quality["official_count"]
        else "thin"
    )
    return {
        "ok": bool(sources),
        "query": clean,
        "queries": queries,
        "sources": sources,
        "quality": quality,
        "fetched_count": quality["fetched_count"],
        "answer_guidance": (
            "Use citation IDs like [S1]. Lead with confirmed facts from A/B sources, "
            "separate official sources from news/community sources, and call out stale or unfetched evidence."
        ),
    }


def format_research_pack(pack: dict) -> str:
    if not pack.get("ok"):
        return pack.get("error") or "No research sources found."
    lines = [f"Research query: {pack.get('query', '')}"]
    lines.append(f"Search variants: {', '.join(pack.get('queries') or [])}")
    quality = pack.get("quality") or {}
    lines.append(
        "Quality: "
        f"{quality.get('confidence', 'unknown')} | "
        f"{quality.get('source_count', len(pack.get('sources') or []))} source(s), "
        f"{quality.get('fetched_count', pack.get('fetched_count', 0))} fetched, "
        f"{quality.get('official_count', 0)} official/primary, "
        f"{quality.get('fresh_or_recent_count', 0)} fresh/recent"
    )
    if pack.get("answer_guidance"):
        lines.append(f"Answer guidance: {pack['answer_guidance']}")
    lines.append("")
    for idx, source in enumerate(pack.get("sources") or [], start=1):
        meta = " · ".join(part for part in [source.get("domain", ""), source.get("date", "")] if part)
        source_id = source.get("id") or f"S{idx}"
        grade = source.get("grade", "?")
        kind = source.get("source_type", "source")
        freshness_label = source.get("freshness", "unknown date")
        lines.append(f"[{source_id}] Grade {grade} · {kind} · {freshness_label}: {source.get('title', '')}{f' ({meta})' if meta else ''}")
        lines.append(f"URL: {source.get('resolved_url') or source.get('url', '')}")
        if source.get("fetched"):
            lines.append("Status: fetched")
        elif source.get("fetch_error"):
            lines.append(f"Status: search result only; fetch failed: {source.get('fetch_error')}")
        else:
            lines.append("Status: search result only")
        evidence = source.get("evidence", "")
        if evidence:
            lines.append(f"Evidence: {evidence[:900]}")
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
        _clear_google_news_error(query)
        feed = _parse_google_news_rss(query)
        return [e.title for e in feed.entries[:max_items] if hasattr(e, "title")]
    except Exception as e:
        _record_google_news_error(query, e)
        logger.warning(f"RSS error for '{query}': {e}")
        return []


def google_news(query, max_items=5, max_age_hours: int | None = None):
    """Fetch latest Google News RSS items for a query."""
    try:
        _clear_google_news_error(query)
        feed = _parse_google_news_rss(query)
        items = []
        for entry in feed.entries[: max_items * 4]:
            description = getattr(entry, "summary", "") or getattr(entry, "description", "")
            description = re.sub(r"<[^>]+>", " ", description or "")
            description = " ".join(description.split())
            items.append({
                "title": getattr(entry, "title", ""),
                "url": getattr(entry, "link", ""),
                "published": getattr(entry, "published", ""),
                "source": getattr(getattr(entry, "source", None), "title", ""),
                "description": description[:500],
            })
        ranked = _rank_news_items(items)
        if max_age_hours:
            max_age = max(1, int(max_age_hours))
            ranked = [
                item for item in ranked
                if (_news_age_hours(item) is not None and _news_age_hours(item) <= max_age)
            ]
        return ranked[:max_items]
    except Exception as e:
        _record_google_news_error(query, e)
        logger.warning(f"RSS error for '{query}': {e}")
        return []


def rss_feed_items(feed_url: str, source_label: str = "", max_items: int = 3) -> list[dict]:
    """Fetch ordinary RSS/Atom feed items for free source/community supplements."""
    url = str(feed_url or "").strip()
    if not _looks_like_url(url):
        return []
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HIRA/1.0; +https://example.com/hira)"},
            timeout=RSS_FEED_TIMEOUT,
        )
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        logger.warning(f"RSS feed error for '{url}': {e}")
        return []
    items = []
    for entry in getattr(feed, "entries", [])[: max(1, int(max_items or 3)) * 3]:
        description = getattr(entry, "summary", "") or getattr(entry, "description", "")
        description = re.sub(r"<[^>]+>", " ", description or "")
        description = " ".join(description.split())
        title = getattr(entry, "title", "")
        link = getattr(entry, "link", "")
        if not title or not link:
            continue
        items.append({
            "title": title,
            "url": link,
            "published": getattr(entry, "published", "") or getattr(entry, "updated", ""),
            "source": source_label or getattr(getattr(feed, "feed", {}), "title", "") or _domain_from_url(url),
            "description": description[:500],
        })
        if len(items) >= max(1, int(max_items or 3)):
            break
    return items


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


def _news_age_hours(item: dict, now: datetime | None = None) -> float | None:
    published = str((item or {}).get("published", "") or "").strip()
    if not published:
        return None
    try:
        parsed = parsedate_to_datetime(published)
    except Exception:
        try:
            parsed = datetime.fromisoformat(published)
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0.0, (current.astimezone(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() / 3600)


def _news_quality_score(item: dict, now: datetime | None = None) -> int:
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
    age_hours = _news_age_hours(item, now=now)
    if age_hours is not None:
        if age_hours <= 36:
            score += 10
        elif age_hours <= 96:
            score += 6
        elif age_hours <= 14 * 24:
            score += 1
        elif age_hours <= 45 * 24:
            score -= 8
        else:
            score -= 28
    return score


def _rank_news_items(items: list[dict], now: datetime | None = None) -> list[dict]:
    filtered = [item for item in items if _news_quality_score(item, now=now) > 0]
    ranked = filtered or items
    return sorted(ranked, key=lambda item: _news_quality_score(item, now=now), reverse=True)


def news_quality_score(item: dict, now: datetime | None = None) -> int:
    return _news_quality_score(item, now=now)


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
