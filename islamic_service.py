from __future__ import annotations

import csv
import html
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from urllib.parse import urljoin

import pytz
import requests


logger = logging.getLogger(__name__)
SGT = pytz.timezone("Asia/Singapore")

DATA_DIR = Path(__file__).resolve().parent / "data"
PRAYER_CACHE_PATH = DATA_DIR / "muis_prayer_times.json"
BUNDLED_PRAYER_PATH = DATA_DIR / "muis_prayer_times_2026.json"
MUIS_CONSOLIDATED_RESOURCE_ID = "d_a6a206cba471fe04b62dd886ef5eaf22"
MUIS_2026_RESOURCE_ID = "d_d441e7242e78efc566024dd5b0d9829c"
DATA_GOV_DATASTORE_URL = "https://data.gov.sg/api/action/datastore_search"
MUIS_KHUTBAH_URL = "https://www.muis.gov.sg/resources/khutbah-and-religious-advice/khutbah/"
MUIS_BASE_URL = "https://www.muis.gov.sg"
KHUTBAH_CACHE_PATH = DATA_DIR / "muis_khutbah_cache.json"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 HIRA/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PRAYER_KEYS = ["subuh", "zohor", "asar", "maghrib", "isyak"]
PRAYER_LABELS = {
    "subuh": "Subuh",
    "syuruk": "Syuruk",
    "zohor": "Zohor",
    "asar": "Asar",
    "maghrib": "Maghrib",
    "isyak": "Isyak",
}

QURAN_REFLECTIONS = [
    {
        "ref": "Qur'an 2:286",
        "text": "Allah does not burden a soul beyond what it can bear.",
    },
    {
        "ref": "Qur'an 94:5-6",
        "text": "With hardship comes ease.",
    },
    {
        "ref": "Qur'an 13:28",
        "text": "In the remembrance of Allah do hearts find rest.",
    },
    {
        "ref": "Qur'an 20:114",
        "text": "My Lord, increase me in knowledge.",
    },
]

HADITH_REFLECTIONS = [
    {
        "ref": "Sahih Muslim",
        "text": "Cleanliness is half of faith.",
    },
    {
        "ref": "Sahih al-Bukhari",
        "text": "The most beloved deeds to Allah are those done consistently, even if small.",
    },
    {
        "ref": "Jami' al-Tirmidhi",
        "text": "Be mindful of Allah and He will protect you.",
    },
]

ISLAMIC_MONTHS = [
    "Muharram",
    "Safar",
    "Rabi' al-Awwal",
    "Rabi' al-Thani",
    "Jumada al-Awwal",
    "Jumada al-Thani",
    "Rajab",
    "Sha'ban",
    "Ramadan",
    "Shawwal",
    "Dhu al-Qi'dah",
    "Dhu al-Hijjah",
]

KEY_HIJRI_DATES = {
    (1, 1): "Awal Muharram",
    (9, 1): "Start of Ramadan",
    (9, 17): "Nuzul al-Quran",
    (10, 1): "Hari Raya Aidilfitri",
    (12, 9): "Day of Arafah",
    (12, 10): "Hari Raya Aidiladha",
}


def _normalise_time(value: str) -> str:
    clean = str(value or "").strip()
    if not clean:
        return ""
    parts = clean.split(":")
    if len(parts) >= 2:
        return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
    return clean


def _normalise_record(row: dict) -> dict | None:
    folded = {str(key).lower().replace(" ", "").replace("_", ""): value for key, value in row.items()}
    record_date = row.get("Date") or row.get("date") or folded.get("date")
    if not record_date:
        return None
    def pick(name: str):
        key = name.lower()
        return (
            row.get(name.title())
            or row.get(name)
            or folded.get(key)
            or folded.get(f"{key}time")
        )
    return {
        "date": str(record_date).strip(),
        "day": str(row.get("Day") or row.get("day") or folded.get("day") or "").strip(),
        "subuh": _normalise_time(pick("subuh")),
        "syuruk": _normalise_time(pick("syuruk")),
        "zohor": _normalise_time(pick("zohor")),
        "asar": _normalise_time(pick("asar")),
        "maghrib": _normalise_time(pick("maghrib")),
        "isyak": _normalise_time(pick("isyak")),
    }


@lru_cache(maxsize=1)
def _load_cache() -> dict:
    bundled = {}
    try:
        bundled_data = json.loads(BUNDLED_PRAYER_PATH.read_text(encoding="utf-8"))
        bundled = bundled_data if isinstance(bundled_data, dict) else {}
    except Exception:
        bundled = {}
    try:
        data = json.loads(PRAYER_CACHE_PATH.read_text(encoding="utf-8"))
        runtime = data if isinstance(data, dict) else {}
    except Exception:
        runtime = {}
    if not bundled:
        return runtime
    merged = dict(bundled)
    bundled_records = bundled.get("records", {}) if isinstance(bundled.get("records"), dict) else {}
    runtime_records = runtime.get("records", {}) if isinstance(runtime.get("records"), dict) else {}
    merged["records"] = {**bundled_records, **runtime_records}
    if runtime.get("source"):
        merged["source"] = runtime.get("source")
    return merged


def _save_cache(cache: dict):
    DATA_DIR.mkdir(exist_ok=True)
    PRAYER_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    _load_cache.cache_clear()


def _fetch_from_data_gov(resource_id: str) -> dict:
    records = {}
    offset = 0
    limit = 500
    while True:
        response = requests.get(
            DATA_GOV_DATASTORE_URL,
            params={"resource_id": resource_id, "limit": limit, "offset": offset},
            headers={"User-Agent": "HIRA/1.0 prayer-time-cache"},
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("result", {}).get("records", [])
        for row in rows:
            record = _normalise_record(row)
            if record and record["date"]:
                records[record["date"]] = record
        if len(rows) < limit:
            break
        offset += limit
    return records


def refresh_prayer_cache() -> dict:
    resource_id = os.environ.get("MUIS_PRAYER_RESOURCE_ID", MUIS_CONSOLIDATED_RESOURCE_ID).strip() or MUIS_CONSOLIDATED_RESOURCE_ID
    try:
        records = _fetch_from_data_gov(resource_id)
        if records:
            _save_cache({"source": "data.gov.sg MUIS Muslim Prayer Timetable", "resource_id": resource_id, "records": records})
            return records
    except Exception as exc:
        logger.warning("Could not fetch consolidated MUIS prayer times: %s", exc)

    try:
        records = _fetch_from_data_gov(MUIS_2026_RESOURCE_ID)
        if records:
            _save_cache({"source": "data.gov.sg MUIS Muslim Prayer Timetable 2026", "resource_id": MUIS_2026_RESOURCE_ID, "records": records})
            return records
    except Exception as exc:
        logger.warning("Could not fetch 2026 MUIS prayer times: %s", exc)
    return {}


def get_prayer_times(target: date | None = None) -> dict:
    target = target or datetime.now(SGT).date()
    date_key = target.isoformat()
    cache = _load_cache()
    records = cache.get("records", {}) if isinstance(cache.get("records"), dict) else {}
    if date_key not in records:
        records = refresh_prayer_cache() or records
    record = records.get(date_key)
    if not record:
        raise ValueError(f"No MUIS prayer time found for {date_key}")
    return record


def prayer_datetime(target: date, time_text: str) -> datetime:
    hour, minute = [int(part) for part in time_text.split(":")[:2]]
    return SGT.localize(datetime(target.year, target.month, target.day, hour, minute))


def prayer_schedule(target: date | None = None) -> list[dict]:
    target = target or datetime.now(SGT).date()
    times = get_prayer_times(target)
    schedule = []
    for key in PRAYER_KEYS:
        value = times.get(key, "")
        if not value:
            continue
        schedule.append({
            "key": key,
            "label": PRAYER_LABELS[key],
            "time": value,
            "datetime": prayer_datetime(target, value),
        })
    return schedule


def next_prayer(now: datetime | None = None) -> dict | None:
    now = (now or datetime.now(SGT)).astimezone(SGT)
    for item in prayer_schedule(now.date()):
        if item["datetime"] >= now:
            return item
    tomorrow = now.date() + timedelta(days=1)
    schedule = prayer_schedule(tomorrow)
    return schedule[0] if schedule else None


def hijri_date(gregorian: date | None = None) -> dict:
    # Civil/tabular approximation, useful for daily context. MUIS moon-sighting
    # announcements remain authoritative for key observances.
    gregorian = gregorian or datetime.now(SGT).date()
    y = gregorian.year
    m = gregorian.month
    d = gregorian.day
    if m < 3:
        y -= 1
        m += 12
    a = y // 100
    b = 2 - a + (a // 4)
    jd = int(365.25 * (y + 4716)) + int(30.6001 * (m + 1)) + d + b - 1524
    islamic = jd - 1948440 + 10632
    n = (islamic - 1) // 10631
    islamic = islamic - 10631 * n + 354
    j = ((10985 - islamic) // 5316) * ((50 * islamic) // 17719) + (islamic // 5670) * ((43 * islamic) // 15238)
    islamic = islamic - ((30 - j) // 15) * ((17719 * j) // 50) - (j // 16) * ((15238 * j) // 43) + 29
    month = (24 * islamic) // 709
    day = islamic - (709 * month) // 24
    year = 30 * n + j - 30
    month_name = ISLAMIC_MONTHS[max(0, min(11, month - 1))]
    return {"day": int(day), "month": int(month), "month_name": month_name, "year": int(year)}


def hijri_context(gregorian: date | None = None) -> str:
    h = hijri_date(gregorian)
    label = f"{h['day']} {h['month_name']} {h['year']}H"
    event = KEY_HIJRI_DATES.get((h["month"], h["day"]))
    return f"{label}" + (f" · {event}" if event else "")


def is_sunnah_fasting_day(gregorian: date | None = None) -> str:
    gregorian = gregorian or datetime.now(SGT).date()
    h = hijri_date(gregorian)
    if h["month"] == 9:
        return "Ramadan fast"
    if gregorian.weekday() in (0, 3):
        return "Sunnah fasting day (Monday/Thursday)"
    if h["day"] in (13, 14, 15):
        return "Ayyam al-Bid fasting day"
    if h["month"] == 10 and 2 <= h["day"] <= 30:
        return "Shawwal fast window"
    if h["month"] == 12 and h["day"] == 9:
        return "Day of Arafah fast"
    if h["month"] == 1 and h["day"] in (9, 10):
        return "Tasu'a/Ashura fast"
    return ""


def daily_reflection(target: date | None = None) -> dict:
    target = target or datetime.now(SGT).date()
    pool = QURAN_REFLECTIONS + HADITH_REFLECTIONS
    return pool[target.toordinal() % len(pool)]


def format_prayer_times(target: date | None = None) -> str:
    target = target or datetime.now(SGT).date()
    times = get_prayer_times(target)
    parts = [f"{PRAYER_LABELS[key]} {times[key]}" for key in PRAYER_KEYS if times.get(key)]
    return " · ".join(parts)


def _fetch_html(url: str) -> str:
    response = requests.get(url, headers=HTTP_HEADERS, timeout=15)
    response.raise_for_status()
    return response.content.decode("utf-8", errors="replace")


def _has_mojibake(value) -> bool:
    return "â" in json.dumps(value, ensure_ascii=False)


def _strip_tags(value: str) -> str:
    clean = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.IGNORECASE)
    clean = re.sub(r"<style[\s\S]*?</style>", " ", clean, flags=re.IGNORECASE)
    clean = re.sub(r"<[^>]+>", " ", clean)
    return re.sub(r"\s+", " ", html.unescape(clean)).strip()


def _parse_display_date(value: str) -> date | None:
    clean = re.sub(r"\s+", " ", value or "").strip()
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(clean, fmt).date()
        except ValueError:
            continue
    return None


def _load_khutbah_cache() -> dict:
    try:
        data = json.loads(KHUTBAH_CACHE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_khutbah_cache(cache: dict):
    DATA_DIR.mkdir(exist_ok=True)
    KHUTBAH_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_khutbah_listing(html_text: str, language: str = "English") -> list[dict]:
    preferred = (language or "English").strip().lower()
    cards = []
    for match in re.finditer(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html_text, flags=re.IGNORECASE | re.DOTALL):
        body = match.group(2)
        lang_match = re.search(r'<p[^>]*>\s*(English|Malay/Jawi|Tamil)\s*</p>', body, flags=re.IGNORECASE)
        if not lang_match or lang_match.group(1).lower() != preferred:
            continue
        date_match = re.search(r'<p[^>]*>\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})\s*</p>', body, flags=re.IGNORECASE)
        title_match = re.search(r'<span[^>]*title="([^"]+)"[^>]*>', body, flags=re.IGNORECASE)
        description_match = re.search(r'<p[^>]*line-clamp-3[^>]*>(.*?)</p>', body, flags=re.IGNORECASE | re.DOTALL)
        published = _parse_display_date(_strip_tags(date_match.group(1))) if date_match else None
        if not published or not title_match:
            continue
        cards.append({
            "date": published.isoformat(),
            "title": _strip_tags(title_match.group(1)),
            "summary": _strip_tags(description_match.group(1)) if description_match else "",
            "language": lang_match.group(1),
            "url": urljoin(MUIS_BASE_URL, html.unescape(match.group(1))),
        })
    return cards


def _parse_khutbah_article(html_text: str, base_record: dict) -> dict:
    text = _strip_tags(html_text)
    pdfs = [html.unescape(item) for item in re.findall(r'https?://[^"\']+?\.pdf', html_text, flags=re.IGNORECASE)]
    title = base_record.get("title") or _strip_tags(re.search(r"<h1[^>]*>(.*?)</h1>", html_text, re.DOTALL | re.IGNORECASE).group(1))
    marker = f"{base_record.get('language', 'English')} {title}"
    start = text.find(marker)
    useful = text[start + len(marker):] if start >= 0 else text
    end = useful.find("Download ")
    if end >= 0:
        useful = useful[:end]
    useful = useful.strip()
    date_text = ""
    if base_record.get("date"):
        try:
            date_text = date.fromisoformat(base_record["date"]).strftime("%-d %B %Y")
        except Exception:
            date_text = ""
    if date_text and useful.startswith(date_text):
        useful = useful[len(date_text):].strip()

    principles = []
    principle_match = re.search(r"(Three guiding principles.*)", useful, flags=re.IGNORECASE)
    if principle_match:
        principle_text = principle_match.group(1)
        principles = [
            item.strip(" :")
            for item in re.split(r"\b(?:First|Second|Third|Fourth):", principle_text, flags=re.IGNORECASE)[1:]
            if item.strip()
        ]
        useful = useful[:principle_match.start()].strip()

    return {
        **base_record,
        "summary": useful or base_record.get("summary", ""),
        "key_points": principles,
        "pdf_url": pdfs[0] if pdfs else "",
        "source": "MUIS Khutbah",
        "fetched_at": datetime.now(SGT).isoformat(),
    }


def latest_khutbah(target: date | None = None, language: str = "English", force_refresh: bool = False) -> dict:
    target = target or datetime.now(SGT).date()
    cache_key = f"{target.isoformat()}:{(language or 'English').lower()}"
    cache = _load_khutbah_cache()
    if not force_refresh and cache.get(cache_key) and not _has_mojibake(cache[cache_key]):
        return cache[cache_key]

    listing = _fetch_html(MUIS_KHUTBAH_URL)
    records = _parse_khutbah_listing(listing, language)
    candidates = [item for item in records if date.fromisoformat(item["date"]) <= target]
    if not candidates:
        raise ValueError(f"No MUIS khutbah found for {target.isoformat()}")
    record = max(candidates, key=lambda item: item["date"])
    article = _parse_khutbah_article(_fetch_html(record["url"]), record)
    cache[cache_key] = article
    _save_khutbah_cache(cache)
    return article
