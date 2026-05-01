from __future__ import annotations

import csv
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pytz
import requests


logger = logging.getLogger(__name__)
SGT = pytz.timezone("Asia/Singapore")

DATA_DIR = Path(__file__).resolve().parent / "data"
PRAYER_CACHE_PATH = DATA_DIR / "muis_prayer_times.json"
MUIS_CONSOLIDATED_RESOURCE_ID = "d_a6a206cba471fe04b62dd886ef5eaf22"
MUIS_2026_RESOURCE_ID = "d_d441e7242e78efc566024dd5b0d9829c"
DATA_GOV_DATASTORE_URL = "https://data.gov.sg/api/action/datastore_search"

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


def _load_cache() -> dict:
    try:
        data = json.loads(PRAYER_CACHE_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_cache(cache: dict):
    DATA_DIR.mkdir(exist_ok=True)
    PRAYER_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_from_data_gov(resource_id: str) -> dict:
    records = {}
    offset = 0
    limit = 500
    while True:
        response = requests.get(
            DATA_GOV_DATASTORE_URL,
            params={"resource_id": resource_id, "limit": limit, "offset": offset},
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
