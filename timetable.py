"""
Herwanto's NBSS timetable data.
Source: Master teaching timetable supplied by Herwanto.
Timetable generated: 13/3/2026 | aSc Timetables.

Week types: O = Odd, E = Even
Use /setweek to set the current week type once — bot calculates all future weeks.
"""

from __future__ import annotations

from datetime import date, timedelta

# ─── PERIOD TIMES ────────────────────────────────────────────────────────────

PERIOD_TIMES = {
    1:  ("7:35",  "8:00"),
    2:  ("8:00",  "8:35"),
    3:  ("8:35",  "9:05"),
    4:  ("9:05",  "9:40"),
    5:  ("9:40",  "10:15"),
    6:  ("10:15", "10:50"),   # Recess window
    7:  ("10:50", "11:25"),
    8:  ("11:25", "11:55"),
    9:  ("11:55", "12:30"),
    10: ("12:30", "13:05"),
    11: ("13:05", "13:40"),
    12: ("13:40", "14:15"),
    13: ("14:15", "14:45"),
    14: ("14:45", "15:20"),
    15: ("15:20", "15:50"),
    16: ("15:50", "16:25"),
    17: ("16:25", "16:55"),
    18: ("16:55", "17:30"),
}

DAY_MAP = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}

# MOE 2026 MK/Primary/Secondary school terms.
SCHOOL_CALENDAR_2026_SOURCE = "https://www.moe.gov.sg/news/press-releases/20250730-school-terms-and-holidays-for-2026"
SCHOOL_TERMS_2026 = [
    ("Term I",  date(2026, 1, 2),  date(2026, 3, 13)),
    ("Term II", date(2026, 3, 23), date(2026, 5, 29)),
    ("Term III", date(2026, 6, 29), date(2026, 9, 4)),
    ("Term IV", date(2026, 9, 14), date(2026, 11, 20)),
]

SCHOOL_CLOSURE_DATES_2026 = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 2, 17),  # Chinese New Year
    date(2026, 2, 18),  # Chinese New Year
    date(2026, 3, 23),  # Hari Raya Puasa off-in-lieu
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 1),   # Labour Day
    date(2026, 5, 27),  # Hari Raya Haji
    date(2026, 6, 1),   # Vesak Day public holiday
    date(2026, 7, 6),   # Youth Day school holiday
    date(2026, 8, 10),  # National Day public holiday
    date(2026, 9, 4),   # Teachers' Day
    date(2026, 11, 9),  # Deepavali public holiday
}

SCHOOL_CALENDAR_MEMORY_2026 = (
    "MOE 2026 MK/Primary/Secondary calendar: Term I 2 Jan-13 Mar; "
    "Term II 23 Mar-29 May; Term III 29 Jun-4 Sep; Term IV 14 Sep-20 Nov. "
    "Timetable week numbers reset at each term start; odd-numbered weeks use Odd timetable, "
    "even-numbered weeks use Even timetable."
)

TIMETABLE_MEMORY_2026 = (
    "Herwanto's 2026 NBSS odd/even master teaching timetable is hardcoded from "
    "the user-supplied TIMETABLE - T. MTL MUHAMMAD HERWANTO JOHARI "
    "(generated 13 Mar 2026 via aSc Timetables). "
    "Use TIMETABLE in timetable.py as the source of truth for lesson periods, rooms, and week parity."
)


def _b(periods, subject, desc, room, code=""):
    """Build a lesson block from period numbers."""
    start = PERIOD_TIMES[periods[0]][0]
    end   = PERIOD_TIMES[periods[-1]][1]
    return {
        "periods": periods,
        "start": start,
        "end": end,
        "subject": subject,
        "description": desc,
        "room": room,
        "code": code,
    }


# ─── TIMETABLE DATA ──────────────────────────────────────────────────────────
#
# Format: TIMETABLE[(day, week_type)] = [list of lesson blocks]
#
# Subject codes:
#   FTCT   = Form Teacher Consultation Time (daily P1 assembly)
#   CCE    = Character & Citizenship Education
#   ML     = Bahasa Melayu lesson
#   IPW    = Interdisciplinary Project Work
#   PLT    = Professional Learning Teams
#
# Class naming: "Sec 1" = all Sec 1 classes, "1 Flagship" = form class only

TIMETABLE = {

    # ── MONDAY ────────────────────────────────────────────────────────────────

    ("Mon", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2, 3],    "ML",    "3 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L3-10", "MLG33A"),
        _b([10, 11],  "ML",    "2 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison/Harbour", "L4-12", "MLG31"),
        _b([12, 13],  "ML",    "1 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-11", "MLG21"),
    ],

    ("Mon", "E"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2, 3],    "ML",    "1 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-11", "MLG21A"),
        _b([7, 8],    "ML",    "3 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L3-10", "MLG33A"),
        _b([10, 11],  "ML",    "2 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison/Harbour", "L4-12", "MLG31"),
    ],

    # ── TUESDAY ───────────────────────────────────────────────────────────────

    ("Tue", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([5, 6],    "ML",    "2 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison/Harbour", "L4-12", "MLG31A"),
        _b([10, 11],  "ML",    "1 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-11", "MLG21A"),
    ],

    ("Tue", "E"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([5, 6],    "ML",    "1 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-11", "MLG21"),
        _b([12, 13],  "ML",    "3 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L3-10", "MLG33"),
    ],

    # ── WEDNESDAY ─────────────────────────────────────────────────────────────

    ("Wed", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([4, 5, 6], "PLT",   "MTL Department",            "-"),
        _b([7, 8],    "ML",    "2 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison/Harbour", "L4-12", "MLG31A"),
        _b([10, 11],  "ML",    "4 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-12", "BMLA"),
    ],

    ("Wed", "E"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([4, 5, 6], "PLT",   "MTL Department",            "-"),
        _b([7, 8],    "ML",    "2 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison/Harbour", "L4-12", "MLG31A"),
        _b([10, 11],  "ML",    "4 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-12", "BMLA"),
    ],

    # ── THURSDAY ──────────────────────────────────────────────────────────────

    ("Thu", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([5, 6],    "IPW",   "2 Compass",                 "L3-03"),
        _b([12, 13],  "ML",    "3 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L3-10", "MLG33"),
    ],

    ("Thu", "E"): [
        _b([1],       "FTCT/CCE", "1 Flagship",             "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([7, 8],    "ML",    "1 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L4-11", "MLG21A"),
    ],

    # ── FRIDAY ────────────────────────────────────────────────────────────────

    ("Fri", "O"): [
        _b([1],       "FTCT/CCE", "1 Flagship",             "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([7, 8],    "ML",    "3 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", "L3-10", "MLG33A"),
    ],

    ("Fri", "E"): [],  # Free day — no lessons
}


# ─── QUERY FUNCTIONS ─────────────────────────────────────────────────────────

def get_week_type(ref_date_str: str, ref_type: str, query_date: date) -> str:
    """
    Given a known reference date and its week type,
    return the week type (O/E) for any query date.

    ref_date_str : any date in the known week, "YYYY-MM-DD"
    ref_type     : "odd" or "even" (or "O" / "E")
    query_date   : the date to look up
    """
    ref = date.fromisoformat(ref_date_str)
    ref_monday   = ref - timedelta(days=ref.weekday())
    query_monday = query_date - timedelta(days=query_date.weekday())
    weeks_diff   = (query_monday - ref_monday).days // 7

    is_odd = ref_type.upper() in ("O", "ODD")
    if is_odd:
        return "O" if weeks_diff % 2 == 0 else "E"
    else:
        return "E" if weeks_diff % 2 == 0 else "O"


def get_school_week_info(query_date: date) -> dict | None:
    """
    Return official 2026 term/week info for MOE primary/secondary dates.

    Week numbers reset at each MOE term start. This matches common school
    timetable usage where week 1 is odd, week 2 is even, etc.
    """
    for term_name, start, end in SCHOOL_TERMS_2026:
        if start <= query_date <= end:
            week_number = ((query_date - start).days // 7) + 1
            week_type = "O" if week_number % 2 else "E"
            return {
                "term": term_name,
                "week_number": week_number,
                "week_type": week_type,
                "is_school_holiday": query_date in SCHOOL_CLOSURE_DATES_2026,
            }
    return None


def format_school_calendar_memory() -> str:
    return f"{SCHOOL_CALENDAR_MEMORY_2026} Source: {SCHOOL_CALENDAR_2026_SOURCE}"


def format_timetable_memory() -> str:
    return TIMETABLE_MEMORY_2026


def get_lessons(target_date: date, ref_date_str: str, ref_type: str) -> list:
    """Return lesson list for a specific date."""
    if target_date.weekday() > 4:   # Saturday / Sunday
        return []
    day_name  = DAY_MAP[target_date.weekday()]
    week_type = get_week_type(ref_date_str, ref_type, target_date)
    return TIMETABLE.get((day_name, week_type), [])


def format_lessons(lessons: list, week_type: str = "") -> str:
    """Format lesson list for Telegram display."""
    if not lessons:
        return "No timetabled lessons — free day."

    lines = []
    for lesson in lessons:
        room = f" · {lesson['room']}" if lesson["room"] != "-" else ""
        lines.append(
            f"• {lesson['start']}–{lesson['end']}  "
            f"*{lesson['subject']}* {lesson['description']}{room}"
        )
    return "\n".join(lines)


def week_type_label(wt: str) -> str:
    return "Odd" if wt == "O" else "Even"
