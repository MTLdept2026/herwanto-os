"""
Herwanto's NBSS timetable data.
Generated: 13 March 2026
Sec 4 (BMLA) on Mondays removed — taken over by another teacher.

Week types: O = Odd, E = Even
Use /setweek to set the current week type once — bot calculates all future weeks.
"""

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
}

DAY_MAP = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}


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
#   MTL    = MTL Department activity (combined with dept colleagues)
#
# Class naming: "Sec 1" = all Sec 1 classes, "1 Flagship" = form class only

TIMETABLE = {

    # ── MONDAY ────────────────────────────────────────────────────────────────

    ("Mon", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2, 3],    "ML",    "Sec 3 (MLG33A)",            "L3-10",  "MLG33A"),
        # Sec 4 BMLA at P7-P9 REMOVED — taken over by another teacher
        _b([10, 11],  "ML",    "Sec 2 (MLG31)",             "L4-12",  "MLG31"),
        _b([12, 13],  "ML",    "Sec 1 (MLG21)",             "L4-11",  "MLG21"),
    ],

    ("Mon", "E"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2],       "ML",    "Sec 1 (MLG21A)",            "L4-11",  "MLG21A"),
        _b([7, 8, 9], "ML",    "Sec 3 (MLG33A)",            "L3-10",  "MLG33A"),
        _b([10, 11],  "ML",    "Sec 2 (MLG31)",             "L4-12",  "MLG31"),
    ],

    # ── TUESDAY ───────────────────────────────────────────────────────────────

    ("Tue", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([5, 6],    "ML",    "Sec 2 (MLG31A)",            "L4-12",  "MLG31A"),
        _b([10, 11],  "ML",    "Sec 1 (MLG21A)",            "L4-11",  "MLG21A"),
    ],

    ("Tue", "E"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([5, 6],    "ML",    "Sec 1 (MLG21)",             "L4-11",  "MLG21"),
        _b([12, 13],  "ML",    "Sec 3 (MLG33)",             "L3-10",  "MLG33"),
    ],

    # ── WEDNESDAY ─────────────────────────────────────────────────────────────

    ("Wed", "O"): [
        _b([2],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([3, 4],    "MTL",   "Dept Activity",             "-"),
        _b([7, 8, 9], "ML",    "Sec 2 (MLG31A)",            "L4-12",  "MLG31A"),
        _b([10, 11],  "ML",    "Sec 4 (BMLA)",              "L3-11",  "BMLA"),
    ],

    ("Wed", "E"): [
        _b([2],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([3, 4],    "MTL",   "Dept Activity",             "-"),
        _b([7, 8, 9], "ML",    "Sec 2 (MLG31A)",            "L4-12",  "MLG31A"),
        _b([10, 11],  "ML",    "Sec 4 (BMLA)",              "L4-12",  "BMLA"),
    ],

    # ── THURSDAY ──────────────────────────────────────────────────────────────

    ("Thu", "O"): [
        _b([1],       "FTCT",  "1 Flagship",                "L4-08"),
        _b([4, 5, 6], "IPW",   "2 Compass",                 "L3-03"),
        _b([12, 13],  "ML",    "Sec 3 (MLG33)",             "L3-10",  "MLG33"),
    ],

    ("Thu", "E"): [
        _b([1],       "FTCT/CCE", "1 Flagship",             "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([7, 8, 9], "ML",    "Sec 1 (MLG21A)",            "L4-11",  "MLG21A"),
    ],

    # ── FRIDAY ────────────────────────────────────────────────────────────────

    ("Fri", "O"): [
        _b([1],       "FTCT/CCE", "1 Flagship",             "L4-08"),
        _b([2],       "CCE",   "1 Flagship",                "L4-08"),
        _b([7, 8, 9], "ML",    "Sec 3 (MLG33A)",            "L3-10",  "MLG33A"),
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
