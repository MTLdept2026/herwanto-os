from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Callable

from timetable import SCHOOL_CLOSURE_DATES_2026


def classops_name_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def classops_record_for(ledger: dict, class_name: str) -> dict:
    classes = ledger.get("classes") if isinstance(ledger, dict) else {}
    if not isinstance(classes, dict):
        return {"lessons": [], "assignments": []}
    class_key = str(class_name or "").strip().upper()
    return classes.get(class_key) or classes.get(str(class_name or "").strip()) or {"lessons": [], "assignments": []}


def parse_classops_date(value: str) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for candidate in (raw[:10], raw):
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            continue
    return None


def classops_assignment_date(assignment: dict) -> date | None:
    return (
        parse_classops_date(assignment.get("lesson_date", ""))
        or parse_classops_date(assignment.get("collect_by", ""))
        or parse_classops_date(assignment.get("created_at", ""))
    )


def _classops_assignment_legacy_key(assignment: dict) -> str:
    return "|".join([
        str(assignment.get("lesson_date") or "").strip(),
        str(assignment.get("folder") or "").strip(),
        str(assignment.get("assignment_title") or "").strip().lower(),
    ])


def _classops_assignment_updated_key(assignment: dict, index: int) -> tuple[str, int]:
    updated = str(assignment.get("updated_at") or assignment.get("created_at") or "").strip()
    return (updated, index)


def normalise_classops_assignments(assignments: list[dict]) -> list[dict]:
    source_by_legacy_key: dict[str, list[str]] = {}
    for assignment in assignments:
        source_path = str(assignment.get("source_path") or "").strip()
        legacy_key = _classops_assignment_legacy_key(assignment)
        if source_path and legacy_key.strip("|"):
            source_by_legacy_key.setdefault(legacy_key, []).append(source_path)

    deduped: dict[tuple[str, str], tuple[tuple[str, int], dict]] = {}
    for index, assignment in enumerate(assignments):
        source_path = str(assignment.get("source_path") or "").strip()
        legacy_key = _classops_assignment_legacy_key(assignment)
        if source_path:
            key = ("source", source_path)
        elif legacy_key in source_by_legacy_key and len(source_by_legacy_key[legacy_key]) == 1:
            key = ("source", source_by_legacy_key[legacy_key][0])
        else:
            key = ("legacy", legacy_key or str(assignment.get("id") or index))
        updated_key = _classops_assignment_updated_key(assignment, index)
        if key not in deduped or updated_key >= deduped[key][0]:
            deduped[key] = (updated_key, assignment)

    return [item for _, item in sorted(deduped.values(), key=lambda value: value[0])]


def classops_timing_context(target: date | None) -> list[dict]:
    if not target:
        return []
    context = []
    if target.weekday() == 0:
        context.append({"key": "after_weekend", "label": "Due after weekend"})
    elif target.weekday() in {5, 6}:
        context.append({"key": "weekend_due", "label": "Due over weekend"})
    if target in SCHOOL_CLOSURE_DATES_2026:
        context.append({"key": "school_closure", "label": "Due on school/public closure"})
    if target - timedelta(days=1) in SCHOOL_CLOSURE_DATES_2026:
        context.append({"key": "after_public_holiday", "label": "Due after school/public closure"})
    return context


def _classops_make_event(assignment: dict, timing_context: list[dict]) -> dict:
    return {
        "assignment_id": str(assignment.get("id", "")),
        "assignment_title": str(assignment.get("assignment_title") or "Tracked work"),
        "lesson_date": str(assignment.get("lesson_date") or ""),
        "collect_by": str(assignment.get("collect_by") or ""),
        "timing_context": timing_context,
    }


def _number_from_mark(value: str) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", raw)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _mark_watch_flags(fields: dict) -> list[dict]:
    flags = []
    if not isinstance(fields, dict):
        return flags
    protected = {"name", "class", "index", "s/n", "no", "student"}
    for label, value in fields.items():
        clean_label = str(label or "").strip()
        if not clean_label or clean_label.lower() in protected:
            continue
        number = _number_from_mark(str(value or ""))
        if number is None:
            continue
        flag = ""
        if number < 50:
            flag = "below 50"
        elif number < 60:
            flag = "near threshold"
        if flag:
            flags.append({
                "label": clean_label,
                "value": str(value),
                "score": number,
                "reason": flag,
            })
    return flags[:4]


def _lesson_terms(lesson_text: str, title: str = "") -> list[str]:
    stop = {
        "about", "after", "akan", "atau", "because", "before", "dalam", "dengan", "from", "have",
        "lesson", "murid", "pada", "that", "their", "there", "this", "untuk", "yang",
    }
    words = re.findall(r"[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'-]{3,}", f"{title} {lesson_text}")
    counts: dict[str, int] = {}
    display: dict[str, str] = {}
    for word in words:
        key = word.lower().strip("-'")
        if key in stop or len(key) < 4:
            continue
        counts[key] = counts.get(key, 0) + 1
        display.setdefault(key, word.strip("-'"))
    ranked = sorted(counts, key=lambda key: (counts[key], len(key)), reverse=True)
    return [display[key] for key in ranked[:6]]


def _lesson_snippets(lesson_text: str, max_items: int = 3) -> list[str]:
    clean = re.sub(r"\s+", " ", str(lesson_text or "")).strip()
    if not clean:
        return []
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", clean) if part.strip()]
    good = [part for part in parts if 28 <= len(part) <= 220]
    return good[:max_items] or [clean[:180]]


def build_classops_insights(class_name: str, roster: list[dict], assignments: list[dict], today: date) -> list[dict]:
    insights = []
    risky = sorted(
        [student for student in roster if student.get("risk_score", 0) > 0],
        key=lambda item: (item.get("risk_score", 0), item.get("missing_count", 0)),
        reverse=True,
    )
    if risky:
        names = [student["name"] for student in risky[:3]]
        insights.append({
            "severity": "critical" if risky[0].get("missing_count", 0) >= 2 else "watch",
            "kind": "student_risk",
            "title": f"{len(risky)} student{'s' if len(risky) != 1 else ''} need follow-up",
            "detail": ", ".join(names),
            "students": names,
        })

    mark_watch = [
        student for student in roster
        if any(flag.get("reason") == "below 50" for flag in (student.get("mark_flags") or []))
    ]
    if mark_watch:
        names = [student["name"] for student in mark_watch[:3]]
        insights.append({
            "severity": "watch",
            "kind": "marks_watch",
            "title": f"{len(mark_watch)} student{'s' if len(mark_watch) != 1 else ''} have marks below threshold",
            "detail": ", ".join(names),
            "students": names,
        })

    pattern_counts: dict[str, int] = {}
    pattern_students: dict[str, set[str]] = {}
    for student in roster:
        for key, count in (student.get("timing_patterns") or {}).items():
            if count <= 0:
                continue
            pattern_counts[key] = pattern_counts.get(key, 0) + count
            pattern_students.setdefault(key, set()).add(student["name"])
    pattern_labels = {
        "after_weekend": "after weekends",
        "after_public_holiday": "after school/public closures",
        "weekend_due": "over weekends",
        "school_closure": "on school/public closures",
    }
    for key, count in sorted(pattern_counts.items(), key=lambda item: item[1], reverse=True)[:2]:
        if count < 2 and key not in {"after_public_holiday", "school_closure"}:
            continue
        names = sorted(pattern_students.get(key, set()))[:3]
        insights.append({
            "severity": "watch",
            "kind": "timing_pattern",
            "title": f"Submission pattern {pattern_labels.get(key, key.replace('_', ' '))}",
            "detail": f"{count} non-submission signal{'s' if count != 1 else ''}: {', '.join(names)}",
            "students": names,
        })

    assignment_dates = [day for day in (classops_assignment_date(item) for item in assignments) if day]
    if assignment_dates:
        latest = max(assignment_dates)
        gap_days = (today - latest).days
        if gap_days >= 10:
            insights.append({
                "severity": "watch" if gap_days < 21 else "critical",
                "kind": "assignment_gap",
                "title": f"{class_name} has not had tracked work for {gap_days} days",
                "detail": f"Last tracked assignment was on {latest.isoformat()}.",
                "days": gap_days,
            })
    elif roster:
        insights.append({
            "severity": "watch",
            "kind": "assignment_gap",
            "title": f"{class_name} has no tracked assignments yet",
            "detail": "Start tracking from a contents item when you next collect work.",
            "days": None,
        })
    return insights[:5]


def build_student_report(class_name: str, students: list[dict], ledger: dict | None = None, today: date | None = None) -> dict:
    ledger = ledger if isinstance(ledger, dict) else {"classes": {}}
    today = today or datetime.now().date()
    record = classops_record_for(ledger, class_name)
    assignments = normalise_classops_assignments([item for item in record.get("assignments", []) if isinstance(item, dict)])
    roster = []
    for index, student in enumerate(students or [], start=1):
        name = str(student.get("name") or "").strip()
        if not name:
            continue
        roster.append({
            "no": str(student.get("no") or index).strip(),
            "class": str(student.get("class") or class_name).strip(),
            "name": name,
            "source": str(student.get("source") or "").strip(),
            "submitted_count": 0,
            "missing_count": 0,
            "absent_count": 0,
            "catchup_count": 0,
            "risk_score": 0,
            "risk_reasons": [],
            "timing_patterns": {},
            "marks": student.get("fields") if isinstance(student.get("fields"), dict) else {},
            "mark_flags": _mark_watch_flags(student.get("fields") if isinstance(student.get("fields"), dict) else {}),
            "status": "clear",
        })

    roster_by_key = {classops_name_key(item["name"]): item for item in roster}
    student_events = {key: {"missing": [], "absent": []} for key in roster_by_key}
    unmatched = {"absent": [], "submitted": [], "non_submitted": []}
    assignment_summaries = []
    for assignment in assignments:
        due_date = parse_classops_date(assignment.get("collect_by", ""))
        timing_context = classops_timing_context(due_date)
        event = _classops_make_event(assignment, timing_context)
        submitted = {classops_name_key(name) for name in assignment.get("submitted", []) if classops_name_key(name)}
        non_submitted = {classops_name_key(name) for name in assignment.get("non_submitted", []) if classops_name_key(name)}
        absent = {classops_name_key(name) for name in assignment.get("absent", []) if classops_name_key(name)}
        non_submission_mode = (
            str(assignment.get("tracking_mode") or "") == "non_submission_list"
            or bool(assignment.get("source_path"))
            or ("non_submitted" in assignment and not submitted)
        )

        for raw in assignment.get("submitted", []) or []:
            key = classops_name_key(raw)
            if key and key not in roster_by_key:
                unmatched["submitted"].append({"assignment_id": assignment.get("id", ""), "name": str(raw)})
        for raw in assignment.get("non_submitted", []) or []:
            key = classops_name_key(raw)
            if key and key not in roster_by_key:
                unmatched["non_submitted"].append({"assignment_id": assignment.get("id", ""), "name": str(raw)})
        for raw in assignment.get("absent", []) or []:
            key = classops_name_key(raw)
            if key and key not in roster_by_key:
                unmatched["absent"].append({"assignment_id": assignment.get("id", ""), "name": str(raw)})

        submitted_total = 0
        missing_total = 0
        absent_total = 0
        for student in roster:
            key = classops_name_key(student["name"])
            timeline_status = "submitted"
            if key in absent:
                absent_total += 1
                student["absent_count"] += 1
                student["catchup_count"] += 1
                student_events[key]["absent"].append(event)
                timeline_status = "absent"
            elif non_submission_mode or non_submitted:
                if key in non_submitted:
                    missing_total += 1
                    student["missing_count"] += 1
                    student_events[key]["missing"].append(event)
                    timeline_status = "missing"
                else:
                    submitted_total += 1
                    student["submitted_count"] += 1
            elif key in submitted:
                submitted_total += 1
                student["submitted_count"] += 1
            else:
                missing_total += 1
                student["missing_count"] += 1
                student_events[key]["missing"].append(event)
                timeline_status = "missing"
            student.setdefault("timeline", []).append({
                **event,
                "status": timeline_status,
            })

        assignment_summaries.append({
            **assignment,
            "roster_count": len(roster),
            "submitted_count": submitted_total,
            "missing_count": missing_total,
            "absent_count": absent_total,
            "timing_context": timing_context,
        })

    for student in roster:
        key = classops_name_key(student["name"])
        events = student_events.get(key, {"missing": [], "absent": []})
        overdue = 0
        timing_patterns: dict[str, int] = {}
        for event in events.get("missing", []):
            due = parse_classops_date(event.get("collect_by", ""))
            if due and due < today:
                overdue += 1
            for timing in event.get("timing_context", []):
                timing_key = timing.get("key", "")
                if timing_key:
                    timing_patterns[timing_key] = timing_patterns.get(timing_key, 0) + 1

        reasons = []
        if student["missing_count"] >= 2:
            reasons.append(f"Repeated non-submission across {student['missing_count']} tracked assignments")
        elif student["missing_count"] == 1:
            reasons.append("One open non-submission")
        if overdue:
            reasons.append(f"{overdue} overdue item{'s' if overdue != 1 else ''}")
        if student["catchup_count"] >= 1:
            reasons.append(f"{student['catchup_count']} absence catch-up item{'s' if student['catchup_count'] != 1 else ''}")
        if timing_patterns.get("after_weekend", 0) >= 2:
            reasons.append("Pattern appears after weekends")
        if timing_patterns.get("after_public_holiday", 0) >= 1:
            reasons.append("Watch after school/public holiday")
        if student.get("mark_flags"):
            reasons.append(f"Marks watch: {student['mark_flags'][0]['label']} {student['mark_flags'][0]['value']}")
        student["timing_patterns"] = timing_patterns
        student["risk_reasons"] = reasons
        student["risk_score"] = student["missing_count"] * 3 + overdue * 2 + student["catchup_count"] + len(student.get("mark_flags") or [])
        if student["missing_count"] >= 2:
            student["status"] = "follow up"
        elif student["catchup_count"] >= 1:
            student["status"] = "catch up"
        elif student["missing_count"] == 1:
            student["status"] = "watch"
        elif student.get("mark_flags"):
            student["status"] = "marks watch"

    concerns = sorted(
        [student for student in roster if student["status"] != "clear"],
        key=lambda item: (int(item.get("risk_score", 0) or 0), int(item.get("missing_count", 0) or 0)),
        reverse=True,
    )
    insights = build_classops_insights(class_name, roster, assignments, today)
    priority_items = build_priority_items(class_name, roster, assignment_summaries, insights, today)
    blind_spots = build_blind_spots(class_name, roster, assignment_summaries, unmatched, insights, today)
    feed_forward_groups = build_feed_forward_groups(roster)
    return {
        "class_name": class_name,
        "roster_count": len(roster),
        "assignment_count": len(assignments),
        "open_non_submission_count": sum(len(item.get("non_submitted", []) or []) for item in assignments),
        "concern_count": len(concerns),
        "insight_count": len(insights),
        "priority_items": priority_items,
        "blind_spots": blind_spots,
        "feed_forward_groups": feed_forward_groups,
        "watchlist": concerns[:8],
        "students": roster,
        "concerns": concerns,
        "assignments": assignment_summaries[-12:],
        "insights": insights,
        "unmatched": unmatched,
    }


def build_feed_forward_groups(roster: list[dict]) -> list[dict]:
    groups = [
        {
            "key": "reteach",
            "label": "Reteach",
            "trigger": "Low marks or repeated missing work",
            "students": [],
            "move": "Start with guided modelling and one worked example before independent practice.",
        },
        {
            "key": "practice",
            "label": "Practice",
            "trigger": "One open submission or near-threshold marks",
            "students": [],
            "move": "Give short deliberate practice and check the first two answers quickly.",
        },
        {
            "key": "catchup",
            "label": "Catch-up",
            "trigger": "Absent or missing submission history",
            "students": [],
            "move": "Recover the missed task before adding fresh extension work.",
        },
        {
            "key": "extension",
            "label": "Extension",
            "trigger": "No visible submission or marks concern",
            "students": [],
            "move": "Offer a higher-order application or peer explanation prompt.",
        },
    ]
    by_key = {item["key"]: item for item in groups}
    for student in roster:
        flags = student.get("mark_flags") or []
        reasons = " ".join(student.get("risk_reasons") or [])
        target = "extension"
        if any(flag.get("reason") == "below 50" for flag in flags) or int(student.get("missing_count", 0) or 0) >= 2:
            target = "reteach"
        elif int(student.get("catchup_count", 0) or 0) or int(student.get("absent_count", 0) or 0):
            target = "catchup"
        elif int(student.get("missing_count", 0) or 0) or flags:
            target = "practice"
        by_key[target]["students"].append({
            "name": student.get("name", ""),
            "reason": reasons or student.get("status", "clear"),
        })
    return [
        {**group, "count": len(group["students"]), "students": group["students"][:8]}
        for group in groups
        if group["students"]
    ]


def build_priority_items(class_name: str, roster: list[dict], assignments: list[dict], insights: list[dict], today: date) -> list[dict]:
    items = []
    for assignment in reversed(assignments or []):
        missing = int(assignment.get("missing_count", 0) or 0)
        if not missing:
            continue
        due = parse_classops_date(assignment.get("collect_by", ""))
        if due and due < today:
            tone = "critical"
            action = "Clear overdue non-submissions before the next lesson."
        elif due == today:
            tone = "watch"
            action = "Collect or confirm outstanding work today."
        else:
            tone = "watch"
            action = "Keep this visible until collection closes."
        items.append({
            "tone": tone,
            "title": f"{class_name}: {assignment.get('assignment_title') or 'Tracked work'}",
            "detail": f"{missing} pending out of {assignment.get('roster_count', 0)} students.",
            "action": action,
        })
    for student in roster:
        flags = student.get("mark_flags") or []
        if not flags:
            continue
        items.append({
            "tone": "watch",
            "title": f"Marks watch: {student.get('name', '')}",
            "detail": f"{flags[0].get('label')}: {flags[0].get('value')} ({flags[0].get('reason')}).",
            "action": "Check whether this needs remediation, retest, or targeted feedback.",
        })
    for insight in insights[:2]:
        items.append({
            "tone": "critical" if insight.get("severity") == "critical" else "watch",
            "title": insight.get("title", "ClassOps signal"),
            "detail": insight.get("detail", ""),
            "action": "Review before planning the next lesson.",
        })
    return items[:6]


def build_blind_spots(
    class_name: str,
    roster: list[dict],
    assignments: list[dict],
    unmatched: dict,
    insights: list[dict],
    today: date,
) -> list[dict]:
    spots = []
    if roster and not assignments:
        spots.append({
            "tone": "watch",
            "title": "No tracked submissions yet",
            "detail": "Classlist is present, but no submission ledger exists for this class.",
        })
    if assignments:
        latest = classops_assignment_date(assignments[-1])
        if latest and (today - latest).days >= 10:
            spots.append({
                "tone": "watch",
                "title": "Tracked work may be stale",
                "detail": f"Last tracked item appears to be {(today - latest).days} days old.",
            })
    if roster and not any(student.get("marks") for student in roster):
        spots.append({
            "tone": "muted",
            "title": "Marks not visible in roster feed",
            "detail": "Google Drive classlist loaded names, but no mark columns were returned.",
        })
    unmatched_count = sum(len(values or []) for values in (unmatched or {}).values())
    if unmatched_count:
        spots.append({
            "tone": "watch",
            "title": "Names need reconciliation",
            "detail": f"{unmatched_count} submission names did not match the classlist exactly.",
        })
    if not any(insight.get("kind") == "marks_watch" for insight in insights) and any(student.get("marks") for student in roster):
        spots.append({
            "tone": "muted",
            "title": "Marks loaded, no low-mark alert",
            "detail": "Scores are present and no below-threshold pattern was detected.",
        })
    return spots[:5]


def build_lesson_reflection_worksheet(class_name: str, lesson: dict, report: dict | None = None) -> dict:
    report = report if isinstance(report, dict) else {}
    watchlist = [
        student for student in (report.get("watchlist") or report.get("concerns") or [])
        if isinstance(student, dict) and student.get("name")
    ][:5]
    title = str(lesson.get("title") or lesson.get("topic") or lesson.get("folder") or "Lesson").strip()
    lesson_date = str(lesson.get("date") or lesson.get("lesson_date") or "").strip()
    lesson_text = str(lesson.get("text") or lesson.get("excerpt") or "").strip()
    source_note = str(lesson.get("index_note") or lesson.get("source_note") or "").strip()
    terms = _lesson_terms(lesson_text, title)
    snippets = _lesson_snippets(lesson_text)
    student_line = ", ".join(student["name"] for student in watchlist) or "No specific student watchlist yet."
    concept_line = ", ".join(terms[:5]) if terms else title
    evidence_line = snippets[0] if snippets else "Use the selected lesson material as the reference text."
    sections = [
        {
            "title": "Core recall",
            "prompts": [
                f"Write three key ideas from the lesson. Anchor your answer on: {concept_line}.",
                "Circle the one idea you are least confident about.",
            ],
        },
        {
            "title": "Misconception check",
            "prompts": [
                f"Use this lesson evidence: {evidence_line}",
                "Describe one mistake someone might make with today's skill.",
                "Correct the mistake using a short example.",
            ],
        },
        {
            "title": "Feed-forward task",
            "prompts": [
                "Complete one similar question without looking at notes.",
                "Write one next step that would improve your answer.",
            ],
        },
        {
            "title": "Teacher follow-up",
            "prompts": [
                f"Watchlist: {student_line}",
                "Note one reteach point, one extension point, and one student to check in with next lesson.",
            ],
        },
    ]
    if len(snippets) > 1:
        sections.insert(2, {
            "title": "Evidence distillation",
            "prompts": [
                f"Explain this part in simpler words: {snippets[1]}",
                "Turn it into one memory hook or exam-ready sentence.",
            ],
        })
    return {
        "class_name": class_name,
        "lesson_title": title,
        "lesson_date": lesson_date,
        "source_path": str(lesson.get("path") or ""),
        "source_note": source_note,
        "keywords": terms,
        "extracted": bool(lesson_text),
        "summary": f"{class_name} post-lesson reflection for {title}{f' ({lesson_date})' if lesson_date else ''}.",
        "sections": sections,
    }


def build_status_summary(
    ledger: dict,
    get_students: Callable[[str], list[dict]],
    now: datetime | None = None,
    logger=None,
) -> dict:
    classes = ledger.get("classes") if isinstance(ledger, dict) else {}
    if not isinstance(classes, dict):
        classes = {}
    current = now or datetime.now()
    today = current.date()
    class_rows = []
    totals = {
        "class_count": 0,
        "assignment_count": 0,
        "pending_count": 0,
        "open_submission_count": 0,
        "concern_count": 0,
        "insight_count": 0,
        "due_today_count": 0,
        "overdue_count": 0,
    }

    for class_name in sorted(classes.keys()):
        record = classes.get(class_name) if isinstance(classes.get(class_name), dict) else {}
        assignments = [item for item in record.get("assignments", []) if isinstance(item, dict)]
        try:
            students = get_students(class_name)
            report = build_student_report(class_name, students, ledger, today=today)
            roster_count = report.get("roster_count", 0)
            concern_count = report.get("concern_count", 0)
            latest = report.get("assignments", [])[-1] if report.get("assignments") else {}
            insights = report.get("insights", []) or []
            concerns = sorted(
                report.get("concerns", []) or [],
                key=lambda item: (int(item.get("risk_score", 0) or 0), int(item.get("missing_count", 0) or 0)),
                reverse=True,
            )
        except Exception as exc:
            if logger:
                logger.warning(f"ClassOps status scan failed for {class_name}: {exc}")
            roster_count = 0
            concern_count = 0
            latest = assignments[-1] if assignments else {}
            insights = []
            concerns = []

        latest_missing = int(latest.get("missing_count") or len(latest.get("non_submitted", []) or []) or 0)
        due_today = 0
        overdue = 0
        open_submissions = 0
        next_due = ""
        for assignment in assignments:
            due = str(assignment.get("collect_by") or "").strip()
            open_count = len(assignment.get("non_submitted", []) or [])
            if open_count:
                open_submissions += open_count
            if not due or not open_count:
                continue
            due_date = parse_classops_date(due)
            if not due_date:
                continue
            if not next_due or due < next_due:
                next_due = due
            if due_date == today:
                due_today += 1
            elif due_date < today:
                overdue += 1

        row = {
            "class_name": class_name,
            "roster_count": roster_count,
            "assignment_count": len(assignments),
            "pending_count": latest_missing,
            "open_submission_count": open_submissions,
            "concern_count": concern_count,
            "due_today_count": due_today,
            "overdue_count": overdue,
            "latest_assignment": latest,
            "insight_count": len(insights),
            "top_insight": insights[0] if insights else {},
            "top_students": [
                {
                    "name": student.get("name", ""),
                    "status": student.get("status", ""),
                    "missing_count": int(student.get("missing_count", 0) or 0),
                    "risk_score": int(student.get("risk_score", 0) or 0),
                    "reason": (student.get("risk_reasons") or [""])[0],
                }
                for student in concerns[:3]
            ],
            "next_due": next_due,
        }
        class_rows.append(row)
        totals["assignment_count"] += len(assignments)
        totals["pending_count"] += latest_missing
        totals["open_submission_count"] += open_submissions
        totals["concern_count"] += concern_count
        totals["insight_count"] += len(insights)
        totals["due_today_count"] += due_today
        totals["overdue_count"] += overdue

    totals["class_count"] = len(class_rows)
    return {
        "connected": True,
        "generated_at": current.isoformat(),
        **totals,
        "classes": class_rows,
        "control_centre_url": "/classops",
    }


def top_home_signal(summary: dict) -> dict:
    classes = summary.get("classes") if isinstance(summary.get("classes"), list) else []
    scored = []
    for item in classes:
        insight = item.get("top_insight") if isinstance(item.get("top_insight"), dict) else {}
        overdue = int(item.get("overdue_count", 0) or 0)
        due_today = int(item.get("due_today_count", 0) or 0)
        concerns = int(item.get("concern_count", 0) or 0)
        pending = int(item.get("pending_count", 0) or 0)
        severity = str(insight.get("severity") or "").lower()
        score = overdue * 35 + due_today * 28 + concerns * 6 + pending * 4
        if severity == "critical":
            score += 30
        elif severity == "watch":
            score += 12
        if score <= 0:
            continue
        scored.append((score, item, insight))
    if not scored:
        return {}
    score, item, insight = sorted(scored, key=lambda part: part[0], reverse=True)[0]
    class_name = item.get("class_name", "ClassOps")
    students = [student.get("name", "") for student in item.get("top_students", []) if student.get("name")]
    student_text = f" Start with {', '.join(students[:3])}." if students else ""
    if item.get("overdue_count"):
        title = f"{class_name} has overdue ClassOps follow-up"
        detail = f"{item.get('overdue_count')} overdue tracked assignment signal(s).{student_text}"
        severity = "red"
    elif item.get("due_today_count"):
        title = f"{class_name} has ClassOps due today"
        detail = f"{item.get('due_today_count')} tracked assignment signal(s) due today.{student_text}"
        severity = "orange"
    elif insight:
        title = str(insight.get("title") or f"{class_name} needs ClassOps attention")
        detail = str(insight.get("detail") or "").strip() or student_text.strip() or "Open the ClassOps control centre for the highest-risk students."
        severity = "red" if insight.get("severity") == "critical" else "yellow"
    else:
        title = f"{class_name} needs student follow-up"
        detail = f"{item.get('concern_count')} student(s) currently flagged.{student_text}"
        severity = "yellow"
    return {
        "score": score,
        "class_name": class_name,
        "title": title,
        "detail": detail,
        "severity": severity,
        "students": students,
        "control_centre_url": summary.get("control_centre_url", "/classops"),
    }


def brief_lines(summary: dict, limit: int = 3) -> list[str]:
    if not isinstance(summary, dict) or not summary.get("connected"):
        return []
    signal = top_home_signal(summary)
    if not signal:
        return []
    classes = summary.get("classes") if isinstance(summary.get("classes"), list) else []
    rows = sorted(
        [
            item for item in classes
            if int(item.get("overdue_count", 0) or 0)
            or int(item.get("due_today_count", 0) or 0)
            or int(item.get("concern_count", 0) or 0)
            or item.get("top_insight")
        ],
        key=lambda item: (
            int(item.get("overdue_count", 0) or 0),
            int(item.get("due_today_count", 0) or 0),
            int(item.get("concern_count", 0) or 0),
            int(item.get("pending_count", 0) or 0),
        ),
        reverse=True,
    )
    lines = ["*ClassOps:*"]
    for item in rows[:limit]:
        class_name = item.get("class_name", "Class")
        parts = []
        if item.get("overdue_count"):
            parts.append(f"{item.get('overdue_count')} overdue")
        if item.get("due_today_count"):
            parts.append(f"{item.get('due_today_count')} due today")
        if item.get("concern_count"):
            parts.append(f"{item.get('concern_count')} student follow-up")
        insight = item.get("top_insight") if isinstance(item.get("top_insight"), dict) else {}
        detail = insight.get("title") or ", ".join(parts) or "ClassOps attention needed"
        students = [student.get("name", "") for student in item.get("top_students", []) if student.get("name")]
        suffix = f" Start with {', '.join(students[:2])}." if students else ""
        lines.append(f"- {class_name}: {detail}.{suffix}")
    return lines


def proactive_insights(summary: dict, now: datetime | None = None) -> list[dict]:
    current = now or datetime.now()
    today = current.date().isoformat()
    if not isinstance(summary, dict) or not summary.get("connected"):
        return []
    signal = top_home_signal(summary)
    if not signal:
        return []
    severity = signal.get("severity", "yellow")
    priority = "high" if severity in {"red", "orange"} else "medium"
    body = signal.get("detail") or "Open ClassOps and clear the highest-risk student follow-up."
    return [{
        "id": f"{today}:classops:{signal.get('class_name', 'all')}",
        "family": "classops",
        "kind": "update",
        "priority": priority,
        "title": signal.get("title") or "ClassOps needs attention",
        "body": body,
        "why": "ClassOps detected due work, overdue submissions, or student follow-up patterns.",
        "action_hint": "Open ClassOps and resolve the highest-risk class first.",
        "metadata": {
            "class_name": signal.get("class_name", ""),
            "students": signal.get("students", []),
            "control_centre_url": signal.get("control_centre_url", "/classops"),
        },
    }]
