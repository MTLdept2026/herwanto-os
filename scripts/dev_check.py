#!/usr/bin/env python3
"""Run HIRA's local reliability checks without external test dependencies."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
PY_CACHE = "/private/tmp/hira-pycache"
COMPILE_TARGETS = [
    "bot.py",
    "web_app.py",
    "google_services.py",
    "postgres_storage.py",
    "classops_intelligence.py",
    "dropbox_service.py",
]
CRITICAL_UNIT_TESTS = [
    "tests.test_agentic_openai.AgenticOpenAITests.test_google_ok_accepts_user_oauth_sheets_credentials",
    "tests.test_agentic_openai.AgenticOpenAITests.test_classlist_permission_message_points_to_work_sheets_oauth_when_missing",
    "tests.test_agentic_openai.AgenticOpenAITests.test_notification_outcomes_are_capped_below_sheet_cell_limit",
    "tests.test_agentic_openai.AgenticOpenAITests.test_retry_after_percentage_failure_forces_percentage_tool_from_context",
    "tests.test_agentic_openai.AgenticOpenAITests.test_pwa_retry_after_classlist_failure_is_not_quick_chat",
    "tests.test_agentic_openai.AgenticOpenAITests.test_pwa_retry_after_classlist_failure_gets_classlist_tools",
    "tests.test_agentic_openai.AgenticOpenAITests.test_medical_leave_context_becomes_teaching_memory",
    "tests.test_agentic_openai.AgenticOpenAITests.test_medical_leave_archives_active_school_calendar_notifications",
    "tests.test_agentic_openai.AgenticOpenAITests.test_not_on_duty_blocks_cca_calendar_reminder",
    "tests.test_agentic_openai.AgenticOpenAITests.test_queue_blocks_cca_notification_after_not_on_duty_memory",
    "tests.test_agentic_openai.AgenticOpenAITests.test_web_push_recovery_archives_blocked_cca_calendar_notification",
    "tests.test_agentic_openai.AgenticOpenAITests.test_calendar_reminder_blocks_cca_when_not_on_roster",
    "tests.test_agentic_openai.AgenticOpenAITests.test_calendar_reminder_blocks_school_events_on_medical_leave",
    "tests.test_agentic_openai.AgenticOpenAITests.test_dispatch_skips_stale_calendar_reminder",
    "tests.test_agentic_openai.AgenticOpenAITests.test_dispatch_marks_action_reminder_after_confirmed_push",
    "tests.test_agentic_openai.AgenticOpenAITests.test_fill_mtl_percentage_scores_updates_blank_fa2_percentages",
    "tests.test_agentic_openai.AgenticOpenAITests.test_fill_mtl_percentage_scores_reuses_blank_column_after_raw_score",
    "tests.test_code_review_fixes",
]


def run(cmd: list[str], env: dict[str, str] | None = None) -> int:
    print(f"+ {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=ROOT, env={**os.environ, **(env or {})})


def smoke_request(base_url: str, path: str, token: str = "") -> tuple[bool, str]:
    url = f"{base_url.rstrip('/')}{path}"
    headers = {"X-Hira-Token": token} if token else {}
    try:
        with urlopen(Request(url, headers=headers), timeout=8) as response:
            body = response.read(4096).decode("utf-8", "replace")
            if "application/json" in response.headers.get("content-type", ""):
                body = json.dumps(json.loads(body), sort_keys=True)[:280]
            return True, f"{response.status} {path} {body[:280]}"
    except HTTPError as exc:
        return False, f"{exc.code} {path} {exc.reason}"
    except URLError as exc:
        return False, f"{path} unavailable: {exc.reason}"


def run_smoke(base_url: str, token: str = "") -> int:
    checks = ["/healthz"]
    if token:
        checks.extend(["/api/app/version", "/api/action-ledger?limit=1"])
    failed = 0
    for path in checks:
        ok, detail = smoke_request(base_url, path, token=token)
        print(("ok " if ok else "!! ") + detail)
        failed += 0 if ok else 1
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--critical", action="store_true", help="Run HIRA's trust-critical regression checks")
    parser.add_argument("--unit", action="store_true", help="Run Python unit tests")
    parser.add_argument("--compile", action="store_true", help="Compile core Python modules")
    parser.add_argument("--smoke-url", default="", help="Optional running app URL, e.g. http://127.0.0.1:4173")
    parser.add_argument("--token", default=os.environ.get("HIRA_WEB_TOKEN", ""), help="Optional PWA token for protected smoke endpoints")
    args = parser.parse_args()

    selected = args.critical or args.unit or args.compile or bool(args.smoke_url)
    run_critical = args.critical or not selected
    run_unit = args.unit or not selected
    run_compile = args.compile or not selected
    status = 0
    if run_compile:
        status |= run([sys.executable, "-m", "py_compile", *COMPILE_TARGETS], env={"PYTHONPYCACHEPREFIX": PY_CACHE})
    if run_critical:
        status |= run([sys.executable, "-m", "unittest", *CRITICAL_UNIT_TESTS, "-q"], env={"PYTHONPYCACHEPREFIX": PY_CACHE})
    if run_unit:
        status |= run([sys.executable, "-m", "unittest", "tests.test_agentic_openai", "-q"], env={"PYTHONPYCACHEPREFIX": PY_CACHE})
    if args.smoke_url:
        status |= run_smoke(args.smoke_url, token=args.token)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
