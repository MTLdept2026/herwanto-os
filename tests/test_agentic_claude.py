import asyncio
import json
import os
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import ANY, patch

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")

import bot
import classops_intelligence as classops_ai
import dropbox_service
import islamic_service
import pdf_service
import search_service
import weather_service
import web_app


REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeMessages:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(
                stop_reason="tool_use",
                content=[
                    SimpleNamespace(
                        type="tool_use",
                        id="tool-1",
                        name="get_gmail_brief",
                        input={"query": "", "max_items": 5},
                    )
                ],
            )
        return SimpleNamespace(
            stop_reason="end_turn",
            content=[
                SimpleNamespace(
                    type="text",
                    text="Your last five emails are mostly about school admin and project updates.",
                )
            ],
        )


def sheet_row(*values):
    return {"values": [{"formattedValue": str(value)} for value in values]}


class FakeSheetsRequest:
    def __init__(self, payload=None, callback=None):
        self.payload = payload or {}
        self.callback = callback

    def execute(self):
        if self.callback:
            self.callback()
        return self.payload


class FakeSheetsValues:
    def __init__(self, ranges=None):
        self.batch_updates = []
        self.updates = []
        self.appends = []
        self.ranges = ranges or {}

    def get(self, spreadsheetId, range):
        return FakeSheetsRequest({"values": self.ranges.get(range, [])})

    def batchUpdate(self, spreadsheetId, body):
        self.batch_updates.append((spreadsheetId, body))
        return FakeSheetsRequest()

    def update(self, spreadsheetId, range, valueInputOption, body):
        self.updates.append((spreadsheetId, range, valueInputOption, body))
        return FakeSheetsRequest()

    def append(self, spreadsheetId, range, valueInputOption, body):
        self.appends.append((spreadsheetId, range, valueInputOption, body))
        return FakeSheetsRequest()


class FakeSheetsSpreadsheets:
    def __init__(self, book, ranges=None):
        self.book = book
        self.batch_updates = []
        self.values_api = FakeSheetsValues(ranges=ranges)

    def get(self, spreadsheetId, includeGridData=False, fields=""):
        return FakeSheetsRequest(self.book)

    def batchUpdate(self, spreadsheetId, body):
        self.batch_updates.append((spreadsheetId, body))
        return FakeSheetsRequest()

    def values(self):
        return self.values_api


class FakeSheetsService:
    def __init__(self, book, ranges=None):
        self.spreadsheets_api = FakeSheetsSpreadsheets(book, ranges=ranges)

    def spreadsheets(self):
        return self.spreadsheets_api


class AgenticClaudeTests(unittest.TestCase):
    def setUp(self):
        bot.gs._invalidate_classlist_cache()
        os.environ.setdefault("HIRA_DIGEST_SOCIAL_SEARCH", "0")

    def test_google_ok_accepts_user_oauth_sheets_credentials(self):
        env = {
            "GOOGLE_SHEET_ID": "sheet-id",
            "GOOGLE_SERVICE_ACCOUNT_JSON": "",
            "GOOGLE_SHEETS_CLIENT_ID": "client-id",
            "GOOGLE_SHEETS_CLIENT_SECRET": "client-secret",
            "GOOGLE_SHEETS_REFRESH_TOKEN": "refresh-token",
        }

        with patch.dict(os.environ, env, clear=False):
            self.assertTrue(bot.google_ok())
            self.assertEqual(bot.gs._sheets_auth_mode(), "user_oauth")

    def test_google_ok_uses_personal_oauth_for_app_sheets_and_work_oauth_for_classlists(self):
        env = {
            "GOOGLE_SHEET_ID": "sheet-id",
            "GOOGLE_SERVICE_ACCOUNT_JSON": "",
            "GOOGLE_WORK_SHEETS_CLIENT_ID": "work-client-id",
            "GOOGLE_WORK_SHEETS_CLIENT_SECRET": "work-client-secret",
            "GOOGLE_WORK_SHEETS_REFRESH_TOKEN": "work-refresh-token",
            "GOOGLE_SHEETS_CLIENT_ID": "personal-client-id",
            "GOOGLE_SHEETS_CLIENT_SECRET": "personal-client-secret",
            "GOOGLE_SHEETS_REFRESH_TOKEN": "personal-refresh-token",
        }

        with patch.dict(os.environ, env, clear=False):
            self.assertTrue(bot.google_ok())
            self.assertEqual(bot.gs._sheets_auth_mode(), "user_oauth")
            self.assertEqual(bot.gs._work_sheets_auth_mode(), "work_user_oauth")
            self.assertEqual(bot.gs.sheets_access_identity()["mode"], "user_oauth")
            self.assertEqual(bot.gs.sheets_access_identity("work")["mode"], "work_user_oauth")

    def test_classlist_permission_message_points_to_work_sheets_oauth_when_missing(self):
        env = {
            "GOOGLE_WORK_SHEETS_REFRESH_TOKEN": "",
            "GOOGLE_SHEETS_REFRESH_TOKEN": "",
            "GOOGLE_SERVICE_ACCOUNT_JSON": "",
        }

        with patch.dict(os.environ, env, clear=False):
            message = bot.gs._classlist_permission_message(None, ["sheet-id"], ["2G3 WA2"])

        self.assertIn("work Google Sheets OAuth is not configured", message)
        self.assertIn("GOOGLE_WORK_SHEETS_REFRESH_TOKEN", message)
        self.assertIn("Fallback option", message)

    def test_google_ok_still_accepts_service_account_sheets_credentials(self):
        with patch.dict(os.environ, {"GOOGLE_SHEET_ID": "sheet-id", "GOOGLE_SERVICE_ACCOUNT_JSON": "encoded"}, clear=False):
            with patch.object(bot.gs, "_user_google_oauth_configured", return_value=False):
                with patch.object(bot.gs, "_work_google_oauth_configured", return_value=False):
                    self.assertTrue(bot.google_ok())
                    self.assertEqual(bot.gs._sheets_auth_mode(), "service_account")

    def test_notification_outcomes_are_capped_below_sheet_cell_limit(self):
        entries = [
            {
                "created": "2026-05-12T17:00:00+08:00",
                "notification_id": str(idx),
                "source": f"calendar_reminder:2026-05-12:event-{idx}" + ("x" * 220),
                "group": "calendar_reminder",
                "kind": "reminder",
                "action": "not_now",
                "rating": "",
                "client_id": "phone" + ("y" * 100),
                "title": "C Div Training " + ("z" * 240),
            }
            for idx in range(500)
        ]
        captured = {}

        with patch.object(bot.gs, "set_config", side_effect=lambda key, value: captured.update({key: value})):
            bot.gs.set_notification_outcomes(entries)

        self.assertEqual(captured.keys(), {"notification_outcomes"})
        self.assertLess(len(captured["notification_outcomes"]), 50000)

    def test_tuesday_even_timetable_uses_hardcoded_source(self):
        result = bot._timetable_for_lookup("Tuesday", "Even")

        self.assertIn("Tue Even week timetable", result)
        self.assertIn("7:35–8:00", result)
        self.assertIn("FTCT", result)
        self.assertIn("8:00–9:05", result)
        self.assertIn("CCE", result)
        self.assertIn("9:40–10:50", result)
        self.assertIn("1 Flagship", result)
        self.assertIn("13:40–14:45", result)
        self.assertIn("3G3", result)
        self.assertIn("L3-10", result)

    def test_master_timetable_key_periods(self):
        monday_even = bot._timetable_for_lookup("Monday", "Even")
        tuesday_odd = bot._timetable_for_lookup("Tuesday", "Odd")
        tuesday_even = bot._timetable_for_lookup("Tuesday", "Even")
        wednesday_odd = bot._timetable_for_lookup("Wednesday", "Odd")
        wednesday_even = bot._timetable_for_lookup("Wednesday", "Even")
        thursday_odd = bot._timetable_for_lookup("Thursday", "Odd")
        thursday_even = bot._timetable_for_lookup("Thursday", "Even")
        friday_odd = bot._timetable_for_lookup("Friday", "Odd")

        self.assertIn("8:00–9:05", monday_even)
        self.assertIn("10:50–11:55", monday_even)
        self.assertIn("8:00–9:05", tuesday_odd)
        self.assertIn("7:35–8:00", tuesday_even)
        self.assertIn("8:00–9:05", tuesday_even)
        self.assertIn("8:00–8:35", wednesday_odd)
        self.assertIn("9:05–10:50", wednesday_odd)
        self.assertIn("11:25–12:30", wednesday_odd)
        self.assertIn("8:00–8:35", wednesday_even)
        self.assertIn("9:05–10:15", wednesday_even)
        self.assertIn("11:25–12:30", wednesday_even)
        self.assertIn("12:30–13:40", wednesday_even)
        self.assertIn("9:40–10:50", thursday_odd)
        self.assertIn("11:25–12:30", thursday_even)
        self.assertIn("11:25–12:30", friday_odd)

    def test_non_hbl_friday_is_explicitly_guarded(self):
        target = date(2026, 5, 8)
        info = bot.tt.get_school_week_info(target)

        self.assertEqual(target.weekday(), 4)
        self.assertFalse(info["is_school_holiday"])
        self.assertFalse(info["is_hbl"])
        self.assertEqual(info["week_type"], "O")
        self.assertIn("HBL status: Not HBL", bot._hbl_status_line(target))

    def test_timetable_question_forces_timetable_tool(self):
        forced = bot._forced_tool_for_text(
            "What's the correct Tuesday Even week timetable?",
            [{"name": "get_timetable"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "get_timetable")

    def test_action_requests_are_not_forced_to_read_only_tools(self):
        calendar_forced = bot._forced_tool_for_text(
            "schedule meeting with HOD tomorrow at 3pm",
            [{"name": "get_assistant_context"}, {"name": "create_calendar_event"}],
        )
        task_forced = bot._forced_tool_for_text(
            "add task to submit CCA attendance by Friday",
            [{"name": "get_task_brief"}, {"name": "add_reminder"}],
        )

        self.assertIsNone(calendar_forced)
        self.assertIsNone(task_forced)

    def test_availability_planning_forces_checked_slot_tool(self):
        forced = bot._forced_tool_for_text(
            "find the best slots to schedule Sahibba training after school not during my CCA day",
            [{"name": "find_available_training_slots"}, {"name": "get_assistant_context"}, {"name": "create_calendar_event"}],
        )

        self.assertEqual(forced, "find_available_training_slots")

    def test_hdb_date_question_forces_calendar_context(self):
        forced = bot._forced_tool_for_text(
            "what date is my HDB appointment?",
            [{"name": "get_assistant_context"}, {"name": "get_timetable"}],
        )

        self.assertEqual(forced, "get_assistant_context")

    def test_score_question_forces_classlist_tool(self):
        forced = bot._forced_tool_for_text(
            "show me the FA2 scores for S4-AN",
            [{"name": "get_mtl_classlists"}, {"name": "get_timetable"}],
        )

        self.assertEqual(forced, "get_mtl_classlists")

    def test_percentage_fill_request_forces_percentage_tool(self):
        forced = bot._forced_tool_for_text(
            "input the converted scores under the percentage sign for FA2",
            [{"name": "fill_mtl_percentage_scores"}, {"name": "get_mtl_classlists"}],
        )

        self.assertEqual(forced, "fill_mtl_percentage_scores")

    def test_failure_highlighting_request_forces_highlighting_tool(self):
        forced = bot._forced_tool_for_text(
            "Colour in the failures with red so I can see it clearly in the percentage column",
            [{"name": "apply_mtl_failure_highlighting"}, {"name": "get_mtl_classlists"}],
        )

        self.assertEqual(forced, "apply_mtl_failure_highlighting")

    def test_sheet_analysis_graph_request_forces_trend_report_tool(self):
        forced = bot._forced_tool_for_text(
            "write the analysis and graphs on the spreadsheet as a new analysis tab",
            [{"name": "generate_mtl_score_trend_report"}, {"name": "analyze_mtl_scores"}],
        )

        self.assertEqual(forced, "generate_mtl_score_trend_report")

    def test_both_followup_after_highlight_and_graph_request_forces_highlighting_first(self):
        messages = [
            {"role": "user", "content": "Colour failures red and write analysis graphs on the spreadsheet"},
            {"role": "assistant", "content": "Do you want failure highlighting or a trend analysis tab?"},
            {"role": "user", "content": "Both"},
        ]
        forced = bot._forced_tool_for_current_turn(
            messages,
            [{"name": "apply_mtl_failure_highlighting"}, {"name": "generate_mtl_score_trend_report"}],
        )

        self.assertEqual(forced, "apply_mtl_failure_highlighting")

    def test_retry_after_percentage_failure_forces_percentage_tool_from_context(self):
        messages = [
            {"role": "user", "content": "Fill in the percentage column of 2G3 WA2 pls"},
            {"role": "assistant", "content": "Failed to fill MTL percentage scores: Google Sheets denied write access."},
            {"role": "user", "content": "Try again"},
        ]
        forced = bot._forced_tool_for_current_turn(
            messages,
            [{"name": "fill_mtl_percentage_scores"}, {"name": "get_mtl_classlists"}],
        )

        self.assertEqual(forced, "fill_mtl_percentage_scores")

    def test_pwa_retry_after_classlist_failure_is_not_quick_chat(self):
        messages = [
            {"role": "user", "content": "Fill in the percentage column of 2G3 WA2 pls"},
            {"role": "assistant", "content": "Failed to fill MTL percentage scores: permission denied."},
        ]

        routed_quick = asyncio.run(bot.should_route_quick_pwa_chat(messages, "Try again"))

        self.assertFalse(routed_quick)

    def test_pwa_retry_after_classlist_failure_gets_classlist_tools(self):
        tools = bot.pwa_tools_for_message(
            "Try again",
            recent_context="Fill in the percentage column of 2G3 WA2 pls\nFailed to fill MTL percentage scores.",
        )
        names = {tool["name"] for tool in tools}

        self.assertIn("fill_mtl_percentage_scores", names)

    def test_score_analysis_request_forces_analysis_tool(self):
        forced = bot._forced_tool_for_text(
            "analyse S4-AN scores and show mean median underperforming most improved drastic drops",
            [{"name": "analyze_mtl_scores"}, {"name": "get_mtl_classlists"}],
        )

        self.assertEqual(forced, "analyze_mtl_scores")

    def test_url_question_forces_fetch_url_tool(self):
        forced = bot._forced_tool_for_text(
            "read this for me https://www.formula1.com/en/teams",
            [{"name": "fetch_url"}, {"name": "web_search"}],
        )

        self.assertEqual(forced, "fetch_url")

    def test_research_question_forces_web_research_tool(self):
        forced = bot._forced_tool_for_text(
            "research the latest MOE AI policy sources for lesson planning",
            [{"name": "web_research"}, {"name": "web_search"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "web_research")

    def test_f1_current_question_forces_structured_sports_tool(self):
        forced = bot._forced_tool_for_text(
            "current F1 driver standings after the latest grand prix",
            [{"name": "get_f1_brief"}, {"name": "web_search"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_f1_brief")

    def test_liverpool_current_question_forces_structured_sports_tool(self):
        forced = bot._forced_tool_for_text(
            "where are Liverpool in the current EPL table and what competitions are they still in?",
            [{"name": "get_liverpool_brief"}, {"name": "web_search"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_liverpool_brief")

    def test_extract_owned_item_from_purchase_signal(self):
        self.assertEqual(
            bot.extract_owned_item("I've just bought a new Garmin Forerunner 265 for runs."),
            "Garmin Forerunner 265",
        )
        self.assertEqual(
            bot.extract_owned_item("I am now a new owner of the Nothing Phone 3."),
            "Nothing Phone 3",
        )

    def test_extract_owned_item_ignores_interest_phrase(self):
        self.assertEqual(bot.extract_owned_item("I got into F1 this season."), "")
        self.assertEqual(bot.extract_owned_item("I just bought a certain item."), "")

    def test_absorb_ownership_signal_stores_topic_profile(self):
        calls = []
        fake_gs = SimpleNamespace(add_topic_profile=lambda profile: calls.append(profile))

        with patch.object(bot, "google_ok", return_value=True), patch.object(bot, "gs", fake_gs):
            self.assertTrue(bot.absorb_ownership_signal("I've just bought a Garmin Forerunner 265."))

        self.assertEqual(len(calls), 1)
        profile = calls[0]
        self.assertEqual(profile["topic"], "Garmin Forerunner 265")
        self.assertEqual(profile["category"], "ownership")
        self.assertEqual(profile["kind"], "ownership")
        self.assertIn("firmware", " ".join(profile["track"]).lower())

    def test_news_item_key_prefers_url_for_stable_deduping(self):
        item_a = {"title": "Same story title", "url": "https://example.com/story"}
        item_b = {"title": "Same story title updated", "url": "https://example.com/story"}

        self.assertEqual(search_service.news_item_key(item_a), search_service.news_item_key(item_b))

    def test_pick_fresh_morning_digest_entries_skips_recent_keys(self):
        def fake_google_news(query, max_items=5):
            return [
                {"title": f"{query} old", "url": f"https://example.com/{query}/old"},
                {"title": f"{query} fresh", "url": f"https://example.com/{query}/fresh"},
            ]

        topics = [("Topic A", "alpha"), ("Topic B", "beta")]
        seen_keys = {search_service.news_item_key({"title": "alpha old", "url": "https://example.com/alpha/old"})}
        with patch("search_service.google_news", side_effect=fake_google_news):
            entries = search_service.pick_fresh_morning_digest_entries(topics=topics, seen_keys=seen_keys, fetch_limit=2)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["label"], "Topic A")
        self.assertEqual(entries[0]["item"]["title"], "alpha fresh")
        self.assertEqual(entries[1]["item"]["title"], "beta old")

    def test_notification_feedback_bias_prefers_useful_over_negative(self):
        now = bot.datetime.now(bot.SGT)
        outcomes = [
            {"created": now.isoformat(), "source": "checkin:7", "group": "checkin", "kind": "reminder", "action": "useful"},
            {"created": now.isoformat(), "source": "checkin:7", "group": "checkin", "kind": "reminder", "action": "dismissed"},
            {"created": now.isoformat(), "source": "checkin:8", "group": "checkin", "kind": "reminder", "action": "dismissed"},
        ]
        with patch("bot.gs.get_notification_outcomes", return_value=outcomes):
            score_exact = bot._notification_feedback_bias("checkin:7", "reminder", now=now)
            score_other = bot._notification_feedback_bias("checkin:8", "reminder", now=now)

        self.assertGreater(score_exact, score_other)

    def test_notification_suppression_honours_recent_negative_feedback(self):
        now = bot.datetime.now(bot.SGT)
        outcomes = [
            {"created": now.isoformat(), "source": "followup:3", "group": "followup", "kind": "reminder", "action": "not_now"}
        ]
        with patch("bot.gs.get_notification_outcomes", return_value=outcomes):
            self.assertTrue(bot._should_suppress_notification("followup:3", "reminder", now=now))
            self.assertTrue(bot._should_suppress_notification("followup:9", "reminder", now=now))
            self.assertFalse(bot._should_suppress_notification("briefing:today", "briefing", now=now))

    def test_dismissed_task_reminder_is_not_regenerated_on_refresh(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 22, 30))
        source = "task_reminder:2026-05-05:31"
        outcomes = [
            {"created": now.isoformat(), "source": source, "group": "task_reminder", "kind": "reminder", "action": "dismissed"}
        ]

        with patch("bot.gs.get_notification_outcomes", return_value=outcomes):
            self.assertTrue(bot._should_suppress_notification(source, "reminder", now=now))

    def test_dismissed_task_reminder_suppresses_same_task_on_later_date(self):
        dismissed_at = bot.SGT.localize(bot.datetime(2026, 5, 5, 22, 30))
        now = bot.SGT.localize(bot.datetime(2026, 5, 6, 9, 0))
        outcomes = [
            {
                "created": dismissed_at.isoformat(),
                "source": "task_reminder:2026-05-05:31",
                "group": "task_reminder",
                "kind": "reminder",
                "action": "dismissed",
            }
        ]

        with patch("bot.gs.get_notification_outcomes", return_value=outcomes):
            self.assertTrue(bot._should_suppress_notification("task_reminder:2026-05-06:31", "reminder", now=now))
            self.assertFalse(bot._should_suppress_notification("task_reminder:2026-05-06:32", "reminder", now=now))

    def test_quiet_hours_supports_daytime_windows(self):
        during = bot.SGT.localize(bot.datetime(2026, 5, 6, 14, 0))
        before = bot.SGT.localize(bot.datetime(2026, 5, 6, 10, 0))
        with patch.dict(os.environ, {"HIRA_QUIET_START_HOUR": "13", "HIRA_QUIET_END_HOUR": "15"}):
            self.assertTrue(bot._quiet_hours_active(now=during))
            self.assertFalse(bot._quiet_hours_active(now=before))

    def test_add_reminder_uses_max_numeric_id_not_row_count(self):
        fake_sheets = FakeSheetsService({})
        with (
            patch.object(bot.gs, "_raw_reminders", return_value=[["1"], ["3"]]),
            patch.object(bot.gs, "_sheets", return_value=fake_sheets),
        ):
            reminder_id = bot.gs.add_reminder("Submit remarks", "2026-05-07", "Teaching")

        self.assertEqual(reminder_id, 4)
        appended = fake_sheets.spreadsheets_api.values_api.appends[0][3]["values"][0]
        self.assertEqual(appended[0], "4")

    def test_web_push_payload_uses_phone_sized_preview(self):
        payloads = []
        fake_pywebpush = ModuleType("pywebpush")
        fake_pywebpush.WebPushException = Exception
        fake_pywebpush.webpush = lambda **kwargs: payloads.append(kwargs["data"])

        with (
            patch.dict(os.environ, {"HIRA_WEB_PUSH_PRIVATE_KEY": "test-key"}),
            patch.dict("sys.modules", {"pywebpush": fake_pywebpush}),
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[{
                "client_id": "phone",
                "subscription": {"endpoint": "https://push.example/sub"},
            }]),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(bot.gs, "set_web_push_delivery_log"),
        ):
            sent = bot.gs.send_web_push_notification(
                "Evening roundup",
                "Line one\n" + ("A long briefing line. " * 200),
                data={"id": "1", "kind": "briefing", "source": "evening_briefing:2026-05-04"},
            )

        self.assertEqual(sent, 1)
        self.assertLess(len(payloads[0].encode("utf-8")), 1200)
        self.assertIn("Open H.I.R.A", json.loads(payloads[0])["body"])

    def test_web_push_prefers_standalone_subscription_over_browser(self):
        endpoints = []
        fake_pywebpush = ModuleType("pywebpush")
        fake_pywebpush.WebPushException = Exception
        fake_pywebpush.webpush = lambda **kwargs: endpoints.append(kwargs["subscription_info"]["endpoint"])

        with (
            patch.dict(os.environ, {"HIRA_WEB_PUSH_PRIVATE_KEY": "test-key"}),
            patch.dict("sys.modules", {"pywebpush": fake_pywebpush}),
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[
                {
                    "client_id": "browser",
                    "display_mode": "browser",
                    "subscription": {"endpoint": "https://push.example/browser"},
                },
                {
                    "client_id": "pwa",
                    "display_mode": "standalone",
                    "subscription": {"endpoint": "https://push.example/pwa"},
                },
            ]),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(bot.gs, "set_web_push_delivery_log"),
        ):
            sent = bot.gs.send_web_push_notification(
                "Morning briefing",
                "Briefing body",
                data={"id": "1", "kind": "briefing", "source": "morning_briefing:2026-05-06"},
            )

        self.assertEqual(sent, 1)
        self.assertEqual(endpoints, ["https://push.example/pwa"])

    def test_save_web_push_subscription_records_display_mode(self):
        with (
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[]),
            patch.object(bot.gs, "set_web_push_subscriptions") as set_subs,
        ):
            ok = bot.gs.save_web_push_subscription(
                "phone",
                {"endpoint": "https://push.example/pwa"},
                metadata={"display_mode": "standalone", "app_version": "20260506-5", "user_agent": "Android Chrome"},
            )

        self.assertTrue(ok)
        saved = set_subs.call_args.args[0][0]
        self.assertEqual(saved["display_mode"], "standalone")
        self.assertEqual(saved["app_version"], "20260506-5")

    def test_web_push_delivery_log_records_failure_reason(self):
        class FakeWebPushException(Exception):
            def __init__(self):
                super().__init__("push failed")
                self.response = SimpleNamespace(status_code=401, text="Unauthorized registration")

        fake_pywebpush = ModuleType("pywebpush")
        fake_pywebpush.WebPushException = FakeWebPushException
        fake_pywebpush.webpush = lambda **kwargs: (_ for _ in ()).throw(FakeWebPushException())

        with (
            patch.dict(os.environ, {"HIRA_WEB_PUSH_PRIVATE_KEY": "test-key"}),
            patch.dict("sys.modules", {"pywebpush": fake_pywebpush}),
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[{
                "client_id": "phone",
                "subscription": {"endpoint": "https://push.example/sub"},
            }]),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(bot.gs, "set_web_push_delivery_log") as set_log,
        ):
            sent = bot.gs.send_web_push_notification(
                "Evening roundup",
                "Briefing body",
                data={"id": "1", "kind": "briefing", "source": "evening_briefing:2026-05-04"},
            )

        entry = set_log.call_args.args[0][-1]
        self.assertEqual(sent, 0)
        self.assertEqual(entry["errors"], {"http_401": 1})
        self.assertIn("Unauthorized registration", entry["last_error"])

    def test_duplicate_source_notification_refreshes_body_before_retry(self):
        existing = {
            "id": "7",
            "kind": "briefing",
            "title": "Morning briefing",
            "body": "Old digest",
            "created": "2026-05-12T06:45:00+08:00",
            "source": "morning_briefing:2026-05-12",
            "seen_by": [],
            "archived": False,
        }

        with (
            patch.object(bot.gs, "get_app_notifications", return_value=[existing]),
            patch.object(bot.gs, "set_app_notifications") as set_notifications,
        ):
            item = bot.gs.enqueue_app_notification(
                "briefing",
                "Morning briefing",
                "Fresh digest",
                source="morning_briefing:2026-05-12",
            )

        self.assertTrue(item["_duplicate"])
        self.assertEqual(item["body"], "Fresh digest")
        set_notifications.assert_called_once()

    def test_web_push_recovery_sends_active_notification_without_confirmed_delivery(self):
        item = {
            "id": "31",
            "kind": "reminder",
            "title": "HDP remarks due",
            "body": "Complete on 7 May",
            "source": "task_reminder:2026-05-06:31",
            "created": bot.datetime.now(bot.SGT).isoformat(),
            "archived": False,
        }
        with (
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[item]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(web_app.bot.gs, "send_web_push_notification", return_value=1) as send_push,
            patch.object(web_app.bot, "_should_send_phone_push", return_value=True),
            patch.object(web_app.bot, "_mark_action_reminder_delivered") as mark_delivered,
            patch.object(web_app.bot, "_record_notification_outcome"),
        ):
            result = web_app.recover_missed_push_notifications(limit=1)

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["sent"], 1)
        send_push.assert_called_once()
        mark_delivered.assert_called_once_with("task_reminder:2026-05-06:31", ANY)

    def test_web_push_recovery_skips_already_confirmed_delivery(self):
        item = {
            "id": "8",
            "kind": "briefing",
            "title": "Evening roundup",
            "body": "Roundup body",
            "source": "evening_briefing:2026-05-06",
            "created": bot.datetime.now(bot.SGT).isoformat(),
            "archived": False,
        }
        log = [{"source": item["source"], "created": item["created"], "sent": 1}]
        with (
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[item]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=log),
            patch.object(web_app.bot, "_should_send_phone_push", return_value=True),
            patch.object(web_app.bot.gs, "send_web_push_notification") as send_push,
        ):
            result = web_app.recover_missed_push_notifications(limit=1)

        self.assertEqual(result["attempted"], 0)
        send_push.assert_not_called()

    def test_web_push_recovery_retries_recent_worker_config_failure(self):
        now = bot.datetime.now(bot.SGT)
        item = {
            "id": "33",
            "kind": "reminder",
            "title": "HDP remarks due",
            "body": "Complete on 7 May",
            "source": "task_reminder:2026-05-06:33",
            "created": now.isoformat(),
            "archived": False,
        }
        log = [{
            "created": now.isoformat(),
            "source": item["source"],
            "kind": "reminder",
            "title": item["title"],
            "attempted": 0,
            "sent": 0,
            "errors": {"missing_private_key": 1},
        }]
        with (
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[item]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=log),
            patch.object(web_app.bot.gs, "send_web_push_notification", return_value=1) as send_push,
            patch.object(web_app.bot, "_should_send_phone_push", return_value=True),
            patch.object(web_app.bot, "_mark_action_reminder_delivered"),
            patch.object(web_app.bot, "_record_notification_outcome"),
        ):
            result = web_app.recover_missed_push_notifications(limit=1)

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["sent"], 1)
        send_push.assert_called_once()

    def test_web_push_recovery_marks_recovered_nudge_sent(self):
        now = bot.datetime.now(bot.SGT)
        item = {
            "id": "34",
            "kind": "reminder",
            "title": "H.I.R.A nudge",
            "body": "Digest requested for 06:26 SGT",
            "source": "nudge:9",
            "created": now.isoformat(),
            "archived": False,
        }
        with (
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[item]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(web_app.bot.gs, "send_web_push_notification", return_value=1),
            patch.object(web_app.bot.gs, "mark_nudge_sent") as mark_nudge,
            patch.object(web_app.bot, "_should_send_phone_push", return_value=True),
            patch.object(web_app.bot, "_record_notification_outcome"),
        ):
            result = web_app.recover_missed_push_notifications(limit=1)

        self.assertEqual(result["sent"], 1)
        mark_nudge.assert_called_once_with("9")

    def test_web_push_recovery_archives_blocked_cca_calendar_notification(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 14, 10))
        item = {
            "id": "35",
            "kind": "reminder",
            "title": "Calendar reminder",
            "body": "C Div Training starts in 50 min (15:00).",
            "source": "calendar_reminder:2026-05-12:evt-cdiv",
            "created": now.isoformat(),
            "archived": False,
        }

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now if tz else now.replace(tzinfo=None)

        with (
            patch.object(web_app, "datetime", FixedDateTime),
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[item]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(web_app.bot, "cca_duty_cleared_memory_for_date", return_value="cca-duty-cleared:2026-05-12"),
            patch.object(web_app.bot.gs, "archive_app_notifications") as archive,
            patch.object(web_app.bot.gs, "send_web_push_notification") as send_push,
            patch.object(web_app.bot, "_record_notification_outcome"),
        ):
            result = web_app.recover_missed_push_notifications(limit=1)

        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["skipped"], 1)
        archive.assert_called_once_with(["35"])
        send_push.assert_not_called()

    def test_web_push_recovery_prioritises_missed_briefing_over_later_nudge(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 7, 20))
        briefing = {
            "id": "40",
            "kind": "briefing",
            "title": "Morning briefing",
            "body": "Morning digest",
            "source": "morning_briefing:2026-05-12",
            "created": bot.SGT.localize(bot.datetime(2026, 5, 12, 7, 0)).isoformat(),
            "archived": False,
        }
        nudge = {
            "id": "41",
            "kind": "reminder",
            "title": "H.I.R.A nudge",
            "body": "Lower priority nudge",
            "source": "nudge:41",
            "created": bot.SGT.localize(bot.datetime(2026, 5, 12, 7, 10)).isoformat(),
            "archived": False,
        }
        sent_sources = []

        def fake_send(_title, _body, data=None):
            sent_sources.append((data or {}).get("source"))
            return 1

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now if tz else now.replace(tzinfo=None)

        with (
            patch.object(web_app, "datetime", FixedDateTime),
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[nudge, briefing]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(web_app.bot.gs, "send_web_push_notification", side_effect=fake_send),
            patch.object(web_app.bot, "_record_notification_outcome"),
            patch.object(web_app.bot.gs, "set_config"),
        ):
            result = web_app.recover_missed_push_notifications(limit=1)

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(sent_sources, ["morning_briefing:2026-05-12"])

    def test_notification_health_diagnostics_survive_subscription_lookup_failure(self):
        with (
            patch.object(web_app.bot.gs, "get_web_push_subscriptions", return_value=[{
                "client_id": "phone",
                "subscription": {"endpoint": "https://push.example/sub"},
                "created": "",
                "last_seen": "",
            }]),
            patch.object(web_app.bot.gs, "get_app_notifications", return_value=[]),
            patch.object(web_app.bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(web_app.bot.gs, "get_notification_outcome_summary", return_value={"actions": {}}),
            patch.object(web_app.bot.gs, "get_web_push_subscription", side_effect=RuntimeError("sheets down")),
        ):
            diagnostics = web_app._safe_notifications_diagnostics("phone")

        self.assertIsNone(diagnostics["current_subscription"])
        self.assertIn("sheets down", diagnostics["subscription_error"])

    def test_delayed_digest_push_schedules_nudge_without_immediate_digest_route(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 6, 6, 21))
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "build_news_digest", return_value="Digest body") as digest,
            patch.object(bot.gs, "add_nudge", return_value={"id": "9", "status": "pending"}) as add_nudge,
        ):
            scheduled = bot.schedule_delayed_digest_push("Digest. Push notifications in 5 mins", now=now)

        self.assertIsNotNone(scheduled)
        self.assertEqual(scheduled["send_at"].isoformat(), "2026-05-06T06:26:00+08:00")
        self.assertIn("Digest body", scheduled["message"])
        digest.assert_called_once()
        add_nudge.assert_called_once()

    def test_delayed_digest_push_exposes_nudge_tool_not_just_news(self):
        tools = bot.pwa_tools_for_message("Digest. Push notifications in 5 mins")
        names = {tool["name"] for tool in tools}

        self.assertIn("create_proactive_nudge", names)
        self.assertIn("get_latest_news", names)

    def test_pwa_notification_click_prefers_standalone_client(self):
        service_worker = (REPO_ROOT / "pwa" / "service-worker.js").read_text()
        app_js = (REPO_ROOT / "pwa" / "app.js").read_text()
        index_html = (REPO_ROOT / "pwa" / "index.html").read_text()
        manifest = json.loads((REPO_ROOT / "pwa" / "manifest.webmanifest").read_text())

        self.assertIn("standaloneClientIds", service_worker)
        self.assertIn("HIRA_CLIENT_MODE", service_worker)
        self.assertIn("standaloneClientIds.has(client.id)", service_worker)
        self.assertIn("reportClientModeToServiceWorker", app_js)
        self.assertIn("GET_HIRA_VERSION", service_worker)
        self.assertIn("renderAppVersion", app_js)
        self.assertIn("versionOutput", index_html)
        self.assertEqual(manifest["id"], "/")

    def test_app_version_endpoint_reports_commit_and_pwa_versions(self):
        with patch.dict(os.environ, {"RAILWAY_GIT_COMMIT_SHA": "abcdef1234567890"}):
            data = web_app.app_version()

        self.assertEqual(data["app_version"], web_app.PWA_APP_VERSION)
        self.assertEqual(data["service_worker_cache"], web_app.PWA_SERVICE_WORKER_CACHE)
        self.assertEqual(data["git_commit"], "abcdef123456")
        self.assertIn("server_time", data)

    def test_relief_context_becomes_teaching_memory(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 6, 6, 21))
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value={"teaching": []}),
            patch.object(bot.gs, "add_memory") as add_memory,
        ):
            captured = bot.absorb_relief_context("I asked for relief for today's lessons.", now=now)

        self.assertTrue(captured)
        add_memory.assert_called_once()
        category, text = add_memory.call_args.args
        self.assertEqual(category, "teaching")
        self.assertIn("relief:2026-05-06", text)

    def test_medical_leave_context_becomes_teaching_memory(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 8, 15))
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value={"teaching": []}),
            patch.object(bot.gs, "add_memory") as add_memory,
        ):
            captured = bot.absorb_day_state_context("I'm on medical leave today.", now=now)

        self.assertTrue(captured)
        add_memory.assert_called_once()
        category, text = add_memory.call_args.args
        self.assertEqual(category, "teaching")
        self.assertIn("absence:2026-05-12", text)
        self.assertIn("medical leave", text)

    def test_medical_leave_archives_active_school_calendar_notifications(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 8, 15))
        notifications = [{
            "id": "7",
            "kind": "reminder",
            "title": "Calendar reminder",
            "body": "C Div Training starts in 89 min (15:00).",
            "source": "calendar_reminder:2026-05-12:evt-cdiv",
            "archived": False,
        }]
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value={"teaching": []}),
            patch.object(bot.gs, "add_memory"),
            patch.object(bot.gs, "get_app_notifications", return_value=notifications),
            patch.object(bot.gs, "archive_app_notifications", return_value=1) as archive,
            patch.object(bot, "_record_notification_outcome"),
        ):
            captured = bot.absorb_day_state_context("I'm on MC today.", now=now)

        self.assertTrue(captured)
        archive.assert_called_once_with(["7"])

    def test_not_on_duty_blocks_cca_calendar_reminder(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 13, 31))
        event = {
            "id": "evt-cdiv",
            "summary": "C Div Training",
            "start": {"dateTime": "2026-05-12T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-12T18:00:00+08:00"},
            "_calendar_id": "primary",
        }

        with (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
            patch.object(bot, "cca_duty_cleared_memory_for_date", return_value="cca-duty-cleared:2026-05-12"),
        ):
            candidate = bot._calendar_event_reminder_candidate(event, now=now)

        self.assertIsNone(candidate)

    def test_queue_blocks_cca_notification_after_not_on_duty_memory(self):
        with (
            patch.object(bot, "cca_duty_cleared_memory_for_date", return_value="cca-duty-cleared:2026-05-12"),
            patch.object(bot.gs, "enqueue_app_notification") as enqueue,
            patch.object(bot.gs, "send_web_push_notification") as send_push,
            patch.object(bot, "_record_notification_outcome"),
        ):
            queued = bot._queue_app_notification(
                "reminder",
                "Calendar reminder",
                "C Div Training starts in 89 min (15:00).",
                source="calendar_reminder:2026-05-12:evt-cdiv",
            )

        self.assertIsNone(queued)
        enqueue.assert_not_called()
        send_push.assert_not_called()

    def test_absence_memory_answers_why_not_at_work_without_agentic_route(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 12, 8))
        memory = {
            "teaching": [
                "absence:2026-05-12: Herwanto said he is away from work/school on 2026-05-12 because medical leave."
            ]
        }
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=memory),
        ):
            reply = bot.absence_memory_response("Do you remember why im not at work today", now=now)

        self.assertIn("Yes", reply)
        self.assertIn("medical leave today", reply)

    def test_absence_memory_counts_lessons_as_zero(self):
        with patch.object(bot, "school_day_cleared_memory_for_date", return_value="absence:2026-05-12"):
            count = bot._effective_lesson_count(bot.date(2026, 5, 12), [{"subject": "ML"}, {"subject": "ML"}])

        self.assertEqual(count, 0)

    def test_source_citation_preference_is_saved_without_live_research(self):
        text = "Always provide the source when surfacing news items in future pls"
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value={"constraints": []}),
            patch.object(bot.gs, "add_memory") as add_memory,
        ):
            reply = bot.source_citation_preference_response(text)

        self.assertIn("source name", reply)
        add_memory.assert_called_once()
        category, memory_text = add_memory.call_args.args
        self.assertEqual(category, "constraints")
        self.assertIn("source-citation:", memory_text)

        discipline = bot.source_discipline_for_text(text)
        self.assertFalse(discipline["needs_live_check"])
        self.assertEqual(discipline["recommended_tools"], [])

    def test_f1_calendar_sync_request_adds_remaining_events_and_memory(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 12, 36))
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "add_memory") as add_memory,
            patch.object(bot.gs, "get_events_between", return_value=[]),
            patch.object(bot.gs, "create_all_day_event", return_value={"id": "evt"}) as create_all_day_event,
        ):
            result = bot.sync_f1_calendar_to_memory_and_calendar(now=now)

        self.assertEqual(len(result["races"]), 18)
        self.assertEqual(result["calendar_created"], 18)
        self.assertTrue(result["memory_saved"])
        add_memory.assert_called_once()
        category, text = add_memory.call_args.args
        self.assertEqual(category, "sports")
        self.assertIn("f1-calendar:2026", text)
        self.assertIn("R5 Canada 2026-05-22 to 2026-05-24", text)
        self.assertIn("R22 Abu Dhabi 2026-12-04 to 2026-12-06", text)
        first_call = create_all_day_event.call_args_list[0].args
        self.assertEqual(first_call[0], "F1: Canadian Grand Prix (Sprint weekend)")
        self.assertEqual(first_call[1], "2026-05-22")
        self.assertEqual(first_call[2], "2026-05-24")

    def test_f1_calendar_sync_response_is_direct_action_not_brief(self):
        with patch.object(bot, "sync_f1_calendar_to_memory_and_calendar", return_value={
            "source": bot.F1_2026_CALENDAR_SOURCE,
            "races": [
                {"short": "Canada", "start": "2026-05-22", "end": "2026-05-24", "sprint": True},
                {"short": "Abu Dhabi", "start": "2026-12-04", "end": "2026-12-06", "sprint": False},
            ],
            "memory_saved": True,
            "calendar_created": 2,
            "calendar_skipped": 0,
            "errors": [],
        }):
            reply = bot.f1_calendar_sync_response(
                "Find the f1 calendar for the rest of this season and append it to your memory and my calendar"
            )

        self.assertIn("Saved the season list to sports memory", reply)
        self.assertIn("Calendar: created 2 event", reply)
        self.assertNotIn("No recent Google News items", reply)

    def test_cca_schedule_selects_current_week_tab_and_blocks_when_name_absent(self):
        book = {
            "properties": {"title": "CCA Calendar"},
            "sheets": [
                {"properties": {"sheetId": 111, "title": "T2 Week 7"}},
                {"properties": {"sheetId": 1961438111, "title": "T2 Week 8"}},
            ],
        }
        ranges = {
            "'T2 Week 7'!A1:Z220": [["Monday", "Coach", "Herwanto"]],
            "'T2 Week 8'!A1:Z220": [
                ["Odd week, Term 2 Week 8"],
                ["Tuesday 12 May", "Football CCA", "Coach A"],
            ],
        }
        fake_service = FakeSheetsService(book, ranges=ranges)
        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "get_config", side_effect=lambda key: {
                "cca_schedule_spreadsheet_id": "1L5FGME5itmc3vknwL0xSsIrz4qJ3n6z1YfxffgeB3nU",
                "cca_schedule_gid": "1961438111",
            }.get(key, "")),
        ):
            snapshot = bot.gs.get_cca_schedule_snapshot(bot.date(2026, 5, 12), week_label="Odd week, Term 2 Week 8")

        self.assertEqual(snapshot["selected_tab"], "T2 Week 8")
        self.assertFalse(snapshot["assigned"])
        text = bot.gs.format_cca_schedule_snapshot(snapshot)
        self.assertIn("Hard stop", text)
        self.assertIn("do not prompt", text)

    def test_cca_schedule_reports_matching_herwanto_row(self):
        book = {
            "properties": {"title": "CCA Calendar"},
            "sheets": [{"properties": {"sheetId": 1961438111, "title": "T2 Week 8"}}],
        }
        ranges = {
            "'T2 Week 8'!A1:Z220": [
                ["Odd week, Term 2 Week 8"],
                ["Tuesday 12 May", "Football CCA", "Herwanto", "1530-1800"],
            ],
        }
        fake_service = FakeSheetsService(book, ranges=ranges)
        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "get_config", return_value=""),
        ):
            snapshot = bot.gs.get_cca_schedule_snapshot(bot.date(2026, 5, 12), week_label="Odd week, Term 2 Week 8")

        self.assertTrue(snapshot["assigned"])
        self.assertIn("Herwanto", bot.gs.format_cca_schedule_snapshot(snapshot))

    def test_cca_schedule_prompt_forces_sheet_tool(self):
        forced = bot._forced_tool_for_text(
            "am I on CCA duty today?",
            [{"name": "get_cca_schedule"}, {"name": "create_calendar_event"}],
        )

        self.assertEqual(forced, "get_cca_schedule")

    def test_nsg_duty_prompt_forces_sheet_tool(self):
        forced = bot._forced_tool_for_text(
            "Am I on NSG duty for Thursday?",
            [{"name": "get_cca_schedule"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "get_cca_schedule")

    def test_n2_football_calendar_gets_cca_tool_in_pwa(self):
        tools = bot.pwa_tools_for_message("You need to check the N2 football calendar and tell me")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_cca_schedule", names)

    def test_weekday_day_question_is_not_quick_chat(self):
        routed_quick = asyncio.run(bot.should_route_quick_pwa_chat([], "Hey whats my Thursday like"))

        self.assertFalse(routed_quick)

    def test_cca_schedule_prefers_target_month_tab_over_stale_configured_gid(self):
        book = {
            "properties": {"title": "CCA Calendar"},
            "sheets": [
                {"properties": {"sheetId": 1961438111, "title": "January"}},
                {"properties": {"sheetId": 222, "title": "May"}},
            ],
        }
        ranges = {
            "'January'!A1:Z220": [["January Football CCA", "Herwanto"]],
            "'May'!A1:Z220": [
                ["May Football CCA"],
                ["Thursday 14 May", "NSG C Div", "Boon Lay", "Mr Tan"],
            ],
        }
        fake_service = FakeSheetsService(book, ranges=ranges)
        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "get_config", side_effect=lambda key: {
                "cca_schedule_spreadsheet_id": "1L5FGME5itmc3vknwL0xSsIrz4qJ3n6z1YfxffgeB3nU",
                "cca_schedule_gid": "1961438111",
            }.get(key, "")),
        ):
            snapshot = bot.gs.get_cca_schedule_snapshot(bot.date(2026, 5, 14), week_label="Odd week, Term 2 Week 8")

        self.assertEqual(snapshot["selected_tab"], "May")
        self.assertFalse(snapshot["assigned"])

    def test_cca_source_parses_resource_key_from_full_link(self):
        url = (
            "https://docs.google.com/spreadsheets/d/sheet123/edit"
            "?gid=987&resourcekey=0-abcDEF_123#gid=987"
        )
        with patch.object(bot.gs, "get_config", side_effect=lambda key: url if key == "cca_schedule_url" else ""):
            source = bot.gs._cca_source_config()

        self.assertEqual(source["spreadsheet_id"], "sheet123")
        self.assertEqual(source["gid"], "987")
        self.assertEqual(source["resource_key"], "0-abcDEF_123")
        self.assertIn("resourcekey=0-abcDEF_123", source["url"])

    def test_cca_source_has_backend_default_url(self):
        with patch.object(bot.gs, "get_config", return_value=""):
            source = bot.gs._cca_source_config()

        self.assertEqual(source["spreadsheet_id"], "1L5FGME5itmc3vknwL0xSsIrz4qJ3n6z1YfxffgeB3nU")
        self.assertEqual(source["gid"], "1961438111")
        self.assertIn("docs.google.com/spreadsheets/d/1L5FGME5itmc3vknwL0xSsIrz4qJ3n6z1YfxffgeB3nU", source["url"])

    def test_public_sheet_csv_uses_resource_key(self):
        captured = {}

        class FakeResponse:
            status_code = 200
            url = "https://docs.google.com/spreadsheets/d/sheet123/export"
            text = "Date,Teacher\nThursday 14 May,Herwanto\n"

        def fake_get(url, params=None, headers=None, timeout=None):
            captured["url"] = url
            captured["params"] = params
            captured["headers"] = headers
            return FakeResponse()

        with patch.object(bot.gs.requests, "get", side_effect=fake_get):
            rows = bot.gs._public_sheet_csv_values("sheet123", "987", "0-key")

        self.assertEqual(rows[1][1], "Herwanto")
        self.assertEqual(captured["params"]["resourcekey"], "0-key")
        self.assertEqual(captured["headers"]["X-Goog-Drive-Resource-Keys"], "sheet123/0-key")

    def test_unsupported_memory_backend_claim_is_blocked(self):
        reply = (
            "The memory store is hitting a Sheets error on the backend. "
            "This means I am running with session memory only and the service account needs access."
        )

        guarded = bot._backend_claim_guardrail(reply, [])

        self.assertIn("cannot verify", guarded)
        self.assertIn("not verified the cause", guarded)
        self.assertNotIn("I should show", guarded)
        self.assertNotIn("service account needs access", guarded)

    def test_memory_backend_claim_allowed_with_tool_evidence(self):
        reply = "Permanent memory failed because Google Sheets denied permission."
        guarded = bot._backend_claim_guardrail(
            reply,
            [{"content": "Failed to remember: Google Sheets denied write access while updating Config."}],
        )

        self.assertEqual(guarded, reply)

    def test_unsupported_google_sheets_rate_limit_claim_is_blocked(self):
        reply = "Rate limit — Google Sheets hit its per-minute quota cap. Try again in about 60 seconds."

        guarded = bot._backend_claim_guardrail(reply, [])

        self.assertIn("cannot verify", guarded)
        self.assertNotIn("per-minute quota", guarded)

    def test_cca_access_failure_does_not_push_sheet_check_to_user(self):
        reply = "Try again in 60 seconds. Or if you have the sheet open, just tell me if your name is on Thursday."
        guarded = bot._cca_sheet_user_burden_guardrail(
            reply,
            [{"content": "CCA schedule unavailable: Sheets API access failed with HTTP 403. Official CCA duty status is unverified."}],
        )

        self.assertIn("should not push", guarded)
        self.assertNotIn("tell me if your name", guarded)

    def test_memory_tool_failure_reports_only_tool_error(self):
        reply = (
            "Annoyingly, the memory write is hitting the same 403 on the backend config sheet. "
            "Open the config sheet and share it with the service account."
        )
        guarded = bot._memory_tool_failure_guardrail(
            reply,
            [{"content": "Failed to remember: Google Sheets denied write access while updating Config."}],
        )

        self.assertIn("Memory save failed: Google Sheets denied write access while updating Config.", guarded)
        self.assertIn("have not written this", guarded)
        self.assertNotIn("service account", guarded)

    def test_commit_to_memory_forces_memory_tool(self):
        forced = bot._forced_tool_for_text(
            "Commit this to memory: once fixed, push immediately",
            [{"name": "remember_user_info"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "remember_user_info")

    def test_memory_recall_question_does_not_force_memory_write(self):
        forced = bot._forced_tool_for_text(
            "Hey HIRA. Remember the CCA calendar I told u to refer to?",
            [{"name": "remember_user_info"}, {"name": "get_cca_schedule"}],
        )

        self.assertEqual(forced, "get_cca_schedule")

    def test_plain_memory_fact_still_forces_memory_tool(self):
        forced = bot._forced_tool_for_text(
            "Remember my mum's name is Salwa",
            [{"name": "remember_user_info"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "remember_user_info")

    def test_memory_rate_limit_error_is_compacted(self):
        guarded = bot._memory_tool_failure_guardrail(
            "raw failure",
            [{
                "content": (
                    "Failed to remember: <HttpError 429 when requesting https://sheets.googleapis.com/v4/spreadsheets/x "
                    "returned \"Quota exceeded for quota metric 'Read requests'\". Details: "
                    "\"reason\": \"RATE_LIMIT_EXCEEDED\", \"quotalimitvalue\": \"60\" lots of raw JSON"
                )
            }],
        )

        self.assertIn("Memory save failed: Google Sheets read quota exceeded (60 read requests/min/user)", guarded)
        self.assertNotIn("sheets.googleapis.com/v4/spreadsheets", guarded)

    def test_commit_to_memory_gets_memory_tool_in_pwa(self):
        tools = bot.pwa_tools_for_message("Save this to memory: stop waiting around after fixes")
        names = {tool["name"] for tool in tools}

        self.assertIn("remember_user_info", names)

    def test_commit_to_memory_is_not_quick_chat(self):
        routed_quick = asyncio.run(bot.should_route_quick_pwa_chat([], "Commit this to memory"))

        self.assertFalse(routed_quick)

    def test_daily_load_counts_relieved_lessons_as_zero(self):
        today_key = bot.datetime.now(bot.SGT).date().isoformat()
        agenda = {
            "days": [{
                "date": today_key,
                "lessons": [{"subject": "ML"}, {"subject": "ML"}, {"subject": "ML"}, {"subject": "ML"}],
                "events": [],
                "due": [],
                "relieved": True,
            }]
        }
        with (
            patch.object(bot, "build_agenda_structured", return_value=agenda),
            patch.object(bot, "google_ok", return_value=False),
            patch.object(bot, "_load_days_for_dates", return_value=[]),
        ):
            load = bot.build_daily_load()

        self.assertEqual(load["today"]["lessons"], 0)
        self.assertLess(load["today"]["score"], 12)

    def test_home_daily_load_keeps_timetable_when_google_side_data_fails(self):
        lessons = [
            {"start": "08:00", "end": "08:30", "subject": "FT", "description": "Form Time", "room": "Class"},
            {"start": "09:00", "end": "10:00", "subject": "ML", "description": "Mother Tongue", "room": "L3"},
        ]

        with (
            patch.object(web_app.bot, "google_ok", return_value=True),
            patch.object(web_app.bot.gs, "get_events_for_days", side_effect=RuntimeError("calendar down")),
            patch.object(web_app.bot.gs, "get_reminders", side_effect=RuntimeError("sheets down")),
            patch.object(web_app.bot.gs, "get_task_metadata", return_value={}),
            patch.object(web_app.bot.gs, "get_marking_tasks", return_value=[]),
            patch.object(web_app.bot.gs, "get_memory", return_value={}),
            patch.object(web_app.bot.gs, "get_events_between", side_effect=RuntimeError("calendar down")),
            patch.object(web_app.bot, "_lessons_for_date", return_value=(lessons, "Odd")),
            patch.object(web_app.bot, "school_day_cleared_memory_for_date", return_value=""),
            patch.object(web_app.bot, "build_curated_digest_snapshot", return_value={"items": []}),
            patch.object(web_app.bot, "build_proactive_v2_snapshot", return_value={"top": []}),
            patch.object(web_app.bot, "build_islamic_brief", return_value=""),
            patch.object(web_app.bot, "prayer_notification_status", return_value={}),
            patch.object(web_app, "_service_status", return_value={}),
            patch.object(web_app, "_classops_status_summary", return_value={}),
            patch.object(web_app, "_briefing_delivery_status", return_value={}),
        ):
            data = web_app._parallel_home_data(3)

        self.assertTrue(data["daily_load"]["days"])
        self.assertEqual(data["daily_load"]["today"]["lessons"], 2)
        self.assertNotIn("unavailable", data["daily_load"]["note"].lower())
        self.assertNotIn("unavailable", data["daily_load"]["rest_note"].lower())

    def test_morning_briefing_waits_for_confirmed_phone_push(self):
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot, "build_briefing", return_value="Morning digest body") as build_briefing,
            patch.object(bot, "_queue_app_notification", return_value={
                "id": "1",
                "kind": "briefing",
                "title": "Morning briefing",
                "body": "Morning digest body",
                "_push_sent": 0,
            }),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            sent = asyncio.run(bot.send_morning_briefing_once())

        self.assertFalse(sent)
        build_briefing.assert_called_once_with(record_news_digest=False)
        set_config.assert_not_called()

    def test_morning_briefing_marks_done_after_phone_push(self):
        pending_entry = {"key": "digest-1", "item": {"title": "Fresh headline"}}
        pending_at = bot.datetime.now(bot.SGT)
        bot._PENDING_NEWS_DIGEST_ENTRIES = [pending_entry]
        bot._PENDING_NEWS_DIGEST_BUILT_AT = pending_at
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot, "build_briefing", return_value="Morning digest body") as build_briefing,
            patch.object(bot, "_queue_app_notification", return_value={
                "id": "1",
                "kind": "briefing",
                "title": "Morning briefing",
                "body": "Morning digest body",
                "_push_sent": 1,
            }),
            patch.object(bot, "_remember_news_digest_entries") as remember_digest,
            patch.object(bot.gs, "set_config") as set_config,
        ):
            sent = asyncio.run(bot.send_morning_briefing_once())

        self.assertTrue(sent)
        build_briefing.assert_called_once_with(record_news_digest=False)
        remember_digest.assert_called_once_with([pending_entry], now=pending_at)
        set_config.assert_called_once()

    def test_morning_briefing_retries_stale_sent_flag_without_push_log(self):
        today_key = bot.datetime.now(bot.SGT).strftime("%Y-%m-%d")
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=today_key),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[{
                "created": bot.datetime.now(bot.SGT).isoformat(),
                "source": f"morning_briefing:{today_key}",
                "sent": 0,
            }]),
            patch.object(bot, "build_briefing", return_value="Morning digest body"),
            patch.object(bot, "_queue_app_notification", return_value={
                "id": "1",
                "kind": "briefing",
                "title": "Morning briefing",
                "body": "Morning digest body",
                "_push_sent": 1,
            }),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            sent = asyncio.run(bot.send_morning_briefing_once())

        self.assertTrue(sent)
        set_config.assert_called_once()

    def test_morning_briefing_skips_when_today_push_was_confirmed(self):
        today_key = bot.datetime.now(bot.SGT).strftime("%Y-%m-%d")
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=today_key),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[{
                "created": bot.datetime.now(bot.SGT).isoformat(),
                "source": f"morning_briefing:{today_key}",
                "sent": 1,
            }]),
            patch.object(bot, "build_briefing") as build_briefing,
        ):
            sent = asyncio.run(bot.send_morning_briefing_once())

        self.assertTrue(sent)
        build_briefing.assert_not_called()

    def test_evening_briefing_waits_for_confirmed_phone_push(self):
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot, "build_evening_briefing", return_value="Evening digest body"),
            patch.object(bot, "_queue_app_notification", return_value={
                "id": "1",
                "kind": "briefing",
                "title": "Evening roundup",
                "body": "Evening digest body",
                "_push_sent": 0,
            }),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            sent = asyncio.run(bot.send_evening_briefing_once())

        self.assertFalse(sent)
        set_config.assert_not_called()

    def test_evening_briefing_marks_done_after_phone_push(self):
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot, "build_evening_briefing", return_value="Evening digest body"),
            patch.object(bot, "_queue_app_notification", return_value={
                "id": "1",
                "kind": "briefing",
                "title": "Evening roundup",
                "body": "Evening digest body",
                "_push_sent": 1,
            }),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            sent = asyncio.run(bot.send_evening_briefing_once())

        self.assertTrue(sent)
        set_config.assert_called_once()

    def test_evening_briefing_retries_stale_sent_flag_without_push_log(self):
        today_key = bot.datetime.now(bot.SGT).strftime("%Y-%m-%d")
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=today_key),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[{
                "created": bot.datetime.now(bot.SGT).isoformat(),
                "source": f"evening_briefing:{today_key}",
                "sent": 0,
            }]),
            patch.object(bot, "build_evening_briefing", return_value="Evening digest body"),
            patch.object(bot, "_queue_app_notification", return_value={
                "id": "1",
                "kind": "briefing",
                "title": "Evening roundup",
                "body": "Evening digest body",
                "_push_sent": 1,
            }),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            sent = asyncio.run(bot.send_evening_briefing_once())

        self.assertTrue(sent)
        set_config.assert_called_once()

    def test_evening_briefing_skips_when_today_push_was_confirmed(self):
        today_key = bot.datetime.now(bot.SGT).strftime("%Y-%m-%d")
        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot.gs, "get_config", return_value=today_key),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[{
                "created": bot.datetime.now(bot.SGT).isoformat(),
                "source": f"evening_briefing:{today_key}",
                "sent": 1,
            }]),
            patch.object(bot, "build_evening_briefing") as build_evening,
        ):
            sent = asyncio.run(bot.send_evening_briefing_once())

        self.assertTrue(sent)
        build_evening.assert_not_called()

    def test_daily_briefing_confirmed_accepts_canonical_or_web_source(self):
        today_key = "2026-05-10"
        delivery_log = [{
            "created": "2026-05-10T21:00:00+08:00",
            "source": f"web_evening_briefing:{today_key}",
            "sent": 1,
        }]

        with patch.object(bot.gs, "get_config", return_value=today_key):
            confirmed = web_app._daily_briefing_confirmed("evening", today_key, delivery_log)

        self.assertTrue(confirmed)

    def test_daily_briefing_safety_net_runs_missing_evening_digest(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = datetime(2026, 5, 10, 21, 5)
                return bot.SGT.localize(value) if tz else value

        calls = []

        async def fake_evening(context=None, source="evening_briefing"):
            calls.append(source)
            return True

        async def fake_morning(context=None, source="morning_briefing"):
            raise AssertionError("morning safety net should not run at 21:05")

        with (
            patch.object(web_app, "datetime", FixedDateTime),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[]),
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot, "send_evening_briefing_once", side_effect=fake_evening),
            patch.object(bot, "send_morning_briefing_once", side_effect=fake_morning),
        ):
            result = asyncio.run(web_app.recover_missed_daily_briefings())

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["delivered"], 1)
        self.assertEqual(calls, ["evening_briefing"])

    def test_briefing_delivery_status_shows_confirmed_pushes(self):
        today_key = "2026-05-10"
        now = bot.SGT.localize(datetime(2026, 5, 10, 21, 10))
        delivery_log = [
            {"created": "2026-05-10T06:50:00+08:00", "source": f"morning_briefing:{today_key}", "sent": 1},
            {"created": "2026-05-10T21:03:00+08:00", "source": f"web_evening_briefing:{today_key}", "sent": 1},
        ]

        with patch.object(bot.gs, "get_config", return_value=today_key):
            status = web_app._briefing_delivery_status(delivery_log, [], now=now)

        self.assertEqual(status["overall"], "ok")
        self.assertEqual([slot["status"] for slot in status["slots"]], ["delivered", "delivered"])
        self.assertEqual(status["slots"][1]["delivered_at"], "21:03")

    def test_briefing_delivery_status_flags_missed_evening_after_catchup(self):
        today_key = "2026-05-10"
        now = bot.SGT.localize(datetime(2026, 5, 10, 22, 45))
        delivery_log = [
            {"created": "2026-05-10T06:50:00+08:00", "source": f"morning_briefing:{today_key}", "sent": 1},
        ]

        with patch.object(bot.gs, "get_config", side_effect=lambda key: today_key if key == bot.MORNING_BRIEFING_SENT_KEY else ""):
            status = web_app._briefing_delivery_status(delivery_log, [], now=now)

        self.assertEqual(status["overall"], "attention")
        self.assertEqual(status["slots"][1]["slot"], "evening")
        self.assertEqual(status["slots"][1]["status"], "missed")

    def test_briefing_delivery_status_shows_active_recovery_window(self):
        today_key = "2026-05-10"
        now = bot.SGT.localize(datetime(2026, 5, 10, 21, 5))
        delivery_log = [
            {"created": "2026-05-10T06:50:00+08:00", "source": f"morning_briefing:{today_key}", "sent": 1},
        ]

        with patch.object(bot.gs, "get_config", side_effect=lambda key: today_key if key == bot.MORNING_BRIEFING_SENT_KEY else ""):
            status = web_app._briefing_delivery_status(delivery_log, [], now=now)

        self.assertEqual(status["overall"], "watching")
        self.assertEqual(status["slots"][1]["slot"], "evening")
        self.assertEqual(status["slots"][1]["status"], "recovering")

    def test_bot_digestcheck_reports_confirmed_delivery_keys(self):
        today_key = "2026-05-10"
        now = bot.SGT.localize(datetime(2026, 5, 10, 21, 10))
        delivery_log = [
            {"created": "2026-05-10T06:50:00+08:00", "source": f"morning_briefing:{today_key}", "sent": 1},
            {"created": "2026-05-10T21:03:00+08:00", "source": f"web_evening_briefing:{today_key}", "sent": 1},
        ]

        with patch.object(bot.gs, "get_config", return_value=today_key):
            status = bot.build_digest_delivery_status(delivery_log, [], now=now)

        self.assertEqual(status["overall"], "ok")
        self.assertEqual([slot["status"] for slot in status["slots"]], ["delivered", "delivered"])
        text = bot.format_digest_delivery_status(status)
        self.assertIn(f"morning_briefing:{today_key}", text)
        self.assertIn(f"web_evening_briefing:{today_key}", text)

    def test_storagecheck_collapses_operations_into_clear_states(self):
        with (
            patch.object(bot.gs, "memory_storage_status", return_value={"connected": True, "source": "postgres"}),
            patch.object(bot.gs, "set_config"),
            patch.object(bot, "_get_redis", return_value=object()),
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[{"client_id": "phone", "subscription": {"endpoint": "x"}}]),
            patch.object(bot.gs, "get_web_push_delivery_log", return_value=[{"source": "morning_briefing:2026-05-10", "sent": 1, "attempted": 1}]),
            patch.object(bot.gs, "get_cca_schedule_snapshot", return_value={"selected_tab": "Week 8"}),
        ):
            status = bot.build_storage_check()

        self.assertEqual(status["overall"], "ok")
        self.assertEqual(status["checks"]["memory"]["source"], "postgres")
        self.assertEqual(status["checks"]["push_subscriptions"]["count"], 1)
        text = bot.format_storage_check(status)
        self.assertIn("Storage check", text)
        self.assertIn("Memory: ok", text)

    def test_proactive_v2_queue_prefers_higher_score_ready_items(self):
        now = bot.datetime.now(bot.SGT)
        with patch("bot.google_ok", return_value=False), \
             patch("bot.build_proactive_intelligence_insights", return_value=[
                 {"id": "alpha", "title": "Alpha", "body": "First", "priority": "medium"},
                 {"id": "beta", "title": "Beta", "body": "Second", "priority": "high"},
             ]), \
             patch("bot.build_task_structured", return_value={"items": []}), \
             patch("bot._should_suppress_notification", side_effect=lambda source, kind, now=None: source.endswith("alpha")), \
             patch("bot._notification_feedback_bias", side_effect=lambda source, kind, now=None, days=30: 4 if source.endswith("beta") else 0):
            queue = bot.build_proactive_v2_queue(now=now, families={"intelligence"})

        self.assertEqual(queue[0]["title"], "Beta")
        self.assertFalse(queue[0]["suppressed"])
        self.assertTrue(any(item["title"] == "Alpha" and item["suppressed"] for item in queue))

    def test_calendar_reminder_candidate_for_upcoming_trigger_event(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 14, 30))
        event = {
            "id": "evt-duty",
            "summary": "CCA duty briefing",
            "location": "Hall",
            "description": "",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
            "_calendar_id": "primary",
        }

        with (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
            patch.object(bot, "_cca_roster_assignment_confirmed", return_value=True),
        ):
            candidate = bot._calendar_event_reminder_candidate(event, now=now)

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["family"], "calendar_reminder")
        self.assertEqual(candidate["kind"], "reminder")
        self.assertIn("CCA duty briefing", candidate["body"])
        self.assertIn("calendar_reminder:2026-05-05:evt-duty", candidate["source"])

    def test_calendar_reminder_blocks_cca_when_not_on_roster(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 13, 31))
        event = {
            "id": "evt-cdiv",
            "summary": "C Div Training",
            "start": {"dateTime": "2026-05-12T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-12T18:00:00+08:00"},
            "_calendar_id": "primary",
        }

        with (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
            patch.object(bot, "_cca_roster_assignment_confirmed", return_value=False),
        ):
            candidate = bot._calendar_event_reminder_candidate(event, now=now)

        self.assertIsNone(candidate)

    def test_calendar_reminder_blocks_school_events_on_medical_leave(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 12, 13, 31))
        event = {
            "id": "evt-workshop",
            "summary": "Staff training workshop",
            "start": {"dateTime": "2026-05-12T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-12T16:00:00+08:00"},
            "_calendar_id": "primary",
        }

        with (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
            patch.object(bot, "school_day_cleared_memory_for_date", return_value="absence:2026-05-12: medical leave"),
        ):
            candidate = bot._calendar_event_reminder_candidate(event, now=now)

        self.assertIsNone(candidate)

    def test_calendar_reminder_skips_unmatched_or_already_delivered_events(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 14, 30))
        focus_event = {
            "id": "evt-focus",
            "summary": "Focus time",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
        }
        duty_event = {
            "id": "evt-duty",
            "summary": "Exam duty",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
        }

        with (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
        ):
            self.assertIsNone(bot._calendar_event_reminder_candidate(focus_event, now=now))

        with (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=True),
        ):
            self.assertIsNone(bot._calendar_event_reminder_candidate(duty_event, now=now))

    def test_calendar_travel_candidate_pushes_when_leave_window_is_due(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 14, 5))
        event = {
            "id": "evt-hdb",
            "summary": "HDB appointment",
            "location": "HDB Hub Toa Payoh",
            "description": "",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
            "_calendar_id": "primary",
        }

        with (
            patch.dict(os.environ, {"HIRA_DEFAULT_TRAVEL_MINUTES": "45", "HIRA_TRAVEL_BUFFER_MINUTES": "10"}),
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
        ):
            candidate = bot._calendar_event_travel_candidate(event, now=now)

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["title"], "Time to leave")
        self.assertIn("Rough estimated travel is about 45 min", candidate["body"])
        self.assertIn("Leave now", candidate["body"])
        self.assertEqual(candidate["source"], "calendar_travel:2026-05-05:evt-hdb")
        self.assertEqual(candidate["confidence"], "must_remind")

    def test_calendar_travel_candidate_uses_place_override(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 13, 55))
        event = {
            "id": "evt-hdb",
            "summary": "HDB appointment",
            "location": "HDB Hub Toa Payoh",
            "description": "",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
        }

        with (
            patch.dict(os.environ, {"HIRA_TRAVEL_TIME_OVERRIDES": "HDB Hub=55", "HIRA_TRAVEL_BUFFER_MINUTES": "10"}),
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
        ):
            candidate = bot._calendar_event_travel_candidate(event, now=now)

        self.assertIsNotNone(candidate)
        self.assertIn("Rough estimated travel is about 55 min", candidate["body"])

    def test_calendar_travel_candidate_skips_internal_locations(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 14, 5))
        event = {
            "id": "evt-hall",
            "summary": "CCA briefing",
            "location": "Hall",
            "description": "",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
        }

        self.assertIsNone(bot._calendar_event_travel_candidate(event, now=now))

    def test_calendar_reminder_scan_fetches_travel_horizon(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 13, 35))
        event = {
            "id": "evt-hdb",
            "summary": "HDB appointment",
            "location": "HDB Hub Toa Payoh",
            "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
            "end": {"dateTime": "2026-05-05T16:00:00+08:00"},
        }

        with (
            patch.dict(os.environ, {
                "HIRA_DEFAULT_TRAVEL_MINUTES": "75",
                "HIRA_TRAVEL_BUFFER_MINUTES": "10",
                "HIRA_CALENDAR_REMINDER_LOOKAHEAD_MINUTES": "10",
            }),
            patch.object(bot.gs, "get_events_between", return_value=[event]) as get_events,
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
        ):
            candidates = bot._calendar_reminder_candidates(now=now)

        self.assertTrue(any(item["source"] == "calendar_travel:2026-05-05:evt-hdb" for item in candidates))
        fetch_end = get_events.call_args.args[1]
        self.assertGreaterEqual(fetch_end, now + bot.timedelta(minutes=85))

    def test_daily_task_candidate_uses_reminder_push_family(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 9, 0))
        task = {
            "id": "31",
            "description": "Submit Sec 2 marks",
            "due": "2026-05-05",
            "category": "Teaching",
            "priority": "high",
            "effort": "medium",
            "next_action": "Upload marks before lunch.",
        }

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "_calendar_reminder_candidates", return_value=[]),
            patch.object(bot, "build_task_structured", return_value={"items": [task]}),
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=False),
            patch.object(bot, "_action_reminder_was_delivered", return_value=False),
        ):
            queue = bot.build_proactive_v2_queue(now=now, days=2, families={"task"})

        self.assertEqual(queue[0]["family"], "task")
        self.assertEqual(queue[0]["kind"], "reminder")
        self.assertEqual(queue[0]["source"], "task_reminder:2026-05-05:31")

    def test_dispatch_marks_action_reminder_after_confirmed_push(self):
        future_start = (bot.datetime.now(bot.SGT) + bot.timedelta(minutes=30)).isoformat()
        candidate = {
            "family": "calendar_reminder",
            "source": "calendar_reminder:2026-05-12:evt-duty",
            "kind": "reminder",
            "title": "Calendar reminder",
            "body": "CCA duty briefing starts in 30 min.",
            "suppressed": False,
            "metadata": {"start": future_start},
        }

        with (
            patch.object(bot, "_queue_app_notification", return_value={"id": "1", "_push_sent": 1}),
            patch.object(bot, "_mark_action_reminder_delivered") as mark_delivered,
        ):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 1)
        mark_delivered.assert_called_once()

    def test_dispatch_skips_stale_calendar_reminder(self):
        stale_start = (bot.datetime.now(bot.SGT) - bot.timedelta(minutes=30)).isoformat()
        candidate = {
            "family": "calendar_reminder",
            "source": "calendar_reminder:2026-05-12:evt-duty",
            "kind": "reminder",
            "title": "Calendar reminder",
            "body": "CCA duty briefing starts in 30 min.",
            "suppressed": False,
            "metadata": {"start": stale_start},
        }

        with patch.object(bot, "_queue_app_notification") as queue:
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 0)
        queue.assert_not_called()

    def test_user_nudge_pushes_during_quiet_hours(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 5, 23, 30))

        self.assertTrue(bot._quiet_hours_active(now=now))
        self.assertTrue(bot._should_send_phone_push("reminder", "nudge:42", now=now))
        self.assertTrue(bot._should_send_phone_push("briefing", "morning_briefing:2026-05-05", now=now))
        self.assertFalse(bot._should_send_phone_push("reminder", "checkin:42", now=now))

    def test_dispatch_keeps_pwa_nudge_pending_without_phone_push(self):
        candidate = {
            "family": "nudge",
            "source": "nudge:42",
            "kind": "reminder",
            "title": "H.I.R.A nudge",
            "body": "Go to bed.",
            "suppressed": False,
            "metadata": {"nudge_id": "42"},
        }

        with (
            patch.object(bot, "_queue_app_notification", return_value={"id": "1", "_push_sent": 0}),
            patch.object(bot.gs, "mark_nudge_sent") as mark_sent,
        ):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 1)
        mark_sent.assert_not_called()

    def test_dispatch_marks_pwa_nudge_after_confirmed_phone_push(self):
        candidate = {
            "family": "nudge",
            "source": "nudge:42",
            "kind": "reminder",
            "title": "H.I.R.A nudge",
            "body": "Go to bed.",
            "suppressed": False,
            "metadata": {"nudge_id": "42"},
        }

        with (
            patch.object(bot, "_queue_app_notification", return_value={"id": "1", "_push_sent": 1}),
            patch.object(bot.gs, "mark_nudge_sent") as mark_sent,
        ):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 1)
        mark_sent.assert_called_once_with("42")

    def test_duplicate_active_nudge_is_not_pushed_twice(self):
        existing = {"id": "1", "source": "nudge:42", "_duplicate": True}

        with (
            patch.object(bot.gs, "enqueue_app_notification", return_value=existing),
            patch.object(bot.gs, "send_web_push_notification") as push,
        ):
            item = bot._queue_app_notification("reminder", "H.I.R.A nudge", "Go to bed.", source="nudge:42")

        self.assertTrue(item["_already_active"])
        self.assertEqual(item["_push_sent"], 0)
        push.assert_not_called()

    def test_dispatch_marks_duplicate_active_nudge_sent(self):
        candidate = {
            "family": "nudge",
            "source": "nudge:42",
            "kind": "reminder",
            "title": "H.I.R.A nudge",
            "body": "Go to bed.",
            "suppressed": False,
            "metadata": {"nudge_id": "42"},
        }

        with (
            patch.object(bot, "_queue_app_notification", return_value={"id": "1", "_push_sent": 0, "_already_active": True}),
            patch.object(bot.gs, "mark_nudge_sent") as mark_sent,
        ):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 1)
        mark_sent.assert_called_once_with("42")

    def test_dispatch_skips_digest_only_reminder_confidence(self):
        candidate = {
            "family": "task",
            "source": "task_reminder:2026-05-05:31",
            "kind": "reminder",
            "title": "Low signal task",
            "body": "Could be handled in digest.",
            "suppressed": False,
            "confidence": "digest_only",
            "metadata": {},
        }

        with patch.object(bot, "_queue_app_notification") as queue:
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 0)
        queue.assert_not_called()

    def test_push_recovery_summary_surfaces_last_success_and_issue(self):
        log = [
            {"created": "2026-05-05T08:00:00+08:00", "source": "morning", "attempted": 1, "sent": 1},
            {
                "created": "2026-05-05T09:00:00+08:00",
                "source": "task",
                "attempted": 1,
                "sent": 0,
                "last_error": "401: Unauthorized registration",
            },
        ]

        with patch.dict(os.environ, {"HIRA_WEB_PUSH_PRIVATE_KEY": "test-key"}):
            summary = web_app._push_recovery_summary(log, queued=[{"id": "1"}], subscriptions=[{"client_id": "phone"}])

        self.assertEqual(summary["status"], "delivery_missed")
        self.assertEqual(summary["last_success_source"], "morning")
        self.assertIn("Unauthorized", summary["issue"])
        self.assertEqual(summary["queued_count"], 1)

    def test_proactive_v2_snapshot_reports_suppressed_and_top_items(self):
        now = bot.datetime.now(bot.SGT)
        fake_queue = [
            {"title": "Top item", "score": 88, "priority": "high", "suppressed": False, "feedback_bias": 2},
            {"title": "Muted item", "score": 61, "priority": "medium", "suppressed": True, "feedback_bias": -2},
        ]
        with patch("bot.build_proactive_v2_queue", return_value=fake_queue):
            snapshot = bot.build_proactive_v2_snapshot(now=now, limit=3)

        self.assertEqual(snapshot["ready_count"], 1)
        self.assertEqual(snapshot["suppressed_count"], 1)
        self.assertEqual(snapshot["top"][0]["title"], "Top item")
        self.assertTrue(snapshot["changed"])

    def test_proactive_v2_snapshot_keeps_digest_out_of_priority_queue(self):
        now = bot.datetime.now(bot.SGT)
        fake_queue = [
            {"family": "digest", "title": "Morning digest", "score": 78, "priority": "medium", "suppressed": False},
            {"family": "task", "title": "Task", "score": 80, "priority": "high", "suppressed": False},
        ]
        with patch("bot.build_proactive_v2_queue", return_value=fake_queue):
            snapshot = bot.build_proactive_v2_snapshot(now=now, limit=3)

        self.assertEqual([item["title"] for item in snapshot["top"]], ["Task"])

    def test_curated_digest_entries_rank_diverse_topics(self):
        fake_topics = [("SG Education", "edu"), ("AI", "ai")]

        def fake_google_news(query, max_items=4):
            if query == "edu":
                return [
                    {"title": "MOE policy update for schools", "url": "https://example.com/edu-1", "source": "CNA"},
                    {"title": "Extra school explainer", "url": "https://example.com/edu-2", "source": "ST"},
                ]
            return [
                {"title": "AI developer release notes", "url": "https://example.com/ai-1", "source": "The Verge"},
                {"title": "AI listicle filler", "url": "https://example.com/ai-2", "source": "Blog"},
            ]

        with patch("bot._news_topics", return_value=fake_topics), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(limit=2, fetch_limit=2, record=False)

        self.assertEqual(len(entries), 2)
        self.assertEqual({entry["label"] for entry in entries}, {"SG Education", "AI"})
        self.assertTrue(all(entry.get("why") for entry in entries))

    def test_curated_digest_skips_interest_slots_without_relevant_items(self):
        fake_topics = [
            ("SG Education", "edu"),
            ("AI", "ai"),
            ("🏎️ F1", "f1"),
            ("⚽ Liverpool / EPL", "lfc"),
            ("Developer", "dev"),
        ]

        def fake_google_news(query, max_items=4):
            return [{
                "title": "Ordinary market update with no matching radar term",
                "url": f"https://example.com/{query}",
                "source": "Example",
            }]

        with patch("bot._news_topics", return_value=fake_topics), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(limit=4, fetch_limit=2, record=False)

        labels = [entry["label"] for entry in entries]
        self.assertNotIn("🏎️ F1", labels)
        self.assertNotIn("⚽ Liverpool / EPL", labels)

    def test_curated_digest_skips_seen_f1_when_no_fresh_f1_item_available(self):
        f1_item = {"title": "F1 paddock update", "url": "https://example.com/f1", "source": "Example"}
        seen_key = search_service.news_item_key(f1_item)

        def fake_google_news(query, max_items=4):
            if query == "f1":
                return [f1_item]
            return [{
                "title": f"{query} policy update today",
                "url": f"https://example.com/{query}",
                "source": "Example",
            }]

        with patch("bot._news_topics", return_value=[("SG Education", "edu"), ("AI", "ai"), ("🏎️ F1", "f1")]), \
             patch("bot._recent_news_digest_keys", return_value={seen_key}), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(limit=3, fetch_limit=2, record=False)

        self.assertNotIn("🏎️ F1", [entry["label"] for entry in entries])

    def test_curated_digest_skips_topic_when_feed_returns_no_items(self):
        with patch("bot._news_topics", return_value=[("SG Education", "edu"), ("🏎️ F1", "f1")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=lambda query, max_items=4: [] if query == "f1" else [{
                 "title": "MOE policy update today",
                 "url": "https://example.com/edu",
                 "source": "Example",
        }]):
            entries = bot.build_curated_digest_entries(limit=2, fetch_limit=2, record=False)

        self.assertNotIn("🏎️ F1", [entry["label"] for entry in entries])

    def test_curated_digest_rejects_stale_general_news_items(self):
        def fake_google_news(query, max_items=4):
            return [
                {
                    "title": "Singapore SMEs navigate pandemic support schemes - CNA",
                    "url": "https://example.com/cna-2020",
                    "source": "CNA",
                    "published": "Mon, 20 Apr 2020 02:00:00 GMT",
                },
                {
                    "title": "Singapore businesses get new grant support today",
                    "url": "https://example.com/sg-current",
                    "source": "Business Times",
                    "published": "Tue, 12 May 2026 01:00:00 GMT",
                },
            ]

        with patch("bot._news_topics", return_value=[("SG News", "sg")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 12, 9, 0)),
                limit=1,
                fetch_limit=2,
                record=False,
            )

        self.assertEqual(entries[0]["item"]["url"], "https://example.com/sg-current")

    def test_curated_digest_returns_empty_when_only_stale_general_news_exists(self):
        def fake_google_news(query, max_items=4):
            return [{
                "title": "Old CNA explainer from 2020",
                "url": "https://example.com/cna-2020",
                "source": "CNA",
                "published": "Mon, 20 Apr 2020 02:00:00 GMT",
            }]

        with patch("bot._news_topics", return_value=[("SG News", "sg")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 12, 9, 0)),
                limit=1,
                fetch_limit=2,
                record=False,
            )

        self.assertEqual(entries, [])

    def test_google_news_ranking_prefers_recent_f1_update_over_stale_item(self):
        now = datetime(2026, 5, 12, 9, 0, tzinfo=timezone.utc)
        stale = {
            "title": "George Russell and Kimi Antonelli confirmed as Mercedes line-up",
            "url": "https://example.com/f1-old",
            "source": "Formula 1",
            "published": "Wed, 15 Oct 2025 13:00:00 GMT",
        }
        fresh = {
            "title": "Kimi Antonelli Mercedes form continues after Miami Grand Prix",
            "url": "https://example.com/f1-fresh",
            "source": "Example",
            "published": "Mon, 11 May 2026 10:00:00 GMT",
        }

        ranked = search_service._rank_news_items([stale, fresh], now=now)

        self.assertEqual(ranked[0]["url"], "https://example.com/f1-fresh")
        self.assertGreater(
            search_service.news_quality_score(fresh, now=now),
            search_service.news_quality_score(stale, now=now),
        )

    def test_curated_digest_rejects_generic_epl_for_liverpool_slot(self):
        def fake_google_news(query, max_items=4):
            if query == "lfc":
                return [{
                    "title": "Premier League title race takes another twist",
                    "url": "https://example.com/epl",
                    "source": "Example",
                    "published": "Sun, 10 May 2026 06:00:00 GMT",
                }]
            return [{
                "title": "MOE policy update today",
                "url": "https://example.com/edu",
                "source": "Example",
            }]

        with patch("bot._news_topics", return_value=[("SG Education", "edu"), ("⚽ Liverpool / EPL", "lfc")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 10, 8, 0)),
                limit=2,
                fetch_limit=2,
                record=False,
            )

        self.assertNotIn("⚽ Liverpool / EPL", [entry["label"] for entry in entries])

    def test_curated_digest_prioritises_recent_liverpool_match_report(self):
        def fake_google_news(query, max_items=4):
            if query == "lfc":
                return [
                    {
                        "title": "Liverpool transfer rumour roundup",
                        "url": "https://example.com/old-transfer",
                        "source": "Example",
                        "published": "Wed, 06 May 2026 08:00:00 GMT",
                    },
                    {
                        "title": "Liverpool 1-1 Chelsea: match report and player ratings",
                        "url": "https://example.com/lfc-chelsea",
                        "source": "Example",
                        "published": "Sat, 09 May 2026 15:00:00 GMT",
                    },
                ]
            return []

        with patch("bot._news_topics", return_value=[("⚽ Liverpool / EPL", "lfc")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 10, 8, 0)),
                limit=1,
                fetch_limit=2,
                record=False,
            )

        self.assertEqual(entries[0]["item"]["title"], "Liverpool 1-1 Chelsea: match report and player ratings")
        self.assertIn("Liverpool match", entries[0]["why"])

    def test_curated_digest_accepts_ai_tools_radar_items(self):
        def fake_google_news(query, max_items=4):
            return [{
                "title": "Claude and Kimi ship new coding agent updates",
                "url": "https://example.com/ai-tools",
                "source": "Example",
                "published": "Sun, 10 May 2026 01:00:00 GMT",
            }]

        with patch("bot._news_topics", return_value=[("AI Tools", "ai")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 10, 8, 0)),
                limit=1,
                fetch_limit=2,
                record=False,
            )

        self.assertEqual(entries[0]["label"], "AI Tools")
        self.assertIn("AI tool radar", entries[0]["why"])

    def test_curated_digest_accepts_nothing_and_teenage_engineering_items(self):
        def fake_google_news(query, max_items=4):
            if query == "nothing":
                return [{
                    "title": "Nothing Phone roadmap and Nothing OS beta features detailed",
                    "url": "https://example.com/nothing",
                    "source": "Example",
                    "published": "Sat, 09 May 2026 23:00:00 GMT",
                }]
            return [{
                "title": "Teenage Engineering OP-XY firmware update reviewed",
                "url": "https://example.com/te",
                "source": "Example",
                "published": "Sat, 09 May 2026 22:00:00 GMT",
            }]

        with patch("bot._news_topics", return_value=[("Nothing Products", "nothing"), ("Teenage Engineering", "te")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 10, 8, 0)),
                limit=2,
                fetch_limit=2,
                record=False,
            )

        labels = [entry["label"] for entry in entries]
        self.assertIn("Nothing Products", labels)
        self.assertIn("Teenage Engineering", labels)

    def test_news_topics_merge_configured_with_builtin_radar(self):
        configured = [{"label": "Custom Topic", "query": "custom query"}]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_news_topics", return_value=configured),
            patch.object(bot, "_ownership_news_topics", return_value=[]),
        ):
            topics = bot._news_topics()

        labels = [label for label, _ in topics]
        self.assertIn("Custom Topic", labels)
        self.assertIn("🤖 Android", labels)
        self.assertIn("🧠 Codex / Claude / Gemini / Kimi", labels)

    def test_curated_digest_can_include_x_social_lead_for_live_topic(self):
        def fake_web_search(query, max_results=5):
            self.assertIn("site:x.com", query)
            return [{
                "title": "Android 17 beta update rolling out to Pixel users",
                "url": "https://x.com/android/status/123",
                "description": "Fresh Android update thread.",
            }]

        with (
            patch("bot._news_topics", return_value=[("🤖 Android", "Android 17 Pixel beta update")]),
            patch("bot._recent_news_digest_keys", return_value=set()),
            patch("search_service.google_news", return_value=[]),
            patch("search_service.web_search", side_effect=fake_web_search),
            patch.dict(os.environ, {
                "HIRA_DIGEST_SOCIAL_SEARCH": "1",
                "HIRA_DIGEST_SOCIAL_DOMAINS": "x.com",
                "HIRA_DIGEST_SOCIAL_TOPIC_LIMIT": "1",
            }),
        ):
            entries = bot.build_curated_digest_entries(
                now=bot.SGT.localize(datetime(2026, 5, 13, 8, 0)),
                limit=1,
                fetch_limit=2,
                record=False,
            )

        self.assertEqual(entries[0]["label"], "🤖 Android")
        self.assertEqual(entries[0]["item"]["source"], "Social: x.com")
        self.assertTrue(entries[0]["item"]["social"])

    def test_classops_date_folder_parses_singapore_day_month_year(self):
        parsed = dropbox_service.parse_classops_date_folder("24/2/26 Peribahasa")
        self.assertTrue(parsed["matched"])
        self.assertEqual(parsed["date"], "2026-02-24")
        self.assertEqual(parsed["label"], "Peribahasa")

    def test_classops_date_folder_handles_plain_date_only(self):
        parsed = dropbox_service.parse_classops_date_folder("5-10-2026")
        self.assertTrue(parsed["matched"])
        self.assertEqual(parsed["date"], "2026-10-05")
        self.assertEqual(parsed["label"], "")

    def test_classops_nested_date_parts_parse_like_dropbox_path(self):
        parsed, consumed = dropbox_service._date_info_from_folder_parts(["24", "2", "26", "Peribahasa"])
        self.assertEqual(consumed, 3)
        self.assertEqual(parsed["date"], "2026-02-24")

    def test_classops_date_folder_parses_dropbox_colon_separator(self):
        parsed = dropbox_service.parse_classops_date_folder("10:2:26")
        self.assertTrue(parsed["matched"])
        self.assertEqual(parsed["date"], "2026-02-10")

    def test_classops_manifest_enrichment_marks_collection_candidates(self):
        manifest = {
            "ok": True,
            "file_count": 2,
            "classes": [{
                "class": "2G3",
                "file_count": 2,
                "folder_count": 1,
                "folders": [{
                    "folder": "24:2:26",
                    "date": "2026-02-24",
                    "topic": "",
                    "files": [
                        {"name": "latihan peribahasa collect next lesson.pdf", "path": "2G3/24:2:26/latihan.pdf"},
                        {"name": "teacher answer scheme.pdf", "path": "2G3/24:2:26/answers.pdf"},
                    ],
                }],
            }],
        }
        enriched = dropbox_service.enrich_classops_manifest(manifest)
        class_item = enriched["classes"][0]
        self.assertEqual(enriched["summary"]["collection_candidate_count"], 1)
        self.assertEqual(enriched["summary"]["content_item_count"], 1)
        self.assertEqual(class_item["lesson_count"], 1)
        self.assertEqual(class_item["collection_candidates"][0]["collection"]["due"], "next_lesson")
        self.assertEqual(class_item["content_items"][0]["title"], "Latihan Peribahasa")
        self.assertEqual(class_item["content_items"][0]["date"], "2026-02-24")
        self.assertEqual(class_item["content_items"][0]["purpose_id"], "submission_task")
        self.assertTrue(class_item["content_items"][0]["trackable"])

    def test_classops_manifest_distinguishes_lesson_folder_content_purposes(self):
        manifest = {
            "ok": True,
            "file_count": 5,
            "classes": [{
                "class": "2G3",
                "file_count": 5,
                "folder_count": 1,
                "folders": [{
                    "folder": "24:2:26 Peribahasa",
                    "date": "2026-02-24",
                    "topic": "Peribahasa",
                    "files": [
                        {"name": "peribahasa minisite.html", "path": "2G3/24:2:26/peribahasa.html"},
                        {"name": "nota murid peribahasa.pdf", "path": "2G3/24:2:26/nota.pdf"},
                        {"name": "latihan peribahasa worksheet.docx", "path": "2G3/24:2:26/worksheet.docx"},
                        {"name": "slaid peribahasa.pptx", "path": "2G3/24:2:26/slides.pptx"},
                        {"name": "karangan submit next lesson.pdf", "path": "2G3/24:2:26/karangan.pdf"},
                    ],
                }],
            }],
        }

        enriched = dropbox_service.enrich_classops_manifest(manifest)
        items = enriched["classes"][0]["content_items"]

        self.assertEqual(
            [(item["title"], item["purpose_id"]) for item in items],
            [
                ("Peribahasa Minisite", "lesson_page"),
                ("Karangan", "submission_task"),
                ("Latihan Peribahasa Worksheet", "worksheet"),
                ("Nota Murid Peribahasa", "notes"),
                ("Slaid Peribahasa", "slides"),
            ],
        )
        self.assertEqual(items[0]["purpose_label"], "Lesson page")
        self.assertEqual(items[1]["purpose_label"], "Submission task")
        self.assertTrue(items[1]["trackable"])
        self.assertFalse(items[-1]["trackable"])

    def test_classops_content_purpose_uses_teacher_file_type_conventions(self):
        examples = [
            ({"name": "lesson-site.html", "kind": "mini-site"}, "lesson_page"),
            ({"name": "peribahasa.pdf", "kind": "pdf"}, "slides"),
            ({"name": "peribahasa.pptx", "kind": "slides"}, "slides"),
            ({"name": "latihan peribahasa.docx", "kind": "worksheet/doc"}, "worksheet"),
            ({"name": "latihan peribahasa.doc", "kind": "worksheet/doc"}, "worksheet"),
            ({"name": "karangan submit next lesson.pdf", "kind": "pdf"}, "submission_task"),
            ({"name": "nota murid.pdf", "kind": "pdf"}, "notes"),
        ]

        purposes = [
            dropbox_service.infer_content_purpose(item, dropbox_service.infer_collection_hint(item["name"]))["id"]
            for item, _ in examples
        ]

        self.assertEqual(purposes, [expected for _, expected in examples])

    def test_classops_manifest_content_items_sort_newest_first(self):
        manifest = {
            "ok": True,
            "file_count": 3,
            "classes": [{
                "class": "2G3",
                "file_count": 3,
                "folder_count": 3,
                "folders": [
                    {
                        "folder": "10:3:26",
                        "date": "2026-03-10",
                        "topic": "",
                        "files": [{"name": "karangan.pdf", "path": "2G3/10:3:26/karangan.pdf"}],
                    },
                    {
                        "folder": "24:2:26",
                        "date": "2026-02-24",
                        "topic": "",
                        "files": [{"name": "peribahasa.pdf", "path": "2G3/24:2:26/peribahasa.pdf"}],
                    },
                    {
                        "folder": "5:3:26",
                        "date": "2026-03-05",
                        "topic": "",
                        "files": [{"name": "kefahaman.pdf", "path": "2G3/5:3:26/kefahaman.pdf"}],
                    },
                ],
            }],
        }

        enriched = dropbox_service.enrich_classops_manifest(manifest)
        dates = [item["date"] for item in enriched["classes"][0]["content_items"]]

        self.assertEqual(dates, ["2026-03-10", "2026-03-05", "2026-02-24"])

    def test_classops_manifest_content_items_sort_cross_year_newest_first(self):
        items = [
            {"title": "Kefahaman HBL March", "date": "27/02/25", "path": "2G3/27:2:25/hbl.pdf"},
            {"title": "ADAB", "date": "08/01/26", "path": "2G3/8:1:26/adab.pdf"},
            {"title": "Older", "date": "2025-01-08", "path": "2G3/8:1:25/older.pdf"},
        ]

        sorted_items = dropbox_service.sort_classops_content_items(items)

        self.assertEqual([item["title"] for item in sorted_items], ["ADAB", "Kefahaman HBL March", "Older"])

    def test_classops_manifest_places_undated_folders_and_content_at_bottom(self):
        manifest = {
            "ok": True,
            "file_count": 3,
            "classes": [{
                "class": "2G3",
                "file_count": 3,
                "folder_count": 3,
                "folders": [
                    {
                        "folder": "Peribahasa no date",
                        "date": "",
                        "topic": "Peribahasa no date",
                        "files": [{"name": "simpulan bahasa.pdf", "path": "2G3/Peribahasa no date/simpulan.pdf"}],
                    },
                    {
                        "folder": "10:3:26",
                        "date": "2026-03-10",
                        "topic": "",
                        "files": [{"name": "karangan.pdf", "path": "2G3/10:3:26/karangan.pdf"}],
                    },
                    {
                        "folder": "24:2:26",
                        "date": "2026-02-24",
                        "topic": "",
                        "files": [{"name": "peribahasa.pdf", "path": "2G3/24:2:26/peribahasa.pdf"}],
                    },
                ],
            }],
        }

        enriched = dropbox_service.enrich_classops_manifest(manifest)
        class_item = enriched["classes"][0]

        self.assertEqual([folder["folder"] for folder in class_item["folders"]], ["24:2:26", "10:3:26", "Peribahasa no date"])
        self.assertEqual([item["date"] for item in class_item["content_items"]], ["2026-03-10", "2026-02-24", ""])
        self.assertTrue(class_item["content_items"][-1]["date_missing"])
        self.assertEqual(class_item["undated_folder_count"], 1)
        self.assertEqual(enriched["summary"]["undated_folder_count"], 1)

    def test_classops_filing_title_uses_minisite_title(self):
        title = dropbox_service._html_title(b"<html><head><title>Fallback</title></head><body><h1>Nota - Masa Senggang</h1></body></html>")

        self.assertEqual(title, "Nota - Masa Senggang")

    def test_classops_filing_title_cleans_filename_noise(self):
        title = dropbox_service.infer_filing_title_from_filename("2G3_latihan peribahasa collect next lesson.pdf")

        self.assertEqual(title, "Latihan Peribahasa")

    def test_classops_filing_title_does_not_download_by_default(self):
        item = {
            "name": "watak-melayu.html",
            "dropbox_path": "/2G3/10:3:26/watak-melayu.html",
            "size": 2048,
        }

        with patch.dict(os.environ, {"DROPBOX_CLASSOPS_INSPECT_TITLES": ""}, clear=False), \
             patch.object(dropbox_service, "_download_file", side_effect=AssertionError("should not download")):
            title = dropbox_service.infer_filing_title(item)

        self.assertEqual(title, "Watak Melayu")

    def test_classops_dropbox_file_link_uses_temporary_link(self):
        with patch.dict(os.environ, {"DROPBOX_CLASSOPS_ROOT": "/ClassOps"}, clear=False), \
             patch.object(dropbox_service, "_post", return_value={"link": "https://tmp.dropbox/link"}) as post_mock:
            link = dropbox_service.get_file_link("2G3/24:2:26/nota.pdf")

        post_mock.assert_called_once_with("/files/get_temporary_link", {"path": "/ClassOps/2G3/24:2:26/nota.pdf"})
        self.assertEqual(link["url"], "https://tmp.dropbox/link")
        self.assertEqual(link["kind"], "temporary_link")

    def test_classops_content_override_persists_title_and_hidden_flag(self):
        store = {}

        def fake_set(key, value):
            store[key] = value

        with patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key)), \
             patch.object(bot.gs, "set_config", side_effect=fake_set):
            override = bot.gs.save_classops_content_override(
                "2G3/24:2:26/latihan.pdf",
                title="Latihan Peribahasa",
                hidden=True,
                no_submission_needed=True,
                purpose_id="worksheet",
            )
            overrides = bot.gs.get_classops_content_overrides()

        self.assertTrue(override["hidden"])
        self.assertTrue(override["no_submission_needed"])
        self.assertEqual(override["purpose_id"], "worksheet")
        self.assertEqual(overrides["2G3/24:2:26/latihan.pdf"]["title"], "Latihan Peribahasa")
        self.assertTrue(overrides["2G3/24:2:26/latihan.pdf"]["hidden"])
        self.assertTrue(overrides["2G3/24:2:26/latihan.pdf"]["no_submission_needed"])
        self.assertEqual(overrides["2G3/24:2:26/latihan.pdf"]["purpose_id"], "worksheet")

    def test_classops_content_overrides_rename_and_hide_manifest_items(self):
        manifest = {
            "summary": {"content_item_count": 2},
            "classes": [{
                "class": "2G3",
                "content_item_count": 2,
                "content_items": [
                    {"path": "2G3/24:2:26/raw.pdf", "title": "Raw", "date": "2026-02-24"},
                    {"path": "2G3/25:2:26/noise.pdf", "title": "Noise", "date": "2026-02-25"},
                ],
            }],
        }
        ledger = {
            "content_overrides": {
                "2G3/24:2:26/raw.pdf": {"title": "Nota - Masa Senggang"},
                "2G3/25:2:26/noise.pdf": {"hidden": True},
            }
        }

        updated = web_app._classops_apply_content_overrides(manifest, ledger)

        self.assertEqual(updated["summary"]["content_item_count"], 1)
        self.assertEqual(updated["classes"][0]["content_item_count"], 1)
        self.assertEqual(updated["classes"][0]["content_items"][0]["title"], "Nota - Masa Senggang")
        self.assertTrue(updated["classes"][0]["content_items"][0]["title_overridden"])

    def test_classops_content_overrides_can_correct_purpose(self):
        manifest = {
            "summary": {"content_item_count": 1},
            "classes": [{
                "class": "2G3",
                "content_item_count": 1,
                "content_items": [
                    {
                        "path": "2G3/24:2:26/raw.pdf",
                        "title": "Raw",
                        "date": "2026-02-24",
                        "purpose_id": "slides",
                        "purpose_label": "Slides",
                        "purpose_tone": "resource",
                        "purpose_rank": 50,
                        "trackable": False,
                    },
                ],
            }],
        }
        ledger = {"content_overrides": {"2G3/24:2:26/raw.pdf": {"purpose_id": "worksheet"}}}

        updated = web_app._classops_apply_content_overrides(manifest, ledger)
        item = updated["classes"][0]["content_items"][0]

        self.assertEqual(item["purpose_id"], "worksheet")
        self.assertEqual(item["purpose_label"], "Worksheet")
        self.assertTrue(item["trackable"])
        self.assertTrue(item["purpose_overridden"])

    def test_classops_content_overrides_mark_no_submission_needed(self):
        manifest = {
            "summary": {"content_item_count": 1},
            "classes": [{
                "class": "2G3",
                "content_item_count": 1,
                "content_items": [
                    {"path": "2G3/24:2:26/raw.pdf", "title": "Raw", "date": "2026-02-24"},
                ],
            }],
        }
        ledger = {
            "content_overrides": {
                "2G3/24:2:26/raw.pdf": {"no_submission_needed": True},
            }
        }

        updated = web_app._classops_apply_content_overrides(manifest, ledger)

        self.assertEqual(updated["classes"][0]["content_item_count"], 1)
        self.assertTrue(updated["classes"][0]["content_items"][0]["no_submission_needed"])

    def test_classops_content_overrides_keep_content_items_newest_first(self):
        manifest = {
            "summary": {"content_item_count": 3},
            "classes": [{
                "class": "2G3",
                "content_item_count": 3,
                "content_items": [
                    {"path": "2G3/10:3:26/raw.pdf", "title": "Raw", "date": "2026-03-10"},
                    {"path": "2G3/24:2:26/nota.pdf", "title": "Nota", "date": "2026-02-24"},
                    {"path": "2G3/5:3:26/kefahaman.pdf", "title": "Kefahaman", "date": "2026-03-05"},
                ],
            }],
        }
        ledger = {"content_overrides": {"2G3/10:3:26/raw.pdf": {"title": "Karangan"}}}

        updated = web_app._classops_apply_content_overrides(manifest, ledger)
        dates = [item["date"] for item in updated["classes"][0]["content_items"]]

        self.assertEqual(dates, ["2026-03-10", "2026-03-05", "2026-02-24"])

    def test_classops_students_filters_combined_teacher_roster_by_class(self):
        classlists = [{
            "grouping": "Herwanto MTL",
            "sheet_title": "Combined",
            "spreadsheet_title": "2026 MTL",
            "students": [
                {"no": "1", "class": "1G2", "name": "Aisyah"},
                {"no": "2", "class": "Secondary 2G3 ML", "name": "Bala"},
                {"no": "3", "class": "3G3", "name": "Chen"},
                {"no": "4", "class": "4NT", "name": "Danish"},
            ],
        }]

        with patch.object(bot.gs, "get_mtl_classlists", return_value=classlists) as classlists_mock:
            students = bot.gs.get_classops_students("2G3")

        classlists_mock.assert_called_once_with(
            teacher_query="HERWANTO",
            class_query="",
            include_students=True,
            include_scores=False,
        )
        self.assertEqual([student["name"] for student in students], ["Bala"])
        self.assertEqual(students[0]["class"], "Secondary 2G3 ML")

    def test_classops_students_allows_single_class_sheet_without_class_column(self):
        classlists = [{
            "grouping": "2G3 ML",
            "sheet_title": "2G3",
            "spreadsheet_title": "2026 MTL",
            "students": [
                {"no": "1", "class": "", "name": "Bala"},
                {"no": "2", "class": "", "name": "Siti"},
            ],
        }]

        with patch.object(bot.gs, "get_mtl_classlists", return_value=classlists):
            students = bot.gs.get_classops_students("2G3")

        self.assertEqual([student["name"] for student in students], ["Bala", "Siti"])

    def test_classops_students_keeps_form_classes_when_sheet_is_mtl_group(self):
        classlists = [{
            "grouping": "2G3",
            "sheet_title": "2G3 MTL",
            "spreadsheet_title": "2026 MTL",
            "students": [
                {"no": "1", "class": "S2-AN", "name": "Amelia"},
                {"no": "2", "class": "S2-BE", "name": "Aulia"},
                {"no": "3", "class": "S2-CO", "name": "Syuhrah"},
            ],
        }, {
            "grouping": "3G3",
            "sheet_title": "3G3 MTL",
            "spreadsheet_title": "2026 MTL",
            "students": [
                {"no": "1", "class": "S3-AN", "name": "Umaira"},
            ],
        }]

        with patch.object(bot.gs, "get_mtl_classlists", return_value=classlists):
            students = bot.gs.get_classops_students("2G3")

        self.assertEqual([student["name"] for student in students], ["Amelia", "Aulia", "Syuhrah"])
        self.assertEqual([student["class"] for student in students], ["S2-AN", "S2-BE", "S2-CO"])
        self.assertEqual(bot.gs.NAVAL_BASE_2026_FORM_CLASSES["AN"], "Anchor")

    def test_classops_assignment_ledger_persists_tracking(self):
        store = {}

        def fake_set(key, value):
            store[key] = value

        with patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key)), \
             patch.object(bot.gs, "set_config", side_effect=fake_set):
            assignment = bot.gs.save_classops_assignment(
                class_name="2g3",
                lesson_date="2026-05-10",
                topic="Lisan",
                folder="10:5:26",
                assignment_title="Latihan Lisan",
                absent=["Ali Bin Ahmad"],
                submitted=["Siti Aminah", "Siti Aminah"],
                non_submitted=["Kumar Das"],
            )
            ledger = bot.gs.get_classops_ledger()

        self.assertEqual(assignment["class_name"], "2G3")
        self.assertEqual(assignment["submitted"], ["Siti Aminah"])
        self.assertEqual(assignment["non_submitted"], ["Kumar Das"])
        self.assertIn("2G3", ledger["classes"])
        self.assertEqual(ledger["classes"]["2G3"]["assignments"][0]["assignment_title"], "Latihan Lisan")

    def test_classops_assignment_updates_same_source_path_when_cleared(self):
        store = {}

        def fake_set(key, value):
            store[key] = value

        with patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key)), \
             patch.object(bot.gs, "set_config", side_effect=fake_set):
            bot.gs.save_classops_content_override(
                "2G3/10:5:26/lisan.pdf",
                no_submission_needed=True,
            )
            first = bot.gs.save_classops_assignment(
                class_name="2g3",
                lesson_date="2026-05-10",
                topic="Lisan",
                folder="10:5:26",
                source_path="2G3/10:5:26/lisan.pdf",
                assignment_title="Latihan Lisan",
                non_submitted=["Kumar Das"],
            )
            second = bot.gs.save_classops_assignment(
                class_name="2g3",
                lesson_date="2026-05-10",
                topic="Lisan",
                folder="10:5:26",
                source_path="2G3/10:5:26/lisan.pdf",
                assignment_title="Latihan Lisan",
                non_submitted=[],
            )
            ledger = bot.gs.get_classops_ledger()
            overrides = bot.gs.get_classops_content_overrides()

        assignments = ledger["classes"]["2G3"]["assignments"]
        self.assertEqual(len(assignments), 1)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(assignments[0]["non_submitted"], [])
        self.assertEqual(assignments[0]["tracking_mode"], "non_submission_list")
        self.assertFalse(overrides["2G3/10:5:26/lisan.pdf"]["no_submission_needed"])

    def test_classops_assignment_delete_by_source_path_removes_tracked_work(self):
        store = {}

        def fake_set(key, value):
            store[key] = value

        with patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key)), \
             patch.object(bot.gs, "set_config", side_effect=fake_set):
            bot.gs.save_classops_assignment(
                class_name="2g3",
                lesson_date="2026-05-10",
                topic="Lisan",
                folder="10:5:26",
                source_path="2G3/10:5:26/lisan.pdf",
                assignment_title="Latihan Lisan",
                non_submitted=["Kumar Das"],
            )
            deleted = bot.gs.delete_classops_assignment(
                class_name="2g3",
                source_path="2G3/10:5:26/lisan.pdf",
            )
            ledger = bot.gs.get_classops_ledger()

        self.assertTrue(deleted["deleted"])
        self.assertEqual(deleted["deleted_count"], 1)
        self.assertEqual(ledger["classes"]["2G3"]["assignments"], [])

    def test_classops_student_report_flags_missing_and_absent_followups(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Latihan Lisan",
                        "absent": ["Ali Bin Ahmad"],
                        "non_submitted": ["Kumar Das"],
                    }]
                }
            }
        }
        students = [
            {"no": "1", "class": "2G3", "name": "Ali Bin Ahmad"},
            {"no": "2", "class": "2G3", "name": "Siti Aminah"},
            {"no": "3", "class": "2G3", "name": "Kumar Das"},
        ]

        report = web_app._classops_student_report("2G3", students, ledger)

        by_name = {student["name"]: student for student in report["students"]}
        self.assertEqual(by_name["Siti Aminah"]["submitted_count"], 1)
        self.assertEqual(by_name["Ali Bin Ahmad"]["status"], "catch up")
        self.assertEqual(by_name["Kumar Das"]["status"], "watch")
        self.assertEqual(report["concern_count"], 2)
        self.assertEqual(report["assignments"][0]["submitted_count"], 1)
        self.assertEqual(report["assignments"][0]["missing_count"], 1)

    def test_classops_student_report_flags_weekend_submission_patterns(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [
                        {
                            "id": "work-1",
                            "assignment_title": "Latihan 1",
                            "lesson_date": "2026-05-01",
                            "collect_by": "2026-05-04",
                            "non_submitted": ["Kumar Das"],
                        },
                        {
                            "id": "work-2",
                            "assignment_title": "Latihan 2",
                            "lesson_date": "2026-05-08",
                            "collect_by": "2026-05-11",
                            "non_submitted": ["Kumar Das"],
                        },
                    ]
                }
            }
        }
        students = [
            {"no": "1", "class": "2G3", "name": "Kumar Das"},
            {"no": "2", "class": "2G3", "name": "Siti Aminah"},
        ]

        report = web_app._classops_student_report("2G3", students, ledger, today=date(2026, 5, 12))

        by_name = {student["name"]: student for student in report["students"]}
        self.assertEqual(by_name["Kumar Das"]["status"], "follow up")
        self.assertEqual(by_name["Kumar Das"]["timing_patterns"]["after_weekend"], 2)
        self.assertIn("Pattern appears after weekends", by_name["Kumar Das"]["risk_reasons"])
        self.assertTrue(any(insight["kind"] == "timing_pattern" for insight in report["insights"]))

    def test_classops_student_report_flags_public_holiday_timing(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Latihan Hari Raya Haji",
                        "lesson_date": "2026-05-26",
                        "collect_by": "2026-05-28",
                        "non_submitted": ["Kumar Das"],
                    }]
                }
            }
        }
        students = [{"no": "1", "class": "2G3", "name": "Kumar Das"}]

        report = web_app._classops_student_report("2G3", students, ledger, today=date(2026, 5, 29))

        student = report["students"][0]
        self.assertEqual(student["timing_patterns"]["after_public_holiday"], 1)
        self.assertIn("Watch after school/public holiday", student["risk_reasons"])
        self.assertEqual(report["assignments"][0]["timing_context"][0]["key"], "after_public_holiday")

    def test_classops_student_report_ignores_cleared_assignment_for_gap(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Old work",
                        "lesson_date": "2026-04-01",
                        "non_submitted": [],
                    }]
                }
            }
        }
        students = [{"no": "1", "class": "2G3", "name": "Siti Aminah"}]

        report = web_app._classops_student_report("2G3", students, ledger, today=date(2026, 5, 10))

        gap = [insight for insight in report["insights"] if insight["kind"] == "assignment_gap"][0]
        self.assertIsNone(gap["days"])
        self.assertIn("no tracked assignments", gap["title"])

    def test_classops_empty_non_submission_list_does_not_mark_everyone_missing(self):
        ledger = {
            "classes": {
                "3G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Latihan Lisan",
                        "lesson_date": "2026-05-11",
                        "non_submitted": [],
                    }]
                }
            }
        }
        students = [
            {"no": "1", "class": "3G3", "name": "Umaira Alfrina Binte Johari"},
            {"no": "2", "class": "3G3", "name": "Nina Ariqa Binte Andywira"},
        ]

        report = web_app._classops_student_report("3G3", students, ledger, today=date(2026, 5, 12))

        self.assertEqual(report["assignment_count"], 0)
        self.assertEqual(report["open_non_submission_count"], 0)
        self.assertEqual(report["concern_count"], 0)
        self.assertEqual(report["assignments"], [])
        self.assertTrue(all(student["missing_count"] == 0 for student in report["students"]))

    def test_classops_student_report_flags_marks_watch(self):
        students = [
            {"no": "1", "class": "2G3", "name": "Siti Aminah", "fields": {"WA1 %": "72"}},
            {"no": "2", "class": "2G3", "name": "Kumar Das", "fields": {"WA1 %": "45"}},
        ]

        report = web_app._classops_student_report("2G3", students, {"classes": {}})

        by_name = {student["name"]: student for student in report["students"]}
        self.assertEqual(by_name["Kumar Das"]["status"], "marks watch")
        self.assertIn("Marks watch", by_name["Kumar Das"]["risk_reasons"][0])
        self.assertTrue(any(insight["kind"] == "marks_watch" for insight in report["insights"]))
        self.assertTrue(any(item["title"].startswith("Marks watch") for item in report["priority_items"]))
        self.assertTrue(any(group["key"] == "reteach" for group in report["feed_forward_groups"]))

    def test_classops_student_report_builds_student_timeline_and_groups(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Latihan Lisan",
                        "lesson_date": "2026-05-11",
                        "collect_by": "2026-05-12",
                        "non_submitted": ["Kumar Das"],
                    }]
                }
            }
        }
        students = [
            {"no": "1", "class": "2G3", "name": "Siti Aminah", "fields": {"WA1 %": "82"}},
            {"no": "2", "class": "2G3", "name": "Kumar Das", "fields": {"WA1 %": "55"}},
        ]

        report = web_app._classops_student_report("2G3", students, ledger, today=date(2026, 5, 12))

        by_name = {student["name"]: student for student in report["students"]}
        self.assertEqual(by_name["Kumar Das"]["timeline"][0]["status"], "missing")
        self.assertEqual(by_name["Siti Aminah"]["timeline"][0]["status"], "submitted")
        practice = [group for group in report["feed_forward_groups"] if group["key"] == "practice"][0]
        self.assertEqual(practice["students"][0]["name"], "Kumar Das")

    def test_classops_student_report_cleared_source_path_is_not_active_tracking(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Latihan Lisan",
                        "lesson_date": "2026-05-11",
                        "source_path": "2G3/11:5:26/lisan.pdf",
                        "tracking_mode": "non_submission_list",
                        "non_submitted": [],
                    }]
                }
            }
        }
        students = [
            {"no": "1", "class": "2G3", "name": "Siti Aminah"},
            {"no": "2", "class": "2G3", "name": "Kumar Das"},
        ]

        report = web_app._classops_student_report("2G3", students, ledger, today=date(2026, 5, 12))

        self.assertEqual(report["assignment_count"], 0)
        self.assertEqual(report["open_non_submission_count"], 0)
        self.assertEqual(report["assignments"], [])
        self.assertTrue(all(student["submitted_count"] == 0 for student in report["students"]))
        self.assertTrue(all(student["missing_count"] == 0 for student in report["students"]))

    def test_classops_student_report_dedupes_stale_legacy_record_for_same_item(self):
        ledger = {
            "classes": {
                "3G3": {
                    "assignments": [
                        {
                            "id": "legacy-work",
                            "assignment_title": "Latihan Lisan",
                            "lesson_date": "2026-05-11",
                            "folder": "11:5:26",
                            "non_submitted": ["Umaira Alfrina Binte Johari"],
                            "updated_at": "2026-05-11T08:00:00+08:00",
                        },
                        {
                            "id": "source-work",
                            "assignment_title": "Latihan Lisan",
                            "lesson_date": "2026-05-11",
                            "folder": "11:5:26",
                            "source_path": "3G3/11:5:26/lisan.pdf",
                            "tracking_mode": "non_submission_list",
                            "non_submitted": [],
                            "updated_at": "2026-05-11T14:00:00+08:00",
                        },
                    ]
                }
            }
        }
        students = [
            {"no": "1", "class": "3G3", "name": "Umaira Alfrina Binte Johari"},
            {"no": "2", "class": "3G3", "name": "Nina Ariqa Binte Andywira"},
        ]

        report = web_app._classops_student_report("3G3", students, ledger, today=date(2026, 5, 12))

        self.assertEqual(report["assignment_count"], 0)
        self.assertEqual(report["open_non_submission_count"], 0)
        self.assertEqual(report["assignments"], [])
        self.assertTrue(all(student["missing_count"] == 0 for student in report["students"]))

    def test_classops_non_submission_count_stays_cumulative_across_files(self):
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [
                        {
                            "id": "work-1",
                            "assignment_title": "Latihan Lisan",
                            "lesson_date": "2026-05-11",
                            "source_path": "2G3/11:5:26/lisan.pdf",
                            "tracking_mode": "non_submission_list",
                            "non_submitted": [],
                        },
                        {
                            "id": "work-2",
                            "assignment_title": "Karangan",
                            "lesson_date": "2026-05-12",
                            "source_path": "2G3/12:5:26/karangan.pdf",
                            "tracking_mode": "non_submission_list",
                            "non_submitted": ["Kumar Das"],
                        },
                        {
                            "id": "work-3",
                            "assignment_title": "Peribahasa",
                            "lesson_date": "2026-05-13",
                            "source_path": "2G3/13:5:26/peribahasa.pdf",
                            "tracking_mode": "non_submission_list",
                            "non_submitted": ["Kumar Das"],
                        },
                    ]
                }
            }
        }
        students = [
            {"no": "1", "class": "2G3", "name": "Siti Aminah"},
            {"no": "2", "class": "2G3", "name": "Kumar Das"},
        ]

        report = web_app._classops_student_report("2G3", students, ledger, today=date(2026, 5, 14))

        by_name = {student["name"]: student for student in report["students"]}
        self.assertEqual(by_name["Kumar Das"]["submitted_count"], 0)
        self.assertEqual(by_name["Kumar Das"]["missing_count"], 2)
        self.assertEqual(report["assignment_count"], 2)
        self.assertEqual(report["open_non_submission_count"], 2)
        self.assertEqual([event["status"] for event in by_name["Kumar Das"]["timeline"]], ["missing", "missing"])

    def test_classops_reflection_worksheet_uses_lesson_and_watchlist(self):
        report = {
            "watchlist": [{
                "name": "Kumar Das",
                "risk_reasons": ["Marks watch: WA1 % 45"],
            }]
        }

        worksheet = classops_ai.build_lesson_reflection_worksheet(
            "2G3",
            {"title": "Peribahasa", "date": "2026-05-11", "path": "2G3/11:5:26/peribahasa.pdf"},
            report,
        )

        self.assertIn("Peribahasa", worksheet["summary"])
        self.assertEqual(worksheet["source_path"], "2G3/11:5:26/peribahasa.pdf")
        teacher_prompts = worksheet["sections"][-1]["prompts"]
        self.assertTrue(any("Kumar Das" in prompt for prompt in teacher_prompts))

    def test_classops_reflection_worksheet_uses_extracted_lesson_text(self):
        worksheet = classops_ai.build_lesson_reflection_worksheet(
            "2G3",
            {
                "title": "Peribahasa",
                "date": "2026-05-11",
                "path": "2G3/11:5:26/peribahasa.pdf",
                "excerpt": "Peribahasa digunakan untuk menyampaikan nasihat. Murid perlu mengenal maksud tersirat.",
                "index_note": "PDF has 2 pages; analysed pages: 1.",
            },
            {},
        )

        self.assertTrue(worksheet["extracted"])
        self.assertIn("Peribahasa", worksheet["keywords"])
        self.assertIn("PDF has 2 pages", worksheet["source_note"])
        self.assertTrue(any("lesson evidence" in prompt.lower() for prompt in worksheet["sections"][1]["prompts"]))

    def test_classops_extract_lesson_material_downloads_supported_dropbox_file(self):
        with patch.object(web_app.dropbox, "download_file", return_value=b"%PDF") as download, \
             patch.object(web_app.docs, "extract_supported_document", return_value=("PDF", "PDF has 1 page.", "Lesson text")) as extract:
            lesson = web_app._classops_extract_lesson_material({"path": "2G3/11:5:26/peribahasa.pdf", "title": "Peribahasa"})

        download.assert_called_once_with("2G3/11:5:26/peribahasa.pdf")
        extract.assert_called_once()
        self.assertEqual(lesson["excerpt"], "Lesson text")
        self.assertEqual(lesson["document_kind"], "PDF")

    def test_classops_status_summary_rolls_up_hira_panel_metrics(self):
        today = datetime.now(web_app.bot.SGT).strftime("%Y-%m-%d")
        ledger = {
            "classes": {
                "2G3": {
                    "assignments": [{
                        "id": "work-1",
                        "assignment_title": "Latihan Lisan",
                        "collect_by": today,
                        "non_submitted": ["Kumar Das"],
                    }]
                }
            }
        }
        students = [
            {"no": "1", "class": "2G3", "name": "Siti Aminah"},
            {"no": "2", "class": "2G3", "name": "Kumar Das"},
        ]

        with patch.object(web_app.bot.gs, "get_classops_ledger", return_value=ledger), \
             patch.object(web_app.bot.gs, "get_classops_students", return_value=students):
            summary = web_app._classops_status_summary()

        self.assertTrue(summary["connected"])
        self.assertEqual(summary["class_count"], 1)
        self.assertEqual(summary["assignment_count"], 1)
        self.assertEqual(summary["pending_count"], 1)
        self.assertEqual(summary["due_today_count"], 1)
        self.assertEqual(summary["classes"][0]["latest_assignment"]["submitted_count"], 1)

    def test_live_briefing_prompt_does_not_replay_stored_briefing(self):
        self.assertEqual(web_app._live_briefing_slot("Give me a crisp H.I.R.A briefing for right now."), "morning")
        self.assertEqual(web_app._briefing_replay_slot("Give me a crisp H.I.R.A briefing for right now."), "")

    def test_briefing_replay_ignores_stale_stored_notification(self):
        stale = {
            "kind": "briefing",
            "title": "Morning briefing",
            "source": "morning_briefing:2026-04-30",
            "body": "Thursday, 30 April 2026",
        }

        with patch.object(web_app.bot.gs, "get_app_notifications", return_value=[stale]), \
             patch.object(web_app.bot, "build_briefing", return_value="Fresh today") as build_briefing:
            text = web_app._briefing_replay_text("morning")

        self.assertEqual(text, "Fresh today")
        build_briefing.assert_called_once_with(record_news_digest=False)

    def test_format_curated_digest_includes_why_lines(self):
        text = bot.format_curated_digest([
            {
                "label": "AI",
                "why": "product/build relevance",
                "item": {"title": "AI developer release notes", "source": "The Verge"},
            }
        ])

        self.assertIn("Why it matters", text)
        self.assertIn("product/build relevance", text)

    def test_pwa_lfc_prompt_includes_news_search_tools(self):
        tools = bot.pwa_tools_for_message("latest LFC transfer rumours and injuries")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertIn("get_liverpool_brief", names)

    def test_pwa_android_update_prompt_includes_news_tools(self):
        text = "There was an Android 17 update. Can you find it?"
        tools = bot.pwa_tools_for_message(text)
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertEqual(bot._forced_tool_for_text(text, tools), "get_latest_news")
        self.assertFalse(asyncio.run(bot.should_route_quick_pwa_chat([], text)))

    def test_source_discipline_treats_android_updates_as_live(self):
        discipline = bot.source_discipline_for_text("Can you find the Android 17 update?")

        self.assertTrue(discipline["needs_live_check"])
        self.assertIn("get_latest_news", discipline["recommended_tools"])

    def test_pwa_preferred_topics_followup_uses_news_tools(self):
        text = "Nothing on my other preferred topics?"
        tools = bot.pwa_tools_for_message(text)
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertEqual(bot._forced_tool_for_text(text, tools), "get_latest_news")
        self.assertFalse(asyncio.run(bot.should_route_quick_pwa_chat([], text)))

    def test_source_discipline_treats_preferred_topics_as_live(self):
        discipline = bot.source_discipline_for_text("Nothing on my other preferred topics?")

        self.assertTrue(discipline["needs_live_check"])
        self.assertIn("get_latest_news", discipline["recommended_tools"])

    def test_pwa_lfc_correction_includes_structured_brief(self):
        tools = bot.pwa_tools_for_message("Liverpool didn't host Man Utd yesterday. Get your facts straight pls")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertIn("get_liverpool_brief", names)

    def test_pwa_lfc_followup_uses_recent_sports_context(self):
        tools = bot.pwa_tools_for_message(
            "So what was the match like?",
            recent_context="User corrected that Liverpool did not host Man Utd yesterday.",
        )
        names = {tool["name"] for tool in tools}

        self.assertIn("get_liverpool_brief", names)

    def test_liverpool_brief_uses_fotmob_before_news_snippets(self):
        fotmob_text = (
            "Recent results for Liverpool: "
            "April 25, 2026: Premier League - 3-1 win vs Crystal Palace. "
            "May 9, 2026: Premier League - 1-1 draw vs Chelsea. "
            "Upcoming fixtures for Liverpool: May 17, 2026: Premier League - at Aston Villa. "
            "Liverpool currently sits in 4th place in the Premier League with 58 points."
        )
        latest = {
            "start": date(2026, 4, 1),
            "end": date(2026, 5, 24),
            "latest_completed": {
                "date": datetime(2026, 5, 9, tzinfo=timezone.utc),
                "date_text": "2026-05-09",
                "league": "Premier League",
                "status": "Full Time",
                "scoreline": "Liverpool 1-1 Chelsea",
                "source_url": "https://example.com/scoreboard",
            },
            "next_fixture": None,
            "events": [],
            "errors": [],
        }
        with (
            patch.object(bot.sports, "_fetch_fotmob_team_text", return_value={"ok": True, "text": fotmob_text}),
            patch.object(bot.sports, "_espn_liverpool_scoreboard_probe", return_value=latest),
            patch.object(bot.sports.ss, "google_news", return_value=[]),
            patch.object(bot.sports.ss, "search_enabled", return_value=False),
        ):
            brief = bot.sports.build_liverpool_brief("latest result", max_items=1)

        self.assertIn("FotMob team-page probe", brief)
        self.assertIn("May 9, 2026: Premier League - 1-1 draw vs Chelsea", brief)
        self.assertLess(brief.index("FotMob team-page probe"), brief.index("Authoritative scoreboard probe"))

    def test_source_discipline_warns_against_older_assistant_turns(self):
        hint = bot.source_discipline_hint("latest LFC result")

        self.assertIn("Do not rely only on memory or older assistant turns", hint)

    def test_backend_fallback_runs_liverpool_source_check(self):
        async def fake_execute_tool(name, inp):
            self.assertEqual(name, "get_liverpool_brief")
            self.assertEqual(inp["focus"], "what's up with lfc")
            return "Liverpool FC structured live brief\nFotMob team-page probe"

        with patch.object(bot, "_execute_tool", side_effect=fake_execute_tool):
            reply = asyncio.run(web_app._source_check_backend_fallback("what's up with lfc"))

        self.assertIn("not going to guess from memory", reply)
        self.assertIn("FotMob team-page probe", reply)

    def test_source_contract_guardrail_blocks_unconfirmed_result(self):
        messages = [{"role": "user", "content": "is that the latest result?"}]
        tool_results = [{
            "content": (
                "SOURCE CONTRACT: status=unconfirmed; as_of=2026-05-10; "
                "source=FotMob/ESPN/news probe; reason=no completed fixture returned"
            )
        }]

        reply = bot._source_contract_guardrail(messages, tool_results)

        self.assertIn("could not confirm the latest result", reply)
        self.assertIn("not going to answer this from older headlines", reply)

    def test_liverpool_result_probe_demotes_stale_news(self):
        latest = {
            "start": date(2026, 4, 1),
            "end": date(2026, 5, 24),
            "latest_completed": {
                "date": datetime(2026, 5, 9, tzinfo=timezone.utc),
                "date_text": "2026-05-09",
                "league": "Premier League",
                "status": "Full Time",
                "scoreline": "Liverpool 1-1 Chelsea",
                "source_url": "https://example.com/scoreboard",
            },
            "next_fixture": None,
            "events": [],
            "errors": [],
        }
        stale_item = {
            "title": "Liverpool 3-1 Crystal Palace match report",
            "published": "Sat, 25 Apr 2026 18:00:00 GMT",
            "source": "Example",
            "description": "Liverpool won 3-1.",
            "url": "https://example.com/stale",
        }
        with (
            patch.object(bot.sports, "_fetch_fotmob_team_text", return_value={"ok": True, "text": ""}),
            patch.object(bot.sports, "_espn_liverpool_scoreboard_probe", return_value=latest),
            patch.object(bot.sports.ss, "google_news", return_value=[stale_item]),
            patch.object(bot.sports.ss, "search_enabled", return_value=False),
        ):
            text = "\n".join(bot.sports._format_liverpool_result_probe("latest result", 1))

        self.assertIn("SOURCE CONTRACT: status=confirmed", text)
        self.assertIn("Demoted stale result/news leads", text)
        self.assertIn("Liverpool 3-1 Crystal Palace", text)

    def test_backend_fallback_summarises_source_readout(self):
        raw = "\n".join([
            "SOURCE CONTRACT: status=confirmed; as_of=2026-05-09; source=ESPN; reason=latest completed",
            "Priority result probe",
            "- filler",
            "- Latest completed: Liverpool 1-1 Chelsea | 2026-05-09 | Premier League | Full Time",
            "Table note: Liverpool currently sits in 4th place",
        ])

        summary = web_app._summarise_source_fallback(raw)

        self.assertIn("SOURCE CONTRACT: status=confirmed", summary)
        self.assertIn("Latest completed: Liverpool 1-1 Chelsea", summary)
        self.assertNotIn("filler", summary)

    def test_source_contracts_from_tool_results_are_structured(self):
        contracts = bot._source_contracts_from_tool_results([{
            "content": (
                "SOURCE CONTRACT: status=confirmed; as_of=2026-05-09; "
                "source=ESPN scoreboard; reason=latest completed fixture"
            )
        }])

        self.assertEqual(contracts[0]["status"], "confirmed")
        self.assertEqual(contracts[0]["source"], "ESPN scoreboard")

    def test_chat_trace_merge_and_finalise(self):
        trace = web_app._new_chat_trace("latest LFC result")
        web_app._merge_chat_trace(trace, {
            "route": "agentic",
            "tools_available": ["get_liverpool_brief", "get_latest_news"],
            "tools_called": ["get_liverpool_brief"],
            "source_contracts_seen": [{
                "status": "confirmed",
                "as_of": "2026-05-09",
                "source": "ESPN",
                "reason": "latest completed",
            }],
        })
        web_app._finalise_chat_trace(trace)

        self.assertEqual(trace["route"], "agentic")
        self.assertEqual(trace["confidence_gate"], "passed")
        self.assertEqual(trace["final_mode"], "answered")
        self.assertEqual(trace["tools_called"], ["get_liverpool_brief"])

    def test_chat_trace_finalise_marks_missing_contract(self):
        trace = web_app._new_chat_trace("latest LFC result")
        web_app._merge_chat_trace(trace, {"route": "agentic"})
        web_app._finalise_chat_trace(trace)

        self.assertEqual(trace["confidence_gate"], "no_contract")

    def test_pwa_followup_reminder_gets_recent_turn_grounding(self):
        history = [
            {"role": "assistant", "content": "Locked in for Sahibba at Pei Hwa Sec, 2-6pm."},
            {"role": "user", "content": "I meant reminder for the Rhino exercise briefing"},
            {"role": "assistant", "content": "You meant the Rhino emergency exercise, but I do not have the date."},
            {"role": "user", "content": "No lessons so its ok. It's for Rhino emergency exercise."},
            {"role": "assistant", "content": "Got it - that's the Rhino emergency exercise marking, not regular coursework."},
        ]

        hint = web_app._recent_turn_grounding_context(
            history,
            "Thanks Hira. Just remind me again during the morning briefing for that day",
        )

        self.assertIn("Rhino emergency exercise", hint)
        self.assertIn("Sahibba", hint)
        self.assertIn("newer user corrections", hint)
        self.assertIn("Do not switch back to an older named event", hint)

    def test_pwa_working_memory_prefers_latest_user_correction(self):
        history_key = "pwa:test-working-memory"
        web_app._WEB_WORKING_MEMORY.pop(web_app._working_memory_storage_key(history_key), None)
        history = [
            {"role": "assistant", "content": "Locked in for Sahibba at Pei Hwa Sec, 2-6pm."},
            {"role": "user", "content": "I meant reminder for the Rhino emergency exercise briefing"},
            {"role": "assistant", "content": "Got it - that's the Rhino emergency exercise marking, not regular coursework."},
        ]

        memory = web_app._update_working_memory(
            history_key,
            history,
            "Thanks Hira. Just remind me again during the morning briefing for that day",
        )
        context = web_app._working_memory_context(memory)
        summary = web_app._working_memory_summary(memory)

        self.assertEqual(memory["current_subject"], "Rhino emergency exercise briefing")
        self.assertIn("Sahibba", memory["competing_subjects"])
        self.assertEqual(summary["subject"], "Rhino emergency exercise briefing")
        self.assertEqual(summary["action"], "morning briefing reminder")
        self.assertIn("Latest user correction/clarification", context)

    def test_pwa_time_specific_remind_me_includes_nudge_tool(self):
        tools = bot.pwa_tools_for_message(
            "Thanks Hira. Just remind me again during the morning briefing for that day"
        )
        names = {tool["name"] for tool in tools}

        self.assertIn("create_proactive_nudge", names)
        self.assertIn("add_reminder", names)

    def test_state_changing_action_validation_blocks_unresolved_subject(self):
        blocked = bot._validated_action_failure(
            "create_proactive_nudge",
            {"message": "Remind me about that day", "send_at": "2026-05-14T06:40:00+08:00"},
        )

        self.assertIsNotNone(blocked)
        self.assertIn("blocked", blocked)
        self.assertIn("unresolved vague references", blocked)

    def test_state_changing_action_result_includes_audit(self):
        with patch.object(bot.gs, "add_reminder", return_value="42"):
            result = asyncio.run(bot._execute_tool(
                "add_reminder",
                {
                    "description": "Rhino emergency exercise morning briefing reminder",
                    "due_date": "2026-05-14",
                    "category": "Teaching",
                },
            ))

        self.assertIn("Added reminder #42", result)
        self.assertIn("Action audit: action=add_reminder", result)
        self.assertIn("subject=Rhino emergency exercise", result)

    def test_redis_guardrail_warns_when_production_not_required(self):
        with patch.dict(os.environ, {
            "RAILWAY_ENVIRONMENT": "production",
            "HIRA_REQUIRE_REDIS": "",
            "REDIS_URL": "",
        }):
            status = bot.redis_guardrail_status()

        self.assertTrue(status["production_detected"])
        self.assertFalse(status["redis_required"])
        self.assertTrue(any("HIRA_REQUIRE_REDIS=1" in warning for warning in status["warnings"]))

    def test_forced_lfc_tool_uses_recent_match_context(self):
        forced = bot._forced_tool_for_current_turn(
            [
                {"role": "user", "content": "Liverpool didn't host Man Utd yesterday."},
                {"role": "assistant", "content": "I should verify the fixture."},
                {"role": "user", "content": "So what was the match like?"},
            ],
            [bot.LIVERPOOL_BRIEF_TOOL, bot.NEWS_TOOL],
        )

        self.assertEqual(forced, "get_liverpool_brief")

    def test_system_prompt_requires_supporter_mood_read_after_sports_facts(self):
        prompt = bot.SYSTEM_PROMPT()

        self.assertIn("After giving verified Liverpool or F1 scores/match details", prompt)
        self.assertIn("supporter-read", prompt)
        self.assertIn("Do not let mood-reading replace the verified facts", prompt)

    def test_lfc_player_chat_is_not_quick_routed(self):
        text = "Still anxious about this weekend lfc big match. Hope wirtz and isak have a banger."

        self.assertTrue(bot._looks_tool_heavy(text))

    def test_lfc_player_names_include_news_tools(self):
        tools = bot.pwa_tools_for_message("Hope Wirtz and Isak have a banger this weekend")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertIn("get_liverpool_brief", names)

    def test_pwa_f1_prompt_includes_structured_sports_tool(self):
        tools = bot.pwa_tools_for_message("latest F1 standings and Mercedes qualifying result")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_f1_brief", names)
        self.assertIn("web_search", names)
        self.assertIn("web_research", names)

    def test_pwa_research_prompt_includes_web_research_tool(self):
        tools = bot.pwa_tools_for_message("research current AI tools for teaching and cite sources")
        names = {tool["name"] for tool in tools}

        self.assertIn("web_research", names)
        self.assertIn("fetch_url", names)
        self.assertIn("remember_source_insight", names)

    def test_web_search_available_without_tavily_key(self):
        with patch.dict(os.environ, {"TAVILY_API_KEY": ""}, clear=False):
            self.assertTrue(search_service.search_enabled())

    def test_web_search_uses_duckduckgo_fallback_when_tavily_missing(self):
        with (
            patch.object(search_service, "TAVILY_API_KEY", ""),
            patch.object(search_service, "_duckduckgo_search", return_value=[
                {"title": "Official F1 calendar", "description": "", "url": "https://www.formula1.com/en/racing/2026"},
            ]) as duckduckgo,
            patch.object(search_service, "_google_news_search_results", return_value=[]),
        ):
            results = search_service.web_search("2026 F1 calendar", max_results=3)

        duckduckgo.assert_called_once()
        self.assertEqual(results[0]["url"], "https://www.formula1.com/en/racing/2026")

    def test_duckduckgo_redirect_url_is_cleaned(self):
        raw = "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.formula1.com%2Fen%2Fracing%2F2026"

        self.assertEqual(search_service._clean_search_url(raw), "https://www.formula1.com/en/racing/2026")

    def test_web_research_plans_variants_fetches_top_source(self):
        def fake_web_search(query, max_results=5):
            if "official" in query:
                return [{
                    "title": "Official AI guidance for schools",
                    "description": "Updated 2026 guidance",
                    "url": "https://www.moe.gov.sg/ai-guidance",
                }]
            return [{
                "title": "AI tools for teaching roundup",
                "description": "A recent overview",
                "url": "https://example.com/ai-tools",
            }]

        with (
            patch.object(search_service, "web_search", side_effect=fake_web_search),
            patch.object(search_service, "fetch_url", return_value={
                "ok": True,
                "url": "https://www.moe.gov.sg/ai-guidance",
                "title": "Official AI guidance",
                "text": "Updated 2026. Schools should evaluate AI tools for privacy, accuracy, and teaching purpose before classroom use.",
            }) as fetch_url,
        ):
            pack = search_service.web_research("AI tools for teaching", max_sources=2, fetch_pages=1)

        self.assertTrue(pack["ok"])
        self.assertGreaterEqual(len(pack["queries"]), 2)
        self.assertEqual(pack["sources"][0]["domain"], "moe.gov.sg")
        self.assertTrue(pack["sources"][0]["fetched"])
        self.assertIn("privacy", pack["sources"][0]["evidence"])
        fetch_url.assert_called_once()

    def test_web_research_grades_sources_and_adds_citation_ids(self):
        with (
            patch.object(search_service, "web_search", return_value=[
                {
                    "title": "Official Formula 1 2026 calendar",
                    "description": "Updated 2026 race schedule",
                    "url": "https://www.formula1.com/en/racing/2026",
                },
                {
                    "title": "Fan discussion of Formula 1 calendar",
                    "description": "Reddit thread",
                    "url": "https://www.reddit.com/r/formula1/comments/test",
                },
            ]),
            patch.object(search_service, "fetch_url", return_value={
                "ok": True,
                "url": "https://www.formula1.com/en/racing/2026",
                "title": "2026 F1 calendar",
                "text": "2026 FIA Formula One World Championship Race Calendar. Canada 22 - 24 May. Abu Dhabi 04 - 06 Dec.",
            }),
        ):
            pack = search_service.web_research("F1 2026 calendar", max_sources=2, fetch_pages=1)

        self.assertEqual(pack["quality"]["confidence"], "moderate")
        self.assertEqual(pack["sources"][0]["id"], "S1")
        self.assertEqual(pack["sources"][0]["grade"], "A")
        self.assertEqual(pack["sources"][0]["source_type"], "official/primary")
        self.assertEqual(pack["sources"][1]["grade"], "D")
        self.assertEqual(pack["sources"][1]["source_type"], "community/low-trust")
        formatted = search_service.format_research_pack(pack)
        self.assertIn("[S1] Grade A", formatted)
        self.assertIn("Quality: moderate", formatted)

    def test_deep_model_selected_for_architecture_work_when_configured(self):
        with patch.object(bot, "DEEP_MODEL", "deep-model"), patch.object(bot, "AGENTIC_MODEL", "agentic-model"):
            selected = bot._agentic_model_for_messages([
                {"role": "user", "content": "review this architecture and refactor the backend"}
            ])

        self.assertEqual(selected, "deep-model")

    def test_agentic_model_selected_for_ordinary_chat(self):
        with patch.object(bot, "DEEP_MODEL", "deep-model"), patch.object(bot, "AGENTIC_MODEL", "agentic-model"):
            selected = bot._agentic_model_for_messages([
                {"role": "user", "content": "how are we doing today?"}
            ])

        self.assertEqual(selected, "agentic-model")

    def test_memory_categories_include_new_buckets_and_aliases(self):
        self.assertIn("teaching", bot.gs.DEFAULT_MEMORY)
        self.assertIn("business", bot.gs.DEFAULT_MEMORY)
        self.assertIn("sports", bot.gs.DEFAULT_MEMORY)
        self.assertIn("constraints", bot.gs.DEFAULT_MEMORY)
        self.assertIn("recent_summaries", bot.gs.DEFAULT_MEMORY)
        self.assertIn("topic_profiles", bot.gs.DEFAULT_MEMORY)
        self.assertIn("correction_ledger", bot.gs.DEFAULT_MEMORY)
        self.assertIn("self_reflections", bot.gs.DEFAULT_MEMORY)
        self.assertIn("source_notes", bot.gs.DEFAULT_MEMORY)

        with (
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            memory = bot.gs.add_memory("lfc", "Liverpool context belongs here")

        self.assertIn("Liverpool context belongs here", memory["sports"])
        self.assertTrue(set_config.called)

        with (
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            memory = bot.gs.add_memory("mistake", "Do not repeat this correction")

        self.assertIn("Do not repeat this correction", memory["correction_ledger"])
        self.assertTrue(set_config.called)

        with (
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            memory = bot.gs.add_memory("knowledge", "Source-backed Liverpool note")

        self.assertIn("Source-backed Liverpool note", memory["source_notes"])
        self.assertTrue(set_config.called)

    def test_topic_profile_storage_replaces_by_topic(self):
        store = {}

        with (
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
        ):
            first = bot.gs.add_topic_profile({
                "topic": "MotoGP",
                "category": "sports",
                "track": ["Ducati", "Marc Marquez"],
                "live_facts": ["standings", "race results"],
            })
            second = bot.gs.add_topic_profile({
                "topic": "MotoGP",
                "category": "sports",
                "track": ["Ducati", "race weekends"],
            })

        self.assertEqual(first["topic"], "MotoGP")
        self.assertEqual(second["topic"], "MotoGP")
        memory = json.loads(store["assistant_memory"])
        self.assertEqual(len(memory["topic_profiles"]), 1)
        self.assertIn("race weekends", memory["topic_profiles"][0])

    def test_new_interest_forces_topic_profile_tool(self):
        forced = bot._forced_tool_for_text(
            "New interest: MotoGP. Track Ducati, Marc Marquez, standings and race weekends.",
            [{"name": "create_topic_profile"}, {"name": "remember_user_info"}],
        )

        self.assertEqual(forced, "create_topic_profile")

    def test_pwa_new_interest_includes_topic_profile_tool(self):
        tools = bot.pwa_tools_for_message("I'm getting into Japanese city pop. Build me a beginner map.")
        names = {tool["name"] for tool in tools}

        self.assertIn("create_topic_profile", names)

    def test_execute_topic_profile_tool(self):
        with patch.object(bot.gs, "add_topic_profile", return_value={
            "topic": "MotoGP",
            "category": "sports",
            "track": ["Ducati"],
            "live_facts": ["standings"],
        }):
            result = asyncio.run(bot._execute_tool("create_topic_profile", {
                "topic": "MotoGP",
                "category": "sports",
                "track": ["Ducati"],
                "live_facts": ["standings"],
            }))

        self.assertIn("Created topic profile: MotoGP", result)

    def test_chat_learning_event_records_correction_and_reflection(self):
        store = {}

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
        ):
            recorded = bot.record_chat_learning_event(
                "Actually Hira, Wirtz and Isak are Liverpool context now.",
                "Got it.",
                source="test",
            )

        self.assertEqual({item["type"] for item in recorded}, {"correction", "self_reflection"})
        memory = json.loads(store["assistant_memory"])
        self.assertEqual(len(memory["correction_ledger"]), 1)
        self.assertEqual(len(memory["self_reflections"]), 1)
        self.assertIn("Wirtz and Isak", memory["correction_ledger"][0])

    def test_execute_source_note_tool(self):
        with patch.object(bot.gs, "add_source_note", return_value={
            "topic": "Liverpool",
            "source": "Official site",
            "durability": "live_check",
        }):
            result = asyncio.run(bot._execute_tool("remember_source_insight", {
                "topic": "Liverpool",
                "source": "Official site",
                "source_url": "https://www.liverpoolfc.com/",
                "insight": "Match line-ups are live facts.",
                "durability": "live_check",
                "confidence": "official",
            }))

        self.assertIn("Stored source note for Liverpool", result)

    def test_source_discipline_flags_volatile_sports_questions(self):
        discipline = bot.source_discipline_for_text("latest LFC lineup and transfer rumours")

        self.assertTrue(discipline["needs_live_check"])
        self.assertEqual(discipline["confidence"], "needs_live_source")
        self.assertIn("get_liverpool_brief", discipline["recommended_tools"])

    def test_memory_review_summarises_buckets(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["correction_ledger"] = ["Correction A"]
        fake_memory["source_notes"] = ["Source note A", "Source note B"]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
        ):
            review = bot.build_memory_review(limit=1)

        self.assertTrue(review["ok"])
        self.assertEqual(review["total_items"], 3)
        self.assertEqual(review["buckets"]["source_notes"]["count"], 2)
        self.assertEqual(review["buckets"]["source_notes"]["recent"], ["Source note B"])

    def test_system_prompt_keeps_plain_profile_memories_visible(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["profile"] = [f"profile fact {index}" for index in range(9)]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
        ):
            prompt = bot.SYSTEM_PROMPT()

        self.assertIn("profile fact 0", prompt)
        self.assertIn("profile fact 8", prompt)

    def test_profile_memory_surfaces_on_single_keyword_overlap(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["profile"] = ["My mum's name is Salwa."]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
        ):
            recalled = bot.retrieve_relevant_memory("who is my mum", limit=1)

        self.assertEqual(recalled[0]["category"], "profile")
        self.assertIn("Salwa", recalled[0]["text"])

    def test_memory_write_invalidates_system_prompt_cache(self):
        bot._SYSTEM_PROMPT_CACHE["key"] = "cached"
        bot._SYSTEM_PROMPT_CACHE["value"] = "old prompt"

        with patch.object(bot.gs, "add_memory", return_value={"profile": ["Remember me."]}) as add_memory:
            result = bot._add_memory("profile", "Remember me.")

        add_memory.assert_called_once_with("profile", "Remember me.")
        self.assertEqual(result, {"profile": ["Remember me."]})
        self.assertIsNone(bot._SYSTEM_PROMPT_CACHE["key"])
        self.assertIsNone(bot._SYSTEM_PROMPT_CACHE["value"])

    def test_memory_tool_input_recovers_missing_text_from_user_message(self):
        messages = [{"role": "user", "content": "Van Dijk is my all time fav player so losing him would be gutting"}]

        normalised = bot._normalise_memory_tool_input({"category": "profile"}, messages)

        self.assertEqual(normalised["text"], "Van Dijk is my all time fav player so losing him would be gutting")
        self.assertEqual(normalised["category"], "sports")

    def test_memory_tool_accepts_fact_alias_without_keyerror(self):
        with patch.object(bot.gs, "add_memory", return_value={"sports": ["Van Dijk is my all time fav player."]}) as add_memory:
            result = asyncio.run(bot._execute_tool("remember_user_info", {
                "fact": "Van Dijk is my all time fav player.",
            }))

        add_memory.assert_called_once_with("sports", "Van Dijk is my all time fav player.")
        self.assertIn("Remembered under sports", result)

    def test_relevant_memory_retrieval_prioritises_corrections(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["correction_ledger"] = [
            {"correction": "When Herwanto asks about HIRA upgrades, update the growth log after changes."}
        ]
        fake_memory["topic_profiles"] = ["Liverpool context should use live sources."]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
        ):
            recalled = bot.retrieve_relevant_memory("upgrade HIRA memory and remember growth log", limit=2)
            hint = bot.intent_lens_hint("upgrade HIRA memory and remember growth log")

        self.assertEqual(recalled[0]["category"], "correction_ledger")
        self.assertIn("growth log", recalled[0]["text"])
        self.assertIn("Likely intent", hint)
        self.assertIn("correction_ledger", hint)

    def test_proactive_intelligence_flags_packed_due_marking_day(self):
        load = {
            "today": {
                "score": 76,
                "load": "Packed",
                "marking_scripts": 22,
            },
            "days": [
                {"date": "2026-05-02", "score": 76, "label": "Today", "load": "Packed"},
                {"date": "2026-05-03", "score": 30, "label": "Sun", "load": "Pretty chill"},
            ],
        }
        tasks = {
            "items": [
                {
                    "id": "7",
                    "description": "Submit CCA attendance",
                    "due": "2026-05-03",
                }
            ]
        }
        now = bot.SGT.localize(bot.datetime(2026, 5, 2, 9, 0))

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "build_daily_load", return_value=load),
            patch.object(bot, "build_task_structured", return_value=tasks),
            patch.object(bot.gs, "get_followups", return_value=[]),
        ):
            insights = bot.build_proactive_intelligence_insights(now=now)

        self.assertTrue(insights)
        self.assertEqual(insights[0]["title"], "Workload pinch point")
        self.assertIn("22 unmarked", insights[0]["body"])

    def test_due_proactive_intelligence_deduplicates_seen_items(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 2, 9, 0))
        insight = {
            "id": "2026-05-02:quiet_window",
            "title": "Quiet window",
            "body": "Move one deeper project.",
            "priority": "low",
        }

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "build_proactive_intelligence_insights", return_value=[insight]),
            patch.object(bot.gs, "get_config", return_value=json.dumps({insight["id"]: now.isoformat()})),
        ):
            self.assertEqual(bot.due_proactive_intelligence(now), [])

    def test_find_available_training_slots_avoids_cca_day_and_calendar_conflicts(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 4, 9, 0))

        def fake_lessons(target):
            return ([{"start": "08:00", "end": "14:30", "subject": "ML", "description": "Lesson"}], "Odd")

        def fake_events(target):
            if target.isoformat() == "2026-05-05":
                return [{
                    "summary": "Football CCA",
                    "description": "",
                    "location": "",
                    "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
                    "end": {"dateTime": "2026-05-05T18:00:00+08:00"},
                }]
            if target.isoformat() == "2026-05-06":
                return [{
                    "summary": "HDB appointment",
                    "description": "",
                    "location": "",
                    "start": {"dateTime": "2026-05-06T15:00:00+08:00"},
                    "end": {"dateTime": "2026-05-06T16:00:00+08:00"},
                }]
            return []

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "datetime", wraps=bot.datetime) as fake_datetime,
            patch.object(bot, "_lessons_for_date", side_effect=fake_lessons),
            patch.object(bot, "_calendar_events_for_date", side_effect=fake_events),
        ):
            fake_datetime.now.return_value = now
            result = bot.find_available_training_slots(
                days=4,
                duration_minutes=60,
                window_start="14:00",
                window_end="18:00",
                avoid_keywords=["cca", "football"],
                purpose="Sahibba training",
            )

        self.assertIn("Checked timetable + Google Calendar", result)
        self.assertIn("Wed 6 May", result)
        self.assertIn("16:00-18:00", result)
        self.assertIn("Tue 5 May: avoided", result)
        self.assertNotIn("Tue 5 May, Odd week: 14:30", result)

    def test_runtime_status_contains_observability_sections(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["sports"] = ["Liverpool"]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
            patch.object(bot.gs, "get_projects", return_value=[{"project": "GamePlan"}]),
            patch.object(bot.gs, "get_app_notifications", return_value=[{"id": "1"}]),
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[{"endpoint": "x"}]),
            patch.object(bot.gs, "gmail_ok", return_value=True),
            patch.object(bot.gs, "get_config", return_value="2026-05-02"),
            patch.object(bot, "_get_redis", return_value=None),
        ):
            status = bot.build_runtime_status()

        self.assertIn("memory", status)
        self.assertIn("integrations", status)
        self.assertEqual(status["memory_buckets"]["sports"], 1)
        self.assertEqual(status["projects"]["count"], 1)
        self.assertEqual(status["notifications"]["queued_count"], 1)

    def test_pwa_link_prompt_includes_fetch_url_tool(self):
        tools = bot.pwa_tools_for_message("check this link https://www.formula1.com/en/teams")
        names = {tool["name"] for tool in tools}

        self.assertIn("fetch_url", names)
        self.assertIn("remember_source_insight", names)

    def test_fill_mtl_percentage_scores_updates_blank_fa2_percentages(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "FA1", "", "", "", "FA2", "", "", ""),
                        sheet_row("NO", "CLASS", "FULL NAME", "15", "30", "45", "%", "10", "25", "35", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "13", "19", "32", "71", "7", "10", "17", ""),
                        sheet_row("2", "S4-AN", "NAURA", "", "", "AB", "AB", "5", "14", "19", ""),
                        sheet_row("3", "S4-AN", "AUNI", "11", "18", "29", "64", "AB", "AB", "AB", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            result = bot.gs.fill_mtl_percentage_scores("S4-AN", "FA2")

        self.assertEqual(result["updated_cells"], 3)
        self.assertEqual(result["filled_numbers"], 2)
        self.assertEqual(result["copied_codes"], 1)
        data = fake_service.spreadsheets_api.values_api.batch_updates[0][1]["data"]
        self.assertEqual(
            [(item["range"], item["values"][0][0]) for item in data],
            [("'CG HERWANTO S4-AN'!K3", "49"), ("'CG HERWANTO S4-AN'!K4", "54"), ("'CG HERWANTO S4-AN'!K5", "AB")],
        )

    def test_fill_mtl_percentage_scores_reuses_blank_column_after_raw_score(self):
        book = {
            "properties": {"title": "2026 S2 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO 2G3 ML", "sheetId": 123},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "2G3 ML"),
                        sheet_row("NO", "CLASS", "FULL NAME", "WA2 (20)", "", "WA3"),
                        sheet_row("1", "S2-AN", "AMELIA", "10", "", ""),
                        sheet_row("2", "S2-AN", "MYSHA", "AB", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            result = bot.gs.fill_mtl_percentage_scores("2G3", "WA2")

        self.assertEqual(result["created_columns"], 1)
        self.assertEqual(fake_service.spreadsheets_api.batch_updates, [])
        data = fake_service.spreadsheets_api.values_api.batch_updates[0][1]["data"]
        self.assertEqual(
            [(item["range"], item["values"][0][0]) for item in data],
            [
                ("'CG HERWANTO 2G3 ML'!E3", "WA2 (100%)"),
                ("'CG HERWANTO 2G3 ML'!E4", "50"),
                ("'CG HERWANTO 2G3 ML'!E5", "AB"),
            ],
        )

    def test_apply_mtl_failure_highlighting_adds_red_conditional_format_rule(self):
        book = {
            "properties": {"title": "2026 S2 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO 2G3 ML", "sheetId": 123},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "2G3 ML"),
                        sheet_row("NO", "CLASS", "FULL NAME", "WA2 (40)", "WA2 (100%)"),
                        sheet_row("1", "S2-AN", "AMELIA", "18", "45"),
                        sheet_row("2", "S2-AN", "MYSHA", "28", "70"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            result = bot.gs.apply_mtl_failure_highlighting("2G3", "WA2")

        self.assertEqual(result["highlighted_columns"], 1)
        spreadsheet_id, body = fake_service.spreadsheets_api.batch_updates[0]
        self.assertEqual(spreadsheet_id, "sheet-1")
        rule = body["requests"][0]["addConditionalFormatRule"]["rule"]
        self.assertEqual(rule["ranges"], [{
            "sheetId": 123,
            "startRowIndex": 3,
            "endRowIndex": 5,
            "startColumnIndex": 4,
            "endColumnIndex": 5,
        }])
        boolean_rule = rule["booleanRule"]
        self.assertEqual(boolean_rule["condition"], {
            "type": "NUMBER_LESS",
            "values": [{"userEnteredValue": "50"}],
        })
        self.assertEqual(
            boolean_rule["format"]["backgroundColorStyle"]["rgbColor"],
            {"red": 0.86, "green": 0.0, "blue": 0.12},
        )

    def test_analyze_mtl_scores_reports_stats_and_progress(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "FA1", "", "FA2", ""),
                        sheet_row("NO", "CLASS", "FULL NAME", "45", "%", "35", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "32", "71", "17", "49"),
                        sheet_row("2", "S4-AN", "NAURA", "AB", "AB", "19", "54"),
                        sheet_row("3", "S4-AN", "AUNI", "29", "64", "AB", "AB"),
                        sheet_row("4", "S4-AN", "HASLIANI", "29", "64", "19", "54"),
                        sheet_row("5", "S4-AN", "DANISH", "29", "64", "21", "60"),
                        sheet_row("6", "S4-AN", "YUSSOFF", "35", "77", "33", "94"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("S4-AN", "%", "FA1 %", "FA2 %")

        self.assertIn("mean 68", brief)
        self.assertIn("median 64", brief)
        self.assertIn("pass 5/5", brief)
        self.assertIn("Underperforming / watchlist", brief)
        self.assertIn("AIRA", brief)
        self.assertIn("Most improved", brief)
        self.assertIn("YUSSOFF", brief)
        self.assertIn("Progress: FA1 % -> FA2 %", brief)
        self.assertNotIn("Progress: FA1 45 -> FA2 35", brief)
        self.assertIn("Drastic drops", brief)

    def test_fa_percentage_analysis_uses_percent_columns_not_components(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "", "", "FA1", "", "", "", "FA2", "", "", ""),
                        sheet_row("NO", "CLASS", "FULL NAME", "LC 1 (20)", "LC 2 (20)", "15", "30", "45", "%", "10", "25", "35", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "14", "18", "13", "19", "32", "71", "7", "10", "17", "49"),
                        sheet_row("2", "S4-AN", "NAURA", "", "", "", "", "AB", "AB", "5", "14", "19", "54"),
                        sheet_row("3", "S4-AN", "AUNI", "14", "16", "11", "18", "29", "64", "AB", "AB", "AB", "AB"),
                        sheet_row("4", "S4-AN", "COLLAR", "0", "4", "5", "2", "11", "24", "0", "6", "6", "17"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("S4-AN", "FA1 %", "FA1 %", "FA2 %")

        self.assertIn("FA1 %: mean", brief)
        self.assertIn("This is the percentage column", brief)
        self.assertIn("Progress: FA1 % -> FA2 %", brief)
        self.assertNotIn("FA1 15: mean", brief)
        self.assertNotIn("FA1 45: mean", brief)

    def test_score_analysis_treats_zero_as_score_and_statuses_as_non_scoring(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "WA1"),
                        sheet_row("NO", "CLASS", "FULL NAME", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "0"),
                        sheet_row("2", "S4-AN", "NAURA", "AB"),
                        sheet_row("3", "S4-AN", "AUNI", "VR"),
                        sheet_row("4", "S4-AN", "HASLIANI", "MC"),
                        sheet_row("5", "S4-AN", "DANISH", "50"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("S4-AN", "WA1")

        self.assertIn("mean 25", brief)
        self.assertIn("pass 1/2", brief)
        self.assertIn("AIRA", brief)
        self.assertIn("0.0 (below 50)", brief)
        self.assertIn("AB (absent): 1", brief)
        self.assertIn("VR (valid reason): 1", brief)
        self.assertIn("MC (medical certificate): 1", brief)

    def test_sec1g2_alias_matches_ml_g2_and_wa1_percent_header(self):
        book = {
            "properties": {"title": "2026 S1 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO ML G2"},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "ML G2"),
                        sheet_row("VENUE:", "L4-11"),
                        sheet_row(),
                        sheet_row(),
                        sheet_row("NO", "CLASS", "FULL NAME", "PSLE MTL GRADE", "TARGET", "WA1 (40)", "WA1 %", "PreWA2 (20)", "WA2", "WA3", "EOY"),
                        sheet_row("1", "1 Anchor", "ZAARA", "6", "", "20", "50", "", "", "", ""),
                        sheet_row("2", "1 Beacon", "AQASYA", "4", "", "26", "65", "", "", "", ""),
                        sheet_row("3", "1 Compass", "XANDER", "6", "", "0", "0", "", "", "", ""),
                        sheet_row("4", "1 Flagship", "FIQRI", "4", "", "AB", "AB", "", "", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("sec1G2", "WA1 %")

        self.assertIn("ML G2", brief)
        self.assertIn("WA1 %: mean 38.3", brief)
        self.assertIn("pass 2/3", brief)
        self.assertIn("AB (absent): 1", brief)

    def test_sec2_prg_wa_columns_are_mock_not_actual_wa2(self):
        book = {
            "properties": {"title": "2026 S2 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO 2G3 ML"},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "2G3 ML"),
                        sheet_row(),
                        sheet_row("NO", "CLASS", "FULL NAME", "WA1", "Prg-WA2", "Prg-WA2", "Prg-WA2", "WA2", "WA3", "EOY"),
                        sheet_row("1", "S2-AN", "AMELIA", "85", "34", "20", "MC", "", "", ""),
                        sheet_row("2", "S2-AN", "MYSHA", "65", "30", "19", "26", "", "", ""),
                        sheet_row("3", "S2-BE", "AULIA", "60", "22", "14", "21", "", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            with self.assertRaises(ValueError):
                bot.gs.format_mtl_score_analysis("2G3", "WA2")
            mock = bot.gs.format_mtl_score_analysis("2G3", "pre WA")

        self.assertIn("Prg-WA2 1", mock)
        self.assertIn("Prg-WA2 2", mock)

    def test_sec3g3_layout_reads_actual_wa1_percent(self):
        book = {
            "properties": {"title": "2026 S3 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO 3G3 ML"},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "3G3 ML"),
                        sheet_row("VENUE:", "L3-10"),
                        sheet_row(),
                        sheet_row(),
                        sheet_row("NO", "CLASS", "FULL NAME", "PSLE MTL GRADE", "S2 MTL RESULTS", "TARGET", "WA1 (20)", "WA1 %", "WA2", "WA3", "EOY"),
                        sheet_row("1", "S3-AN", "UMAIRA", "3", "", "", "10", "50", "", "", ""),
                        sheet_row("2", "S3-CO", "AYRA", "5", "", "", "12", "60", "", "", ""),
                        sheet_row("3", "S3-DA", "NINA", "4", "", "", "7", "35", "", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("3G3", "WA1 %")

        self.assertIn("3G3 ML", brief)
        self.assertIn("WA1 %: mean 48.3", brief)
        self.assertNotIn("WA1 (20): mean", brief)

    def test_reset_marking_request_forces_reset_tool(self):
        forced = bot._forced_tool_for_text(
            "reset marking load",
            [{"name": "reset_marking_load"}, {"name": "update_marking_progress"}],
        )

        self.assertEqual(forced, "reset_marking_load")

    def test_forced_tool_does_not_repeat_after_tool_result(self):
        fake_messages = FakeMessages()
        fake_claude = SimpleNamespace(messages=fake_messages)

        async def fake_execute_tool(name, inp):
            return "- Subject | From: sender@example.com | Snippet"

        messages = [{"role": "user", "content": "tell me about my last 5 emails"}]
        tools = [{"name": "get_gmail_brief"}]

        with (
            patch.object(bot, "claude", fake_claude),
            patch.object(bot, "SYSTEM_PROMPT", return_value="system"),
            patch.object(bot, "_execute_tool", side_effect=fake_execute_tool),
        ):
            reply = asyncio.run(bot._run_agentic_claude(messages, tools=tools))

        self.assertIn("last five emails", reply)
        self.assertEqual(fake_messages.calls[0]["tool_choice"], {"type": "tool", "name": "get_gmail_brief"})
        self.assertNotIn("tool_choice", fake_messages.calls[1])

    def test_forced_tool_ignores_structured_tool_result_turns(self):
        messages = [
            {"role": "user", "content": "tell me about my last 5 emails"},
            {"role": "assistant", "content": [SimpleNamespace(type="tool_use")]},
            {"role": "user", "content": [{"type": "tool_result", "content": "email result"}]},
        ]

        self.assertIsNone(bot._forced_tool_for_current_turn(messages, [{"name": "get_gmail_brief"}]))

    def test_agentic_claude_continues_after_max_tokens(self):
        class MaxTokenMessages:
            def __init__(self):
                self.calls = []

            def create(self, **kwargs):
                self.calls.append(kwargs)
                if len(self.calls) == 1:
                    return SimpleNamespace(
                        stop_reason="max_tokens",
                        content=[SimpleNamespace(type="text", text="Done. Here's what was written:")],
                    )
                return SimpleNamespace(
                    stop_reason="end_turn",
                    content=[SimpleNamespace(type="text", text="\n- Filled all FA2 percentages.")],
                )

        fake_messages = MaxTokenMessages()
        fake_claude = SimpleNamespace(messages=fake_messages)

        with (
            patch.object(bot, "claude", fake_claude),
            patch.object(bot, "SYSTEM_PROMPT", return_value="system"),
        ):
            reply = asyncio.run(bot._run_agentic_claude(
                [{"role": "user", "content": "fill percentages"}],
                tools=[],
                max_tokens=10,
            ))

        self.assertIn("Here's what was written", reply)
        self.assertIn("Filled all FA2 percentages", reply)
        self.assertEqual(len(fake_messages.calls), 2)

    def test_email_followup_forces_gmail_before_action(self):
        messages = [{"role": "user", "content": "read my latest personal email and note the meeting details for follow up"}]
        tools = [{"name": "get_gmail_brief"}, {"name": "create_followup"}]

        self.assertEqual(bot._forced_tool_for_current_turn(messages, tools), "get_gmail_brief")

    def test_weather_question_forces_nea_weather_tool(self):
        forced = bot._forced_tool_for_text(
            "Will it rain in Yishun later?",
            [{"name": "get_nea_weather"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_nea_weather")

    def test_temperature_question_forces_nea_weather_tool(self):
        forced = bot._forced_tool_for_text(
            "Yishun high temp tomorrow?",
            [{"name": "get_nea_weather"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_nea_weather")

    def test_pwa_weather_message_gets_weather_tool(self):
        tools = bot.pwa_tools_for_message("latest weather from NEA")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_nea_weather", names)

    def test_pwa_temperature_message_gets_weather_tool(self):
        tools = bot.pwa_tools_for_message("Yishun high temp tomorrow?")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_nea_weather", names)

    def test_prayer_question_forces_muis_prayer_tool(self):
        forced = bot._forced_tool_for_text(
            "What time is zuhur today?",
            [{"name": "get_muis_prayer_times"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "get_muis_prayer_times")

    def test_pwa_prayer_message_gets_muis_prayer_tool(self):
        tools = bot.pwa_tools_for_message("What time is zohor today?")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_muis_prayer_times", names)

    def test_khutbah_question_forces_muis_khutbah_tool(self):
        forced = bot._forced_tool_for_text(
            "What is today's Friday khutbah about?",
            [{"name": "get_muis_prayer_times"}, {"name": "get_muis_friday_khutbah"}],
        )

        self.assertEqual(forced, "get_muis_friday_khutbah")

    def test_pwa_khutbah_message_gets_muis_khutbah_tool(self):
        tools = bot.pwa_tools_for_message("khutbah summary before jumuah")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_muis_friday_khutbah", names)

    def test_execute_muis_prayer_tool_uses_bundled_muis_data(self):
        async def run():
            return await bot._execute_tool("get_muis_prayer_times", {
                "date": "2026-05-01",
                "prayer": "zuhur",
            })

        self.assertIn("Zohor 13:03", asyncio.run(run()))

    def test_execute_khutbah_tool_uses_muis_service(self):
        khutbah = {
            "date": "2026-05-01",
            "title": "Youth and today's challenges",
            "summary": "Youth challenges affect society tomorrow.",
            "key_points": ["Evaluate ethics through Islam"],
            "url": "https://www.muis.gov.sg/resources/khutbah-and-religious-advice/khutbah/youth-and-today-s-challenges-/",
            "pdf_url": "https://example.com/khutbah.pdf",
        }

        async def run():
            with patch.object(bot.isl, "latest_khutbah", return_value=khutbah):
                return await bot._execute_tool("get_muis_friday_khutbah", {"date": "2026-05-01"})

        result = asyncio.run(run())
        self.assertIn("Friday khutbah heads-up", result)
        self.assertIn("Youth and today's challenges", result)
        self.assertIn("Evaluate ethics through Islam", result)

    def test_muis_khutbah_listing_parser_reads_latest_english_card(self):
        html = '''
        <a href="/resources/khutbah-and-religious-advice/khutbah/youth-and-today-s-challenges-/">
          <p>1 May 2026</p>
          <span title="Youth and today’s challenges ">Youth and today’s challenges</span>
          <p class="line-clamp-3">The challenges faced by youth today.</p>
          <p>English</p>
        </a>
        '''

        records = islamic_service._parse_khutbah_listing(html)

        self.assertEqual(records[0]["date"], "2026-05-01")
        self.assertEqual(records[0]["title"], "Youth and today’s challenges")
        self.assertEqual(records[0]["language"], "English")

    def test_execute_weather_tool_uses_weather_service(self):
        async def run():
            with patch.object(bot.ws, "build_weather_brief", return_value="NEA weather: Yishun"):
                return await bot._execute_tool("get_nea_weather", {"area": "Yishun"})

        self.assertEqual(asyncio.run(run()), "NEA weather: Yishun")

    def test_weather_brief_includes_current_readings_and_air_quality(self):
        payloads = {
            weather_service.TWO_HOUR_V2: {
                "code": 0,
                "data": {
                    "items": [{
                        "update_timestamp": "2026-04-30T20:00:00+08:00",
                        "valid_period": {"start": "2026-04-30T20:00:00+08:00", "end": "2026-04-30T22:00:00+08:00"},
                        "forecasts": [{"area": "Yishun", "forecast": {"text": "Cloudy"}}],
                    }]
                },
            },
            weather_service.AIR_TEMPERATURE_V1: {
                "metadata": {"stations": [{"id": "S1", "name": "Yishun"}]},
                "items": [{"readings": [{"station_id": "S1", "value": 29.4}]}],
            },
            weather_service.RELATIVE_HUMIDITY_V1: {
                "metadata": {"stations": [{"id": "S2", "name": "Yishun"}]},
                "items": [{"readings": [{"station_id": "S2", "value": 78}]}],
            },
            weather_service.PSI_V1: {
                "items": [{
                    "readings": {
                        "psi_twenty_four_hourly": {"north": 42},
                        "pm25_twenty_four_hourly": {"north": 8},
                    }
                }],
            },
            weather_service.PM25_V1: {
                "items": [{"readings": {"pm25_one_hourly": {"north": 6}}}],
            },
        }

        with patch.object(weather_service, "_get_json", side_effect=lambda url: payloads[url]):
            brief = weather_service.build_weather_brief("Yishun", include_24h=False)

        self.assertIn("Nowcast: Cloudy", brief)
        self.assertIn("Temp 29.4 deg C", brief)
        self.assertIn("Humidity 78%", brief)
        self.assertIn("24h PSI 42", brief)
        self.assertIn("1h PM2.5 6 ug/m3", brief)

    def test_gmail_body_text_decodes_plain_parts(self):
        encoded = bot.base64.urlsafe_b64encode(
            b"Meeting on Friday at 2pm. Please follow up with the vendor."
        ).decode().rstrip("=")
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": encoded},
                }
            ],
        }

        self.assertIn("Meeting on Friday at 2pm", bot.gs._gmail_body_text(payload))

    def test_task_brief_hides_internal_metadata(self):
        due = (bot.datetime.now(bot.SGT).date() + bot.timedelta(days=1)).isoformat()
        tasks = [
            {
                "id": "31",
                "description": "Arrange relief teacher for ML Sec 2",
                "due": due,
                "category": "Teaching",
                "priority": "medium",
                "effort": "medium",
                "next_action": "",
            }
        ]

        with patch.object(bot.gs, "enriched_reminders", return_value=tasks):
            brief = bot.build_task_brief(days=7)

        self.assertIn("Arrange relief teacher for ML Sec 2", brief)
        self.assertNotIn("Teaching; medium; medium", brief)
        self.assertNotIn("_Teaching", brief)

    def test_marking_brief_shows_outstanding_and_collected_date(self):
        collected = (bot.datetime.now(bot.SGT).date() - bot.timedelta(days=2)).isoformat()
        tasks = [
            {
                "id": "1",
                "title": "Kefahaman 2G3",
                "total_scripts": 34,
                "marked_count": 12,
                "stack_count": 1,
                "collected_date": collected,
                "notes": "",
                "done": False,
            }
        ]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_marking_tasks", return_value=tasks),
        ):
            brief = bot.build_marking_brief()

        self.assertIn("Kefahaman 2G3", brief)
        self.assertIn("12 of 34 scripts marked", brief)
        self.assertIn("22 outstanding", brief)
        self.assertIn("2 days ago", brief)
        self.assertNotIn("[1]", brief)

    def test_marking_persists_at_full_count_until_explicitly_completed(self):
        store = {}

        def fake_get_config(key):
            return store.get(key, "")

        def fake_set_config(key, value):
            store[key] = value

        with (
            patch.object(bot.gs, "get_config", side_effect=fake_get_config),
            patch.object(bot.gs, "set_config", side_effect=fake_set_config),
        ):
            task = bot.gs.add_marking_task("Kefahaman 2G3", total_scripts=34, collected_date="2026-04-27")
            bot.gs.update_marking_progress(task["id"], marked_count=34)

            active = bot.gs.get_marking_tasks()
            stored = json.loads(store["marking_tasks"])[0]

            self.assertEqual(len(active), 1)
            self.assertFalse(stored["done"])
            self.assertEqual(active[0]["marked_count"], 34)

            bot.gs.update_marking_progress(task["id"], done=True)

            self.assertEqual(bot.gs.get_marking_tasks(), [])
            self.assertEqual(len(bot.gs.get_marking_tasks(include_done=True)), 1)

    def test_reset_marking_tasks_clears_active_stacks(self):
        store = {}

        def fake_get_config(key):
            return store.get(key, "")

        def fake_set_config(key, value):
            store[key] = value

        with (
            patch.object(bot.gs, "get_config", side_effect=fake_get_config),
            patch.object(bot.gs, "set_config", side_effect=fake_set_config),
        ):
            bot.gs.add_marking_task("1G2: Karangan", total_scripts=10, collected_date="2026-04-30")
            bot.gs.add_marking_task("2G3: Kefahaman", total_scripts=12, collected_date="2026-04-30")

            result = bot.gs.reset_marking_tasks()

            self.assertEqual(result["cleared_count"], 2)
            self.assertEqual(bot.gs.get_marking_tasks(), [])
            self.assertEqual(len(bot.gs.get_marking_tasks(include_done=True)), 2)

    def test_completing_marking_reminder_closes_matching_marking_stack(self):
        reminders = [
            {
                "id": "12",
                "description": "Finish Kefahaman 2G3 marking",
                "due": "2026-04-29",
                "category": "Marking",
                "done": False,
            }
        ]
        marking_tasks = [
            {
                "id": "1",
                "title": "Kefahaman 2G3",
                "total_scripts": 34,
                "marked_count": 34,
                "stack_count": 1,
                "collected_date": "2026-04-27",
                "notes": "",
                "done": False,
            }
        ]

        with (
            patch.object(bot.gs, "get_reminders", return_value=reminders),
            patch.object(bot.gs, "mark_done", return_value=True),
            patch.object(bot.gs, "get_marking_tasks", return_value=marking_tasks),
            patch.object(bot.gs, "update_marking_progress", return_value={**marking_tasks[0], "done": True}) as update_marking,
        ):
            ok, synced = bot.complete_reminder_by_id("12")

        self.assertTrue(ok)
        self.assertEqual(synced["title"], "Kefahaman 2G3")
        update_marking.assert_called_once_with("1", done=True)

    def test_complete_task_tool_marks_plural_matching_reminders(self):
        reminders = [
            {
                "id": "21",
                "description": "ESWG EdTech vendor follow-up",
                "due": "2026-04-15",
                "category": "Teaching",
                "done": False,
            },
            {
                "id": "22",
                "description": "PLT EdTech project update",
                "due": "2026-04-16",
                "category": "Projects",
                "done": False,
            },
            {
                "id": "23",
                "description": "PLT admin project update",
                "due": "2026-04-16",
                "category": "Projects",
                "done": False,
            },
        ]

        with (
            patch.object(bot.gs, "get_reminders", return_value=reminders),
            patch.object(bot.gs, "mark_done", return_value=True) as mark_done,
        ):
            result = asyncio.run(bot._execute_tool(
                "complete_task_by_text",
                {"query": "The edtech entries have been completed"},
            ))

        self.assertIn("Marked 2 reminders done", result)
        self.assertIn("#21 ESWG EdTech vendor follow-up", result)
        self.assertIn("#22 PLT EdTech project update", result)
        self.assertNotIn("#23", result)
        self.assertEqual([call.args[0] for call in mark_done.call_args_list], ["21", "22"])

    def test_home_marking_summary_ignores_completed_stacks(self):
        completed = {
            "id": "1",
            "title": "Kefahaman 2G3",
            "total_scripts": 34,
            "marked_count": 34,
            "stack_count": 1,
            "collected_date": "2026-04-27",
            "notes": "",
            "done": True,
            "completed_at": bot.datetime.now(bot.SGT).date().isoformat(),
        }

        with patch.object(web_app.bot.gs, "get_marking_tasks", return_value=[]):
            summary = web_app._marking_summary()

        self.assertEqual(summary["active_stacks"], 0)
        self.assertEqual(summary["total_scripts"], 0)
        self.assertEqual(summary["marked_scripts"], 0)
        self.assertEqual(summary["unmarked_scripts"], 0)
        self.assertTrue(summary["all_clear"])
        self.assertEqual(summary["sets"], [])

    def test_home_marking_summary_returns_per_set_breakdown(self):
        tasks = [
            {
                "id": "1",
                "title": "1G2: Karangan",
                "total_scripts": 34,
                "marked_count": 12,
                "stack_count": 1,
                "collected_date": "2026-04-27",
                "notes": "",
                "done": False,
            },
            {
                "id": "2",
                "title": "1G2: Kefahaman",
                "total_scripts": 22,
                "marked_count": 5,
                "stack_count": 1,
                "collected_date": "2026-04-29",
                "notes": "",
                "done": False,
            },
        ]

        with patch.object(web_app.bot.gs, "get_marking_tasks", return_value=tasks):
            summary = web_app._marking_summary()

        self.assertEqual(summary["active_stacks"], 2)
        self.assertEqual(summary["total_scripts"], 56)
        self.assertEqual(summary["marked_scripts"], 17)
        self.assertEqual(summary["unmarked_scripts"], 39)
        self.assertEqual(summary["sets"][0]["title"], "1G2: Karangan")
        self.assertEqual(summary["sets"][0]["display_title"], "1G2 [Karangan]")
        self.assertEqual(summary["sets"][1]["title"], "1G2: Kefahaman")
        self.assertEqual(summary["sets"][1]["display_title"], "1G2 [Kefahaman]")
        self.assertEqual(summary["sets"][0]["progress_label"], "12/34")
        self.assertEqual(summary["sets"][0]["unmarked_scripts"], 22)

    def test_marking_display_title_accepts_bracket_format(self):
        self.assertEqual(web_app._marking_display_title("3G3: karangan naratif"), "3G3 [karangan naratif]")
        self.assertEqual(web_app._marking_display_title("2G3 [HBL on SLS]"), "2G3 [HBL on SLS]")

    def test_archive_app_notifications_hides_selected_items(self):
        store = {
            "app_notifications": json.dumps([
                {
                    "id": "1",
                    "kind": "reminder",
                    "title": "H.I.R.A nudge",
                    "body": "Check bills",
                    "created": "2026-04-28T19:19:00+08:00",
                    "source": "nudge:1",
                    "seen_by": [],
                    "archived": False,
                },
                {
                    "id": "2",
                    "kind": "update",
                    "title": "H.I.R.A",
                    "body": "Still here",
                    "created": "2026-04-28T19:20:00+08:00",
                    "source": "",
                    "seen_by": [],
                    "archived": False,
                },
            ])
        }

        def fake_get_config(key):
            return store.get(key, "")

        def fake_set_config(key, value):
            store[key] = value

        with (
            patch.object(bot.gs, "get_config", side_effect=fake_get_config),
            patch.object(bot.gs, "set_config", side_effect=fake_set_config),
        ):
            archived = bot.gs.archive_app_notifications(["1"])
            visible = bot.gs.get_app_notifications()

        self.assertEqual(archived, 1)
        self.assertEqual([item["id"] for item in visible], ["2"])

    def test_notification_action_snooze_creates_nudge_and_archives(self):
        item = {
            "id": "9",
            "kind": "reminder",
            "title": "Task",
            "body": "Submit marks",
            "source": "task_reminder:2026-05-05:31",
        }
        req = web_app.NotificationActionRequest(id="9", action="snooze", snooze_minutes=30)

        with (
            patch.object(web_app, "_require_token"),
            patch.object(bot.gs, "get_app_notification", return_value=item),
            patch.object(bot.gs, "add_nudge", return_value={"id": "44"}) as add_nudge,
            patch.object(bot, "_record_notification_outcome") as record,
            patch.object(bot.gs, "archive_app_notifications") as archive,
            patch.object(bot.gs, "add_action_ledger") as ledger,
        ):
            result = web_app.notifications_action(req, x_hira_token="token", x_hira_client="phone")

        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "snooze")
        add_nudge.assert_called_once()
        archive.assert_called_once_with(["9"])
        self.assertEqual(record.call_args.args[0], "snoozed")
        self.assertEqual(ledger.call_args.kwargs["action"], "notification.snooze")
        self.assertEqual(ledger.call_args.kwargs["metadata"]["nudge_id"], "44")

    def test_pwa_nudges_command_lists_pending_nudges(self):
        nudges = [
            {
                "id": "7",
                "message": "Sahibba tournament",
                "send_at": "2026-05-14T07:30:00+08:00",
                "status": "pending",
            }
        ]

        with patch.object(bot.gs, "get_nudges", return_value=nudges):
            reply, tool = web_app._pwa_nudge_command_reply("/nudges")

        self.assertEqual(tool, "list_nudges")
        self.assertIn("Pending nudges", reply)
        self.assertIn("Sahibba tournament", reply)
        self.assertIn("/cancelnudge <id>", reply)

    def test_pwa_cancelnudge_command_cancels_and_archives_notification(self):
        notifications = [
            {"id": "9", "source": "nudge:7", "archived": False},
            {"id": "10", "source": "nudge:8", "archived": False},
        ]

        with (
            patch.object(bot.gs, "cancel_nudge", return_value=True) as cancel,
            patch.object(bot.gs, "get_app_notifications", return_value=notifications),
            patch.object(bot.gs, "archive_app_notifications", return_value=1) as archive,
        ):
            reply, tool = web_app._pwa_nudge_command_reply("/cancelnudge 7")

        self.assertEqual(tool, "cancel_nudge")
        self.assertIn("Nudge #7 cancelled", reply)
        cancel.assert_called_once_with("7")
        archive.assert_called_once_with(["9"])

    def test_notification_action_done_completes_linked_task(self):
        item = {
            "id": "9",
            "kind": "reminder",
            "title": "Task",
            "body": "Submit marks",
            "source": "task_reminder:2026-05-05:31",
        }
        req = web_app.NotificationActionRequest(id="9", action="done")

        with (
            patch.object(web_app, "_require_token"),
            patch.object(bot.gs, "get_app_notification", return_value=item),
            patch.object(bot, "complete_reminder_by_id", return_value=(True, None)) as complete,
            patch.object(bot, "_record_notification_outcome") as record,
            patch.object(bot.gs, "archive_app_notifications") as archive,
            patch.object(bot.gs, "add_action_ledger") as ledger,
        ):
            result = web_app.notifications_action(req, x_hira_token="token", x_hira_client="phone")

        self.assertTrue(result["completed"])
        complete.assert_called_once_with("31")
        archive.assert_called_once_with(["9"])
        self.assertEqual(record.call_args.args[0], "done")
        self.assertEqual(ledger.call_args.kwargs["action"], "notification.done")
        self.assertEqual(ledger.call_args.kwargs["metadata"]["reminder_id"], "31")

    def test_notification_action_done_completes_linked_checkin(self):
        item = {
            "id": "9",
            "kind": "reminder",
            "title": "Check-in",
            "body": "Done?",
            "source": "checkin:7",
        }
        req = web_app.NotificationActionRequest(id="9", action="done")

        with (
            patch.object(web_app, "_require_token"),
            patch.object(bot.gs, "get_app_notification", return_value=item),
            patch.object(bot.gs, "complete_checkin_today", return_value=True) as complete,
            patch.object(bot, "_record_notification_outcome"),
            patch.object(bot.gs, "archive_app_notifications"),
        ):
            result = web_app.notifications_action(req, x_hira_token="token", x_hira_client="phone")

        self.assertTrue(result["completed"])
        complete.assert_called_once_with("7")

    def test_notification_action_not_useful_records_feedback_and_archives(self):
        item = {
            "id": "9",
            "kind": "update",
            "title": "Digest",
            "body": "News",
            "source": "digest:abc",
        }
        req = web_app.NotificationActionRequest(id="9", action="not_useful")

        with (
            patch.object(web_app, "_require_token"),
            patch.object(bot.gs, "get_app_notification", return_value=item),
            patch.object(bot.gs, "add_insight_feedback") as feedback,
            patch.object(bot, "_record_notification_outcome") as record,
            patch.object(bot.gs, "archive_app_notifications") as archive,
        ):
            result = web_app.notifications_action(req, x_hira_token="token", x_hira_client="phone")

        self.assertEqual(result["rating"], "not_useful")
        feedback.assert_called_once_with("notification", "9", "not_useful")
        archive.assert_called_once_with(["9"])
        self.assertEqual(record.call_args.args[0], "not_useful")

    def test_pdf_excerpt_prioritises_herwanto_timetable_pages(self):
        pages = [
            pdf_service.PdfPageText(1, "General staff briefing and school notices."),
            pdf_service.PdfPageText(2, "T. MTL Muhammad Herwanto Johari\nMon P2 ML L3-10\nTue P5 ML L4-12"),
            pdf_service.PdfPageText(3, "Canteen duty roster unrelated page."),
        ]

        excerpt, selected, text_pages = pdf_service.build_pdf_excerpt(
            pages,
            caption="Find Herwanto timetable",
            max_pages=1,
        )

        self.assertEqual(text_pages, 3)
        self.assertEqual(selected, [2])
        self.assertIn("Muhammad Herwanto Johari", excerpt)
        self.assertIn("Mon P2", excerpt)

    def test_calendar_event_matching_finds_event_by_text(self):
        events = [
            {
                "id": "evt-1",
                "summary": "CCA football briefing",
                "location": "Hall",
                "description": "",
                "start": {"dateTime": "2026-04-28T15:00:00+08:00"},
                "end": {"dateTime": "2026-04-28T16:00:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-2",
                "summary": "Parent meeting",
                "location": "General Office",
                "description": "",
                "start": {"dateTime": "2026-04-29T10:00:00+08:00"},
                "end": {"dateTime": "2026-04-29T10:30:00+08:00"},
                "_calendar_id": "primary",
            },
        ]

        with patch.object(bot.gs, "get_events_between", return_value=events):
            event, score = bot._find_best_calendar_event("football briefing")

        self.assertEqual(event["id"], "evt-1")
        self.assertGreater(score, 0.45)

    def test_duplicate_delete_query_prefers_exact_duplicate_group(self):
        events = [
            {
                "id": "evt-1",
                "summary": "CCA NSG duty",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T15:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T18:00:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-2",
                "summary": "NSG C Div Game - N2 vs Whitley / N2A vs Assumption Pathway",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T15:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T18:00:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-3",
                "summary": "NSG C Div Game - N2 vs Whitley / N2A vs Assumption Pathway",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T15:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T18:00:00+08:00"},
                "_calendar_id": "secondary",
            },
        ]

        with patch.object(bot.gs, "get_events_between", return_value=events):
            event, score = bot._resolve_calendar_event_for_deletion(
                "duplicate of my CCA NSG duty on calendar. Please remove 1"
            )

        self.assertEqual(event["id"], "evt-2")
        self.assertGreaterEqual(score, 0.45)

    def test_duplicate_delete_query_returns_no_match_when_multiple_duplicate_groups_are_ambiguous(self):
        events = [
            {
                "id": "evt-1",
                "summary": "Team sync",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T09:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T09:30:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-2",
                "summary": "Team sync",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T09:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T09:30:00+08:00"},
                "_calendar_id": "secondary",
            },
            {
                "id": "evt-3",
                "summary": "Parent meeting",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-08T10:00:00+08:00"},
                "end": {"dateTime": "2026-05-08T10:30:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-4",
                "summary": "Parent meeting",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-08T10:00:00+08:00"},
                "end": {"dateTime": "2026-05-08T10:30:00+08:00"},
                "_calendar_id": "secondary",
            },
        ]

        with patch.object(bot.gs, "get_events_between", return_value=events):
            event, score = bot._resolve_calendar_event_for_deletion("remove 1 duplicate from my calendar")

        self.assertIsNone(event)
        self.assertEqual(score, 0)

    def test_work_gmail_request_routes_to_work_account(self):
        account, query = bot._extract_gmail_account_from_text("show my last 5 work emails")

        self.assertEqual(account, "work")
        self.assertEqual(query, "show my last 5")
        self.assertFalse(bot.is_removed_work_gmail_request("show my last 5 work emails"))

    def test_work_gmail_env_can_enable_service_layer(self):
        env = {
            "GOOGLE_GMAIL_CLIENT_ID": "client",
            "GOOGLE_GMAIL_CLIENT_SECRET": "secret",
            "GOOGLE_WORK_GMAIL_REFRESH_TOKEN": "work-refresh",
        }

        with patch.dict(os.environ, env, clear=True):
            self.assertTrue(bot.gs.gmail_ok("work"))
            self.assertFalse(bot.gs.gmail_ok("personal"))
            self.assertEqual(bot._normalise_gmail_account("work"), "work")

    def test_work_gmail_monitor_first_run_still_notifies_recent_action_mail(self):
        now_header = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
        messages = [
            {
                "id": "recent-action",
                "from": "HOD <hod@example.com>",
                "subject": "Action required: submit form by today",
                "snippet": "Please submit the form by today.",
                "body": "",
                "date": now_header,
            },
            {
                "id": "old-action",
                "from": "Admin <admin@example.com>",
                "subject": "Action required: old briefing",
                "snippet": "Please respond.",
                "body": "",
                "date": "Thu, 01 Jan 2026 09:00:00 +0800",
            },
        ]
        store = {}

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "gmail_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot, "_finish_background_job"),
            patch.object(bot.gs, "list_gmail_messages", return_value=messages),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
            patch.object(bot, "_queue_app_notification", return_value={"id": "n1", "_push_sent": 1}) as queue,
            patch.dict(os.environ, {
                "HIRA_WORK_GMAIL_NOTIFY_ON_FIRST_RUN": "0",
                "HIRA_WORK_GMAIL_FIRST_RUN_GRACE_MINUTES": "90",
                "HIRA_WORK_GMAIL_ACTION_SCORE": "2",
            }),
        ):
            asyncio.run(bot.work_gmail_monitor_job(None))

        queue.assert_called_once()
        self.assertEqual(queue.call_args.kwargs["source"], "work_gmail:recent-action")
        seen = set(json.loads(store[bot.WORK_GMAIL_MONITOR_SEEN_KEY]))
        self.assertEqual(seen, {"recent-action", "old-action"})
        status = json.loads(store[bot.WORK_GMAIL_MONITOR_STATUS_KEY])
        self.assertEqual(status["status"], "notified")
        self.assertEqual(status["candidates"], 1)

    def test_work_gmail_monitor_records_checked_status_with_no_new_mail(self):
        store = {bot.WORK_GMAIL_MONITOR_SEEN_KEY: json.dumps(["known"])}
        messages = [{
            "id": "known",
            "from": "Admin <admin@example.com>",
            "subject": "Known",
            "snippet": "Already seen",
            "body": "",
            "date": datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000"),
        }]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "gmail_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot, "_finish_background_job"),
            patch.object(bot.gs, "list_gmail_messages", return_value=messages),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
            patch.object(bot, "_queue_app_notification") as queue,
        ):
            asyncio.run(bot.work_gmail_monitor_job(None))

        queue.assert_not_called()
        self.assertIn(bot.WORK_GMAIL_MONITOR_LAST_RUN_KEY, store)
        status = json.loads(store[bot.WORK_GMAIL_MONITOR_STATUS_KEY])
        self.assertEqual(status["status"], "checked")
        self.assertEqual(status["incoming"], 0)

    def test_work_gmail_monitor_queues_reconnect_notice_on_revoked_token(self):
        store = {}

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "gmail_ok", return_value=True),
            patch.object(bot, "_acquire_job_lock", return_value=True),
            patch.object(bot, "_finish_background_job"),
            patch.object(
                bot.gs,
                "list_gmail_messages",
                side_effect=Exception("invalid_grant: Token has been expired or revoked."),
            ),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
            patch.object(bot, "_queue_app_notification", return_value={"id": "n1", "_push_sent": 1}) as queue,
        ):
            asyncio.run(bot.work_gmail_monitor_job(None))

        queue.assert_called_once()
        self.assertEqual(queue.call_args.kwargs["source"], "work_gmail_monitor:error")
        self.assertIn("reconnect", queue.call_args.args[1].lower())
        status = json.loads(store[bot.WORK_GMAIL_MONITOR_STATUS_KEY])
        self.assertEqual(status["status"], "error")
        self.assertIn("invalid_grant", status["detail"])

    def test_pwa_service_status_marks_revoked_work_gmail_as_reconnect(self):
        with (
            patch.object(web_app.bot, "google_ok", return_value=True),
            patch.object(web_app.bot.gs, "gmail_ok", side_effect=lambda account="personal": True),
            patch.object(web_app.bot, "work_gmail_monitor_status", return_value={
                "enabled": True,
                "connected": True,
                "status": "error",
                "detail": "invalid_grant: Token has been expired or revoked.",
                "last_run": "2026-05-13T06:37:00+08:00",
            }),
            patch.object(web_app.dropbox, "configured", return_value=False),
        ):
            services = web_app._service_status()

        self.assertFalse(services["work_gmail"])
        self.assertTrue(services["_details"]["work_gmail"]["configured"])
        self.assertEqual(services["_details"]["work_gmail"]["state"], "reconnect")
        self.assertEqual(services["_details"]["work_gmail"]["label"], "Reconnect")

    def test_successful_work_gmail_fetch_marks_monitor_healthy(self):
        store = {}
        messages = [{
            "id": "work-1",
            "from": "Admin <admin@example.com>",
            "subject": "Working",
            "snippet": "Token works again.",
            "body": "",
            "date": datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000"),
        }]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "gmail_ok", return_value=True),
            patch.object(bot.gs, "list_gmail_messages", return_value=messages),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
        ):
            result = asyncio.run(bot._execute_tool("get_gmail_brief", {
                "account": "work",
                "query": "",
                "max_items": 5,
            }))

        self.assertIn("Working", result)
        status = json.loads(store[bot.WORK_GMAIL_MONITOR_STATUS_KEY])
        self.assertEqual(status["status"], "checked")
        self.assertEqual(status["source"], "tool_gmail_brief")
        self.assertEqual(status["messages_scanned"], 1)

    def test_prayer_reminder_has_catchup_window(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 1, 13, 18))
        plan = [{
            "key": "zohor",
            "label": "Zohor",
            "time": "13:03",
            "blocked_until": None,
            "note": "Pray as soon as it enters.",
        }]
        store = {}

        with (
            patch.object(bot, "_prayer_plan_for_date", return_value=plan),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
            patch.dict(os.environ, {"HIRA_PRAYER_REMINDER_WINDOW_MINUTES": "20"}),
        ):
            due = bot._prayer_reminder_due(now)

        self.assertEqual(due["key"], "zohor")
        self.assertEqual(store["prayer_prompt:2026-05-01:zohor"], "13:18")

    def test_prayer_reminder_uses_fallback_when_config_unavailable(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 1, 13, 5))
        plan = [{
            "key": "zohor",
            "label": "Zohor",
            "time": "13:03",
            "blocked_until": None,
            "note": "Pray as soon as it enters.",
        }]
        bot._PRAYER_PROMPT_FALLBACK_KEYS.clear()

        with (
            patch.object(bot, "_prayer_plan_for_date", return_value=plan),
            patch.object(bot.gs, "get_config", side_effect=RuntimeError("sheets down")),
            patch.object(bot.gs, "set_config", side_effect=RuntimeError("sheets down")),
        ):
            first = bot._prayer_reminder_due(now)
            second = bot._prayer_reminder_due(now)

        self.assertEqual(first["key"], "zohor")
        self.assertIsNone(second)

    def test_config_reads_are_cached_for_nearby_calls(self):
        class Request:
            def __init__(self, payload=None):
                self.payload = payload or {}

            def execute(self):
                return self.payload

        class Values:
            def __init__(self):
                self.get_calls = 0
                self.updates = []
                self.appends = []

            def get(self, spreadsheetId, range):
                self.get_calls += 1
                return Request({"values": [["proactive_nudges", "[]"], ["foo", "bar"]]})

            def update(self, spreadsheetId, range, valueInputOption, body):
                self.updates.append((range, body))
                return Request()

            def append(self, spreadsheetId, range, valueInputOption, body):
                self.appends.append((range, body))
                return Request()

        class Sheets:
            def __init__(self):
                self.values_api = Values()

            def spreadsheets(self):
                return self

            def values(self):
                return self.values_api

        fake = Sheets()
        bot.gs.invalidate_config_cache()
        with (
            patch.object(bot.gs, "_sheets", return_value=fake),
            patch.object(bot.gs, "_CONFIG_CACHE_TTL_SECONDS", 45),
        ):
            self.assertEqual(bot.gs.get_config("foo"), "bar")
            self.assertEqual(bot.gs.get_config("proactive_nudges"), "[]")
            bot.gs.set_config("foo", "baz")
            self.assertEqual(bot.gs.get_config("foo"), "baz")

        self.assertEqual(fake.values_api.get_calls, 1)
        self.assertEqual(fake.values_api.updates, [("Config!B3", {"values": [["baz"]]})])
        bot.gs.invalidate_config_cache()

    def test_memory_cache_avoids_repeated_config_reads_and_read_before_write(self):
        class Request:
            def __init__(self, payload=None):
                self.payload = payload or {}

            def execute(self):
                return self.payload

        class Values:
            def __init__(self):
                self.get_calls = 0
                self.updates = []

            def get(self, spreadsheetId, range):
                self.get_calls += 1
                if str(range).startswith("MemoryLog!"):
                    return Request({"values": []})
                return Request({
                    "values": [[
                        "assistant_memory",
                        json.dumps({"profile": ["My mum's name is Salwa."]}),
                    ]]
                })

            def update(self, spreadsheetId, range, valueInputOption, body):
                self.updates.append((range, body))
                return Request()

            def append(self, spreadsheetId, range, valueInputOption, body):
                raise AssertionError("assistant_memory should update an existing cached row")

        class Sheets:
            def __init__(self):
                self.values_api = Values()

            def spreadsheets(self):
                return self

            def values(self):
                return self.values_api

        fake = Sheets()
        bot.gs.invalidate_config_cache()
        with (
            patch.object(bot.gs, "_sheets", return_value=fake),
            patch.object(bot.gs, "_CONFIG_CACHE_TTL_SECONDS", 300),
            patch.object(bot.gs, "_MEMORY_CACHE_TTL_SECONDS", 300),
        ):
            first = bot.gs.get_memory()
            second = bot.gs.get_memory()
            bot.gs.add_memory("profile", "Another durable fact.")

        self.assertEqual(first["profile"], ["My mum's name is Salwa."])
        self.assertEqual(second["profile"], ["My mum's name is Salwa."])
        self.assertEqual(fake.values_api.get_calls, 2)
        self.assertEqual(fake.values_api.updates[0][0], "Config!B2")
        saved = json.loads(fake.values_api.updates[0][1]["values"][0][0])
        self.assertIn("Another durable fact.", saved["profile"])
        bot.gs.invalidate_config_cache()

    def test_add_memory_uses_memory_log_when_config_read_quota_is_capped(self):
        class Request:
            def __init__(self, payload=None):
                self.payload = payload or {}

            def execute(self):
                return self.payload

        class Values:
            def __init__(self):
                self.get_calls = 0
                self.appends = []

            def get(self, spreadsheetId, range):
                self.get_calls += 1
                raise RuntimeError("Quota exceeded for quota metric 'Read requests' reason: RATE_LIMIT_EXCEEDED")

            def append(self, spreadsheetId, range, valueInputOption, body):
                self.appends.append((range, body))
                return Request()

        class Sheets:
            def __init__(self):
                self.values_api = Values()

            def spreadsheets(self):
                return self

            def values(self):
                return self.values_api

        fake = Sheets()
        bot.gs.invalidate_config_cache()
        with (
            patch.object(bot.gs, "_sheets", return_value=fake),
            patch.object(bot.gs, "_CONFIG_CACHE_TTL_SECONDS", 300),
            patch.object(bot.gs, "_MEMORY_CACHE_TTL_SECONDS", 300),
        ):
            memory = bot.gs.add_memory("profile", "Quota fallback fact.")

        self.assertEqual(fake.values_api.get_calls, 1)
        self.assertEqual(fake.values_api.appends[0][0], "MemoryLog!A:D")
        self.assertEqual(fake.values_api.appends[0][1]["values"][0][1:3], ["profile", "Quota fallback fact."])
        self.assertIn("Quota fallback fact.", memory["profile"])
        bot.gs.invalidate_config_cache()

    def test_add_nudge_uses_redis_fallback_when_sheets_are_capped(self):
        stored = []

        def capture_redis_nudges(nudges):
            stored[:] = nudges
            return True

        with (
            patch.object(bot.gs, "_sheet_nudges", side_effect=RuntimeError("quota exceeded")),
            patch.object(bot.gs, "_redis_nudges", return_value=[]),
            patch.object(bot.gs, "_set_redis_nudges", side_effect=capture_redis_nudges),
        ):
            nudge = bot.gs.add_nudge("Evening digest", "2026-05-05T22:04:00+08:00")

        self.assertTrue(nudge["id"].startswith("r-"))
        self.assertEqual(nudge["message"], "Evening digest")
        self.assertEqual(stored, [nudge])

if __name__ == "__main__":
    unittest.main()
