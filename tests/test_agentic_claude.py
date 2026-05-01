import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")

import bot
import islamic_service
import pdf_service
import weather_service
import web_app


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


class AgenticClaudeTests(unittest.TestCase):
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

    def test_email_followup_forces_gmail_before_action(self):
        messages = [{"role": "user", "content": "read my latest work email and note the meeting details for follow up"}]
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

    def test_gmail_account_extraction_detects_work_email(self):
        account, query = bot._extract_gmail_account_from_text("show my last 5 work emails")

        self.assertEqual(account, "work")
        self.assertEqual(query, "show my last 5")

    def test_work_gmail_can_reuse_personal_oauth_client(self):
        env = {
            "GOOGLE_GMAIL_CLIENT_ID": "client",
            "GOOGLE_GMAIL_CLIENT_SECRET": "secret",
            "GOOGLE_WORK_GMAIL_REFRESH_TOKEN": "work-refresh",
        }

        with patch.dict(os.environ, env, clear=True):
            self.assertTrue(bot.gs.gmail_ok("work"))
            self.assertFalse(bot.gs.gmail_ok("personal"))

if __name__ == "__main__":
    unittest.main()
