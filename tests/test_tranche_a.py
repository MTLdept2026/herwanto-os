import asyncio
import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("OPENAI_API_KEY", "test-key")

import bot


class _FakeMessage:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class _FakeBot:
    async def send_chat_action(self, **_kwargs):
        return None


def _fake_update():
    message = _FakeMessage()
    return SimpleNamespace(
        message=message,
        effective_user=SimpleNamespace(id=42),
        effective_chat=SimpleNamespace(id=123),
    )


def _fake_context(args=None):
    return SimpleNamespace(args=list(args or []), bot=_FakeBot())


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = bot.SGT.localize(datetime(2026, 6, 10, 9, 0))
        if tz is not None:
            return current.astimezone(tz)
        return current.replace(tzinfo=None)


class TrancheATests(unittest.TestCase):
    def setUp(self):
        bot._LATENCY_RECENT.clear()

    def tearDown(self):
        bot._LATENCY_RECENT.clear()

    def test_today_cmd_offloads_blocking_io(self):
        update = _fake_update()
        inside_to_thread = {"active": False}
        offloaded = []

        async def recording_to_thread(fn, *args, **kwargs):
            offloaded.append(getattr(fn, "__name__", repr(fn)))
            inside_to_thread["active"] = True
            try:
                return fn(*args, **kwargs)
            finally:
                inside_to_thread["active"] = False

        def get_today_events():
            self.assertTrue(inside_to_thread["active"])
            return [{"summary": "Assembly"}]

        def format_events(events, **_kwargs):
            self.assertTrue(inside_to_thread["active"])
            self.assertEqual(events, [{"summary": "Assembly"}])
            return "10:00 Assembly"

        with patch.object(bot.asyncio, "to_thread", side_effect=recording_to_thread), \
             patch.object(bot, "datetime", _FrozenDateTime), \
             patch.object(bot, "google_ok", return_value=True), \
             patch.object(bot, "_lessons_for_date", return_value=([], "")), \
             patch.object(bot.gs, "get_today_events", side_effect=get_today_events), \
             patch.object(bot.gs, "format_events", side_effect=format_events):
            asyncio.run(bot.today_cmd(update, _fake_context()))

        self.assertEqual(offloaded, ["_today_payload"])
        self.assertEqual(update.message.replies, [(
            "*Wednesday, 10 June 2026*\n\n"
            "_Timetable: use /setweek to activate_\n\n"
            "*Calendar:*\n"
            "10:00 Assembly",
            {"parse_mode": "Markdown"},
        )])

    def test_process_user_text_no_direct_gs_on_loop(self):
        update = _fake_update()
        inside_to_thread = {"active": False}
        offloaded = []

        async def recording_to_thread(fn, *args, **kwargs):
            offloaded.append(getattr(fn, "__name__", repr(fn)))
            inside_to_thread["active"] = True
            try:
                return fn(*args, **kwargs)
            finally:
                inside_to_thread["active"] = False

        def get_history(_user_id):
            self.assertTrue(inside_to_thread["active"])
            return []

        def absorb(_text):
            self.assertTrue(inside_to_thread["active"])

        def set_week(week_type, week_number=None):
            self.assertTrue(inside_to_thread["active"])
            self.assertEqual((week_type, week_number), ("odd", None))
            return "2026-06-08"

        with patch.object(bot.asyncio, "to_thread", side_effect=recording_to_thread), \
             patch.object(bot, "get_history", side_effect=get_history), \
             patch.object(bot, "absorb_taste_hint", side_effect=absorb), \
             patch.object(bot, "google_ok", return_value=True), \
             patch.object(bot, "_set_current_school_week", side_effect=set_week):
            asyncio.run(bot._process_user_text(update, _fake_context(), "this week is odd"))

        self.assertEqual(offloaded, ["_process_user_text_preflight"])
        self.assertEqual(update.message.replies, [(
            "Locked in. this week is *ODD* week for the timetable.",
            {"parse_mode": "Markdown"},
        )])

    def test_measure_latency_records_total_and_marks(self):
        with patch.object(bot.time, "perf_counter", side_effect=[1.0, 1.123]), \
             self.assertLogs(bot.logger, level="INFO") as logs:
            with bot._measure_latency("unit", chars=5) as marks:
                marks["first_token_ms"] = 17

        self.assertEqual(len(bot._LATENCY_RECENT), 1)
        entry = bot._LATENCY_RECENT[-1]
        self.assertEqual(entry["route"], "unit")
        self.assertEqual(entry["chars"], 5)
        self.assertEqual(entry["first_token_ms"], 17)
        self.assertEqual(entry["t_total_ms"], 123)
        self.assertTrue(any("HIRA_LATENCY" in line for line in logs.output))

    def test_runtime_status_latency_block(self):
        bot._LATENCY_RECENT.extend([
            {"route": "telegram_chat", "t_total_ms": 100},
            {"route": "telegram_chat", "t_total_ms": 200},
            {"route": "telegram_chat", "t_total_ms": 300},
            {"route": "tool", "t_total_ms": 50},
        ])

        with patch.object(bot, "google_ok", return_value=False), \
             patch.object(bot.gs, "memory_storage_status", return_value={"connected": False, "source": "unavailable"}), \
             patch.object(bot.gs, "gmail_ok", return_value=False), \
             patch.object(bot.ss, "search_enabled", return_value=False), \
             patch.object(bot, "_get_redis", return_value=None), \
             patch.object(bot, "openai_usage_status", return_value={}), \
             patch.object(bot, "api_usage_status", return_value={}):
            status = bot.build_runtime_status()

        self.assertEqual(status["latency"]["count"], 4)
        self.assertEqual(status["latency"]["routes"]["telegram_chat"], {
            "count": 3,
            "p50_ms": 200,
            "p95_ms": 300,
        })
        self.assertEqual(status["latency"]["routes"]["tool"], {
            "count": 1,
            "p50_ms": 50,
            "p95_ms": 50,
        })


if __name__ == "__main__":
    unittest.main()
