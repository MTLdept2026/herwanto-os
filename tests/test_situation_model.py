import asyncio
import unittest
from datetime import datetime
from unittest.mock import patch

import bot
import web_app


def _lesson(start, end, subject="ML", description="2G3", room="L4-12"):
    return {
        "start": start,
        "end": end,
        "subject": subject,
        "description": description,
        "room": room,
    }


class SituationModelTests(unittest.TestCase):
    def setUp(self):
        self.now = bot.SGT.localize(datetime(2026, 7, 7, 10, 20))

    def test_next_lesson_companion_computes_current_next_and_countdown(self):
        lessons = [
            _lesson("10:15", "10:50", description="2G3"),
            _lesson("10:50", "11:25", description="2G3"),
        ]

        with patch.object(bot, "_lessons_for_date", return_value=(lessons, "Odd")), \
             patch.object(bot, "_visible_lessons_for_date", return_value=lessons), \
             patch.object(bot.dropbox, "configured", return_value=False):
            result = bot.build_next_lesson_companion(self.now)

        self.assertEqual(result["current_lesson"]["class"], "2G3")
        self.assertEqual(result["current_lesson"]["start"], "10:15")
        self.assertEqual(result["next_lesson"]["start"], "10:50")
        self.assertEqual(result["next_lesson"]["minutes_until"], 30)
        self.assertEqual(result["files"], [])
        self.assertEqual(result["source"], "timetable")

    def test_next_lesson_companion_handles_no_lesson_and_end_of_day(self):
        with patch.object(bot, "_lessons_for_date", return_value=([], "Odd")), \
             patch.object(bot, "_visible_lessons_for_date", return_value=[]), \
             patch.object(bot.dropbox, "configured", return_value=False):
            empty = bot.build_next_lesson_companion(self.now)

        past_lessons = [_lesson("08:00", "08:35", description="3G3")]
        with patch.object(bot, "_lessons_for_date", return_value=(past_lessons, "Odd")), \
             patch.object(bot, "_visible_lessons_for_date", return_value=past_lessons), \
             patch.object(bot.dropbox, "configured", return_value=False):
            ended = bot.build_next_lesson_companion(self.now)

        self.assertIsNone(empty["current_lesson"])
        self.assertIsNone(empty["next_lesson"])
        self.assertIsNone(ended["current_lesson"])
        self.assertIsNone(ended["next_lesson"])
        self.assertEqual(ended["files"], [])

    def test_situation_model_degrades_one_failed_source_without_writes(self):
        with patch.object(bot, "build_next_lesson_companion", return_value={"current_lesson": None, "next_lesson": None}), \
             patch.object(bot, "build_agenda_structured", return_value={"days": [{"date": "2026-07-07"}]}), \
             patch.object(bot, "build_task_structured", side_effect=RuntimeError("task source down")), \
             patch.object(bot, "build_classops_status_summary", return_value={"classes": []}), \
             patch.object(bot, "_build_prayer_situation", return_value={"schedule": [], "next": None}), \
             patch.object(bot.ws, "build_weather_brief", return_value="Weather calm."), \
             patch.object(bot, "_build_situation_flags", return_value={"google": True}), \
             patch.object(bot, "_queue_app_notification", side_effect=AssertionError("no notification writes")), \
             patch.object(bot.gs, "set_config", side_effect=AssertionError("no config writes")):
            result = bot.build_situation_model(self.now)

        self.assertTrue(result["lesson"]["ok"])
        self.assertTrue(result["agenda"]["ok"])
        self.assertFalse(result["tasks"]["ok"])
        self.assertIn("task source down", result["tasks"]["error"])
        self.assertTrue(result["weather"]["ok"])

    def test_submission_risk_candidate_for_overdue_outstanding_assignment(self):
        summary = {
            "classes": [{
                "class_name": "2G3",
                "assignments": [{
                    "id": "karangan-1",
                    "assignment_title": "Karangan",
                    "collect_by": "2026-07-06",
                    "non_submitted": ["Ali", "Bala"],
                }],
            }],
        }

        with patch.object(bot, "build_classops_status_summary", return_value=summary), \
             patch.object(bot, "_notification_feedback_bias", return_value=0), \
             patch.object(bot, "_should_suppress_notification", return_value=False):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"submission_risk"})

        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["family"], "submission_risk")
        self.assertEqual(queue[0]["source"], "submission_risk:2G3:karangan-1")
        self.assertEqual(queue[0]["priority"], "high")
        self.assertIn("2 not submitted", queue[0]["body"])

    def test_submission_risk_respects_existing_suppression_before_dispatch(self):
        summary = {
            "classes": [{
                "class_name": "2G3",
                "assignments": [{
                    "id": "latihan-1",
                    "assignment_title": "Latihan",
                    "collect_by": "2026-07-07",
                    "non_submitted": ["Ali"],
                }],
            }],
        }

        with patch.object(bot, "build_classops_status_summary", return_value=summary), \
             patch.object(bot, "_notification_feedback_bias", return_value=0), \
             patch.object(bot, "_should_suppress_notification", return_value=True):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"submission_risk"})

        self.assertTrue(queue[0]["suppressed"])
        with patch.object(bot, "_queue_app_notification", side_effect=AssertionError("suppressed candidate dispatched")):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, queue, limit=1))
        self.assertEqual(sent, 0)

    def test_lesson_now_endpoint_uses_auth_and_threaded_builder(self):
        with patch.object(web_app, "_require_token") as require_token, \
             patch.object(web_app.bot, "build_next_lesson_companion", return_value={"current_lesson": None}) as builder:
            result = asyncio.run(web_app.lesson_now(x_hira_token="token"))

        require_token.assert_called_once_with("token")
        builder.assert_called_once()
        self.assertEqual(result, {"current_lesson": None})


if __name__ == "__main__":
    unittest.main()
