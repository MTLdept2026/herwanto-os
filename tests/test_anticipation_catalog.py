import asyncio
import unittest
from datetime import datetime
from unittest.mock import patch

import bot


def _lesson(start, end, subject="ML", description="2G3", room="L4-12"):
    return {
        "start": start,
        "end": end,
        "subject": subject,
        "description": description,
        "room": room,
    }


def _event(event_id, summary, start, end):
    return {
        "id": event_id,
        "summary": summary,
        "start": {"dateTime": start},
        "end": {"dateTime": end},
    }


def _all_day_event(event_id, summary, start, end):
    return {
        "id": event_id,
        "summary": summary,
        "start": {"date": start},
        "end": {"date": end},
    }


class AnticipationCatalogTests(unittest.TestCase):
    def setUp(self):
        self.now = bot.SGT.localize(datetime(2026, 7, 7, 10, 20))

    def _candidate_context(self, suppressed=False):
        return (
            patch.object(bot, "_notification_feedback_bias", return_value=0),
            patch.object(bot, "_should_suppress_notification", return_value=suppressed),
        )

    def test_prep_gap_fires_for_near_teaching_lesson_without_files(self):
        companion = {
            "next_lesson": {
                "class": "2G3",
                "subject": "ML",
                "start": "10:50",
                "minutes_until": 30,
            },
            "files": [],
            "source": "timetable+classops",
        }

        feedback, suppress = self._candidate_context()
        with feedback, suppress, \
             patch.object(bot, "build_next_lesson_companion", return_value=companion):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"prep_gap"})

        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["family"], "prep_gap")
        self.assertEqual(queue[0]["source"], "prep_gap:2G3:2026-07-07:10:50")
        self.assertEqual(queue[0]["priority"], "medium")
        self.assertIn("no worksheet", queue[0]["body"])

    def test_prep_gap_stays_silent_when_files_exist_or_subject_is_not_teaching(self):
        with_files = {
            "next_lesson": {"class": "2G3", "subject": "ML", "start": "10:50", "minutes_until": 30},
            "files": [{"title": "Worksheet"}],
            "source": "timetable+classops",
        }
        non_teaching = {
            "next_lesson": {"class": "2G3", "subject": "CCE", "start": "10:50", "minutes_until": 30},
            "files": [],
            "source": "timetable+classops",
        }

        feedback, suppress = self._candidate_context()
        with feedback, suppress, \
             patch.object(bot, "build_next_lesson_companion", side_effect=[with_files, non_teaching]):
            self.assertEqual(bot.build_proactive_v2_queue(now=self.now, families={"prep_gap"}), [])
            self.assertEqual(bot.build_proactive_v2_queue(now=self.now, families={"prep_gap"}), [])

    def test_stale_data_fires_for_old_classops_manifest_and_stays_silent_when_fresh(self):
        stale_status = {"available": True, "fresh": False, "age_seconds": 7201, "ttl_seconds": 600}
        fresh_status = {"available": True, "fresh": True, "age_seconds": 30, "ttl_seconds": 600}

        feedback, suppress = self._candidate_context()
        with feedback, suppress, \
             patch.object(bot.dropbox, "configured", return_value=True), \
             patch.object(bot.dropbox, "classops_manifest_cache_status", return_value=stale_status):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"stale_data"})

        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["family"], "stale_data")
        self.assertEqual(queue[0]["source"], "stale_data:classops_manifest")
        self.assertEqual(queue[0]["priority"], "low")

        with feedback, suppress, \
             patch.object(bot.dropbox, "configured", return_value=True), \
             patch.object(bot.dropbox, "classops_manifest_cache_status", return_value=fresh_status):
            self.assertEqual(bot.build_proactive_v2_queue(now=self.now, families={"stale_data"}), [])

    def test_calendar_conflict_fires_for_event_lesson_overlap(self):
        events = [_event(
            "evt-1",
            "Parent call",
            "2026-07-07T10:40:00+08:00",
            "2026-07-07T11:00:00+08:00",
        )]

        feedback, suppress = self._candidate_context()
        with feedback, suppress, \
             patch.object(bot.gs, "get_events_for_days", return_value=events), \
             patch.object(bot, "_lessons_for_date", side_effect=lambda target: ([_lesson("10:50", "11:25")], "Odd") if target == self.now.date() else ([], "Odd")):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"calendar_conflict"})

        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["family"], "calendar_conflict")
        self.assertTrue(queue[0]["source"].startswith("calendar_conflict:2026-07-07:"))
        self.assertEqual(queue[0]["priority"], "high")
        self.assertIn("overlaps", queue[0]["body"])

    def test_calendar_conflict_ignores_back_to_back_and_all_day_events(self):
        back_to_back = [_event(
            "evt-1",
            "Parent call",
            "2026-07-07T10:20:00+08:00",
            "2026-07-07T10:50:00+08:00",
        )]
        all_day = [_all_day_event("evt-2", "School holiday", "2026-07-07", "2026-07-08")]

        feedback, suppress = self._candidate_context()
        with feedback, suppress, \
             patch.object(bot.gs, "get_events_for_days", side_effect=[back_to_back, all_day]), \
             patch.object(bot, "_lessons_for_date", side_effect=lambda target: ([_lesson("10:50", "11:25")], "Odd") if target == self.now.date() else ([], "Odd")):
            self.assertEqual(bot.build_proactive_v2_queue(now=self.now, families={"calendar_conflict"}), [])
            self.assertEqual(bot.build_proactive_v2_queue(now=self.now, families={"calendar_conflict"}), [])

    def test_marking_crunch_fires_from_script_load_and_stays_silent_below_threshold(self):
        heavy = [
            {"total_scripts": 60, "marked_count": 10, "stack_count": 2},
            {"total_scripts": 45, "marked_count": 15, "stack_count": 2},
        ]
        light = [{"total_scripts": 30, "marked_count": 5, "stack_count": 1}]

        feedback, suppress = self._candidate_context()
        with feedback, suppress, patch.object(bot.gs, "get_marking_tasks", return_value=heavy):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"marking_crunch"})

        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["family"], "marking_crunch")
        self.assertEqual(queue[0]["source"], "marking_crunch")
        self.assertEqual(queue[0]["priority"], "medium")
        self.assertIn("~80 scripts", queue[0]["body"])

        with feedback, suppress, patch.object(bot.gs, "get_marking_tasks", return_value=light):
            self.assertEqual(bot.build_proactive_v2_queue(now=self.now, families={"marking_crunch"}), [])

    def test_marking_crunch_can_fire_from_stack_count(self):
        stacks = [{"total_scripts": 20, "marked_count": 10, "stack_count": 4}]

        feedback, suppress = self._candidate_context()
        with feedback, suppress, patch.object(bot.gs, "get_marking_tasks", return_value=stacks):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"marking_crunch"})

        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["metadata"]["active_stacks"], 4)

    def test_suppressed_candidate_is_not_dispatched(self):
        companion = {
            "next_lesson": {
                "class": "2G3",
                "subject": "ML",
                "start": "10:40",
                "minutes_until": 20,
            },
            "files": [],
            "source": "timetable+classops",
        }

        feedback, suppress = self._candidate_context(suppressed=True)
        with feedback, suppress, patch.object(bot, "build_next_lesson_companion", return_value=companion):
            queue = bot.build_proactive_v2_queue(now=self.now, families={"prep_gap"})

        self.assertTrue(queue[0]["suppressed"])
        with patch.object(bot, "_queue_app_notification", side_effect=AssertionError("suppressed candidate dispatched")):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, queue, limit=1))
        self.assertEqual(sent, 0)

    def test_candidate_builders_are_read_only(self):
        companion = {
            "next_lesson": {"class": "2G3", "subject": "ML", "start": "10:50", "minutes_until": 30},
            "files": [],
            "source": "timetable+classops",
        }
        stale_status = {"available": True, "fresh": False, "age_seconds": 7201, "ttl_seconds": 600}
        events = [_event("evt-1", "Parent call", "2026-07-07T10:40:00+08:00", "2026-07-07T11:00:00+08:00")]
        marking = [{"total_scripts": 90, "marked_count": 0, "stack_count": 2}]

        feedback, suppress = self._candidate_context()
        with feedback, suppress, \
             patch.object(bot, "_queue_app_notification", side_effect=AssertionError("no notification writes")), \
             patch.object(bot.gs, "set_config", side_effect=AssertionError("no config writes")), \
             patch.object(bot, "build_next_lesson_companion", return_value=companion), \
             patch.object(bot.dropbox, "configured", return_value=True), \
             patch.object(bot.dropbox, "classops_manifest_cache_status", return_value=stale_status), \
             patch.object(bot.gs, "get_events_for_days", return_value=events), \
             patch.object(bot, "_lessons_for_date", side_effect=lambda target: ([_lesson("10:50", "11:25")], "Odd") if target == self.now.date() else ([], "Odd")), \
             patch.object(bot.gs, "get_marking_tasks", return_value=marking):
            self.assertEqual(len(bot._prep_gap_candidates(self.now)), 1)
            self.assertEqual(len(bot._stale_data_candidates(self.now)), 1)
            self.assertEqual(len(bot._calendar_conflict_candidates(self.now)), 1)
            self.assertEqual(len(bot._marking_crunch_candidates(self.now)), 1)


if __name__ == "__main__":
    unittest.main()
