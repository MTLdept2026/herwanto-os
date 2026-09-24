import unittest
from datetime import date, datetime

import bot
import timetable as tt
import web_app


class TimetableInactiveTests(unittest.TestCase):
    def test_past_lessons_remain_available(self):
        past = date(2026, 9, 23)
        self.assertTrue(tt.is_timetable_active(past))
        self.assertTrue(bot._lessons_for_date(past)[0])
        self.assertTrue(web_app._home_lessons_for_date(past)[0])

    def test_ended_timetable_does_not_feed_current_or_future_days(self):
        for target in (date(2026, 9, 24), date(2026, 10, 2), date(2027, 1, 4)):
            with self.subTest(target=target):
                self.assertFalse(tt.is_timetable_active(target))
                self.assertEqual(bot._lessons_for_date(target), ([], ""))
                self.assertEqual(web_app._home_lessons_for_date(target), ([], ""))
                self.assertEqual(tt.get_lessons(target, "2026-09-21", "even"), [])
                self.assertEqual(bot._hbl_status_line(target), "")

    def test_old_odd_friday_hbl_rule_does_not_extend_past_lessons(self):
        self.assertTrue(tt.get_school_week_info(date(2026, 7, 3))["is_hbl"])
        self.assertFalse(tt.get_school_week_info(date(2026, 10, 2))["is_hbl"])
        self.assertNotIn("Odd week", bot._agenda_week_display(date(2026, 10, 2)))

    def test_next_lesson_companion_has_no_current_lesson(self):
        now = bot.SGT.localize(datetime(2026, 9, 24, 10, 20))
        companion = bot.build_next_lesson_companion(now)
        self.assertIsNone(companion["current_lesson"])
        self.assertIsNone(companion["next_lesson"])
        self.assertEqual(companion["files"], [])


if __name__ == "__main__":
    unittest.main()
