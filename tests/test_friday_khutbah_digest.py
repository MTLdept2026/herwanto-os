import unittest
from datetime import datetime
from unittest.mock import patch

import bot
import islamic_service as isl


FRIDAY = bot.SGT.localize(datetime(2026, 9, 25, 6, 45))
KHUTBAH = {
    "date": "2026-09-25",
    "title": "Building resilient families: Balancing foresight and faith",
    "summary": "Plan responsibly and place trust in Allah.",
    "url": "https://www.muis.gov.sg/resources/khutbah-and-religious-advice/khutbah/building-resilient-families--balancing-foresight-and-faith-/",
}


class FridayKhutbahDigestTests(unittest.TestCase):
    def test_parser_accepts_current_muis_language_markup(self):
        listing = '''
        <a href="/resources/khutbah-and-religious-advice/khutbah/building-resilient-families-/">
          <p class="prose-label-md-regular">25 September 2026</p>
          <span class="line-clamp-3" title="Building resilient families ">Building resilient families</span>
          <p class="prose-body-base line-clamp-3">Plan responsibly and trust Allah.</p>
          <div><span>English</span></div>
        </a>
        '''
        records = isl._parse_khutbah_listing(listing)
        self.assertEqual(records[0]["date"], "2026-09-25")
        self.assertEqual(records[0]["title"], "Building resilient families")
        self.assertEqual(records[0]["language"], "English")

    def test_friday_sermon_leads_morning_and_pwa_digests(self):
        with patch.object(bot, "build_curated_digest_entries", return_value=[]), \
             patch.object(bot.isl, "latest_khutbah", return_value=KHUTBAH):
            morning = bot._fresh_morning_digest(now=FRIDAY)
            pwa = bot.build_curated_digest_snapshot(now=FRIDAY)

        self.assertIn("Friday khutbah: Building resilient families", morning)
        self.assertIn("Source: https://www.muis.gov.sg/", morning)
        self.assertEqual(pwa["items"][0]["title"], KHUTBAH["title"])
        self.assertEqual(pwa["items"][0]["url"], KHUTBAH["url"])

    def test_previous_week_sermon_is_not_reused(self):
        with patch.object(bot.isl, "latest_khutbah", return_value={**KHUTBAH, "date": "2026-09-18"}):
            self.assertIsNone(bot._friday_khutbah_digest_item(FRIDAY.date()))

    def test_sermon_survives_news_digest_failure(self):
        with patch.object(bot, "build_curated_digest_entries", side_effect=RuntimeError("news unavailable")), \
             patch.object(bot.isl, "latest_khutbah", return_value=KHUTBAH), \
             patch.object(bot.logger, "warning"):
            morning = bot._fresh_morning_digest(now=FRIDAY)
            pwa = bot.build_curated_digest_snapshot(now=FRIDAY)

        self.assertIn(KHUTBAH["title"], morning)
        self.assertEqual(pwa["items"][0]["source"], "MUIS")

    def test_full_morning_briefing_shows_sermon_once_in_digest(self):
        with patch.object(bot, "datetime", wraps=bot.datetime) as clock, \
             patch.object(bot, "build_curated_digest_entries", return_value=[]), \
             patch.object(bot.isl, "latest_khutbah", return_value=KHUTBAH), \
             patch.object(bot, "build_islamic_brief", return_value="Prayer rhythm") as islamic_brief, \
             patch.object(bot, "google_ok", return_value=False), \
             patch.object(bot, "conversation_carryover_brief_lines", return_value=[]):
            clock.now.return_value = FRIDAY
            briefing = bot.build_briefing()

        self.assertEqual(briefing.count(KHUTBAH["title"]), 1)
        self.assertLess(briefing.index("*Morning digest:*"), briefing.index(KHUTBAH["title"]))
        islamic_brief.assert_called_once_with(FRIDAY.date(), include_khutbah=False)


if __name__ == "__main__":
    unittest.main()
