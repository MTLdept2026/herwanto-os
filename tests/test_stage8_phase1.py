import json
import os
import unittest
from datetime import datetime
from unittest.mock import patch

import bot


class _FrozenDateTime(datetime):
    value = None

    @classmethod
    def now(cls, tz=None):
        current = cls.value
        if tz is not None and current.tzinfo is not None:
            return current.astimezone(tz)
        return current


class Stage8Phase1Tests(unittest.TestCase):
    def test_quality_trend_default_off_does_not_touch_storage(self):
        with patch.dict(os.environ, {}, clear=True), \
             patch.object(bot.gs, "get_quality_signals", side_effect=AssertionError("quality storage read")), \
             patch.object(bot.gs, "get_config", side_effect=AssertionError("quality counter read")):
            summary = bot._quality_trend_summary()

        self.assertFalse(summary["enabled"])
        self.assertEqual(summary["signals_total"], 0)

    def test_self_repair_quality_signal_is_flag_gated_and_signal_dense(self):
        with patch.dict(os.environ, {"HIRA_QUALITY_SIGNALS": "1"}, clear=False), \
             patch.object(bot.gs, "add_quality_signal", return_value=[]) as add_quality_signal:
            bot.record_self_repair_quality_signal(
                "hello",
                {"repaired": False, "verdict": {"flags": []}},
                surface="pwa",
                route="chat",
            )
            add_quality_signal.assert_not_called()

            bot.record_self_repair_quality_signal(
                "hello",
                {"repaired": True, "verdict": {"flags": ["generic"], "reason": "too generic"}},
                surface="pwa",
                route="chat",
            )

        add_quality_signal.assert_called_once()
        payload = add_quality_signal.call_args.args[0]
        self.assertEqual(payload["kind"], "self_repair")
        self.assertEqual(payload["surface"], "pwa")
        self.assertEqual(payload["route"], "chat")
        self.assertEqual(payload["flags"], ["generic"])
        self.assertTrue(payload["repaired"])
        self.assertEqual(payload["user_chars"], 5)
        self.assertNotIn("reply_text", payload)

    def test_guardrail_counter_retains_last_eight_iso_weeks(self):
        existing = {
            f"2026-W{week:02d}": {"guardrails": {"backend_claim": 1}, "total": 1}
            for week in range(1, 9)
        }
        _FrozenDateTime.value = bot.SGT.localize(datetime(2026, 3, 4, 10, 0))

        with patch.dict(os.environ, {"HIRA_QUALITY_SIGNALS": "1"}, clear=False), \
             patch.object(bot, "datetime", _FrozenDateTime), \
             patch.object(bot.gs, "get_config", return_value=json.dumps(existing)), \
             patch.object(bot.gs, "set_config") as set_config:
            bot._increment_quality_guardrail_counter("backend_claim", surface="openai", route="agentic")

        set_config.assert_called_once()
        self.assertEqual(set_config.call_args.args[0], bot.QUALITY_GUARDRAIL_COUNTERS_KEY)
        payload = json.loads(set_config.call_args.args[1])
        self.assertEqual(len(payload), 8)
        self.assertNotIn("2026-W01", payload)
        self.assertIn("2026-W10", payload)
        self.assertEqual(payload["2026-W10"]["guardrails"]["backend_claim"], 1)
        self.assertEqual(payload["2026-W10"]["surfaces"]["openai"], 1)
        self.assertEqual(payload["2026-W10"]["routes"]["agentic"], 1)

    def test_quality_trend_summary_counts_recent_signals_and_current_guardrails(self):
        _FrozenDateTime.value = bot.SGT.localize(datetime(2026, 7, 10, 17, 15))
        counters = {
            "2026-W28": {
                "guardrails": {"backend_claim": 2, "weekday_date": 1},
                "total": 3,
            }
        }
        signals = [
            {"created": "2026-07-01T10:00:00+08:00", "kind": "self_repair", "repaired": True},
            {"created": "2026-07-10T10:00:00+08:00", "kind": "self_repair", "repaired": True},
        ]

        with patch.dict(os.environ, {"HIRA_QUALITY_SIGNALS": "1"}, clear=False), \
             patch.object(bot, "datetime", _FrozenDateTime), \
             patch.object(bot.gs, "get_quality_signals", return_value=signals), \
             patch.object(bot.gs, "get_config", return_value=json.dumps(counters)):
            summary = bot._quality_trend_summary()

        self.assertTrue(summary["enabled"])
        self.assertEqual(summary["signals_total"], 2)
        self.assertEqual(summary["signals_7d"], 1)
        self.assertEqual(summary["self_repair_rewrites_7d"], 1)
        self.assertEqual(summary["guardrail_current_week_total"], 3)
        self.assertEqual(summary["guardrail_current_week_counters"]["weekday_date"], 1)


if __name__ == "__main__":
    unittest.main()
