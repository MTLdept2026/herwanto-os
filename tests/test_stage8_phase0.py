import asyncio
import os
import unittest
from datetime import datetime
from unittest.mock import patch

import bot
import postgres_storage


class _FrozenDateTime(datetime):
    value = None

    @classmethod
    def now(cls, tz=None):
        current = cls.value
        if tz is not None and current.tzinfo is not None:
            return current.astimezone(tz)
        return current


class Stage8Phase0TargetTests(unittest.TestCase):
    def test_prompt_static_prefix_precedes_fresh_dynamic_tail(self):
        first_now = bot.SGT.localize(datetime(2026, 7, 10, 10, 5))
        second_now = bot.SGT.localize(datetime(2026, 7, 10, 10, 6))
        static_marker = "You are Herwanto's personal AI assistant"

        with patch.object(bot, "memory_ok", return_value=False), \
             patch.object(bot, "WORK_DRIVE_REFERENCES", []), \
             patch.object(bot, "datetime", _FrozenDateTime):
            _FrozenDateTime.value = first_now
            first = bot.SYSTEM_PROMPT()
            _FrozenDateTime.value = second_now
            second = bot.SYSTEM_PROMPT()

        self.assertLess(first.index(static_marker), first.index("Today is"))
        self.assertIn("10:05 SGT", first)
        self.assertIn("10:06 SGT", second)

    @unittest.expectedFailure
    def test_structured_memory_entries_render_and_roundtrip_without_stringifying(self):
        entry = {
            "value": "Use source-backed tools before answering current facts.",
            "source": "unit",
            "confidence": "high",
        }

        self.assertEqual(
            bot._memory_item_text(entry),
            "Use source-backed tools before answering current facts.",
        )
        formatted = bot._format_memory({"preferences": [entry]})
        self.assertIn("- Use source-backed tools before answering current facts.", formatted)
        self.assertNotIn("{'value'", formatted)
        self.assertEqual(postgres_storage._coerce_items([entry]), [entry])

    @unittest.expectedFailure
    def test_shadow_family_logs_after_all_dispatch_gates_and_never_dispatches(self):
        candidate = {
            "family": "prep_gap",
            "source": "prep_gap:2G3:2026-07-10:10:50",
            "kind": "update",
            "title": "Prep gap",
            "body": "2G3 ML in 20m and no worksheet uploaded yet.",
            "priority": "high",
            "confidence": "probably_remind",
            "metadata": {"class_name": "2G3"},
        }

        with patch.dict(os.environ, {"HIRA_SHADOW_FAMILIES": "prep_gap"}, clear=False), \
             patch.object(bot, "_calendar_candidate_is_stale", return_value=False), \
             patch.object(bot, "_notification_is_learned_muted", return_value=False), \
             patch.object(bot, "_devotional_notification_block_reason", return_value=""), \
             patch.object(bot, "_calendar_notification_block_reason", return_value=""), \
             patch.object(bot.gs, "add_shadow_log", create=True) as add_shadow_log, \
             patch.object(bot, "_queue_app_notification", side_effect=AssertionError("shadow candidate dispatched")):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))

        self.assertEqual(sent, 0)
        add_shadow_log.assert_called_once()

    @unittest.expectedFailure
    def test_outcome_v2_dual_write_accepts_candidate_metadata(self):
        metadata = {"family": "prep_gap", "class_name": "2G3"}
        with patch.object(bot.gs, "add_notification_outcome", return_value=[]), \
             patch.object(bot.gs, "add_notification_outcome_v2", create=True, return_value=[]) as add_v2:
            bot._record_notification_outcome(
                "queued",
                source="prep_gap:2G3:2026-07-10:10:50",
                kind="update",
                title="Prep gap",
                candidate_id="cand-1",
                metadata=metadata,
            )

        add_v2.assert_called_once()
        self.assertEqual(add_v2.call_args.kwargs["candidate_id"], "cand-1")
        self.assertEqual(add_v2.call_args.kwargs["metadata"], metadata)

    @unittest.expectedFailure
    def test_quality_eval_job_judges_stored_answers_without_agentic_rerun(self):
        with patch.object(bot, "_run_agentic_chat", side_effect=AssertionError("agentic rerun is unsafe")), \
             patch.object(bot, "_llm_text", return_value='{"results": []}'):
            bot.quality_eval_job(context=None)


if __name__ == "__main__":
    unittest.main()
