import asyncio
import json
import unittest
from datetime import datetime
from unittest.mock import patch

import bot


def _summary(groups):
    return {"groups": groups, "actions": {}, "sources": {}, "recent": []}


def _memory(learned=None):
    memory = {category: [] for category in bot.gs.DEFAULT_MEMORY}
    memory["learned_preferences"] = list(learned or [])
    return memory


def _correction(text, assistant_response=""):
    return json.dumps({
        "date": "2026-07-10 17:15 SGT",
        "source": "test",
        "correction": text,
        "assistant_response": assistant_response,
        "priority": "high",
    })


def _reflection(learned, trigger="", next_behavior=""):
    return json.dumps({
        "date": "2026-07-10 17:15 SGT",
        "source": "test",
        "trigger": trigger,
        "learned": learned,
        "next_behavior": next_behavior,
    })


class SelfAuditTests(unittest.TestCase):
    def setUp(self):
        self.now = bot.SGT.localize(datetime(2026, 7, 10, 17, 15))

    def test_learned_preferences_category_and_runtime_status_are_visible(self):
        self.assertIn("learned_preferences", bot.gs.DEFAULT_MEMORY)
        self.assertIn("learned_preferences", bot.MEMORY_DISPLAY_CATEGORIES)

        memory = _memory(["[auto] Easing off prep_gap nudges — dismissed 4× in 14 days."])

        def get_config(key):
            if key == bot.LEARNED_MUTE_CONFIG_KEY:
                return json.dumps(["prep_gap"])
            return "2026-07-10"

        with patch.object(bot, "google_ok", return_value=True), \
             patch.object(bot.gs, "memory_storage_status", return_value={"connected": True, "source": "sheets"}), \
             patch.object(bot.gs, "get_memory", return_value=memory), \
             patch.object(bot.gs, "get_projects", return_value=[]), \
             patch.object(bot.gs, "get_app_notifications", return_value=[]), \
             patch.object(bot.gs, "get_web_push_subscriptions", return_value=[]), \
             patch.object(bot.gs, "get_notification_outcome_summary", return_value={"actions": {}}), \
             patch.object(bot.gs, "get_web_push_delivery_log", return_value=[]), \
             patch.object(bot.gs, "gmail_ok", return_value=False), \
             patch.object(bot.gs, "get_config", side_effect=get_config), \
             patch.object(bot, "_get_redis", return_value=None):
            status = bot.build_runtime_status()

        self.assertEqual(status["memory_buckets"]["learned_preferences"], 1)
        self.assertEqual(status["learned_preferences"]["auto_count"], 1)
        self.assertEqual(status["learned_muted_families"], ["prep_gap"])

    def test_strong_dismiss_pattern_mutes_and_writes_auto_preference_digest(self):
        store = _memory(["manual preference stays"])
        config = {}

        def set_memory(value):
            store.clear()
            store.update({key: list(value.get(key, [])) for key in bot.gs.DEFAULT_MEMORY})

        with patch.object(bot.gs, "get_notification_outcome_summary", return_value=_summary({
                "marking_crunch": {"count": 5, "negative": 5, "positive": 0},
             })), \
             patch.object(bot.gs, "get_memory", side_effect=lambda: {key: list(value) for key, value in store.items()}), \
             patch.object(bot.gs, "set_memory", side_effect=set_memory) as set_memory_mock, \
             patch.object(bot.gs, "set_config", side_effect=lambda key, value: config.__setitem__(key, value)) as set_config, \
             patch.object(bot, "_queue_app_notification", return_value={"id": "audit"}) as queue:
            result = bot.run_self_audit(self.now)

        self.assertEqual([item["group"] for item in result["muted"]], ["marking_crunch"])
        self.assertEqual(json.loads(config[bot.LEARNED_MUTE_CONFIG_KEY]), ["marking_crunch"])
        self.assertIn("manual preference stays", store["learned_preferences"])
        self.assertTrue(any(item.startswith("[auto] Easing off marking_crunch") for item in store["learned_preferences"]))
        self.assertTrue(set_memory_mock.called)
        set_config.assert_called_once()
        queue.assert_called_once()
        self.assertIn("Easing off", queue.call_args.args[2])

    def test_soft_pattern_is_watching_not_muted(self):
        config = {}
        with patch.object(bot.gs, "get_notification_outcome_summary", return_value=_summary({
                "prep_gap": {"count": 4, "negative": 2, "positive": 0},
             })), \
             patch.object(bot.gs, "get_memory", return_value=_memory()), \
             patch.object(bot.gs, "set_memory") as set_memory, \
             patch.object(bot.gs, "set_config", side_effect=lambda key, value: config.__setitem__(key, value)), \
             patch.object(bot, "_queue_app_notification", return_value={"id": "audit"}):
            result = bot.run_self_audit(self.now)

        self.assertEqual(result["muted"], [])
        self.assertEqual([item["group"] for item in result["watching"]], ["prep_gap"])
        self.assertEqual(json.loads(config[bot.LEARNED_MUTE_CONFIG_KEY]), [])
        set_memory.assert_not_called()

    def test_reversibility_lifts_prior_mute_and_removes_old_auto_memory(self):
        store = _memory(["manual preference stays", "[auto] Easing off marking_crunch nudges — old."])
        config = {bot.LEARNED_MUTE_CONFIG_KEY: json.dumps(["marking_crunch"])}

        def set_memory(value):
            store.clear()
            store.update({key: list(value.get(key, [])) for key in bot.gs.DEFAULT_MEMORY})

        with patch.object(bot.gs, "get_memory", side_effect=lambda: {key: list(value) for key, value in store.items()}), \
             patch.object(bot.gs, "set_memory", side_effect=set_memory), \
             patch.object(bot.gs, "set_config", side_effect=lambda key, value: config.__setitem__(key, value)), \
             patch.object(bot, "_queue_app_notification", return_value={"id": "audit"}):
            with patch.object(bot.gs, "get_notification_outcome_summary", return_value=_summary({
                    "marking_crunch": {"count": 5, "negative": 5, "positive": 0},
                 })):
                bot.run_self_audit(self.now)
            self.assertEqual(json.loads(config[bot.LEARNED_MUTE_CONFIG_KEY]), ["marking_crunch"])

            with patch.object(bot.gs, "get_notification_outcome_summary", return_value=_summary({
                    "marking_crunch": {"count": 6, "negative": 5, "positive": 1},
                 })):
                result = bot.run_self_audit(self.now)

        self.assertEqual(result["muted"], [])
        self.assertEqual(json.loads(config[bot.LEARNED_MUTE_CONFIG_KEY]), [])
        self.assertEqual(store["learned_preferences"], ["manual preference stays"])

    def test_critical_families_never_auto_mute(self):
        config = {}
        with patch.object(bot.gs, "get_notification_outcome_summary", return_value=_summary({
                "prayer": {"count": 8, "negative": 8, "positive": 0},
                "briefing": {"count": 6, "negative": 6, "positive": 0},
             })), \
             patch.object(bot.gs, "get_memory", return_value=_memory()), \
             patch.object(bot.gs, "set_memory") as set_memory, \
             patch.object(bot.gs, "set_config", side_effect=lambda key, value: config.__setitem__(key, value)), \
             patch.object(bot, "_queue_app_notification", return_value={"id": "audit"}):
            result = bot.run_self_audit(self.now)

        self.assertEqual(result["muted"], [])
        self.assertEqual(json.loads(config[bot.LEARNED_MUTE_CONFIG_KEY]), [])
        set_memory.assert_not_called()

    def test_learned_mute_suppresses_dispatch_additively(self):
        candidate = {
            "family": "marking_crunch",
            "source": "marking_crunch",
            "kind": "update",
            "title": "Marking crunch",
            "body": "Block marking time.",
        }

        with patch.object(bot.gs, "get_config", return_value=json.dumps(["marking_crunch"])), \
             patch.object(bot, "_queue_app_notification", side_effect=AssertionError("muted family dispatched")):
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))
        self.assertEqual(sent, 0)

        with patch.object(bot.gs, "get_config", return_value="[]"), \
             patch.object(bot, "_queue_app_notification", return_value={"id": "1", "_push_sent": 0}) as queue:
            sent = asyncio.run(bot._dispatch_proactive_candidates(None, [candidate], limit=1))
        self.assertEqual(sent, 1)
        queue.assert_called_once()

    def test_run_self_audit_scoped_writes(self):
        config_calls = []
        memory_store = _memory(["manual preference stays"])

        def set_memory(value):
            changed = [
                category
                for category in bot.gs.DEFAULT_MEMORY
                if list(value.get(category, [])) != memory_store.get(category, [])
            ]
            self.assertEqual(changed, ["learned_preferences"])
            memory_store.update({key: list(value.get(key, [])) for key in bot.gs.DEFAULT_MEMORY})

        with patch.object(bot.gs, "get_notification_outcome_summary", return_value=_summary({
                "calendar_conflict": {"count": 5, "negative": 5, "positive": 0},
             })), \
             patch.object(bot.gs, "get_memory", side_effect=lambda: {key: list(value) for key, value in memory_store.items()}), \
             patch.object(bot.gs, "set_memory", side_effect=set_memory), \
             patch.object(bot.gs, "set_config", side_effect=lambda key, value: config_calls.append((key, value))), \
             patch.object(bot, "_queue_app_notification", return_value={"id": "audit"}):
            bot.run_self_audit(self.now)

        self.assertEqual([key for key, _value in config_calls], [bot.LEARNED_MUTE_CONFIG_KEY])

    def test_retrospective_correction_strong_signal_uses_clusters(self):
        memory = _memory()
        memory["correction_ledger"] = [
            _correction("Do not guess the weekday; verify the date first."),
            _correction("Never assume the date or weekday from memory."),
            _correction("You must check dates before saying today or tomorrow."),
        ]

        signals = bot._retrospective_correction_signals(memory, now=self.now)

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["cluster"], "date_discipline")
        self.assertEqual(signals[0]["confidence"], "strong")

    def test_retrospective_stopword_collision_does_not_fire(self):
        memory = _memory()
        memory["correction_ledger"] = [
            _correction("Do not guess the weekday."),
            _correction("That was not the wrong class."),
        ]

        signals = bot._retrospective_correction_signals(memory, now=self.now)

        self.assertEqual(signals, [])

    def test_retrospective_correction_ignores_assistant_response(self):
        memory = _memory()
        memory["correction_ledger"] = [
            _correction("Small correction.", assistant_response="I must use live latest web sources and citations."),
            _correction("Another small correction.", assistant_response="I should search the latest web sources and cite them."),
        ]

        signals = bot._retrospective_correction_signals(memory, now=self.now)

        self.assertEqual(signals, [])

    def test_retrospective_reflection_matches_learned_only(self):
        memory = _memory()
        memory["self_reflections"] = [
            _reflection(
                "I failed source discipline by not checking latest web sources.",
                trigger="wrong class student name",
                next_behavior="confirm class student entity",
            ),
            _reflection(
                "I should use web search and cite sources for latest facts.",
                trigger="wrong person entity",
                next_behavior="confirm class room",
            ),
        ]

        signals = bot._retrospective_reflection_signals(memory, now=self.now)

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["cluster"], "source_discipline")
        self.assertEqual(signals[0]["confidence"], "watch")

    def test_retrospective_corrections_use_last_30_entries(self):
        memory = _memory()
        memory["correction_ledger"] = [
            _correction("Use live latest web sources."),
            _correction("Search latest web sources before answering."),
        ] + [_correction(f"Filler correction {index}") for index in range(29)]

        signals = bot._retrospective_correction_signals(memory, now=self.now)

        self.assertEqual(signals, [])

    def test_retrospective_action_ledger_undone_is_watch_only(self):
        ledger = [
            {
                "id": "1",
                "created": self.now.isoformat(),
                "action": "create_calendar_event",
                "subject": "Meeting A",
                "result": "Created event",
                "source": "assistant_tool",
                "undo_status": "undone",
            },
            {
                "id": "2",
                "created": self.now.isoformat(),
                "action": "create_calendar_event",
                "subject": "Meeting B",
                "result": "Created event",
                "source": "assistant_tool",
                "undo_status": "undone",
            },
        ]

        signals = bot._retrospective_action_signals(ledger, now=self.now, days=14)

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["key"], "action:create_calendar_event")
        self.assertEqual(signals[0]["confidence"], "watch")

    def test_retrospective_action_ledger_exempts_critical_sources(self):
        ledger = [
            {
                "id": "1",
                "created": self.now.isoformat(),
                "action": "create_proactive_nudge",
                "source": "nudge:1",
                "undo_status": "undone",
            },
            {
                "id": "2",
                "created": self.now.isoformat(),
                "action": "create_proactive_nudge",
                "source": "nudge:2",
                "undo_status": "undone",
            },
        ]

        signals = bot._retrospective_action_signals(ledger, now=self.now, days=14)

        self.assertEqual(signals, [])

    def test_build_retrospective_evidence_degrades_action_ledger_failure(self):
        memory = _memory()
        memory["correction_ledger"] = [
            _correction("Use live latest web sources."),
            _correction("Search latest web sources before answering."),
        ]

        with patch.object(bot.gs, "get_memory", return_value=memory), \
             patch.object(bot.gs, "get_action_ledger", side_effect=RuntimeError("ledger down")), \
             patch.object(bot.gs, "set_memory", side_effect=AssertionError("no writes")), \
             patch.object(bot.gs, "set_config", side_effect=AssertionError("no config writes")), \
             patch.object(bot, "_queue_app_notification", side_effect=AssertionError("no notifications")):
            result = bot.build_retrospective_evidence(now=self.now, days=14)

        self.assertTrue(any("ledger down" in error for error in result["errors"]))
        self.assertEqual(result["watch"][0]["cluster"], "source_discipline")


if __name__ == "__main__":
    unittest.main()
