import json
import os
import unittest
from datetime import datetime
from unittest.mock import patch

os.environ.setdefault("OPENAI_API_KEY", "test-key")

import bot
import google_services as gs
import postgres_storage


class _NoopContext:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _FakePgCursor:
    def __init__(self, rows):
        self.rows = rows
        self.memory_log = []
        self.saved_bucket = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        params = params or ()
        if "INSERT INTO memory_log" in sql:
            source = "cap_overflow" if "cap_overflow" in sql else "postgres"
            if len(params) >= 3:
                source = params[2]
            self.memory_log.append({"category": params[0], "text": params[1], "source": source})
        elif "INSERT INTO assistant_memory" in sql:
            self.saved_bucket = list(params[1])

    def fetchall(self):
        return self.rows


class _FakePgConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def transaction(self):
        return _NoopContext()

    def cursor(self):
        return self._cursor


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = bot.SGT.localize(datetime(2026, 6, 10, 9, 0))
        if tz is not None:
            return current.astimezone(tz)
        return current.replace(tzinfo=None)


def _entry(day: str, text: str, source: str = "test"):
    return json.dumps({
        "date": f"{day} 08:00 SGT",
        "source": source,
        "correction": text,
        "assistant_response": "",
        "priority": "high",
    }, ensure_ascii=False, sort_keys=True)


class TrancheBTests(unittest.TestCase):
    def test_storage_caps_cover_all_display_categories(self):
        self.assertEqual(set(bot.MEMORY_DISPLAY_CATEGORIES), set(gs.MEMORY_STORAGE_CAPS))
        for category in bot.MEMORY_DISPLAY_CATEGORIES:
            prompt_limit = bot.MEMORY_PROMPT_LIMITS.get(category) or 0
            self.assertGreaterEqual(gs.MEMORY_STORAGE_CAPS[category], prompt_limit)

    def test_pg_add_memory_archives_overflow_fifo(self):
        cursor = _FakePgCursor([("profile", ["old", "middle"])])
        postgres_storage._initialized_markers_written.discard("assistant_memory_initialized")

        with patch.object(postgres_storage, "ensure_schema"), \
             patch.object(postgres_storage, "connect", return_value=_FakePgConnection(cursor)), \
             patch.object(postgres_storage, "_jsonb", side_effect=lambda value: value):
            memory = postgres_storage.add_memory(
                "profile",
                "new",
                {"profile": []},
                storage_caps={"profile": 2},
                caps_disabled=False,
            )

        self.assertEqual(memory["profile"], ["middle", "new"])
        self.assertEqual(cursor.saved_bucket, ["middle", "new"])
        self.assertIn({"category": "profile", "text": "old", "source": "cap_overflow"}, cursor.memory_log)
        postgres_storage._initialized_markers_written.discard("assistant_memory_initialized")

    def test_sheets_add_memory_archives_overflow(self):
        store = {"profile": ["old", "middle"]}
        archived = []
        saved = {}

        with patch.object(gs, "_postgres_available", return_value=False), \
             patch.object(gs, "get_memory", return_value={key: list(value) for key, value in {**gs.DEFAULT_MEMORY, **store}.items()}), \
             patch.object(gs, "set_memory", side_effect=lambda memory: saved.update({"memory": memory})), \
             patch.object(gs, "_append_memory_log", side_effect=lambda category, text, source="fallback": archived.append((category, text, source))), \
             patch.object(gs, "MEMORY_STORAGE_CAPS", {**gs.MEMORY_STORAGE_CAPS, "profile": 2}):
            memory = gs.add_memory("profile", "new")

        self.assertEqual(memory["profile"], ["middle", "new"])
        self.assertEqual(saved["memory"]["profile"], ["middle", "new"])
        self.assertEqual(archived, [("profile", "old", "cap_overflow")])

    def test_append_memory_json_archives_dropped(self):
        first = json.dumps({"date": "2026-05-01", "correction": "first"}, ensure_ascii=False, sort_keys=True)
        second = json.dumps({"date": "2026-05-02", "correction": "second"}, ensure_ascii=False, sort_keys=True)
        archived = []
        saved = {}

        with patch.object(gs, "_postgres_available", return_value=False), \
             patch.object(gs, "get_memory", return_value={**gs.DEFAULT_MEMORY, "correction_ledger": [first, second]}), \
             patch.object(gs, "set_memory", side_effect=lambda memory: saved.update({"memory": memory})), \
             patch.object(gs, "_append_memory_log", side_effect=lambda category, text, source="fallback": archived.append((category, text, source))):
            gs._append_memory_json("correction_ledger", {"date": "2026-05-03", "correction": "third"}, limit=2)

        self.assertEqual(saved["memory"]["correction_ledger"][0], second)
        self.assertEqual(archived, [("correction_ledger", first, "cap_overflow")])

    def test_caps_kill_switch(self):
        store = {"profile": ["old", "middle"]}
        archived = []
        saved = {}

        with patch.dict(os.environ, {"HIRA_DISABLE_MEMORY_CAPS": "1"}, clear=False), \
             patch.object(gs, "_postgres_available", return_value=False), \
             patch.object(gs, "get_memory", return_value={key: list(value) for key, value in {**gs.DEFAULT_MEMORY, **store}.items()}), \
             patch.object(gs, "set_memory", side_effect=lambda memory: saved.update({"memory": memory})), \
             patch.object(gs, "_append_memory_log", side_effect=lambda category, text, source="fallback": archived.append((category, text, source))), \
             patch.object(gs, "MEMORY_STORAGE_CAPS", {**gs.MEMORY_STORAGE_CAPS, "profile": 2}):
            memory = gs.add_memory("profile", "new")

        self.assertEqual(memory["profile"], ["old", "middle", "new"])
        self.assertEqual(saved["memory"]["profile"], ["old", "middle", "new"])
        self.assertEqual(archived, [])

    def test_consolidation_prune_applies_matched_old_entries(self):
        valid_one = _entry("2026-05-01", "always use live prayer tools")
        valid_two = _entry("2026-05-02", "never guess weekdays")
        recent = _entry("2026-06-05", "recent correction")
        memory = {key: [] for key in gs.DEFAULT_MEMORY}
        memory["correction_ledger"] = [valid_one, valid_two, recent]
        removed = []

        def remove(category, entries, source="consolidation_prune"):
            count = 0
            for entry in entries:
                if entry in memory[category]:
                    memory[category].remove(entry)
                    removed.append((category, entry, source))
                    count += 1
            return count

        llm_response = json.dumps({
            "promote": [],
            "growth_note": "Use durable correction lessons.",
            "prune": [
                {"kind": "correction", "date": "2026-05-01", "snippet": "always use live prayer tools"},
                {"kind": "correction", "date": "2026-05-02", "snippet": "never guess weekdays"},
                {"kind": "correction", "date": "2026-05-03", "snippet": "not in memory"},
                {"kind": "correction", "date": "2026-06-05", "snippet": "recent correction"},
            ],
        })

        with patch.object(bot, "datetime", _FrozenDateTime), \
             patch.object(bot.gs, "get_memory", side_effect=lambda: {key: list(value) for key, value in memory.items()}), \
             patch.object(bot, "_llm_text", return_value=llm_response), \
             patch.object(bot.gs, "remove_memory_entries", side_effect=remove), \
             patch.object(bot.gs, "add_self_reflection") as add_reflection:
            result = bot._memory_consolidation_payload()

        self.assertEqual(result["pruned"], 2)
        self.assertEqual(result["skipped"], 2)
        self.assertEqual([item[0] for item in removed], ["correction_ledger", "correction_ledger"])
        self.assertEqual(memory["correction_ledger"], [recent])
        self.assertIn("pruned=2", add_reflection.call_args.args[0]["learned"])

    def test_consolidation_prune_budget(self):
        memory = {key: [] for key in gs.DEFAULT_MEMORY}
        entries = [_entry("2026-05-01", f"old correction {idx}") for idx in range(15)]
        memory["correction_ledger"] = list(entries)

        def remove(category, entries_to_remove, source="consolidation_prune"):
            count = 0
            for entry in entries_to_remove:
                if entry in memory[category]:
                    memory[category].remove(entry)
                    count += 1
            return count

        llm_response = json.dumps({
            "promote": [],
            "growth_note": "",
            "prune": [
                {"kind": "correction", "date": "2026-05-01", "snippet": f"old correction {idx}"}
                for idx in range(15)
            ],
        })

        with patch.object(bot, "datetime", _FrozenDateTime), \
             patch.object(bot.gs, "get_memory", side_effect=lambda: {key: list(value) for key, value in memory.items()}), \
             patch.object(bot, "_llm_text", return_value=llm_response), \
             patch.object(bot.gs, "remove_memory_entries", side_effect=remove):
            result = bot._memory_consolidation_payload()

        self.assertEqual(result["pruned"], 10)
        self.assertEqual(result["skipped"], 5)
        self.assertEqual(len(memory["correction_ledger"]), 5)

    def test_consolidated_dedup_keeps_newest(self):
        old = "[consolidated 2026-05-01] Always verify live facts before current claims."
        middle = "[consolidated 2026-05-10] Always verify live facts before making current claims."
        newest = "[consolidated 2026-06-01] Always verify live facts before current claims."
        memory = {key: [] for key in gs.DEFAULT_MEMORY}
        memory["preferences"] = [old, middle, newest]
        removed = []

        def remove(category, entries, source="consolidation_prune"):
            count = 0
            for entry in entries:
                if entry in memory[category]:
                    memory[category].remove(entry)
                    removed.append((category, entry, source))
                    count += 1
            return count

        with patch.object(bot, "datetime", _FrozenDateTime), \
             patch.object(bot.gs, "get_memory", return_value=memory), \
             patch.object(bot.gs, "remove_memory_entries", side_effect=remove), \
             patch.object(bot, "_llm_text", side_effect=AssertionError("no LLM needed for dedup-only pass")):
            result = bot._memory_consolidation_payload()

        self.assertEqual(result["pruned"], 2)
        self.assertEqual(memory["preferences"], [newest])
        self.assertEqual([item[2] for item in removed], ["consolidation_prune", "consolidation_prune"])


if __name__ == "__main__":
    unittest.main()
