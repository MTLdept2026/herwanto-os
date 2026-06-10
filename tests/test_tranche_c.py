import asyncio
import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

os.environ.setdefault("OPENAI_API_KEY", "test-key")

import bot
import google_services as gs


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = bot.SGT.localize(datetime(2026, 6, 10, 9, 0))
        if tz is not None:
            return current.astimezone(tz)
        return current.replace(tzinfo=None)


class _MemoryHarness:
    def __init__(self):
        self.memory = {key: [] for key in gs.DEFAULT_MEMORY}
        self.config = {}
        self.archived = []

    def get_memory(self):
        return {key: list(value) for key, value in self.memory.items()}

    def set_memory(self, value):
        self.memory = {key: list(value.get(key, [])) for key in gs.DEFAULT_MEMORY}

    def get_config(self, key):
        return self.config.get(key, "")

    def set_config(self, key, value):
        self.config[key] = value

    def archive(self, category, text, source="fallback"):
        self.archived.append((category, text, source))

    def patches(self):
        return patch.multiple(
            gs,
            _postgres_available=lambda: False,
            get_memory=self.get_memory,
            set_memory=self.set_memory,
            get_config=self.get_config,
            set_config=self.set_config,
            _append_memory_log=self.archive,
        )


class TrancheCTests(unittest.TestCase):
    def test_playbook_upsert_replaces_case_insensitive_and_skips_malformed(self):
        harness = _MemoryHarness()
        harness.memory["playbooks"] = ["not-json"]

        with harness.patches(), self.assertLogs(gs.logger, level="WARNING") as logs:
            self.assertEqual(gs.get_playbooks(), [])
            first = gs.upsert_playbook("LFC News", "old desc", "old body")
            second = gs.upsert_playbook("lfc-news", "new desc", "new body")
            playbooks = gs.get_playbooks()

        self.assertEqual(first["name"], "lfc-news")
        self.assertEqual(second["name"], "lfc-news")
        self.assertEqual(len(playbooks), 1)
        self.assertEqual(playbooks[0]["description"], "new desc")
        self.assertEqual(playbooks[0]["body"], "new body")
        self.assertTrue(any("Skipping malformed playbook" in line for line in logs.output))
        self.assertEqual(harness.archived[-1][2], "playbook_replace")

    def test_select_playbooks_fail_open(self):
        async def run():
            with patch.object(bot.gs, "get_playbooks", return_value=[
                {"name": "lfc-news", "description": "Liverpool news", "body": "rules", "updated": "2026-06-10"},
            ]), patch.object(bot, "_llm_text_async", side_effect=RuntimeError("router down")):
                return await bot._select_playbooks("latest lfc transfer news")

        self.assertEqual(asyncio.run(run()), [])

    def test_select_playbooks_caps_at_two(self):
        playbooks = [
            {"name": "alpha", "description": "A", "body": "A body", "updated": "2026-06-10"},
            {"name": "beta", "description": "B", "body": "B body", "updated": "2026-06-10"},
            {"name": "gamma", "description": "C", "body": "C body", "updated": "2026-06-10"},
        ]

        async def run():
            with patch.object(bot.gs, "get_playbooks", return_value=playbooks), \
                 patch.object(bot, "_llm_text_async", return_value="alpha, beta, gamma"):
                return await bot._select_playbooks("use alpha beta gamma")

        self.assertEqual([item["name"] for item in asyncio.run(run())], ["alpha", "beta"])

    def test_playbook_injection_is_after_specialist_and_budgeted(self):
        policy = {
            "specialist": "sports_live",
            "playbooks": [
                {"name": "one", "description": "", "body": "a" * 3000},
                {"name": "two", "description": "", "body": "b" * 2000},
            ],
        }

        with patch.object(bot, "SYSTEM_PROMPT", return_value="STATIC"):
            instructions = bot._openai_instructions_for_policy(policy)

        self.assertTrue(instructions.startswith("STATIC"))
        self.assertLess(instructions.index("Specialist mode: live sports analyst"), instructions.index("Active playbook"))
        self.assertEqual(instructions.count("Active playbook"), 2)
        self.assertIn("[playbook truncated]", instructions)
        body_payloads = []
        for block in instructions.split("Active playbook")[1:]:
            body = block.split("):\n", 1)[1]
            body = body.split("\n\nActive playbook", 1)[0]
            body_payloads.append(body.replace("\n[playbook truncated]", ""))
        self.assertLessEqual(sum(len(body) for body in body_payloads), bot.PLAYBOOK_BODY_BUDGET)

    def test_propose_apply_expired_and_discard_flow(self):
        harness = _MemoryHarness()
        with harness.patches(), patch.object(bot, "datetime", _FrozenDateTime):
            proposal = gs.set_pending_playbook_proposal("LFC News", "desc", "body v1", "tighten sources")
            self.assertEqual(proposal["name"], "lfc-news")
            applied = bot.apply_pending_playbook_update()
            self.assertIn("Applied playbook: lfc-news", applied)
            self.assertEqual(gs.get_pending_playbook_proposal(), None)
            self.assertEqual(gs.get_playbooks()[0]["body"], "body v1")

            harness.config[gs.PLAYBOOK_PENDING_PROPOSAL_KEY] = json.dumps({
                "name": "lfc-news",
                "description": "desc",
                "body": "body v2",
                "summary": "expired update",
                "proposed_at": "2026-06-08T08:59:00+08:00",
            })
            expired = bot.apply_pending_playbook_update()
            self.assertIn("expired", expired.lower())
            self.assertEqual(gs.get_pending_playbook_proposal(), None)

            gs.set_pending_playbook_proposal("lfc-news", "desc", "body v3", "discard me")
            discarded = bot.discard_pending_playbook_update()
            self.assertIn("Discarded pending playbook proposal: lfc-news", discarded)
            self.assertEqual(gs.get_pending_playbook_proposal(), None)

    def test_seed_playbook_once(self):
        harness = _MemoryHarness()
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write("# Seed\n\nUse reliable sources.")
            seed_path = handle.name
        try:
            with harness.patches(), patch.object(bot, "PLAYBOOK_SEED_FILE", seed_path):
                first = bot.seed_playbooks_if_needed()
                second = bot.seed_playbooks_if_needed()
                playbooks = gs.get_playbooks()

                self.assertTrue(first["seeded"])
                self.assertEqual(second["reason"], "already_seeded")
                self.assertEqual(len(playbooks), 1)
                self.assertEqual(playbooks[0]["name"], "lfc-news")
        finally:
            os.unlink(seed_path)

    def test_quick_route_skips_playbook_selection(self):
        async def run():
            with patch.object(bot.gs, "get_playbooks", side_effect=AssertionError("should not load playbooks")):
                return await bot._select_playbooks("thanks")

        self.assertEqual(asyncio.run(run()), [])


if __name__ == "__main__":
    unittest.main()
