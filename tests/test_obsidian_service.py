import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import obsidian_service
import bot


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


class ObsidianServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.vault = Path(self.tmp.name)
        _write(self.vault / "Projects" / "Hira.md", "# Hira\nVault tools roadmap and project notes.")
        _write(self.vault / "31 ClassOps" / "Submission Tracker.md", "student submission secret")
        _write(self.vault / "Private" / "Journal.md", "private secret")
        _write(self.vault / "Student-Sensitive" / "Case Note.md", "student sensitive details")
        _write(self.vault / ".obsidian" / "workspace.md", "system metadata")
        self.env = {
            "HIRA_OBSIDIAN_VAULT_PATH": str(self.vault),
            "HIRA_OBSIDIAN_INBOX_NOTE": "Inbox.md",
            "HIRA_OBSIDIAN_EXTRA_EXCLUDE_TERMS": "",
        }

    def tearDown(self):
        self.tmp.cleanup()

    def test_search_returns_readable_notes_only(self):
        with patch.dict(os.environ, self.env, clear=False):
            result = obsidian_service.search_vault("hira vault", max_results=5)
            self.assertTrue(result["ok"])
            self.assertEqual([item["path"] for item in result["results"]], ["Projects/Hira.md"])

            blocked = obsidian_service.search_vault("secret student private", max_results=5)
            self.assertTrue(blocked["ok"])
            self.assertEqual(blocked["results"], [])

    def test_read_note_blocks_excluded_paths(self):
        with patch.dict(os.environ, self.env, clear=False):
            note = obsidian_service.read_note("Projects/Hira.md")
            self.assertTrue(note["ok"])
            self.assertIn("Vault tools roadmap", note["content"])

            with self.assertRaises(PermissionError):
                obsidian_service.read_note("31 ClassOps/Submission Tracker.md")
            with self.assertRaises(PermissionError):
                obsidian_service.read_note("Private/Journal.md")
            with self.assertRaises(PermissionError):
                obsidian_service.read_note("Student-Sensitive/Case Note.md")

    def test_recent_notes_omits_excluded_paths(self):
        with patch.dict(os.environ, self.env, clear=False):
            result = obsidian_service.list_recent_notes(limit=10)

        paths = {item["path"] for item in result["notes"]}
        self.assertEqual(paths, {"Projects/Hira.md"})

    def test_append_to_inbox_writes_inside_vault_only(self):
        with patch.dict(os.environ, self.env, clear=False):
            result = obsidian_service.append_to_inbox("Capture this", heading="Unit test")
            self.assertTrue(result["ok"])
            self.assertEqual(result["path"], "Inbox.md")
            self.assertIn("Capture this", (self.vault / "Inbox.md").read_text(encoding="utf-8"))

            blocked = obsidian_service.append_to_inbox(
                "Do not write",
                heading="Private",
                inbox_path="Private/Inbox.md",
            )
            self.assertFalse(blocked["ok"])
            self.assertIn("excluded", blocked["error"])

            with self.assertRaises(ValueError):
                obsidian_service.append_to_inbox("Do not escape", inbox_path="../outside.md")

    def test_bot_exposes_and_routes_vault_tools(self):
        tools = bot.pwa_tools_for_message("search my Obsidian vault for Hira")
        names = {tool["name"] for tool in tools}

        self.assertIn("search_vault", names)
        self.assertIn("read_note", names)
        self.assertIn("list_recent_notes", names)
        self.assertEqual(bot._forced_tool_for_text("search my Obsidian vault for Hira", tools), "search_vault")
        inbox_tools = bot.pwa_tools_for_message("append this to inbox: Capture this")
        self.assertIn("append_to_inbox", {tool["name"] for tool in inbox_tools})
        self.assertEqual(bot._forced_tool_for_text("append this to inbox: Capture this", inbox_tools), "append_to_inbox")


if __name__ == "__main__":
    unittest.main()
