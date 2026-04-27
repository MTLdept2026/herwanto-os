import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")

import bot


class FakeMessages:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(
                stop_reason="tool_use",
                content=[
                    SimpleNamespace(
                        type="tool_use",
                        id="tool-1",
                        name="get_gmail_brief",
                        input={"query": "", "max_items": 5},
                    )
                ],
            )
        return SimpleNamespace(
            stop_reason="end_turn",
            content=[
                SimpleNamespace(
                    type="text",
                    text="Your last five emails are mostly about school admin and project updates.",
                )
            ],
        )


class AgenticClaudeTests(unittest.TestCase):
    def test_tuesday_even_timetable_uses_hardcoded_source(self):
        result = bot._timetable_for_lookup("Tuesday", "Even")

        self.assertIn("Tue Even week timetable", result)
        self.assertIn("7:35–8:00", result)
        self.assertIn("FTCT", result)
        self.assertIn("8:00–8:35", result)
        self.assertIn("CCE", result)
        self.assertIn("9:40–10:50", result)
        self.assertIn("1 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", result)
        self.assertIn("13:40–14:45", result)
        self.assertIn("3 Anchor/Beacon/Compass/Danforth/Expedition/Flagship/Garrison", result)
        self.assertIn("L3-10", result)

    def test_master_timetable_key_periods(self):
        monday_even = bot._timetable_for_lookup("Monday", "Even")
        tuesday_odd = bot._timetable_for_lookup("Tuesday", "Odd")
        wednesday_odd = bot._timetable_for_lookup("Wednesday", "Odd")
        thursday_odd = bot._timetable_for_lookup("Thursday", "Odd")
        friday_odd = bot._timetable_for_lookup("Friday", "Odd")

        self.assertIn("8:00–9:05", monday_even)
        self.assertIn("10:50–11:55", monday_even)
        self.assertIn("8:00–9:05", tuesday_odd)
        self.assertIn("8:00–8:35", wednesday_odd)
        self.assertIn("9:05–10:50", wednesday_odd)
        self.assertIn("11:25–12:30", wednesday_odd)
        self.assertIn("9:40–10:50", thursday_odd)
        self.assertIn("10:50–11:55", friday_odd)

    def test_timetable_question_forces_timetable_tool(self):
        forced = bot._forced_tool_for_text(
            "What's the correct Tuesday Even week timetable?",
            [{"name": "get_timetable"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "get_timetable")

    def test_forced_tool_does_not_repeat_after_tool_result(self):
        fake_messages = FakeMessages()
        fake_claude = SimpleNamespace(messages=fake_messages)

        async def fake_execute_tool(name, inp):
            return "- Subject | From: sender@example.com | Snippet"

        messages = [{"role": "user", "content": "tell me about my last 5 emails"}]
        tools = [{"name": "get_gmail_brief"}]

        with (
            patch.object(bot, "claude", fake_claude),
            patch.object(bot, "SYSTEM_PROMPT", return_value="system"),
            patch.object(bot, "_execute_tool", side_effect=fake_execute_tool),
        ):
            reply = asyncio.run(bot._run_agentic_claude(messages, tools=tools))

        self.assertIn("last five emails", reply)
        self.assertEqual(fake_messages.calls[0]["tool_choice"], {"type": "tool", "name": "get_gmail_brief"})
        self.assertNotIn("tool_choice", fake_messages.calls[1])

    def test_forced_tool_ignores_structured_tool_result_turns(self):
        messages = [
            {"role": "user", "content": "tell me about my last 5 emails"},
            {"role": "assistant", "content": [SimpleNamespace(type="tool_use")]},
            {"role": "user", "content": [{"type": "tool_result", "content": "email result"}]},
        ]

        self.assertIsNone(bot._forced_tool_for_current_turn(messages, [{"name": "get_gmail_brief"}]))


if __name__ == "__main__":
    unittest.main()
