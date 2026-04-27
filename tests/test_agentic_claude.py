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
