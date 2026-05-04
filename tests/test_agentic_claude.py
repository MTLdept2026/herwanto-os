import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")

import bot
import islamic_service
import pdf_service
import search_service
import weather_service
import web_app


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


def sheet_row(*values):
    return {"values": [{"formattedValue": str(value)} for value in values]}


class FakeSheetsRequest:
    def __init__(self, payload=None, callback=None):
        self.payload = payload or {}
        self.callback = callback

    def execute(self):
        if self.callback:
            self.callback()
        return self.payload


class FakeSheetsValues:
    def __init__(self):
        self.batch_updates = []
        self.updates = []

    def batchUpdate(self, spreadsheetId, body):
        self.batch_updates.append((spreadsheetId, body))
        return FakeSheetsRequest()

    def update(self, spreadsheetId, range, valueInputOption, body):
        self.updates.append((spreadsheetId, range, valueInputOption, body))
        return FakeSheetsRequest()


class FakeSheetsSpreadsheets:
    def __init__(self, book):
        self.book = book
        self.values_api = FakeSheetsValues()

    def get(self, spreadsheetId, includeGridData, fields):
        return FakeSheetsRequest(self.book)

    def values(self):
        return self.values_api


class FakeSheetsService:
    def __init__(self, book):
        self.spreadsheets_api = FakeSheetsSpreadsheets(book)

    def spreadsheets(self):
        return self.spreadsheets_api


class AgenticClaudeTests(unittest.TestCase):
    def test_tuesday_even_timetable_uses_hardcoded_source(self):
        result = bot._timetable_for_lookup("Tuesday", "Even")

        self.assertIn("Tue Even week timetable", result)
        self.assertIn("7:35–8:00", result)
        self.assertIn("FTCT", result)
        self.assertIn("8:00–9:05", result)
        self.assertIn("CCE", result)
        self.assertIn("9:40–10:50", result)
        self.assertIn("1 Flagship", result)
        self.assertIn("13:40–14:45", result)
        self.assertIn("3G3", result)
        self.assertIn("L3-10", result)

    def test_master_timetable_key_periods(self):
        monday_even = bot._timetable_for_lookup("Monday", "Even")
        tuesday_odd = bot._timetable_for_lookup("Tuesday", "Odd")
        tuesday_even = bot._timetable_for_lookup("Tuesday", "Even")
        wednesday_odd = bot._timetable_for_lookup("Wednesday", "Odd")
        wednesday_even = bot._timetable_for_lookup("Wednesday", "Even")
        thursday_odd = bot._timetable_for_lookup("Thursday", "Odd")
        thursday_even = bot._timetable_for_lookup("Thursday", "Even")
        friday_odd = bot._timetable_for_lookup("Friday", "Odd")

        self.assertIn("8:00–9:05", monday_even)
        self.assertIn("10:50–11:55", monday_even)
        self.assertIn("8:00–9:05", tuesday_odd)
        self.assertIn("7:35–8:00", tuesday_even)
        self.assertIn("8:00–9:05", tuesday_even)
        self.assertIn("8:00–8:35", wednesday_odd)
        self.assertIn("9:05–10:50", wednesday_odd)
        self.assertIn("11:25–12:30", wednesday_odd)
        self.assertIn("8:00–8:35", wednesday_even)
        self.assertIn("9:05–10:15", wednesday_even)
        self.assertIn("11:25–12:30", wednesday_even)
        self.assertIn("12:30–13:40", wednesday_even)
        self.assertIn("9:40–10:50", thursday_odd)
        self.assertIn("11:25–12:30", thursday_even)
        self.assertIn("11:25–12:30", friday_odd)

    def test_timetable_question_forces_timetable_tool(self):
        forced = bot._forced_tool_for_text(
            "What's the correct Tuesday Even week timetable?",
            [{"name": "get_timetable"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "get_timetable")

    def test_action_requests_are_not_forced_to_read_only_tools(self):
        calendar_forced = bot._forced_tool_for_text(
            "schedule meeting with HOD tomorrow at 3pm",
            [{"name": "get_assistant_context"}, {"name": "create_calendar_event"}],
        )
        task_forced = bot._forced_tool_for_text(
            "add task to submit CCA attendance by Friday",
            [{"name": "get_task_brief"}, {"name": "add_reminder"}],
        )

        self.assertIsNone(calendar_forced)
        self.assertIsNone(task_forced)

    def test_availability_planning_forces_checked_slot_tool(self):
        forced = bot._forced_tool_for_text(
            "find the best slots to schedule Sahibba training after school not during my CCA day",
            [{"name": "find_available_training_slots"}, {"name": "get_assistant_context"}, {"name": "create_calendar_event"}],
        )

        self.assertEqual(forced, "find_available_training_slots")

    def test_hdb_date_question_forces_calendar_context(self):
        forced = bot._forced_tool_for_text(
            "what date is my HDB appointment?",
            [{"name": "get_assistant_context"}, {"name": "get_timetable"}],
        )

        self.assertEqual(forced, "get_assistant_context")

    def test_score_question_forces_classlist_tool(self):
        forced = bot._forced_tool_for_text(
            "show me the FA2 scores for S4-AN",
            [{"name": "get_mtl_classlists"}, {"name": "get_timetable"}],
        )

        self.assertEqual(forced, "get_mtl_classlists")

    def test_percentage_fill_request_forces_percentage_tool(self):
        forced = bot._forced_tool_for_text(
            "input the converted scores under the percentage sign for FA2",
            [{"name": "fill_mtl_percentage_scores"}, {"name": "get_mtl_classlists"}],
        )

        self.assertEqual(forced, "fill_mtl_percentage_scores")

    def test_score_analysis_request_forces_analysis_tool(self):
        forced = bot._forced_tool_for_text(
            "analyse S4-AN scores and show mean median underperforming most improved drastic drops",
            [{"name": "analyze_mtl_scores"}, {"name": "get_mtl_classlists"}],
        )

        self.assertEqual(forced, "analyze_mtl_scores")

    def test_url_question_forces_fetch_url_tool(self):
        forced = bot._forced_tool_for_text(
            "read this for me https://www.formula1.com/en/teams",
            [{"name": "fetch_url"}, {"name": "web_search"}],
        )

        self.assertEqual(forced, "fetch_url")

    def test_f1_current_question_forces_structured_sports_tool(self):
        forced = bot._forced_tool_for_text(
            "current F1 driver standings after the latest grand prix",
            [{"name": "get_f1_brief"}, {"name": "web_search"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_f1_brief")

    def test_liverpool_current_question_forces_structured_sports_tool(self):
        forced = bot._forced_tool_for_text(
            "where are Liverpool in the current EPL table and what competitions are they still in?",
            [{"name": "get_liverpool_brief"}, {"name": "web_search"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_liverpool_brief")

    def test_news_item_key_prefers_url_for_stable_deduping(self):
        item_a = {"title": "Same story title", "url": "https://example.com/story"}
        item_b = {"title": "Same story title updated", "url": "https://example.com/story"}

        self.assertEqual(search_service.news_item_key(item_a), search_service.news_item_key(item_b))

    def test_pick_fresh_morning_digest_entries_skips_recent_keys(self):
        def fake_google_news(query, max_items=5):
            return [
                {"title": f"{query} old", "url": f"https://example.com/{query}/old"},
                {"title": f"{query} fresh", "url": f"https://example.com/{query}/fresh"},
            ]

        topics = [("Topic A", "alpha"), ("Topic B", "beta")]
        seen_keys = {search_service.news_item_key({"title": "alpha old", "url": "https://example.com/alpha/old"})}
        with patch("search_service.google_news", side_effect=fake_google_news):
            entries = search_service.pick_fresh_morning_digest_entries(topics=topics, seen_keys=seen_keys, fetch_limit=2)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["label"], "Topic A")
        self.assertEqual(entries[0]["item"]["title"], "alpha fresh")
        self.assertEqual(entries[1]["item"]["title"], "beta old")

    def test_notification_feedback_bias_prefers_useful_over_negative(self):
        now = bot.datetime.now(bot.SGT)
        outcomes = [
            {"created": now.isoformat(), "source": "checkin:7", "group": "checkin", "kind": "reminder", "action": "useful"},
            {"created": now.isoformat(), "source": "checkin:7", "group": "checkin", "kind": "reminder", "action": "dismissed"},
            {"created": now.isoformat(), "source": "checkin:8", "group": "checkin", "kind": "reminder", "action": "dismissed"},
        ]
        with patch("bot.gs.get_notification_outcomes", return_value=outcomes):
            score_exact = bot._notification_feedback_bias("checkin:7", "reminder", now=now)
            score_other = bot._notification_feedback_bias("checkin:8", "reminder", now=now)

        self.assertGreater(score_exact, score_other)

    def test_notification_suppression_honours_recent_negative_feedback(self):
        now = bot.datetime.now(bot.SGT)
        outcomes = [
            {"created": now.isoformat(), "source": "followup:3", "group": "followup", "kind": "reminder", "action": "not_now"}
        ]
        with patch("bot.gs.get_notification_outcomes", return_value=outcomes):
            self.assertTrue(bot._should_suppress_notification("followup:3", "reminder", now=now))
            self.assertTrue(bot._should_suppress_notification("followup:9", "reminder", now=now))
            self.assertFalse(bot._should_suppress_notification("briefing:today", "briefing", now=now))

    def test_proactive_v2_queue_prefers_higher_score_ready_items(self):
        now = bot.datetime.now(bot.SGT)
        with patch("bot.google_ok", return_value=False), \
             patch("bot.build_proactive_intelligence_insights", return_value=[
                 {"id": "alpha", "title": "Alpha", "body": "First", "priority": "medium"},
                 {"id": "beta", "title": "Beta", "body": "Second", "priority": "high"},
             ]), \
             patch("bot.build_task_structured", return_value={"items": []}), \
             patch("bot._should_suppress_notification", side_effect=lambda source, kind, now=None: source.endswith("alpha")), \
             patch("bot._notification_feedback_bias", side_effect=lambda source, kind, now=None, days=30: 4 if source.endswith("beta") else 0):
            queue = bot.build_proactive_v2_queue(now=now, families={"intelligence"})

        self.assertEqual(queue[0]["title"], "Beta")
        self.assertFalse(queue[0]["suppressed"])
        self.assertTrue(any(item["title"] == "Alpha" and item["suppressed"] for item in queue))

    def test_proactive_v2_snapshot_reports_suppressed_and_top_items(self):
        now = bot.datetime.now(bot.SGT)
        fake_queue = [
            {"title": "Top item", "score": 88, "priority": "high", "suppressed": False, "feedback_bias": 2},
            {"title": "Muted item", "score": 61, "priority": "medium", "suppressed": True, "feedback_bias": -2},
        ]
        with patch("bot.build_proactive_v2_queue", return_value=fake_queue):
            snapshot = bot.build_proactive_v2_snapshot(now=now, limit=3)

        self.assertEqual(snapshot["ready_count"], 1)
        self.assertEqual(snapshot["suppressed_count"], 1)
        self.assertEqual(snapshot["top"][0]["title"], "Top item")
        self.assertTrue(snapshot["changed"])

    def test_proactive_v2_snapshot_keeps_digest_out_of_priority_queue(self):
        now = bot.datetime.now(bot.SGT)
        fake_queue = [
            {"family": "digest", "title": "Morning digest", "score": 78, "priority": "medium", "suppressed": False},
            {"family": "task", "title": "Task", "score": 80, "priority": "high", "suppressed": False},
        ]
        with patch("bot.build_proactive_v2_queue", return_value=fake_queue):
            snapshot = bot.build_proactive_v2_snapshot(now=now, limit=3)

        self.assertEqual([item["title"] for item in snapshot["top"]], ["Task"])

    def test_curated_digest_entries_rank_diverse_topics(self):
        fake_topics = [("SG Education", "edu"), ("AI", "ai")]

        def fake_google_news(query, max_items=4):
            if query == "edu":
                return [
                    {"title": "MOE policy update for schools", "url": "https://example.com/edu-1", "source": "CNA"},
                    {"title": "Extra school explainer", "url": "https://example.com/edu-2", "source": "ST"},
                ]
            return [
                {"title": "AI developer release notes", "url": "https://example.com/ai-1", "source": "The Verge"},
                {"title": "AI listicle filler", "url": "https://example.com/ai-2", "source": "Blog"},
            ]

        with patch("bot._news_topics", return_value=fake_topics), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(limit=2, fetch_limit=2, record=False)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["label"], "SG Education")
        self.assertEqual(entries[1]["label"], "AI")
        self.assertTrue(all(entry.get("why") for entry in entries))

    def test_curated_digest_keeps_first_class_interest_slots(self):
        fake_topics = [
            ("SG Education", "edu"),
            ("AI", "ai"),
            ("🏎️ F1", "f1"),
            ("⚽ Liverpool / EPL", "lfc"),
            ("Developer", "dev"),
        ]

        def fake_google_news(query, max_items=4):
            return [{
                "title": f"{query} ordinary update",
                "url": f"https://example.com/{query}",
                "source": "Example",
            }]

        with patch("bot._news_topics", return_value=fake_topics), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(limit=4, fetch_limit=2, record=False)

        labels = [entry["label"] for entry in entries]
        self.assertIn("🏎️ F1", labels)
        self.assertIn("⚽ Liverpool / EPL", labels)

    def test_curated_digest_keeps_seen_f1_when_no_fresh_f1_item_available(self):
        f1_item = {"title": "F1 paddock update", "url": "https://example.com/f1", "source": "Example"}
        seen_key = search_service.news_item_key(f1_item)

        def fake_google_news(query, max_items=4):
            if query == "f1":
                return [f1_item]
            return [{
                "title": f"{query} policy update today",
                "url": f"https://example.com/{query}",
                "source": "Example",
            }]

        with patch("bot._news_topics", return_value=[("SG Education", "edu"), ("AI", "ai"), ("🏎️ F1", "f1")]), \
             patch("bot._recent_news_digest_keys", return_value={seen_key}), \
             patch("search_service.google_news", side_effect=fake_google_news):
            entries = bot.build_curated_digest_entries(limit=3, fetch_limit=2, record=False)

        self.assertIn("🏎️ F1", [entry["label"] for entry in entries])

    def test_curated_digest_shows_required_topic_when_feed_returns_no_items(self):
        with patch("bot._news_topics", return_value=[("SG Education", "edu"), ("🏎️ F1", "f1")]), \
             patch("bot._recent_news_digest_keys", return_value=set()), \
             patch("search_service.google_news", side_effect=lambda query, max_items=4: [] if query == "f1" else [{
                 "title": "MOE policy update today",
                 "url": "https://example.com/edu",
                 "source": "Example",
             }]):
            entries = bot.build_curated_digest_entries(limit=2, fetch_limit=2, record=False)

        f1_entry = next(entry for entry in entries if entry["label"] == "🏎️ F1")
        self.assertIn("no fresh headline returned", f1_entry["item"]["title"])

    def test_format_curated_digest_includes_why_lines(self):
        text = bot.format_curated_digest([
            {
                "label": "AI",
                "why": "product/build relevance",
                "item": {"title": "AI developer release notes", "source": "The Verge"},
            }
        ])

        self.assertIn("Why it matters", text)
        self.assertIn("product/build relevance", text)

    def test_pwa_lfc_prompt_includes_news_search_tools(self):
        tools = bot.pwa_tools_for_message("latest LFC transfer rumours and injuries")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertIn("get_liverpool_brief", names)

    def test_pwa_lfc_correction_includes_structured_brief(self):
        tools = bot.pwa_tools_for_message("Liverpool didn't host Man Utd yesterday. Get your facts straight pls")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertIn("get_liverpool_brief", names)

    def test_pwa_lfc_followup_uses_recent_sports_context(self):
        tools = bot.pwa_tools_for_message(
            "So what was the match like?",
            recent_context="User corrected that Liverpool did not host Man Utd yesterday.",
        )
        names = {tool["name"] for tool in tools}

        self.assertIn("get_liverpool_brief", names)

    def test_forced_lfc_tool_uses_recent_match_context(self):
        forced = bot._forced_tool_for_current_turn(
            [
                {"role": "user", "content": "Liverpool didn't host Man Utd yesterday."},
                {"role": "assistant", "content": "I should verify the fixture."},
                {"role": "user", "content": "So what was the match like?"},
            ],
            [bot.LIVERPOOL_BRIEF_TOOL, bot.NEWS_TOOL],
        )

        self.assertEqual(forced, "get_liverpool_brief")

    def test_system_prompt_requires_supporter_mood_read_after_sports_facts(self):
        prompt = bot.SYSTEM_PROMPT()

        self.assertIn("After giving verified Liverpool or F1 scores/match details", prompt)
        self.assertIn("supporter-read", prompt)
        self.assertIn("Do not let mood-reading replace the verified facts", prompt)

    def test_lfc_player_chat_is_not_quick_routed(self):
        text = "Still anxious about this weekend lfc big match. Hope wirtz and isak have a banger."

        self.assertTrue(bot._looks_tool_heavy(text))

    def test_lfc_player_names_include_news_tools(self):
        tools = bot.pwa_tools_for_message("Hope Wirtz and Isak have a banger this weekend")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_latest_news", names)
        self.assertIn("get_liverpool_brief", names)

    def test_pwa_f1_prompt_includes_structured_sports_tool(self):
        tools = bot.pwa_tools_for_message("latest F1 standings and Mercedes qualifying result")
        names = {tool["name"] for tool in tools}

        self.assertIn("get_f1_brief", names)

    def test_deep_model_selected_for_architecture_work_when_configured(self):
        with patch.object(bot, "DEEP_MODEL", "deep-model"), patch.object(bot, "AGENTIC_MODEL", "agentic-model"):
            selected = bot._agentic_model_for_messages([
                {"role": "user", "content": "review this architecture and refactor the backend"}
            ])

        self.assertEqual(selected, "deep-model")

    def test_agentic_model_selected_for_ordinary_chat(self):
        with patch.object(bot, "DEEP_MODEL", "deep-model"), patch.object(bot, "AGENTIC_MODEL", "agentic-model"):
            selected = bot._agentic_model_for_messages([
                {"role": "user", "content": "how are we doing today?"}
            ])

        self.assertEqual(selected, "agentic-model")

    def test_memory_categories_include_new_buckets_and_aliases(self):
        self.assertIn("teaching", bot.gs.DEFAULT_MEMORY)
        self.assertIn("business", bot.gs.DEFAULT_MEMORY)
        self.assertIn("sports", bot.gs.DEFAULT_MEMORY)
        self.assertIn("constraints", bot.gs.DEFAULT_MEMORY)
        self.assertIn("recent_summaries", bot.gs.DEFAULT_MEMORY)
        self.assertIn("topic_profiles", bot.gs.DEFAULT_MEMORY)
        self.assertIn("correction_ledger", bot.gs.DEFAULT_MEMORY)
        self.assertIn("self_reflections", bot.gs.DEFAULT_MEMORY)
        self.assertIn("source_notes", bot.gs.DEFAULT_MEMORY)

        with (
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            memory = bot.gs.add_memory("lfc", "Liverpool context belongs here")

        self.assertIn("Liverpool context belongs here", memory["sports"])
        self.assertTrue(set_config.called)

        with (
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            memory = bot.gs.add_memory("mistake", "Do not repeat this correction")

        self.assertIn("Do not repeat this correction", memory["correction_ledger"])
        self.assertTrue(set_config.called)

        with (
            patch.object(bot.gs, "get_config", return_value=""),
            patch.object(bot.gs, "set_config") as set_config,
        ):
            memory = bot.gs.add_memory("knowledge", "Source-backed Liverpool note")

        self.assertIn("Source-backed Liverpool note", memory["source_notes"])
        self.assertTrue(set_config.called)

    def test_topic_profile_storage_replaces_by_topic(self):
        store = {}

        with (
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
        ):
            first = bot.gs.add_topic_profile({
                "topic": "MotoGP",
                "category": "sports",
                "track": ["Ducati", "Marc Marquez"],
                "live_facts": ["standings", "race results"],
            })
            second = bot.gs.add_topic_profile({
                "topic": "MotoGP",
                "category": "sports",
                "track": ["Ducati", "race weekends"],
            })

        self.assertEqual(first["topic"], "MotoGP")
        self.assertEqual(second["topic"], "MotoGP")
        memory = json.loads(store["assistant_memory"])
        self.assertEqual(len(memory["topic_profiles"]), 1)
        self.assertIn("race weekends", memory["topic_profiles"][0])

    def test_new_interest_forces_topic_profile_tool(self):
        forced = bot._forced_tool_for_text(
            "New interest: MotoGP. Track Ducati, Marc Marquez, standings and race weekends.",
            [{"name": "create_topic_profile"}, {"name": "remember_user_info"}],
        )

        self.assertEqual(forced, "create_topic_profile")

    def test_pwa_new_interest_includes_topic_profile_tool(self):
        tools = bot.pwa_tools_for_message("I'm getting into Japanese city pop. Build me a beginner map.")
        names = {tool["name"] for tool in tools}

        self.assertIn("create_topic_profile", names)

    def test_execute_topic_profile_tool(self):
        with patch.object(bot.gs, "add_topic_profile", return_value={
            "topic": "MotoGP",
            "category": "sports",
            "track": ["Ducati"],
            "live_facts": ["standings"],
        }):
            result = asyncio.run(bot._execute_tool("create_topic_profile", {
                "topic": "MotoGP",
                "category": "sports",
                "track": ["Ducati"],
                "live_facts": ["standings"],
            }))

        self.assertIn("Created topic profile: MotoGP", result)

    def test_chat_learning_event_records_correction_and_reflection(self):
        store = {}

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
        ):
            recorded = bot.record_chat_learning_event(
                "Actually Hira, Wirtz and Isak are Liverpool context now.",
                "Got it.",
                source="test",
            )

        self.assertEqual({item["type"] for item in recorded}, {"correction", "self_reflection"})
        memory = json.loads(store["assistant_memory"])
        self.assertEqual(len(memory["correction_ledger"]), 1)
        self.assertEqual(len(memory["self_reflections"]), 1)
        self.assertIn("Wirtz and Isak", memory["correction_ledger"][0])

    def test_execute_source_note_tool(self):
        with patch.object(bot.gs, "add_source_note", return_value={
            "topic": "Liverpool",
            "source": "Official site",
            "durability": "live_check",
        }):
            result = asyncio.run(bot._execute_tool("remember_source_insight", {
                "topic": "Liverpool",
                "source": "Official site",
                "source_url": "https://www.liverpoolfc.com/",
                "insight": "Match line-ups are live facts.",
                "durability": "live_check",
                "confidence": "official",
            }))

        self.assertIn("Stored source note for Liverpool", result)

    def test_source_discipline_flags_volatile_sports_questions(self):
        discipline = bot.source_discipline_for_text("latest LFC lineup and transfer rumours")

        self.assertTrue(discipline["needs_live_check"])
        self.assertEqual(discipline["confidence"], "needs_live_source")
        self.assertIn("get_liverpool_brief", discipline["recommended_tools"])

    def test_memory_review_summarises_buckets(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["correction_ledger"] = ["Correction A"]
        fake_memory["source_notes"] = ["Source note A", "Source note B"]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
        ):
            review = bot.build_memory_review(limit=1)

        self.assertTrue(review["ok"])
        self.assertEqual(review["total_items"], 3)
        self.assertEqual(review["buckets"]["source_notes"]["count"], 2)
        self.assertEqual(review["buckets"]["source_notes"]["recent"], ["Source note B"])

    def test_relevant_memory_retrieval_prioritises_corrections(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["correction_ledger"] = [
            {"correction": "When Herwanto asks about HIRA upgrades, update the growth log after changes."}
        ]
        fake_memory["topic_profiles"] = ["Liverpool context should use live sources."]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
        ):
            recalled = bot.retrieve_relevant_memory("upgrade HIRA memory and remember growth log", limit=2)
            hint = bot.intent_lens_hint("upgrade HIRA memory and remember growth log")

        self.assertEqual(recalled[0]["category"], "correction_ledger")
        self.assertIn("growth log", recalled[0]["text"])
        self.assertIn("Likely intent", hint)
        self.assertIn("correction_ledger", hint)

    def test_proactive_intelligence_flags_packed_due_marking_day(self):
        load = {
            "today": {
                "score": 76,
                "load": "Packed",
                "marking_scripts": 22,
            },
            "days": [
                {"date": "2026-05-02", "score": 76, "label": "Today", "load": "Packed"},
                {"date": "2026-05-03", "score": 30, "label": "Sun", "load": "Pretty chill"},
            ],
        }
        tasks = {
            "items": [
                {
                    "id": "7",
                    "description": "Submit CCA attendance",
                    "due": "2026-05-03",
                }
            ]
        }
        now = bot.SGT.localize(bot.datetime(2026, 5, 2, 9, 0))

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "build_daily_load", return_value=load),
            patch.object(bot, "build_task_structured", return_value=tasks),
            patch.object(bot.gs, "get_followups", return_value=[]),
        ):
            insights = bot.build_proactive_intelligence_insights(now=now)

        self.assertTrue(insights)
        self.assertEqual(insights[0]["title"], "Workload pinch point")
        self.assertIn("22 unmarked", insights[0]["body"])

    def test_due_proactive_intelligence_deduplicates_seen_items(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 2, 9, 0))
        insight = {
            "id": "2026-05-02:quiet_window",
            "title": "Quiet window",
            "body": "Move one deeper project.",
            "priority": "low",
        }

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "build_proactive_intelligence_insights", return_value=[insight]),
            patch.object(bot.gs, "get_config", return_value=json.dumps({insight["id"]: now.isoformat()})),
        ):
            self.assertEqual(bot.due_proactive_intelligence(now), [])

    def test_find_available_training_slots_avoids_cca_day_and_calendar_conflicts(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 4, 9, 0))

        def fake_lessons(target):
            return ([{"start": "08:00", "end": "14:30", "subject": "ML", "description": "Lesson"}], "Odd")

        def fake_events(target):
            if target.isoformat() == "2026-05-05":
                return [{
                    "summary": "Football CCA",
                    "description": "",
                    "location": "",
                    "start": {"dateTime": "2026-05-05T15:00:00+08:00"},
                    "end": {"dateTime": "2026-05-05T18:00:00+08:00"},
                }]
            if target.isoformat() == "2026-05-06":
                return [{
                    "summary": "HDB appointment",
                    "description": "",
                    "location": "",
                    "start": {"dateTime": "2026-05-06T15:00:00+08:00"},
                    "end": {"dateTime": "2026-05-06T16:00:00+08:00"},
                }]
            return []

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot, "datetime", wraps=bot.datetime) as fake_datetime,
            patch.object(bot, "_lessons_for_date", side_effect=fake_lessons),
            patch.object(bot, "_calendar_events_for_date", side_effect=fake_events),
        ):
            fake_datetime.now.return_value = now
            result = bot.find_available_training_slots(
                days=4,
                duration_minutes=60,
                window_start="14:00",
                window_end="18:00",
                avoid_keywords=["cca", "football"],
                purpose="Sahibba training",
            )

        self.assertIn("Checked timetable + Google Calendar", result)
        self.assertIn("Wed 6 May", result)
        self.assertIn("16:00-18:00", result)
        self.assertIn("Tue 5 May: avoided", result)
        self.assertNotIn("Tue 5 May, Odd week: 14:30", result)

    def test_runtime_status_contains_observability_sections(self):
        fake_memory = {category: [] for category in bot.MEMORY_DISPLAY_CATEGORIES}
        fake_memory["sports"] = ["Liverpool"]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_memory", return_value=fake_memory),
            patch.object(bot.gs, "get_projects", return_value=[{"project": "GamePlan"}]),
            patch.object(bot.gs, "get_app_notifications", return_value=[{"id": "1"}]),
            patch.object(bot.gs, "get_web_push_subscriptions", return_value=[{"endpoint": "x"}]),
            patch.object(bot.gs, "gmail_ok", return_value=True),
            patch.object(bot.gs, "get_config", return_value="2026-05-02"),
            patch.object(bot, "_get_redis", return_value=None),
        ):
            status = bot.build_runtime_status()

        self.assertIn("memory", status)
        self.assertIn("integrations", status)
        self.assertEqual(status["memory_buckets"]["sports"], 1)
        self.assertEqual(status["projects"]["count"], 1)
        self.assertEqual(status["notifications"]["queued_count"], 1)

    def test_pwa_link_prompt_includes_fetch_url_tool(self):
        tools = bot.pwa_tools_for_message("check this link https://www.formula1.com/en/teams")
        names = {tool["name"] for tool in tools}

        self.assertIn("fetch_url", names)
        self.assertIn("remember_source_insight", names)

    def test_fill_mtl_percentage_scores_updates_blank_fa2_percentages(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "FA1", "", "", "", "FA2", "", "", ""),
                        sheet_row("NO", "CLASS", "FULL NAME", "15", "30", "45", "%", "10", "25", "35", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "13", "19", "32", "71", "7", "10", "17", ""),
                        sheet_row("2", "S4-AN", "NAURA", "", "", "AB", "AB", "5", "14", "19", ""),
                        sheet_row("3", "S4-AN", "AUNI", "11", "18", "29", "64", "AB", "AB", "AB", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            result = bot.gs.fill_mtl_percentage_scores("S4-AN", "FA2")

        self.assertEqual(result["updated_cells"], 3)
        self.assertEqual(result["filled_numbers"], 2)
        self.assertEqual(result["copied_codes"], 1)
        data = fake_service.spreadsheets_api.values_api.batch_updates[0][1]["data"]
        self.assertEqual(
            [(item["range"], item["values"][0][0]) for item in data],
            [("'CG HERWANTO S4-AN'!K3", "49"), ("'CG HERWANTO S4-AN'!K4", "54"), ("'CG HERWANTO S4-AN'!K5", "AB")],
        )

    def test_analyze_mtl_scores_reports_stats_and_progress(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "FA1", "", "FA2", ""),
                        sheet_row("NO", "CLASS", "FULL NAME", "45", "%", "35", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "32", "71", "17", "49"),
                        sheet_row("2", "S4-AN", "NAURA", "AB", "AB", "19", "54"),
                        sheet_row("3", "S4-AN", "AUNI", "29", "64", "AB", "AB"),
                        sheet_row("4", "S4-AN", "HASLIANI", "29", "64", "19", "54"),
                        sheet_row("5", "S4-AN", "DANISH", "29", "64", "21", "60"),
                        sheet_row("6", "S4-AN", "YUSSOFF", "35", "77", "33", "94"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("S4-AN", "%", "FA1 %", "FA2 %")

        self.assertIn("mean 68", brief)
        self.assertIn("median 64", brief)
        self.assertIn("pass 5/5", brief)
        self.assertIn("Underperforming / watchlist", brief)
        self.assertIn("AIRA", brief)
        self.assertIn("Most improved", brief)
        self.assertIn("YUSSOFF", brief)
        self.assertIn("Progress: FA1 % -> FA2 %", brief)
        self.assertNotIn("Progress: FA1 45 -> FA2 35", brief)
        self.assertIn("Drastic drops", brief)

    def test_fa_percentage_analysis_uses_percent_columns_not_components(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "", "", "FA1", "", "", "", "FA2", "", "", ""),
                        sheet_row("NO", "CLASS", "FULL NAME", "LC 1 (20)", "LC 2 (20)", "15", "30", "45", "%", "10", "25", "35", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "14", "18", "13", "19", "32", "71", "7", "10", "17", "49"),
                        sheet_row("2", "S4-AN", "NAURA", "", "", "", "", "AB", "AB", "5", "14", "19", "54"),
                        sheet_row("3", "S4-AN", "AUNI", "14", "16", "11", "18", "29", "64", "AB", "AB", "AB", "AB"),
                        sheet_row("4", "S4-AN", "COLLAR", "0", "4", "5", "2", "11", "24", "0", "6", "6", "17"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("S4-AN", "FA1 %", "FA1 %", "FA2 %")

        self.assertIn("FA1 %: mean", brief)
        self.assertIn("This is the percentage column", brief)
        self.assertIn("Progress: FA1 % -> FA2 %", brief)
        self.assertNotIn("FA1 15: mean", brief)
        self.assertNotIn("FA1 45: mean", brief)

    def test_score_analysis_treats_zero_as_score_and_statuses_as_non_scoring(self):
        book = {
            "properties": {"title": "2026 S4 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO S4-AN"},
                "data": [{
                    "rowData": [
                        sheet_row("", "", "", "WA1"),
                        sheet_row("NO", "CLASS", "FULL NAME", "%"),
                        sheet_row("1", "S4-AN", "AIRA", "0"),
                        sheet_row("2", "S4-AN", "NAURA", "AB"),
                        sheet_row("3", "S4-AN", "AUNI", "VR"),
                        sheet_row("4", "S4-AN", "HASLIANI", "MC"),
                        sheet_row("5", "S4-AN", "DANISH", "50"),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("S4-AN", "WA1")

        self.assertIn("mean 25", brief)
        self.assertIn("pass 1/2", brief)
        self.assertIn("AIRA", brief)
        self.assertIn("0.0 (below 50)", brief)
        self.assertIn("AB (absent): 1", brief)
        self.assertIn("VR (valid reason): 1", brief)
        self.assertIn("MC (medical certificate): 1", brief)

    def test_sec1g2_alias_matches_ml_g2_and_wa1_percent_header(self):
        book = {
            "properties": {"title": "2026 S1 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO ML G2"},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "ML G2"),
                        sheet_row("VENUE:", "L4-11"),
                        sheet_row(),
                        sheet_row(),
                        sheet_row("NO", "CLASS", "FULL NAME", "PSLE MTL GRADE", "TARGET", "WA1 (40)", "WA1 %", "PreWA2 (20)", "WA2", "WA3", "EOY"),
                        sheet_row("1", "1 Anchor", "ZAARA", "6", "", "20", "50", "", "", "", ""),
                        sheet_row("2", "1 Beacon", "AQASYA", "4", "", "26", "65", "", "", "", ""),
                        sheet_row("3", "1 Compass", "XANDER", "6", "", "0", "0", "", "", "", ""),
                        sheet_row("4", "1 Flagship", "FIQRI", "4", "", "AB", "AB", "", "", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("sec1G2", "WA1 %")

        self.assertIn("ML G2", brief)
        self.assertIn("WA1 %: mean 38.3", brief)
        self.assertIn("pass 2/3", brief)
        self.assertIn("AB (absent): 1", brief)

    def test_sec2_prg_wa_columns_are_mock_not_actual_wa2(self):
        book = {
            "properties": {"title": "2026 S2 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO 2G3 ML"},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "2G3 ML"),
                        sheet_row(),
                        sheet_row("NO", "CLASS", "FULL NAME", "WA1", "Prg-WA2", "Prg-WA2", "Prg-WA2", "WA2", "WA3", "EOY"),
                        sheet_row("1", "S2-AN", "AMELIA", "85", "34", "20", "MC", "", "", ""),
                        sheet_row("2", "S2-AN", "MYSHA", "65", "30", "19", "26", "", "", ""),
                        sheet_row("3", "S2-BE", "AULIA", "60", "22", "14", "21", "", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            with self.assertRaises(ValueError):
                bot.gs.format_mtl_score_analysis("2G3", "WA2")
            mock = bot.gs.format_mtl_score_analysis("2G3", "pre WA")

        self.assertIn("Prg-WA2 1", mock)
        self.assertIn("Prg-WA2 2", mock)

    def test_sec3g3_layout_reads_actual_wa1_percent(self):
        book = {
            "properties": {"title": "2026 S3 MTL CLASSLIST"},
            "sheets": [{
                "properties": {"title": "CG HERWANTO 3G3 ML"},
                "data": [{
                    "rowData": [
                        sheet_row("TEACHER NAME:", "CG HERWANTO"),
                        sheet_row("GROUPING:", "3G3 ML"),
                        sheet_row("VENUE:", "L3-10"),
                        sheet_row(),
                        sheet_row(),
                        sheet_row("NO", "CLASS", "FULL NAME", "PSLE MTL GRADE", "S2 MTL RESULTS", "TARGET", "WA1 (20)", "WA1 %", "WA2", "WA3", "EOY"),
                        sheet_row("1", "S3-AN", "UMAIRA", "3", "", "", "10", "50", "", "", ""),
                        sheet_row("2", "S3-CO", "AYRA", "5", "", "", "12", "60", "", "", ""),
                        sheet_row("3", "S3-DA", "NINA", "4", "", "", "7", "35", "", "", ""),
                    ]
                }]
            }]
        }
        fake_service = FakeSheetsService(book)

        with (
            patch.object(bot.gs, "_sheets", return_value=fake_service),
            patch.object(bot.gs, "_configured_classlist_sheet_ids", return_value=["sheet-1"]),
        ):
            brief = bot.gs.format_mtl_score_analysis("3G3", "WA1 %")

        self.assertIn("3G3 ML", brief)
        self.assertIn("WA1 %: mean 48.3", brief)
        self.assertNotIn("WA1 (20): mean", brief)

    def test_reset_marking_request_forces_reset_tool(self):
        forced = bot._forced_tool_for_text(
            "reset marking load",
            [{"name": "reset_marking_load"}, {"name": "update_marking_progress"}],
        )

        self.assertEqual(forced, "reset_marking_load")

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

    def test_agentic_claude_continues_after_max_tokens(self):
        class MaxTokenMessages:
            def __init__(self):
                self.calls = []

            def create(self, **kwargs):
                self.calls.append(kwargs)
                if len(self.calls) == 1:
                    return SimpleNamespace(
                        stop_reason="max_tokens",
                        content=[SimpleNamespace(type="text", text="Done. Here's what was written:")],
                    )
                return SimpleNamespace(
                    stop_reason="end_turn",
                    content=[SimpleNamespace(type="text", text="\n- Filled all FA2 percentages.")],
                )

        fake_messages = MaxTokenMessages()
        fake_claude = SimpleNamespace(messages=fake_messages)

        with (
            patch.object(bot, "claude", fake_claude),
            patch.object(bot, "SYSTEM_PROMPT", return_value="system"),
        ):
            reply = asyncio.run(bot._run_agentic_claude(
                [{"role": "user", "content": "fill percentages"}],
                tools=[],
                max_tokens=10,
            ))

        self.assertIn("Here's what was written", reply)
        self.assertIn("Filled all FA2 percentages", reply)
        self.assertEqual(len(fake_messages.calls), 2)

    def test_email_followup_forces_gmail_before_action(self):
        messages = [{"role": "user", "content": "read my latest work email and note the meeting details for follow up"}]
        tools = [{"name": "get_gmail_brief"}, {"name": "create_followup"}]

        self.assertEqual(bot._forced_tool_for_current_turn(messages, tools), "get_gmail_brief")

    def test_weather_question_forces_nea_weather_tool(self):
        forced = bot._forced_tool_for_text(
            "Will it rain in Yishun later?",
            [{"name": "get_nea_weather"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_nea_weather")

    def test_temperature_question_forces_nea_weather_tool(self):
        forced = bot._forced_tool_for_text(
            "Yishun high temp tomorrow?",
            [{"name": "get_nea_weather"}, {"name": "get_latest_news"}],
        )

        self.assertEqual(forced, "get_nea_weather")

    def test_pwa_weather_message_gets_weather_tool(self):
        tools = bot.pwa_tools_for_message("latest weather from NEA")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_nea_weather", names)

    def test_pwa_temperature_message_gets_weather_tool(self):
        tools = bot.pwa_tools_for_message("Yishun high temp tomorrow?")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_nea_weather", names)

    def test_prayer_question_forces_muis_prayer_tool(self):
        forced = bot._forced_tool_for_text(
            "What time is zuhur today?",
            [{"name": "get_muis_prayer_times"}, {"name": "get_assistant_context"}],
        )

        self.assertEqual(forced, "get_muis_prayer_times")

    def test_pwa_prayer_message_gets_muis_prayer_tool(self):
        tools = bot.pwa_tools_for_message("What time is zohor today?")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_muis_prayer_times", names)

    def test_khutbah_question_forces_muis_khutbah_tool(self):
        forced = bot._forced_tool_for_text(
            "What is today's Friday khutbah about?",
            [{"name": "get_muis_prayer_times"}, {"name": "get_muis_friday_khutbah"}],
        )

        self.assertEqual(forced, "get_muis_friday_khutbah")

    def test_pwa_khutbah_message_gets_muis_khutbah_tool(self):
        tools = bot.pwa_tools_for_message("khutbah summary before jumuah")
        names = [tool["name"] for tool in tools]

        self.assertIn("get_muis_friday_khutbah", names)

    def test_execute_muis_prayer_tool_uses_bundled_muis_data(self):
        async def run():
            return await bot._execute_tool("get_muis_prayer_times", {
                "date": "2026-05-01",
                "prayer": "zuhur",
            })

        self.assertIn("Zohor 13:03", asyncio.run(run()))

    def test_execute_khutbah_tool_uses_muis_service(self):
        khutbah = {
            "date": "2026-05-01",
            "title": "Youth and today's challenges",
            "summary": "Youth challenges affect society tomorrow.",
            "key_points": ["Evaluate ethics through Islam"],
            "url": "https://www.muis.gov.sg/resources/khutbah-and-religious-advice/khutbah/youth-and-today-s-challenges-/",
            "pdf_url": "https://example.com/khutbah.pdf",
        }

        async def run():
            with patch.object(bot.isl, "latest_khutbah", return_value=khutbah):
                return await bot._execute_tool("get_muis_friday_khutbah", {"date": "2026-05-01"})

        result = asyncio.run(run())
        self.assertIn("Friday khutbah heads-up", result)
        self.assertIn("Youth and today's challenges", result)
        self.assertIn("Evaluate ethics through Islam", result)

    def test_muis_khutbah_listing_parser_reads_latest_english_card(self):
        html = '''
        <a href="/resources/khutbah-and-religious-advice/khutbah/youth-and-today-s-challenges-/">
          <p>1 May 2026</p>
          <span title="Youth and today’s challenges ">Youth and today’s challenges</span>
          <p class="line-clamp-3">The challenges faced by youth today.</p>
          <p>English</p>
        </a>
        '''

        records = islamic_service._parse_khutbah_listing(html)

        self.assertEqual(records[0]["date"], "2026-05-01")
        self.assertEqual(records[0]["title"], "Youth and today’s challenges")
        self.assertEqual(records[0]["language"], "English")

    def test_execute_weather_tool_uses_weather_service(self):
        async def run():
            with patch.object(bot.ws, "build_weather_brief", return_value="NEA weather: Yishun"):
                return await bot._execute_tool("get_nea_weather", {"area": "Yishun"})

        self.assertEqual(asyncio.run(run()), "NEA weather: Yishun")

    def test_weather_brief_includes_current_readings_and_air_quality(self):
        payloads = {
            weather_service.TWO_HOUR_V2: {
                "code": 0,
                "data": {
                    "items": [{
                        "update_timestamp": "2026-04-30T20:00:00+08:00",
                        "valid_period": {"start": "2026-04-30T20:00:00+08:00", "end": "2026-04-30T22:00:00+08:00"},
                        "forecasts": [{"area": "Yishun", "forecast": {"text": "Cloudy"}}],
                    }]
                },
            },
            weather_service.AIR_TEMPERATURE_V1: {
                "metadata": {"stations": [{"id": "S1", "name": "Yishun"}]},
                "items": [{"readings": [{"station_id": "S1", "value": 29.4}]}],
            },
            weather_service.RELATIVE_HUMIDITY_V1: {
                "metadata": {"stations": [{"id": "S2", "name": "Yishun"}]},
                "items": [{"readings": [{"station_id": "S2", "value": 78}]}],
            },
            weather_service.PSI_V1: {
                "items": [{
                    "readings": {
                        "psi_twenty_four_hourly": {"north": 42},
                        "pm25_twenty_four_hourly": {"north": 8},
                    }
                }],
            },
            weather_service.PM25_V1: {
                "items": [{"readings": {"pm25_one_hourly": {"north": 6}}}],
            },
        }

        with patch.object(weather_service, "_get_json", side_effect=lambda url: payloads[url]):
            brief = weather_service.build_weather_brief("Yishun", include_24h=False)

        self.assertIn("Nowcast: Cloudy", brief)
        self.assertIn("Temp 29.4 deg C", brief)
        self.assertIn("Humidity 78%", brief)
        self.assertIn("24h PSI 42", brief)
        self.assertIn("1h PM2.5 6 ug/m3", brief)

    def test_gmail_body_text_decodes_plain_parts(self):
        encoded = bot.base64.urlsafe_b64encode(
            b"Meeting on Friday at 2pm. Please follow up with the vendor."
        ).decode().rstrip("=")
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": encoded},
                }
            ],
        }

        self.assertIn("Meeting on Friday at 2pm", bot.gs._gmail_body_text(payload))

    def test_task_brief_hides_internal_metadata(self):
        due = (bot.datetime.now(bot.SGT).date() + bot.timedelta(days=1)).isoformat()
        tasks = [
            {
                "id": "31",
                "description": "Arrange relief teacher for ML Sec 2",
                "due": due,
                "category": "Teaching",
                "priority": "medium",
                "effort": "medium",
                "next_action": "",
            }
        ]

        with patch.object(bot.gs, "enriched_reminders", return_value=tasks):
            brief = bot.build_task_brief(days=7)

        self.assertIn("Arrange relief teacher for ML Sec 2", brief)
        self.assertNotIn("Teaching; medium; medium", brief)
        self.assertNotIn("_Teaching", brief)

    def test_marking_brief_shows_outstanding_and_collected_date(self):
        collected = (bot.datetime.now(bot.SGT).date() - bot.timedelta(days=2)).isoformat()
        tasks = [
            {
                "id": "1",
                "title": "Kefahaman 2G3",
                "total_scripts": 34,
                "marked_count": 12,
                "stack_count": 1,
                "collected_date": collected,
                "notes": "",
                "done": False,
            }
        ]

        with (
            patch.object(bot, "google_ok", return_value=True),
            patch.object(bot.gs, "get_marking_tasks", return_value=tasks),
        ):
            brief = bot.build_marking_brief()

        self.assertIn("Kefahaman 2G3", brief)
        self.assertIn("12 of 34 scripts marked", brief)
        self.assertIn("22 outstanding", brief)
        self.assertIn("2 days ago", brief)
        self.assertNotIn("[1]", brief)

    def test_marking_persists_at_full_count_until_explicitly_completed(self):
        store = {}

        def fake_get_config(key):
            return store.get(key, "")

        def fake_set_config(key, value):
            store[key] = value

        with (
            patch.object(bot.gs, "get_config", side_effect=fake_get_config),
            patch.object(bot.gs, "set_config", side_effect=fake_set_config),
        ):
            task = bot.gs.add_marking_task("Kefahaman 2G3", total_scripts=34, collected_date="2026-04-27")
            bot.gs.update_marking_progress(task["id"], marked_count=34)

            active = bot.gs.get_marking_tasks()
            stored = json.loads(store["marking_tasks"])[0]

            self.assertEqual(len(active), 1)
            self.assertFalse(stored["done"])
            self.assertEqual(active[0]["marked_count"], 34)

            bot.gs.update_marking_progress(task["id"], done=True)

            self.assertEqual(bot.gs.get_marking_tasks(), [])
            self.assertEqual(len(bot.gs.get_marking_tasks(include_done=True)), 1)

    def test_reset_marking_tasks_clears_active_stacks(self):
        store = {}

        def fake_get_config(key):
            return store.get(key, "")

        def fake_set_config(key, value):
            store[key] = value

        with (
            patch.object(bot.gs, "get_config", side_effect=fake_get_config),
            patch.object(bot.gs, "set_config", side_effect=fake_set_config),
        ):
            bot.gs.add_marking_task("1G2: Karangan", total_scripts=10, collected_date="2026-04-30")
            bot.gs.add_marking_task("2G3: Kefahaman", total_scripts=12, collected_date="2026-04-30")

            result = bot.gs.reset_marking_tasks()

            self.assertEqual(result["cleared_count"], 2)
            self.assertEqual(bot.gs.get_marking_tasks(), [])
            self.assertEqual(len(bot.gs.get_marking_tasks(include_done=True)), 2)

    def test_completing_marking_reminder_closes_matching_marking_stack(self):
        reminders = [
            {
                "id": "12",
                "description": "Finish Kefahaman 2G3 marking",
                "due": "2026-04-29",
                "category": "Marking",
                "done": False,
            }
        ]
        marking_tasks = [
            {
                "id": "1",
                "title": "Kefahaman 2G3",
                "total_scripts": 34,
                "marked_count": 34,
                "stack_count": 1,
                "collected_date": "2026-04-27",
                "notes": "",
                "done": False,
            }
        ]

        with (
            patch.object(bot.gs, "get_reminders", return_value=reminders),
            patch.object(bot.gs, "mark_done", return_value=True),
            patch.object(bot.gs, "get_marking_tasks", return_value=marking_tasks),
            patch.object(bot.gs, "update_marking_progress", return_value={**marking_tasks[0], "done": True}) as update_marking,
        ):
            ok, synced = bot.complete_reminder_by_id("12")

        self.assertTrue(ok)
        self.assertEqual(synced["title"], "Kefahaman 2G3")
        update_marking.assert_called_once_with("1", done=True)

    def test_complete_task_tool_marks_plural_matching_reminders(self):
        reminders = [
            {
                "id": "21",
                "description": "ESWG EdTech vendor follow-up",
                "due": "2026-04-15",
                "category": "Teaching",
                "done": False,
            },
            {
                "id": "22",
                "description": "PLT EdTech project update",
                "due": "2026-04-16",
                "category": "Projects",
                "done": False,
            },
            {
                "id": "23",
                "description": "PLT admin project update",
                "due": "2026-04-16",
                "category": "Projects",
                "done": False,
            },
        ]

        with (
            patch.object(bot.gs, "get_reminders", return_value=reminders),
            patch.object(bot.gs, "mark_done", return_value=True) as mark_done,
        ):
            result = asyncio.run(bot._execute_tool(
                "complete_task_by_text",
                {"query": "The edtech entries have been completed"},
            ))

        self.assertIn("Marked 2 reminders done", result)
        self.assertIn("#21 ESWG EdTech vendor follow-up", result)
        self.assertIn("#22 PLT EdTech project update", result)
        self.assertNotIn("#23", result)
        self.assertEqual([call.args[0] for call in mark_done.call_args_list], ["21", "22"])

    def test_home_marking_summary_ignores_completed_stacks(self):
        completed = {
            "id": "1",
            "title": "Kefahaman 2G3",
            "total_scripts": 34,
            "marked_count": 34,
            "stack_count": 1,
            "collected_date": "2026-04-27",
            "notes": "",
            "done": True,
            "completed_at": bot.datetime.now(bot.SGT).date().isoformat(),
        }

        with patch.object(web_app.bot.gs, "get_marking_tasks", return_value=[]):
            summary = web_app._marking_summary()

        self.assertEqual(summary["active_stacks"], 0)
        self.assertEqual(summary["total_scripts"], 0)
        self.assertEqual(summary["marked_scripts"], 0)
        self.assertEqual(summary["unmarked_scripts"], 0)
        self.assertTrue(summary["all_clear"])
        self.assertEqual(summary["sets"], [])

    def test_home_marking_summary_returns_per_set_breakdown(self):
        tasks = [
            {
                "id": "1",
                "title": "1G2: Karangan",
                "total_scripts": 34,
                "marked_count": 12,
                "stack_count": 1,
                "collected_date": "2026-04-27",
                "notes": "",
                "done": False,
            },
            {
                "id": "2",
                "title": "1G2: Kefahaman",
                "total_scripts": 22,
                "marked_count": 5,
                "stack_count": 1,
                "collected_date": "2026-04-29",
                "notes": "",
                "done": False,
            },
        ]

        with patch.object(web_app.bot.gs, "get_marking_tasks", return_value=tasks):
            summary = web_app._marking_summary()

        self.assertEqual(summary["active_stacks"], 2)
        self.assertEqual(summary["total_scripts"], 56)
        self.assertEqual(summary["marked_scripts"], 17)
        self.assertEqual(summary["unmarked_scripts"], 39)
        self.assertEqual(summary["sets"][0]["title"], "1G2: Karangan")
        self.assertEqual(summary["sets"][0]["display_title"], "1G2 [Karangan]")
        self.assertEqual(summary["sets"][1]["title"], "1G2: Kefahaman")
        self.assertEqual(summary["sets"][1]["display_title"], "1G2 [Kefahaman]")
        self.assertEqual(summary["sets"][0]["progress_label"], "12/34")
        self.assertEqual(summary["sets"][0]["unmarked_scripts"], 22)

    def test_marking_display_title_accepts_bracket_format(self):
        self.assertEqual(web_app._marking_display_title("3G3: karangan naratif"), "3G3 [karangan naratif]")
        self.assertEqual(web_app._marking_display_title("2G3 [HBL on SLS]"), "2G3 [HBL on SLS]")

    def test_archive_app_notifications_hides_selected_items(self):
        store = {
            "app_notifications": json.dumps([
                {
                    "id": "1",
                    "kind": "reminder",
                    "title": "H.I.R.A nudge",
                    "body": "Check bills",
                    "created": "2026-04-28T19:19:00+08:00",
                    "source": "nudge:1",
                    "seen_by": [],
                    "archived": False,
                },
                {
                    "id": "2",
                    "kind": "update",
                    "title": "H.I.R.A",
                    "body": "Still here",
                    "created": "2026-04-28T19:20:00+08:00",
                    "source": "",
                    "seen_by": [],
                    "archived": False,
                },
            ])
        }

        def fake_get_config(key):
            return store.get(key, "")

        def fake_set_config(key, value):
            store[key] = value

        with (
            patch.object(bot.gs, "get_config", side_effect=fake_get_config),
            patch.object(bot.gs, "set_config", side_effect=fake_set_config),
        ):
            archived = bot.gs.archive_app_notifications(["1"])
            visible = bot.gs.get_app_notifications()

        self.assertEqual(archived, 1)
        self.assertEqual([item["id"] for item in visible], ["2"])

    def test_pdf_excerpt_prioritises_herwanto_timetable_pages(self):
        pages = [
            pdf_service.PdfPageText(1, "General staff briefing and school notices."),
            pdf_service.PdfPageText(2, "T. MTL Muhammad Herwanto Johari\nMon P2 ML L3-10\nTue P5 ML L4-12"),
            pdf_service.PdfPageText(3, "Canteen duty roster unrelated page."),
        ]

        excerpt, selected, text_pages = pdf_service.build_pdf_excerpt(
            pages,
            caption="Find Herwanto timetable",
            max_pages=1,
        )

        self.assertEqual(text_pages, 3)
        self.assertEqual(selected, [2])
        self.assertIn("Muhammad Herwanto Johari", excerpt)
        self.assertIn("Mon P2", excerpt)

    def test_calendar_event_matching_finds_event_by_text(self):
        events = [
            {
                "id": "evt-1",
                "summary": "CCA football briefing",
                "location": "Hall",
                "description": "",
                "start": {"dateTime": "2026-04-28T15:00:00+08:00"},
                "end": {"dateTime": "2026-04-28T16:00:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-2",
                "summary": "Parent meeting",
                "location": "General Office",
                "description": "",
                "start": {"dateTime": "2026-04-29T10:00:00+08:00"},
                "end": {"dateTime": "2026-04-29T10:30:00+08:00"},
                "_calendar_id": "primary",
            },
        ]

        with patch.object(bot.gs, "get_events_between", return_value=events):
            event, score = bot._find_best_calendar_event("football briefing")

        self.assertEqual(event["id"], "evt-1")
        self.assertGreater(score, 0.45)

    def test_duplicate_delete_query_prefers_exact_duplicate_group(self):
        events = [
            {
                "id": "evt-1",
                "summary": "CCA NSG duty",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T15:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T18:00:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-2",
                "summary": "NSG C Div Game - N2 vs Whitley / N2A vs Assumption Pathway",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T15:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T18:00:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-3",
                "summary": "NSG C Div Game - N2 vs Whitley / N2A vs Assumption Pathway",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T15:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T18:00:00+08:00"},
                "_calendar_id": "secondary",
            },
        ]

        with patch.object(bot.gs, "get_events_between", return_value=events):
            event, score = bot._resolve_calendar_event_for_deletion(
                "duplicate of my CCA NSG duty on calendar. Please remove 1"
            )

        self.assertEqual(event["id"], "evt-2")
        self.assertGreaterEqual(score, 0.45)

    def test_duplicate_delete_query_returns_no_match_when_multiple_duplicate_groups_are_ambiguous(self):
        events = [
            {
                "id": "evt-1",
                "summary": "Team sync",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T09:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T09:30:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-2",
                "summary": "Team sync",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-07T09:00:00+08:00"},
                "end": {"dateTime": "2026-05-07T09:30:00+08:00"},
                "_calendar_id": "secondary",
            },
            {
                "id": "evt-3",
                "summary": "Parent meeting",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-08T10:00:00+08:00"},
                "end": {"dateTime": "2026-05-08T10:30:00+08:00"},
                "_calendar_id": "primary",
            },
            {
                "id": "evt-4",
                "summary": "Parent meeting",
                "location": "",
                "description": "",
                "start": {"dateTime": "2026-05-08T10:00:00+08:00"},
                "end": {"dateTime": "2026-05-08T10:30:00+08:00"},
                "_calendar_id": "secondary",
            },
        ]

        with patch.object(bot.gs, "get_events_between", return_value=events):
            event, score = bot._resolve_calendar_event_for_deletion("remove 1 duplicate from my calendar")

        self.assertIsNone(event)
        self.assertEqual(score, 0)

    def test_gmail_account_extraction_detects_work_email(self):
        account, query = bot._extract_gmail_account_from_text("show my last 5 work emails")

        self.assertEqual(account, "work")
        self.assertEqual(query, "show my last 5")

    def test_work_gmail_can_reuse_personal_oauth_client(self):
        env = {
            "GOOGLE_GMAIL_CLIENT_ID": "client",
            "GOOGLE_GMAIL_CLIENT_SECRET": "secret",
            "GOOGLE_WORK_GMAIL_REFRESH_TOKEN": "work-refresh",
        }

        with patch.dict(os.environ, env, clear=True):
            self.assertTrue(bot.gs.gmail_ok("work"))
            self.assertFalse(bot.gs.gmail_ok("personal"))

    def test_prayer_reminder_has_catchup_window(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 1, 13, 18))
        plan = [{
            "key": "zohor",
            "label": "Zohor",
            "time": "13:03",
            "blocked_until": None,
            "note": "Pray as soon as it enters.",
        }]
        store = {}

        with (
            patch.object(bot, "_prayer_plan_for_date", return_value=plan),
            patch.object(bot.gs, "get_config", side_effect=lambda key: store.get(key, "")),
            patch.object(bot.gs, "set_config", side_effect=lambda key, value: store.__setitem__(key, value)),
            patch.dict(os.environ, {"HIRA_PRAYER_REMINDER_WINDOW_MINUTES": "20"}),
        ):
            due = bot._prayer_reminder_due(now)

        self.assertEqual(due["key"], "zohor")
        self.assertEqual(store["prayer_prompt:2026-05-01:zohor"], "13:18")

    def test_prayer_reminder_uses_fallback_when_config_unavailable(self):
        now = bot.SGT.localize(bot.datetime(2026, 5, 1, 13, 5))
        plan = [{
            "key": "zohor",
            "label": "Zohor",
            "time": "13:03",
            "blocked_until": None,
            "note": "Pray as soon as it enters.",
        }]
        bot._PRAYER_PROMPT_FALLBACK_KEYS.clear()

        with (
            patch.object(bot, "_prayer_plan_for_date", return_value=plan),
            patch.object(bot.gs, "get_config", side_effect=RuntimeError("sheets down")),
            patch.object(bot.gs, "set_config", side_effect=RuntimeError("sheets down")),
        ):
            first = bot._prayer_reminder_due(now)
            second = bot._prayer_reminder_due(now)

        self.assertEqual(first["key"], "zohor")
        self.assertIsNone(second)

if __name__ == "__main__":
    unittest.main()
