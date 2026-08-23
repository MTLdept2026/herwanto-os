import unittest
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import bot
import postgres_storage
import start
import web_app


class RailwayCronSchedulingTests(unittest.IsolatedAsyncioTestCase):
    def test_cron_specs_preserve_all_worker_jobs(self):
        daily = {item["name"] for item in bot._pwa_daily_job_specs()}
        repeating = {name for name, _job in bot._pwa_repeating_job_specs()}

        self.assertEqual(daily, {
            "morning_briefing",
            "evening_briefing",
            "weekly_planning",
            "friday_khutbah",
            "friday_checkin",
            "self_audit",
            "memory_consolidation",
        })
        self.assertEqual(repeating, {
            "proactive_nudges",
            "calendar_reminders",
            "proactive_intelligence",
            "daily_checkins",
            "prayer_reminders",
            "followups",
            "work_gmail_monitor",
        })

    def test_daily_window_uses_singapore_weekday_and_grace(self):
        friday = bot.SGT.localize(datetime(2026, 8, 21, 17, 19))
        after_grace = bot.SGT.localize(datetime(2026, 8, 21, 17, 21))
        saturday = bot.SGT.localize(datetime(2026, 8, 22, 17, 10))

        self.assertTrue(bot._pwa_daily_job_due(friday, 17, 0, days=(4,), grace_minutes=20))
        self.assertFalse(bot._pwa_daily_job_due(after_grace, 17, 0, days=(4,), grace_minutes=20))
        self.assertFalse(bot._pwa_daily_job_due(saturday, 17, 0, days=(4,), grace_minutes=20))

    async def test_cron_runs_repeating_jobs_and_continues_after_failure(self):
        calls = []

        async def succeeds(_context):
            calls.append("succeeds")

        async def fails(_context):
            calls.append("fails")
            raise RuntimeError("temporary provider failure")

        now = bot.SGT.localize(datetime(2026, 8, 22, 12, 0))
        with (
            patch.object(bot, "_pwa_daily_job_specs", return_value=[]),
            patch.object(bot, "_pwa_repeating_job_specs", return_value=[
                ("first", fails),
                ("second", succeeds),
            ]),
            patch.object(bot, "_release_unused_memory"),
        ):
            result = await bot.run_pwa_notification_cron(now=now)

        self.assertEqual(calls, ["fails", "succeeds"])
        self.assertEqual(result["attempted"], ["first", "second"])
        self.assertEqual(result["errors"], {"first": "temporary provider failure"})

    async def test_non_retry_daily_job_is_claimed_once_per_date(self):
        job = AsyncMock()
        claims = iter((True, False))
        spec = {
            "name": "friday_checkin",
            "hour": 17,
            "minute": 0,
            "job": job,
            "days": (4,),
        }
        now = bot.SGT.localize(datetime(2026, 8, 21, 17, 5))
        with (
            patch.object(bot, "_pwa_daily_job_specs", return_value=[spec]),
            patch.object(bot, "_pwa_repeating_job_specs", return_value=[]),
            patch.object(bot, "_acquire_job_lock", side_effect=lambda *_args: next(claims)),
            patch.object(bot, "_release_unused_memory"),
        ):
            await bot.run_pwa_notification_cron(now=now)
            await bot.run_pwa_notification_cron(now=now)

        job.assert_awaited_once_with(None)


class RailwayPushRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_shot_recovery_preserves_notification_and_briefing_checks(self):
        notifications = {"attempted": 2, "sent": 1, "skipped": 1}
        briefings = {"attempted": 1, "delivered": 1, "skipped": 0}
        with (
            patch.object(web_app, "recover_missed_push_notifications", return_value=notifications),
            patch.object(web_app, "recover_missed_daily_briefings", new=AsyncMock(return_value=briefings)),
        ):
            result = await web_app.run_web_push_recovery_once()

        self.assertEqual(result["notifications"], notifications)
        self.assertEqual(result["briefings"], briefings)
        self.assertEqual(result["errors"], {})


class RailwayCronCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_shot_cron_closes_clients_before_process_exit(self):
        redis_client = MagicMock()
        with (
            patch.object(bot, "_get_redis", return_value=redis_client),
            patch.object(bot, "run_pwa_notification_cron", new=AsyncMock()),
            patch.object(web_app, "run_web_push_recovery_once", new=AsyncMock(return_value={"errors": {}})),
            patch.object(postgres_storage, "close_pool") as close_pool,
        ):
            await start._run_pwa_cron()

        redis_client.close.assert_called_once_with()
        close_pool.assert_called_once_with()


class RailwayCronProcessExitTests(unittest.TestCase):
    def test_cron_mode_forces_process_exit_after_async_work(self):
        def run_and_close(coroutine):
            coroutine.close()

        with (
            patch.dict(os.environ, {"HIRA_SERVICE_MODE": "pwa_cron"}),
            patch("asyncio.run", side_effect=run_and_close) as run,
            patch.object(start, "_exit_cron_process") as process_exit,
        ):
            start.main()

        run.assert_called_once()
        process_exit.assert_called_once_with(0)


class RailwayWebSleepTests(unittest.TestCase):
    def test_idle_redis_connections_are_closed_without_touching_active_ones(self):
        bot_client = MagicMock()
        storage_client = MagicMock()
        with (
            patch.object(bot, "_redis", bot_client),
            patch.object(bot.gs, "_redis_client", storage_client),
        ):
            web_app._disconnect_idle_redis_connections()

        bot_client.connection_pool.disconnect.assert_called_once_with(inuse_connections=False)
        storage_client.connection_pool.disconnect.assert_called_once_with(inuse_connections=False)


if __name__ == "__main__":
    unittest.main()
