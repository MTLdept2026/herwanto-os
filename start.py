from __future__ import annotations

import os
import sys


async def _run_pwa_cron() -> None:
    import bot

    if bot._get_redis() is None:
        bot.logger.error("Redis is required for the H.I.R.A PWA cron worker to deduplicate scheduled jobs.")
        raise SystemExit(1)
    await bot.run_pwa_notification_cron()

    import web_app

    await web_app.run_web_push_recovery_once()


def main() -> None:
    mode = os.environ.get("HIRA_SERVICE_MODE", "bot").strip().lower()
    if mode in {"pwa", "web", "web_app"}:
        import bot

        if not bot.require_redis_for_service("H.I.R.A PWA web service"):
            raise SystemExit(1)
        port = os.environ.get("PORT", "8000")
        os.execvp(
            "uvicorn",
            [
                "uvicorn",
                "web_app:app",
                "--host",
                "0.0.0.0",
                "--port",
                port,
                "--workers",
                "1",
                "--limit-concurrency",
                os.environ.get("HIRA_UVICORN_LIMIT_CONCURRENCY", "40"),
                "--timeout-keep-alive",
                os.environ.get("HIRA_UVICORN_KEEP_ALIVE", "5"),
            ],
        )
    if mode in {"pwa_worker", "worker", "notifications"}:
        import asyncio
        import bot

        if not bot.require_redis_for_service("H.I.R.A PWA worker"):
            raise SystemExit(1)
        asyncio.run(bot.run_pwa_notification_worker())
        return
    if mode in {"pwa_cron", "cron", "notifications_cron"}:
        import asyncio

        asyncio.run(_run_pwa_cron())
        return
    os.execvp(sys.executable, [sys.executable, "bot.py"])


if __name__ == "__main__":
    main()
