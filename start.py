from __future__ import annotations

import os
import sys


def _exit_cron_process(status: int) -> None:
    """Flush logs and terminate after a completed one-shot cron pass."""
    import logging

    logging.shutdown()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    os._exit(status)


async def _run_pwa_cron() -> None:
    import bot

    redis_client = bot._get_redis()
    if redis_client is None:
        bot.logger.error("Redis is required for the H.I.R.A PWA cron worker to deduplicate scheduled jobs.")
        raise SystemExit(1)
    try:
        await bot.run_pwa_notification_cron()

        import web_app

        recovery = await web_app.run_web_push_recovery_once()
        bot.logger.info(
            "H.I.R.A PWA push recovery pass complete: errors=%s",
            sorted((recovery.get("errors") or {}).keys()),
        )
    finally:
        try:
            redis_client.close()
        except Exception as exc:
            bot.logger.warning("Could not close Redis client after cron pass: %s", exc)
        import postgres_storage

        postgres_storage.close_pool()


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

        status = 0
        try:
            asyncio.run(_run_pwa_cron())
        except BaseException:
            status = 1
            import logging

            logging.getLogger(__name__).exception("H.I.R.A PWA cron process failed")
        _exit_cron_process(status)
        return
    os.execvp(sys.executable, [sys.executable, "bot.py"])


if __name__ == "__main__":
    main()
