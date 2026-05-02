from __future__ import annotations

import os
import sys


def main() -> None:
    mode = os.environ.get("HIRA_SERVICE_MODE", "bot").strip().lower()
    if mode in {"pwa", "web", "web_app"}:
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

        asyncio.run(bot.run_pwa_notification_worker())
        return
    os.execvp(sys.executable, [sys.executable, "bot.py"])


if __name__ == "__main__":
    main()
