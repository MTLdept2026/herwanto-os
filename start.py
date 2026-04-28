from __future__ import annotations

import os
import sys


def main() -> None:
    mode = os.environ.get("HIRA_SERVICE_MODE", "bot").strip().lower()
    if mode in {"pwa", "web", "web_app"}:
        port = os.environ.get("PORT", "8000")
        os.execvp(
            "uvicorn",
            ["uvicorn", "web_app:app", "--host", "0.0.0.0", "--port", port],
        )
    os.execvp(sys.executable, [sys.executable, "bot.py"])


if __name__ == "__main__":
    main()
