# Contributing

Thanks for taking an interest in Herwanto OS. This project is a personal AI
assistant stack, so useful contributions are usually small, practical, and
focused on reliability, privacy, deployment, or assistant workflow quality.

## Before You Start

- Open an issue for behavior changes, new integrations, or anything that
  changes data storage, permissions, scheduled jobs, or model/tool behavior.
- Keep changes narrow. Prefer a targeted fix with a clear test over a broad
  refactor.
- Do not include private data, production logs, OAuth tokens, service-account
  JSON, personal calendars, exported documents, or real student/user data.

## Local Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
```

Fill only the environment variables needed for the feature you are testing.
For local Telegram testing, set `HIRA_TELEGRAM_OPEN_DEV_MODE=1`; production
deployments should use `HIRA_ALLOWED_USER_IDS`.

## Checks

Run the local reliability sweep before opening a pull request:

```bash
python3 scripts/dev_check.py
```

For web/PWA changes, also run the app locally and smoke-check it:

```bash
uvicorn web_app:app --reload
python3 scripts/dev_check.py --smoke-url http://127.0.0.1:8000
```

## Pull Requests

Please include:

- What changed and why.
- How you verified it.
- Any environment variables, migrations, or deployment notes.
- Screenshots for visible PWA changes.

Security fixes are welcome, but please report exploitable issues privately
using the process in `SECURITY.md`.
