# Security Policy

Herwanto OS connects to high-trust personal services: Telegram, OpenAI,
Google APIs, Gmail, Google Drive, Redis, Postgres, Dropbox, and browser push
notifications. Treat configuration and logs as sensitive.

## Supported Versions

Security fixes are accepted against the `main` branch.

## Reporting a Vulnerability

Please do not open a public issue for exploitable vulnerabilities or leaked
credentials. Email the maintainer, or use GitHub's private vulnerability
reporting if it is enabled for the repository.

Include:

- A short description of the issue.
- Steps to reproduce or the affected code path.
- Whether credentials, private files, calendar data, email data, or student/user
  data could be exposed.
- Suggested mitigation, if you have one.

## Secret Handling

- Never commit `.env`, service-account JSON, OAuth refresh tokens, VAPID private
  keys, API keys, database URLs, production logs, uploaded files, or exported
  personal documents.
- Rotate any credential that may have been exposed, even if the exposure was
  brief.
- Keep production access restricted with `HIRA_ALLOWED_USER_IDS` and
  `HIRA_WEB_TOKEN`.
