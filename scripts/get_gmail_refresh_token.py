from __future__ import annotations

import os

from google_auth_oauthlib.flow import InstalledAppFlow


SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
]


def main():
    output_name = os.environ.get("GOOGLE_GMAIL_REFRESH_ENV", "GOOGLE_GMAIL_REFRESH_TOKEN").strip()
    work_token_requested = output_name == "GOOGLE_WORK_GMAIL_REFRESH_TOKEN"
    if work_token_requested:
        client_id = (
            os.environ.get("GOOGLE_WORK_GMAIL_CLIENT_ID", "").strip()
            or os.environ.get("GOOGLE_GMAIL_CLIENT_ID", "").strip()
        )
        client_secret = (
            os.environ.get("GOOGLE_WORK_GMAIL_CLIENT_SECRET", "").strip()
            or os.environ.get("GOOGLE_GMAIL_CLIENT_SECRET", "").strip()
        )
    else:
        client_id = os.environ.get("GOOGLE_GMAIL_CLIENT_ID", "").strip()
        client_secret = os.environ.get("GOOGLE_GMAIL_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SystemExit(
            "Set GOOGLE_GMAIL_CLIENT_ID/SECRET first, or GOOGLE_WORK_GMAIL_CLIENT_ID/SECRET for a work token."
        )

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(
        host="localhost",
        port=0,
        access_type="offline",
        prompt="consent",
    )
    print("\nAdd this to Railway:")
    print(f"{output_name}={creds.refresh_token}")


if __name__ == "__main__":
    main()
