# Herwanto OS — Personal AI Assistant

Your AI second brain on Telegram. Calendar-aware, project-tracking, daily briefings.

---

## What it does

| Feature | Command / Trigger |
|---|---|
| Today's schedule | `/today` |
| Tomorrow's schedule | `/tomorrow` |
| This week | `/week` |
| All reminders & deadlines | `/due` |
| Add a reminder | `/remind Description \| Date \| Category` |
| Mark reminder done | `/done <id>` |
| All project statuses | `/projects` |
| Update a project | `/update Project \| Status \| Milestone \| Date \| Notes` |
| Morning briefing now | `/briefing` |
| AI chat (any topic) | Just type naturally |
| Auto morning briefing | 7:00 AM SGT daily |
| Project check-in | Friday 5:00 PM SGT |

---

## Setup

### Step 1 — Telegram bot (5 min)

1. Open Telegram → search `@BotFather`
2. `/newbot` → name it `Herwanto OS`
3. Copy the **bot token**

---

### Step 2 — Anthropic API key (5 min)

1. `https://console.anthropic.com` → API Keys → Create Key
2. Copy it

---

### Step 3 — Google Cloud setup (20 min)

This is the most involved step. Follow exactly.

**3a. Create project & enable APIs**
1. Go to `https://console.cloud.google.com`
2. Top left → click project dropdown → **New Project**
3. Name it `herwanto-os`, click **Create**
4. In the search bar, search **"Google Calendar API"** → Enable
5. Search **"Google Sheets API"** → Enable

**3b. Create Service Account**
1. Left menu → **IAM & Admin** → **Service Accounts**
2. Click **+ Create Service Account**
3. Name: `herwanto-bot` → click **Create and Continue** → **Done**
4. Click on the service account you just created
5. Tab: **Keys** → **Add Key** → **Create new key** → JSON → **Create**
6. A JSON file downloads — keep it safe

**3c. Base64 encode the key**

On Mac:
```bash
base64 -i ~/Downloads/herwanto-bot-xxxx.json | tr -d '\n'
```
On Linux:
```bash
base64 -w 0 ~/Downloads/herwanto-bot-xxxx.json
```

Copy the entire output — this is your `GOOGLE_SERVICE_ACCOUNT_JSON` value.

---

### Step 4 — Google Sheet setup (10 min)

**4a. Create the sheet**
1. Go to `https://sheets.google.com` → Blank spreadsheet
2. Rename it `Herwanto OS`
3. Create these 4 tabs (click + at bottom):

**Tab: Reminders**
Row 1 headers exactly: `id | description | due_date | category | done | created`

**Tab: Projects**
Row 1 headers exactly: `project | status | last_update | next_milestone | milestone_date | notes`

Add your current projects in rows 2+:
```
Rūḥ | App Store review | 2026-04-26 | Address rejection | 2026-05-01 | Capacitor iOS app
GamePlan | Active development | 2026-04-26 | Land first school client | 2026-05-15 | Sports CCA websites
```

**Tab: Config**
Row 1 headers: `key | value`
(Leave rows empty — bot fills these in)

**Tab: Sheet1**
(Leave the default sheet — just ignore it)

**4b. Share sheet with service account**
1. Click **Share** (top right)
2. In the email field, paste the service account email (looks like `herwanto-bot@herwanto-os-xxxxx.iam.gserviceaccount.com` — find it in your downloaded JSON under `"client_email"`)
3. Set permission to **Editor**
4. Uncheck "Notify people" → **Share**

**4c. Get your Sheet ID**
From the URL: `https://docs.google.com/spreadsheets/d/`**THIS_PART**`/edit`
Copy the bold part — this is your `GOOGLE_SHEET_ID`

---

### Step 5 — Share your Google Calendar (5 min)

1. Go to `https://calendar.google.com`
2. Find your main calendar on the left → three dots → **Settings and sharing**
3. Scroll to **Share with specific people** → **+ Add people**
4. Paste the service account email (same one as above)
5. Set permission to **See all event details** → **Send**

---

### Step 6 — Push to GitHub (10 min)

> ⚠️ **Security: never commit secrets.** This repo includes a `.gitignore` that
> excludes `*.json`, `.env`, the `files/` folder, and other private material.
> Before running `git add .` for the first time, sanity-check with
> `git status` — if you see anything like `herwanto-bot-xxxx.json`,
> `GOOGLE_SERVICE_ACCOUNT_JSON.txt`, or `.env`, **do not commit**. All secrets
> belong in Railway environment variables, not in git. If you ever do leak a
> service-account key, rotate it immediately in Google Cloud Console (the
> leaked one can be used by anyone).

```bash
git init
# Use the provided .gitignore — do not delete it.
git add .
git status   # confirm no JSON keys / .env files are listed
git commit -m "Herwanto OS v1"
# Create a new GitHub repo, then:
git remote add origin https://github.com/YOUR_USERNAME/herwanto-os.git
git push -u origin main
```

---

### Step 7 — Deploy to Railway (15 min)

1. `https://railway.app` → sign up with GitHub
2. **New Project** → **Deploy from GitHub repo** → select `herwanto-os`
3. Go to **Variables** tab → add all 4 required env vars:
   - `TELEGRAM_BOT_TOKEN`
   - `ANTHROPIC_API_KEY`
   - `GOOGLE_SERVICE_ACCOUNT_JSON`
   - `GOOGLE_SHEET_ID`
4. Click **Deploy**
5. Watch logs — you should see: `Herwanto OS running — scheduler active.`

---

### Step 8 — First message (2 min)

Open Telegram → find your bot → `/start`

The bot stores your chat ID on first `/start`. This is needed for the scheduled morning briefings. **You must send /start at least once after deployment.**

---

## Usage examples

**Add a reminder:**
```
/remind Set Sec 2A test | 2026-05-10 | Teaching
/remind Submit GamePlan proposal to NBSS | 2026-05-15 | GamePlan
/remind Submit CCA attendance report | 2026-05-30 | CCA
```

**Update a project:**
```
/update Rūḥ | Resubmitted to App Store | Apple approval | 2026-05-05 | Fixed metadata issues
/update GamePlan | Onboarding first client | Launch website | 2026-05-20 | Demo scheduled
```

**Natural AI chat:**
```
Draft a BM worksheet on peribahasa for Sec 3
Here's my React error: [paste code]
Write a cold email to NBSS admin about GamePlan
What should my next milestone for Rūḥ be?
```

---

## Cost estimate (monthly)

| Service | Cost |
|---|---|
| Railway (hobby plan) | ~$5 USD |
| Claude Sonnet API (~50 msgs/day) | ~$4-6 USD |
| Google APIs | Free |
| **Total** | **~$10-11 USD/month** |

---

## Upgrading later

- **Voice messages** → Telegram voice → Whisper transcription → Claude
- **Document upload** → Send PDFs/worksheets for Claude to read and summarise
- **WhatsApp** → Same backend, swap to Meta WhatsApp Cloud API when you're ready
- **Persistent AI memory** → Replace in-memory `histories` dict with Redis on Railway

---

## File structure

```
herwanto-os/
├── bot.py              # Bot handlers + scheduler
├── google_services.py  # Calendar + Sheets integration
├── requirements.txt
├── railway.toml
├── .env.example
└── README.md
```
