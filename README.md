# Herwanto OS — Personal AI Assistant

Your AI second brain on Telegram. Calendar-aware, project-tracking, daily briefings, and a steady assistant personality named Hira.

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
| Proactive timed nudges | `/nudge`, `/nudges`, `/cancelnudge` |
| Daily check-ins until affirmed | `/checkin`, `/checkin ... \| breaks \| ...`, `/checkins`, `/cancelcheckin` |
| Full assistant agenda | `/agenda` or `/agenda 14` |
| Store assistant memory | `/remember Category \| Fact` or say "remember..." |
| View / clear memory | `/memory`, `/forget all` |
| Screenshot/PDF schedule extraction | Send a photo, screenshot, image document, or PDF |
| Heavy document analysis | Send searchable PDF, DOCX, or PPTX files |
| Create DOCX / Google Docs | `/doc`, or ask naturally |
| Create PPTX / Google Slides | `/slides`, or ask naturally |
| Remember artifact templates | `/template`, `/templates`, `/artifacts` |
| Voice notes | Send a Telegram voice note |
| Smart task brief | `/tasks`, `/taskmeta`, `/donetask` |
| Follow-up tracker | `/followup`, `/followups`, `/donefollowup` |
| File memory | `/files` |
| Prep briefings | `/evening`, `/weekly` |
| Gmail brief/drafts | `/gmail`, `/gmaildraft` |
| All project statuses | `/projects` |
| Update a project | `/update Project \| Status \| Milestone \| Date \| Notes` |
| Latest news shortlist | `/news`, `/news Apple AI`, `/watch`, `/watchlist`, `/unwatch` |
| Morning briefing now | `/briefing` |
| AI chat (any topic) | Just type naturally |
| Private PWA interface | `uvicorn web_app:app --reload` |
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
6. Search **"Google Drive API"** → Enable
7. Optional: search **"Gmail API"** → Enable if you are setting up Gmail brief/draft support

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
   - Optional for editable generated Google Docs/Slides links: `GOOGLE_ARTIFACT_SHARE_EMAIL`
   - Optional for voice notes: `OPENAI_API_KEY`
   - Optional for Gmail: `GOOGLE_GMAIL_USER`
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

**Proactive nudges:**
```
/nudge Check on GamePlan proposal | 2026-05-02 16:30
/nudge Ping me tomorrow 7am to check my duty roster
/nudges
/cancelnudge 3
```

Hira checks pending nudges every minute and sends the message proactively in Telegram. Use this for time-specific heads-ups when you are likely to be buried in work.

**Daily check-ins:**
```
/checkin Istigfar & Salawat | 09:00, 13:00, 21:30 | Have you done your istigfar and salawat today?
/checkin Selawat & Istighfar | breaks | Have you done your selawat and istighfar today?
/checkins
/cancelcheckin 1
```

Hira checks every minute. For each daily check-in, it pings at the configured times until you reply `yes`, `done`, or `alhamdulillah`. Once you reply affirmatively, it stays quiet for that check-in until the next day.

Use `breaks` instead of fixed times when the reminder should adapt to the day. Hira will read today's timetable and Google Calendar, find free windows, and place the check-ins during suitable breaks.
Adding a check-in with the same name updates the existing one, so you can switch `Istighfar & Selawat` from fixed times to break-aware reminders without creating duplicates.

**Documents, slides, and templates:**
```
/doc Peribahasa Sec 3 Worksheet | 20-minute BM worksheet with instructions, practice items, and answer key
/slides GamePlan Pitch | 8-slide pitch deck for a Singapore school leader
/template NBSS BM Worksheet | Title, learning objectives, short practice, answer key, DBP Bahasa Melayu
/templates
/artifacts
```

Hira creates downloadable `.docx` and `.pptx` files in Telegram. If Google Drive is connected and the Drive API is enabled, Hira also uploads and converts them into Google Docs or Google Slides links. Set `GOOGLE_ARTIFACT_SHARE_EMAIL` to your Gmail/Workspace email if you want those generated links shared back to your account automatically. Reusable template memories are applied to future generated worksheets, decks, lesson plans, proposals, and briefing materials.

**Pro assistant workflows:**
```
/tasks
/taskmeta 12 | high | quick | Draft the first paragraph
/donetask CCA attendance
/followup Faizal | Jerseys quote | 2026-05-05 | WhatsApp | Ask for final price
/followups
/donefollowup jerseys
/files
/evening
/weekly
```

For marking, just say it naturally: "add 1 stack of kefahaman 2G3 to marking task", "I collected kefahaman 2G3 today, 34 scripts", "I've marked 12 scripts for kefahaman 2G3", or "what marking is outstanding?"

Hira can now prioritise reminders with priority/effort/next-action metadata, track marking stacks and follow-ups, send evening prep and weekly planning briefings, and search remembered file/artifact summaries.

**Voice notes:**
```
Send a Telegram voice note.
```

Voice notes require `OPENAI_API_KEY`. Hira transcribes the note and then treats it like a normal message, so it can create reminders, events, documents, follow-ups, or drafts from speech.

**Gmail:**
```
/gmail
/gmail work
/gmail is:unread newer_than:7d
/gmail work is:unread newer_than:7d
/gmaildraft recipient@example.com | Subject | Email body
/gmaildraft work recipient@example.com | Subject | Email body
```

Gmail support is optional. It requires the Gmail API and delegated access for `GOOGLE_GMAIL_USER`; for ordinary Gmail accounts this is not as simple as Calendar/Sheets service-account sharing. If Gmail is not configured, the commands fail gracefully.

For Gmail OAuth:

1. Google Cloud Console → APIs & Services → Library → enable **Gmail API**.
2. APIs & Services → OAuth consent screen → set up an external/testing app.
3. Add your Gmail address as a test user.
4. APIs & Services → Credentials → Create credentials → OAuth client ID.
5. Choose **Desktop app**.
6. Copy the client ID and client secret.
7. Set these locally:
```bash
export GOOGLE_GMAIL_CLIENT_ID="..."
export GOOGLE_GMAIL_CLIENT_SECRET="..."
```
8. Run:
```bash
python3 -m pip install -r requirements.txt
python3 scripts/get_gmail_refresh_token.py
```
9. Sign in with your Gmail and approve read/compose access.
10. Add these Railway variables for your personal inbox:
```env
GOOGLE_GMAIL_CLIENT_ID=...
GOOGLE_GMAIL_CLIENT_SECRET=...
GOOGLE_GMAIL_REFRESH_TOKEN=...
```
11. For a second work/MOE Gmail inbox, run the token script again and sign in with the work account. Add the work token separately:
```env
GOOGLE_WORK_GMAIL_REFRESH_TOKEN=...
```
If you are using the same OAuth app, you do not need separate work client ID/secret. Hira will reuse `GOOGLE_GMAIL_CLIENT_ID` and `GOOGLE_GMAIL_CLIENT_SECRET`. If you create a separate OAuth app for work, set:
```env
GOOGLE_WORK_GMAIL_CLIENT_ID=...
GOOGLE_WORK_GMAIL_CLIENT_SECRET=...
```
12. Redeploy Railway and test:
```text
/gmail is:unread newer_than:7d
/gmail work is:unread newer_than:7d
/gmaildraft someone@example.com | Test from Hira | Hello, this is a draft created by Hira.
```

**Use it like a full assistant:**
```
/agenda
/agenda 14
/remember preferences | Keep replies concise unless I ask for detail
/memory
What should I focus on this week?
Remember that my usual CCA training is on Tuesdays and Thursdays.
GamePlan is now in pilot mode with first-school onboarding as the next milestone.
```

**Hira PWA:**

The PWA is a Telegram-free interface for Hira. It can be installed from Chrome/Edge/Safari-compatible browsers on Android and macOS.

Local run:
```bash
python3 -m pip install -r requirements.txt
uvicorn web_app:app --reload
```

Open:
```text
http://127.0.0.1:8000
```

For Railway, create a second service from this repo and use:
```bash
uvicorn web_app:app --host 0.0.0.0 --port $PORT
```

Copy the same environment variables as the Telegram bot service. Set `HIRA_WEB_TOKEN` to a private phrase if you want the PWA API protected; the app will ask for it on first use.

Current PWA surfaces:
- Chat with Hira
- Agenda
- Tasks
- Personal/work Gmail fetch
- Personal/work Gmail draft creation
- PDF/DOCX/PPTX/image upload analysis
- Voice-note upload/transcription when `OPENAI_API_KEY` is configured
- Marking-load dashboard with marked/unmarked segmented bars
- Light/dark/auto theme switcher

The PWA chat uses the same Hira tool brain as Telegram. With the same production env vars, it can create/delete calendar events, add/complete reminders and follow-ups, manage marking progress, read Gmail, create drafts, generate DOCX/PPTX artifacts, process uploaded documents/images/voice notes, remember context, use timetable context, and fetch news when search is configured.

**Personality:**

Hira is designed to feel like a calm chief-of-staff in your pocket: concise, observant, Singapore-aware, wickedly witty when appropriate, and protective of your attention. It should prioritise next actions over long explanations, steady things when workload piles up, and adapt naturally across teaching, coding, business, normal conversation, and the latest news you care about.

**News shortlist:**
```
/news
/news Liverpool
/watch Apple AI | Apple artificial intelligence
/watchlist
/unwatch Apple AI
What's the latest from my shortlist?
Anything interesting in AI or SG education today?
```

**Screenshots and PDFs:**
```
Send a timetable screenshot, duty roster, match fixture, PDF letter, or event notice.
Hira will extract dated schedule items, add clear events to Google Calendar,
add dated tasks as reminders, and ask only for missing details when needed.
```

**Heavy documents:**
```
Send large searchable PDFs, Word documents, or PowerPoint decks.
Hira extracts text locally first, ranks the most relevant pages/slides/sections,
then analyses only those excerpts so large school files do not overload the model.
For scanned/image-only PDFs, send an OCR/searchable version or the relevant page screenshots.
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
- **Persistent AI memory** → Assistant memory is stored in the `Config` tab; Redis still improves chat history persistence

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
