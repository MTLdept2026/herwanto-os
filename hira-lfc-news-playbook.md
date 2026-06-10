# Hira — LFC / X News Playbook

## Why the old approach returned 0
"Scan X / find trending X posts" fails because X (Twitter) blocks
unauthenticated and automated access and isn't reliably indexed by web
search. The instruction can't be fulfilled, so it falls back to a bare
Sky Sports link. Fix: stop targeting X directly; target where X content
*surfaces*.

## Rule 1 — Never bypass site restrictions
Do not use proxy/mirror/cache services (e.g. r.jina.ai, archive mirrors)
to get around a blocked site. If a page won't open normally, find the
same quote on another legitimate source — football reporters' posts are
republished within minutes, so the content is almost always elsewhere.

## Rule 2 — Default method: aggregator search (no login)
Web-search these, which quote the original X posts verbatim:
- This Is Anfield — thisisanfield.com
- Empire of the Kop — empireofthekop.com
- Anfield Watch — anfieldwatch.co.uk
- TeamTalk (Liverpool) — teamtalk.com/liverpool
- NewsNow Liverpool feed — newsnow.co.uk/h/Sport/Football/Premier+League/Liverpool/Transfer+News
- NewsNow Fabrizio Romano feed — newsnow.co.uk/h/Sport/Football/Pundits/Fabrizio+Romano

Good search queries:
- "Liverpool transfer news rumours <current month/year>"
- "Fabrizio Romano Liverpool latest"
- "David Ornstein Liverpool"
- "<player name> Liverpool transfer"

## Rule 3 — Live X reading (when the real timeline is wanted)
Use the Claude in Chrome extension to open X logged into the user's own
account and read the timeline / a search like "Liverpool" or a specific
journalist. Only works with the extension installed and the user signed in.

## Rule 4 — Label reliability every time
Tag each item:
- **Confirmed** — only if a club or official source states it.
- **Reported (tier-1)** — Romano, Ornstein/The Athletic, club beat reporters.
- **Rumour / gossip** — everything else.
Never present a rumour as confirmed.

## Output format
Group by Incoming / Outgoing / Other, one line each:
`<player> — <claim> — <source + reliability tag>`
Then a one-line "freshest item" at top if something broke today.
