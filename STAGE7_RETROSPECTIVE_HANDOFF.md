# Stage 7 — Retrospective Evidence + Codex Handoff Discipline

> **For Claude review:** This is an implementation plan, not completed code. Please review for overreach, unsafe auto-learning, missing tests, and any places where this duplicates existing HIRA behavior instead of extending it. The intended style is additive, conservative, and easy to revert.
>
> **Review reconciliation note:** Claude's first review correctly flagged the need to clarify boundaries, action-ledger schema mapping, and tests. The specific claim that `"[auto-retro]".startswith("[auto]")` is true is not correct in Python, but the prefix is visually confusing. This revised plan avoids that ambiguity anyway by using `[retro]`.

## Context

HIRA already has the core pieces for learning from use:

- `run_self_audit()` reviews notification outcomes, writes reversible `[auto]` learned preferences, and queues a weekly "What I learned this week" digest.
- `learned_preferences` is already in memory and runtime status.
- `_notification_feedback_bias()` and `_should_suppress_notification()` already down-rank or suppress candidates from feedback.
- `memory_consolidation_job()` already reviews `correction_ledger` and `self_reflections` weekly.
- The action ledger records user-visible state changes and undo/review status.

This stage should not rebuild those systems. It should add a small evidence layer so self-audit can look beyond notification dismissals, and add a lightweight handoff convention for future Codex/Claude work on HIRA.

Boundary with `memory_consolidation_job()`:

- `memory_consolidation_job()` is the LLM-based durable promotion path into `preferences` and `constraints`.
- The Stage 7 retrospective layer is deterministic and lower-fidelity. It writes only a small number of tagged `learned_preferences` hints.
- Retrospective hints should help HIRA notice likely behavior adjustments quickly, but they must not replace consolidation or compete with manual preferences.
- If a similar manual or consolidated preference already exists, the retrospective layer should skip or downgrade that signal.

## Assumptions

- HIRA remains OpenAI-only at runtime; this plan does not introduce Claude as an app dependency.
- Retrospective learning must be evidence-backed, reversible, and transparent.
- Manual memory always wins over auto-generated memory.
- Development handoff docs help future coding agents, but they should not become product UI unless explicitly requested later.
- The existing Friday rhythm stays: self-audit and memory consolidation run weekly.

## Goals

1. Extend self-audit with retrospective-style evidence rules across corrections, reflections, and action outcomes.
2. Keep all auto-learning scoped to tagged `learned_preferences` entries and the existing reversible notification mute layer.
3. Add repository handoff and maintenance templates so future HIRA development sessions start with context instead of rediscovery.
4. Verify with focused tests and `python3 scripts/dev_check.py`.

## Non-Goals

- Do not let HIRA freely rewrite `preferences`, `constraints`, `AGENTS.md`, or stage docs.
- Do not introduce a new agent framework, scheduler, database table, or external service.
- Do not auto-delete user-written memory.
- Do not auto-mute critical notification families such as briefings, prayers, or explicit nudges.
- Do not promote action-ledger patterns into automatic learned preferences in the first pass; action signals are watch-only until observed.
- Do not add UI until the evidence layer has been observed in real use.

## Track A — Retrospective Evidence Rules

### 1. Add Evidence Builders

Add small read-only helpers near the current self-audit code in `bot.py`:

```python
RETROSPECTIVE_PATTERN_CLUSTERS = {
    "source_discipline": {"live", "source", "sources", "latest", "web", "search", "cite", "citation"},
    "date_discipline": {"guess", "weekday", "date", "dates", "today", "tomorrow", "assume"},
    "entity_precision": {"wrong", "class", "student", "person", "name", "entity", "meant", "room"},
    "verbosity_tone": {"verbose", "verbosity", "short", "concise", "direct", "preamble", "long", "brief"},
    "write_safety": {"confirm", "clarify", "delete", "draft", "recipient", "event", "calendar", "reminder"},
}

def _retrospective_entry_clusters(text: str) -> set[str]:
    ...

def _retrospective_correction_text(entry: dict | str) -> str:
    ...

def _retrospective_reflection_text(entry: dict | str) -> str:
    ...

def _retrospective_correction_signals(memory: dict, now: datetime) -> list[dict]:
    ...

def _retrospective_reflection_signals(memory: dict, now: datetime) -> list[dict]:
    ...

def _retrospective_action_signals(action_ledger: list[dict], now: datetime) -> list[dict]:
    ...

def build_retrospective_evidence(now: datetime | None = None, days: int = 14) -> dict:
    ...
```

Keep signals as plain dicts, not classes. Suggested shape:

```python
{
  "kind": "correction|reflection|action",
  "key": "stable-dedupe-key",
  "label": "human-readable pattern",
  "evidence_count": 2,
  "confidence": "watch|strong",
  "evidence": ["short excerpt 1", "short excerpt 2"],
  "recommendation": "proposed learned preference text"
}
```

Matching algorithm:

- Normalize text to lowercase ASCII-ish words with punctuation removed.
- Assign each correction/reflection entry to zero or more predefined `RETROSPECTIVE_PATTERN_CLUSTERS`.
- A cluster is assigned only when an entry contains at least two distinct keywords from that cluster.
- Do not include high-frequency connector words in clusters. In particular, avoid `not`, `instead`, `before`, `ask`, `current`, `create`, and `update`.
- Two entries share a pattern only if they share at least one cluster.
- Ignore entries that match no cluster.
- Keep this deterministic; do not call an LLM for cluster matching.
- Tests should use cluster examples, not arbitrary one-off strings.
- Add a negative test where two entries share only a common stopword such as `not` or `before`; this must produce no signal.

Memory entry text extraction:

- `correction_ledger` entries are usually dicts serialized in memory. Match only `entry["correction"]`.
- Do not match on `assistant_response`, because that clusters on HIRA's own wording rather than the user's correction.
- `self_reflections` entries are usually dicts serialized in memory. Match only `entry["learned"]`.
- If a legacy entry is a plain string, use the string itself as a fallback.
- If a dict cannot be parsed or the target field is empty, skip that entry.

Recency:

- For `correction_ledger` and `self_reflections`, use last-N-entry recency, not date parsing. Start with the last 30 entries, mirroring `memory_consolidation_job()`.
- The `days` parameter applies to action-ledger and notification-outcome sources.
- Do not parse free-form memory `date` strings in this stage. That can be added later if needed.

### 2. Evidence Sources

Use only existing stores:

- `gs.get_memory()` for `correction_ledger`, `self_reflections`, and current `learned_preferences`.
- `gs.get_action_ledger(include_reviewed=True)` for recent state-changing actions.
- Existing `gs.get_notification_outcome_summary(days=14)` stays the source for notification mutes and valued/watching groups.

Each source must fail independently. If action ledger read fails, self-audit should still complete using notifications and memory.

### 3. Rule Set

Start with three conservative rules.

**Repeated Correction Rule**

Fire when two or more recent correction entries share a predefined pattern cluster.

Examples:

- `source_discipline`: repeated "use live sources" corrections
- `entity_precision`: repeated "wrong class/student/entity" corrections
- `date_discipline`: repeated "do not guess dates/weekdays" corrections

Output:

- `watch` for two weak matches
- `strong` only for repeated explicit language such as "always", "never", "do not", "must", or three or more matching corrections
- Strong-trigger scanning must read the same extracted field used for clustering: `entry["correction"]` for corrections and `entry["learned"]` for reflections. Do not scan `assistant_response`, `trigger`, or `next_behavior`.
- Exact token matching intentionally misses variants such as "guessing" vs. "guess" or "cited" vs. "cite". This recall loss is acceptable in Stage 7 because it fails toward silence instead of false learning.

**Repeated Reflection Rule**

Fire when `self_reflections` show HIRA noting the same predefined pattern cluster more than once.

Examples:

- `source_discipline`: weak source discipline
- `entity_precision`: wrong class/person/entity handling
- `verbosity_tone`: too much verbosity
- `write_safety`: failed clarification before a write action

Output:

- usually `watch`
- `strong` only if the reflection repeats and aligns with a user correction

**Undone Action Rule**

Fire when the action ledger shows repeated undone actions in the same category.

Examples:

- repeated undone calendar edits
- repeated undone reminders
- repeated Gmail draft corrections

Schema mapping:

- `undo_status` is the only direct undo signal.
- Treat an action as undone when `undo_status` is non-empty and not in `{"pending", "not_applicable", "unavailable"}`.
- `reviewed: true` means the user has reviewed the ledger item, not that it was positive.
- There is no `reviewed-positive` / `reviewed-negative` field. Do not infer positive sentiment from `reviewed`.

Output:

- `watch` only in this stage
- no `[retro]` learned preference writes from action-ledger signals yet
- skip sources/families in `LEARNED_MUTE_EXEMPT_GROUPS`, including briefings, prayers, and explicit nudges

### 4. Extend `run_self_audit()`

Current behavior:

- reviews notification outcome groups
- writes `learned_muted_families`
- replaces prior `[auto]` learned preferences
- queues weekly digest

New behavior:

- call `build_retrospective_evidence(now=current, days=14)`
- include `retrospective.watch` and `retrospective.strong` in the returned result
- add `[retro]` learned preferences only for strong correction/reflection evidence
- preserve all manual preferences and all existing non-retro `[auto]` entries unless owned by the same refresh pass
- add a short "Patterns I noticed" section to the weekly digest

Important: notification mute behavior remains unchanged. Retrospective evidence should not mute notification families directly unless it flows through the existing notification outcome logic.

### 5. Auto-Learning Guardrails

Use a separate prefix:

```python
RETROSPECTIVE_AUTO_PREFIX = "[retro]"
```

When refreshing retrospective preferences:

- remove only prior entries that start with `[retro]`
- keep manual entries
- keep current `[auto] Easing off ...` entries from notification self-audit
- cap retrospective entries at 3
- require short, human-readable text
- before writing, skip a recommendation if a similar manual/consolidated preference already exists in `preferences`, `constraints`, or non-retro `learned_preferences`

Similarity algorithm:

- Normalize the recommendation and existing entry to lowercase word tokens.
- Generate all four-word windows from the recommendation.
- Treat an existing entry as similar if it contains any exact four-word window from the recommendation.
- Ignore windows made only of generic words such as `before answering similar questions`.
- The generic-window exclusion is implementer's discretion, bounded by the cap of 3 retrospective entries. Do not add a broad stopword corpus in this stage.
- This is intentionally conservative and easy to test. It may allow near-duplicates, but it should avoid suppressing every retro hint after consolidation.

Example learned preference:

```text
[retro] Before answering current/news-like questions, use live/source-backed tools when available - repeated corrections/reflections showed source discipline matters (2026-06-01).
```

Implementation note:

- Existing `[auto]` notification self-audit replacement can remain as-is, because `[retro]` does not share the `[auto]` prefix.
- Still add a two-run regression test to prove retrospective entries survive repeated `run_self_audit()` calls.

### 6. Runtime Status

Add a small status surface to `build_runtime_status()`:

```python
"retrospective": {
  "auto_count": ...,
  "recent": [...],
  "last_week": ...
}
```

Keep it cheap. Do not run fresh LLM calls inside runtime status.

Implementation detail:

- Reuse the memory dict already loaded inside `build_runtime_status()`; do not call `gs.get_memory()` a second time.
- If action-ledger status is ever added, read it only inside the existing Google-connected guard and tolerate failure.

## Track B — Codex Handoff / Maintenance Habits

### 1. Add Documentation Templates

Add three docs:

- `docs/HANDOFF_TEMPLATE.md`
- `docs/RETROSPECTIVE_TEMPLATE.md`
- `docs/MAINTENANCE.md`

These are repo maintenance artifacts, not app features.

### 2. Handoff Template Contents

The handoff template should be short and repeatable:

```markdown
# HIRA Handoff

Date:
Branch/commit:
Goal:

## What Changed

## Files Touched

## Verification

## Known Risks

## Next Safest Step

## Notes For Next Agent
```

### 3. Retrospective Template Contents

The retrospective template should support evidence-backed process improvements:

```markdown
# HIRA Development Retrospective

Date:
Workstream:

## What Went Well

## What Went Wrong

## Repeated Pattern Observed

## Evidence

## Proposed Rule Or Habit

## Should This Change AGENTS.md?

## Follow-Up
```

Rule: changing `AGENTS.md` or durable project instructions should require repeated evidence, not a single awkward session.

### 4. Maintenance Doc Contents

`docs/MAINTENANCE.md` should explain:

- when to create a handoff
- where to store handoffs
- when to run `python3 scripts/dev_check.py`
- how to treat stage docs
- how to avoid broad refactors
- how to document known risks

Suggested convention:

- Major feature or multi-file change: create `docs/handoffs/YYYY-MM-DD-short-topic.md`
- Small one-file fix: no handoff required; final response is enough
- Architecture decision: update or add a stage doc
- Never use handoff docs as a substitute for tests

## Implementation Sequence

### Commit 1 — Plan Docs

Add:

- `STAGE7_RETROSPECTIVE_HANDOFF.md`
- `docs/HANDOFF_TEMPLATE.md`
- `docs/RETROSPECTIVE_TEMPLATE.md`
- `docs/MAINTENANCE.md`

Verify:

- Markdown renders clearly.
- No runtime behavior changed.

### Commit 2 — Retrospective Evidence Builders

Add read-only helpers:

- `_retrospective_correction_signals`
- `_retrospective_reflection_signals`
- `_retrospective_action_signals`
- `build_retrospective_evidence`

Verify:

- mocked memory/action-ledger inputs produce expected watch/strong signals
- source read failures degrade cleanly
- no `gs.set_*`, `_queue_app_notification`, or `_add_memory` calls from evidence builders
- `gs.get_action_ledger()` is mocked in tests so unit tests never hit the real config store
- cluster matching is deterministic: shared cluster fires; no shared cluster stays silent
- stopword collision is blocked: entries sharing only `not` or `before` produce no signal
- correction matching reads `correction` only and ignores `assistant_response`
- reflection matching reads `learned` only and ignores `trigger` / `next_behavior`
- correction/reflection recency uses the last 30 entries, not date parsing

### Commit 3 — Extend Self-Audit

Update `run_self_audit()` to:

- call `build_retrospective_evidence`
- write `[retro]` learned preferences for strong correction/reflection patterns
- preserve manual and existing notification `[auto]` learned preferences
- include retrospective watch/strong patterns in digest body

Verify:

- current notification mute tests still pass unchanged
- strong retrospective pattern writes a tagged learned preference
- weak pattern appears in digest but writes nothing
- action-ledger pattern appears as watch-only and writes nothing
- manual memory remains untouched
- two consecutive `run_self_audit()` calls preserve `[retro]` entries unless the underlying evidence disappears
- any existing `test_run_self_audit_scoped_writes`-style test must patch `bot.gs.get_action_ledger` to return `[]`, even when it is not the focus of the assertion

### Commit 4 — Runtime Status

Expose retrospective counts and recent entries in `build_runtime_status()`.

Verify:

- status works when memory is available
- status degrades when memory storage is unavailable
- no slow LLM or network calls added

### Commit 5 — Documentation Polish

Add an example handoff in `docs/handoffs/README.md` or keep the folder absent until first use. Prefer no placeholder churn unless needed.

Verify:

- docs match actual workflow
- no duplicate instructions conflict with `AGENTS.md`

## Tests

Add or extend focused tests:

- `tests/test_self_audit.py`
  - strong repeated correction -> `[retro]` learned preference
  - weak repeated correction -> digest/watch only
  - action ledger undone pattern -> watch signal
  - action ledger source/family in `LEARNED_MUTE_EXEMPT_GROUPS` -> no signal
  - source read failure -> self-audit still completes
  - scoped writes -> only learned preferences, learned mute config, and digest
  - scoped-write tests explicitly patch `bot.gs.get_action_ledger(return_value=[])`
  - `bot.gs.get_action_ledger` raises -> `run_self_audit()` still returns notification results normally
  - two Friday runs -> `[retro]` entries survive while evidence persists
  - existing `[auto]` notification entries and manual preferences are preserved correctly
  - existing notification mute behavior remains intact

Optional new file:

- `tests/test_retrospective_evidence.py`
  - only if `tests/test_self_audit.py` becomes too large
  - cluster matching cases may fit better here if they make self-audit tests noisy
  - stopword-collision and dict-field extraction cases should live here if split out

Gate:

```bash
python3 scripts/dev_check.py
```

## Acceptance Criteria

- HIRA can produce a weekly self-audit that includes notification learning plus retrospective evidence from corrections/reflections/actions.
- Retrospective evidence matching uses explicit keyword clusters, not ad hoc substring choices.
- Cluster assignment requires at least two distinct keywords, so common words cannot create false signals.
- Corrections match only on `correction`; reflections match only on `learned`.
- Correction/reflection recency is defined as the last 30 entries.
- Duplicate suppression uses a deterministic four-word-window overlap rule.
- Strong correction/reflection retrospective patterns create only tagged `[retro]` learned preferences.
- Action-ledger retrospective patterns are watch-only in this stage.
- Weak patterns are visible but do not change behavior.
- Manual memory is never deleted or rewritten.
- Existing `[auto]` notification self-audit entries are preserved or refreshed by their own path only.
- Existing notification suppression and learned mute behavior are unchanged.
- Codex/Claude handoff docs exist and are short enough to actually use.
- `python3 scripts/dev_check.py` passes.

## Risks

- Over-learning from too little evidence.
- Duplicating `memory_consolidation_job()`.
- Creating too many auto-memory entries.
- Adding maintenance docs that future agents ignore.

## Risk Controls

- Strong evidence threshold is intentionally high.
- Retrospective entries are capped at 3.
- Retrospective writes use a unique prefix and are refreshable.
- Memory consolidation remains the only LLM-based weekly promotion path.
- Action-ledger signals are watch-only for the first implementation.
- Handoff docs stay lightweight and are required only for substantial work.

## Claude Review Questions

1. Is the boundary between deterministic retrospective hints and LLM-based `memory_consolidation_job()` clear enough now?
2. Are the evidence thresholds too weak, too strict, or about right?
3. Is making action-ledger retrospective signals watch-only the right first step?
4. Is `[retro]` in `learned_preferences` the right storage strategy, or should retrospective observations live only in `self_reflections`?
5. Are the handoff docs useful enough to justify adding them to the repo?
6. What test would catch the most dangerous failure mode here?
