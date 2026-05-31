# Stage 8 — Exponential Foundations / Measurement Flywheel (Codex handoff)

**Date:** 2026-05-31 · **Status:** APPROVED FOR IMPLEMENTATION · **Owner:** Codex · **Reviewer:** Claude
**Supersedes:** the prior draft of this file and the engine half of `HIRA_EXPONENTIAL_PLAN.md`.
**Reconciliation:** This is the merged verdict of two independent multi-agent analyses (Claude's five deep `file:line` audits + Codex's four strategy agents), after two rounds of argument. **Claude wins the technical roadmap; Codex wins the product destination; six amendments below correct both.** Build the spine that lets HIRA improve itself, then build Daily Closure on top.

---

## Doctrine

> HIRA already has visible intelligence (situation model, anticipation rules, voice, self-audit, memory, runtime status). The next exponential gain is to make it **measurable, cheaper, safer, and self-correcting before adding any new autonomy.** Until "Was HIRA right, useful, restrained, and safe?" is durably measured, new features are guessing.
>
> **Closure is the product. Measurement is the engine. The cache fix is the down payment.**

**Universal discipline — applies to every commit, no exceptions:**
1. **Additive + reversible.** Every behavioral change ships **dark behind an env flag defaulting OFF** (`_env_flag`/`_env_int`, `bot.py:230-233, 465-469`). Reverting any commit restores prior behavior.
2. **Telemetry ≠ memory.** Quality signals, counters, eval results, shadow logs, snapshots, candidate/notification/prediction events are **telemetry** → capped `get_config`/Postgres stores with **byte caps**. `set_memory` flattens every memory bucket to a list of strings (`google_services.py:4844-4846`) — dict-shaped telemetry put in a memory bucket **vanishes on the next write**. Memory buckets hold only human-readable behavioral memory.
3. **Gated.** `python3 scripts/dev_check.py` green after every commit. New jobs register but no-op when their flag is unset.
4. **Fail quiet.** Every new hook is `try/except → logger.debug`; never block the chat path; mirror scheduler hygiene (re-raise `CancelledError`, back off on generic exceptions).
5. **No new proactive families until the spine exists and is boring.**

---

## The six amendments (build these correctly or the spec is wrong)

| # | Rule | Why | Anchor |
|---|---|---|---|
| **A1** | **Cache the static prefix ONLY; render date/memory/specialist tail fresh every turn.** Do NOT coarsen the whole cache key to daily. | The prompt carries live time; a daily-keyed whole-prompt cache **freezes the clock**. OpenAI caches the longest common *prefix*, so a stable static prefix + fresh tail gives the win without freezing time. | `SYSTEM_PROMPT()` return `bot.py:3032`; cache key `:3174`; live-time sole source `date_ctx :2986` |
| **A2** | **Telemetry goes in capped config/Postgres, never memory buckets.** | `set_memory` coercion destroys dicts. | `google_services.py:4844-4846` |
| **A3** | **Outcome v2 = dual-write + explicit metadata pass-through.** Metadata does not reach the funnel automatically; dispatch sites must pass `candidate_id`/`metadata`; keep writing the old blob during transition. | `_record_notification_outcome` is the shared funnel, but it is metadata-blind today. | `bot.py:17499`; dispatch `:8129/:8139` |
| **A4** | **Situation snapshots are Postgres-row-backed**, with only a small capped degraded fallback. | A 220 KB ring buffer is unsafe in a single Sheets cell (≈50 K char limit; outcome blob already capped at 45 KB for this reason). | `postgres_storage.py` row stores; blob cap `google_services.py:3825` |
| **A5** | **Precision first; promise recall only where the universe is enumerable** (`submission_risk`, `prep_gap`, `marking_crunch`). `calendar_conflict`/`stale_data` recall is approximated later, never promised. | You cannot enumerate clashes your own detector missed. | resolvers read classops ledger `google_services.py:2948` |
| **A6** | **Extract `anticipation/` LAST**, only after queue behavior is pinned by tests + shadow/resolution is stable. | Early extraction is architecture theatre with regression risk. Discipline when it happens: `import bot` late-binding + shim, **never `from bot import X`** (breaks `patch.object`). | block `bot.py:6717-8183`; 37 external calls |

---

## Phase 0 — Regression tests FIRST (pin the dangerous seams)

Before changing any behavior, write tests that pin the seams the amendments flagged. These tests must pass against today's code where applicable, and become the guardrail for every later phase. New file(s) under `tests/`, added to `scripts/dev_check.py` `CRITICAL_UNIT_TESTS`.

- **Prompt dynamic-tail freshness** — assert the rendered system prompt's time/memory tail changes when `now`/memory changes, AND that the static prefix is byte-identical across two different `now` values (the cacheable-prefix invariant). Guards A1.
- **Structured-memory rendering/coercion** — assert readers render both bare-string and `{value,...}` dict entries; assert `postgres._coerce_items` preserves dicts. Guards the C-phase migration + A2.
- **Shadow-logging placement** — assert a shadowed family is logged only when it would have passed the full restraint chain, and is never dispatched. Guards Phase 4.
- **Outcome v2 dual-write parity** — assert a write lands in both old blob and new store, and `candidate_id`/`metadata` round-trips when dispatch passes it. Guards A3.
- **Eval job never reruns agentic chat** — assert `quality_eval_job` calls the judge on stored answers and never invokes `_run_agentic_chat`. Guards Phase 2 cost/reproducibility.

**Acceptance:** all five test groups exist, green, in `dev_check`. No behavior changed yet.

---

## Phase 1 — Tier-0 Measurement Primer (highest return; ~zero new LLM cost)

Reclaim signals HIRA already computes and discards, and split the prompt cache. Flag `HIRA_QUALITY_SIGNALS` (default off) except P1.4 which is behavior-neutral.

- **P1.1 Persist the self-repair verdict.** `_repair_meta` is discarded at `bot.py:16731`. New `quality_signals` **telemetry store** (Postgres row-backed when available, capped config fallback otherwise); record only flagged/repaired turns (cap 200, keep signal-dense). Do **not** add this to `DEFAULT_MEMORY` or prompt-rendered memory categories. Zero added LLM cost.
- **P1.2 Count guardrail fires.** `_source_contract_guardrail:12907`, `_backend_claim_guardrail:12932`, `_cca_sheet_user_burden_guardrail:12965`, `_correct_weekday_date_mismatches:13127` — capture "fired iff output≠input" at the emit seams (`bot.py:15104-15111, 15734-15740, 15054, 15637`) with one-line `before=/if changed` pairs; **do not mutate the guards.** Counters are dict-shaped → **`get_config` key `quality_guardrail_counters`** (A2), retain 8 ISO weeks.
- **P1.3 Surface the trend.** Read-only `_quality_trend_summary()` → a `quality` block in `build_runtime_status` (`bot.py:603`) + a line in `_self_audit_digest_body` (`bot.py:17384`). No LLM.
- **P1.4 Split the prompt cache (A1).** In `SYSTEM_PROMPT()` (`bot.py:3032`): emit `STATIC_PREFIX` (the ~190-line / ~8,120-token static block, verified >> 1,024 min) **first**, then a stable delimiter, then a **freshly rendered** `{date_ctx}{memory_ctx}` tail (and the specialist suffix lands in the tail). The static prefix may be day-cached in-process; the tail is rendered every call so time never freezes. **Behavior-neutral; ship live after verifying.** **Verify:** the `cached=` field (`bot.py:3833`) jumps from ≈0 to a 0.5–0.8 hit ratio; cached input bills 10× cheaper (`_OPENAI_PRICING:3618`); confirmed re-sent every turn incl. stateful (`bot.py:15034`).

**Acceptance:** quality trend visible in runtime status + digest; `cached=` ratio measurably up; no answer-path behavior change; Phase-0 tests green.

---

## Phase 2 — Eval Harness (the ruler)

Flag `HIRA_QUALITY_EVAL` (default off). Depends on Phase 1's `quality_signals`.

- **P2.1 Corpus auto-builder.** `build_quality_eval_corpus` reads `correction_ledger` (stores the bad-answer text `assistant_response` + `correction`, `google_services.py:4910`), dedups via `_retrospective_clustered_texts:17201` (≥2 threshold), emits cases `{id, cluster, context, known_bad_answer, what_was_wrong, ideal_behavior, priority}`. **Store as a `get_config` JSON blob** key `quality_eval_corpus` (A2), cap 60, monotonic union-by-id.
- **P2.2 Weekly judge job.** `quality_eval_job` clones `memory_consolidation_job:18477` (lock, Fri 23:00 after consolidation, `_llm_text`+`_json_from_llm_text`, `_finish_background_job`). **Judges the STORED answer — never re-runs `_run_agentic_chat`** (cost + non-reproducibility + side-effects). Rubric = **5 dimensions 1-5** (correctness, source_discipline, restraint, tone, repeated_mistake), `any(≤2)→fail`, temp 0, **anonymized** (people→tokens, emails/dates/phones→tokens). **Cost = 1 `_llm_text`/week**, batch ≤20.
- **P2.3 Regression guards.** `tests/test_hira_regressions.py` Tier A deterministic (assert existing guards on documented corrections — weekday fix, unsourced-result block, invented-backend block, generic-vent flag); Tier B `@skipUnless(HIRA_EVAL_LIVE)`. Add `--eval` to `dev_check.py` **warn-only first**; promote Tier A into `CRITICAL_UNIT_TESTS` after one green week.

**Acceptance:** corpus builds from real corrections; weekly judge produces a fail count + summary into the digest; deterministic regressions green; `--eval` warn-only wired.

---

## Phase 3 — Memory Hardening (stop silent-forget + bloat)

8 commits, each flag-gated default-OFF, **readers-first** so structured entries never render as raw JSON. **No structured writes until dict handling + rendering are proven safe (P0 + C1).**

- **C1 readers (no flag):** `_memory_item_text:1573` recognizes `"value"`; `postgres._coerce_items:252` **preserves dicts** (the highest-risk silent corruptor); route `_format_memory:5330` + SYSTEM_PROMPT render `:3004` through `_memory_entry_value`.
- **C2 caps + cap-evict log:** `MEMORY_STORAGE_LIMITS` (profile 150 / people 150 / preferences 120 / teaching 100 / constraints 120 / …) enforced in `gs.add_memory:4866` + `postgres.add_memory:305` (new `cap` param); write `memory_log source='cap_evict'`. Flag `HIRA_MEMORY_CAPS`.
- **C3 soft-delete schema:** `ALTER TABLE memory_log ADD COLUMN deleted_at/restored_at` (it is append-only today); `soft_delete_memory` + `restore_memory` + `/restore <id>`.
- **C4 near-dup dedup:** generalize `_retrospective_four_word_windows:17115` + `_strip_provenance_prefix` (kills `[consolidated DATE]` dup growth at `:18536`). Flag `HIRA_MEMORY_NEARDUP_DEDUP`.
- **C5 prune wiring:** consolidation prompt returns explicit `prune:[{category,match,reason}]`; conservative loop — allow-list `{correction_ledger, self_reflections}` only, ≤5/run & ≤20% window, ≥12-char match, every removal a reversible `memory_log` soft-delete; surface in digest. Flag `HIRA_MEMORY_PRUNE_ENABLED`.
- **C6 structured writes + trust ladder:** `_make_memory_entry{value,source,fetched_at,confidence}`; encode the **8-tier trust ladder** for conflict resolution (manual > consolidated > `[auto]` > `[retro]` > source_notes > episodes > vector > volatile) + **contradiction detection** (a new entry that negates a higher-tier fact is flagged, not silently appended). Flag `HIRA_MEMORY_STRUCTURED_WRITE`.
- **C7 recall window:** drop the `[-200:]` slice (`bot.py:2316`) for bounded durable categories only (caps from C2 are the precondition); **do NOT touch the tuned scoring weights/threshold** (2026-05-12 regression history at `:2287`). Flag `HIRA_MEMORY_RECALL_FULL_SCAN`.
- **C8 semantic recall:** per-**category** vector upload (not one blob `:5461`), hash-gated, orphan reconciliation via `:5446`; replace literal-keyword `file_search` gate (`:446`) with "attach when lexical recall under-delivered." **Vector/file search is retrieval infrastructure, not a source of truth.** Flags `HIRA_OPENAI_VECTOR_SYNC_ENABLED`, `HIRA_OPENAI_FILE_SEARCH_AUTO`.

**Acceptance:** bare + structured entries both render correctly; categories bounded; prune reversible via `/restore`; no recall regression vs the 2026-05-12 baseline.

---

## Phase 4 — Anticipation Shadow + Resolvers (safe truth, no new families)

Flags per commit, default off. **No new proactive rules** — only measurement of existing ones.

- **D1 shadow gate:** `HIRA_SHADOW_FAMILIES` env set; in `_dispatch_proactive_candidates:8106`, **after the full restraint chain, before any side effect**, `gs.add_shadow_log(candidate); continue`. Capped `proactive_shadow_log` store (telemetry → config/Postgres). Logs only what *would* have fired.
- **D2 situation snapshots (A4):** `build_situation_snapshot` = `build_situation_model:6470` + raw resolver inputs (classops ledger counts/ids, marking tasks, calendar events); throttled ~2-3/day inside `calendar_reminders_job:18380`; **Postgres-row-backed**, 90-day retention, small capped degraded fallback only. Flag `HIRA_SITUATION_SNAPSHOTS`.
- **D3 resolvers:** build `submission_risk`, `prep_gap`, `calendar_conflict` resolvers keyed on candidate `source`/`metadata`; write `predicted_correct`. **Measure recall only on `submission_risk` + `prep_gap`** (+ `marking_crunch` when added); `calendar_conflict` is **precision-only** (A5). Flag `HIRA_RESOLUTION_ENABLED`.
- **D4 precision summary + push budget:** read-only `proactive_quality_summary(days)` (precision for all; recall where enumerable) → admin status: **prediction precision, suppression count, push success, cost, memory growth, cached-token ratio.** Enforce a **global passive-push budget: max 3/day excluding explicit reminders, prayers, briefings** (hard cap; rules compete by priority).
- **D5 durable outcome store (A3):** `add_notification_outcome_v2` (candidate_id + metadata, **Postgres-aware, dual-write, blob fallback**); change `_record_notification_outcome:17499` to forward optional kwargs (default `candidate_id=source`); dispatch sites pass real metadata where the candidate dict is in scope. Feedback-bias keeps reading the blob → no behavior change. Flag `HIRA_OUTCOMES_DURABLE`.
- **D6 promotion gate (later):** shadow→speaking when `n_resolved≥12 & precision≥0.70 (& recall≥0.40 where measurable)`; auto-demote on `precision<0.45` or neg-rate>0.6 with 2-run hysteresis; manual override always wins; dry-run unless `HIRA_AUTO_PROMOTE`.

**Acceptance:** shadow log captures would-fire candidates; resolvers label `submission_risk`/`prep_gap` with measurable precision *and* recall; admin status shows the full scorecard; 3/day budget enforced.

---

## Phase 5 — Daily Closure Cockpit (the product; read-only first)

Only after measured truth exists. A **thin read-only surface**, not a dashboard:
- **Now** (next few hours) · **Next** (upcoming class/risk/action) · **Waiting** (people/students/parents/admin/Gmail threads/submissions) · **Later** (safe to defer) · **Done Today** (proof HIRA helped) · **Why HIRA thinks this** (provenance from the situation model + resolvers).
Reads the situation model + the new telemetry; writes nothing. This is HIRA's daily operating rhythm.

**Acceptance:** the six panes render from real data with provenance; zero writes; no new proactive surface.

---

## Phase 6 — Action Cards (closure → time saved)

Convert closure items into **safe actions**: Draft chase message · Block marking time · Open ClassOps · Mark collected · Snooze · Don't suggest this again. Every action: **preview → confirm → log to action ledger (undoable) → feed outcome learning** (the existing audited write path + the Phase 4 resolvers).

**Acceptance:** each action previews + confirms + logs reversibly + records an outcome that flows into precision measurement.

---

## Phase 7 — Extraction (LAST, A6)

Only after queue behavior is pinned by tests and shadow/resolution is stable: extract `anticipation/` behind an `AnticipationRule` registry via `import bot` shim (**never `from bot import X`**), behavior-preserving commit-by-commit, with `test_anticipation_catalog.py` + a new registry test as the proof. Then (separate stages) decompose `_execute_tool:15926` and `_llm_text_async:4245`.

---

## What NOT to do (vetoes — both analyses agree)

- ❌ Add more anticipation rules now.
- ❌ Make Realtime voice the flagship now (voice makes mistakes feel authoritative; it sits on top of the measured spine, last).
- ❌ Build a polished cockpit before measured truth exists.
- ❌ Reduce tools for cheap-mode until there is tool-unavailable recovery (`bot.py:15001` has none).
- ❌ Store dict telemetry inside memory buckets (A2).
- ❌ Prune memory without soft-delete/restore (C3).
- ❌ Coarsen the whole prompt cache to daily / freeze the clock (A1).
- ❌ Big-bang refactor `bot.py`; extract only after behavior is pinned (A6).
- ❌ Auto-promote memory or rules without measured evidence.

---

## Execution order (hand to Codex top-to-bottom)

```
Phase 0  Regression tests first          ← pin the dangerous seams
Phase 1  Tier-0 primer + cache split     ← reclaim discarded signals; cost/latency win
Phase 2  Eval harness                    ← the ruler (judge stored answers, weekly, warn-only)
Phase 3  Memory hardening                ← readers→caps→soft-delete→dedup→prune→structured→recall
Phase 4  Shadow + resolvers + budget     ← measured truth for existing rules
Phase 5  Daily Closure Cockpit           ← the product surface (read-only)
Phase 6  Action Cards                    ← closure → safe, logged, learning actions
Phase 7  Extraction (anticipation/)      ← only after behavior is pinned
```

**Doctrine, final:** *Amended Stage 8 first — measurement, prompt-cache split, evals, memory hardening, shadow/resolvers — then Daily Closure. Fix the obvious cost leak before measuring; measure before you build; every commit dark behind a flag; reclaim what HIRA already computes before adding anything new.*

> Per-commit diffs, schemas, thresholds, and risk tables are preserved in the Stage 8 deep-audit transcript. Each phase is independently shippable and independently revertible. Stage docs are not a substitute for tests (`docs/MAINTENANCE.md`).
