# HIRA Maintenance Notes

Use these notes for development hygiene. They are not product features and should not replace tests, code review, or clear final summaries.

## When To Create A Handoff

- Create a handoff for major features, multi-file changes, architecture decisions, or interrupted work that another agent may need to resume.
- Skip handoffs for small one-file fixes where the final response and git diff are enough.
- Prefer `docs/handoffs/YYYY-MM-DD-short-topic.md` for substantial handoffs once the folder is needed.

## Handoff Contents

Use `docs/HANDOFF_TEMPLATE.md`. Keep it short:

- what changed
- files touched
- verification run
- known risks
- next safest step

## Retrospectives

Use `docs/RETROSPECTIVE_TEMPLATE.md` when a repeated development pattern appears. Do not change durable instructions from a single awkward session.

Before proposing an `AGENTS.md` change, record:

- the repeated pattern
- concrete evidence
- why the rule belongs in durable instructions instead of a one-off note

## Verification

Run the lightweight gate before shipping behavior changes:

```bash
python3 scripts/dev_check.py
```

For PWA or UI changes, also run the relevant smoke check or browser verification for the changed surface.

## Stage Docs

- Stage docs describe planned work and review decisions.
- Keep them scoped to the stage.
- Do not use a stage doc as a substitute for tests.
- When a stage plan changes after review, reconcile the review in the doc rather than leaving contradictory instructions.

## Change Discipline

- Keep edits surgical.
- Match existing code style.
- Do not refactor unrelated code while implementing a stage.
- Preserve user-written memory, docs, and uncommitted changes unless explicitly asked to modify them.
