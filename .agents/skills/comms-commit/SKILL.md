---
name: comms-commit
description: Write a Flowstate commit message from the actual diff and completed verification.
---

# Commit communication

Write for the future reader who is deciding what changed, why it changed, and
whether the commit belongs in a history.

## Gather evidence

Inspect the staged or intended diff, the relevant issue or decision, and the
verification that actually completed. Do not write from the conversation's
memory when the tree can answer.

## Shape

- Use an imperative subject that names the affected area and the behavioral
  result. Keep it specific enough to distinguish this change from its neighbors.
- Add a body when the motivation, compatibility choice, security property,
  non-obvious mechanism, or verification cannot fit honestly in the subject.
- Explain the decision and consequence, not a file-by-file tour or a diary of how
  the agent arrived there.
- Name a limitation or unverified leg when it changes how the commit should be
  reviewed or used.

## Guardrails

- Do not claim a test, mutation, review, or generated check ran unless its result
  is available.
- Do not include generic praise, repeated summaries, token/tool counts, or agent
  plumbing.
- Do not hard-code a model name or add a duplicate co-author or provenance
  trailer. Preserve the active host's automatic attribution and add anything
  else only when the user or repository explicitly requires it.
- A small commit may need only a subject. Length is earned by information the
  next reader cannot recover cheaply from the diff.

## Historical field notes

Read [the archived comms-commit guidance](../../../.agent-history/skills/comms-commit/SKILL.md) only when a prior incident, exemplar, or host-specific rationale is relevant. It is evidence and history, not a second current procedure.
