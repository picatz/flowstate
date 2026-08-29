---
name: comms-pr
description: Draft or update a Flowstate pull request from the actual diff and verification.
---

# Pull request communication

A PR body should make the change reviewable without restating the diff.

## Gather evidence

Inspect the base/head diff, commits, linked issue or design decision, generated
artifacts, and command results. Treat these as authoritative over conversational
recollection. If the branch moved, refresh them before editing the body.

## Default shape

1. **Why** — the problem, decision, or missing capability this change addresses.
2. **What changed** — the behavioral shape and the important boundaries, not a
   list of files.
3. **Design choices** — only non-obvious choices a reviewer must evaluate,
   including alternatives deliberately refused.
4. **Verification** — exact checks that completed and the behavior they prove.
5. **Risk, compatibility, and rollout** — where the change can fail, what remains
   compatible, and any migration or operational consequence.
6. **Remaining uncertainty** — checks not run, known limitations, or follow-up
   work that is intentionally outside this PR.

Use tables, examples, or mutation evidence only when they materially reduce the
reviewer's work. A small change should have a small body.

## Guardrails

- Do not hard-wrap GitHub prose; keep each paragraph and list item on one
  source line and let the browser wrap it.
- Do not narrate every changed file or repeat the linked issue's full history.
- Do not say “fully tested,” “safe,” “backward-compatible,” or “no impact” unless
  the available evidence supports the exact claim.
- Distinguish a local pass from CI, and a targeted check from the full gate.
- Do not hide a skipped or unavailable check behind silence.
- Do not include internal agent mechanics, reasoning transcripts, or decorative
  structure that carries no review decision.
- Draft by default. Open or update the PR only when the user has asked for it.
- Preserve platform-generated attribution and avoid duplicate model footers.

## Historical field notes

Read [the archived comms-pr guidance](../../../.agent-history/skills/comms-pr/SKILL.md) only when a prior incident, exemplar, or host-specific rationale is relevant. It is evidence and history, not a second current procedure.
