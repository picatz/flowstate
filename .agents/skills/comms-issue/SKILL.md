---
name: comms-issue
description: Use when drafting or updating a Flowstate GitHub issue or design proposal, so it lands as a durable problem statement with evidence and acceptance criteria rather than a transcript.
---

# Issue communication

An issue is a durable problem statement and decision surface, not a polished
transcript of the investigation.

## Before writing

Read the current issue when one exists. Verify claims against the current tree,
linked incidents, and documentation. Search for duplicate issues and for the
repository's existing spelling of the concept; `docs/DSL.md` records most
language decisions and why they were taken.

## Default shape

1. **Problem or observed behavior** — what is true now and why it matters.
2. **Evidence** — reproducer, diagnostic, code path, measurement, or concrete
   example.
3. **Desired outcome** — observable behavior after the work, not a premature
   implementation prescription.
4. **Acceptance criteria** — conditions a reviewer or test can evaluate.
5. **Constraints and dependencies** — compatibility, threat boundary, rollout,
   or sequencing facts that materially constrain the solution.
6. **Open questions** — only decisions that are genuinely unresolved.

For a design issue, show the smallest concrete sketch that exposes the API or
language shape, then compare alternatives against the existing invariants. Cite
the current source or documentation that makes a constraint real.

## Repository specifics

- Do not hard-wrap GitHub prose; keep each paragraph and list item on one source
  line and let the browser wrap it.
- Keep observation, hypothesis, proposal, and decision separate; a likely
  explanation is not a recorded fact.
- Labels: `kind/bug` needs a reproducer, `kind/design-record` records a decision
  before code, `kind/decision` is the maintainer's call, `kind/umbrella` groups
  slices; area labels say where.

## History

[Archived comms-issue guidance](../../../.agent-history/skills/comms-issue/SKILL.md)
is evidence and history, not a second current procedure.
