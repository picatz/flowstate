---
name: comms-issue
description: Draft or update a Flowstate GitHub issue or design proposal.
---

# Issue communication

An issue is a durable problem statement and decision surface, not a polished
transcript of the investigation.

## Before writing

Read the current issue when one exists. Verify claims against the current tree,
linked incidents, and relevant documentation. Search for duplicate issues and
for the repository's existing spelling of the concept.

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

For a design issue, show the smallest concrete sketch needed to expose the API or
language shape, then compare alternatives against existing invariants. Cite the
current source or documentation that makes a constraint real.

## Guardrails

- Do not hard-wrap GitHub prose; keep each paragraph and list item on one
  source line and let the browser wrap it.
- Separate observation, hypothesis, proposal, and decision. Do not turn a likely
  explanation into a recorded fact.
- Do not invent priority, customer impact, ownership, dates, consensus, or scope.
- Do not turn a narrow defect into a strategy memo or attach unrelated cleanup.
- Prefer acceptance criteria over phrases such as “fully support” or “handle all
  cases.”
- Draft by default. Create, edit, close, label, or comment on the issue only when
  the user has authorized that external action.
- Preserve host-supplied attribution; never guess or duplicate a model footer.

## Historical field notes

Read [the archived comms-issue guidance](../../../.agent-history/skills/comms-issue/SKILL.md) only when a prior incident, exemplar, or host-specific rationale is relevant. It is evidence and history, not a second current procedure.
