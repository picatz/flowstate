---
name: comms-review
description: Review a Flowstate diff or respond to review findings with concrete evidence.
---

# Review communication

External review should confirm rather than discover the basic work. Finding
count is not a quality metric; `PASS` is a valid result.

## Review the current change

Identify the exact base and head, then inspect the actual diff and the surrounding
code needed to understand changed behavior. Prioritize:

1. correctness and reachable failure paths;
2. security and trust-boundary changes;
3. compatibility and durable-state consequences;
4. local/Temporal driver agreement;
5. bounds, cancellation, cleanup, and fail-closed behavior;
6. tests that can pass without exercising the claimed mechanism;
7. generated artifacts and public documentation that can drift.

Do not spend a human's attention on preferences already handled by formatting,
linting, or an established local idiom unless the choice creates real
maintenance or correctness risk.

## Finding contract

A material finding contains:

- severity proportional to the consequence;
- exact location;
- the affected behavior and concrete execution path;
- why the current result is wrong or unsafe;
- user, reliability, compatibility, or security impact;
- evidence, reproducer, or a falsifiable way to validate it;
- the smallest credible direction for repair.

Label uncertainty. Do not upgrade model confidence into fact. Deduplicate
findings that share one root cause.

## Responding to findings

Verify the finding against the current head before changing code. Then choose one
honest disposition:

- fix it and cite the resulting evidence;
- explain, with concrete repository evidence, why it does not apply;
- record it as a scoped follow-up when it is real but intentionally outside this
  change;
- mark it stale when the referenced code no longer exists.

Do not obey a suggested patch merely because the diagnosis was useful. Repair the
mechanism the evidence identifies.

## GitHub review state

Read the complete current state rather than a default first page. Use pagination
for list endpoints. GitHub REST and GraphQL budgets are independent, so a working
REST call does not prove review-thread GraphQL was available. If unresolved-thread
state cannot be queried, report that check as unavailable rather than treating
silence as approval.

## Remote-state guardrails

Draft review comments unless the user asked to post them. Resolve threads only
when the disposition is visible and supported. Merge only with explicit
permission and after current review state and required checks have been
inspected. Claude's merge hook is an additional control, not a substitute for
that evidence, and other hosts do not run it.

## Historical field notes

Read [the archived comms-review guidance](../../../.agent-history/skills/comms-review/SKILL.md) only when a prior incident, exemplar, or host-specific rationale is relevant. It is evidence and history, not a second current procedure.
