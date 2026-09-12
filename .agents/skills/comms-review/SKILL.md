---
name: comms-review
description: Use when reviewing a Flowstate diff or pull request, or when responding to review findings from a person, a bot, or another model; it defines what counts as a finding and how a disposition is recorded.
---

# Review communication

External review should confirm rather than discover the basic work. Finding
count is not a quality metric; `PASS` is a valid result, and a reviewer asked to
find gaps must not manufacture them.

## Review the current change

Identify the exact base and head, then inspect the actual diff and the
surrounding code needed to understand the changed behavior. Prioritize:

1. correctness and reachable failure paths;
2. security and trust-boundary changes;
3. compatibility and durable-state consequences;
4. local/Temporal driver agreement;
5. bounds, cancellation, cleanup, and fail-closed behavior;
6. tests that can pass without exercising the claimed mechanism;
7. generated artifacts and public documentation that can drift.

Preferences already handled by formatting, linting, or an established local
idiom are not findings unless the choice creates real maintenance or correctness
risk. Report them, if at all, as optional and say so.

## Finding contract

A material finding contains severity proportional to the consequence, the exact
location, the affected behavior and concrete execution path, why the current
result is wrong or unsafe, the impact, evidence or a falsifiable way to validate
it, and the smallest credible direction for repair. Label uncertainty rather
than upgrading model confidence into fact. Deduplicate findings that share one
root cause.

## Responding to findings

Verify the finding against the current head before changing code, then choose
one honest disposition: fix it and cite the evidence; explain with repository
evidence why it does not apply; record it as a scoped follow-up when it is real
but outside this change; or mark it stale when the referenced code no longer
exists. Repair the mechanism the evidence identifies rather than obeying a
suggested patch because the diagnosis was useful.

Two rounds that keep producing new material findings mean the change, not the
review, needs rethinking: stop, name the recurring root cause, and ask.

## GitHub review state

Read the complete current state rather than a default first page; list
endpoints paginate. GitHub REST and GraphQL budgets are independent, so a
working REST call does not prove review-thread GraphQL was available. If
unresolved-thread state cannot be queried, report that check as unavailable
rather than treating silence as approval. Resolve a thread only when its
disposition is visible on it, and hide bot summary comments that carry no
decision once their round is dispositioned, so a human reviewer sees decisions
rather than AI traffic; never reply to an AI reviewer with AI prose. Merging is
governed by the `flowstate-ship` skill; Claude's merge hook is an additional
control, not a substitute for that evidence, and other hosts do not run it.

## History

[Archived comms-review guidance](../../../.agent-history/skills/comms-review/SKILL.md)
is evidence and history, not a second current procedure.
