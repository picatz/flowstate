---
name: comms-session
description: Session and status updates to the owner during agent work
---

Audience: the owner, supervising several concurrent agents. Their attention
is the scarcest resource in the factory; every update spends it.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

## When to speak

Interrupt when the owner's mental model should change: a discovery, a
constraint, a blocker, a risk. Never as a progress bar. Report delta over
restatement, and never re-report an agent report the owner already saw
summarized; double-summarization is a named sin in the ledger
(docs/plans/factory.md).

An end-of-turn update states where things stand and what, if anything,
needs the owner. Nothing else.

## Target shapes

No committed exemplar exists for this surface yet; these are the target
shapes, and real examples should replace them as good ones occur.

A discovery that changes the plan:

> TestTheMirrorMatchesTheRepository fails in my tree over a sibling
> worktree's uncommitted DSL.md edits. The test reaches outside its own
> tree; filed as factory friction. My slice is unaffected; continuing.

A blocker, carrying the decision it needs:

> Blocked on one decision: #412 settles the edition axis but not the
> follow-up issue for optional construction. File it, or fold the
> remainder into this slice?

An end-of-turn status:

> PR #479 open, gate green, Codex round pending. Nothing needs you until
> the review lands.

## Failure modes

- **The progress bar**: "now reading X, now running tests" with no model
  change. Silence is the correct rendering of progress.
- **The double summary**: re-summarizing a subagent's already-summarized
  report. The owner has read it once.
- **The mechanics narration**: worktree paths, tool-call counts, agent
  plumbing as content. Report outcomes, not machinery.

## Self-check

Does this update change what the owner knows or decides? If it fires
mid-turn, was the interruption worth its cost? Is any sentence a
restatement of something already on their screen?
