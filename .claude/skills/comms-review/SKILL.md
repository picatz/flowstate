---
name: comms-review
description: Author review comments and respond to review findings, ours and bots'
---

Audience: the PR author, who needs to know what to do and why it matters;
and the owner, who reads threads to judge whether review is finding signal.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

## Authoring a comment

Shape: severity, concern, consequence, expected action. Attach evidence
(the failing input, the code path, the execution that breaks the claim).
No tutorials; the author needs the defect, not the lecture behind it.

A review may return PASS. The metric is significance times confidence minus
verification burden, never comment count. A manufactured finding costs the
author a verification round and teaches them to discount the next real one.

The exemplar is Codex's P2 on #479: mechanism, consequence, action, in
that order:

> nothing actually blocks the first branch, so the Temporal test scheduler
> may finish `slow_a`/`slow_b` before `quick`. In that execution, a
> regression that appends child logs in completion order produces the same
> order as the expected written-order merge, allowing the test added
> specifically to catch that regression to pass. Add deterministic
> synchronization ...

## Responding to a review

Shape: decision, action, location. The pushed fix is the response to a bot
finding; no prose debates between AIs through GitHub accounts. When the
finding is right, say so in the fix commit and let its message carry the
mechanism and the re-proof. When it is wrong, one comment: the decision,
the evidence, done.

#479's response to the comment above was a commit, not a thread:

> tests: make the reverse-completion forcing deterministic, not probable

#478's response commit concedes on the merits and states the correction:

> Codex's review of #478 is right on the merits: identityFor deliberately
> turns a missing principal into an identity with no subject ... Say both,
> instead of an absolute claim the code does not make.

## Failure modes

- **The debate**: paragraphs arguing with a bot in a thread. The bot does
  not update, and the owner reads the argument twice.
- **The finding quota**: comments manufactured to prove the review ran.
  PASS is a result.
- **The tutorial**: explaining the general principle instead of the
  specific defect, its consequence, and the expected action.

## Self-check

Authoring: does each comment name what breaks, what that costs, and what to
do, with evidence? Would I stake the severity label on it? Responding: is
the fix pushed, and does my reply say anything the commit message does not?
