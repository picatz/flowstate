---
name: comms-pr
description: Write a PR body that explains what the diff cannot (why, decisions, testing, risks, omissions)
---

Audience: the owner deciding whether to merge, the reviewer (human or bot)
deciding where to look, and the future reader who reaches this text through
the merged PR.

Purpose: the diff shows what changed. The body carries what the diff cannot
show: why, the non-obvious decision, tradeoffs, testing, risks, and
intentional omissions. Never narrate files.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

## What must survive

- The decision this PR implements, with the issue and decision comment
  named in the first sentence.
- Any claim about the tree the diff cannot show: what was already true on
  main, what is unreachable, what is deferred and why.
- Proof of the tests' teeth: which mutation fails which case.
- Which gate ran, and anything it caught on the way.
- What was deliberately not done.

## What must not appear

- File-by-file narration. The diff already shows it.
- Effort narration: exploration stories, tool-call counts. Twenty tool
  calls do not justify twenty paragraphs.
- The literary register of DSL.md. That voice belongs to durable doctrine,
  not to a merge decision.

## Shape

One opening sentence tying the diff to its decision. A short section for
any load-bearing claim about the tree. What landed, grouped by claim rather
than by file. Gate status. What this unblocks or closes.

## Examples (from PR #479, the exemplar)

Opening sentence, decision first:

> Slice 0.5 of #418, per the owner's decision comment (2026-08-11):
> `undo:` unwinds in reverse *written* order, never reverse completion
> order.

A claim the diff cannot show, stated before the change list:

> So the ordering is not reachable-nondeterministic on `main` today. What
> was missing, and what this PR lands, is the *contract*: nothing pinned
> parallel siblings' unwind order against a completion-order regression.

A test bullet that carries its proof:

> Mutation-tested: reversing the durable driver's merge loop fails this
> case.

## Failure modes

- **The file tour**: bullets restating the diff per file. Delete them;
  keep only what a reader cannot get from the diff.
- **The effort diary**: paragraphs proving work happened. Reasoning depth
  and publication verbosity are separate controls.
- **The bare gate claim**: "all tests pass" without naming the gate that
  ran or what it caught. #479's body names the first-run mirror failure it
  hit and fixed; that sentence saved its reviewer a question.

## Self-check

Could the owner merge from this body plus a skim of the diff? Does every
paragraph say something the diff cannot? Is anything here already in the
linked issue? Cut whatever fails.
