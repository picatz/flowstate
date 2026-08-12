---
name: pre-pr-review
description: Self-review before opening any PR, over the diff and every shipped artifact
---

Run this before opening a PR, over the diff and over every artifact the PR
ships: body, commit messages, comments, docs. The review may return PASS;
finding-count is not the metric. Its purpose is that external review
confirms rather than discovers.

Founding examples: the two Codex P2s and the P1 from the 2026-08 wave.
Each is an instance of a lens below, and each would have been caught by a
pre-push pass.

## Correctness lenses

1. **Absolutes qualified against actual code paths.** For every "never",
   "always", "refuses" in prose or comments: find the code path that would
   break it. Founding example (#478 P2): server doc.go claimed the server
   refuses unauthenticated requests; `identityFor` deliberately accepts a
   missing principal under `--insecure-no-auth` or when the handler is
   mounted without the interceptor. The claim was qualified, not defended.

2. **Ordering tests force deterministically, never probabilistically.**
   A test that makes the bad order likely lets the regression pass whenever
   the scheduler picks the good one. Founding example (#479 P2): the undo
   ordering case gave one branch more work instead of holding it; the fix
   is a hold the virtual clock cannot race past, plus a mutation re-proof.

3. **Bounds asserted reached as well as not exceeded.**
   `scanned <= maxListScan` is also satisfied by a walk that gave up after
   one batch. Every bound needs a case that reaches it.

4. **Isolation tests written in the negative direction.** A cannot reach
   B, not A can reach A. A test that each party reads its own resource is
   a functionality test wearing a security test's clothes.

5. **A test claiming to catch a regression carries a mutation proof.**
   Introduce the regression, watch the case fail, and say so where the
   test is presented ("reversing the durable driver's merge loop fails
   this case").

6. **Rewriter changes byte-compared, with semantic-equivalence proofs.**
   Founding example (#483 P1): the fixoptional textual match consumed
   `has(x.y) && x.y` out of `has(x.y) && x.y == false`, so `flow fix`
   could silently reverse a gate's meaning for an absent field. Matches
   respect operand boundaries: parse the AST, never trust the adjacent
   byte. Test by comparing bytes or compiling the result; "output still
   validates" has let every rewriter bug through.

## Receiver-cost checks

Over the PR body and every shipped artifact:

- **Compression test**: what can be cut without harming the reader's
  decision or action? Cut it.
- **Handoff test**: can a cold reader continue from this alone?
- **Context test**: am I restating what the reader already has (the
  issue, the diff, the previous comment)?
- **Surface test**: would a strong practitioner write it this way, here?
  The DSL.md register stays out of PR bodies and review replies.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

## Output

For each lens: PASS, or the finding with its location and the concrete
fix. Do not manufacture findings to prove the review ran; a
mandatory-finding editor creates churn, and churn teaches readers to
discount the next real finding.
