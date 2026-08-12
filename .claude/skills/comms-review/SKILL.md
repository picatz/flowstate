---
name: comms-review
description: Author review comments and respond to review findings, ours and bots'
---

Audience: the PR author, who needs to know what to do and why it matters;
and the owner, who reads threads to judge whether review is finding signal.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

Every comment or reply a Claude agent posts ends with the attribution
footer; see comms-pr for the exact form and why a compact variant does not
substitute for it there.

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

## The bot-review loop

Automated review (Codex, Copilot) lands on every PR. The procedure is
fixed, and it is a token and API budget as much as a writing one:

1. **Fetch once per PR, not once per finding.** `get_review_comments`
   returns the threads with their `isResolved`/`isOutdated` metadata.
   Never fetch the full PR object just to check state; its body is large
   and you already know the PR. Use `get_check_runs` for CI status.
2. **Triage the whole set in one pass.** Classify each finding: real (fix
   it), stale (a later commit already fixed it; no action), or wrong
   (skip, and say why in the session report, not on GitHub).
3. **One agent per PR carrying all real findings**, never one agent per
   finding. Route to the branch owner if it has context left, otherwise a
   fresh cheaper agent with the branch as the handoff.
4. **The pushed fix is the reply.** No prose replies to bots, and never
   argue with one: two AIs debating through a human's GitHub account is
   noise the owner has to read.
5. **Resolve the thread once the fix has landed**, with
   `resolve_review_thread` and the node ID from step 1. Resolve stale,
   outdated, and duplicate threads too, not only the one you acted on;
   multiple reviewers frequently file the same finding, and every copy of
   it needs closing. Resolution is the acknowledgment, and it is also what
   keeps the next review round cheap to read: an unresolved thread that
   needed no action still costs a fetch.
6. **Respect the API budget.** The GitHub REST and GraphQL limit is shared
   across everything the account does, and polling PR state exhausted it
   in one wave. Prefer webhook events over polling, batch reads, and back
   off when the limit is hit rather than retrying.
7. **Know what the API cannot do.** GitHub exposes resolve and unresolve
   for review threads only. There is no dismiss and no minimize for a
   review's top-level summary body, so that block cannot be collapsed after
   the fact once posted. The way to reduce those is reviewer configuration,
   automatic review set to request-only rather than on every push, which is
   a repository setting and therefore an owner decision, not something an
   agent changes.

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
Every thread on this PR is resolved or has a pending fix.
