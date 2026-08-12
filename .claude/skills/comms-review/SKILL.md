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

## Merging

Never merge pull requests back to back. Each PR's CI proves exactly one
tree: that branch merged with whatever main existed when the run started.
It says nothing about a tree that includes a second PR merged a minute
later, because no check re-runs against the new main. On 2026-08-12, four
PRs merged inside ninety seconds broke main this way: one PR added a call
to `runNodes` with six arguments while another, merged seconds after, gave
`runNodes` a seventh parameter. Neither diff touched a line the other
touched, so git merged both without a textual conflict, and every CI run
that had passed was proving a tree that no longer existed. The breakage
surfaced later on an unrelated docs PR, whose five red jobs had nothing to
do with its own diff. Merge one PR, confirm main still builds, then merge
the next. Issue #489 carries the full account and the branch-protection
fix that came out of it.

## The bot-review loop

Automated review (Codex, Copilot) lands on every PR. The procedure is
fixed, and it is a token and API budget as much as a writing one. Handle
every thread before merge, never after: a PR merged once with its review
still unread and its threads still unresolved is the failure the rest of
this section exists to prevent.

1. **Read reviewer state cheaply before fetching threads.** Codex signals
   on the PR description itself: an eyes reaction while it is still
   reviewing, a thumbs up once it has finished and agrees. One read of the
   description's reactions tells you whether a review is in flight, for a
   fraction of the cost of fetching threads. Do not merge while one is in
   flight.
2. **Fetch once per PR, not once per finding.** `get_review_comments`
   returns the threads with their `isResolved`/`isOutdated` metadata.
   Never fetch the full PR object just to check state; its body is large
   and you already know the PR. Use `get_check_runs` for CI status.
3. **Triage the whole set in one pass.** Classify each finding: real (fix
   it), stale (a later commit already fixed it; no action), or wrong
   (skip, and say why in the session report, not on GitHub).
4. **One agent per PR carrying all real findings**, never one agent per
   finding. Route to the branch owner if it has context left, otherwise a
   fresh cheaper agent with the branch as the handoff.
5. **The pushed fix is the reply.** No prose replies to bots, and never
   argue with one: two AIs debating through a human's GitHub account is
   noise the owner has to read.
6. **Prefer a reaction over a comment.** Codex asks for a thumbs up or
   down on every finding it files. A reaction costs one call and adds no
   visual noise, and `add_reply_to_pull_request_comment` accepts a
   reaction with no body, so it never creates a comment of its own. Thumbs
   up on a real finding that got a fix, thumbs down on a false positive
   that was declined, nothing on duplicates and stale threads where there
   is no judgment to send.
7. **Write a line only when a human reading later would otherwise be
   lost.** A resolved thread is collapsed, so one line inside it costs no
   visual noise on the PR, but it still costs tokens and attention, so it
   is not automatic. Less is more for agent-to-agent traffic:
   - A declined finding always gets a line, and is never silently
     resolved. File it as an issue first, then point the thread at it:
     "Not doing this: `<one clause>`. Filed as #NNN."
   - A non-obvious stale thread gets one line: "Stale: `<what superseded
     it>`."
   - A fix that is obvious from the diff gets a reaction and no line; the
     commit is the argument.
   - A duplicate gets neither reaction nor line.
8. **Resolve every thread**, with `resolve_review_thread` and the node ID
   from step 2, including stale, outdated, and duplicate ones, not only
   the one you acted on. Multiple reviewers frequently file the same
   finding, and every copy needs closing. Resolution is the
   acknowledgment, and it is also what keeps the next review round cheap
   to read: an unresolved thread that needed no action still costs a fetch
   on every later round.
9. **Respect the API budget.** The GitHub REST and GraphQL limit is shared
   across everything the account does, and polling PR state exhausted it
   in one wave. Prefer webhook events over polling, batch reads, and back
   off when the limit is hit rather than retrying.
10. **Know what the API cannot do.** GitHub exposes resolve and unresolve
    for review threads only. There is no dismiss and no minimize for a
    review's top-level summary body, so that block cannot be collapsed
    after the fact once posted. The way to reduce those is reviewer
    configuration, automatic review set to request-only rather than on
    every push, which is a repository setting and therefore an owner
    decision, not something an agent changes.

## The API budget is bounded

The account's GitHub API budget is a shared, hourly, bounded resource, so
the repo's own doctrine applies here too: bound the resource the consumer
controls. It was exhausted twice in one session, both times by PR status
polling, and both times it blocked real work.

- **Events beat polling.** Webhook activity already wakes a session on CI
  failures, review comments, and merges. The only legitimate polls are for
  state webhooks do not cover, and those get one deliberate check rather
  than a loop.
- **Cheapest read first.** A reaction on the PR description beats
  fetching threads. `list_pull_requests` with an explicit fields subset
  that omits `body` gets every open PR's head and state in one call,
  instead of a get per PR. Fetch threads once per PR, never per finding.
  Never re-read something to confirm what you already know, and never
  verify a merge you just performed: the merge response is the
  confirmation.
- **REST and GraphQL are metered separately, and exhaust independently.**
  Observed directly: with GraphQL gone, review-thread reads failed on
  every attempt while check runs, PR creation, and merges all succeeded in
  the same minute. Exhaustion is not a full stop; check whether the work
  you need lives on the other budget. Pushing a branch is neither REST nor
  GraphQL, it is git over the proxy, so pushing work and deferring PR
  creation beats stalling.
- **Do not let the working budget paper over the missing one.** With
  GraphQL down you can still merge, which is exactly the temptation that
  produces a merge with unread review state. If review state cannot be
  read, the merge waits. A rule that only holds when it is convenient is
  not a rule.
- **On exhaustion:** stop cleanly, do not retry in a loop, do not spread
  the same calls across subagents, report what is blocked, schedule one
  retry past the reset, and switch to work that needs no API.

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
