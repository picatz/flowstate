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

Do not hard-wrap a review comment or reply. GitHub renders a single
newline in one as a line break, so wrapped prose arrives ragged; write
each paragraph as one line. comms-pr has the rule and the reason it
differs from a file in the repository. A one-line disposition is short
enough that this rarely bites, which is exactly why it gets forgotten on
the longer replies where it shows.

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

### Read the base before you merge

A merge button says "Merge pull request" whether the base is `main` or a
branch that merged three days ago, and the API answers `"merged": true`
either way. On 2026-08-15 four PRs in one queue targeted branches rather
than `main` — #622 onto `claude/envvars-read-locations`, #624 onto
`claude/serving-tls-internal-listener`, #625 and #626 likewise — because
each was authored from a worktree checked out on the branch its work
followed. Every one of those base branches had already merged and been
left behind. #622 was merged before anyone looked: it reported success,
main did not move, no CI ran, and the work sat on a dead branch.

So read `base.ref` as part of deciding to merge, not afterwards. It costs
one field on a call already being made, and the failure it prevents is
silent in every direction — green checks, a successful merge response,
and nothing on `main`.

Two things follow when the base is wrong. Changing a PR's base is not
available to every session, so the fallback is a second PR from the same
head branch against `main`, and a comment on the original saying which PR
carries the work and why. And a branch whose base was a stale branch is
itself stale: merging `main` into it before opening the replacement is
what stops its diff from reverting whatever landed in between. Diff the
branch against `main` and read the stat line before opening anything —
three of those four would have deleted a PR merged an hour earlier, and
the diff said so plainly.

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
   up on a real finding that got a fix, thumbs down on a finding you
   determined is wrong, nothing on duplicates and stale threads where
   there is no judgment to send.
7. **React and resolve. Do not reply.** This is the default for roughly
   nineteen threads in twenty, and it is a rule about cost as much as
   about noise: every reply is tokens spent writing prose to a bot that
   does not read it, on a thread a human will see collapsed. The fix is
   the answer, the commit is the argument, and the reaction is the
   acknowledgment. Say nothing else.

   Three exceptions, and they are the whole list:
   - **Refuting.** You determined the finding is wrong. It gets a thumbs
     down, one sentence of evidence so a human scanning the thread knows
     it was judged rather than ignored, and a resolve. Never file an
     issue for a defect you have established does not exist. A refuted P1
     on #492 was handled exactly this way.
   - **Deferring.** The finding is real and you are not fixing it here.
     File the issue first, then one line pointing at it: "Real, not
     fixing here: `<one clause>`. Filed as #NNN." Never silently resolve
     a real finding.
   - **Correcting a wrong premise.** The reviewer's claim rests on
     something factually untrue about the code, and leaving it unsaid
     would mislead the next reader of that thread.

   Everything else gets a reaction and a resolve. A fix that landed, a
   stale thread, a duplicate, a finding that was already handled in
   another thread: no words. If you find yourself explaining a fix that
   the diff already shows, stop and delete the reply.
8. **Resolve every thread**, with `resolve_review_thread` and the node ID
   from step 2, including stale, outdated, and duplicate ones, not only
   the one you acted on. Multiple reviewers frequently file the same
   finding, and every copy needs closing. Resolution is the
   acknowledgment, and it is what keeps the next review round cheap to
   *read*, not cheap to fetch: `get_review_comments` returns resolved and
   unresolved threads alike, exposing `isResolved` only as metadata, so an
   unresolved thread costs no extra API call. What it costs is triage and
   human attention, and, on the PR page, visual noise. Resolve for that
   reason, not to save a fetch.

   **Leave nothing open.** The target is a PR page a human can read with
   no bot traffic expanded on it. That means every thread from every
   reviewer, on every PR you touched, resolved before you consider the PR
   done, and swept again before you stop working: a review that arrives
   after your last pass is the one that gets forgotten. A merged PR still
   deserves the sweep, because its page is what someone reads later when
   they are trying to understand the change.

   **The root review body counts too, and the gap is ours.** A review is
   one top-level summary plus its threads, and resolving only the threads
   leaves the summary expanded on the page — which is the noise a human
   actually lands on. GitHub's API can hide that summary: `PullRequestReview`
   implements `Minimizable` in the GraphQL schema, exactly as
   `PullRequestReviewComment` does, so `minimizeComment` accepts a review's
   node id with a `minimizedReason` of `resolved` or `outdated`.

   All three ways to reach it are blocked for this session type — tested on
   2026-08-16 against a Copilot `COMMENTED` review, not assumed:

       gh api graphql … minimizeComment       403  not in the pinned set
       PUT  …/pulls/N/reviews/ID/dismissals   403  "not permitted for this
                                                    session type"
       DELETE …/pulls/N/reviews/ID            403  pending reviews only

   So a review body cannot be hidden from here at all. Say that plainly
   rather than reporting a PR as swept when its most visible block is not,
   and do not spend calls rediscovering it: those three results are the
   whole surface, and the fix is an added capability or an owner hiding it
   in the UI.

   Keep the three classes straight, because each has a different mechanism
   and only one of them is a resolve: a review **thread** resolves, a
   **top-level bot comment** deletes (below), and a review **body** does
   neither.

   The complementary lever is reviewer configuration: automatic review set
   to request-only rather than on every push produces fewer summaries at
   the source. That is a repository setting and therefore an owner
   decision, and it reduces the inflow — it does not close what is already
   on the page.

   **A bot's operational notice is a third class, and it has its own
   mechanism.** Codex posts things like `You have reached your Codex usage
   limits for security reviews. Please try again later.` as a *top-level
   PR comment*, not a review thread. Nothing resolves a top-level comment —
   GitHub offers no resolve for one, and `minimizeComment` needs GraphQL
   this environment refuses. So a sweep that only resolves threads leaves
   these sitting on the page, and twenty-four of them accumulated across one
   wave of PRs before anyone counted.

   REST deletes them, and that is the right answer *for this class only*:

       gh api -X DELETE repos/{owner}/{repo}/issues/comments/{id}

   **Never delete a finding.** Not a review comment, not a review summary,
   not a human's comment, not a bot's comment that contains an actual claim
   about the code — even a wrong one, which gets a thumbs-down and a
   sentence of evidence so the judgment is on the record. Deletion destroys
   the record, and the record is why the thread rule exists at all.

   What qualifies is narrow and testable: the comment carries **no claim
   about the diff**. A rate-limit notice, a "try again later", a bot
   announcing it is starting. If you have to think about whether it carries
   signal, it does — resolve it, or leave it and say so.

   If `minimizeComment` becomes reachable, prefer it: hiding preserves the
   record where deleting does not, and the narrowness of this rule is a
   consequence of only having the destructive tool.
9. **Read threads over REST, resolve over GraphQL.** This is the single
   biggest budget lever and it was found the expensive way: one wave of 27
   PRs burned 10,757 GraphQL points and exhausted the pool, almost all of it
   on *reading* threads rather than resolving them.

   `GET /repos/{owner}/{repo}/pulls/{n}/comments` is REST, costs the core
   budget (15,000/hour, rarely the constraint), and returns every review
   comment with its id, author, path, line and `in_reply_to_id` — enough to
   group comments into threads and decide what to act on. What REST does
   *not* return is resolution state; there is no `isResolved` on that
   payload, verified.

   So: sweep with REST, act with REST (reactions, replies, deletes are all
   REST), and spend GraphQL only on `resolve_review_thread` itself — one
   call per thread you actually resolve. Track what you resolved in your own
   notes rather than re-reading to confirm it; a resolve that returned
   success succeeded, and re-reading to check costs the budget the sweep was
   trying to save.

   The failure mode this prevents is not slowness. With GraphQL exhausted,
   threads cannot be resolved at all, and the rule that nothing merges with
   unresolved threads then blocks every merge — the pool running dry stops
   the entire pipeline, not just the sweep.

10. **Respect the API budget.** REST and GraphQL each have their own pool.
   A pool is shared across everything the account does, so polling PR
   state exhausted one of them in a single wave, but the two are not
   shared with each other and go down independently. Prefer webhook
   events over polling, batch reads, and when a pool is exhausted back
   off from *that* pool rather than stopping altogether: see the budget
   section below for what still works when one of them is gone.
11. **Know what the API can do, and separately what our tools can do.**
    These are two different questions and conflating them turns a missing
    tool into an imagined law of nature. GitHub's API offers all three
    operations: resolve/unresolve on threads; dismissal of a review, in
    both REST and GraphQL, which invalidates that review's state so a
    `REQUEST_CHANGES` stops blocking without hiding anything; and
    `minimizeComment`, which does hide a body and accepts a
    `PullRequestReview` node id because that type implements
    `Minimizable`. Our MCP surface exposes only the first. So dismissal
    and minimize are unavailable *in practice here*, which is a sentence
    about our tooling — write it that way, and do not claim the API lacks
    what it has. Checked against the GraphQL reference on 2026-08-13; the
    earlier version of this rule asserted the opposite and was wrong.

## The API budget is bounded — and measurable

The account's GitHub API budget is a shared, hourly, bounded resource, so
the repo's own doctrine applies here too: bound the resource the consumer
controls. It was exhausted three times in one session, and every time the
session treated it as a full stop when it was not.

**Measure it before rationing it.** One call, and it does not count
against anything:

    curl -sS https://api.github.com/rate_limit

The first time anyone actually ran it, the numbers reframed the whole
problem: `graphql` at 10039 used against a 5000 limit — 2× over — while
`core` sat at 13 used against 15000. The session had been holding merges
because "the API is exhausted" with 14,987 REST calls unused. Guessing
at a bounded resource produced a 500× misestimate of what was left.

- **The two pools are not comparable in size or in kind.** REST is
  metered in *calls* (15,000/hr). GraphQL is metered in *points*
  (5,000/hr), charged by query complexity — a thread listing with nested
  connections costs many points per call, which is why GraphQL drains
  invisibly while REST barely moves. A few dozen thread reads went 2×
  over.
- **Route the operation to the pool that has budget, and prefer REST.**
  Only two things in this loop genuinely require GraphQL: resolving a
  thread, and reading whether a thread is resolved. Everything else has a
  REST spelling, so the scarce pool is spent on resolve mutations at
  roughly a point each — hundreds per hour, far more than any review wave
  needs:

      gh api repos/{o}/{r}/pulls/{n}/comments --paginate    # findings, with diff_hunk
      gh api repos/{o}/{r}/pulls/{n}/reviews  --paginate    # review summary bodies
      gh api repos/{o}/{r}/pulls/comments/{id}/reactions -f content=+1
      gh api repos/{o}/{r}/pulls/{n}/comments/{id}/replies -f body=…
      gh api repos/{o}/{r}/commits/{sha}/check-runs --paginate

  **`--paginate`, always, on any listing.** `per_page` defaults to 30, so
  the bare call silently truncates a PR with 31 findings, and the review
  loop then reports it handled having never seen the rest — the exact
  failure this section exists to prevent, reintroduced by the fix for it.
  Codex caught this in the first draft of this very paragraph. `gh`
  follows the `Link` headers itself, which is most of why it beats
  hand-rolled requests here: the correct thing is also the shorter thing
  to type.
- **The MCP tools are not the only door.** `gh` is authenticated through
  the proxy and reaches the whole REST API, including endpoints the MCP
  surface does not expose. Install it once with
  `GOBIN=/usr/local/bin go install github.com/cli/cli/v2/cmd/gh@latest`
  if it is missing. When an MCP tool fails on a GraphQL limit, ask
  whether the thing you wanted has a REST endpoint before concluding you
  are blocked — twice now the answer was yes and the block was
  self-imposed. Direct requests also appear not to draw down the pool
  `rate_limit` reports (five calls left `core.used` unmoved), so treat
  that counter as a floor on what is left rather than a precise gauge,
  and do not build a plan on its exact value. The same door does not open
  onto GraphQL: the proxy serves only a pinned set of PR-review
  operations there and answers anything else with a 403 telling you to
  use REST, so hand-written or batched GraphQL is not a way around an
  exhausted GraphQL pool — `gh api graphql` gets the same 403. Resolving
  threads goes through the MCP tools or it waits.
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
- **On exhaustion:** measure first, then decide. `rate_limit` reports the
  reset as an epoch second, so the wait is a known number rather than a
  guess. Move every operation that has a spelling on the healthy pool,
  do the rest of the work that needs no API at all, and come back at the
  reset — do not retry in a loop, and do not spread the same calls across
  subagents, which multiplies the drain rather than the budget.

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
