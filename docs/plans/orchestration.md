# Orchestration reference

> [!NOTE]
> **Internal process, not product documentation.** This file is part of
> `docs/plans/`: how agent work is dispatched here, and what past waves
> measured. Nothing in it describes Flowstate to someone using it — the map of
> the documentation that does is [docs/README.md](../README.md).

For the session that dispatches agents. Agents do not read this; what an
agent must know goes in its dispatch prompt or in CLAUDE.md.

## Model and effort routing

Route mechanical work to cheaper tiers: reservations, doc regeneration,
corpus sweeps, mirror updates, dependency bumps. Route design passes,
engine and driver work, rewriter changes, and anything touching a safety
invariant to the deeper tier. The split is by consequence of a subtle
mistake, not by diff size: a one-line rewriter change is deep work, a
300-line generated-docs refresh is not.

## Effort and token discipline

Slice G of #482 (owner direction, 2026-08-12). Routing is a factory
control, not a preference. Match the tier to the hardest decision in the
slice, never to the file count: a 500-line mechanical sweep is Sonnet
work, a 40-line change to compensation ordering is not.

| Tier | Work |
|---|---|
| Fable | Design passes, architecture decisions, semantics that must be right the first time, adjudicating a P1. |
| Opus | Implementation from a settled spec, review-fix rounds, conflict resolution. The default builder. |
| Sonnet | Mechanical and bounded: keyword reservations, workflow and YAML authoring, doc sweeps, corpus sweeps, fixture additions, dedup and lint fixes, regeneration chores. |

Seven waste sources measured in wave 1, each with the rule it produced:

1. **Model inheritance.** Subagents inherit the orchestrator's model when
   a dispatch omits `model`. Wave 1 ran nine agents at the top tier for
   roughly 1.9M tokens, several of them doing mechanical work. Rule:
   every dispatch sets `model` explicitly; an unset model is a silent
   top-tier spend.
2. **Gate polling.** Agents waiting on a full `make check` spent six
   figures of tokens on monitor and status loops. Rule: run the
   diff-scoped gate, push, and let PR CI be the full gate; never poll a
   long-running gate in a loop.
3. **Duplicate gates.** One agent had two full `make check` runs alive at
   once. Rule: one gate run per branch state; check before starting
   another.
4. **Saturation.** An agent near its context limit loops on its own final
   summary instead of taking follow-ups, and recovery costs a fresh agent
   anyway. Rule: one slice per agent; route post-completion fixes to a
   fresh cheaper agent with the branch as the handoff, and confirm the
   original has stopped before the replacement writes.
5. **Context ballast.** Completed task-board entries are re-injected into
   the orchestrator's context every few turns. Rule: prune the board when
   work merges; the durable record is git and GitHub.
6. **Unbounded reports.** Rule: dispatch briefs state a report budget,
   for example "report in under 200 words: what landed, gate result,
   deviations."
7. **Re-derivation.** Rule: the brief carries the settled decisions, file
   paths, and constraints the agent would otherwise rediscover. A brief
   that costs 500 tokens to write saves tens of thousands.
8. **API polling.** The account's GitHub API budget is shared, hourly, and
   bounded, and it was exhausted twice in one session by PR status
   polling, both times blocking real work. Prefer webhook events over
   polling, never spread the same polling calls across subagents, and
   back off on exhaustion rather than retrying in a loop; see
   comms-review for the full mechanics, including that REST and GraphQL
   meter separately and one can be down while the other still works.

Pace against the weekly window: heavy design work early in the cycle,
mechanical and review-fix work late. When the top-tier budget is spent,
design pauses and building continues, not the reverse.

Route mechanical work to Sonnet rather than the session model. A golden
re-record on 2026-08-12 cost 66k tokens on Sonnet; the identical result on
the session's larger model would have cost several times that for no
difference in output. Run one agent at a time unless the owner explicitly
asks for a wave: subagents share the account's token and API budgets, they
do not get their own, so a wave multiplies the spend against the same
ceiling rather than dividing it.

## Pausing and standing down

Stopping an agent preserves its worktree, so a paused agent resumes from
where it stopped rather than restarting from scratch. Record the worktree
path when you pause one; it is the only thing a resume needs.

Never resume a large-context agent merely to confirm it has stood down. A
completed agent cannot write unless messaged, so it is already silent, and
waking one to ask costs its entire transcript to produce one sentence.
Confirm an agent has stopped by observing that it is not writing, not by
asking it.

## Monitors

Do not arm a monitor for something you will observe directly anyway. A
command you run yourself reports its own exit status; watch it by running
it, not by arming a watcher on it. Monitors exist for events that arrive
from outside the agent, on somebody else's schedule: a webhook, a sibling
session, a scheduled trigger.

One agent on 2026-08-12 armed monitors on its own gate runs and produced
three spurious wake-ups when they timed out after the work had already
finished by other means. If a monitor is armed and its work finishes
another way, cancel it. For the parent: a notification saying a monitor
timed out but the work finished anyway is noise, not a signal, so do not
re-verify state on the strength of one.

## Dispatch-prompt checklist

Every dispatch prompt carries:

- **An explicit `model`.** Never dispatch on inheritance.
- **A report budget**, stated in words and in content.
- **The gate tier expected**: diff-scoped, not full `make check`.
- **Claims verified against main at write time.** A load-bearing assertion
  about the tree ("X is reserved", "Y already lands this") is checked
  against origin/main before it enters the prompt. Wave-1 friction item 6:
  a prompt asserted `switch` was reserved on main; the reservation lived
  only on a dead branch (#482).
- **The drive-to-completion clause.** Never end a turn parked on a
  watcher. When a gate or CI run is pending, keep driving or hand back
  with a concrete status and what happens next. Four of five wave-1
  builders parked mid-gate and needed a nudge each (#482, item 2).
- **Rebase warnings for in-flight sibling PRs.** Name the sibling branches
  that may merge under the agent and the surfaces they touch, so a
  conflict is planned rather than discovered.
- **Worktree isolation by default.** Each agent gets its own worktree; no
  agent edits a file another owns; processes are killed by PID, never by
  pattern.
- **The communication constitution below**, embedded verbatim.

## Fresh agent on saturation

An agent near its context limit loops on its final summary instead of
taking follow-ups. Do not send post-completion fixes to the agent that
built the thing; route them to a fresh agent with the branch as the
handoff. The branch, PR body, and commit messages must therefore suffice
for a cold start, which is what the comms skills enforce.

## Sequencing

When several PRs land in one wave, decide the merge order up front and
tell each agent where it sits: who rebases on whom, and which PR carries
a shared surface (a CLAUDE.md amendment, a schema change) that the others
must not touch in flight.

## Bot review findings

Codex and Copilot review every pull request, and the account's Codex
security-review lane rate-limits under waves (#1352 was found in a sweep
of ~45 merges made while it did). The rule dispatched with every builder,
recorded here so a session does not re-derive it:

- A bot finding is a bug report. Verify it against the tree before
  anything else; the diff is not evidence about itself.
- Real: fix and push. The pushed fix is the reply — no narration, no
  thanks, no restating the finding.
- Wrong: one reply with evidence (file:line, the test that proves it),
  then leave the thread for the merger.
- Resolve only threads you addressed; the merge guard denies a merge with
  any thread unresolved, so an unaddressed thread is a blocked merge, not
  a nit.
- If a bot's findings stop converging — each fix draws a new or reshaped
  one — stop pushing for them and hand the lead what is still flagged.
- A Codex "usage limit" refusal is a review that did not happen, not a
  pass; the lead schedules a security-review skill pass for that PR.

## The communication constitution

Embed this in every dispatch prompt:

> Think as much as the task deserves; publish what the recipient needs.
> Optimize every human-facing artifact for minimum receiver effort at the
> required fidelity. Reasoning depth and publication verbosity are
> separate controls; twenty tool calls do not justify twenty paragraphs.
> Lead with the answer. Never narrate: no file tours, no effort diaries,
> no restating what the reader already has. Fit the surface: a PR body
> explains what the diff cannot; a commit message says what changed and
> why when non-obvious; the pushed fix is the response to a bot finding;
> a decision record is one line per decision plus what changed from the
> recommendation; a session update fires only when the owner's mental
> model should change, never as a progress bar. The literary register
> belongs to durable doctrine and does not leak into PR bodies or review
> replies. Reviews may return PASS; finding-count is not the metric.
