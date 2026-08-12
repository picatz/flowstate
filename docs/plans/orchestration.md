# Orchestration reference

For the session that dispatches agents. Agents do not read this; what an
agent must know goes in its dispatch prompt or in CLAUDE.md.

## Model and effort routing

Route mechanical work to cheaper tiers: reservations, doc regeneration,
corpus sweeps, mirror updates, dependency bumps. Route design passes,
engine and driver work, rewriter changes, and anything touching a safety
invariant to the deeper tier. The split is by consequence of a subtle
mistake, not by diff size: a one-line rewriter change is deep work, a
300-line generated-docs refresh is not.

## Dispatch-prompt checklist

Every dispatch prompt carries:

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
