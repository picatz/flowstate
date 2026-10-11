---
name: flowstate-delegate
description: Use when choosing which model, effort level, or subagent should do a Flowstate task, before delegating work, escalating a stuck problem, or spending on a review.
---

# Delegating by model and effort

Spend follows the cost of being wrong, not the size of the task. Every turn
resends the conversation, and a subagent pays for its own tokens, so
delegating saves money only when the work is read-heavy or output-light, or
when it keeps bulk output out of the lead's context.

## Pick the model

| Work | Model |
| --- | --- |
| Lookups, "where is this defined", CI and log triage (`flowstate-scout`) | Haiku |
| A well-scoped slice with a runnable check, tests, verification, review | Sonnet |
| Planning, multi-file design, final judgment, any `AGENTS.md` invariant | Opus (the lead) |
| A design fork with no precedent, or a problem Opus has failed twice | Fable, as an advisor |

- The lead is Opus. A Sonnet lead suits a thread whose spec and proving
  command are already written. Choose the lead at session start: switching it
  mid-session breaks the prompt cache, while changing effort does not.
  Subagents are separate contexts, so their model costs the lead nothing.
- A subagent's model comes from its definition: `flowstate-scout` is Haiku,
  `flowstate-verifier`, `flowstate-pr-tidy` and `flowstate-reviewer` are
  Sonnet. Override one call with the Agent tool's `model` parameter.
- Escalate on evidence: when Opus fails the same problem twice, ask Fable once
  with the question, the evidence, and the options already ruled out, then
  return to Opus. Fable advises on a fork; it does not draft or implement.
- Keep code edits on the model that holds the design. Hand a cheaper model an
  edit only when the spec is written and a command proves it.
- When a cheaper model fails a kind of task, move that kind up a tier.

## Pick the effort

Medium is the default for the lead; raise it to high for work on an invariant
and for verification of edge cases. Effort fixes missed edge cases, not a wrong
approach: a wrong approach calls for a better spec or a higher tier. Each
subagent role carries its own model and effort, pinned in its definition:

| Role | Model | Effort |
| --- | --- | --- |
| `flowstate-scout` | haiku | low |
| `flowstate-verifier` | sonnet | low |
| `flowstate-pr-tidy` | sonnet | low |
| `flowstate-reviewer` | sonnet | high |

The reviewer runs high because thoroughness is the product and its report is
small.

## Keep context cheap

Every turn resends the thread, so the long thread is the real cost. One
thread per pull request, `/clear` between tasks, compact at a milestone, and
push logs, test output, and review reading into subagents so only the
conclusion returns.

## Brief the subagent

- State the goal, what is ruled out, the files that matter, and the command
  whose result proves the work. A subagent starts cold.
- Give a reviewer the diff, not your conclusion, and tell it to report only
  gaps that affect correctness or a stated requirement.
- Ask for a bounded report. A claim in it is a lead to check, not evidence:
  compare any revision it names with the real head.
- Do not run what you delegated, and do not fan out agents whose work
  overlaps; agent teams cost several times a single session.

## Review by risk

Every merge keeps its one exact-head review; risk sets the reviewer, never
whether it happens. Default `flowstate-reviewer` (Sonnet). A change to
authentication, policy, secrets, durability, tenancy, or egress runs it with
`model: opus` and `flowstate-security-review`, then an adversarial pass. A
docs or comment change is a short review, not a skipped one. Any push after a
review needs a verdict on the new head; a fix that changes only formatting or
wording gets a short delta review, a logic change a full one.

## Unattended runs

Read from stored state rather than a fixed window, report a failed read as
unreadable rather than quiet, count an action done only on confirmation, and
cap the spend at a few times a normal run.
