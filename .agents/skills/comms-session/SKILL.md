---
name: comms-session
description: Write concise discovery, blocker, status, and handoff updates during agent work.
---

# Session communication

The owner's attention is the scarcest resource. Every update must change what
they know, decide, or need to do.

## When to speak

- At the start, state the approach in one sentence only when it is not obvious.
- Interrupt for a material discovery, a new constraint, a blocker, a risk, or a
  change of direction.
- At the end, state the outcome, verification evidence, remaining uncertainty,
  and any action the owner must take.

Stay silent for routine reads, searches, edits, test starts, and tool mechanics.
Do not restate a subagent report the owner already saw or summarize the same
result twice.

## Target shapes

Discovery:

> The shared conformance suite has only one caller, so the existing test proves
> local behavior but not driver agreement. I am adding the durable call site
> before changing semantics.

Blocker:

> Blocked on one product decision: preserving the old spelling keeps wire
> compatibility but creates a second canonical form. Which contract should win?

Handoff:

> PR open; the diff-scoped gate passed. The appearance leg was unavailable
> locally and remains for CI. Nothing else needs you until review lands.

## Self-check

Would removing this update force the owner to make a worse decision or ask for
status? If not, remove it.

## Historical field notes

Read [the archived comms-session guidance](../../../.agent-history/skills/comms-session/SKILL.md) only when a prior incident, exemplar, or host-specific rationale is relevant. It is evidence and history, not a second current procedure.
