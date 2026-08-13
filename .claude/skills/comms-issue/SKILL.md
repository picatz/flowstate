---
name: comms-issue
description: Issue bodies, design passes, and decision records
---

Audience: the owner, who decides; and a future agent who arrives cold,
which makes these the highest-handoff-distance surfaces in the repo. Depth
is paid for here. Restatement is not.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

Every issue body and comment a Claude agent posts ends with the attribution
footer; see comms-pr for the exact form and why a compact variant does not
substitute for it there.

Do not hard-wrap an issue body or comment. GitHub renders a single newline
in one as a line break, so wrapped prose arrives ragged; write each
paragraph as one line and let the browser wrap it. comms-pr has the rule
and the reason it differs from a file in the repository.

## Issue body

Problem, desired outcome, constraints, acceptance criteria, evidence.
Distinguish verified fact from hypothesis; a claim about the tree is
checked against main before it is written down (a dispatch prompt once
asserted `switch` was reserved on main when the reservation lived only on
a dead branch).

The exemplar is #482's friction inventory: each item one measured fact and
its consequence:

> Five agents each ran the full `make check` serially on one contended
> machine; wall-clock per gate ran 30-60+ minutes under contention. The
> same list runs on PR CI as seven parallel jobs in about six minutes.

## Sketch the thing, do not only describe it

Prose about a schema is a description of something nobody can see. A
sketch is the thing itself, small enough to read in one screen and
concrete enough to argue with — and arguing with it is the point, since
an issue exists to be critiqued and designed against before anyone
builds. Every issue proposing a capability carries at least one, and
usually the set: the proto message, the Go it generates or the Go that
consumes it, the Flowfile spelling an author would write, and the CLI
session an operator would see.

The sketches must match this project's real semantics even though they
are not real code. Field numbers, `buf` naming, protovalidate rules on
required fields, the `${...}` fence, the actual command names and flag
spellings — a sketch that invents a convention teaches the wrong one, and
someone will implement what it showed. Say once, in a line above the
block, that it is illustrative and not the landed shape. Do not paste a
sketch large enough that the reader skims it; three tight blocks beat one
exhaustive one.

An attribute-schema issue, in the shape #537 wants:

    // Illustrative, not the landed shape.
    message MetricAttribute {
      string key = 1 [(buf.validate.field).string.min_len = 1];
      Cardinality cardinality = 2 [(buf.validate.field).enum.defined_only = true];
      uint32 max_len = 3;
    }

with the Go the consumer actually calls:

    // Recorder drops an attribute the schema does not declare, never the
    // measurement carrying it: a plugin cannot erase an operator's signal.
    func (r *Recorder) Record(ctx context.Context, name string, attrs ...Attribute)

and what the operator types:

    $ flow plugin describe ./bin/deploy --telemetry
    ATTRIBUTE        CARDINALITY   MAX LEN
    deploy.target    bounded       64
    deploy.attempt   bounded       8

Three blocks, three audiences: the schema owner, the caller, the
operator. Whoever reads the issue cold now knows what "declare
attributes in the manifest" means concretely enough to say it is wrong.

## Design pass

Keeps its depth: it is durable and will be executed by a cold reader. But
it does not restate what the issue already carries; it hangs on the issue
and adds only analysis, recommendation, and the questions. End with
numbered questions, recommendation first, so the owner can answer each in
one line (from the #412 pass):

> 1. **Delivery axis**: edition-gated as recommended, or a separate
>    profile-version number? (Edition recommended; one dialect axis.)

## Decision record

The decisions, one line each, plus only what changed from the
recommendation. Not a mirror of the pass. From #418:

> 2. **Compensation unwinds in reverse written order**, landed on
>    `parallel:` first as its own prerequisite PR (design note + tests),
>    before any `async:` code

When the owner accepts as recommended, the line says so and stops. A line
grows only where the decision departs from the recommendation, because the
delta is the only new information (#412's scope decision adds one clause:
the deferred remainder gets a follow-up issue).

## Failure modes

- **The mirror**: a decision comment restating the design pass it decides.
  The reader has the pass one scroll up.
- **The hypothesis in fact's clothing**: tree state asserted from memory
  or from a branch. Verify against main at write time.
- **Carried-context restatement**: a design pass re-summarizing its issue,
  or an issue re-summarizing a linked decision. Link and move on.

## Self-check

Issue: could a cold agent start from this alone? Pass: does every section
add analysis the issue lacks? Decision record: is every line either a
one-line decision or a delta from the recommendation?
