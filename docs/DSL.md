# The Flowstate language: decisions

A design proposal for the DSL exists and is largely right. This is the response to
it: what is accepted, what is changed, and what is refused — with the reasoning, so
that a decision can be revisited on its merits rather than rediscovered.

[docs/ARCHITECTURE.md](ARCHITECTURE.md) is what the system is. [CLAUDE.md](../CLAUDE.md)
is how to change it. This is what the language should become and in what order.

## The bet

One claim is worth more than the rest of the proposal put together, and everything
here is arranged to protect it:

**The language cannot express nondeterminism.**

A hand-written Temporal workflow is deterministic by discipline — every engineer on
every team must know not to read a clock, not to range over a map, not to touch the
network in workflow code, and one lapse corrupts a run that may already be a month
old. Flowstate inverts that. CEL is pure and I/O-free. Every effect is an activity by
grammar. `now` is bound only where a replay-safe clock exists. Replay safety becomes a
property of the engine, verified once by the people who maintain it, rather than a
property of ten thousand files, hoped for individually.

That is the sentence the product is: *your least careful author cannot break replay.*

It is also fragile in a specific way, which the proposal does not state and which is
recorded below as a precondition rather than an open question.

## Accepted, and why

**Rooted ambient names, bare local names** (`inputs.*`, `vars.*`, `steps.<id>.*`,
`run.*`; bare loop bindings, private vars, `now`). This is the strongest idea in the
proposal because it is a deletion. Three separate collision rules and a hand-copied
CEL reserved-word list exist today only because step ids, iterators, and `now` share
one flat namespace. Rooting the ambient half makes collision unrepresentable, and the
rules that guard against it stop needing to exist. Simplifications that remove code
outrank features that add it.

**Verb-key flattening** (`http:` instead of `task: → name: http → inputs:`). Accepted
because of what it *doesn't* touch: it lowers to the existing `Task{name, inputs}`
message. The DSL gets four levels shallower and the wire contract does not move,
which is exactly what proto-first is for.

**One language profile, pinned per run.** Per-step `libs:` means `if:` and `items:`
speak a poorer dialect than a `cel` step, in the same file, for no reason a reader
could infer. One dialect, recorded in the compiled spec.

**Signal payloads under `steps.<id>.payload.*`.** The proposal files this as
ergonomics. It is a security fix and should be prioritised as one: today a signal
sender injects arbitrary names into a step's output namespace, which is the namespace
later expressions resolve against. A sender controlling part of an expression
namespace is an integrity problem, and `timed_out` being unforgeable only because of
ordering is not a defence. Rooting sender data under `payload.*` makes it unforgeable
by grammar.

**Specifying the absent/null/default algebra before shipping it.** Terraform shipped
`optional()` with nested-default semantics left implicit and spent three years on the
consequences. Deciding first costs a paragraph; deciding later costs a major version.

**`flow sim`.** The observation underneath it is real and rare: the state space is the
*definition's*, not arbitrary code's, so a failure-injection matrix over every step ×
every error kind is generated rather than written. Most systems cannot do this at any
price.

## Changed

### No deprecation window. One edition, and a rewriter.

The proposal keeps `task:`/`name:` and the `echo`/`printf`/`cel` tasks working
"for a deprecation window", with old spellings compiling under a diagnostic.

Refused. This is pre-1.0, there is no published catalog or registry through which a
third party could have written against these spellings, and the compiled proto — the actual stable contract, the thing Temporal histories are written
against — is unchanged by the flattening. What a deprecation window would buy is
compatibility for a user population that does not exist yet. What it would cost is
two spellings in the parser, the validator, the LSP table, the marshaller, and every
test matrix that crosses them, for as long as the window lasts — and windows do not
close on schedule.

Instead: the edition boundary is the break, `flow fix --edition` rewrites files
behaviour-preservingly, and the old spellings are *gone* rather than deprecated. A
rewriter that runs in one second is a better migration story than a diagnostic that
lives for a year.

The general rule this repo should hold: **a deprecation is a decision deferred at
everyone else's expense.** Carry compatibility for artifacts that outlive us — the
wire format, compiled specs, running histories. Do not carry it for surface syntax
before anyone has written it.

### The type system is not a later phase

The proposal ships `inputs:`/`outputs:` blocks with `type:` declarations in Phase 1,
and `env.Check` in Phase 2.

That ships a contract surface that lies for a release. `type: string` on an input
nothing checks is decoration, and by this repo's own rule — a capability is not done
until it is reachable, and a check that never runs is the same defect as validation
that cannot be enforced — a declared type with no checker is not a feature.

So the phases are re-cut. Phase 1 is the changes that **delete rules**: the naming
model, verb flattening, one profile, `vars`. None of them need a type system, and all
of them make the language smaller. Phase 2 is the contract surface *with* its
checker — `inputs`, `outputs`, `check:`, and `env.Check` land together or not at all.

This is slower to a demo and faster to something trustworthy.

### `state:` gets a byte bound now, not an open question

The proposal flags a bound on entity `state:` as an open question. It is not one.
`state:` rides in `RunState` through every Continue-As-New, and handler callers
control its growth — an untrusted-growth path, which this repo's rule says gets an
explicit bound at the point the feature lands. The bound is part of the design, and
`state:` does not ship without it.

The same reasoning applies to entity Continue-As-New cadence: driving it from
Temporal's own history-size hint is right, because that is the signal that knows what
it is measuring.

## Refused, or held

**Workflow-side `vars` evaluation is conditional on Worker Versioning.** The proposal
leans toward evaluating `vars` in workflow code, "made safe by language-version
pinning". Pinning the profile is not sufficient and the gap matters: a profile pins
which *functions* exist, not how cel-go *implements* them. A bug fix upstream changes
behaviour under an unchanged profile, and that is precisely a replay divergence in
workflow code — invariant 4, violated by a dependency bump nobody reviewed as one.

What actually pins the implementation is that cel-go is compiled into the interpreter
binary, and the interpreter is pinned per run by Temporal Worker Versioning. So:

> Workflow-side expression evaluation is sound **only** in deployments where Worker
> Versioning pins the interpreter. A deployment without it must evaluate through the
> batched resolution activity.

That is a deployment precondition the language is taking a dependency on. It is
writable down, testable, and fine — but it must be stated, because a silent
dependency on an optional feature is how an invariant dies quietly.

*Since written:* the feature this depends on now exists. `flow worker
--deployment-name --build-id` pins a run to the interpreter it started on and
takes the current version at Continue-As-New, and invariant 9 in
[ARCHITECTURE.md](ARCHITECTURE.md) records what that costs. The precondition is
therefore checkable rather than hypothetical — but it is still a precondition, and
the workflow-side evaluation above must refuse to enable itself on a worker that
has not opted in. A capability that assumes a deployment posture and does not
verify it is the same defect as one that assumes a bound and does not enforce it.

**`flow test` mocks must not see resolved secrets.** The proposal matches mocks with
`where: ${...}`, a CEL predicate over the task's *resolved* inputs. Resolved inputs
are exactly where a `SecretRef` has become a real value. That makes a test file a
place where a secret can be matched on, and a failing assertion a place where one can
be printed — into CI logs, which are broadly readable, which is the same threat model
that put secrets out of workflow history in the first place.

Either mocks match against inputs with references still unresolved, or the `secret:`
taint machinery lands before `flow test` does. This is a design bug in the proposal,
not a detail of implementation.

**Plugins do not get CEL functions.** Accepted from the proposal and restated here
because it will be asked for repeatedly: an IPC call inside a cost-bounded,
replay-sensitive evaluator breaks both properties at once. Complex logic is a task.

## Order of work

Each phase lands green and reachable from a Flowfile.

**Phase 0 — pay the debt that is already found.** No language change. Every item
verified against the code before it is worked, because a diagnosis is a claim.
Currently confirmed: nineteen unenforceable schema rules masking a real hole (fixed);
the plugin protocol's six naming violations, which block `buf lint` entering CI.

**Phase 1 — make the language smaller.** The naming model, verb-key flattening, one
pinned profile, `vars` in its three positions, `edition:`, reserved-keyword
diagnostics for the grammar Phase 4 will need, `flow fix --edition`. Everything here
removes a rule or a spelling.

**Phase 2 — the contract, with its checker.** `inputs`, `outputs`, `check:`,
`env.Check` at validate and in the LSP, typed hover, the absent/null/default matrix,
`secret:` taint. One phase, because half of it is decoration without the other half.

**Phase 3 — composition and the dev loop.** `call:` to child workflows, cross-file
signature checking, `flow dev`, `flow test` (after the taint work above).

**Phase 4 — the substrate.** Handlers, schedules, priority and fairness, heartbeats,
queues, `undo:` compensation, search attributes, Nexus consume.

**Phase 5 — the living workflow.** `state:` and entities (with their bound),
`flow migrate` with frame-checked proofs, `flow sim`, `flow prove`, catalogs, Nexus
publish.

## The standing rule

Every claim in a design document about what this codebase currently does is a claim,
and claims about this codebase have been wrong before — a handoff's confident
diagnosis of a test flake was wrong, and a CI comment's count of lint violations was
low by nineteen. Verify before building on it. A refuted premise is worth as much as
a confirmed one, and both are worth more than the time spent implementing against a
diagnosis nobody checked.
