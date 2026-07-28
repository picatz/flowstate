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

It has a precondition that turned out to be the interesting part, and that is now
enforced (`pkg/flowstate/v1/stepkeys.go`). Once a step names its task directly, a
key on a step means one of two things and the reader tells them apart by asking the
registry — which only works while the two sets are disjoint. They were not.
`TaskManifest.name` in the plugin protocol is validated as `^[a-z][a-z0-9_]*$`, and
every word the step grammar uses matches it, so a plugin advertising a task called
`sleep`, `retry` or `timeout` was legal and registered verbatim. No parser can
recover an author's intent from `timeout: 30s` when both readings are legitimate,
so the constraint belongs at the moment the name is chosen: `Registry.Register`
refuses a reserved name and a misconfigured plugin fails at startup.

The rest of the flattening is not landed. It is a single sweep across the parser,
the marshaller, the language server's positional model and completion tables,
fifteen examples, and roughly three hundred embedded YAML fixtures — and it must
land in one piece, because half a grammar is not a grammar. The parser and
marshaller changes were written and backed out rather than left half-applied; the
work is scoped, not started.

Two findings from that attempt are worth keeping, because both would have been
discovered late:

- **Positions move.** Every diagnostic in a task step shifts two lines and two
  columns, so the position assertions are not incidental to the migration — they
  are most of it, and each one needs reading rather than resetting to whatever the
  new output says.
- **Flow style and cursor markers do not rewrite mechanically.** `task: {name: echo,
  inputs: {...}}` appears in the README and in one LSP fixture, and the completion
  tests place a `|` cursor marker *inside* the shape being flattened, so the marker's
  meaning changes with it. A rewriter must refuse these rather than guess, which is
  also what `flow fix` will have to do.
- **`Task.description` has to go somewhere, and removing it is a reviewed break.**
  The flat form has no place to write one: the value under `http:` is the inputs,
  so a `description` key there would collide with an input of that name. Deleting
  the field is the obvious answer and `buf breaking` refuses it — the repo runs
  FILE as well as WIRE rules, and FILE covers generated Go, so `GetDescription()`
  disappearing is a break even with the number reserved.

  That refusal is correct and should not be worked around by loosening the rule.
  A `reserved 2;` keeps the *wire* safe, which is the property that matters for a
  spec written by an older build; what FILE is protecting is source compatibility
  for embedders, and the honest way to spend it is deliberately, in the commit
  that lands the grammar, rather than quietly in a commit about something else.
  It was removed here as a consequence of the flattening and put back when the
  flattening was backed out.

**One language profile, pinned per run.** Per-step `libs:` means `if:` and `items:`
speak a poorer dialect than a `cel` step, in the same file, for no reason a reader
could infer. One dialect, recorded in the compiled spec.

**Signal payloads under `<id>.payload.*`.** *(landed)* The proposal files this as
ergonomics. It is a security fix and was prioritised as one: a signal sender used to
inject arbitrary names into a step's output namespace, which is the namespace later
expressions resolve against. A sender controlling part of an expression namespace is
an integrity problem, and `timed_out` being unforgeable only because of ordering is
not a defence — it is a defence that holds for exactly the names somebody thought to
write down, and this engine will grow more wait outputs. Rooting sender data under
`payload` makes it unforgeable by grammar rather than by vigilance.

Two things fell out of doing it that the proposal did not name. A wait with no sender
— `sleep`, `wait_until` — gets *no* payload rather than an empty one, because
offering an empty mapping invites `${pause.payload.x}` on a step where one can never
arrive, and the honest answer to that is a diagnostic. And the mapping is built in
sorted order, because it is serialised into `RunState` and carried across every
Continue-As-New: a protobuf map has no iteration order, so two encodings of one
payload could otherwise differ, in persisted workflow state, for no cause a reader
could see.

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
takes the current version at Continue-As-New, and invariant 10 in
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
the plugin protocol's six naming violations, which blocked `buf lint` entering CI.

The second turned out to have a cheaper first move than the one recorded. The six
violations are one rule pair in one file, and the stated fix — moving the plugin
protocol into its own package — is a FILE-level break that has to be spent
deliberately. Meanwhile the *other* forty-one lint rules were going unenforced
while everyone waited for it. They now run in CI, with the pair suppressed for
that one file and nowhere else, so the move gets to be its own reviewed change
and deleting four lines from `proto/buf.yaml` is the whole of what it owes here.

The general shape is worth naming, because it recurs: when one known problem
blocks a check, scope the exception to the problem rather than deferring the
check. A check that runs everywhere except one file is worth vastly more than a
check that does not run.

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
