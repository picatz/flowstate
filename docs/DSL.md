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
`run.*`; bare loop bindings, private vars, `now`). Accepted, and the `steps.` half has
landed: a step's outputs are `${steps.<id>.<output>}`, and what stays bare is what is
bound *where the expression is written* — a `for_each` iterator, `now` inside
`wait_until:`, and the names a task resolves against its own scope (`status_code`,
`headers`, `body`, and — when the step asked for `parse_json` — `json`, in the `http`
task's `expect:` and `outputs:`). `inputs.*`, `vars.*` and `run.*` do not exist; see
[Order of work](#order-of-work).

*Since written:* the reason this section gave for accepting it was checked against the
code and was half wrong, so it is replaced here rather than left to be rediscovered.

**It is not a deletion.** The claim was that three collision rules and a hand-copied CEL
reserved-word list exist only because step ids, iterators and `now` share one flat
namespace. There were **five** rules, and rooting deletes two:

| Rule | Fate |
| --- | --- |
| a step id may not be a CEL reserved word | narrowed — 21 words to 4 |
| a step id may not be `now` | deleted |
| an iterator may not be a CEL reserved word | untouched |
| an iterator may not be a step id | deleted |
| an iterator may not be `now` | untouched |

The two left intact are the two about the iterator, and that is not tidying left for
later. An iterator stays bare *by this proposal's own design*, so it and `now` genuinely
do share one namespace and the collision between them is unaffected — a loop variable
called `now` is still refused, because a body saying `${now}` would mean the item
everywhere except inside a `wait_until:`, where the clock is bound on top and wins.
Rooting makes a collision unrepresentable **between** the two halves, never within one.

**The reserved-word list survives in full**, for the same reason: it guards the iterator,
which is still written as an identifier. What the change added there is a *second*,
smaller list — `celUnusableStepIDs`, four words — so `validate.go` now carries two lists
where it carried one. cel-go refuses a reserved word in identifier position only, and
`steps.<id>` is a field selection, so seventeen of the twenty-one became legal step ids
the moment references were rooted. The four that did not are refused a level lower, by
the lexer: `true`, `false` and `null` are literals and `in` is an operator, so `steps.in`
is a syntax error in the grammar itself. `in` is the easy one to miss, and missing it is
not harmless — the step would compile and every reference to it would fail to *parse*,
sending the author to an expression rather than to the id, which is exactly the failure
the list exists to prevent.

Net: five rules become three and a half, and the change is additive in code — +1166/−214
lines of non-test Go across `validate.go`, `eval.go`, `flowfile/fix.go`, the new
`flowfile/fixexpr.go`, and the language server. "Simplifications that remove code outrank
features that add it" is still the right rule. This is not an instance of it.

**It is still worth doing, on the argument [ARCHITECTURE.md](ARCHITECTURE.md) invariant 2
already makes:** attributes grouped under an object beat attributes declared flat, because
grouping is what stops a plausible name colliding with a reserved one. That was written
about policy environments, and the DSL is now its worked example. What an author gets is
concrete, and all of it was verified in one file: a step called `name` alongside an
iterator called `name`, a step called `loop`, a step called `now`, and a
`wait_until: ${now + seconds(1)}` in the same workflow that still reads the clock rather
than the step.

`flow fix` performs the migration — it roots bare references in place, in the same pass as
the `task:` rewrite — and migrated all sixteen shipped examples itself. Until it is run,
`flow validate` reports a bare reference as the migration it is rather than as an unknown
name:

```
workflow.yaml:8:16: step "b" input "message": `a` is a step, and a step is named `steps.a` now; run `flow fix` to rewrite this file
```

Recording all of this is an instance of [the standing rule](#the-standing-rule) below,
which is why the original paragraph is replaced instead of patched: a design document's
count of what exists is a claim, and this one was short by two rules in one direction and
a whole surviving list in the other. The decision survived the correction; the argument
for it did not.

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

*Since written:* the rest of it landed, in one sweep across the parser, the
marshaller, the language server's positional model and completion tables, every
example, and the embedded YAML fixtures — in one piece, because half a grammar is
not a grammar.

Three findings from the attempt that preceded it turned out to be the whole cost,
and all three would otherwise have been discovered late:

- **Positions move.** Every diagnostic in a task step shifts two lines and two
  columns, so the position assertions are not incidental to the migration — they
  are most of it, and each one needs reading rather than resetting to whatever the
  new output says.
- **Flow style and cursor markers do not rewrite mechanically.** `task: {name: echo,
  inputs: {...}}` appeared in the README and in one LSP fixture, and the completion
  tests place a `|` cursor marker *inside* the shape being flattened, so the marker's
  meaning changes with it. A rewriter must refuse these rather than guess, which is
  what `flow fix` does.
- **`Task.description` had to go somewhere, and the answer was not deletion.**
  The flat form has no place to write one: the value under `http:` is the inputs,
  so a `description` key there would collide with an input of that name. Deleting
  the field is the obvious answer and `buf breaking` refuses it — the repo runs
  FILE as well as WIRE rules, and FILE covers generated Go, so `GetDescription()`
  disappearing is a break even with the number reserved.

  That refusal is correct and was not worked around by loosening the rule. A
  `reserved 2;` keeps the *wire* safe, which is the property that matters for a
  spec written by an older build; what FILE is protecting is source compatibility
  for embedders, and that is spent deliberately or not at all.

  What landed instead is `Node.description`: prose about a step, written directly
  under `id:`. That is the better answer regardless of the break, because the step
  is what an author was describing either way — and a `for_each` or a `sleep` is as
  worth explaining as a task is, which a field on `Task` could never serve.

  *Since written:* the break has been spent. `Task.description` is gone, with
  `reserved 2` and `reserved "description"` in its place, bundled into one commit
  with the plugin protocol's move to its own package so that a single release
  carries both FILE breaks rather than two carrying one each. Verified as a FILE
  break and *not* a WIRE one — a `breaking` run configured with `WIRE` alone and no
  suppression at all passes, which is the property that matters for a spec an older
  build wrote and a worker may still be replaying.

**One language profile, pinned per run.** *(landed)* Per-step `libs:` meant `if:` and
`items:` spoke a poorer dialect than a `cel` step, in the same file, for no reason a
reader could infer. One dialect now, named by `Workflow.profile` and recorded in the
compiled spec, so a worker evaluates a run against the vocabulary it was compiled with
rather than whatever that worker calls current. `libs:` is deleted; the key is reported
as retired, which it has to be, because `cel` binds an unrecognised input as a
*variable* — silence there would have turned a leftover `libs:` into a binding nobody
reads.

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
offering an empty mapping invites `${steps.pause.payload.x}` on a step where one can never
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

Instead: the edition boundary is the break, a rewriter moves files across it
behaviour-preservingly, and the old spellings are *gone* rather than deprecated. A
rewriter that runs in one second is a better migration story than a diagnostic that
lives for a year.

*Since written:* both halves exist — `edition:` in `flowfile/edition.go`, the rewriter
in `flowfile/fix.go` and `cmd/flow/fix.go`, with `examples/edition-and-descriptions`
carrying the explanation an author meets first. Building them settled four things this
section left open:

- **The edition is optional, and absent means the current one.** Requiring it would
  put a line of ceremony at the top of every file to say the only thing it can
  currently say, and a file that does not care which grammar it is in is the common
  case. What writing one buys is a refusal instead of a silent reinterpretation, which
  is worth having and not worth mandating.
- **`flow fix` does not stamp an edition onto a file that lacks one.** A file with no
  `edition:` has not asked to be pinned, and adding one would be the rewriter holding
  an opinion the author did not. A marker that *is* written and is recognised does get
  brought forward — otherwise the diagnostic saying "run `flow fix`" would name a
  command that leaves the cause of the diagnostic in place.
- **The command is `flow fix`, with no `--edition` flag.** The name proposed here
  described the migration by the boundary it crosses, but a build has one grammar and
  there is nothing else to rewrite a file *to*. A flag naming a target edition would
  advertise a choice that cannot exist.
- **An edition is a property of a file and stops there.** Declaring an older one does
  not make an older grammar compile; carrying two grammars is the cost this section
  refused. So an edition this build does not know is refused rather than translated,
  and the schema has no field for it — no workload already running is touched by any
  of this, because the stable contract is the compiled spec and not the source.

Two implementation surprises are worth recording. A dated edition is a YAML float:
`edition: 2026.1` arrives as a number, so the token's own source text is read rather
than the value — converting would make `2026.10` and `2026.1` the same edition, and the
tenth of a year would compile as the first. And the rewriter is deliberately not
parse-then-marshal: the retired grammar no longer parses, which is the entire point, and
a formatter would move every comment and requote every string in a diff that is supposed
to be about one key. It edits the lines it must, copies the rest through byte for byte —
so a file with nothing to change comes back identical, which is what makes running it
over a directory safe — and refuses the shapes it would have to guess at: flow style,
which has no line structure to rewrite, and a task standing behind a YAML alias, which
has no name to rewrite it to. A refusal exits non-zero, as does `--check` finding work,
so `flow fix . && git commit` cannot succeed over a file still written in a spelling
nothing compiles.

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
diagnostics for the grammar Phase 4 will need, and a rewriter across the edition
boundary.

Done: signal payloads under `payload`; verb-key flattening, with the execution notes
it produced kept below; `edition:`, optional and refusing rather than translating; and
the rewriter, which is `flow fix` and takes no `--edition`. Prose about a step landed
alongside the flattening as `description:`, since that is where the flattening left it
with nowhere to go.

Done, of the naming model — and only this much of it: step outputs are rooted as
`${steps.<id>.<output>}` with locals staying bare, the diagnostic names the migration and
the command that performs it, `flow fix` roots references in place, all sixteen examples
are migrated, and the language server completes a reference in three levels and hovers a
rooted one.

**`run.*`, `inputs.*` and `vars.*` are not started.** They do not exist anywhere: no `run`
object in any scope, no `inputs:` block, and no `vars:` in any of its three positions —
`flow validate` answers `unknown key "inputs"` and `references unknown name "run"`. It is
worth stating plainly, because the naming-model item reads as one change and is not: step
outputs were a namespace the language already had, and rooting only gave it a root. Each
of those three is a *new* namespace with a new source of values, so they are three
features rather than the remainder of this one, and none of them is bought by the work
above. (The `cel` task's `vars` input is unrelated — a task input, not a scope.)

Remaining in Phase 1: `vars` in its three positions, one pinned profile, and the
reserved-keyword diagnostics. Pinning the profile is the only Phase 1 item that needs a
schema break, so it goes last rather than first. `inputs` is not a Phase 1 item at all; it
is Phase 2 and lands with its checker or not at all, per
[The type system is not a later phase](#the-type-system-is-not-a-later-phase).

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

## Executing the flattening

*This landed.* It was written down when the change had been attempted once and backed
out, because the reasons were not the ones expected; it is kept because those reasons
held, and because the next grammar change will have the same shape.

**It touches no proto.** `http:` with the request under it lowers to the same
`Task{name, inputs}`, so `buf breaking` has nothing to say about it. That makes it
the *cheapest* break in Phase 1, not the most expensive — the one that needs a
deliberate schema break is pinning the language profile, which deletes the `libs`
input. Sequence accordingly.

*Since written:* both landed, in that order — the profile first, then the deletion,
because deleting `libs` before every expression reached every library would have taken
capability away from `cel` steps rather than given it to everything else.

**Its precondition is already enforced.** A step key is a property or a task name,
told apart by asking the registry, which only works while the two sets are
disjoint — and `Registry.Register` now refuses a task named for the grammar. That
was the part that would have made the grammar genuinely ambiguous.

**The parser and marshaller are the small half.** Roughly forty lines: `stepKeys`
and `stepKindKeys` become functions over `v1.TaskNames()`, the kind switch grows a
default arm that reads the key as a task name, `task()` loses its `name`/`inputs`
lookup, and `stepToYAML` emits the task's name as the key. Both were written and
worked.

**The assertions are the large half, and they are the risk.** Every diagnostic
inside a task step moves two lines and two columns. That makes ~59 of them fail at
once, and resetting them to whatever the new output prints is how a real
regression gets absorbed into a migration diff. Read each one. The good news is
that they are not uniformly fragile:

- `flowfile/parse_test.go` asserts line and column numbers directly. These are the
  ones that need reading.
- The language server's tests mostly assert the *text* a diagnostic underlines
  (`underlines: "nope"`), which survives the move untouched. Do not assume the LSP
  is the hard part; it is mostly not.

**Two fixture shapes do not rewrite mechanically.** Flow style — `task: {name:
echo, inputs: {...}}` — appeared in the README and one LSP fixture. And the
completion tests place a `|` cursor marker *inside* the shape being flattened, so
the marker's meaning moves with it. A rewriter must refuse both rather than guess,
which is what `flow fix` does; a script that silently mangles them is worse than one
that stops.

**`Task.description` has nowhere to live afterwards**, since the value under
`http:` is the inputs and a `description` key there would collide with an input of
that name. Prose moved to the step instead — `Node.description`, a property every
kind of step has — which is a schema addition rather than the removal this
anticipated. The removal has since been spent, together with the plugin protocol's
move, so one release carries both FILE breaks instead of two carrying one each.

**Order that keeps every step green:** parser and marshaller together (they are
inverses and the round-trip tests cover both); then examples; then `flowfile`'s own
tests, reading each position; then the language server; then the README.

The one place the plan and the outcome part company is sequencing. It said `edition:`
belonged in the same change, on the grounds that a boundary nothing crosses is
scaffolding, and that the examples should be migrated by the rewriter so it was
exercised on real files first. What happened is that the grammar landed alone and the
edition and the rewriter followed one commit later — which left the retired spelling
uncompilable with no migration for a day, the gap the no-deprecation rule is least able
to afford. The property the plan was reaching for was recovered a different way:
`TestFixRoundTripsEveryExample` un-flattens every shipped example with a separately
written, deliberately naive inverse, checks that the result no longer compiles, and
requires the rewriter to reproduce the original byte for byte. That is stronger than
migrating the examples once, because it re-runs on every example added since.

## The standing rule

Every claim in a design document about what this codebase currently does is a claim,
and claims about this codebase have been wrong before — a handoff's confident
diagnosis of a test flake was wrong, and a CI comment's count of lint violations was
low by nineteen. Verify before building on it. A refuted premise is worth as much as
a confirmed one, and both are worth more than the time spent implementing against a
diagnosis nobody checked.
