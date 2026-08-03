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

## The constitution

The bet is the first principle of a set this document had been carrying implicitly,
one decision at a time. They are hoisted here so a proposal can be checked against
them before it is argued on its merits — most of what [the second
round](#the-second-round-vocabulary-expressions-extension) refuses is refused by one
of these.

1. **The language cannot express nondeterminism.** Every effect is an activity by
   grammar; expressions are pure and I/O-free.
2. **A value is not an effect.** Literals and expressions produce data. Anything
   that touches the world outside evaluation is a node kind or a task, and nothing
   that stays inside it is either.
3. **Expressions are explicit.** A reader tells data from code without consulting a
   registry: where a field can hold either, the `${...}` fence decides, and nothing
   is ever evaluated because it happens to parse.
4. **One dialect per file, pinned per run.**
5. **Author-chosen names stay bare; system-chosen names are rooted.** An author can
   see every name they bound; nobody can see the names a future engine will inject.
   `now` is the single blessed exception and pays for itself in standing guard
   rules.
6. **YAML is an authoring projection.** The compiled proto is the contract, and
   anything a Flowfile can say, another surface — a form, an API, an agent — says by
   producing the same proto.
7. **Fail closed.** Deny beats allow, an errored rule denies, and capability is
   granted by deployment posture rather than assumed by vocabulary.
8. **Secrets are references until the activity that needs the value.**
9. **Anything an outside party can grow has an explicit bound**, matched to the
   resource that party controls.
10. **Fewer orthogonal primitives beat many convenient ones.** A capability earns a
    word by being inexpressible without one.
11. **Surface syntax breaks at editions, with a rewriter.** Compiled specs and
    running histories never break.
12. **Built-ins and plugins describe themselves through one metadata contract**, and
    every surface — validator, language server, CLI, UI, agent — derives from it
    rather than keeping a copy.

## Accepted, and why

**Rooted ambient names, bare local names** (`inputs.*`, `vars.*`, `steps.<id>.*`,
`run.*`; bare loop bindings, private vars, `now`). Accepted, and the `steps.` half has
landed: a step's outputs are `${steps.<id>.<output>}`, and what stays bare is what is
bound *where the expression is written* — a `for_each` iterator, `now` inside
`wait_until:`. The names a task resolves against its own scope were bare when this was
written and are not any more: the `http` task's `expect:` and `outputs:` reach the
response under a root of its own, `${response.status_code}`, `response.headers`,
`response.body` and — when the step asked for `parse_json` — `response.json`, per the
disposition table below. `vars.*` has since landed too, in both of its positions;
`inputs.*` and `run.*` do not exist; see [Order of work](#order-of-work).

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

*A correction, on where a profile actually takes effect.* "A worker evaluates a run
against the vocabulary it was compiled with" is true of the profile's *functions* and
was false of its *macros*, and the difference is not a detail of implementation — it
is where in the pipeline each is resolved. A function is looked up while an expression
evaluates, so recording the profile name in the spec is what pins it. A macro is
expanded by the **parser**, so it is settled when the file compiles, and a compiler
that has not been told which libraries the profile has does not expand one at all: it
stores an ordinary call on an identifier nothing binds.

Which is what it did. Flowfile expressions were parsed against a bare environment, so
every macro the profile adds — `cel.bind`, all six two-variable comprehensions,
`sortBy`, `math.least` and `math.greatest`, `optMap`, `optFlatMap`, `proto.getExt` and
`proto.hasExt` — could be written, could be validated, and could never run.
`${math.greatest(1, 2)}` died with `no such attribute(s): math`, and
`${[3,1,2].sortBy(v, -v)}` was refused by the validator as a reference to an unknown
name `v`, which is the macro's own bound variable. Only cel-go's standard macros
worked, because those are in its default environment rather than in a library — so
`filter` worked and `transformList` did not, which reads like a fact about two
functions rather than about where either is declared.

Two consequences worth keeping in view now that they are expanded. A run carries the
expansion rather than the spelling, so what a spec pins for a macro is stronger than
what it pins for a function: the meaning is frozen at compile time and no later worker
can reinterpret it. And a compiled expression records the call it came from, so
`flow fix` can still write the file back — without that, an expanded `math.greatest`
would have been rewritten as `math.@max`, which is not a spelling anybody can write.
That recording costs about 25% of an expression's encoded size where a macro is used,
and 1.7% across the shipped corpus; it also repaired `flow fix`, which until now
refused outright to write back any file containing `filter` or `map`.

*A second correction, on the one position that spoke a different dialect.* Retiring
`libs:` removed the case where one step spoke a **richer** dialect than the rest of
the file. It did not remove the mirror image: the http task evaluated its own two
deferred inputs, `outputs:` and `expect:`, against an environment holding the response
root and the json library and nothing else. So `${response.body.upperAscii()}` — a
function that works in a `vars:` binding, an `if:`, `items:`, `wait_until:` and every
other task input — failed inside `outputs:`, after the request had been made, since
that is when the expression runs.

It survived the retirement because nothing in the grammar mentions it: `libs:` was a
key an author could see, and this was a `cel.NewEnv` call in a task. One dialect means
these two positions as well, and it is the profile's environment now, resolved from
the scope rather than from what the running build calls current.

Both of these were found by consequences rather than by review of the section they
belong to: the macro gap by an evaluation count taken for something else, the dialect
split by a type checker disagreeing with the runtime. The pattern is worth naming — a
claim about "one language" is not verified by any test that only ever writes the
language one way, and the positions this page kept getting wrong are exactly the ones
no example happened to exercise.

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

*Since written:* the first two of those four are reversed by [explicit
versions](#versioning-one-scheme-for-everything-that-can-break). The edition becomes
required, and `flow fix` stamps a missing one — which stops being the rewriter
holding an opinion the moment the language requires the line. The second two stand.

Two implementation surprises are worth recording. A dated edition is a YAML float:
`edition: 2026.1` arrives as a number, so the token's own source text is read rather
than the value — converting would make `2026.10` and `2026.1` the same edition, and the
tenth of a year would compile as the first. (*Since written:* the `v` prefix adopted
with [explicit versions](#versioning-one-scheme-for-everything-that-can-break) retires
this hack structurally — `v2026.1` is a plain string to YAML, and the float reading is
unrepresentable.) And the rewriter is deliberately not
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

### The precondition, and how it is enforced

The section above reads as though workflow-side evaluation were a proposal still to
be gated. It is not: **only** the workflow's own `vars:` block goes through an
activity, and everything else already evaluates CEL inline in workflow code — step
conditions, a loop's `items:`, a step's own `vars:`, and every task input that does
not declare `needs_prev_outputs`. The executor holds a `workflow.Context` and calls
the evaluator directly (`engine/execute.go`). So the precondition is not a condition
on a future feature. It is a condition on every worker running today.

Worker Deployment Versioning is opt-in and Temporal's default is unversioned, which
for a long time meant the shipped default did not meet the precondition and nothing
said so — `flow worker` warned, and a warning is not a gate. It is a gate now:

    # Pinned: a run finishes on the interpreter it started on, and takes the
    # current version at continue-as-new.
    flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"

    # Unpinned, accepted out loud.
    flow worker --allow-unversioned-interpreter

Without one of those two, `flow worker` refuses to start, and the refusal names both
of them. Three properties of that gate are deliberate.

**The escape hatch accepts a risk rather than silencing a check.**
`--allow-unversioned-interpreter` is named for the exposure it takes on, in the same
spirit as `--allow-insecure-plugin-dir` and `--insecure-no-auth`: a reader of the
command line can see what was accepted without reading the code that enforced it.
The worker still warns on every start, because the person reading a worker's logs a
month later is usually not the person who wrote its command line.

**Half a version is an error, not a fallback.** Setting `--deployment-name` without
`--build-id` (or the reverse) used to drop silently to unversioned — an operator who
asked for the guarantee, did not get it, and was not told, which is a fail-open on
the exact posture this section is about. `engine.DeploymentOptions` now names the
missing half and the command stops. `--allow-unversioned-interpreter` does not
rescue it either: half a version is a mistake in a command line, not a posture
anyone chose.

**Invariant 8 is kept by the flag, not by an exemption.** Invariant 8 forbids a
*cloud* prerequisite — "no cloud dependency and no external identity provider" — and
Worker Deployment Versioning is not one:
`TestAPinnedRunTakesTheCurrentVersionAtContinueAsNew` runs two versioned builds
against `testsuite.StartDevServer`, which *is* `temporal server start-dev`, and moves
a suspended run between them. So both answers are available on a laptop, and what
invariant 8 actually rules out is a dead end. The refusal is therefore written as a
signpost: it says what to type, and typing it is the whole of the fix.

Detecting the dev server and exempting it was the other candidate, and it was
rejected for being a guess. A dev server's address is configurable, a production
cluster can be reached at localhost through a tunnel, and a rule that decides how
much safety to enforce by pattern-matching a hostname fails open on precisely the
deployment that most needs it.

Gating is therefore settled. What it does not settle is what an operator who will
not run versioned workers should get, and that is where the second option lives.

The remaining option — routing every evaluation through an activity — has a cost
worth having a number for, since "a round trip per condition" is the kind of estimate
that gets repeated without being checked.

Counted at `Evaluator.Eval` (`celenv.go`), which is where a compiled program is
actually run and where every path — a condition, a loop's `items:`, a step's `vars:`,
a task's inputs — arrives. Counting the entry points instead does not work, and the
first version of this table did exactly that: `EvalConditionInScope` on a step with no
`if:` returns without evaluating anything, so a call to it is not an evaluation.
Counted that way `hello-world` reported 2 and contains no expressions at all, every
row was inflated two- to four-fold, and `ResolveItems` — a loop's `items:`, the one
expression whose cost multiplies — was not among the three entry points and so was
missed entirely. A count of the places evaluation is *asked for* is not a count of
evaluation.

Over the shipped corpus, per run:

| workflow | evaluations | of which workflow `vars:` |
|---|---|---|
| `hello-world` | 0 | 0 |
| `http-form`, `simple-http-multi-step` | 1 | 0 |
| `hello-world-multi-step`, `http-output-shaping` | 2 | 0 |
| `string-formatting` | 2 | 1 |
| `conditional-and-retry`, `http-json` | 3 | 0 |
| `headers-and-nested` | 3 | 1 |
| `expressions`, `http-expect` | 4 | 0 |
| `approval-gate` | 5 | 0 |
| `wait-until-a-moment` | 5 | 1 |
| `logging` | 6 | 0 |
| `edition-and-descriptions`, `fan-out-and-parallel`, `workflow-vars` | 6 | 1 |
| `step-vars` | 11 | 0 |

The second column is subtracted rather than added: a workflow's `vars:` block is
*already* an activity on the durable driver (`engine.WorkflowVars`), so those
evaluations would not become new round trips. It is broken out because the numbers
come from `flow run local`, which evaluates that block inline.

Two other things follow from measuring through the local driver, and both were
checked rather than assumed. The two drivers reach the same four sites —
`EvalConditionInScope`, `EvalStepVars`, `ResolveTaskInputs`, `ResolveItems` — so the
set being counted is the right one. But a retried step re-resolves its inputs locally
(`runStepWithPolicy` calls `EvalInScope` per attempt) where the durable driver
resolves once before `ExecuteActivity` and never returns to workflow code between
attempts: measured, a one-expression step retried three times counts 3 locally and
would be 1 there. No corpus workflow retries a step carrying an expression, so no row
above is affected — but the local count is an upper bound, not an equal one.

None of which decides it, because the corpus is not what this costs on. A loop is,
because the body is evaluated once per item. Measured over a one-step body with a
condition, a `vars:` entry, and one task input:

| items | evaluations |
|---|---|
| 1 | 4 |
| 10 | 31 |
| 50 | 151 |
| 200 | 601 |

Which is `3n + 1`: three per iteration — the condition, the step's `vars:`, and the
task's inputs — over one for the `items:` expression itself, evaluated once. Every
task is already an activity (`engine.Task` or `TaskInScope`), so a 200-item loop over
a one-step body schedules 200 activities today and would schedule 801: four times the
activities for the same work.

That is the real shape of the cost, and it lands on history size rather than only on
latency: Temporal records every activity, and history size is what forces
Continue-As-New in the first place. The option that looks safest for determinism is
the one that most increases how often a run has to suspend.

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

## The second round: vocabulary, expressions, extension

A second proposal arrived — two, in fact, from different reviewers, converging
independently on the same complaints — about the task vocabulary (`cel`, `echo`,
`printf`), the expression marker, and how plugins should appear in the grammar. This
section is the response, in the same shape as the first: accepted, changed, refused,
with reasoning. Claims about current behaviour below were verified against the code
and by running Flowfiles through `flow validate` and `flow run local`, per [the
standing rule](#the-standing-rule).

### The admission test for a built-in

The roster was being decided task by task, which is how it came to hold an identity
function. A step's key belongs to one of three categories, and the category decides
where a capability lives:

- **Values** — literals and expressions. Pure, replay-safe, engine-evaluated. Never
  a task, because a task is an activity and evaluating a pure expression as an
  effect is a category error against principles 1 and 2.
- **Control flow** — `if:`, `for_each`, `parallel`, `sleep`, `wait_until`,
  `wait_for_signal`, later `call:`. Node kinds in the schema. Some evaluate
  expressions; that does not make them tasks either.
- **Effects** — the registry. A task earns a place in the *built-in* registry only
  by passing all four of:

  1. **Vendor-free and credential-free.** Nothing that names a product.
  2. **Its danger is governed by engine policy.** `http` qualifies *because of*
     `netpolicy`. A task whose risk the engine cannot govern does not belong inside
     the trust boundary.
  3. **Universally reimplemented otherwise.** If every second plugin would ship its
     own copy, centralize it; if only some would, don't.
  4. **It is actually an effect.**

Everything people will ask for that is really control — retries, gates, timeouts,
fan-out — stays grammar, which is the moat: elsewhere retry logic is a marketplace
action, here it is a property of every step because the engine owns it.

### `cel:` is retired — not renamed *(landed)*

The name is wrong the way `re2:` would be wrong where `regexp` is meant: it answers
"which evaluator?" when the author is asking "what role does this value play?". But
renaming it to `expr:` or `eval:` would treat the symptom. The task fails the
admission test's fourth condition: evaluating a pure expression is not an effect,
and paying an activity round trip plus a history entry to compute what replay could
recompute for free is the grammar miscategorizing its own bet.

What the task actually serves is three needs, each with a better home:

| Need | Home |
| --- | --- |
| name an intermediate value | `vars:` — workflow-level and step-level |
| derive data at the point of use | inline `${...}`, which works everywhere today |
| shape a final result | the workflow `outputs:` contract (Phase 2) |

So `cel:` is deleted at the same edition boundary that lands `vars:`, and not
before — retiring it first would take capability away. Before and after:

```yaml
# before
steps:
  - id: greet
    echo:
      message: hello
  - id: modify
    cel:
      expr: "regex.replace(vars.greeting.upperAscii(), 'HELLO', 'HI')"
      vars:
        greeting: ${steps.greet.result}
  - id: shout
    if: ${steps.modify.result.startsWith('HI')}
    echo:
      message: ${steps.modify.result.lowerAscii()}
```

```yaml
# after
vars:
  greeting: hello
steps:
  - id: shout
    vars:
      modified: ${regex.replace(vars.greeting.upperAscii(), 'HELLO', 'HI')}
    if: ${modified.startsWith('HI')}
    log:
      message: ${modified.lowerAscii()}
```

Three steps and two activity round trips become one step and one; the `cel` task's
private `vars` input — a compatibility shim the parser already documents as one —
disappears along with the confusion of two things named `vars`.

*Since written:* landed at v2026.2, and the "after" block above is one line wrong, which
is worth leaving visible rather than quietly correcting. **A step's own `vars:` are not in
scope for that step's `if:`.** The condition decides whether the step runs, so at the
moment it is asked the step's bindings do not exist yet — `flow validate` says
`references unknown name "modified"` and names the three things a bare name can be. The
shape that compiles either lifts the binding to the workflow, or writes the condition
against what the binding was derived from:

```yaml
vars:
  greeting: hello
steps:
  - id: shout
    if: ${vars.greeting == 'hello'}
    vars:
      modified: ${regex.replace(vars.greeting.upperAscii(), 'HELLO', 'HI')}
    log:
      message: ${modified.lowerAscii()}
```

That is a smaller language than the one proposed and it is the right one: a scope whose
members can be read by the test deciding whether the scope exists is a scope that has to
be evaluated twice, or evaluated before it is known to be wanted. Neither is worth a line
saved.

### `echo:` is retired; `log:` is the capability it was imitating *(landed)*

`echo` is an identity function registered as an activity. Its shipped uses are (a) a
poor author's `vars:`, (b) hello-world scaffolding, and (c) "this branch ran"
markers. Use (a) dies with `vars:`. Uses (b) and (c) were always a *logging* need
served under a misleading name — echo returns; it does not emit.

`log:` is a real product surface: a message a human sees in `flow run` output, the
TUI, and the run record, durable and honest about its purpose. Its inputs are
`message` (required), `level` (optional: `info`, `warn`, `error`; default `info`),
and `fields` (optional string-keyed map for structured sinks). It has **no
outputs** — referencing `${steps.announce.result}` off a log line is the `vars:`
use case sneaking back in, and offering nothing is how the language says so. It is
also the trivial effect the shared driver-agreement tests need, which is the one
legitimate job `echo` was doing.

```yaml
# before
- id: hello
  echo:
    message: hello world
```

```yaml
# after
- id: hello
  log:
    message: hello world
```

*Since written:* `log:` shipped as specified — `message` required, `level` defaulting to
info, `fields` a bounded string map, and no outputs. `echo` has since gone with `cel:`
and `printf:` at the v2026.2 boundary, in one sweep, so a file had one change to make
rather than three.

Three details the plan did not have to settle and the implementation did.

**Where the message goes is the caller's, not the task's.** The task emits through an
`slog.Logger` taken from its context. That is what lets one workflow render to a
person's terminal under `flow run local` and reach a worker's log aggregator in
production — through Temporal's activity logger, so every line is tagged with the run
that emitted it rather than being the one line in a worker's output nobody can trace. A
package-level destination would have been one place per *process*, which is wrong for a
worker serving several tenants and wrong for two tests running at once.

**`level:` is the language's first enum-typed input,** and an author writes the choice
rather than the storage: `level: warn`, not `LEVEL_WARN` and certainly not `2`. Both the
spelling and the diagnostic are derived from the enum descriptor, so the next enum input
on any task inherits them. `flow validate` names the three choices when a fourth is
written, and suggests `warn` for `warning` — three edits apart, which no edit-distance
threshold tight enough to be useful will reach, and the single likeliest mistake here.

**A task declaring no outputs made an old gap total.** `${steps.web.nonsense}` used to
validate cleanly and resolve to nothing at run time: the output name was discarded where
the reference was parsed, so only the step id was ever checked. With `log` producing
nothing, *every* reference to a log step is that mistake. References now carry the
output name and are checked against the task's declared outputs — silently, wherever the
set is not knowable in full: an `http` step naming its own `outputs:`, a `for_each`, a
`parallel`, a gate, or a plugin that describes none.

```console
$ flow validate workflow.yaml
workflow.yaml:11:16: step "use" input "message": step "say" has no output "result";
the log task produces no outputs, because a log step is an effect rather than a value
```

### `printf:` is retired — the replacement already exists *(landed)*

Verified: `${'hello %s, you have %d step(s) left'.format([vars.name, 0])}` compiles
and runs today, because the pinned profile ships CEL's strings extension. The
`format` function is specified at the CEL level — cross-language, versioned, pinned
by the profile and by Worker Versioning — which is precisely the determinism story a
formatting facility needs, and which a task wrapping Go's `fmt` can never have,
because that leaks the implementation language into the DSL contract. The task also
duplicates conversion logic the expression layer owns, and rejects doubles, which
`format` handles.

```yaml
# before
steps:
  - id: name
    echo:
      message: flowstate
  - id: greeting
    printf:
      format: "hello %s, you have %d step(s) left"
      args:
        - ${steps.name.result}
        - 0
```

```yaml
# after
vars:
  name: flowstate
steps:
  - id: greeting
    log:
      message: ${'hello %s, you have %d step(s) left'.format([vars.name, 0])}
```

*Since written:* landed at v2026.2. `format` needed nothing built for it, which was the
argument; what the sweep did need was the diagnostic, since a `printf:` key now has to
teach the expression rather than report an unknown task — `flow validate` prints the
`format()` call, and `flow fix` performs the rewrite where the step's result is read.

One thing the shape above hides, worth naming because it now applies wherever a message
is composed: a format string containing `: ` is YAML mapping syntax, so a `${...}`
holding one has to be quoted. The corpus entry below was written with an unquoted
`'paging %s: %s %s is rolling'` and did not parse — an acceptance target that was not
accepted, which is the exact failure the corpus rule exists to prevent, and which was
invisible until the documented workflows were run through the compiler.

### `edition:` is required, and v-prefixed *(landed)*

Both halves reverse a decision this document recorded, and both were reversed by
evidence rather than by taste.

**Optional became required.** The reasoning for optional was good: a line of ceremony at
the top of every file to say the only thing it could say, when a file that does not care
which grammar it is in is the common case. What it missed is that "absent means current"
is not a default — it is a promise to *reinterpret*. This edition renamed `iterator:` to
`as:` and rooted the http response. A file written last month with no marker is not a
file that does not care; it is a file written in the older grammar, and reading it as
this one is precisely the silent reinterpretation `edition:` was introduced to prevent.
The optional spelling made the mechanism unable to do its job for exactly the files most
likely to need it.

The ceremony is not an author's problem, because `flow fix` writes the line — below any
header comment, since a comment block at the top is about the file and a key inserted
above it would read as describing the edition.

**Unprefixed became v-prefixed.** A date with one dot is a YAML *float*, so `2026.1`
arrives as a number. The workaround — reading the token's source text rather than
converting — exists because converting is quietly wrong (2026.10 and 2026.1 are the same
float, so the tenth edition of a year would compile as the first) and refusing teaches an
author to quote a value for a reason no other key has. A `v` makes it a string in every
YAML parser. The workaround stays only for reading `2026.1`, and goes when that edition
does.

Two consequences worth stating.

A *missing* marker is reported and then the file is compiled anyway, unlike a *declared*
edition this build does not know. The abort exists because a file claiming another
grammar makes every other diagnostic describe the wrong language; a file that declares
nothing is not that. It is almost always this grammar with a line missing, so the rest of
what is wrong with it is worth reading rather than being hidden behind one ceremonial
sentence.

And `Marshal` writes the marker. The schema has no field for an edition and should not
grow one — it is a property of a *file* — but a document without one is a document this
build refuses, and Marshal's contract is that its output reads back as the same workflow.
It writes the current edition, which is not a guess: this build compiles one grammar.
Omitting it would leave whoever writes `flow fmt` with a formatter that quietly
invalidates every file it touches.

### `vars:`, and the shadowing rule that ships with it *(landed)*

`vars` lands in the positions already planned, and the scope rules land in the same
change — deciding them later is the Terraform `optional()` mistake this document
already refused once.

- **Workflow-level `vars:`** — constants and start-time derivations, referenced
  rooted: `${vars.region}`. Rooted because ambient, per principle 5.
- **Step-level `vars:`** — private bindings, referenced bare within the step:
  `${modified}`. Bare because author-chosen and lexically local, the same standing
  as a loop binding.
- **Shadowing is refused, not resolved.** A bare name must be unique against every
  bare name in scope: a step var may not collide with an enclosing loop binding, a
  loop binding may not collide with an enclosing step var, and neither may be
  `now`. `flow validate` reports the collision at the inner declaration. Silent
  shadowing is how `${body}` comes to mean two things eleven lines apart.
- **The fence stays required inside `vars` values.** A var legitimately holds the
  literal string `"steps.greet.result"`, so this is exactly the ambiguous position
  the fence exists for. No exception.

*Since written:* all of the above shipped, plus four rules the plan did not have to
decide and the implementation did.

- **A `vars:` block on a *block* step** — a `for_each` or a `parallel:` — is in scope
  for its items expression and throughout its body. That follows from "lexically
  local" rather than extending it, and it is where the validator and the engine first
  disagreed: the engine bound them for the body, the validator did not know they were
  bound, and a body step redeclaring one shadowed it silently. Both walks now derive
  the scope before they know what kind of work the node does.
- **A var may not read its siblings**, at either position. A protobuf map has no
  order, so "the one declared above" is not something a file can mean; accepting it
  would work exactly as often as the map iterated conveniently. The alternatives were
  a dependency sort with a cycle diagnostic, or nothing. Nothing is the smaller
  language and allowing it later is additive.
- **A workflow-level var may reference nothing at all** — not a step (none has run),
  not another var, not a root written bare. `flow validate` says which of those three
  it was, because they are three different misunderstandings.
- **`vars` written bare is a legal operand.** `${vars["region"]}` with a computed key,
  or `size(vars)`, resolve — the activation answers a root whole. `steps` was exempted
  from the bare-name check when rooting landed and `vars` was not, which is the shape
  a second root always takes: the exemption goes where the first one needed it rather
  than where the category does.

Two schema notes for anyone reading `Scope`. Its `vars` field holds the *bare*
bindings — a loop's iterator, a step's own vars, `now` — because that is what it has
always held, and the rooted namespace went to a new `ambient_vars` field beside it. A
field that crosses a version boundary means what it has always meant: an activity
scheduled before a worker upgrade is retried after it, reading the payload already in
history. And the durable driver evaluates a step's vars in workflow code, where it
already resolves that step's expression inputs, while the *workflow's* block goes
through an activity — the difference is that the workflow block is evaluated once per
run and carried across Continue-As-New, so a segment must not be able to recompute a
different answer.

`examples/workflow-vars` shows the ambient half and `examples/step-vars` the lexical
one; both run in CI.

Two spellings improve for free once static data stops impersonating computation.
Verified: `items:` already accepts a literal YAML list — the shipped example routes
one through a `cel` step for no reason the language imposes.

```yaml
# before (the shipped example)
steps:
  - id: targets
    cel:
      expr: "['alpha', 'beta', 'gamma']"
  - id: process
    for_each:
      items: ${steps.targets.result}
      iterator: name
```

```yaml
# after
vars:
  targets: [alpha, beta, gamma]
steps:
  - id: process
    for_each:
      items: ${vars.targets}
      as: name
```

### `for_each` reads `as:` *(landed)*

`iterator:` is retired for `as:` in the same edition sweep. It is shorter, names the
binding rather than the mechanism, and reads as the sentence it is: *for each item
as name*. `flow fix` rewrites it.

*Since written:* landed as specified, and the machinery it needed is the reusable part.

A retired key is not an unknown one, and the difference is the whole diagnostic. "unknown
key `iterator`; did you mean `items`?" sends an author to correct a word they spelled
correctly. What helps is the new spelling, the reason, and the command that writes it:

```console
$ flow validate workflow.yaml
workflow.yaml:6:7: step "each": `iterator:` is now `as:` — it names the binding rather
than the mechanism, and reads as the sentence it is: *for each item as name*; run
`flow fix` to rewrite this file
```

Two properties make that table safe to grow for the rest of the sweep. Each entry is
guarded by its *position* rather than by the word alone — `iterator:` written on a step
is still simply an unknown key, because `as:` is not a key there, and answering "run
`flow fix`" for a file the command will not touch is the one response worse than none.
And an entry lives for exactly one edition: the grammar carries one spelling at a time,
so an entry buys a good sentence for a file written before the sweep rather than a
second spelling the parser accepts.

The rewrite itself edits the key token and copies the rest of the line through, so the
value keeps its quoting, an inline comment keeps its column, and a comment written
beneath the key stays where the author put it. That is what makes `flow fix .` something
people run on a directory rather than a file at a time.

### `http:` stays; its response scope gets a root *(landed)*

The name passes the test the others fail — it names the protocol, not an
implementation or a vendor — and its danger is governed where principle 7 wants it,
in `netpolicy`. What changes is the private scope its `expect:` and `outputs:`
expressions evaluate in. Today those bind bare `status_code`, `headers`, `body`,
`json`. Those are *system-chosen* names injected into an author's namespace, exactly
the shape the signal-payload fix already rooted under `payload.*`, and for the same
two reasons: the set will grow (a future `duration_ms` must not capture anyone's
binding), and collisions are already representable — `as: body` on a loop enclosing
an `http` step whose `expect:` says `body` silently reads the response, not the
item. Every bare system name costs a guard rule forever; a root costs one spelling.

So the response scope becomes `response.*`, keeping the existing field names, and
`flow fix` roots existing expressions mechanically:

```yaml
# before
- id: web
  http:
    url: https://httpbin.org/json
    parse_json: true
    expect: ${status_code == 200}
    outputs:
      title: ${json.slideshow.title}
```

```yaml
# after
- id: web
  http:
    url: https://httpbin.org/json
    parse_json: true
    expect: ${response.status_code == 200}
    outputs:
      title: ${response.json.slideshow.title}
```

This narrows the statement of principle 5 that the first round's naming model made:
"bare local names" always meant the names an *author* binds. A loop's `as:` stays
bare; what a task or the engine injects gets a root. `now` remains the single
exception, already fenced by its own rules.

*Since written:* landed as specified, and it removed an ambiguity rather than adding a
spelling. A bare `status_code` inside `expect:` used to be genuinely undecidable — it
could have been the response's or a step called `status_code` — which is why `flow fix`
declined to rewrite a deferred input at all and only *noted* it. Under the rooted
grammar a step is `steps.<id>`, so the four names the task binds can only be the
response's, and `flow fix` rewrites them:

```console
$ flow fix workflow.yaml
workflow.yaml:14: response references rooted under `response.`
```

The note survives for the names that are *not* the response's, which is where the
ambiguity genuinely still lives — and is suppressed for the four, so an author is never
told to undo the migration the same command just performed.

The rewriter generalised at the same time: `rootedUnder` takes a root and a set of
names, so the step rooting and this one share the parts that are hard and easy to get
subtly wrong — verifying an offset holds the identifier it claims to before splicing,
and applying splices from the back. What did *not* generalise is the permission. This
is the one deferred scope whose shape is knowable; every other one is still left alone.

Held for their own reviewed changes, in this order of need: structured `auth:` (so
authors stop hand-building secret-bearing headers), an `idempotency_key`, and
declared egress capabilities. Each interacts with policy or secrets and none blocks
the vocabulary work.

### `${...}` stays; `!expr` is refused

The fence survives two challenges on its merits.

**Interpolation stays refused.** `"deployed ${a} to ${b}"` is two languages in one
scalar — a template language containing CEL fragments — and it is the direct
mechanism of the injection class that string-splicing CI systems keep shipping.
A value is one literal or one expression, never a string with holes; `format()` and
concatenation are the answer, and the diagnostic already teaches them.

**A YAML `!expr` tag is refused.** It looks structurally cleaner — the AST would
know literal from expression without delimiter scanning — but it fails on the case
that decides it: a tag changes how YAML parses the node beneath it. Write the
natural thing,

```yaml
json: !expr {'service': vars.service}
```

and YAML reads `{...}` as a flow *mapping* with a tag on it; the expression is
destroyed by the host syntax before CEL sees the text. It must be quoted anyway, and
a tag plus quotes is a worse-spelled fence. The fence's whole virtue is that
expression text always travels inside a plain string, immune to YAML's own syntax —
`${ {'k': 1} }` works today for exactly this reason. Add that generic YAML emitters
(the form builders and agents principle 6 exists for) handle custom tags poorly
while `${...}` is what every model has seen, and that the fence's load-bearing
surface *shrinks* every time the schema types another position as
expression-known — churn on a contracting surface buys nothing. Refused, and
recorded so the next proposal engages the flow-mapping counterexample rather than
the aesthetics.

### `value:` is refused, for now

If a pure result-producing step existed, `value:` would be its right name — it says
what the step contributes, accepts literals and expressions equally, and avoids the
false implications of `set:`, `eval:`, `compute:`, and `run:`. But everything it
does is `vars:` wearing a step id, and it drags real debt behind it: whether
`${steps.roster}` should then mean the value directly, which breaks the uniform
named-outputs model every tool reads. Adding it later is additive and cheap;
removing it later will never happen. So: refused until a corpus file hurts without
it, and the word joins the reserved list so no plugin takes it in the meantime.
`assert:` is held on the same terms — it is pure, so it would be a node kind, and
`if:` plus a failing step nearly covers it; whichever of it and Phase 2's `check:`
lands first must be designed knowing the other is coming, because two spellings of
"refuse to proceed" is one too many.

### Plugins appear in the syntax, deliberately distinguishable

ARCHITECTURE.md says a plugin task should be "indistinguishable from a built-in."
Half of that survives contact with this round: indistinguishable in *tooling* —
validation, completion, hover, docs, all derived from the descriptors the manifest
already ships — yes, that is the registry invariant across a process boundary.
Indistinguishable in *spelling*, no, for three reasons: provenance is a fact a
reviewer needs at the step ("this line leaves the engine's code and enters code
somebody installed"); a flat namespace makes installation order load-bearing the
day two plugins want `deploy`; and a file using a missing plugin deserves "plugin
`slack` is not installed on this worker," not "unknown task." That sentence in
ARCHITECTURE.md gets amended when this lands.

The spelling is **`<plugin>.<task>:`** — dotted, two segments:

```yaml
steps:
  - id: notify
    slack.post:
      channel: "#deploys"
      text: ${'released %s'.format([steps.build.version])}
```

The grammar slot is already vacant, verified twice over: `TaskManifest.name` is
validated `^[a-z][a-z0-9_]*$`, so no task can contain a dot, and the parser's
`couldBeATaskName` rejects dots, so a dotted key today is an unknown key rather than
a task. A dotted key is therefore *unambiguously* a plugin reference — the
disjointness guarantee extends without a new rule, the first segment is the name
discovery already establishes (`flowstate-plugin-<name>`), and the language server
gets a two-level completion tree. The dot also rhymes with the language's one idiom:
`steps.build.version` on the read side, `slack.post` on the write side — selection,
both times. What is refused alongside: `uses:`-style indirection (the `task:` /
`name:` nesting this document spent an edition deleting) and distribution metadata
in the step key (`owner/repo@ref` welds deployment concerns into every call site).

Version requirements belong to the file's header, not the step — declared as
minimums, per [the versioning scheme](#versioning-one-scheme-for-everything-that-can-break):

```yaml
plugins:
  slack: v2.1.0    # a minimum: at least v2.1.0, same major
  github: v1.4.0
```

Declared once, auditable in one place, resolved against the worker's catalog at
submit and refused closed — "this deployment cannot run this file" at the terminal,
not a failing step forty minutes in. It is also what gives `flow validate` and the
language server a schema source for `slack.post` on a machine that has the plugin,
and the compiled spec a place to record what a run was compiled against — the
profile-pinning logic, extended to the task surface, which replay will eventually
demand. It lands with Phase 3, where `call:` forces the cross-file question anyway;
the dotted-key *shape rule* is reserved now, while it costs one sentence. Task-queue
routing — a plugin task running on a specialized worker — stays a deployment
concern and never becomes a Flowfile spelling.

*Since written:* the dotted key landed with the registration seam rather than
waiting for Phase 3, because the seam forced the question early: the first plugin
tasks were about to register under bare manifest names, and renaming a spelling
people have used is exactly the migration cost this section was written to avoid.
What shipped is the spelling and its enforcement — the host registers
`<plugin>.<task>`, the manifest name stays dot-free so a plugin cannot choose its
own qualifier, `Task.name` admits at most one dot, and a dotted key naming an
uninstalled plugin is diagnosed as an installation question rather than a spelling
one. The `plugins:` version header, the submit-time catalog check, and the language
server's two-level completion tree remain Phase 3 as planned.

### `exec:` will be built-in, denied by default

Competitiveness with CI systems needs process execution; the admission test's second
condition says it cannot ship before its policy exists, because today the engine has
no exec analog to `netpolicy`. Both facts are honoured: `exec` is built-in, **denied
by default**, enabled per deployment — the loopback-egress posture, applied to a
sharper knife. And it takes **`argv` as a list, never a shell string**:

```yaml
- id: test
  exec:
    argv: [go, test, -count=1, ./...]
    dir: ${steps.checkout.dir}
```

A shell-string form with expressions producing pieces of it is command injection by
construction — the same reasoning that refused `${...}` interpolation, with higher
stakes. An author who wants shell semantics writes `argv: [bash, -c, "..."]` and
owns it visibly. There is no `shell:` task. Outputs are `exit_code` and
byte-bounded `stdout`/`stderr` (principle 9: the process, not the worker, decides
what it prints, so the worker bounds what it keeps). The full input surface —
environment, working directory, what a nonzero exit means — is settled when the
policy is, in its own reviewed change.

### The disposition table

| Today | Fate | Replaced by |
| --- | --- | --- |
| `cel:` | **retired at v2026.2 (landed)** | `vars:`, inline `${...}`, Phase 2 `outputs:` |
| `echo:` | **retired at v2026.2 (landed)** | `vars:` for data, `log:` for visibility |
| `printf:` | **retired at v2026.2 (landed)** | `format()` in the profile (already present) |
| `iterator:` | **retired at v2026.2 (landed)** | `as:` |
| bare `status_code`/`body`/`headers`/`json` | **rerooted (landed)** | `response.*` |
| `http:` | kept | — (auth, idempotency key, egress declarations held) |
| `log:` | **new (landed)** | — |
| `exec:` | new, gated on its policy | — |
| `value:` | refused for now, name reserved | `vars:` until a corpus file proves otherwise |
| `assert:` | held | `if:` + failure, pending Phase 2 `check:` |
| `!expr` | refused | whole-value `${...}`, fence-optional where the schema knows |
| plugin tasks | dotted keys, `plugins:` header in Phase 3 | — |

Registry today: **`log`, `http`**. End state, once `exec` has its policy: **`log`,
`http`, `exec`** — small enough to memorize, which is the property worth copying from
the standard library this vocabulary keeps being compared to.

### One edition, one sweep, and what the rewriter may not guess

All of the retirements ride one edition boundary — `cel`, `echo`, `printf`,
`iterator:`, and the response rooting together — because each boundary is a whole
migration story and five small ones would be five of them. `flow fix` rewrites the
mechanical cases: rooting response names, `iterator:` to `as:`, `printf` to a
`format()` call, `cel`/`echo` steps whose results feed later steps into the
equivalent `vars:` bindings with references rewritten, the same way it rooted step
outputs. What it must refuse rather than guess is intent it cannot see — an `echo`
whose result nothing references might have meant "show a human this line" (`log:`)
or nothing at all; the fixer says so and stops, which is already its contract for
flow style and aliases. Every retired spelling gets a `retiredStepKeys` entry
naming its replacement, so the diagnostic teaches the migration instead of
reporting an unknown task.

*Since written:* the sweep landed as one edition, and two things about the shape that
shipped differ from what this section describes. Both are recorded here rather than
edited into the text above, because a design document that quietly matches the outcome
stops being a record of a decision.

**The tasks were deleted, not kept and refused.** The plan reads as though a retired
spelling stays known — a `retiredStepKeys` entry, a diagnostic, a task still registered
under a refusal. What shipped removes `echo`, `printf` and `cel` from the schema and
from the task registry outright; what remains of them is the *key* entry the parser
consults to write a good sentence, and nothing an author writes can reach an
implementation. That is the stronger reading of "one dialect per file, pinned per run":
a build does not merely decline to compile the old grammar, it does not contain it. The
diagnostic is documentation with a position attached, not a gate in front of a
still-present task.

**With no local task returning a value, nothing left produces a value the workflow
itself computed.** The enumeration this paragraph first gave was wrong and is corrected
rather than quietly dropped: `http` and `for_each` are not the only producers, because a
wait produces outputs too — every wait reports `timed_out`, and a `wait_for_signal:`
additionally carries whatever the sender supplied under `payload.*`, which is what makes
`${approval.timed_out}` and `${approval.payload.approved}` writable at all. `log`
declares none by design, and what remains is control flow.

The correction does not rescue the conclusion, though, because the outputs it adds are
of exactly the kind already named: a wait's `timed_out` is something the *clock* decided
and a signal's `payload` is something a *sender* asserted, just as an `http` step's
outputs are what a server handed back. Every remaining producer of a value is
therefore something outside the workflow handed in, so a workflow still has no supported
way to name a computed result as an output of the run. Inside a file `vars:` covers it,
and the moment the question is what the run *returns* — to a caller, to a schedule, to
Phase 3's `call:` — the answer is the `outputs:` contract, which is not built. Phase 2
was already scheduled as the phase that ships the contract with its checker. It is now
also the phase that restores a capability the sweep removed, and it should be sequenced
knowing that rather than as the type system arriving on its own timetable.

### The corpus is the acceptance list

Designing from hello-world is how `echo` happened. The fix is a corpus of
representative workloads — CI pipeline, deployment with approval, ETL, incident
runbook, fan-out matrix, agent loop with a gate, saga/compensation, scheduled
maintenance — but held to this repo's reachability rule, which "design fixtures"
would otherwise erode: each corpus file is written down as the *acceptance target*
for the phase that makes it compile, and graduates into `examples/` (and so into
CI) the day it runs. A corpus file that never graduates is a design that never
landed, kept visible. The two below are the first two entries.

### What the language looks like when this round lands

Everything in this file is decided surface: Phase 1 vocabulary plus the response
root. It is the deployment-with-approval corpus entry, and it was the acceptance
target for the retirement edition — which it now meets: `flow validate` accepts it
exactly as written below.

```yaml
edition: v2026.2
name: deploy
description: Ship a build, gate production behind a human, then page in order.

vars:
  service: billing
  version: 2026.07.30-r1
  api: https://deploys.internal.example.com
  oncall: [ada, grace, katherine]

steps:
  - id: submit
    description: Ask the deployment API to roll the service.
    http:
      method: POST
      url: ${vars.api + '/deployments'}
      json:
        service: ${vars.service}
        version: ${vars.version}
      parse_json: true
      expect: ${response.status_code == 202}
      outputs:
        deployment: ${response.json.id}
    retry:
      attempts: 3

  - id: approval
    description: A human approves the roll, or the day ends without one.
    wait_for_signal:
      name: approve
      timeout: 24h

  - id: halt
    if: ${steps.approval.timed_out || !steps.approval.payload.approved}
    log:
      level: warn
      message: ${'deployment %s not approved; stopping'.format([steps.submit.deployment])}

  - id: page
    if: ${!steps.approval.timed_out && steps.approval.payload.approved}
    for_each:
      items: ${vars.oncall}
      as: person
      max_parallel: 1
      steps:
        - id: notify
          log:
            # Quoted, because the `: ` inside the format string is YAML mapping syntax.
            message: "${'paging %s: %s %s is rolling'.format([person, vars.service, vars.version])}"
            fields:
              deployment: ${steps.submit.deployment}
              approved_by: ${steps.approval.payload.by}
```

Worth noticing what is absent: no `cel:`, no `expr:` nested inside anything, no
`echo:`, no `printf:`, no template sublanguage, no hand-built JSON string, no bare
response names, and no evaluator branding anywhere an author looks. CEL is doing
all of the work and none of the talking.

And the CI-pipeline corpus entry, which is the same language plus the Phase 3
plugin surface and a policied `exec` — the file that makes "could be used for CI"
a demonstration rather than a claim. **It does not compile today, deliberately:** it is
the acceptance target for Phase 3, and this build answers it with `unknown key
"plugins"`, `unknown key "github.clone"`, and `unknown task "exec"`. That is the corpus
rule working as intended — a design that has not landed, kept visible — rather than a
file that has gone stale.

The fence says `(proposed)` rather than plain `yaml`, which is how the corpus check
tells a design sketch from a documented file: every plain `yaml` block opening with
`edition:` is compiled by `TestREADMEWorkflowsCompile`, and this one would fail that
for the reasons just given. A renderer still highlights it as YAML — the mark is for
the reader as much as for the test.

```yaml (proposed)
edition: v2026.2
name: ci
plugins:
  github: v1.2.0

vars:
  repo: picatz/flowstate
  ref: main

steps:
  - id: checkout
    github.clone:
      repo: ${vars.repo}
      ref: ${vars.ref}

  - id: test
    exec:
      argv: [go, test, -count=1, ./...]
      dir: ${steps.checkout.dir}
    timeout: 15m

  - id: report
    if: ${steps.test.exit_code != 0}
    github.create_issue:
      repo: ${vars.repo}
      title: ${'CI failed on %s'.format([vars.ref])}
      body: ${steps.test.stderr}
```

Nothing in the *language* says CI — the same shapes serve the runbook above — which
is the positioning: one execution model, with CI as one workload it happens to beat
incumbents at by owning durability, retries, gates, and policy in the grammar.

### Versioning: one scheme for everything that can break

This is a greenfield project with one user. Nothing pre-1.0 is kept for
compatibility — old spellings are deleted, not carried, and the question "may we
break this?" is always yes. What that freedom does **not** buy is the right to
break *carelessly*: histories replay for months, and an agent trained on last
quarter's examples will keep writing last quarter's grammar forever. So the
mechanics of breaking are the product surface here, and they should be one
coherent scheme rather than five dials invented separately. The model is Go
modules, because it is the one versioning design whose choices have survived
contact with a decade of ecosystems: a language version in the file, a toolchain
that refuses what it cannot honour, minimum-version dependency selection with no
solver, major versions as part of identity, and a lockfile of exact resolutions.

Mapped onto this system, five things version, and each binds at a different
moment:

| Layer | Declared | Binds at | May break |
| --- | --- | --- | --- |
| grammar | `edition:` in the file (required) | parse | at any edition, with `flow fix` across the boundary |
| expression dialect | `Workflow.profile`, stamped by the compiler | compile; honoured at run and replay | with the edition; never within one |
| compiled spec | proto package `flowstate.v1` | forever — histories replay against it | WIRE never; FILE spent deliberately |
| engine | worker `--deployment-name --build-id` | run start; Continue-As-New takes current | freely between runs; never within one |
| plugins | `plugins:` minimums in the file; exact resolutions recorded in the compiled spec | resolve at submit, pin for the run | majors freely; a major is a different requirement |

The coherence rules, and what each refuses:

**Versions are explicit, and spelled the Go way.** Every version in a Flowfile is
written, never inferred, and every one carries the `v` prefix: `edition: v2026.2`,
`slack: v2.1.0`. This reverses the earlier decision that an absent `edition:` means
the current one. That decision optimized a line of ceremony away and bought a
latent ambiguity with it: a file without an edition means whatever the build
reading it says, so the same bytes compile differently on two machines — which is
`GOPATH`'s defect, the one `go.mod`'s explicit `go` directive exists to close. The
ceremony objection also weakens to nothing in a greenfield: every file is new,
`flow fix` stamps the line, and `flow validate` on a file without one says exactly
what to add. The `v` prefix is not decoration either — it makes an edition a plain
string to YAML, so `v2026.10` and `v2026.1` are distinct by construction and the
read-the-source-token hack is deleted. Plugin requirements are canonical full
semver, `vMAJOR.MINOR.PATCH`, no shorthand and no ranges, exactly as `go.mod`
requires — a spelling with one form needs no normalizer and produces no
almost-equal diffs.

**The key is `edition:`, and `version:` is refused for it — twice over.** A
top-level `version:` is ambiguous about whose version it names, and the ambiguity
has a track record: Docker Compose's schema `version:` was universally misread as
the author's own and was deprecated for it. Worse, the word is *needed*: Phase 3
makes a workflow a callable module with its own contract semver, and `version:` is
the only natural spelling for that — spending it on the grammar dial gives the
actually-versioned artifact a second-choice name forever. `edition` also names the
mechanism precisely, because this *is* Rust's editions down to the rewriter
(`flow fix` ≈ `cargo fix --edition`): dated boundaries, breaks only at them,
unknown ones refused. Go's own spelling of this dial — the language name, as in
`go 1.22` — is refused too: `flowstate: v2026.2` puts branding in every file to
say what `edition:` says generically.

**The author sees one version dial: the edition.** Explicit does not mean many.
Everything else is stamped, resolved, or pinned by machinery. The profile is already compiler-stamped —
`profile:` is not a Flowfile key and must never become one, because two files in
one repo speaking two dialects is the situation the one-profile decision exists
to prevent. The edition names the grammar *and* implies the dialect; the two are
recorded separately because they die at different times — the edition at compile
(the schema deliberately has no field for it), the profile at the end of the last
run that replays against it.

**There is no toolchain directive, because its two halves already exist.** Go
needs `toolchain` because old toolchains must build new modules. Here, an edition
this build does not know is *refused rather than translated* — that is the
toolchain check, fail-closed — and every capability question beyond grammar
("does this worker have `slack.post` at ≥2.1?") is answered by submit-time
resolution against the deployment's catalog. An `engine:` key would be a second
dial answering the same two questions worse.

**Plugin versions follow the import-compatibility rule, without the solver.**
A `plugins:` entry declares a *minimum*: `slack: v2.1.0` means at least v2.1.0,
same major. There is no range syntax and no constraint solver, for Go's reason —
a deployment installs exactly one version of a plugin, so resolution is a
comparison, not a search. A new major is a different requirement declared
explicitly, never satisfied silently by an installed older one; the major lives
in the requirement, not in step keys — `slack.post:` never grows a `/v2`, because
one deployment holds one major of a plugin and the header already says which. The declared
minimums are the `go.mod`; the compiled spec records the *exact* versions
resolved at submit — the lockfile — so a run is reproducible from its spec alone:
spec + profile + worker build + resolved plugin versions is the complete answer
to "what exactly did this run mean?".

**Workflows themselves become modules in Phase 3, not before.** The day `call:`
lands, a workflow is a dependency and needs an identity and a version. The shape
to hold: name plus major as identity (the import-compatibility rule again), and
the version speaks about the workflow's *contract* — its typed `inputs:` and
`outputs:` — which is why it cannot precede Phase 2: a semver promise over an
untyped surface is decoration, by the same rule that keeps `type:` out until its
checker exists.

**Feature flags are refused.** No `experiments:`, no opt-in grammar, no
per-file switches. A file compiles under its edition or it does not; flags make
2ⁿ dialects out of one, which principle 4 exists to prevent. The one legitimate
shape that looks like a flag — `exec` enabled, loopback egress allowed — is
deployment *posture*, deliberately not expressible in the file, per principle 7.

**And the retirement machinery is for models, not for users.** With one user
there is nobody to deprecate for, so `retiredStepKeys` and the `flow fix`
refusal-not-guess contract might look like baggage. They are the opposite: agents
generating Flowfiles are trained on every example that ever existed, so the old
spellings keep arriving *forever*, from authors who were never using the old
version at all. A diagnostic that names the replacement is how the language
teaches the training-data gap away; that is a permanent product feature, not a
transition cost.

### What this round adds to the order of work

Phase 1 grows: `vars:` with its shadowing rules, `log:`, the retirement edition
(`cel`, `echo`, `printf`, `iterator:`→`as:`, `response.*` rooting), the
`retiredStepKeys` entries, and reserving `value` alongside the dotted-key shape
rule — the last two cost sentences now and compatibility later. The same edition
flips `edition:` itself to required with the `v`-prefixed spelling, and `flow fix`
stamps the line into files that lack it.

*Since written:* all of that has landed as v2026.2. What Phase 1 has left is the
reserved-keyword diagnostics for the grammar Phase 4 will need; `vars:` is in both of
its positions, the profile is pinned, and the three tasks are gone from the schema.
Phase 2 is what the sweep leaves load-bearing, per [the
sweep](#one-edition-one-sweep-and-what-the-rewriter-may-not-guess): with no local task
returning a value, `http` and `for_each` are the only steps that produce outputs, so
the `outputs:` contract is now the only route by which a run can report a computed
result at all.

The `plugins:`
header and dotted-key resolution land in Phase 3 with `call:`. `exec` lands only
with its policy, gated the way workflow-side evaluation is gated on Worker
Versioning: a capability that assumes a posture verifies it or stays off. The
`Host.Register` seam — one call wide — is the highest-leverage unbuilt item in this
document, because the catalog it populates is what every surface in principle 12
reads.

## The third round: versions in flight, and what the engine already knows

The question this round answers: where does a version *live* once a workload is
running, what may move underneath a run that takes a year, and what the language
owes to observability. The answers fall out of one structural fact worth naming
before any of them.

### The interpreter dividend

In hand-written Temporal, the workflow definition *is* code, so evolving it while
runs are in flight is the hardest problem in the ecosystem: `GetVersion` patches,
per-workflow-type worker versioning, and team-wide discipline, forever. Flowstate
split that atom without saying so: the definition is **data** — a compiled spec
carried in `RunState` — and the only workflow-side *code* is the interpreter.
Two consequences, and they are the product's second-best sentence after the bet:

- **A definition change cannot touch a run in flight.** Every run carries its own
  spec; editing a Flowfile and resubmitting creates new runs and strands nothing.
  No author will ever write a patch, call `GetVersion`, or reason about which
  branch of their own workflow history took. The bet extends: the least careful
  author cannot break replay, *and no author can break an in-flight run by
  editing a file.*
- **The only code with a versioning problem is the interpreter**, and that
  problem is already solved and landed: pinned per run, auto-upgrade at
  Continue-As-New, with `RunState` obeying published-message rules across the
  seam (ARCHITECTURE.md invariants 4 and 10). Rollout mechanics — ramping a new
  build id, draining an old one, finding the runs still pinned to it through
  visibility — are deployment posture and `flow deployments` tooling, and are
  never spelled in a Flowfile.

So "where are versions stored" has a layered answer with nothing implicit in it.
Source lives in git with its `edition:` — the authoring artifact, owned like Go
source. The compiled spec lives *in the run* — self-contained, with the stamped
profile, the resolved plugin versions, and the build-id pin forming the complete
reproducibility record; a run can be explained forever from what it carries.
And when Phase 3 makes workflows callable, published definitions live in a
catalog as **immutable** `name@vMAJOR.MINOR.PATCH` entries with content digests —
the module-proxy posture: a published version never changes, and a caller's spec
records the digest it resolved, which is `go.sum` for workflows.

### Migration is explicit, at the seam, and checked

The default is that nothing migrates: a run finishes on the spec and interpreter
it started with, and a deploy touches nothing in flight. Explicit beats implicit
here more than anywhere — a silently migrated year-long run is an integrity
failure with a calendar.

When an operator must move a run — the definition of a long workload has a bug
ahead of where it is — the only sound seam is the one the engine already uses:
Continue-As-New, where the next iteration starts from `RunState` rather than
replaying history. And because both specs and the state are typed data, legality
is *mechanical*, which is what Phase 5's "frame-checked proofs" should mean: the
completed prefix recorded in `RunState` — which steps ran, by id, with what
output shapes — must remain meaningful under the new spec, and the check is a
comparison, not a judgment call. `flow migrate` performs it or refuses with the
step that fails it. Raw Temporal cannot offer this to arbitrary code; an
interpreter over data can, and it is the single most powerful thing this
abstraction adds on top of the substrate rather than merely surfacing from it.

Two things follow, and one binds **now**:

- **Step ids are the migration contract.** The frame check compares by id, so
  renaming an id is a breaking change to every in-flight run of that workflow —
  which is why this is recorded in the present even though `flow migrate` is
  Phase 5: `flow fix` must never rename a step id, and a future rename needs the
  same deliberateness as a schema break. Ids joined "diagnostics anchor" and
  "reference root" as load-bearing roles today; "migration frame" is the third.
- **Reset becomes step-shaped.** Temporal's native repair is reset-by-event-id,
  which is archaeology. Here the unit is a step, so the ergonomic spelling is
  exact: `flow rerun <run> --from steps.deploy` — truncate state to the frame
  before that id and continue on the current (or a named) spec version. Held to
  Phase 5 with `flow migrate`, shape recorded now.

Schedules, when they land, pin an exact catalog version; updating the schedule
*is* the deploy action, and there is no `@latest` — a schedule that follows a
moving name is the implicit-edition defect with a timer attached.

### Observability is derived, not authored

The engine durably records everything observability wants: a run is a trace, a
step is a span with start, end, attempts, outcome, tenant, and queue — that *is*
the history. So traces and metrics are **projections of recorded truth**, emitted
by the engine and worker — OTel spans per step, engine metrics for durations,
outcomes, retries, queue latency — ambient, requiring nothing in any file, and
honest about retries by construction: a step that ran three times is one span
with `attempts=3`, because the projection reads what happened rather than firing
when it happens. Every derived point also carries a stable identity — run id,
step id, attempt — so an exporter that crashes and re-reads history re-emits
*the same point*, deduplicable by key at any sink; its delivery is at-least-once
like every exporter's, but nothing about the data is lost or doubled by saying
it twice. And it can be computed retroactively, for runs that finished before
the exporter existed.

A `metric:` task is **refused**, and the reason is load-bearing rather than
taste: activities are at-least-once, so a counter incremented inside a step
fires again on every retry — and an imperative increment carries no identity, so
no sink can tell the retry from a real second event. A projected point can
always be deduplicated, because history gives it a key; an emitted increment can
never be, because nothing does. That asymmetry, not a preference, is the
refusal.
Business dimensions ride the planned label/search-attribute surface and Phase 2's
typed outputs — any metric someone wants is then a query over visibility and
history, not a side effect an author remembered to fire.

`log:` stays the only *authored* observability surface, because it carries the
one thing the engine cannot derive: intent, written for the human reading the
run. Heartbeats — progress inside a long `exec` or plugin task — are task-side
(the plugin protocol and `exec` report progress; a policy key can surface a
deadline later) and are not vocabulary.

### What this round adds

One obligation binds immediately: the id-stability rule for `flow fix` and the
sentence in `validate.go`'s orbit that treats an id rename as the breaking change
it is. Catalog immutability lands with Phase 3's `call:`; `flow migrate` and
`flow rerun --from` stay Phase 5 with their shapes now recorded; ambient OTel
export is engine work that touches no grammar and can land whenever it earns its
place in a phase.

## The fourth round: the tool is a product surface

The rounds above decide what an author writes. This one decides what a person and
a *program* meet when they operate the result — the CLI, the TUI, the API, the
language server, and the agent are one system or they are five systems that
drift. [CLI.md](CLI.md) holds the presentation contract (streams, colour,
symbols, vocabulary, error voice); these are the architectural decisions above
it, and they are mostly one decision applied five times.

### Every command is a projection of an RPC

`flow` is a thin client of the same Connect services everything else speaks —
`WorkflowService` today, the catalog and plugin services as they land. The rule,
scoped to where it means something: **a command whose answer comes from the
running system is a projection of an RPC, and one that cannot be expressed as an
RPC names a missing RPC.** This is principle 6 turned on the tooling: the CLI is
an operations *projection*, exactly as YAML is an authoring projection of the
spec.

The scoping matters, because the rule read literally would demand RPCs of
commands that deliberately need no server. `validate`, `fix`, and `run local`
operate on local files and must keep working offline — that is invariant 8 —
and `worker` and `server` do not *call* the services, they *are* them. What
those commands share with the remote ones is the other half of the rule: the
**message types**. A diagnostic from an offline `validate` is the same schema
message the hosted validation RPC returns, so one object still renders
everywhere, and no capability exists only as CLI code.

What holding the rule buys, with no additional design:

- **`--output json` is free and cannot drift.** Machine output is the protojson
  of the RPC response message — there is no second encoder, so there is no
  second schema. `flow inspect <run> --output json` and a Connect call return
  the *same message*: identical field names, enum spellings, and presence
  semantics, from one schema. Not identical bytes — the CLI deliberately emits
  unpopulated fields and indentation as presentation (`cmd/flow/output.go` says
  why: `.closeTime` on an unfinished run should be null, not missing) — and the
  parity promised is the schema, which is the one a program depends on. The
  rule here is that this is the *only* machine shape any command may grow.
- **The TUI is another renderer.** It draws the same messages the plain output
  prints; entering it is deliberate (per CLI.md), and it can never know
  something the pipe cannot.
- **MCP is generated, not written.** `flow mcp` serves the same services as MCP
  tools with schemas derived from the protos — proto-first applied to the agent
  surface, so there is no hand-maintained tool list to fall behind the engine.
- **API parity is structural.** Anything a person can do from a terminal, a
  program can do from any language with a Connect client, because they are the
  same call.

Diagnostics complete the loop. They are already a typed shape — line, column,
step, field, message — and they become a schema message, so *one* diagnostic
object renders as `path:line:col:` on a terminal, a squiggle in the editor, an
annotation in CI, and a structured MCP tool result. One diagnostic, four
renderings, zero translations.

### One dialect, including for questions

Operational queries speak the language the workflows speak. `flow runs --where`
takes a CEL expression in the pinned profile, over typed, rooted run metadata —
the same rooted-name discipline as the DSL (`run.*`, `steps.*`), the same cost
bounds, no second query mini-language to learn, document, or secure:

```
$ flow runs --workflow vendor-onboarding --since 90d \
    --where 'steps.reviews.duration > days(30)' --count
14
```

Filters push down to the visibility store where the store can answer them and
evaluate engine-side where it cannot, under the scan and request bounds the
listing already enforces — a query surface is a paged listing wearing a
predicate, and inherits its rules.

### Plan and apply: anything that touches a run checks first

Every mutating operation against a run — `migrate`, `rerun`, `terminate` — has a
`--check` form that is pure, free, and safe to run forever; the mutating form
runs the *same* check, refuses on failure, and in a non-interactive stream
requires its explicit confirmation flag. This is Terraform's plan/apply lesson
applied where it matters most: the objects being mutated have been running for
months and belong to someone.

And the apply is *recorded into the run it changed*. A migration or a rerun
appears in the run's own history with the actor's authenticated identity and the
before/after spec digests, so provenance is a chain rather than a snapshot, and
the answer to "who moved this run, from what, to what, and when" is `flow trace`
— not an expedition through a SIEM. For an enterprise this is the difference
between an audit feature and an audit *property*.

### The agent is a first-class operator

Nothing here is a bolted-on "AI feature"; the agent surface is the machine
surface, taken seriously:

- **Schemas from the source of truth.** An agent discovers tasks, plugins, and
  their typed inputs from the catalog RPC — the same registry-derived
  descriptors that drive completion and docs (principle 12). A model never needs
  a prose cheat-sheet that can go stale.
- **Diagnostics are the training loop.** The teaching messages — position, what
  is wrong, what to write instead, with the fix pasteable — are how a model
  converges on the current grammar in one round trip. The retirement diagnostics
  exist for exactly this population, as the versioning round records.
- **Pure verbs make unattended iteration safe.** `validate`, `fix --check`,
  `migrate --check`, and every `--output json` read are side-effect-free by
  construction, so an agent can loop on them without supervision; the mutating
  verbs sit behind the plan/apply gate above, which is the same gate a human
  gets. One permission model, not one per kind of caller.

### The transcripts are the acceptance bar

Output format is API — a person greps it, a pipe parses it, and a golden test
freezes it. So the operational corpus below follows the Flowfile corpus rule:
each transcript is the acceptance target for the feature that produces it, and
graduates into a golden test the day the feature lands. Live today: the edition
refusal, `flow fix`'s contract, and worker pinning. Decided, with phases in
[the third round](#the-third-round-versions-in-flight-and-what-the-engine-already-knows):
provenance, the fleet view, `migrate`, `rerun`, schedules, `trace`. Stream
discipline per CLI.md applies throughout: answers on stdout, accounts on stderr.

The compiler teaching the current grammar (live; the vocabulary landed with the
retirement edition, and the wording of each sentence below is the design target
rather than a transcript — the shipped diagnostics say the same things at greater
length, because a retirement has to teach the replacement and not merely name it):

```
$ flow validate old-style.yaml
old-style.yaml:1:1: a Flowfile declares its edition; add `edition: v2026.2`, or run `flow fix` to stamp it
old-style.yaml:9:5: step "targets": `cel:` is no longer a step key; a computed value is a `vars:` binding now, and a static list needs no expression at all
old-style.yaml:15:7: step "process" for_each: `iterator:` is now `as:`; run `flow fix` to rewrite this file
old-style.yaml:30:15: step "check" input "expect": references bare `status_code`, and the response is named `response.status_code` now; run `flow fix` to rewrite this file

$ flow fix old-style.yaml
old-style.yaml: stamped edition v2026.2; rewrote cel: to vars:, iterator: to as:; rooted 3 response references
old-style.yaml:18:5: step "announce": `echo:` result is never referenced; it may have meant `log:` — this tool does not guess intent, rewrite it yourself
$ echo $?
1
```

A run explaining itself, forever (spec-in-run live; plugin resolution Phase 3):

```
$ flow inspect 018f3c2e --provenance
spec digest:      sha256:9f41c07a…
compiled from:    edition v2026.2
profile:          cel-2026-07
plugins resolved: slack v2.3.1 (declared minimum v2.1.0, same major)
pinned build:     bld-2026-07-28.1  (deployment: prod)
```

The fleet, and the seam (pinning live; the view is decided tooling over
visibility):

```
$ flow runs --pinned-before bld-2026-09-03.1
RUN        WORKFLOW           PINNED BUILD      AGE   STATE
018f3c2e   vendor-onboarding  bld-2026-07-28.1  61d   waiting: security-review
0190aa17   cert-rotation      bld-2026-08-15.2  9d    running: renew
```

Migration, checked and refused (Phase 5; this output format is decided now):

```
$ flow migrate 018f3c2e --to vendor-onboarding.yaml --check
frame check against RunState:
  create    ok    completed; present in target, outputs unchanged (vendor)
  reviews   ok    in flight; wait_for_signal shapes identical, carried signals preserved
  activate  --    not reached; replaced by target definition
  outcome   --    not reached
would migrate at the next Continue-As-New seam: sha256:9f41c07a… -> sha256:2bb8d1e4…

$ flow migrate 018f3c2e --to renamed.yaml
frame check failed:
  create    REFUSED   RunState records completed step "create"; target has no step
                      with that id (found "register" — if this is a rename, an
                      in-flight run cannot know that)
nothing was changed
$ echo $?
1
```

The run as its own audit trail (Phase 5, with the third round's derived
observability):

```
$ flow trace 018f3c2e
vendor-onboarding 018f3c2e ------------------------------------ 63d 4h
  create                 http        1.2s    attempts=2
  reviews                for_each    63d 4h
    [legal] decision     wait        2d 1h   signal=legal-review
    [security] decision  wait        61d 3h  signal=security-review
  migrate                operator    kent@…  9f41c07a… -> 2bb8d1e4…
  activate               http        800ms
  outcome                log         "vendor v-7731 onboarding finished"
```

The migration is a row in the trace because it is an event in the history — the
audit property, visible where the run is read.

### What this round adds

Immediately cheap and immediately valuable: the diagnostic schema message and
`--output json` as protojson of existing responses (the convention
`cmd/flow/output.go` already holds, extended to every command with an answer).
`--where` lands when the listing
surfaces grow past flags. The plan/apply gate binds `terminate` now and
`migrate`/`rerun` when Phase 5 builds them — with these transcripts as their
golden tests. `flow mcp` follows the catalog and `Host.Register`, because an
agent surface generated from services is only as complete as the services.

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

*Since written:* [the second round](#the-second-round-vocabulary-expressions-extension)
grows this phase — `log:`, the retirement edition (`cel`, `echo`, `printf`,
`iterator:`→`as:`, `response.*` rooting), and two reservations (`value`, the dotted
plugin-key shape). The vars shadowing rules are part of `vars` landing, not a
separate item.

*Since written, again:* every item above except the reserved-keyword diagnostics has
landed. `vars:` is in both of its Flowfile positions — `inputs.*` and `run.*` are still
unstarted and are separate features rather than the tail of this one — the profile is
pinned and `libs:` is deleted, `edition:` is required and `v`-prefixed, and `echo`,
`printf` and `cel` are gone from the schema and the registry rather than registered and
refused. **Phase 1 shrank the language further than this section planned**, which is the
paragraph below's problem: the sweep left `http` and `for_each` as the only steps that
produce outputs, so a run has no way to report a value it computed rather than fetched.

**Phase 2 — the contract, with its checker.** `inputs`, `outputs`, `check:`,
`env.Check` at validate and in the LSP, typed hover, the absent/null/default matrix,
`secret:` taint. One phase, because half of it is decoration without the other half.

It is also, since the retirement edition, the phase that restores something. `outputs:`
was filed here as the third home for what `cel:` used to do — shape a final result —
and with `cel:` gone it is the *only* one: `vars:` names a value inside a file and
nothing carries one out of the run. That does not change the rule this phase exists to
hold, which is that a declared type without a checker is decoration. It changes the
cost of the phase slipping, which is worth knowing before it does.

*Since written:* **the schema half of `inputs:`/`outputs:` landed first, and the
language half followed in the same week.** `Workflow.declared_inputs` and
`Workflow.declared_outputs` exist, as do `InputDeclaration` (name, type, required,
default, description), `OutputDeclaration` (name, expression, description),
`RunOutputs`, `RunRequest.inputs`, `RunState.inputs`/`RunState.run_outputs`, and
`GetResponse.run_outputs` — and a Flowfile now writes both blocks in the spelling
below, `flow validate` checks the deferred list (duplicate names, the four lexer
tokens, literal-only defaults, the required-with-default contradiction, default
type), `${inputs.<name>}` resolves as a fourth root in every expression position,
and both drivers bind submitted values, apply defaults, and evaluate declared
outputs once in the segment that finishes the run. The house gate's last clause is
met too: `flow run` and `flow run local` take `--input name=value` and
`--input-file inputs.json`, `flowstate_run_local` takes an `inputs` object (and
`flowstate_run` carried `inputs` the day the field landed, its schema being
derived from the descriptor), declared outputs are reported as `runOutputs` by
`flow get` and by both drivers' documents, and `examples/parameterized-deploy/` and
`examples/computed-outputs/` exercise both blocks in CI. What is still owed is
`check:`, `env.Check` in the LSP, typed hover, and the `secret:` taint — the rest
of this phase, not the tail of this item.

One limit worth writing down where the spelling is, because it is not obvious from
the blocks: the workflow-level `vars:` block cannot read `${inputs.<name>}`. Vars
are evaluated once before the first step, against a scope holding literals,
operators and the profile's functions — `EvalWorkflowVars` builds it as
`NewScope(profile, nil)`, the run's arguments are bound into the scope only
afterwards, and the durable driver evaluates the block in an activity handed the
declared vars and the profile and nothing else. So an argument is in scope for a
step's `if:`, a step's own `vars:`, and a task's inputs, but not for the ambient
block above them.

That is refused with a diagnostic rather than left to fail at run time: `flow
validate` names the var, the line and column, and the reference — "a var may not
read an input: `vars:` is evaluated before the run's arguments are in scope, so
write `inputs.<name>` where the value is used — in a step's `if:`, its own `vars:`,
or a task input". Reported as its own sentence because "unknown name" would be
false: the input exists and the reference is spelled correctly, and what the author
needs is where to write it instead.

The spelling it is landing under, so that the schema and the surface cannot be
designed twice:

```yaml
inputs:
  region:
    type: string
    required: true
    description: which region to deploy to
  retries:
    type: int
    default: 3

outputs:
  url:
    value: ${steps.deploy.response.body.url}
    description: where the thing ended up
```

Two blocks at the workflow level, beside `vars:`. An input is read as
`${inputs.<name>}` — grouped under an object root, per invariant 2 and for the
reason `steps.` and `vars.` are: a root makes a collision with a step id or a var
unrepresentable rather than a rule somebody has to write, and it turns a name into
a field selection, so seventeen of CEL's twenty-one reserved words are legal input
names for free. The four that are not (`true`, `false`, `null`, `in`) are lexer
tokens and are refused by the compiler with a diagnostic naming the declaration,
the same way they are refused as step ids.

A declared type is an enum in the schema rather than a type *expression* like the
one `flow tasks` prints, because this one is enforced against a value a caller
chose and the catalog's is rendered for a reader. What the schema checks is one
declaration at a time — name shape, a defined type, lengths. What the compiler owes
is everything about a *set*: no two declarations sharing a name, the four lexer
tokens, a default that is a literal rather than an expression, and the
contradiction of a required input carrying one. And what the server owes at submit
is the caller's half — every required input present, nothing undeclared, every
value of its declared type, values and never expressions — refused while the caller
is still there to be told.

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
