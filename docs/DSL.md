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

### `cel:` is retired — not renamed

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

### `echo:` is retired; `log:` is the capability it was imitating

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

### `printf:` is retired — the replacement already exists

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

### `vars:`, and the shadowing rule that ships with it

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

### `for_each` reads `as:`

`iterator:` is retired for `as:` in the same edition sweep. It is shorter, names the
binding rather than the mechanism, and reads as the sentence it is: *for each item
as name*. `flow fix` rewrites it.

### `http:` stays; its response scope gets a root

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
  slack: "2.1"     # at least 2.1, same major
  github: "1.4"
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
| `cel:` | retired at the `vars:` edition | `vars:`, inline `${...}`, Phase 2 `outputs:` |
| `echo:` | retired at the same edition | `vars:` for data, `log:` for visibility |
| `printf:` | retired at the same edition | `format()` in the profile (already present) |
| `iterator:` | retired at the same edition | `as:` |
| bare `status_code`/`body`/`headers`/`json` | rerooted | `response.*` |
| `http:` | kept | — (auth, idempotency key, egress declarations held) |
| `log:` | new | — |
| `exec:` | new, gated on its policy | — |
| `value:` | refused for now, name reserved | `vars:` until a corpus file proves otherwise |
| `assert:` | held | `if:` + failure, pending Phase 2 `check:` |
| `!expr` | refused | whole-value `${...}`, fence-optional where the schema knows |
| plugin tasks | dotted keys, `plugins:` header in Phase 3 | — |

End state of the built-in registry: **`log`, `http`, `exec`**. Small enough to
memorize, which is the property worth copying from the standard library this
vocabulary keeps being compared to.

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
root. It is the deployment-with-approval corpus entry, and the acceptance target
for the retirement edition.

```yaml
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
            message: ${'paging %s: %s %s is rolling'.format([person, vars.service, vars.version])}
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
a demonstration rather than a claim:

```yaml
name: ci
plugins:
  github: "1.2"

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
| grammar | `edition:` in the file (absent = current) | parse | at any edition, with `flow fix` across the boundary |
| expression dialect | `Workflow.profile`, stamped by the compiler | compile; honoured at run and replay | with the edition; never within one |
| compiled spec | proto package `flowstate.v1` | forever — histories replay against it | WIRE never; FILE spent deliberately |
| engine | worker `--deployment-name --build-id` | run start; Continue-As-New takes current | freely between runs; never within one |
| plugins | `plugins:` minimums in the file; exact resolutions recorded in the compiled spec | resolve at submit, pin for the run | majors freely; a major is a different requirement |

The coherence rules, and what each refuses:

**The author sees one version: the edition.** Everything else is stamped,
resolved, or pinned by machinery. The profile is already compiler-stamped —
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
A `plugins:` entry declares a *minimum*: `slack: "2.1"` means at least 2.1,
same major. There is no range syntax and no constraint solver, for Go's reason —
a deployment installs exactly one version of a plugin, so resolution is a
comparison, not a search. A new major is a different requirement declared
explicitly, never satisfied silently by an installed older one. The declared
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
rule — the last two cost sentences now and compatibility later. The `plugins:`
header and dotted-key resolution land in Phase 3 with `call:`. `exec` lands only
with its policy, gated the way workflow-side evaluation is gated on Worker
Versioning: a capability that assumes a posture verifies it or stays off. The
`Host.Register` seam — one call wide — is the highest-leverage unbuilt item in this
document, because the catalog it populates is what every surface in principle 12
reads.

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
