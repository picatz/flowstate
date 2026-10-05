# The Flowfile language

A Flowfile is a YAML document that describes one workflow: the arguments it
takes, the steps it runs, and the result it returns. Expressions inside it are
written in [CEL](https://cel.dev/), the Common Expression Language. This page
teaches every construct in the current edition, `v2026.4`, with its defaults,
limits, and the mistakes worth knowing about.

New to Flowstate? [Get started](GETTING_STARTED.md) first, then come back here
for detail. Two generated references complete this one: the
[task reference](reference/tasks.md) lists every task's inputs and outputs, and
the [CEL reference](reference/cel.md) lists every function an expression can
call. The reasons behind the language's design are recorded in
[Language design decisions](DSL.md).

## Contents

- [A Flowfile at a glance](#a-flowfile-at-a-glance)
- [Values and types](#values-and-types)
- [Expressions](#expressions)
- [Steps](#steps)
- [Tasks](#tasks)
- [Control flow](#control-flow)
- [Waiting and signals](#waiting-and-signals)
- [Failure, retries, and compensation](#failure-retries-and-compensation)
- [Starting runs: triggers](#starting-runs-triggers)
- [Who may act on a run](#who-may-act-on-a-run)
- [Secrets](#secrets)
- [Plugins](#plugins)
- [Editions and migration](#editions-and-migration)
- [Limits](#limits)
- [Keys at a glance](#keys-at-a-glance)

## A Flowfile at a glance

```yaml
edition: v2026.4
name: restock
description: Decides how to restock each low item, and says what it ordered.
inputs:
  items:
    type: list(dyn)
    required: true
    description: inventory records, each with a sku, a count, and a reorder point
  priority:
    type: enum
    values:
      - normal
      - urgent
    default: normal
vars:
  batch_size: 25
steps:
  - id: low
    value: ${inputs.items.filter(i, i.count < i.reorder_at)}
  - id: batches
    value: '${(inputs.priority == "urgent") ? 2 : 1}'
  - id: order
    for_each:
      items: ${steps.low.value}
      as: item
      steps:
        - id: place
          log:
            message: ${"ordering " + string(vars.batch_size * steps.batches.value) + " of " + item.sku}
outputs:
  ordered:
    value: ${steps.low.value.map(i, i.sku)}
    description: the skus this run ordered
```

```console
$ flow run local restock.yaml --input priority=urgent \
    --input 'items=[{"sku":"bolt","count":3,"reorder_at":10},{"sku":"nut","count":50,"reorder_at":10}]'
running locally
INFO ordering 50 of bolt
COMPLETED workflow restock
outputs
  ordered ["bolt"]
```

The top-level keys:

| Key | Required | Meaning |
| --- | --- | --- |
| `edition` | yes | The grammar version the file is written in. [Editions](#editions-and-migration) |
| `name` | yes | The workflow's name: letters, digits, `-`, and `_`, up to 128 characters. It names schedules and webhook routes, and it is what `flow list --filter 'name == "…"'` matches. |
| `description` | no | Prose for people, `--help` output, and agents. |
| `labels` | no | Fixed `key: value` metadata recorded on every run, for filtering listings. [Labels](#labels) |
| `inputs` | no | The run's typed arguments. [Inputs](#inputs) |
| `outputs` | no | The run's result. [Outputs](#outputs) |
| `vars` | no | Named constants, read as `vars.<name>`. [Vars](#vars) |
| `steps` | yes | What the workflow does, 1 to 100 steps. [Steps](#steps) |
| `triggers` | no | How runs may start besides `flow run`: a schedule, webhooks, and rules for manual starts. [Triggers](#starting-runs-triggers) |
| `concurrency` | no | At most one run per key. [One run at a time](#one-run-at-a-time-concurrency) |
| `signals` | no | Who may send each signal the workflow waits for. [signals](#who-may-send-a-signal-signals) |
| `debug` | no | Who may pause a durable run. [debug](#who-may-pause-a-run-debug) |
| `plugins` | no | The minimum version of each plugin the file uses. [Plugins](#plugins) |

Any other key is an error, with a suggestion when it looks like a typo.

The file is a strict subset of YAML: one document per file, no anchors (`&a`),
aliases (`*a`), merge keys (`<<:`), or tags (`!!str`), and no duplicate keys.
`flow fix` inlines an alias it can. A file is at most 1 MiB.

## Values and types

### Literal values

A YAML scalar written without a `${...}` fence is a literal value. Flowfiles
read YAML 1.2, so `yes`, `on`, and `off` are strings, not booleans.

| Written | Value |
| --- | --- |
| `hello`, `yes`, `2026-01-01`, `12:30` | string |
| `"42"`, `'true'` | string |
| `true`, `false` | bool |
| `42`, `1_000`, `0x10` | int (`0x10` is 16) |
| `017` | int 15: a leading zero is octal |
| `1.5` | double |
| `null`, `~` | null |
| `[a, 2, {k: v}]` | list |
| `{region: eu, replicas: 3}` or a nested mapping | map with string keys |

A key with no value (`key:`) is refused rather than read as null.

The types a value can have are string, int, double, bool, bytes, timestamp,
duration, null, list, and map. Maps always have string keys.

### Inputs

`inputs:` declares the run's arguments. Each is checked when the run is
submitted, before any step runs, so a caller who sends the wrong type or forgets
a required input is refused with a message naming it.

```yaml
inputs:
  version:
    type: string
    required: true
    description: the release version
    example: v1.4.2
    must: this.matches(r'^v?[0-9]+\.[0-9]+\.[0-9]+$')
  environment:
    type: enum
    values:
      - staging
      - production
    default: staging
  replicas:
    type: int
    default: 3
    must: this >= 1 && this <= 50
  hosts:
    type: list(string)
    max_items: 20
```

| Key | Meaning |
| --- | --- |
| `type` | Required: `string`, `int`, `double`, `bool`, `timestamp`, `duration`, `bytes`, `enum` (a string from a fixed set), or a container written as a type expression: `list(string)`, `map(string, int)`, `list(list(int))`. `list(dyn)` and `map(string, dyn)` hold anything. The bare words `list`, `struct`, and `float` were retired in `v2026.4`; `flow fix` rewrites them. |
| `required` | `true` if the caller must supply it. Default `false`. |
| `default` | The value used when the caller leaves it out. A literal, never an expression. Cannot be combined with `required: true`. |
| `values` | For `enum` only: the allowed strings, up to 64. |
| `min_len`, `max_len` | For `string`: bounds on length, counted in characters rather than bytes. |
| `min_items`, `max_items` | For `list`: bounds on length. |
| `must` | Any other constraint, as a CEL predicate over `this`. It is written without a fence, cannot read anything but `this`, and is checked on every value, default, and example. |
| `description` | Repeated back in refusals, so it is written for the caller. |
| `example` | An illustrative value for documentation and tools, checked against the type and constraints but never used. |
| `sensitive` | Withhold this value from displays. See [Secrets](#sensitive-values). |

Read an input as `${inputs.<name>}`. A few things to know:

- **An optional input with no default is absent, not null.** `${inputs.region}`
  fails with `no such key: region` when the caller did not send one. Read it
  with `${inputs.?region.orValue("eu-west-1")}`, or give it a default.
- **A type expression says what a container holds.** `type: list(string)` makes
  `${inputs.ids.map(i, i.lowerAscii())}` valid and `${inputs.ids.map(i, i + 1)}`
  a refusal when the file is validated, and a `default:` or a submitted value
  whose elements are not strings is refused naming the element
  (`declared list(string) but was given an integer at [1]`). A map's keys are
  always `string`. A worker built before typed containers existed refuses
  a run whose workflow declares one, so upgrade every worker before submitting
  it. Inside a YAML flow mapping, quote the value
  (`{ type: "map(string, int)" }`), because the comma ends it otherwise.
- **`timestamp`, `duration` and `bytes` are bound as the type CEL reads.** They
  travel as text (an RFC 3339 timestamp, a Go-form duration such as `90m`, and
  padded base64) and are bound once at submit, so `${inputs.opens + inputs.window}`
  is time arithmetic and `must: this > timestamp("2026-01-01T00:00:00Z")` reads
  `this` as a timestamp. Text that is not one is refused without repeating it.
  They work wherever a type is written: a field of a record, the elements of
  `list(timestamp)` and values of `map(string, duration)`, and an output's
  `type:`. A rule across a record's fields compares times, not text, and an
  output is reported in the run document as the text a caller would submit (an
  RFC 3339 timestamp, a Go-form duration such as `1h30m0s`, base64). See
  `examples/typed-moments`. A worker built before they existed refuses the
  declaration when the run starts.
- **There is no int-to-float widening.** A `double` input's `default: 1` is
  refused; write `1.0`. On the command line, `--input ratio=2` is converted for
  you.
- **The retired `pattern:`, `min:`, `max:`, and `unique:` keys** are refused
  with the equivalent `must:` to write instead.

Supplying inputs:

- `flow run` and `flow run local` take `--input name=value`, converted by the
  declared type (`int`, `double`, `bool`; JSON for lists and maps; the text itself
  for a string, timestamp, duration or bytes), and `--input-file args.json`, a JSON object keyed by input name.
  `--input` wins over the file for the same name.
- The API's `Run` takes literal values; an expression or secret reference from a
  caller is refused.
- A schedule binds its inputs once, when created. A webhook maps its payload to
  inputs with `with:`. A `call:` step binds the callee's inputs with `with:`.

### Outputs

`outputs:` is the run's result: what `flow run` prints, what `Get` returns under
`runOutputs`, and what a caller of this workflow through `call:` reads.

```yaml
outputs:
  scope:
    value: '${(steps.requested.value > 2) ? "fleet" : "canary"}'
    type: enum
    values:
      - canary
      - fleet
    description: how wide this rollout was
```

Outputs are evaluated once, after the last step, in the order written. They can
read `inputs`, `vars`, `run`, `trigger`, and any step the top-level scope can
see: top-level steps, the steps of the `switch:` arm that ran, and the steps
written directly in a `parallel:` branch. Steps inside a `for_each:` or `loop:`
body, or nested in a block inside a `parallel:` branch, are not visible. `value:` is required;
`type:`, `values:`, and `must:` are checked when the value is computed, and
`description:` and `sensitive:` mean what they do on an input. If an output
cannot be computed, or fails its type or `must:`, the run fails and its
[compensation](#compensation-undo) runs.

### Vars

Workflow `vars:` are named constants, read as `${vars.<name>}` in steps and
outputs.

```yaml
vars:
  region: eu-west-1
  banner: ${"deploying to " + "eu-west-1"}
  targets:
    - alpha
    - beta
```

They are evaluated once, before the first step, and cannot read anything the run
provides: not `inputs`, not steps, not `run` or `trigger`, and not each other.
They exist for values the file would otherwise repeat. A value that depends on
the run belongs in a step's own `vars:` or a [`value:` step](#computing-a-value-value).
A var cannot hold a secret reference.

A step can also declare `vars:`, read **bare** inside that step:

```yaml
- id: describe
  vars:
    subject: ${"release for " + vars.environment}
  log:
    message: ${subject}
```

A step's vars are evaluated after its `if:` and before its inputs, and can read
`inputs`, workflow `vars`, earlier steps, and any loop binding around the step.
They cannot read each other, and they are private to the step. On a `for_each`,
`loop`, `parallel`, or `switch` step, they are also in scope for the step's own
expressions and its whole body. One trap: a step's `if:` is evaluated *before*
its vars, so it cannot read them.

### Records: `types:`

A shape that several declarations repeat is declared once under `types:` and
used by name wherever a type is written:

```yaml
types:
  Line:
    fields:
      sku: {type: string, required: true, min_len: 2}
      quantity: {type: int, required: true, must: this > 0}
  Order:
    must: this.status != "paid" || size(this.lines) > 0
    fields:
      id: {type: string, required: true}
      status: {type: enum, values: [open, paid], required: true}
      lines: {type: "list(Line)", required: true, max_items: 20}
inputs:
  order: {type: Order, required: true}
```

A field is written like an input and takes the same bounds (`values`,
`min_len`, `max_len`, `min_items`, `max_items`, `must`). A field's `must:` is a
CEL predicate over `this`, the field's value; a `must:` on the type is one over
the whole record, which is where a rule across fields goes. A record may name
another record, alone or inside `list(...)` and `map(string, ...)`, but not
itself. Records are closed: a name the type does not declare, or a missing
`required:` field, is refused, at `flow validate` for a literal in the file and
at submit for a value that arrives. Expressions are checked against the record,
so `inputs.order.id + 1` is refused before the run starts and a misspelled field
gets the nearest real one. `default:`, `example:` and `sensitive:` on a field
are refused rather than ignored. See `examples/record-types/`.

### Labels

```yaml
labels:
  team: payments
  cost-center: cc-1234
```

Labels are literal strings recorded on every run of the workflow (and on its
schedules), up to 64 pairs. They are for finding runs, not for computing:
`flow list --filter '"team" in labels && labels["team"] == "payments"'`. An
expression cannot read them.

## Expressions

### The `${...}` fence

`${...}` holds one CEL expression. What the whole value becomes depends on what
surrounds the fence:

- **A value that is exactly one fence keeps the expression's type.**
  `replicas: ${inputs.replicas + 1}` is an int; `ok: ${steps.check.value}` is
  whatever that step produced.
- **A value that mixes text with fences is a string.** Each fence is converted
  with CEL's `string()` and the pieces are joined, so
  `message: deploying ${inputs.version} to ${inputs.environment}` reads the way
  it prints. `string()` works on scalars; render a list or map with
  `json.encode(...)`.
- **`$${` is a literal `${`**, for text that must contain one.

> [!NOTE]
> `flow fmt` currently rewrites a value that mixes text and fences into the
> single expression it compiles to:
> `${"deploying " + string(inputs.version) + " to " + string(inputs.environment)}`.
> Both forms mean the same thing; the examples in this repository use the
> rewritten form because they are kept in formatter output.

A fence is an expression, not a template language: there are no loops,
conditionals, or filters in text. Use CEL for those.

**YAML quoting.** YAML reads `: ` as the start of a mapping and ` #` as the
start of a comment, even inside `${...}`. Quote the whole value when an
expression contains either, such as a ternary or a `#` in a string:
`value: '${(x > 2) ? "fleet" : "canary"}'`. The compiler names the `: ` mistake
and offers the fix; a stray ` #` shows up as an unterminated expression.

### Which fields are expressions

Every field is one of three kinds, and the kind decides what an unfenced value
means:

| Kind | An unfenced value is | Fields |
| --- | --- | --- |
| Expression | CEL | `if:`, `value:`, `for_each.items`, `loop.until`, `switch.value`, `wait_until:`, an output's `value:`, webhook `when:`, `idempotency_key:` and `correlate:` |
| Value | Literal text | Task inputs, `vars:`, `with:`, loop `init:` and `update:`, a wait's `prompt:` and `outputs:` |
| Literal | Literal; a fence is refused | `id`, `name`, `description`, `as:`, `must:`, input defaults, the step `timeout:` and `retry:` settings, a signal's `name:`, and other fields read when the file compiles |

In an expression field, `value: yes` is a reference to an unknown name and
`value: "42"` is the integer 42. Write the fence everywhere an expression is
meant, `value: ${"yes"}`; `flow fmt` adds it. `sleep:` and a wait's `timeout:`
take either a duration literal (`30s`) or a fenced expression.

### What an expression can read

| Name | Holds | Where |
| --- | --- | --- |
| `inputs.<name>` | The run's arguments, after defaults | Everywhere except workflow `vars:`, `must:`, and trigger expressions |
| `vars.<name>` | Workflow vars | Everywhere except workflow `vars:`, `must:`, `concurrency.key`, a `signals:` or `debug:` rule's `subject:`, and trigger expressions |
| `steps.<id>.<output>` | An earlier step's outputs | After that step, in the same scope |
| `run.workflow_id`, `run.run_id` | This run's address, for callbacks. `"local"` under `flow run local`. | Steps and outputs |
| `run.identity.subject`, `.issuer`, `.namespace`, `.claims`, `.principal` | Who started the run, as the server verified it. Empty when nobody authenticated. `principal` is `<issuer>#<subject>`, and `""` unless both are non-empty. | Steps and outputs |
| `run.local` | `true` under the local driver | Steps and outputs |
| `trigger.kind`, `.name`, `.principal`, `.delivery_id` | How the run started: `manual`, `schedule`, or `webhook`. [Triggers](#what-a-run-knows-about-its-start-trigger) | Steps and outputs |
| a bare name | A loop's `as:` binding, or a step's own `vars:` | Inside that step or loop body |
| `now` | The current time, as a timestamp | Inside a wait's own expressions only. [Waits](#the-clock-now) |
| `payload`, `sender`, `timed_out`; `deliveries`, `count` | A wait's results | Inside that wait's `outputs:` |
| `response.status_code`, `.headers`, `.body`, `.json` | The HTTP response | Inside an `http` step's `expect:` and `outputs:` |
| `event.headers`, `event.body` | A webhook delivery | Inside that webhook trigger's expressions |
| `this` | The value being checked | Inside an input's or output's `must:` |

`run` and `trigger` have fixed fields; `flow validate` reports a misspelled
one, and deliberately there is no start time or attempt count, because either
would let an expression observe something that changes on replay. A step id may
not be `steps`, `vars`, `inputs`, `run`, `trigger`, `true`, `false`, `null`, or
`in`.

A reference is checked when the file is validated: a step that does not exist
yet, a misspelled output, an unknown input, or a bare name that nothing binds
is an error with its position and, usually, a suggestion.

### Types and conversions

CEL is strictly typed. Coming from other YAML-based tools, these are the
differences that matter:

- **No implicit conversion.** `"3" == 3` and `1 + "a"` are type errors, not
  `true` and `"1a"`. Convert explicitly: `int("3")`, `string(3)`, `double(x)`.
- **Integer division truncates.** `7 / 2` is `3`. Mixing an int and a double in
  arithmetic is an error: write `double(x) / 2.0`, and `int(...)` to convert
  back.
- **JSON numbers are doubles.** Numbers from `json_parse`, an `http` response's
  `json`, or a signal's payload arrive as doubles. Compare with `1.0` or convert
  with `int()`. (Numbers in a webhook's `event.body` become ints when whole.)
- **`||` and `&&` take and return booleans.** A default is written
  `x.?y.orValue(d)`, not `x || d`.
- **Durations and timestamps are types.** `duration("90s")`, `hours(36)`, and
  `timestamp("2026-01-01T09:00:00Z")` build them; `string(hours(36))` is
  `"129600s"`.

`"%s has %d item(s)".format([name, n])` formats text with width, precision, or
positional reuse.

### Absent values: `.?`, `orValue`, and `has`

Reading a map key that is not there is an error, not an empty value. Three
spellings deal with absence:

| Expression | When the key is absent | When present and `null` |
| --- | --- | --- |
| `m.key` | Error: `no such key: key` | `null` |
| `has(m.key)` | `false` | `true` |
| `m.?key.orValue(d)` | `d` | `null` |
| `m.?key.hasValue()` | `false` | `true` |

`.?` also works on lists (`xs[?5].orValue(0)`) and on steps
(`steps.?maybe.value.orValue(0)`).

Choose the spelling by the question. To read with a default, use
`.?key.orValue(d)`. To ask whether something was sent at all, use `has()` or
`.hasValue()`, never `.orValue(false)`: a default cannot tell "not sent" from
"sent as false", and an approval gate usually needs to.
[examples/optional-dispatch](../examples/optional-dispatch/workflow.yaml) keeps
all three outcomes apart.

A step skipped by its `if:` has no outputs at all, so read it with
`has(steps.<id>)` or `steps.?<id>.<output>.orValue(...)`.

### Functions

Every expression can use CEL's standard functions and macros (`size`, `has`,
`all`, `exists`, `map`, `filter`, `in`, `contains`, `startsWith`, `matches`,
conversions) plus these libraries: strings, lists, math, sets, regex, optional
values, encoders, two-variable comprehensions, and `cel.bind`. Flowstate adds:

| Function | Meaning |
| --- | --- |
| `seconds(n)`, `minutes(n)`, `hours(n)`, `days(n)`, `weeks(n)` | Durations. A day is exactly 24 hours. |
| `json_parse(s)` | Parse JSON text into maps and lists. Numbers become doubles. |
| `json.encode(v)` | Render a value as JSON text. |
| `digest.sha256(s)` | `"sha256:<hex>"` of a string or bytes. |
| `lists.range(n)` | `[0, 1, …, n-1]`, for `n` up to 10,000. |
| `xs.sum()`, `xs.reduce(acc, v, init, expr)` | Fold a list. |
| `math.greatest(a, b)`, `math.least(a, b)` | There is no `max()` or `min()` builtin. |

The complete list, with signatures, is the [CEL reference](reference/cel.md),
also printed by `flow tasks --expressions`. An unknown function is an error
that suggests the nearest real one.

### Named computations: `functions:`

`vars:` hold values and `cel.bind` names a value inside one expression. A
computation used in several places is declared once under `functions:`:

```yaml
functions:
  slug:
    description: A title as it appears in a URL.
    params:
      title: string
    returns: string
    body: ${title.trim().lowerAscii().replace(" ", "-")}
```

and called like any CEL function: `${slug(inputs.title)}`. A body reads its
parameters and the standard vocabulary and nothing else, not `inputs`, `vars`,
`steps` or `run`, so a call shows every value it depends on. `params:` and
`returns:` are required and typed like an input; the body is checked against
them once, and each call is checked against the signature. Functions may call
one another but not themselves. Names are lowerCamel and may not shadow a
built-in. The compiler inlines each call, so a compiled spec holds plain CEL and
the runtime has no user-defined function. At most 64 functions per file and 16
parameters each. A `call:` does not carry them across. See `examples/functions/`.

### Where expressions run, and why they are limited

An expression cannot read the network, the filesystem, or the clock, and cannot
produce a random number. That is deliberate: on the durable driver, most
expressions are evaluated in workflow code, which Temporal replays from history
after a restart, and a replayed expression must produce the same answer it did
the first time. Anything with an effect belongs in a task.

| Evaluated in workflow code | Evaluated inside the task's activity |
| --- | --- |
| `if:`, step `vars:`, `value:`, `switch.value`, loop and `for_each` expressions, task inputs, wait expressions, `with:`, outputs | `http`'s `expect:` and `outputs:` (they need the response), and inputs a plugin task evaluates against the run's scope |

Workflow `vars:` are evaluated once per run, in an activity on the durable
driver, so a long-running workflow keeps the same values across upgrades.

Each run records the language profile it was compiled with, so a worker
upgraded mid-run evaluates the run's expressions with the vocabulary they were
written for.

### Expression limits

Every evaluation is bounded, and exceeding a bound is an ordinary failure of the
step (or of the run, for workflow `vars:` and `outputs:`), with kind
`Expression`:

| Bound | Limit |
| --- | --- |
| Cost of one evaluation | 1,000,000 units, roughly ten million characters of string work |
| Elements in a list built by one expression | 10,000 |
| Expression source | 100,000 characters, and 250 levels of nesting |
| Fences in one value | 64 |
| Nesting of literal structures | 32 levels |

## Steps

A step has an `id`, exactly one key that says what it does, and optional
properties:

```yaml
- id: deploy
  description: roll the release out to one region
  if: ${inputs.environment != "dev"}
  vars:
    target: ${"api-" + inputs.region}
  timeout: 30s
  retry:
    attempts: 3
  log:
    message: ${"deploying " + target}
```

**The id** is how everything else refers to the step: later expressions
(`steps.deploy.<output>`), tests, the debugger, and `flow get`. It must be a
valid identifier (letters, digits, `_`; no `-`), unique among the steps that can
see each other.

**The kind key** is one of:

| Kind | Does |
| --- | --- |
| a task name: `log:`, `http:`, `<plugin>.<task>:` | Work with an effect. [Tasks](#tasks) |
| `value:` | Computes a value. [value](#computing-a-value-value) |
| `switch:` | Runs one of several branches. [switch](#choosing-a-branch-switch) |
| `for_each:` | Runs its steps once per item. [for_each](#repeating-over-a-list-for_each) |
| `parallel:` | Runs branches concurrently. [parallel](#running-branches-at-once-parallel) |
| `loop:` | Repeats until a condition holds. [loop](#repeating-until-done-loop) |
| `call:` | Runs another Flowfile. [call](#reusable-workflows-call) |
| `sleep:`, `wait_until:`, `wait_for_signal:`, `wait_for_signals:` | Waits. [Waiting](#waiting-and-signals) |

**The properties:**

| Property | On | Meaning |
| --- | --- | --- |
| `description` | any step | Prose about the step. |
| `if` | any step | Run the step only when this is `true`. [Conditions](#conditions-if) |
| `vars` | any step | Named values private to the step. [Vars](#vars) |
| `continue_on_error` | any step | Let the run continue if this step fails. [continue_on_error](#tolerating-a-failure-continue_on_error) |
| `timeout`, `total_timeout`, `retry` | task steps | Bound and repeat the work. [Retries and timeouts](#retries-and-timeouts) |
| `undo` | task steps | A compensating task if the run later fails. [undo](#compensation-undo) |
| `async` | task steps | Start the step without waiting for it. [async](#starting-work-early-async) |
| `with`, `digest` | `call:` steps | The callee's arguments, and a pin on its content. [call](#reusable-workflows-call) |

A property that does nothing on a kind of step, such as `retry:` on a
`for_each:`, is refused with a message pointing to where it belongs.

### Order and data flow

Steps run in the order written. A step can read the outputs of steps before it,
never after. Reading a step also makes the dependency visible: a reviewer, a
test, and the validator all see that `deploy` uses `plan` because it says
`${steps.plan.value}`.

What a step produces, by kind:

| Step | Outputs |
| --- | --- |
| `log` | none |
| `http` | `status_code`, `headers`, `body`, and `json` when `parse_json: true`; or exactly the names its `outputs:` defines |
| a plugin task | Its declared outputs, or the names its `outputs:` defines if it supports shaping |
| `value` | `value` |
| `switch` | `value` (what it matched on) and `case` (the case that matched, or `null`) |
| `for_each`, `loop` | `results`: one entry per iteration, each a map from body step id to that step's outputs. A `loop` with `as:` also has `state`. |
| `parallel` | Nothing under its own id; the outputs of the steps written directly in each branch join the enclosing scope. |
| `call` | The callee's declared `outputs:` |
| `sleep`, `wait_until` | `timed_out` (always `false`) |
| `wait_for_signal` | `payload`, `sender`, `timed_out`, or the names its `outputs:` defines |
| `wait_for_signals` | `deliveries`, `count`, `timed_out`, or the names its `outputs:` defines |
| a failed step with `continue_on_error: true` | `error` |

A step skipped by its `if:` produces nothing, not even an empty map.

### Computing a value: `value:`

```yaml
- id: low
  value: ${inputs.items.filter(i, i.count < i.reorder_at)}
- id: count
  value: ${size(steps.low.value)}
```

A `value:` step evaluates an expression and records the result as
`steps.<id>.value`. It runs no task, is never retried, and costs nothing on the
durable driver beyond the evaluation. Use it to name a fact that later steps and
outputs read, rather than repeating the expression.

### Conditions: `if:`

```yaml
- id: rollback
  if: ${steps.verify.status_code != 200}
  log:
    message: rolling back
```

`if:` must produce a boolean; any other type is an error, never coerced. When it
is `false`, the step is skipped and produces no outputs. An error while
evaluating `if:` fails the run, even with `continue_on_error:`. `if:` cannot
read the step's own `vars:` or `now`.

## Tasks

A task step names the task as its key and writes the task's inputs beneath it.
This build has three built-in tasks, `log`, `http` and `exec` (denied until an operator loads `--exec-policy`); plugins add more, named
`<plugin>.<task>`. Before writing one, check what a deployment can run:
`flow tasks` locally, or `GetCatalog` against a server.

### `log`

```yaml
- id: carry_on
  log:
    level: warn
    message: notification failed, continuing anyway
    fields:
      why: ${steps.notify.error}
```

Writes a message to the run's log. `message` is required; `level` is `info`
(the default), `warn`, or `error`; `fields` is up to 32 string pairs. It
produces no outputs.

### `http`

```yaml
- id: lookup
  http:
    method: GET
    url: https://api.example.com/orders
    query:
      status: open
    headers:
      Accept: application/json
    parse_json: true
    expect: ${response.status_code == 200 || response.status_code == 404}
    outputs:
      found: ${response.status_code == 200}
      ids: '${(response.status_code == 200) ? response.json.orders.map(o, o.id) : []}'
```

| Input | Meaning |
| --- | --- |
| `url` | Required. An absolute `http` or `https` URL. |
| `method` | `GET` (default), `POST`, `PUT`, `PATCH`, or `DELETE`. |
| `headers` | Request headers. An entry may be a secret reference. |
| `query` | Query parameters, escaped and appended to the URL. |
| `body` | A raw string body, sent as written. |
| `json` | A value sent as JSON, with `Content-Type: application/json`. |
| `form` | A map sent URL-encoded, as OAuth token endpoints expect. |
| `bearer` | A secret reference sent as `Authorization: Bearer …`. |
| `credential` | A deployment-configured target that mints a short-lived token for this request. [Secrets](SECRETS.md#short-lived-credentials-instead-of-stored-ones) |
| `parse_json` | Parse the body as JSON into `json`. A malformed body fails the step. |
| `expect` | A condition over `response` that decides success. Without it, any 2xx succeeds. |
| `outputs` | Replace the default outputs with the names you define, computed from `response`. |
| `retry_on_unknown_outcome` | Allow retrying a `POST` or `PATCH` whose effect is unknown. |

`body`, `json`, and `form` are mutually exclusive, as are `bearer` and
`credential`.

**`expect:` and `outputs:` run after the response arrives,** against a
`response` root: `response.status_code` (int), `response.body` (string),
`response.headers` (a map from name to a list of values), and `response.json`
(only with `parse_json: true`). The rest of the run's scope is visible too.
Shaping with `outputs:` is how a step keeps only what later steps need, instead
of carrying a whole response body through the run's history.

**How the result is classified.** A 2xx succeeds. A 429 is rate limiting and is
retried, honoring `Retry-After` up to five minutes. Other 4xx responses fail
permanently. A 5xx or a transport failure is retried, with one exception: a
`POST` or `PATCH` that got a 502, 503, or 504, or no answer at all after being
sent, may have taken effect, so it fails permanently as `UpstreamUnknown`
unless the step sets `retry_on_unknown_outcome: true` to say the endpoint is
idempotent. An `expect:` that is false on a 2xx response fails permanently; on
any other status, that status's classification applies.

**What it may reach.** By default a request may go only to public addresses:
loopback, private, link-local, and cloud metadata addresses are refused at
connection time, every redirect is re-checked, and HTTPS-to-HTTP redirects are
refused. A deployment replaces this with an egress policy
(`--egress-policy`), and a local run can allow loopback with
`FLOWSTATE_ALLOW_LOOPBACK_EGRESS=true`. Each request is bounded at 30 seconds,
a 1 MiB response body, and 5 redirects unless the egress policy says otherwise.

### `exec`

```yaml
- id: version
  exec:
    argv: [git, --version]
    dir: /var/lib/flowstate/workspaces/demo
    env:
      LANG: C
```

Runs one program and returns `exit_code`, bounded `stdout` and `stderr`, and how
it ended (`outcome`, `signal`, `duration_ms`). `argv` is a list, never a shell
string; the program is a bare name looked up in the operator's allowlist, the
environment is assembled from nothing (the operator's `env`, variables the
policy passes through from the worker, and the step's `env:` only for keys the
policy lists as authored), and a nonzero exit is an output to branch on, not a
failure. It is denied unless the operator loads `--exec-policy` (or
`FLOWSTATE_EXEC_POLICY`) on the command that runs tasks; a policy limits
executables, directories, arguments, environment, time and output, and an
erroring rule denies. A secret may not appear
in `argv`. `exec` is not a sandbox: it limits what a Flowfile may ask
for, not what the program can do once it runs. See [DEPLOYMENT.md](DEPLOYMENT.md)
and `examples/exec-checks/`.

## Control flow

### Choosing a branch: `switch:`

```yaml
- id: on_event
  switch:
    value: ${inputs.action}
    cases:
      - case: opened
        steps:
          - id: triage
            log:
              message: '${"triaging #" + string(inputs.number)}'
      - case:
          - closed
          - merged
        steps:
          - id: archive
            log:
              message: '${"archiving #" + string(inputs.number)}'
      - case: synchronize
        steps: []
    default:
      steps:
        - id: unhandled
          log:
            level: warn
            message: '${"unhandled action: " + inputs.action}'
```

`value:` is evaluated once. Cases are literal values (or lists of them) tried in
order; the first match runs its `steps:`, with no fallthrough. `steps: []`
handles a case by doing nothing. When nothing matches, `default:` runs; with no
`default:`, nothing does. The switch records `value` and `case` (`null` when no
case matched), and steps inside the branch that ran join the enclosing scope, so
a later step can read `steps.triage.<output>` when that branch ran.

The validator checks a switch against what its value can be. Over an `enum`
input, or a step whose possible values are string literals, it refuses a case
that cannot occur, requires every value to be covered when there is no
`default:`, and refuses a `default:` that can never run. A computed case is
refused: a comparison that is not a literal match is what `if:` is for.

Prefer `switch:` over several steps with `if:` testing the same value for
equality; only the switch lets the validator check the branches.

### Repeating over a list: `for_each:`

```yaml
- id: enrich
  for_each:
    items: ${inputs.records}
    as: record
    max_parallel: 4
    steps:
      - id: lookup
        continue_on_error: true
        http:
          url: ${"https://api.example.com/records/" + record}
```

`items:` must produce a list, of at most 1,000 elements. The body `steps:` run
once per item, with the item bound to the `as:` name (default `item`). Each
iteration sees the steps before the loop, not other iterations.

`max_parallel:` lets up to that many iterations run at once on the durable
driver (up to 1,000); without it, iterations run one at a time. The local driver
always runs them one at a time.

The step's output is `results`, a list in input order, one map per iteration
from body step id to that step's outputs:
`${steps.enrich.results.map(r, r.lookup.status_code)}`. A skipped body step is
absent from its entry; a tolerated failure carries `error` and the `item` it
failed on.

A failing iteration fails the loop. Sequentially, that stops at the first
failure; with `max_parallel:`, the iterations already started finish, and the
first failure by position is reported.

To cross two lists, build the combinations in `items:`
([examples/matrix-fan-out](../examples/matrix-fan-out/workflow.yaml)).

### Running branches at once: `parallel:`

```yaml
- id: checks
  parallel:
    - steps:
        - id: check_config
          log:
            message: config ok
    - steps:
        - id: check_quota
          log:
            message: quota ok
```

Each branch runs concurrently on the durable driver and in order on the local
driver. A branch sees the steps before the block and its own steps, not the
other branches'. The block waits for every branch. When all succeed, the outputs of the
steps written directly in each branch join the enclosing scope (a step nested
in a block inside a branch does not join), so a later step reads
`steps.check_quota.<output>`; the `parallel` step itself has none. If a branch
fails, the others still finish and the first failure by branch position is
reported.

### Starting work early: `async:`

```yaml
- id: build
  async: true
  http:
    method: POST
    url: https://ci.example.com/build
- id: provision
  async: true
  http:
    method: POST
    url: https://infra.example.com/machines
- id: deploy
  http:
    method: POST
    url: https://infra.example.com/deployments
    body: ${steps.provision.body}
```

An `async: true` task step starts and the run moves on without waiting. The first
later step that mentions it, anywhere in its `if:`, `vars:`, or inputs, waits for
it to finish; a scope that ends also waits for everything it started, so nothing
is left running. Above, `build` and `provision` run together, and `deploy` waits
only for `provision`.

`async:` is for task steps in sequential positions: not on waits, `value:`, or
blocks, and not inside a `for_each` body or `parallel` branch. At most 100 async
steps can be outstanding in one scope. A failure is reported against the async
step and surfaces where it is joined.

### Repeating until done: `loop:`

```yaml
- id: countup
  loop:
    as: acc
    init:
      "n": 1
      sum: 0
    update:
      "n": ${acc.n + 1}
      sum: ${acc.sum + acc.n}
    until: ${acc.n >= inputs.target}
    max_iterations: 100
    steps:
      - id: term
        log:
          message: ${"n=" + string(acc.n)}
```

A `loop` runs its body, then evaluates `until:`; it stops when `until:` is
true, so the body always runs at least once. `until:` can read the body's step
outputs from that iteration.

To carry state between iterations, name it with `as:` and give `init:`
(evaluated once, before the first iteration) and `update:` (evaluated after each
iteration that does not stop the loop). The state is one value, so use a map for
several fields. Without state, a loop is a bounded retry-until, such as polling
([examples/loop-poll-until](../examples/loop-poll-until/workflow.yaml)).

`max_iterations:` defaults to 1,000 and can be raised to 100,000. Reaching it
without `until:` holding **fails** the step, rather than stopping quietly.

The step's outputs are `results`, as for `for_each`, and `state`, the final
state: `${steps.countup.state.sum}`, not the `as:` name, which exists only
inside the loop. A loop cannot contain another `loop:`, even through a `call:`;
a `for_each` inside a loop body is fine.

A loop that waits for a signal in its body is how a long-lived entity is written:
an order that accepts updates until it closes, or a reconciler that wakes on a
schedule or an event. See [examples/entity-order](../examples/entity-order/workflow.yaml)
and [examples/deployment-reconciler](../examples/deployment-reconciler/workflow.yaml).

### Reusable workflows: `call:`

```yaml
- id: provision
  call: ./workflows/provision-tenant.yaml
  with:
    tenant: acme
- id: announce
  log:
    message: ${"tenant ready at " + steps.provision.url}
```

`call:` runs another Flowfile as a step. The path is relative to the calling
file, cannot climb out of its directory with `..`, and is read when the caller
compiles: the callee's whole specification is embedded in the caller's, so a
worker never reads a file.

The callee is isolated. It sees only the inputs `with:` binds (checked against
its declared `inputs:`), its own `vars:`, and the run's `run.*` and `trigger.*`
facts. It cannot see the caller's steps. The call step's outputs are exactly the
callee's declared `outputs:`.

`digest: sha256:<hex>` beside `call:` pins the callee's content: if the file
changes, the caller no longer compiles until someone updates the pin, and the
error prints the new digest.
[examples/pinned-call](../examples/pinned-call/workflow.yaml) shows it.

Calls nest up to 8 deep, and cycles are refused. A callee's `undo:` actions join
the caller's compensation. The callee runs inside the caller's own history, not
as a separate Temporal child workflow. A file compiled from bytes, as through
the API's `Compile` or `embed.Compile`, has no directory to resolve a call
against, so it cannot call.

## Waiting and signals

On the durable driver, a wait is state in Temporal. No thread, process, or
worker is held while a run waits, for a second or for a month.

### `sleep:` and `wait_until:`

```yaml
- id: settle
  sleep: 5m
- id: grace
  sleep: '${(inputs.plan == "enterprise") ? days(7) : days(1)}'
- id: embargo
  wait_until: ${timestamp(inputs.release_at)}
```

`sleep:` takes a duration literal (`30s`, `5m`, `1h`, `7d`) or an expression
producing a duration (or a string like `"90s"`). A number is refused, because it
does not say what unit it counts. Zero is allowed; negative fails the run.

`wait_until:` takes a timestamp, an RFC 3339 string, Unix seconds, or a duration
from now. A moment in the past releases at once. A boolean is refused: a
condition over the run's state cannot change while the run is waiting, so wait
for a signal instead.

### `wait_for_signal:`

```yaml
- id: approval
  wait_for_signal:
    name: deploy-approved
    prompt: ${"Approve deploying " + inputs.version + "?"}
    timeout: 24h
```

The step waits for a signal named `name`. Its outputs:

- `payload`: the JSON object the sender sent, read as
  `${steps.approval.payload.approved}`. Empty when the wait timed out.
- `sender`: who sent it, as the server verified: `sender.identity.subject`,
  `.issuer`, `.namespace`, `.principal`, and `.deployment`, `sender.accepted_at`, and
  `sender.local` (`true` for a local rehearsal). A sender cannot forge these;
  they are outside `payload`. `sender.identity` has the shape of
  `run.identity` less `claims` (the sender is a third party, and a wait's outputs are
  durable history) plus `deployment`; `principal` is `<issuer>#<subject>`, and `""`
  for a local or unauthenticated sender or one missing either half.
- `timed_out`: `true` if `timeout:` lapsed first.

**A timeout is not a failure.** When `timeout:` lapses, the step succeeds with
`timed_out: true` and the workflow decides what that means. Without `timeout:`,
the step waits as long as the run lasts.

**`prompt:`** is the question the gate asks, shown by `flow get`, `flow watch`,
the `Get` RPC, and the MCP approval card. It is evaluated when the wait starts,
cannot include a sensitive input or a secret, and is cut at 2 KiB. In a
workflow that declares a sensitive output, a reader who is not shown sensitive
values sees `[prompt withheld: this run declares a sensitive output]` instead,
because nothing checks that a prompt avoids what such an output reads.

**A signal can arrive early.** One delivered before the run reaches the wait is
held and consumed when it does, including across Continue-As-New.

**Sending a signal:**

- durably: `flow signal <workflow-id> deploy-approved --data '{"approved": true}'`,
  or the `Signal` RPC;
- to a local run: `flow run local --signal 'deploy-approved={"approved": true}'`,
  given up front;
- in a test: a case's `signals:`.

Who may send it is the workflow's [`signals:` policy](#who-may-send-a-signal-signals).
A payload is at most 64 KiB.

### Shaping a wait's outputs

`outputs:` on a wait replaces `payload`, `sender`, and `timed_out` with names
you choose, computed once when the wait resolves:

```yaml
- id: gate
  wait_for_signal:
    name: decision-made
    timeout: 2h
    outputs:
      approved: ${payload.?approved.orValue(false)}
      responded: ${payload.?approved.hasValue()}
      approver: ${sender.identity.subject}
      timed_out: ${timed_out}
```

Inside, `payload`, `sender`, `timed_out`, and `now` are bare names. Every later
step then reads one well-named fact, `steps.gate.approved`, instead of
repeating the decision logic.

### `wait_for_signals:`

```yaml
- id: batch
  wait_for_signals:
    name: order-placed
    max_batch: 50
    timeout: 5m
```

Waits for the first signal, then takes every other signal of that name already
waiting, up to `max_batch` (default and maximum 128), without waiting again.
Anything past the limit stays for the next drain. Outputs: `deliveries` (a list
of `{payload, sender}`, oldest first), `count`, and `timed_out`. This drains a
burst in one step where a loop would spend an iteration per event.

### Counting approvals: `quorum:`

`wait_for_signals:` can decide a vote instead of draining a burst:

```yaml
- id: gate
  wait_for_signals:
    name: release-approved
    max_batch: 3
    timeout: 1h
    quorum:
      approve: 2
      distinct: true
      exclude:
        - ${run.identity.subject}
      veto: ${has(payload.approved) && payload.approved == false}
```

With `quorum:` the step takes deliveries one at a time until the vote is
decided or `timeout:` lapses; later deliveries stay buffered. A delivery
approves when its payload has `approved: true` and its sender passed the
`signals:` policy. `approve` is the count needed; `distinct` (the default)
counts each verified identity once, and a delivery with no identity never
counts; `exclude` lists subjects whose approvals do not count, though they may
still veto (`${run.identity.subject}` is the four-eyes rule); `veto` is a
predicate over `payload` and `sender` that ends the wait at once. The step keeps
the batch outputs `deliveries`, `count` and `timed_out` and adds `decision`
(`approved`, `vetoed` or `timed_out`), `approvals` (the deliveries that
counted) and `vetoed_by` (bound only when the decision is `vetoed`).
An `approve` larger than the `signals:` allow-list can supply is refused by
`flow validate`. See `examples/signal-quorum/`.

### The clock: `now`

`now` is bound only inside a wait's own expressions: `sleep:`, `wait_until:`,
a wait's `timeout:`, `prompt:`, and `outputs:`. Durably it is Temporal's
replay-safe workflow time, so a deadline computed from it survives a restart:

```yaml
- id: remind
  wait_until: ${now + days(3)}
- id: sign_off
  wait_for_signal:
    name: signed-off
    timeout: '${(timestamp(inputs.due) > now) ? (timestamp(inputs.due) - now) : duration("0s")}'
```

Everywhere else, `now` is an error with an explanation. To use a time in a task,
compute the length in the wait, or pass the time in as an input.

## Failure, retries, and compensation

### Retries and timeouts

Every task step is retried by default: up to **5 attempts**, starting 1 second
apart and doubling to at most 30 seconds, with each attempt bounded at **2
minutes** and the whole step at **10 minutes**. Adjust any of it on the step:

```yaml
- id: deploy
  timeout: 30s
  total_timeout: 2m
  retry:
    attempts: 3
    interval: 1s
    backoff: 2.0
    max_interval: 10s
  http:
    method: POST
    url: https://deploy.example.com/releases
```

| Key | Default | Meaning |
| --- | --- | --- |
| `timeout` | `2m` | How long one attempt may take. A timed-out attempt can be retried. |
| `total_timeout` | `10m` | How long the step may take across all attempts and waits between them. When only `timeout:` is set and `timeout × attempts` is larger, that product is used instead. |
| `retry.attempts` | 5 | Total attempts, including the first. `1` disables retries. |
| `retry.interval` | `1s` | Wait before the second attempt. |
| `retry.backoff` | 2.0 | Multiplier for each later wait; at least 1. |
| `retry.max_interval` | `30s` | The longest wait between attempts. |

These keys work on task steps only; each has literal values. A step's inputs are
resolved once, before the first attempt, so an error in an input expression is
not retried.

**What is retried** is decided by the failure, not by preference. Transient
failures (`Upstream`, `Timeout`, `RateLimited`, `Internal`) are retried.
Permanent ones (`InvalidInput`, `Expression`, `PolicyDenied`, `LimitExceeded`,
`UnknownTask`, and `UpstreamUnknown`, where the effect may already have
happened) are not. A policy denial is never retried.

There are three different timeouts, and they promise different things: a step's
`timeout:` bounds one attempt, `total_timeout:` bounds the step, and a
`wait_for_signal:`'s `timeout:` bounds how long a gate waits and ends in
`timed_out: true` rather than a failure.

### Tolerating a failure: `continue_on_error:`

```yaml
- id: notify
  continue_on_error: true
  http:
    method: POST
    url: https://chat.example.com/hooks/deploys
- id: note
  if: ${has(steps.notify.error)}
  log:
    level: warn
    message: '${"notification failed: " + steps.notify.error}'
```

A step with `continue_on_error: true` that fails, after its retries, does not
fail the run. Its outputs become `{error: "<what happened>"}`, and `error` is
absent when the step succeeded, so test it with `has()`. Cancellation, and an
error in the step's own `if:`, are never tolerated.

### Tolerating or retrying by kind

`continue_on_error:` and `retry:` can name the failure kinds they mean:

```yaml
- id: notify
  retry:
    attempts: 3
    except: [RateLimited]
  continue_on_error: [Upstream, RateLimited]
  http:
    method: POST
    url: https://hooks.example.com/notify
```

| Spelling | Meaning |
| --- | --- |
| `continue_on_error: true` | Tolerate every failure. |
| `continue_on_error: [Kind, ...]` | Tolerate only these kinds; any other ends the run. |
| `retry: {only: [Kind, ...]}` | Retry only these kinds. |
| `retry: {except: [Kind, ...]}` | Never retry these kinds. |

A kind is a built-in kind or one declared under `errors:`. The retry lists only
narrow what would be retried anyway; naming a kind that is never retried is
refused. A step writes at most one of `only:` and `except:`. A tolerated step
also records `steps.<id>.failure` with `kind`, `message` and `retryable`, so
compare `failure.kind` to a name and not a substring of `error`. See
`examples/failure-kinds/`.

### Naming a failure: `errors:` and `fail:`

A refusal the workflow means has a name. `errors:` declares them and `fail:`
raises one:

```yaml
errors:
  InsufficientFunds:
    description: the account cannot cover the amount requested
steps:
  - id: reject_overdraft
    if: ${inputs.amount_cents > inputs.balance_cents}
    fail:
      error: InsufficientFunds
      message: ${"balance " + string(inputs.balance_cents) + " cannot cover the amount"}
```

The run fails with `InsufficientFunds` as its kind, on both drivers, and a
client reading the run gets that name as `error.kind`. A name starts with a
capital letter and may not be a built-in kind. `fail:` is evaluated in workflow
code and schedules nothing, so it refuses `retry:`, `timeout:`, `total_timeout:`
and `undo:` and cannot be `async:`. The `message` is an expression of at most
4096 bytes and may not read a secret or a `sensitive` input, because it is
written to history. A declared error is never retried. See
`examples/declared-errors/`.

### Compensation: `undo:`

```yaml
- id: network
  http:
    method: POST
    url: https://infra.example.com/networks
    parse_json: true
    outputs:
      id: ${response.json.id}
  undo:
    http:
      method: DELETE
      url: ${"https://infra.example.com/networks/" + steps.network.id}
```

`undo:` names one task that reverses a task step's effect. It is registered only
when the step succeeds, with its inputs resolved at that moment, so it can read
the step's own outputs (`steps.network.id` above). If the run later fails, or is
cancelled, the registered compensations run in reverse order: the last thing
done is the first undone. This is the saga pattern.

- A failed, tolerated, or skipped step registers nothing, and a run that
  succeeds compensates nothing.
- Each compensation runs once, under the default retry and timeout policy, and
  one that fails does not stop the others. The run's error lists what was and was
  not undone. The run's status stays `FAILED`.
- Compensations inside `for_each` bodies and `parallel` branches unwind in a
  fixed order (by position, not by completion time), and a callee's
  compensations join the caller's.
- `undo:` goes on task steps only. For a `call:`, put it on the callee's steps.

[examples/saga-provisioning](../examples/saga-provisioning/workflow.yaml) fails
on purpose to show the unwinding.

### Cancelling and terminating

`flow cancel` (or the `Cancel` RPC) asks a run to stop. The current step's
activity is cancelled, registered compensations run (within a two-minute
budget), and the run ends `CANCELED`. `flow terminate` stops the run at once and
runs no workflow code, so nothing is compensated: use it only when cancelling
does not work.

## Starting runs: triggers

Unless its `manual:` says otherwise, any workflow can be started by hand with
`flow run` or the `Run` RPC. `triggers:` adds other ways, and narrows the manual
one. Declaring a trigger does nothing on
its own: a schedule starts firing only when someone runs
`flow schedule create`, and a webhook is served only by a server started with
`flow server --webhook`. `flow run local` ignores triggers.

`triggers:` is written as a mapping when it has no webhook:

```yaml
triggers:
  schedule:
    cron: 0 7 * * MON-FRI
    time_zone: Europe/Dublin
  manual:
    require_reason: true
```

and as a list when it has any:

```yaml
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.body.id}
    with:
      order_id: ${event.body.data.object.metadata.order_id}
      amount: ${event.body.data.object.amount}
  - schedule:
      every: 1h
```

### Manual starts: `manual:`

With no `manual:`, any authenticated caller in the workflow's tenant may start
it. `manual:` can only narrow that:

- `manual: denied` refuses manual starts. The workflow must have another
  trigger.
- `require_reason: true` requires `flow run --reason "..."`, recorded on the run.
- `allow: ${...}` is one predicate over the caller that says who may start it:
  `allow: ${sender.identity.claims.team == "ops"}`, or
  `allow: ${sender.identity.principal in ["https://issuer.example.com#oncall@example.com"]}`
  for named callers, each written `"<issuer>#<subject>"`. It reads
  `sender.identity.{principal,subject,issuer,namespace,claims}` (the verified
  caller) and `inputs` (the arguments submitted with this start), and nothing
  else; there is no run yet, so reading `run` is a compile error. Only a clean
  `true` allows, and a caller with no authenticated principal is refused. A
  predicate that reads `inputs` must also read `sender.identity.claims`.

`flow run local` and `flow test` are not gated by `manual:`.

### Schedules

| Key | Meaning |
| --- | --- |
| `cron` | A cron expression, or a list of them. Five fields, or six with a year, or seven with seconds first, or `@daily`-style shorthands. |
| `every` | A fixed interval, at least one minute. |
| `calendars` | Calendar specifications, for what cron cannot say. Each entry matches on `second`, `minute`, `hour`, `day_of_month`, `month`, `year` and `day_of_week`, and may carry a `comment`. |
| `time_zone` | An IANA time zone for `cron` and `calendars`. UTC when unset. |
| `jitter` | Delay each firing by a random amount up to this long. |
| `overlap` | What to do when a firing finds the previous run still going: `skip` (default), `buffer_one`, `buffer_all`, `cancel_other`, `terminate_other`, or `allow_all`. |
| `start_at`, `end_at` | RFC 3339 bounds on when the schedule fires. |
| `catchup_window` | How late a missed firing may still run, from one minute to 30 days. The server uses one hour if unset. |
| `pause_on_failure` | Pause the schedule when a run it started fails. |

`cron`, `every`, and `calendars` combine. A schedule's inputs are bound once,
when it is created (`flow schedule create workflow.yaml --input k=v`), and
`flow schedule create` refuses a cadence that can never fire or fires more than
once a minute. `--backfill` runs past intervals at creation.
[examples/schedule-overlap-policies](../examples/schedule-overlap-policies/workflow.yaml)
explains when each overlap policy is right.

### Webhooks

A `- webhook: <name>` entry lets a signed HTTP delivery start a run. The server
receives it at `POST /webhooks/<workflow>/<webhook>` when started with
`flow server --webhook workflow.yaml`.

| Key | Meaning |
| --- | --- |
| `verify` | Required. How to check the signature: `hmac_sha256` (an `X-Flowstate-Signature` HMAC of the body) or `stripe` (Stripe's signature scheme), each keyed by a secret reference. |
| `when` | Optional. A boolean over the delivery that admits it, such as `${event.body.action == "opened"}`. Only a clean `true` admits; `false` answers `204` and starts nothing, and an expression that errors, is not a bool, or exceeds its bound is refused. Applies to a `signal:` webhook too, before `correlate:` runs. |
| `idempotency_key` | Required. An expression over the delivery that names the *event*, such as `${event.body.id}`. A redelivery of the same event joins the run the first one started. Never key on a signature header, which changes on every retry. |
| `with` | Maps the delivery to the workflow's inputs. Checked against `inputs:` both ways: every required input must be bound. |
| `signal` | Instead of starting a run, deliver a signal to the run whose entity key `correlate:` computes. See [examples/webhook-approval-bridge](../examples/webhook-approval-bridge/workflow.yaml). |

Inside a webhook's expressions, `event.headers` and `event.body` are the only
names in scope; the run does not exist yet. `flow test` can replay a stored
delivery, including one whose signature does not verify
([examples/webhook-trigger](../examples/webhook-trigger/workflow.yaml)).

### What a run knows about its start: `trigger`

`trigger.kind` is `manual`, `schedule`, or `webhook`; `trigger.name` is the
schedule or webhook name; `trigger.principal` is who or what started it; and
`trigger.delivery_id` identifies a webhook delivery. Use them for behavior, such
as not paging anyone for a scheduled run:

```yaml
- id: page
  if: ${trigger.kind != "schedule"}
  log:
    message: paging on-call
```

Authorization belongs in `manual:` and `signals:`, not in an `if:` on
`trigger`.

### One run at a time: `concurrency:`

```yaml
concurrency:
  key: ${inputs.cluster}
  on_conflict: reject
```

At most one run of the workflow may hold a key at a time, per tenant. The key is
computed from `inputs` when the run is submitted. `on_conflict:` decides what a
second submission gets:

- `reject` (default): refused, naming the run that holds the key.
- `join`: answered with the running run instead of starting another.
- `terminate_other`: the running run is terminated (without compensation) and
  the new one starts.

It does not queue. It cannot be combined with a webhook or schedule trigger,
whose runs already have their own addressing. `flow run local` is unaffected.
[examples/exclusive-cluster-drain](../examples/exclusive-cluster-drain/workflow.yaml)
shows all three.

## Who may act on a run

### Who may send a signal: `signals:`

```yaml
signals:
  deploy-approved:
    allow: ${(sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver && sender.identity.claims.team == "release-managers" || sender.identity.claims.role == "sre-lead") && sender.identity.principal != run.identity.principal}
```

Each entry is a signal name the workflow waits for, and `allow:` is one `${...}`
predicate that says which senders may deliver it. It reads
`sender.identity.{principal,subject,issuer,namespace,claims}` (the verified sender;
`principal` is `"<issuer>#<subject>"`, and empty when either half is missing),
`run.identity` (the starter, with the same fields) and `inputs`, and nothing else:

- `sender.identity.principal == "<issuer>#<subject>"` names one sender exactly. The
  right-hand side may be an expression over `inputs`, evaluated on every delivery.
- `sender.identity.claims.team == "release-managers"` matches a claim the server was
  configured to record (`flow server --identity-claim team`). A missing claim is an
  error, which refuses the sender.
- `sender.identity.namespace == "payments"` is the sender's tenant.
- `sender.identity.principal != run.identity.principal` requires that the sender is not
  the person who started the run: separation of duties.

Only a clean `true` allows: an error (a missing claim key, an unrecorded starter that
the predicate reads), a result that is not a bool, or an expression over its cost bound
all refuse the sender. A predicate that reads `inputs` must also read
`sender.identity.claims` or `run.identity`: whoever starts the run chooses its inputs, so
a predicate over them alone would let them name their own approver. When a name comes
from `inputs`, require exactly one `#` in the principal first
(`sender.identity.principal.split("#").size() == 2 && sender.identity.principal ==
"https://issuer.example.com#" + inputs.approver`), because an unauthenticated sender's
`principal` is empty and equals an empty input, and a computed name could otherwise be
matched by a principal holding a second `#`. Write conjunctions: the narrowing check is
syntactic, and a predicate that reads `inputs` records the inputs it names in the policy
scope memo (64 KiB cap). It may not read a `sensitive:` input or name no input at all
(`inputs[k]`, `inputs` passed whole); both are refused.

`debug:` takes the same predicate.

The server checks the policy before the signal reaches Temporal, and refuses a
sender who does not match with `PermissionDenied`. A signal name with **no**
policy may be sent by any authenticated caller who can see the run. Local
rehearsals and tests apply the same policy to the sender you give them, and a
durable run always refuses a sender that was only asserted locally.

### Who may pause a run: `debug:`

```yaml
debug:
  allow: ${sender.identity.claims.team == "sre"}
```

`debug:` has the same grammar as one `signals:` entry, so `allow:` is one predicate
over the same scope (`sender`, the debugged run's `run.identity` and `inputs`). It says who may attach
a debugger to a durable run — hold it at a step boundary, step it, and set
breakpoints — under a lease that expires on its own. Evaluating expressions
against it, or setting a breakpoint that carries a condition or a log message,
also needs the `workload.debug_inspect` action.
**Without `debug:`, nobody can**, including the person who started the run.
Local debugging needs no policy. See
[Debugging](DEBUGGING.md#debugging-a-durable-run).

### Labels are not policy

`labels:` are for finding runs. Nothing authorizes on them.

## Secrets

```yaml
- id: health
  http:
    bearer: ${secret('env:API_TOKEN')}
    url: https://api.example.com/status
```

`${secret('scheme:name')}` is a reference, resolved on the worker inside the
task that uses it; the value never enters the run's history. It must be the
whole value of an input that accepts one, such as `http`'s `bearer:` or one
entry of its `headers:`. It is refused in `vars:`, in anything the workflow
evaluates itself, and in text. [Secrets and credentials](SECRETS.md) covers
where references are allowed and how a deployment resolves them.

### Sensitive values

`sensitive: true` on an input or output withholds it from displays: `flow get`,
`flow watch`, test output, and the MCP server show `[redacted: <name>]` unless
the reader passes `--reveal-sensitive`. A server withholds these values before
they leave it, and honours `--reveal-sensitive` only for a caller whose trust
policy entry lists the `workload.reveal_sensitive` action. It is display etiquette, not
protection: the value is stored in the run's history like any other, in the
clear unless the deployment encrypts history with a payload keyring
([Payload encryption](ENCRYPTION.md)). The validator refuses a `log:` message
that prints a sensitive input directly, and a wait `prompt:` that includes one
at all.

## Plugins

A plugin task is written `<plugin>.<task>:`, such as `slack.post:` or
`github.pull_request_get:`. Declare the plugins a file needs, with a minimum
version:

```yaml
plugins:
  slack: v0.1.0
steps:
  - id: announce
    slack.post:
      channel: C0123456789
      message_key: ${inputs.announcement_id}
      text: release is out
      token: ${secret('env:SLACK_BOT_TOKEN')}
```

A submission is refused when the deployment's plugin is older than, or a
different major version from, what the file declares, and the exact versions are
recorded on the run. Without `plugins:`, a worker lacking the plugin fails the
step with `unknown task` instead.

Validation knows a plugin's tasks only when told where the plugin is:
`flow validate --plugin-dir ./plugins` launches it to read its schema, and
`--plugin-catalog catalog.json` reads a saved catalog without running anything.
The first-party plugins are listed in [plugins/](../plugins/), each with its
tasks and bounds.

## Editions and migration

`edition:` names the grammar a file is written in, and is required. This build
compiles `v2026.4` only. A file from an older edition is refused with an
instruction to run `flow fix`, which rewrites it in place to the current edition,
preserving comments, and changes nothing if any part of the rewrite is
ambiguous. `flow fix --check` reports without writing.

An edition is a property of the file, not of a run: a run carries its compiled
specification, so changing the grammar never affects a run in flight.

The retired spellings `flow fix` rewrites include `task:` blocks (now the task
name as the key), `echo:` and `printf:` (now `log:`), `cel:` (now `value:`),
`iterator:` (now `as:`), bare step references (now `steps.<id>`), and
`has(x.y) && x.y` (now `x.?y.orValue(false)`), and the who-may-act forms (an `allow:`
list of rules, `distinct_from_starter:` and `manual: allowed_principals:`, now one
`allow: ${...}` predicate; see "What `flow fix` writes for who may act" in
[DSL.md](DSL.md)).

## Limits

| Limit | Value |
| --- | --- |
| Flowfile size | 1 MiB, 64 levels of nesting |
| Top-level steps | 100 |
| Steps in a body or branch | 100 |
| `for_each` items | 1,000 |
| `for_each` `max_parallel` | 1,000 |
| `loop` iterations | 1,000 by default; `max_iterations` up to 100,000 |
| `parallel` branches | 100 |
| `switch` cases | 100 |
| Outstanding `async:` steps per scope | 100 |
| `call:` depth | 8 |
| Inputs, outputs, vars, labels | 64 each |
| `wait_for_signals` batch | 128 |
| Signal payload | 64 KiB |
| Wait prompt | 2 KiB |
| A step's outputs | about 2 MiB |
| `for_each`/`loop` `results` | about 500 KiB |
| Activities between Continue-As-New points | 5,000 in a `parallel:` block or concurrent `for_each` |

A durable run continues as new automatically as its history grows, carrying
only what later steps read. Work that cannot be split at a step boundary (a
`parallel:` block, or a `for_each` with `max_parallel:`) is bounded up front, so
the run is refused before it starts rather than failing partway.

## Keys at a glance

**Top level:** `edition`, `name`, `labels`, `description`, `plugins`, `types`,
`errors`, `functions`, `inputs`, `triggers`, `concurrency`, `signals`, `debug`,
`vars`, `steps`, `outputs`.

**Type declaration:** `description`, `fields` (each written like an input), `must`.
**Function declaration:** `description`, `params`, `returns`, `body`. **Error
declaration:** `description`.

**Input declaration:** `type`, `values`, `required`, `default`, `description`,
`example`, `sensitive`, `min_len`, `max_len`, `min_items`, `max_items`, `must`.

**Output declaration:** `value`, `type`, `values`, `description`, `must`,
`sensitive`.

**Step:** `id`, `description`, `if`, `vars`, `async`, `timeout`,
`total_timeout`, `retry` (`attempts`, `interval`, `backoff`, `max_interval`, `only`, `except`),
`continue_on_error`, `undo`, `with`, `digest`, and one kind: a task name,
`value`, `switch` (`value`, `cases` with `case` and `steps`, `default`),
`for_each` (`items`, `as`, `max_parallel`, `steps`), `parallel` (a list of
`steps`), `loop` (`as`, `init`, `update`, `until`, `max_iterations`, `steps`),
`call`, `fail` (`error`, `message`), `sleep`, `wait_until`, `wait_for_signal` (`name`, `timeout`, `prompt`,
`outputs`), `wait_for_signals` (`name`, `max_batch`, `timeout`, `prompt`,
`outputs`, `quorum` with `approve`, `distinct`, `exclude`, `veto`).

**Triggers:** `manual` (`denied`, or `require_reason` and an `allow` predicate),
`schedule` (`cron`, `every`, `calendars`, `time_zone`, `jitter`, `overlap`,
`start_at`, `end_at`, `catchup_window`, `pause_on_failure`; a calendar has
`second`, `minute`, `hour`, `day_of_month`, `month`, `year`, `day_of_week`,
`comment`, and a range within one is written `start`, `end`, `step`), `webhook`
(`verify`, `when`, `idempotency_key`, `with`, `signal` with `name`, `correlate`,
`with`).

**Concurrency:** `key`, `on_conflict`. **Signal policy:** `allow` (one `${...}`
predicate). **Debug policy:** the same.

`needs` and `assert` are reserved for future versions of the grammar and are
refused today.
