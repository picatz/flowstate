# Testing workflows

A `*.test.yaml` file beside a workflow says what the workflow should do under
conditions you choose: which inputs it gets, what each task answers, who sends
which signal and when. `flow test` runs those cases in milliseconds, with no
server, no Temporal, and no network, so you can run it on every edit.

```console
$ flow test examples/release-approval/
PASS  examples/release-approval/workflow.test.yaml: an approval rolls out every planned target
PASS  examples/release-approval/workflow.test.yaml: a rejection rolls out nothing
PASS  examples/release-approval/workflow.test.yaml: nobody answering within the hour counts as a rejection
examples/release-approval/workflow.test.yaml  6/6 steps reached

1 file · 3 cases · 3 passed · 0.0s
```

New to Flowstate? [Get started](GETTING_STARTED.md) writes and tests that
workflow step by step. This page is the full account of the test file format
and the `flow test` command.

## What a test runs, and what it replaces

A case runs the real workflow through the local driver: the same compiled
specification and step executor as `flow run local` and a durable run. Every
`if:`, `switch:`, loop, retry, timeout, `undo:` compensation, `call:`, and
expression runs for real. Only two things are replaced:

- **Tasks.** Each task invocation is answered by a *stub* you declare. A task
  with no matching stub fails the step; `flow test` never lets a task run for
  real. A stub can replace a task inside a called workflow too, while the
  `call:` itself still runs; to replace the call itself, stub its step (see
  [Stubbing a call](#stubbing-a-call)).
- **Time.** Each case gets a virtual clock that starts at
  `2020-01-01T00:00:00Z` and jumps to the next deadline whenever nothing else
  can run. A one-day approval timeout lapses instantly; a `retry:` interval
  costs nothing.

Signals are scripted in the case. They reach the waiting step through the same
signal-policy check a server applies, so a case can prove who may and may not
answer a gate.

What a test does not prove is anything the local driver skips: persisted
history, recovery after a crash, Continue-As-New, server authentication, or a
real service behaving like its stub. Run the workflow durably for those; see
[Architecture](ARCHITECTURE.md#execution-model) for the exact boundary. Shared
conformance cases that both drivers run are what keep a test's answer the same
as production's.

## Anatomy of a test file

<!-- mirrors: examples/release-approval/workflow.test.yaml -->
```yaml
edition: v2026.4
defaults:
  inputs:
    version: 1.4.0
  stubs:
    - task: log
      returns: {}
tests:
  - name: an approval rolls out every planned target
    workflow: ./workflow.yaml
    signals:
      - name: release-approved
        payload:
          approved: true
    expect:
      ran: [plan, ask, approval, approved, rollout]
      outputs:
        approved: true
        targets: [api@1.4.0, worker@1.4.0]

  - name: a rejection rolls out nothing
    workflow: ./workflow.yaml
    signals:
      - name: release-approved
        payload:
          approved: false
    expect:
      ran: [plan, ask, approval, approved]
      others: skipped
      outputs:
        approved: false
        targets: [api@1.4.0, worker@1.4.0]

  - name: nobody answering within the hour counts as a rejection
    workflow: ./workflow.yaml
    expect:
      ran: [plan, ask, approval, approved]
      others: skipped
      outputs:
        approved: false
        targets: [api@1.4.0, worker@1.4.0]
```

The top-level keys:

| Key | Meaning |
| --- | --- |
| `edition` | The grammar edition, as in a workflow. `flow fix` stamps it. |
| `vars` | Values stated once and referenced as `${vars.<name>}` from fixtures. See [Values stated once](#values-stated-once-vars). |
| `defaults` | Fixture shared by every case in the file: `workflow`, `inputs`, `stubs`, `sender`, `check`. See [Sharing a fixture](#sharing-a-fixture). |
| `tests` | The cases. Required. |
| `coverage` | `allow_unreached:` names steps or `switch:` arms that no case needs to reach, each with a reason. |

The loader is strict: an unknown key is refused with the nearest legal one
(`unknown field "expct"; did you mean "expect"?`). A file holds at most 500
cases and 1 MiB.

## A case

| Key | Meaning |
| --- | --- |
| `name` | Required. Shown in results and matched by `--run`. |
| `skip` | A reason this case is not run. It is reported with the reason (`SKIP` in text, `<skipped>` in JUnit), counts in the summary, reaches no coverage, never fails the run, and is listed under `skipped` in the `-o json` report. On a table entry it skips every row. |
| `workflow` | The workflow file, relative to the test file. Usually stated once in `defaults:`. |
| `inputs` | The run's arguments, bound and checked exactly as a real run's are. |
| `stubs` | How each task invocation is answered. |
| `signals` | Signals to deliver, with an optional delay and sender. |
| `secrets` | Values for `${secret('scheme:name')}` references, for this case only. An unbound reference is refused. |
| `starter` | Who the run starts as, for the workflow's `signals:` policy. |
| `trigger` | How the run started: a replayed webhook delivery, or the `trigger.*` context set directly. |
| `cases` | Table rows that share this entry as a template. See [Tables](#one-fixture-many-rows). |
| `expect` | What must be true when the run ends. |

### Stubs

A stub names what it answers and how:

```yaml
stubs:
  - task: http                         # every http invocation...
    where: inputs.url.endsWith('/health')  # ...whose resolved url matches
    returns:
      status_code: 200
  - step: deploy                       # or one step, by id
    times: 1                           # answer once, then retire
    fails:
      kind: Upstream
      message: control plane briefly unreachable
  - step: deploy
    returns: {}
```

- **`task:` or `step:`** selects invocations of a task by name, or of one step
  by id. A step id is checked against the workflow, with a did-you-mean. A
  `step:` stub does not answer that step's `undo:`.
- **`where:`** is an optional CEL condition (no `${...}` fence). Stubs are tried
  in the order written, and the first that matches answers.
- **One answer:** `returns:` gives the step's outputs; `fails:` fails the
  attempt with an error `kind` (default `Upstream`) and `message`; `response:`
  gives an `http` step a raw response (`status_code`, `headers`, `body`) so the
  step's own `expect:` and `outputs:` run over it.
- **`times: N`** retires the stub after N answers, and matching falls through
  to the next one. That is how a case scripts "fail once, then succeed" and
  exercises a real `retry:` to recovery.

A `returns:` value is literal unless it is a whole-value `${...}`, which is
evaluated per invocation. Text mixed with a fence is refused.

**What a stub can see.** A stub's `where:` and `returns:` evaluate in the scope
of the step being stubbed, plus that task's own resolved inputs:

| Name | Holds |
| --- | --- |
| `inputs.<name>` | The task's resolved inputs: the URL an `http` step is about to fetch, the message a `log` step is about to write. This shadows the run's `inputs`. |
| a bare name | Whatever is bound where the step is written: a `for_each`'s `as:` name, the step's own `vars:`. |
| `vars.<name>` | The *workflow's* `vars:`, not the test file's. |
| `steps.<id>.<output>` | What earlier steps produced. |

The loop binding is what lets one stub answer each iteration differently:

```yaml
stubs:
  - task: http
    returns:
      name: '${service.name}'              # this iteration's own value
  - task: http
    where: service.name == 'search'        # or fail exactly one iteration
    fails: {kind: Upstream, message: service unavailable}
```

Inputs a task evaluates for itself, like `http`'s `expect:` and `outputs:`, are
not in `inputs`, and a `returns:` stub replaces them: the stub *is* the step's
answer, so it supplies the shaped output names later steps read. To exercise the
shaping itself, stub with `response:` instead.

### Stubbing a call

A `step:` stub may name a `call:` step. The callee's body does not run: the
stub's `returns:` stand for the outputs the callee declares, and the caller
reads them back as it would the real ones.

```yaml
stubs:
  - step: provision                 # a `call: ./workflows/provision-tenant.yaml` step
    where: inputs.tenant == "acme"  # inputs are the call's `with:` arguments
    returns: {url: https://acme.test, region_count: 2}
```

`returns:` is held to the callee: a name the callee does not declare under
`outputs:` is refused with a did-you-mean, and so is a declared output the stub
leaves out. Each value an answer carries is held to its output's declared type
and `must:` when the stub answers, so a string for an `int` fails the step as
the real call would. A callee that declares a `sensitive:` input or output
cannot be stubbed at its boundary (the stub would erase what keeps the value
out of the transcript), and `expect.compensated` is refused in a case that
stubs a call; run the callee inline for either. `where:`, `times:`, `fails:` and `invocations:` work as for a task
stub; a call is counted as the task `call.<callee name>` with hyphens written
as underscores. A call step no stub names still runs inline, so one file can
hold a case that stubs the boundary beside one that runs the callee. A
stubbed boundary runs none of the callee's steps, so test the callee's own
workflow to cover them.

**Warnings.** `flow test` warns about a stub that never answered, and about an
invocation with no stub or no matching stub (which also fails the step).
Inherited default stubs are exempt from the idle warning. `--fail-on-warning`
turns warnings into failures.

### Signals and time

```yaml
signals:
  - name: finance-approved
    at: 6s                       # delivered six virtual seconds into the run
    payload:
      approved: true
    sender:
      subject: finance-ops@example.com
      issuer: https://issuer.example.com
      claims:
        team: finance
```

- `at:` is a duration from the start of the run. Without it, the signal is
  delivered at once and held until a step waits for it. Ties are delivered in
  the order written.
- `sender:` is the identity the signal claims to come from. It is checked
  against the workflow's `signals:` policy through the same check the server
  performs. If the policy names senders and a case gives none, the signal is
  refused, exactly as an unauthenticated one would be.
- There is no clock key. Time moves only when the run is waiting: through a
  `sleep:`, a `wait_until:`, a wait's `timeout:`, a retry interval, or the next
  `at:`. Assertions should not depend on the absolute starting time.
- A case that waits for something nothing will ever deliver is stopped after 30
  seconds of real time.

### Expectations

| Field | Claim |
| --- | --- |
| `outputs` | The run's declared `outputs:`, exactly: every declared output must be named. Ignored when `failed: true`. |
| `failed` | Whether the run failed. `failed: false` is how a case claims "it finishes" and nothing more. |
| `error_contains` | Text the run's error must contain. |
| `ran` | Steps that must have run. Checked on failed runs too. |
| `skipped` | Steps that must not have run. |
| `others: skipped` | Closes `ran:`: every step not listed there must have been skipped, so a step added later fails the case until the case mentions it. |
| `compensated` | The steps whose `undo:` ran. |
| `invocations` | How often tasks ran, and in what order. See below. |
| `check` | CEL claims over the finished run. See below. |
| `inputs`, `refused`, `idempotency_key` | For a case with a webhook `trigger:`: what the delivery bound, whether it was refused, and the key it produced. |

An `expect:` with nothing in it is refused, because a case that asserts nothing
passes whatever the run did.

### Invocations: how often, and in what order

`ran:` says a step ran; `invocations:` says how many times its task did, and in
what order, without a ladder of `where:` clauses on a stub.

```yaml
expect:
  invocations:
    - {task: slack.post, count: 2}          # exactly twice
    - {task: http, at_least: 1, at_most: 3} # a range
    - {step: notify_finance, never: true}   # never invoked
    - order: [charge, ship, notify]         # first invocations in this order
```

A `task:` entry counts every invocation of that task, callees and `undo:`
compensations included. A `step:` entry counts the attempts made for that step
of the workflow under test (a `retry:` that ran three times counts three), and
neither a callee's identically named step nor a compensation. An entry takes
exactly one of `count:`, `never:`, or `at_least:`/`at_most:`; `order:` stands
alone. Names are checked before the run, with a did-you-mean, and `order:` is
refused for a step inside a `parallel:` block, where the order is not
observable. A run past 100,000 invocations fails every entry rather than judge
a truncated log.

`ran:` and `skipped:` name top-level steps. A step inside a loop body reports
through its loop's `results`, so assert on those through `outputs:` or `check:`.

A mismatch prints both sides with their types, as a Flowfile declares them:
`expected string "1", got int 1`.

### Claims the named fields cannot make: `check:`

Each `check:` entry is a CEL expression over the finished run, or
`{that:, because:}` to add the sentence a failure prints:

```yaml
expect:
  ran: [plan, join]
  check:
    - size(steps.join.value.regions) == 2
    - that: steps.join.value.regions[0] == inputs.region
      because: the join must keep the order's own region, not the fleet default
```

A check can read `steps.*`, `inputs.*`, `vars.*`, and a `run` root with
`failed`, `error`, and `local`. It runs whether or not the run failed, so
`run.error.contains('must satisfy')` is a claim about a failure. A failing check
prints the values it read:

```text
expect.check[1]: check failed: steps.join.value.regions[0] == inputs.region
           because: the join must keep the order's own region, not the fleet default
           steps.join.value.regions[0] = "us-east-1"
           inputs.region = "eu-west-1"
```

A check is evaluated by the engine's own evaluator, under the workflow's
language profile and cost limit, the same as `inspect` in the
[debugger](DEBUGGING.md). An expression you settle on at a breakpoint pastes
into `check:` unchanged.

### Faults and invariants: `--seeds` under a failing world

`expect:` says what the workflow does when nothing goes wrong. `faults:` and
`invariants:` say what must stay true when something does:

```yaml
- name: survives a flaky gateway
  workflow: ./workflow.yaml
  stubs:
    - task: http
      returns: {status_code: 200, body: ''}
  faults:
    - step: fetch                       # or `task: http`; exactly one
      fails: {kind: Upstream, message: connection reset}
      rate: 0.5                         # chance per matching invocation, in (0, 1]; default 0.5
      at_most: 1                        # times it may fire in one run, 1..100; default 1
  invariants:
    - that: run.failed == false
      because: one failed attempt must be absorbed
  expect: {failed: false}
```

A plain `flow test` injects nothing, so a case with faults gives the same
verdict as the same case without them; it only refuses a fault whose target the
case never invokes, because a fault no run can reach reports resilience to
something that never happened. Under `--seeds N` each seed draws, per matching
invocation, whether the fault fires, and the run is judged by the case's
`invariants:` alone (the claims of `check:`, over the same run) plus one oracle
it owes without being told: a failure the world causes must not surface as an
`Internal` error. A violation is reported as a finding with the seed that
produced it, and `flow test --seed S` replays exactly those faults.

A fault answers before the stubs and spends none of their `times:`. `fails.kind`
is any error kind a task reports except `Internal` and `Expression`, which are
defects, not faults. Rows of a table inherit the entry's `faults:` and
`invariants:` when they state none. Not yet covered: delay faults, signal jitter,
and shrinking a violating seed to its minimal fault set.

## One fixture, many rows

Cases that differ in one or two values can share an entry and list their
differences under `cases:`, like a Go table test:

```yaml
defaults:
  workflow: ./workflow.yaml
tests:
  - name: a declaration is enforced before the first step runs
    stubs:
      - task: http
        returns: {}
    expect:
      failed: true                   # true of every row
    cases:
      - name: the required argument is missing
        expect:
          error_contains: which service to deploy
      - name: a replica count above the declared bound
        inputs: {service: checkout, replicas: 99}
        expect:
          error_contains: must satisfy
```

The entry itself does not run; each row runs, reported as
`<entry name>/<row name>`. `--run` matches that whole name as a regular
expression, so `--run 'enforced before'` selects every row and
`--run 'enforced.*/a replica count'` selects one. Tables are one level deep, and
an empty `cases:` is refused.
[`examples/parameterized-deploy`](../examples/parameterized-deploy/workflow.test.yaml)
is a complete example.

## Sharing a fixture

Values flow down one chain, and the nearer level wins: a directory's
`testdefaults.yaml`, then the file's `defaults:`, then the table entry, then the
row.

- `inputs:` and `expect:` merge one level deep: a row that writes
  `expect.error_contains` keeps its entry's `expect.failed`.
- `signals:`, `secrets:`, `trigger:`, and `starter:` are inherited whole or
  replaced whole.
- `stubs:` merge: a case's stub for the same target and the same `where:`
  replaces its inherited twin, every other case stub is tried first, and the
  remaining inherited stubs answer what those do not.
- `check:` lists accumulate: every level's claims must hold.
- `sender:` in `defaults:` fills in only signals that name no sender. A case
  that means "no sender" writes `sender: {}`.

Because `expect:` merges, a row cannot assert less than its entry. Put only
what is true of every row on the entry.

Two stubs that select the same calls in the same way (same target, same
`where:`, the first without `times:`) are refused, since the second can never
answer. A case stub whose `where:` differs from a filtered default's for the
same target draws a warning, because both stay live.

### `testdefaults.yaml`

A directory of suites can share a fixture in a file named `testdefaults.yaml`.
It holds `vars:` and `defaults:` only; its name does not match `*.test.yaml`, so
it never runs as a suite.

```yaml
# testdefaults.yaml — shared by every suite in this directory
vars:
  issuer: https://issuer.example.com
defaults:
  workflow: ./workflow.yaml
  sender: {subject: approver@example.com, issuer: "${vars.issuer}"}
```

Only the suite's own directory is consulted, never a parent, so a suite depends
on at most two files you can see side by side. A suite loaded from bytes (the
MCP tool) or built in Go gets no directory defaults.
[`examples/testing-defaults`](../examples/testing-defaults/) shows two suites
sharing one.

## Values stated once: `vars:`

A test file's `vars:` holds literals the file repeats: a URL, an issuer, a
payload fragment. A fixture position (a case's `inputs:`, a trigger's fields, a
scripted `sender:`, `expect.outputs:`) references one as a whole-value
`${vars.x}`, substituted when the file loads. A `check:` reads `vars.x` when it
evaluates.

```yaml
vars:
  issuer: https://issuer.example.com
  order: {id: ord_123, region: eu-west-1}
tests:
  - name: the order is stated once
    inputs: {order: "${vars.order}"}
    signals:
      - name: approve
        sender: {subject: approver@example.com, issuer: "${vars.issuer}"}
    expect:
      check:
        - steps.join.value.region == vars.order.region
```

A var whose value is itself a whole-value `${...}` is computed from the other
vars when the file loads, in dependency order:

```yaml
vars:
  region: eu-west-1
  base: "${ {'id': 'ord_1', 'region': vars.region} }"
  rush: "${ {'id': vars.base.id + '_rush', 'region': vars.base.region} }"
  endpoint: "${'https://api.' + vars.region + '.example.com/v1'}"
```

The rules:

- **A computed var reads only other vars.** No `steps`, `inputs`, `run`, or
  `trigger`: the block is evaluated before any case runs. A cycle is refused
  with its path (`vars.a → vars.b → vars.a`).
- **No profile libraries.** A file's vars are not tied to one workflow, so a
  function from the workflow's language profile (`json_parse`, `split`,
  `base64.encode`) is refused here. Write that expression in `check:`, which
  compiles under the case's own profile.
- **A fence occupies a whole value.** `"https://${vars.host}/v1"` is refused;
  write `"${'https://' + vars.host + '/v1'}"`. Inside a YAML map or list, a
  fence may compute a scalar leaf but not return a map or list.
- **Bounded.** At most 200 computed values per file, which together may spend
  what one ordinary expression may.
- **`vars.` means the workflow's vars inside a stub.** A stub's `where:` and
  `returns:` evaluate in the run's scope, where `vars.` is the workflow's own
  block. Everywhere else in the test file, `vars.` is the file's.

### Secrets in a test file

A case's `secrets:` binds a value to a secret reference for that case only:

```yaml
secrets:
  env:API_TOKEN: test-token
```

A var that feeds a secret is treated as secret material. The taint follows the
dependency graph both ways, to every var computed from it and every var it was
computed from, and those values are withheld wherever the test prints them. A
tainted value redaction cannot hide is refused when the file loads: a number or
boolean derived from a secret (`${size(vars.token)}`), an empty string, or a map
or list built in CEL, since each can reveal the secret through its value or its
shape. Keep the shape in YAML and compute only string leaves:

```yaml
vars:
  token: s3cr3t
  headers:
    Authorization: "${'Bearer ' + vars.token}"   # withheld
    Accept: application/json                     # still visible
```

A var stated in `testdefaults.yaml` may not be on a path to a secret, because
the other suites in the directory would print it. Move it into the suite's own
`vars:`.

## Identity in a test

A case can say who started the run (`starter:`) and who sent each signal
(`sender:`). Both are assertions a case makes, not identities anyone attested.

They reach the workflow's own `signals:` policy, including
`distinct_from_starter:`, so a case can prove that an approver is admitted and
that the requester cannot approve their own run. They do not reach
`run.identity` (empty in every case, with `run.local` true), egress policy (a
stub answers the request that would have been checked), task-shape policy, or
secret-access policy. A green case therefore says what the workflow does for a
given identity; it says nothing about whether a deployment would let that
identity do it.

The `flow test` command takes no deployment policy flags, so no task-shape
policy applies and every dispatch is allowed. A suite run through the
`flowstate_test` MCP tool is the exception: under `flow mcp --task-policy`,
that policy is checked on every dispatch, stubbed or not, against the empty
`run.identity`. A rule that requires an identity therefore denies the case
there, although the same suite passes under `flow test`.
[`examples/approval-gate`](../examples/approval-gate/workflow.test.yaml) tests
its separation of duties this way.

## Triggers

A case can start the run the way a trigger would:

- `trigger: {webhook: <name>, payload: <file>, signature: valid|invalid}`
  replays a stored delivery through the real verifier and binder, so the case
  checks the webhook's `with:` mapping and its signature handling.
  `expect.inputs`, `expect.refused`, and `expect.idempotency_key` assert what the
  delivery produced.
- `trigger: {kind: schedule, name: nightly}` sets `trigger.*` directly, so both
  sides of a step guarded by `trigger.kind` can be tested without a real
  schedule.

See [`examples/webhook-trigger`](../examples/webhook-trigger/) and
[`examples/trigger-context`](../examples/trigger-context/).

## Running tests

```console
$ flow test <path>...
```

A directory is walked for `*.test.yaml` and `*.test.yml`; a named file is taken
as given. Finding no test files is an error, and so is naming a workflow file.

| Flag | Effect |
| --- | --- |
| `--run <regex>` | Run only cases whose full name matches. |
| `--list` | Print the names of the cases that would run, one per line under their file, without running any. Honours `--run`, and names the cases a `skip:` leaves out. A file that cannot be run is reported `REFUSED` and fails the command. Refused with `-o json`, `--junit`, `--debug`, `--watch`, `--fail-fast`, `--seeds` and `--coverage-required`, which all read a run's result. |
| `--fail-fast` | Stop at the first failing case, or the first schedule divergence under `--seeds`. The cases not run are reported as skipped with the reason. Refused with `--coverage-required`, whose bar a stopped suite cannot meet. |
| `--timeout <duration>` | The real-time limit for one case, default 30s and at most 10m. The virtual clock still decides what a workflow waits for, so this bounds a case that is stuck, not one that waits long. |
| `--coverage-required` | Fail when a step or `switch:` arm is reached by no case and not listed under `coverage.allow_unreached`. |
| `--fail-on-warning` | Treat warnings as failures. |
| `--seeds N` | Also run each case under N seeded orderings of `parallel:` branches and `async:` steps, and fail if any ordering changes what the case observes. `--seed` replays one reported seed. |
| `--debug` | Step through one case. See [Debugging](DEBUGGING.md). |
| `-o json` | A machine-readable report: cases, failures, warnings, coverage. |
| `--watch` | Run once, then again after every change to a YAML file under the paths given (a named file watches the YAML beside it; `.git` and `node_modules` are skipped; symlinks are followed to their targets), until Ctrl-C. A terminal showing text is cleared between runs; `-o json` and `-o jsonl` get one document per run and are never cleared. The walk is bounded at 50,000 directory entries. A failing run does not end the loop. Refused with `--debug`. |
| `--junit <file>` | Also write the results as JUnit XML for CI systems. A failed expectation is a `<failure>`; a case or file that could not be judged is an `<error>`. Written even when the run fails. A promoted warning, a required-coverage gap, or a schedule divergence appears as a `(run verdict)` failure, so the report never shows green over a non-zero exit. It carries only what the report already shows. |
| `-v` | Print every case's transcript, not only failing ones. |

**Coverage** is always reported, as `N/M steps reached` per file. A step counts
as reached when any case ran it (a loop-body step when any iteration did); a step
skipped by `if:` does not count. `switch:` arms are counted separately.

**A failing case** prints its transcript: each step's outputs, the virtual time
it happened at, which stub answered (marked `from defaults` when inherited),
each signal, and each `switch:` arm taken. The value of any input declared
`sensitive:` renders `[redacted]` there, in the case's failures and in a
`--seeds` divergence report, whichever workflow the run reached declares it, a
called one included. So does a called workflow's output declared `sensitive:`,
and a step, task or signal name the transcript prints that spells such a value.
A divergence report shows both of its runs under what either run withholds,
since they are read together, and the diverging case's own report, printed
beside it, withholds what any schedule's run withheld, its name and warnings
included. Claims, and the comparison `--seeds` makes, still read the real
value.

Every case's report withholds under that case's own inputs (and `secrets:`), wherever it prints a
name that spells such a value: the step in a failure, a stub's target in a
warning, and the case's own name, in text and in `-o json`. A field path, code
and position are the harness's own words and are kept. (A case whose inputs
are too many or too large to enumerate withholds everything it can and keeps
its failures readable, so it withholds no name it cannot tell from a value; the
coverage report below has no such text and withholds all of its names. A case the run was stopped before starting never loaded its workflow, so only its `secrets:` and withheld `vars:` are known: a name spelling a `sensitive:` input's value is not withheld there.) The coverage report is
one for the whole file, so it withholds under every case's inputs together: a
step id or `switch:` arm label that spells a value any case withholds prints
`[redacted]`, and is still counted and still listed as a gap. Two names that
withhold alike are told apart by a number (`[redacted]`, `[redacted]#2`), in
the order they were written; the numbers count within one report, so a
divergence's two runs number independently. Exit status is 0 when everything passed, 1 when a case failed, and 2 for
a usage error.

## Tests in Go

A program that embeds Flowstate can run a suite inside `go test`, with each
case as a subtest:

```go
func TestWorkflows(t *testing.T) {
	flowtesting.RunFile(t, "workflows/deploy.test.yaml")
}
```

The verdicts are `flow test`'s own. See
[Embedding](EMBEDDING.md#testing-the-workflows-you-embed).

## Next steps

- [Debugging a workflow](DEBUGGING.md): hold a case at any step and inspect it.
- [Examples](../examples/README.md): every example has a test file; most show
  one testing technique in their comments.
- [The Flowfile language](LANGUAGE.md): the constructs these tests exercise.
