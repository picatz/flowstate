# Worker deployment versioning

`flow worker --deployment-name --build-id`: a run finishes on the interpreter it
started on, takes the current version at Continue-As-New, and a worker given half
the pair refuses to start.

Like [tenant-routing](../tenant-routing/), this is a property of the processes a
deployment runs rather than of any workflow, so it is a walkthrough rather than a
Flowfile. Unlike tenant-routing, it is not optional: a worker with no version
refuses to start unless you say, by name, that you accept what that means.

## Why an interpreter makes this different

Most Temporal deployments version workflows because *their* workflow code changes.
Flowstate has exactly one workflow type — `Run`, the interpreter — so every workload
in the fleet is running the same function. A change to loop compaction, or to how a
wait consumes a carried signal, is a change to every run in flight at once.

Temporal replays a run's history through the code the worker is running *now*. That
makes interpreter behavior a determinism input in the same way a clock read is. And
the exposure is bigger than the engine's own logic: expression evaluation runs in
workflow code, so cel-go's behavior — what `format()` does, what a comparison means
— is pinned by the binary and by nothing else. Deploying a different binary with no
version changes what every run already in flight computes.

That is why the gate exists at all, and why it is a refusal rather than a warning:
a shipped capability depends on the guarantee rather than merely benefiting from it.

## Run it

```console
$ flow server &
$ flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
```

The build id is what has to be unique per build; the commit is the obvious source
and the one the flag's own help suggests. The startup line echoes both:

> starting worker  task_queue=flowstate-run-task-queue deployment=flowstate build_id=1a2b3c4

Then submit a run long enough to still be going when you deploy again — anything
with a durable wait will do:

```console
$ flow run examples/wait-timeout/workflow.yaml
```

An existing example rather than one shipped here, per
[the note in the parent README](../README.md#why-these-are-here-and-not-somewhere-else).

Now start a second worker at a different build id, as a deploy would, and watch what
does *not* happen to the run already in flight:

```console
$ flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)-next"
```

Nothing. The in-flight run keeps executing on the version it started on. That is the
whole guarantee, and it is worth confirming by watching rather than trusting.

## Pinned within a run, upgraded between segments

`engine.Register` registers `Run` as **pinned**, so a run finishes on the interpreter
it started on and deploying does not touch anything in flight.

Pinning alone would be a trap: a long workload would be held on its original version
forever, and an operator would have no way to drain one. So the Continue-As-New in
`engine/workflow.go` is issued with **auto-upgrade**.

Continue-As-New is the only safe seam for that, and the reason is precise: the next
segment replays *nothing*. It starts from `RunState` rather than from history, so the
new version never has to reproduce the old one's decisions — it only has to
understand the message crossing the seam. That is what invariant 10 exists to
protect, and it is the constraint any change to `RunState` inherits.

Two consequences worth holding onto:

- A run's version can change *during* it, at a boundary the author never wrote and
  cannot see. That is by design; the alternative is undrainable runs.
- Whether a seam is reached is a function of the step budget, not of anything in the
  file. A short run may finish entirely on its original version; a long one may cross
  several deploys.

## The refusals

**Half a version.** Both halves arrive together or not at all:

```console
$ flow worker --deployment-name flowstate
Error: worker deployment "flowstate" has no build id: a version is the pair, so set
--build-id (or FLOWSTATE_BUILD_ID) to something unique per build, such as the commit

$ flow worker --build-id 1a2b3c4
Error: build id "1a2b3c4" has no worker deployment: a version is the pair, so set
--deployment-name (or FLOWSTATE_DEPLOYMENT_NAME) to the deployment this worker
belongs to
```

Each message names the missing half *and echoes the half that was given*, which is
what identifies whose command line is wrong when several fleets are being deployed
at once.

Note the case that looks like it should be an exception and is not: passing
`--deployment-name` with `--allow-unversioned-interpreter` and no build id is still
refused for the missing build id. The flag accepts running unversioned; it does not
accept a version that is half-written. Nobody chose that state, so the answer is to
name the missing half rather than to offer to proceed without either.

**Neither half.** A worker with no version at all refuses too, and the message is
the argument rather than a code:

```console
$ flow worker
Error: refusing to start an unversioned worker: this worker evaluates workflow
expressions (step conditions, a loop's items:, a step's vars:, task inputs) in
workflow code, so the expression engine built into this binary decides what they
mean — and with no version, deploying a different binary changes what every run
already in flight computes, including where a run resumes after continue-as-new.
Pass --deployment-name and --build-id (or FLOWSTATE_DEPLOYMENT_NAME and
FLOWSTATE_BUILD_ID) to pin each run to the interpreter it started on, or
--allow-unversioned-interpreter to accept that exposure, which is what a local
`temporal server start-dev` session usually wants
```

Typing the flag is the whole cost of a dev-server session, which is what keeps this
from being a rule people route around. And a worker started that way says so on
every start, not only at the moment the flag was typed:

> starting worker unversioned; deploying this binary changes every run in flight

Same reasoning as tenant-routing's restricted-worker line: the person reading a
worker's logs a month later is usually not the person who wrote its command line.

## Why the dev server is not detected and exempted

That was the alternative, and it was rejected for being a guess. The address a dev
server listens on is configurable; a production cluster can be reached at
`localhost` through a tunnel. A rule that decides how much safety to enforce by
pattern-matching a hostname fails open on exactly the deployment that most needs it.

So the exemption is a flag somebody types, which is a decision with an author.

## Setting it from the environment instead

A build id is a property of the artifact, so the thing that built it is what knows
the value. Both flags default from the environment, which lets one command line stay
identical across every deployment:

```console
$ export FLOWSTATE_DEPLOYMENT_NAME=flowstate
$ export FLOWSTATE_BUILD_ID="$(git rev-parse --short HEAD)"
$ flow worker
```

If those defaults ever stopped being read, every such deployment would begin
refusing to start — safe, but it would look like the gate itself had broken rather
than like the values had stopped arriving. `TestWorkerVersioningFlagsDefaultFromTheEnvironment`
is what keeps that from happening quietly.
