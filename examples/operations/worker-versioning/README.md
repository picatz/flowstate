# Worker deployment versioning

`flow worker --temporal-deployment-name --build-id`: a run finishes on the interpreter it
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

You need the `flow` binary, the [Temporal CLI](https://docs.temporal.io/cli), and
four terminals: one for Temporal, one for each of two workers, and one for the
commands. The Temporal CLI is what makes a build current, which `flow` does not
do.

1. Start a Temporal development server:

   ```console
   $ temporal server start-dev
   ```

2. In a second terminal, start a Flowstate server and a versioned worker:

   ```console
   $ flow server --insecure-no-auth &
   $ flow worker --temporal-deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
   ```

   `--insecure-no-auth` makes this a rehearsal rather than a deployment: the
   server authenticates every caller as anonymous, which is only ever right on a
   machine nobody else can reach. A real one passes `--auth-policy` instead,
   plus `--rpc-resource` when that policy trusts an issuer minting bearer
   tokens.

   The build id has to be unique per build; the commit is the obvious source and
   the one the flag's own help suggests. The startup line echoes both:

   > starting worker task_queue=flowstate-run-task-queue deployment=flowstate build_id=1a2b3c4

3. In a third terminal, make that build the deployment's **current version**:

   ```console
   $ temporal worker deployment set-current-version \
       --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
   Successfully set the current worker deployment version
   ```

   A versioned worker receives new runs only once its version is current, and
   nothing in `flow` sets it. Skip this step and a run submitted next is
   accepted, then waits with nothing recorded (`flow timeline` says *This run
   has recorded nothing yet*) until some version is made current.

4. Submit a run that will still be going when you deploy again. The
   [release-approval example](../../release-approval/) waits up to an hour for
   an approval:

   ```console
   $ ID=$(flow run --detach examples/release-approval/workflow.yaml --input version=1.4.0 -o json | jq -r .workflowId)
   $ flow get "$ID"
   RUNNING workflow flowstate-request-… run 01a0e4fb-… (running for 25s) on approval
     waiting at approval for signal "release-approved", lapsing in 59m56s
   ```

5. In a fourth terminal, deploy a second build beside the first:

   ```console
   $ flow worker --temporal-deployment-name flowstate --build-id "$(git rev-parse --short HEAD)-next"
   ```

   Back in the third, make it current:

   ```console
   $ temporal worker deployment set-current-version \
       --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)-next"
   ```

6. Approve the first run, and see which version finished it:

   ```console
   $ flow signal "$ID" release-approved --data '{"approved": true}'
   $ temporal workflow describe -w "$ID"
   ...
   Versioning Info:
     Behavior        Pinned
     DeploymentName  flowstate
     BuildId         1a2b3c4
   ```

   The run finished on the version it started on, even though another version
   was current by then. A run submitted after step 5 reports the `-next` build
   instead, and `temporal worker deployment describe --name flowstate` shows the
   first version as `draining`: keep its worker running until the runs pinned
   to it finish.

To clean up, stop the workers and the server, then the Temporal development
server; it keeps nothing unless you gave it `--db-filename`.

Gradual rollouts (`set-ramping-version`), draining, and retiring a version are
Temporal's [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
operations, and they apply to Flowstate workers unchanged.

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
$ flow worker --temporal-deployment-name flowstate
ERROR
worker deployment "flowstate" has no build id: a version is the pair, so set
--build-id (or FLOWSTATE_BUILD_ID) to something unique per build, such as the
commit

$ flow worker --build-id 1a2b3c4
ERROR
build id "1a2b3c4" has no worker deployment: a version is the pair, so set
--temporal-deployment-name (or FLOWSTATE_TEMPORAL_DEPLOYMENT_NAME) to the Worker
Deployment this worker belongs to
```

Each message names the missing half *and echoes the half that was given*, which is
what identifies whose command line is wrong when several fleets are being deployed
at once.

Note the case that looks like it should be an exception and is not: passing
`--temporal-deployment-name` with `--allow-unversioned-interpreter` and no build id
is still refused for the missing build id. The flag accepts running unversioned; it does not
accept a version that is half-written. Nobody chose that state, so the answer is to
name the missing half rather than to offer to proceed without either.

**Neither half.** A worker with no version at all refuses too, and the message is
the argument rather than a code:

```console
$ flow worker
ERROR
refusing to start an unversioned worker: this worker evaluates workflow
expressions (step conditions, a loop's items:, a step's vars:, task inputs) in
workflow code, so the expression engine built into this binary decides what they
mean; with no version, deploying a different binary changes what every run
already in flight computes, including where a run resumes after continue-as-new.
Pass --temporal-deployment-name and --build-id (or
FLOWSTATE_TEMPORAL_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID) to pin each run to
the interpreter it started on, or --allow-unversioned-interpreter to accept that
exposure, which is what a local `temporal server start-dev` session usually
wants
```

Typing the flag is the whole cost of a dev-server session, which is what keeps this
from being a rule people route around. And a worker started that way says so on
every start, not only at the moment the flag was typed:

> starting worker unversioned; deploying this binary changes every run in flight

Same reasoning as tenant-routing's restricted-worker line: the person reading a
worker's logs a month later is usually not the person who wrote its command line.

**The old spelling.** The worker's flag used to be `--deployment-name`, the same
spelling `flow server` uses for the Flowstate installation recorded in workload
identities. It is refused on the worker rather than accepted, so a pinned command
line fails saying which flag it meant:

```console
$ flow worker --deployment-name flowstate
ERROR
--deployment-name was removed from `flow worker`: it named Temporal's Worker
Deployment (picatz/flowstate#2121)
--temporal-deployment-name names Temporal's Worker Deployment
--deployment-name on `flow server` names the Flowstate installation recorded in
workload identities; a worker does not take it

Try `flow --help` for the commands and flags.
```

`FLOWSTATE_DEPLOYMENT_NAME` is `flow server`'s variable; a worker ignores it.

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
$ export FLOWSTATE_TEMPORAL_DEPLOYMENT_NAME=flowstate
$ export FLOWSTATE_BUILD_ID="$(git rev-parse --short HEAD)"
$ flow worker
```

If those defaults ever stopped being read, every such deployment would begin
refusing to start — safe, but it would look like the gate itself had broken rather
than like the values had stopped arriving. `TestWorkerVersioningFlagsDefaultFromTheEnvironment`
is what keeps that from happening quietly.
