# Why Flowstate, and when not

Flowstate is a durable, policy-governed workload engine. Authors declare a
workload in a YAML+CEL `Flowfile`; Flowstate compiles it to a typed Protobuf
specification and executes it on Temporal. It is not a CI system. The target is
any workload that must finish correctly despite crashes, network failures, and
long waits, and that somebody other than its author has to be able to run
safely.

This page is for the first ten minutes of an evaluation, when the question is
"why this and not the thing I already know". It is written to be fair enough
that a maintainer of the other project would not object, and every claim about
Flowstate links to the document or test that proves it. Flowstate is
[super-alpha software](../README.md); the capabilities named here are shipped,
the interfaces are not yet stable, and an evaluation should weigh both.

## What Flowstate does that the alternatives mostly do not

These are the properties the comparison below keeps returning to. Each is a
choice with a cost, and the cost is named.

- **The workflow is a specification, not a program.** A `Flowfile` is data:
  YAML for structure, cost-bounded CEL for data flow and policy, compiled to a
  [typed, versioned Protobuf contract](ARCHITECTURE.md) with a
  [required `edition:`](DSL.md). A reviewer, a policy, an editor, and a
  debugger all read the same object. The cost is that there is no escape hatch
  into arbitrary code inside the workflow; code lives in tasks and
  [plugins](PLUGINS.md), behind a typed boundary.
- **Two drivers, one model, proved to agree.** `flow run local` executes the
  same compiled contract through the same step executor as a durable run on
  Temporal. That claim is held by a
  [shared conformance corpus](../pkg/flowstate/v1/internal/conformance) both
  drivers run, and by [architectural invariant 3](ARCHITECTURE.md); a
  divergence is a bug, not a caveat.
- **A test tier with no infrastructure and no waiting.** `flow test` runs a
  `*.test.yaml` beside a workflow under a
  [virtual clock](../pkg/flowstate/v1/clock.go), so a case that waits a day
  for an approval finishes in milliseconds, and the
  [wait cases](../pkg/flowstate/v1/internal/conformance/wait.go) keep that
  from drifting from what a durable run observes. A
  [debugger](DEBUGGING.md) holds a run at each step when a verdict is not
  enough.
- **Fail-closed boundaries, in one place.** Authentication, tenancy, egress,
  secret access, and task shape are deployment policy, written in CEL and
  enforced at the server and worker rather than restated by every workflow;
  each denies on missing state and on evaluation error. The boundaries, their
  enforcement, and their honest gaps are in the
  [threat model](../THREAT_MODEL.md); the operator's side is in the
  [deployment guide](DEPLOYMENT.md).
- **Self-hosted is the baseline.** Everything works against a Temporal you run,
  including the [dev server one command starts](CLI.md); a cloud dependency
  is an optional integration, never the only path
  ([invariant 10](ARCHITECTURE.md)).

## The alternatives

| Alternative | What it is best at | What Flowstate does differently | Choose it over Flowstate when |
| --- | --- | --- | --- |
| **The Temporal SDK directly** (Go, Java, TypeScript, Python, .NET) | The full power of a general-purpose language inside a durable workflow: any control flow, any type, any library, with Temporal's replay guaranteeing determinism. Flowstate runs on it and inherits every durability property from it. | Trades the language for a specification. A `Flowfile` cannot express what the SDK can, and in exchange it can be validated, policy-checked, diffed for breaking changes, rehearsed locally with a driver [proved to match](../pkg/flowstate/v1/internal/conformance), and governed by identity and egress rules the author does not write. One workflow type, pinned by edition, instead of one per team. | Your workflows need arbitrary code, you already run Temporal workers your engineers are fluent in, or the governance Flowstate adds is something you have built yourself. |
| **Argo Workflows** | Kubernetes-native batch: every step is a pod, fan-out over artifacts, tight integration with Kubernetes RBAC, volumes, and events. | Steps are typed tasks and plugin calls in a worker process, not containers; durability, timers, signals, and long waits come from Temporal rather than from a controller's CRD state; the same file runs on a laptop with no cluster. Flowstate has no notion of a pod. | Your units of work are containers, your operators live in Kubernetes, or your workload is batch compute rather than coordination that waits on people and systems. |
| **AWS Step Functions** (and similar managed state machines) | A managed service with no infrastructure to run, native IAM, and direct integration with the rest of one cloud. | Self-hosted by default, cloud-agnostic, and inspectable: the definition is a file in your repository with a local rehearsal, a test tier, and a debugger, rather than a console. Policy is CEL you can read and test, and [workload identity](WORKLOAD_IDENTITY_FEDERATION.md) federates to a cloud rather than living in one. | You want a managed service today, your work is entirely within one cloud provider, or running a Temporal cluster is not something your team will take on. |
| **A CI system** (GitHub Actions, GitLab CI, Buildkite, Jenkins) | Building, testing, and publishing code, triggered by a repository event, with an enormous ecosystem of reusable steps. | Flowstate is not a CI system. A CI job is a process that must stay alive for its work; a Flowstate run is durable state that survives the process, waits for days on a [signal](DSL.md), and resumes. CI should still build and test; Flowstate coordinates what happens around and after that, and a CI job can start a run and hand it off. | The workload *is* a build or a test suite, or its entire life fits inside one job's lifetime. |
| **Durable functions in code** (Restate, Inngest, Azure Durable Functions, DBOS) | Durable execution expressed as ordinary functions with retries, sleeps, and event waits, deployed as services or serverless handlers, often with a managed control plane. | The same durability class, reached through Temporal rather than a new runtime, and expressed as a declarative file rather than code. Flowstate's addition is the governance layer between the author and the executor: tenancy, fail-closed [egress](../THREAT_MODEL.md), secret access rules, and typed task contracts that a plugin author cannot bypass. | Your team wants durability inside application code with minimal ceremony, or a managed control plane matters more than a self-hosted one. |
| **Script runners with a UI** (Windmill, n8n, Airflow for data) | Getting an internal tool, a script, or a data DAG running quickly, with a visual editor and a catalog of integrations. | A text file under review, a compiler, and a policy engine rather than a UI: Flowstate is optimised for the change being reviewed and the run being governed, not for the first draft being fast. There is no visual editor, and the integration surface is typed tasks and [plugins](PLUGINS.md) rather than a catalog of hundreds. | You need a broad connector catalog now, a visual editor is how your users work, or the workload is a scheduled data DAG whose operators already know Airflow. |
| **cron and scripts** | Nothing to learn, nothing to run. | A durable run, a typed contract, retries and timeouts as declarations, an audit trail, and a rehearsal that fails the way production fails. | The job is idempotent, short, and nobody is paged when it silently does not run. |

Temporal's predecessor Cadence, and Temporal Cloud, sit in the first row:
Flowstate runs on Temporal, and a hosted Temporal is one of the places it can
run.

## Not a fit

Read this before the examples. A tool that lists only fits reads as marketing.

- **Arbitrary code inside the workflow.** The specification is the whole
  workflow; logic that does not fit YAML and CEL belongs in a task or plugin,
  and if most of your workflow is that logic, write it against the Temporal
  SDK.
- **Container-per-step batch on Kubernetes.** Flowstate has no pod, no volume,
  and no artifact store; Argo does.
- **A managed service with no cluster.** Flowstate is self-hosted on Temporal
  by design. If nobody will run Temporal, that is the answer.
- **Builds and tests.** Keep them in CI.
- **A stable interface today.** Flowstate is super-alpha; editions pin the
  language and a [rewriter](DSL.md) carries files forward, but the CLI, the
  server API, and the plugin protocol still change.

## Where to go next

- [Choose a journey](../examples/README.md) through validated examples.
- [The enterprise use cases](USE_CASES.md), each with what it honestly does
  not yet do.
- [Architecture](ARCHITECTURE.md), for the invariants the comparison leans
  on, and [Vision](VISION.md), for where the project is going.
