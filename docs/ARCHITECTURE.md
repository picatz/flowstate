# Flowstate Architecture

## What Flowstate is

Flowstate is a **durable, policy-governed workload engine**. You declare a workload in
YAML with CEL expressions; Flowstate compiles it into a typed Protobuf specification and
executes it on [Temporal]'s durable execution substrate.

The emphasis matters: Flowstate is not a CI/CD system. CI is one workload shape among
many, and a fairly simple one. The engine is built for any workload that has to *finish
correctly* despite process crashes, network failures, and long waits:

- data pipelines and ETL
- infrastructure provisioning and orchestration
- incident-response and operational runbooks
- agentic and LLM pipelines with retries and human approval gates
- business processes spanning hours, days, or months
- scheduled and event-driven maintenance work
- API orchestration and webhook fan-out

If it could replace a hand-written Temporal worker, an Argo/Step Functions state machine,
a pile of cron jobs, or a fragile bash-and-retry script, it is in scope.

[Temporal]: https://temporal.io/

## Why this substrate

Every foundational choice is a bet that a well-designed layer beneath us is better than
one we invent.

**Temporal** provides durability, retries, timers, signals, child workflows, schedules,
cancellation, and visibility — the genuinely hard parts of reliable execution. Flowstate's
job is to expose that power *declaratively*, so that using it does not require writing and
operating a Go worker by hand. Anything Temporal already does well, we surface rather than
reimplement.

**CEL** ([Common Expression Language]) provides a safe, non-Turing-complete, statically
typed, cost-boundable expression language. It is the right tool for both halves of the
problem: *data flow* (shaping step inputs and outputs) and *policy* (step conditions,
network egress rules, authorization). Using one expression language for both is the
central cohesion win — an operator who learns CEL for a workflow condition already knows
how to write an egress rule.

**Protobuf** provides a typed, versioned, wire-stable specification. The YAML DSL is
deliberately sugar over a real API: the compiled spec is the durable artifact Temporal
persists, and it can be produced by hand, by another tool, or by a future UI without
going through YAML at all.

**Connect RPC** provides an HTTP/1.1 and HTTP/2 API that is browser-compatible and
gRPC-compatible without gRPC's operational weight, which keeps the self-hosted story
light.

[Common Expression Language]: https://cel.dev/

## Layers

Flowstate is six layers, each with one responsibility and a narrow contract to the next.

| Layer | Responsibility | Key artifacts |
| --- | --- | --- |
| **1. Authoring** | Humans express intent | `Flowfile` (YAML+CEL), LSP, `flow validate` / `fmt` / `compile` / `tasks` |
| **2. Specification** | Typed, validated, versioned workload definition | `Workflow` protobuf, protovalidate constraints |
| **3. Capability** | What a workload *can do* | Task registry: `TaskDef` with typed Protobuf input/output descriptors; built-ins and plugins |
| **4. Execution** | Run a step, then run a workload | One `StepExecutor`; two drivers (local in-process, Temporal durable) |
| **5. Control plane** | Submit, observe, schedule, and govern runs | Connect RPC service, authn (OIDC/WIF), authorization, schedules and triggers |
| **6. Governance** | Constrain what runs may do | Egress policy, CEL cost limits, secrets, audit trail |

The important structural property is that **layer 3 is the single source of truth for
capability**. Execution dispatch, spec validation, LSP completion, and the documented task
table all derive from the same `TaskDef` descriptors. When they are derived from one place
they cannot drift; when they are maintained separately they always do — the hand-written
task table in the README drifting from the code is exactly the failure this prevents.

## Invariants

These are the rules that keep the system coherent as it grows. A change that violates one
of these is a bug, even if it passes tests.

1. **Proto-first.** Types that describe the system — workloads, steps, values, execution
   state, evaluation scope, identity — are defined in the schema, not as hand-written Go
   structs. A Go type that mirrors a schema concept is a second definition of it, and two
   definitions drift: the hand-written DSL structs that once sat between YAML and the
   `Workflow` message had to be updated in lockstep with every schema change, and silently
   lost source positions along the way. Defining a type once also means it is versioned,
   wire-stable, and usable from another language for free. Behavior attaches to generated
   types as methods in hand-written files; the *shape* comes from the schema.

2. **One evaluator.** All CEL flows through a single construction path with one cost policy,
   one cancellation contract, and one fail-closed rule. Scattering `cel.NewEnv()` across call
   sites produces environments that silently diverge in capability and cost limits.

   This is about *how* expressions are compiled and bounded, not about every policy surface
   sharing one set of variables. CEL is used for several distinct decisions — data flow
   between steps, network egress, credential assumption, secret access — and each declares
   the attributes its own decision is about. Forcing one attribute set across all of them
   would mean declaring a resolved IP address in a policy about assuming a role. What must
   not vary is the machinery: environments are cached and cost-limited, evaluation is
   cancelable, rules are compiled and type-checked when configuration is loaded rather than
   when a request arrives, and a rule that errors denies.

   Note that CEL reserves identifiers that make plausible variable names, `namespace` among
   them (the full list is in `flowfile/validate.go`). Attributes are therefore better grouped
   under an object than declared flat, which also keeps compile-time field checking so a
   typo fails at startup instead of evaluating to nothing.

3. **One executor.** The local driver and the Temporal driver call the same
   `StepExecutor`. Any behavior that differs between `flow run local` and a Temporal run —
   expression resolution timing, retries, timeouts, error shape — is a defect, because it
   makes local development lie about production.

4. **Workflow-side code is pure and frozen.** Anything nondeterministic, version-sensitive,
   or I/O-bound happens inside an activity, never in workflow code. Temporal replays
   workflow code against recorded history; impurity there corrupts in-flight runs. This
   constraint is why expression evaluation belongs in activities.

5. **The registry is the single source of truth.** The engine never special-cases a task by
   name. Behavioral differences between tasks are declared as data on `TaskDef`.

6. **Fail closed.** For authentication, egress policy, and spec validation, both the default
   configuration and the error path must deny. Anything that "allows on error" will
   eventually allow everything.

7. **Secrets never enter workflow history.** A secret reference stays a reference through
   compilation, submission, and workflow-side resolution; only the activity that needs the
   value resolves it, worker-side, at execution time. Temporal history is durable and
   widely readable — a credential written there is a credential leaked.

8. **Self-hosted first.** Every feature must work against `temporal server start-dev` with
   no cloud dependency and no external identity provider. Temporal Cloud, Nexus endpoints,
   and federated identity are opt-in configuration, never prerequisites.

## Leaning into Temporal

Temporal's primitives are the vocabulary of durable execution. The roadmap is largely a
program of surfacing each one declaratively — the DSL should make the substrate's power
reachable without a Go compiler.

Rows marked **(done)** are implemented; the rest are the shape the surface should take.

| Temporal primitive | Flowstate surface |
| --- | --- |
| Activity | a `task:` step **(done)** |
| Activity retry policy | per-step `retry:` **(done)** |
| Activity timeouts | per-step `timeout:` **(done)** |
| Error classification | retryable vs permanent, decided by the failure not a preference **(done)** |
| Durable timer | `sleep:` / `wait_until:` step |
| Signal | `wait_for_signal:` step, `flow signal` — human-in-the-loop approval gates |
| Query | `flow inspect` — read live state of a running workload |
| Update | synchronous request/response against a running workload |
| Child workflow | `workflow:` step — sub-workflow composition with its own history |
| Continue-As-New | transparent history and payload management (already implemented) |
| Schedules | `triggers: { schedule: ... }`, `flow schedule` |
| Search attributes and memo | labels projected into visibility, `flow list --filter` |
| Cancellation scopes | `on_failure:` compensation, saga semantics |
| Conditional execution | per-step `if:`, evaluated in workflow code so the branch is in history **(done)** |
| Bounded concurrency | `for_each` with `max_parallel:`, fanning out over a computed list **(done)** |
| Concurrent branches | `parallel:` branch groups, joined before dependents run **(done)** |
| Best-effort steps | per-step `continue_on_error:`, recording the failure as `${step.error}` **(done)** |
| Activity heartbeats | progress reporting for long-running tasks |
| Task queues | routing steps to specialized or plugin workers |
| Priorities and rate limits | per-step scheduling controls |
| **Nexus** | cross-namespace and cross-team calls — both consuming and *exposing* operations |

### Nexus

[Temporal Nexus] connects workloads across namespace, team, and deployment boundaries with
durable, typed operations. It is available in self-hosted Temporal and in Temporal Cloud,
which makes it a natural fit for Flowstate's portability goals. It matters in two
directions, and the second is the interesting one:

**Consuming.** A `nexus:` step calls another team's Nexus operation without sharing a
namespace, task queue, or deployment. Today, composing across organizational boundaries
means one team's worker reaching into another's infrastructure, or an HTTP call that
abandons durability at the boundary. A Nexus step keeps durable semantics across the
boundary.

**Exposing.** A Flowfile workflow can be *published* as a Nexus operation, so other teams
— or entirely separate Flowstate deployments — invoke it as a durable service. This makes
Flowstate a way to author Nexus services declaratively, in YAML, which is a capability
that does not otherwise exist. A platform team can offer "provision a tenant" as a durable
operation defined in a reviewable file rather than a bespoke worker codebase.

Nexus support must remain entirely optional and must never become a prerequisite for local
development.

[Temporal Nexus]: https://docs.temporal.io/nexus

## Deployment portability

One binary; the difference between environments is configuration only. Self-hosted is the
default and the primary target.

| | Temporal connection | Identity | Egress | Secrets |
| --- | --- | --- | --- | --- |
| **Local development** | `temporal server start-dev` on loopback | anonymous, explicit opt-in flag | loopback allowed via explicit opt-in | environment |
| **Self-hosted production** | mTLS to your own cluster | OIDC or workload identity federation | default-deny plus CEL rules | environment or KMS |
| **Temporal Cloud** (optional) | API key or mTLS, namespace and endpoint config | OIDC or WIF | default-deny plus CEL rules | KMS |

The connection layer is responsible for making these interchangeable, so that moving from
a laptop to a self-hosted cluster to Cloud never requires touching a Flowfile. It follows
Temporal's own environment configuration rather than a scheme invented here: the standard
`TEMPORAL_*` variables, and the same TOML profile file the `temporal` CLI reads. A profile
already configured for the CLI therefore works without being restated, and selecting an
environment is `TEMPORAL_PROFILE=staging`. Adopting the ecosystem's convention is worth
more than a bespoke one, because it composes with the tools operators already run.

### Identity, in both directions

Trust runs two ways, and a workload engine needs both. Flowstate sits between the systems
that ask for work and the systems the work is done against, so it is a relying party in one
direction and an identity in the other:

- **Inbound.** A caller presents a token and Flowstate decides whether it may start a
  workload. The caller may be a person via single sign-on, or another workload: a CI job's
  OIDC token, a Kubernetes projected service-account token, a cloud instance identity.
  Configuration is a trust policy naming the issuers to accept and the claims each must
  present, so who may start a workload is reviewable configuration rather than code.

- **Outbound.** A step needs to act against something else — assume an AWS role, call a
  partner API, reach a service in another namespace — and presents Flowstate's own
  short-lived, audience-scoped identity assertion to get a credential for that specific
  purpose. Publishing the public half of those keys is what lets other systems verify
  Flowstate without anyone sharing a secret.

The point of doing both is that no long-lived shared credential has to exist anywhere in
the chain. A CI job proves what it is to Flowstate, Flowstate proves what it is to AWS, and
each hop is a short-lived assertion scoped to one audience rather than a static key someone
has to rotate. Which workloads may assume which downstream identity is governed by CEL — the
same language as workflow expressions and egress rules, so operators learn one policy
language rather than three.

Credentials obtained this way are resolved worker-side at execution time and never enter
workflow history, per the secrets invariant above.

### Secrets

A secret appears in a compiled workload only as a reference — a scheme and a name, never a
value. The scheme selects the provider that resolves it, and that is the whole extension
point: a backend is one interface with one method, plus the scheme it answers for. Nothing
else in the engine knows or cares which backend a deployment uses.

That matters because the right backend differs by environment, and a workload should not
have to change when it moves. A laptop reasonably resolves secrets from the OS keychain or a
password manager; a cluster resolves them from a mounted file or a secrets manager; a
regulated deployment resolves them from a vault with audit logging. Those are the same
reference resolved by different providers, so `${secret('db:password')}` is portable in a
way an embedded credential never is.

A deployment registers only the schemes it permits. An unregistered scheme is refused rather
than guessed at, and a deployment that registers no providers refuses every reference —
which is the correct configuration for one that should not handle secrets at all.

The value's lifetime is deliberately narrow: it exists inside the activity that uses it, for
that call. It is not returned as a step output, not logged, and not carried in an error. The
engine cannot enforce that by construction alone, so the value type refuses to serialize
itself, redacts itself when formatted, and errors are scrubbed before leaving the activity.
A revealed value cannot be wiped from memory — Go strings are immutable — so the guarantee is
about where a value travels, not how long it lives.

### Tenancy

Multiple teams sharing a deployment need their workloads, secrets, and egress kept apart. A
namespace is that boundary, and one decision determines whether it is real:

**A workload's namespace comes from the authenticated caller, never from the workload
itself.** A Flowfile may not declare which tenant it belongs to, because a file that names
its own tenant can name someone else's. The namespace is established when the run is
submitted, recorded in the run's identity, and carried through every step — which is what
makes "team A cannot read team B's secrets" a property of the system rather than a
convention.

Namespaces scope the things a tenant should not share: which secret schemes and names
resolve, which egress rules apply, which runs are visible, and which downstream identities a
workload may assume. The same reference can resolve differently in two namespaces, which is
what lets one workload definition serve several teams.

Temporal has its own namespaces, providing isolation of history and visibility. Mapping a
Flowstate namespace onto a Temporal namespace is worth supporting for deployments that want
that isolation, and worth keeping optional: a single-team deployment should not have to
operate several Temporal namespaces to use the engine, and a self-hosted first-run should
need none at all.

## Execution model

A run proceeds through the layers in one direction:

```
Flowfile (YAML+CEL)
  │  compile: parse, resolve ${...} to CEL ASTs, validate against the registry
  ▼
Workflow (Protobuf spec)  ── validated by protovalidate; content-addressable
  │  submit
  ▼
Control plane (Connect RPC)  ── authenticate, authorize, record ownership
  │  start
  ▼
Temporal  ── durable orchestration: history, timers, retries, Continue-As-New
  │  schedule step
  ▼
Worker → StepExecutor → TaskDef.Fn  ── resolve inputs, enforce policy, execute
```

The local driver short-circuits from the spec directly to the `StepExecutor`, skipping the
control plane and Temporal. That is the entire difference between `flow run local` and a
durable run, and invariant 3 exists to keep it that way.

### Data flow between steps

Each step produces named, typed outputs, recorded under its step ID. Later steps reference
them with `${step_id.output_name}`, which the compiler turns into a CEL AST carried in the
spec. Because Temporal persists everything a workflow passes to an activity, the engine
statically analyzes which references each remaining step actually needs and carries only
those forward — both when scheduling a step and when performing Continue-As-New.

Payload discipline matters, but the framing is about defaults rather than hard ceilings.
Temporal's default per-payload and history limits mean an unbounded blob flowing through
history will fail a run, and carrying only what is needed keeps ordinary workloads well
inside them. Genuinely large data is a solved problem on this substrate: a custom payload
codec offloads the blob to external storage and carries a reference through history — the
claim-check pattern — so raising a limit becomes a deliberate infrastructure choice rather
than a worker buffering more and hoping. That codec is the right place to absorb large
payloads; per-task byte caps exist to bound worker memory, not to express what the system
can handle.

## Design tensions worth knowing

Honest notes on where the design strains, so future work does not rediscover them:

- **Expression evaluation placement.** Evaluating CEL in workflow code makes payloads
  smaller and step scheduling cheaper, but it puts an evolving evaluator inside the replay
  path, where a semantic change breaks in-flight runs (invariant 4). Resolution belongs in
  activities; the workflow keeps orchestration, compaction, and Continue-As-New.

- **Static reference analysis is conservative by necessity.** Compaction reads CEL ASTs to
  decide what to carry forward, which cannot see dynamically constructed expressions. When
  analysis cannot prove what a step needs, it must carry more rather than less; dropping a
  needed output is a correctness failure, while carrying an extra one is only a cost.

- **The spec travels with the run.** Carrying the full workload specification in every
  payload and every Continue-As-New is simple and self-contained, but it charges the spec
  against Temporal's limits repeatedly. Content-addressing the spec and passing a digest
  trades that for a storage dependency in the control plane.

- **Expressiveness versus safety.** CEL is deliberately not Turing-complete, so the DSL
  cannot express arbitrary computation, and it should not try. Genuinely complex logic
  belongs in a task — written in Go, tested, and registered — not in an ever-growing pile
  of expression built-ins.
