# Examples

The corpus has four jobs: teach a first workflow, isolate language and policy
features, show production-shaped compositions, and pin regressions. Start with a
journey below; use the complete inventory afterward when you already know which
construct you need.

## First five minutes

These commands run from a clean repository checkout and do not require an
installed `flow` binary, a server, a worker, or Temporal.

<!-- first-run-smoke:start -->

```console
$ go run ./cmd/flow validate examples/hello-world/workflow.yaml
examples/hello-world/workflow.yaml: ok
$ go run ./cmd/flow compile examples/hello-world/workflow.yaml -o json | jq -r .name
hello-world
$ go run ./cmd/flow test examples/hello-world/
PASS  examples/hello-world/workflow.test.yaml: the one step logs the literal it was given
$ go run ./cmd/flow run local examples/hello-world/workflow.yaml
INFO hello world
COMPLETED workflow hello-world
```

<!-- first-run-smoke:end -->

`validate` and `compile` execute no tasks. `test` replaces tasks and signals with
fixtures and uses a virtual clock. `run local` executes real tasks in the current
process; for a network example that means making a real request. Prefer
`validate` or `test` until you have reviewed its egress and secret requirements.

Local execution is an ephemeral rehearsal, not a durable-run simulation. The
local and Temporal drivers share the compiled model and step semantics; Temporal
adds persisted history, crash recovery, durable timers and signals, and worker
versioning. A production claim is made only where a durable-driver test or the
adjacent README says so. Follow the [durable run in Get started](../docs/GETTING_STARTED.md#7-run-it-durably)
before using `flow run` without `local`.

## Choose a journey

Still deciding whether to start one? [Why Flowstate, and when
not](../docs/COMPARISON.md) puts it beside the alternatives and names the
workloads that are not a fit.

The role labels classify the representative portfolio. Focused demonstrations
teach one mechanism; production-shaped compositions show mechanisms interacting;
regression fixtures exist primarily to keep an edge from returning. The complete
inventory below remains the source of truth for every directory.

| Journey | Start with | Role |
| --- | --- | --- |
| Hello and the authoring loop | [hello-world](hello-world), then [hello-world-multi-step](hello-world-multi-step) | first-run tutorial |
| A first real workflow, local then durable | [refund-approval](refund-approval), with the [getting-started tutorial](../docs/GETTING_STARTED.md) | first-run tutorial |
| Typed inputs, outputs, and CEL | [parameterized-deploy](parameterized-deploy), [computed-outputs](computed-outputs), [expressions](expressions) | focused feature demonstration |
| Refusing a value rather than carrying it | [enum-input](enum-input), [alert-title-bound](alert-title-bound), [utilization-guard](utilization-guard) | focused feature demonstration |
| Branching and optional values | [webhook-routing](webhook-routing), [optional-dispatch](optional-dispatch) | focused feature demonstration |
| Loops and bounded fan-out | [loop-accumulate](loop-accumulate), [fan-out-and-parallel](fan-out-and-parallel), [matrix-fan-out](matrix-fan-out) | focused feature demonstration |
| Reusable workflow composition | [call-a-workflow](call-a-workflow), then [enterprise-customer-onboarding](enterprise-customer-onboarding) | production-shaped composition |
| Declaring and raising your own errors | [declared-errors](declared-errors) | focused feature demonstration |
| Tolerating and retrying only some failure kinds | [failure-kinds](failure-kinds) | focused feature demonstration |
| Retries, timeouts, cancellation, and undo | [conditional-and-retry](conditional-and-retry), [wait-timeout](wait-timeout), [order-fulfillment](order-fulfillment) | focused feature demonstration → production-shaped composition |
| Signals and human decisions | [approval-gate](approval-gate), then [approval-escalation](approval-escalation), then [signal-quorum](signal-quorum) | policy/governance → production-shaped composition |
| A business process with a person in it | [refund-request](refund-request) | production-shaped composition |
| Many parties answering one request | [vendor-bids](vendor-bids), then [signal-batch-drain](signal-batch-drain) | production-shaped composition |
| Long-lived entities many callers address | [subscription](subscription), [entity-order](entity-order), [renewal-reminder](renewal-reminder), [signal-batch-drain](signal-batch-drain) | focused feature demonstration |
| Schedules and trigger context | [scheduled-report](scheduled-report), [schedule-overlap-policies](schedule-overlap-policies), [webhook-trigger](webhook-trigger), [webhook-respond](webhook-respond), [webhook-approval-bridge](webhook-approval-bridge), [trigger-context](trigger-context) | focused feature demonstration |
| Local rehearsal and durable execution | [deployment-reconciler](deployment-reconciler), [approval-gate](approval-gate) | local-vs-Temporal parity |
| `flow test`, directory fixtures, and `testdefaults.yaml` | [testing-defaults](testing-defaults), then any sibling `workflow.test.yaml` | testing/debugging/editor/agent journey; regression fixture |
| CLI, MCP, and DAP debugging, local and durable | [debugging](debugging), [loop-accumulate](loop-accumulate), [debugger guide](../docs/DEBUGGING.md) | testing/debugging/editor/agent journey |
| LSP and editor setup | [editor setup](../docs/EDITORS.md), [VS Code client](../editors/vscode/README.md) | testing/debugging/editor/agent journey |
| Task, egress, and identity policy | [task-shape-policy](task-shape-policy), [signal-rule-identity](signal-rule-identity), [http-secret](http-secret) | policy/governance |
| Testing a deployment policy against cases | [policy-test](policy-test) | policy/governance |
| Running a program under an operator's allowlist | [exec-checks](exec-checks) | policy/governance |
| Holding a credential a step needs | [http-secret](http-secret), then [vault-secret](vault-secret), [http-federated](http-federated) | policy/governance |
| One run at a time, and one tenant's fleet | [exclusive-cluster-drain](exclusive-cluster-drain), [operations/tenant-routing](operations/tenant-routing/) | policy/governance |
| Observability and audit | [observability](observability), [enterprise-access-review](enterprise-access-review) | production-shaped composition |
| Plugin discovery and one safe invocation | [plugins/greet](plugins/greet), then the read-only [Git](plugins/git) or [VCS](plugins/vcs) journey | plugin integration |
| Agent and MCP authoring | [agentic-loop](agentic-loop); [agentic-fix](plugins/agentic-fix) only after its plugin prerequisites | testing/debugging/editor/agent journey → plugin integration |

## House style

- Run `go run ./cmd/flow fmt <workflow>` and accept its key ordering, quoting,
  and blank-line rhythm. Do not align or space a file into a shape the formatter
  immediately removes.
- Comment *why*: a trust boundary, surprising evaluation point, retry or
  cancellation consequence, or operational hazard. Do not narrate obvious keys.
  If the explanation needs a section heading, commands, or more than one short
  paragraph, put it in the directory README and link to the canonical generated
  reference instead of copying task fields.
- Keep commands runnable from the directory they claim. Repository documentation
  uses repository-root commands unless it explicitly changes directories.
- Say **local rehearsal** for `flow run local` and **durable run** for server and
  Temporal execution. Never imply that a local assertion authenticates identity
  or that passing locally proves crash recovery.
- Examples carry references such as `${secret('env:NAME')}`, never secret-looking
  literals. Network calls need explicit bounds and safe defaults; writes require
  inputs or setup that prevent accidental invocation.
- Each top-level example has a behavior-focused `*.test.yaml`. A README is needed
  when setup, security posture, multiple files, or operational semantics cannot
  fit beside the relevant key without overwhelming the Flowfile.

## Complete inventory

Each row links a maintained example. “Network” means a real local run performs
external I/O; validation and tests remain offline unless that example's README
says otherwise.

| Example | Shows | Network |
| --- | --- | --- |
| [hello-world](hello-world) | The smallest possible workflow: one `log:` step | no |
| [refund-approval](refund-approval) | The [getting-started tutorial](../docs/GETTING_STARTED.md)'s workflow: typed inputs, a `value:` step, a `wait_for_signal:` approval with a timeout, a gated `for_each`, and declared `outputs:`, with tests for approval, rejection, and silence | no |
| [hello-world-multi-step](hello-world-multi-step) | Several steps in order, each reading a value named once at the top | no |
| [logging](logging) | `log:` — a message for a person to read, with `level:` and `fields:`, and no outputs | no |
| [string-formatting](string-formatting) | `format()` from the profile, building a message from a var | no |
| [conditional-and-retry](conditional-and-retry) | `if:`, `timeout:`, `retry:` and `continue_on_error:` per step, tolerating a step that really does fail | no |
| [declared-errors](declared-errors) | `errors:` and `fail:` — a workflow names the ways it refuses (`InsufficientFunds`), raises one with a message built from its inputs, and the run fails with that name as its kind; the tests assert the refusal | no |
| [failure-kinds](failure-kinds) | `continue_on_error:` and `retry:` naming failure kinds (`only:`) — a step tolerates and retries the failures it expects and nothing else; the tests assert both directions | no |
| [webhook-routing](webhook-routing) | `switch:` dispatching a webhook's action field — literal cases, a shared list case, written-down ignoring with `steps: []`, and a `default:` whose run is recorded | no |
| [fan-out-and-parallel](fan-out-and-parallel) | `for_each` fan-out over a computed list, and concurrent `parallel:` branches | no |
| [crossing-dependencies](crossing-dependencies) | `async:` — the N-graph, where each later step waits only for what it names, with the two-barrier version it replaces written in the file's own comment | yes |
| [loop-accumulate](loop-accumulate) | `loop:` carrying state between iterations until a condition holds, bounded by `max_iterations:`, reporting `results` and `state` | no |
| [loop-poll-until](loop-poll-until) | `loop:` in its stateless mode — a bounded poll that repeats a check until the body reports ready, or gives up at `max_iterations:` | yes |
| [paged-fan-out](paged-fan-out) | The batch shape — a `loop:` walking a cursor API to exhaustion with a `for_each` inside it fanning out over each page under `max_parallel:`, and the file honest about the window draining at every page boundary | yes |
| [entity-order](entity-order) | An entity — `loop:` + `wait_for_signal:`, addressable, mutated by repeated signals, surviving Continue-As-New, closing on a terminal event rather than by exhausting its loop | no |
| [subscription](subscription) | A service-shaped entity: a status, plan and failure count carried by a `loop:` around one `event` signal, a decision table indexed by event kind, silence as an event, and the ending state read from the last pass | no |
| [vendor-bids](vendor-bids) | Fan-in from many attested senders: sleep out a bidding window, drain every `bid` with `wait_for_signals:`, drop invalid and late ones, and award the cheapest only if enough different vendors answered | no |
| [signal-batch-drain](signal-batch-drain) | `wait_for_signals:` — the accumulator that drains a whole burst in one step rather than paying a loop iteration per event, with `max_batch:` reached rather than merely declared and the remainder left for the next drain | no |
| [signal-quorum](signal-quorum) | `wait_for_signals:` with a `quorum:` — two of three named approvers, each counted once, the requester's own approval ignored by `exclude:`, a `veto:` that ends the wait at once, and the approvers named in the step's outputs | no |
| [renewal-reminder](renewal-reminder) | The same two nodes as `entity-order` with the polarity reversed — a `loop:` around a `wait_for_signal:` whose *lapse* is the work (send the reminder, go round again) and whose delivered signal is the stop. Temporal's `sleep-for-days`, and the shape drift detection and certificate rotation take | no |
| [deployment-reconciler](deployment-reconciler) | The reconciler shape: a `loop:` whose every pass reads the world and compares it against the state the run carries, so what a pass *decides* never depends on which event woke it — with a `wait_for_signal:` `timeout:` as the resync interval and a delivered signal as the interrupt. Level-triggered convergence, and a README naming what an n-way `select:` would add | yes |
| [ops-healthcheck](ops-healthcheck) | `for_each` over a list of services, `continue_on_error:` tolerating the one that is down, and structured outputs shaped for a pager | yes |
| [matrix-fan-out](matrix-fan-out) | The matrix shape: two axes crossed into every combination in `items:`, one combination filtered out, and the trip-count ceiling that governs a product | no |
| [data-enrichment](data-enrichment) | `for_each` over a worklist with bounded `max_parallel`, per-item `retry:`, and which records could not be enriched named in `outputs:` | yes |
| [fan-out-calls](fan-out-calls) | `call:` inside `for_each` — a worklist where each item is handled by a reusable called workflow, bounded by `max_parallel:`, each callee's outputs read back per iteration, and one item's call failing tolerated without touching the others | yes |
| [workflow-vars](workflow-vars) | `vars:` at the top of a file, read as `vars.<name>`, beside a loop's bare binding | no |
| [step-vars](step-vars) | `vars:` on a step and on a loop, bare and private to what declares them | no |
| [testing-defaults](testing-defaults) | Two suites inheriting one directory-level `testdefaults.yaml`: workflow, safe task stub, assertion, and shared variable stated once | no |
| [expressions](expressions) | Expressions as values: a step's own `vars:`, and one dialect an `if:` reaches too | no |
| [optional-dispatch](optional-dispatch) | Why `.orValue(false)` on a three-way dispatch is a bug — `hasValue()` keeping "nobody answered" apart from "answered no" through a signal's payload, dispatched with `switch:` | no |
| [string-utilities](string-utilities) | `trim()`, `startsWith()`, `substring()`, `lowerAscii()` and `split()` decomposed across named steps to strip a reply prefix and derive a routing key | no |
| [list-comprehensions](list-comprehensions) | `all`, `exists`, `filter`, `map` and the `lists` library's `sort` classifying a batch of health checks as healthy, degraded, or down | no |
| [feature-flags](feature-flags) | Map comprehension over a caller's flags (`filter` ranging over keys) beside `.?` reading one named key that might not be sent at all | no |
| [usage-billing](usage-billing) | `math.greatest`, and `double()` before dividing so CEL's int-truncating division does not silently undercharge a partial block | no |
| [interpolation](interpolation) | Text and expressions in one value: several `${...}` in a message, the `$${` escape, and the whole-value fence that keeps its type | no |
| [refund-request](refund-request) | A typed `Refund` record, an optional field read with `.?`, a finance-only approval that cannot be given by the requester, an idempotent payout, and `undo:` when a later step fails | yes |
| [approval-gate](approval-gate) | `wait_for_signal:` as a human approval gate, shaping its own `outputs:` so the gate is stated once and every branch and report reads one name | no |
| [signal-rule-identity](signal-rule-identity) | Two `signals:` rules that gate on identity rather than a claim — `subject:` pinning one automated caller with no role to name, and `namespace:` beside `claims:` naming one tenant's holders of a role — and why each is the exception to gating on `claims:` alone | no |
| [approval-escalation](approval-escalation) | The chase a real approval is — a `loop:` asking on a cadence, escalating to a backup approver the `signals:` policy already named, and auto-rejecting when the ask budget runs out, with a README on why that budget is `until:`'s and not `max_iterations:`'s | no |
| [wait-timeout](wait-timeout) | The same gate going unanswered: `timeout:` lapses, `timed_out` is true, and the run carries on rather than failing | no |
| [wait-until-a-moment](wait-until-a-moment) | `wait_until:` a computed moment, with `now` and the duration builders | no |
| [computed-durations](computed-durations) | A `sleep:` and a `wait_for_signal:` `timeout:` computed rather than written down — a grace period sized by the plan, a deadline sized by the contract, and `now` in both | no |
| [expense-approval](expense-approval) | Two `wait_for_signal:` gates in sequence — a manager approval that escalates to finance ops on timeout, fail-closed if neither ever answers | no |
| [callback-address](callback-address) | `run.workflow_id` and `run.run_id` — a run telling an external system where to send the answer, then waiting for the signal that address carries back | yes |
| [headers-and-nested](headers-and-nested) | Request headers, and selecting into a nested result | yes |
| [http-json](http-json) | Parsing a JSON body with `json_parse`, named once in a step's `vars:` | yes |
| [http-query-and-json](http-query-and-json) | `query:` parameters, a structured `json:` body, and `parse_json:` | yes |
| [http-form](http-form) | A url-encoded `form:` body, as OAuth token endpoints expect | yes |
| [http-expect](http-expect) | `expect:` — accepting a 404, and rejecting a 200 with an error in the body | yes |
| [http-output-shaping](http-output-shaping) | A step that shapes its own result — returning only chosen fields from a response via `outputs:`, the same key `wait_for_signal:` uses | yes |
| [http-secret](http-secret) | Resolving an authorized bearer reference only inside the HTTP task | yes |
| [vault-secret](vault-secret) | `vault:` — the regulated-deployment backend, HashiCorp Vault or OpenBao, with a KV v2 path and token or Kubernetes auth | yes |
| [keychain-secret](keychain-secret) | `keychain:` — the macOS-only local-development backend, and the platform check that refuses it elsewhere with a clear message | yes |
| [onepassword-secret](onepassword-secret) | `op:` — a password manager shared across a team, through the 1Password CLI | yes |
| [command-secret](command-secret) | `command:` — the escape hatch that reaches any external tool (`sops`, `age`, `aws kms`, `doppler`, …) with no shell involved | yes |
| [http-federated](http-federated) | Exchanging the workload identity for a short-lived API credential inside the task | yes |
| [federation-flow-to-flow](federation-flow-to-flow) | The `assertion` target — presenting the minted assertion itself to a relying party that verifies OIDC, here another Flowstate deployment, with no exchange and no shared secret | yes |
| [exec-checks](exec-checks) | The built-in `exec:` task, denied until an operator's `--exec-policy` names the programs, directories and environment — `argv` as a list, a nonzero exit as output, and a policy file whose rules are exact argv shapes. Read its README; `flow test` stubs it, nothing here starts a process | no |
| [policy-test](policy-test) | `flow policy test` putting cases to the egress and task-shape policies already in this tree, refusals first, with the rule that denied each one named. Read its README; it runs no workflow | no |
| [task-shape-policy](task-shape-policy) | A deployment-side `--task-policy` refusing a step whose own `if:` and `signals:` have already been stripped out — the author-proof complement to `approval-gate`'s in-file gate | no |
| [simple-http-multi-step](simple-http-multi-step) | Using a response status code in a later step | yes |
| [edition-and-descriptions](edition-and-descriptions) | `description:` as a property of the step, and the required `edition:` naming the grammar the file is written in | no |
| [parameterized-deploy](parameterized-deploy) | `inputs:` — typed arguments with defaults and a required one, read from an `if:`, a step's `vars:`, and a task input | yes |
| [enum-input](enum-input) | `type: enum` — an input whose `values:` declare the closed set of strings it will accept, refused by name (not a hand-built `must:`) the moment a caller sends anything else | no |
| [record-types](record-types) | `types:` — a named, closed record (`Order`, with a `Line` inside it) declared once and used as an input's and an output's `type:`; a missing required field, an undeclared field and a wrong nested value are each refused at submit with the path to the mistake | no |
| [typed-moments](typed-moments) | `type: timestamp`, `duration` and `bytes` — inputs bound as the values CEL reads, so `inputs.opens + inputs.window` is time arithmetic and `must:` binds `this` to the same type; text that is not one is refused where the input is bound | no |
| [functions](functions) | `functions:` — a computation (`slug`, `postPath`, `isLong`) declared once with typed parameters and a result, called from a step, an `if:` and an output; each call is replaced by the body when the file compiles, so both drivers run plain CEL, and `flow fmt` writes the calls back as written | no |
| [alert-title-bound](alert-title-bound) | `max_len:` on a `type: string` input, refusing a title too long for the pager display it is headed for — counted in runes, not bytes, so a multi-byte title at the bound is let through and one rune past it is refused | no |
| [saga-provisioning](saga-provisioning) | `undo:` — saga compensation: three steps, a failure on the third, and the first two taken back in reverse order. The smallest example that ends in a failed run, on purpose | yes |
| [order-fulfillment](order-fulfillment) | The same compensation over a business transaction — reserve stock, charge a card, undo both when the carrier step is asked to fail | yes |
| [progressive-rollout](progressive-rollout) | `loop:` + `call:` + `undo:` together — traffic shifted 5% → 25% → 50% by a loop carrying the percentage, each stage a reusable called workflow with its own compensation, and every stage unwound newest-first when the canary is asked to fail | yes |
| [computed-outputs](computed-outputs) | `outputs:` — what the run answers with, computed from its steps and its arguments | no |
| [utilization-guard](utilization-guard) | `must:` on a declared output, refusing a computed percentage two individually valid inputs produced together — a bound no single input's own `must:` could ever state | no |
| [call-a-workflow](call-a-workflow) | `call:` — running another Flowfile as a step, isolated from the caller, with `with:` binding its declared inputs and its `outputs:` read back under the step id | no |
| [debugging](debugging/README.md) | The step debugger over one workflow with a `for_each`, a `parallel:` block and a `call:`: occurrence addresses such as `orders[1]/charge` and `checks#1/fraud`, conditional breakpoints and logpoints at a prompt, a `debug:` policy, and the same session attached to a durable run, from an editor, and over MCP. Read its README | no |
| [pinned-call](pinned-call) | `digest:` on a `call:`, pinning the callee to the bytes the caller reviewed and verified when the file compiles, so a callee that changed since cannot reach a run without somebody reading the change | no |
| [scheduled-report](scheduled-report) | `triggers:` — the cadence a file declares, which `flow schedule create` turns into a schedule and `flow run` ignores | no |
| [schedule-interval](schedule-interval) | The other cadence and the other kind of bound: `every:` rather than `cron:`, and `start_at:`/`end_at:` closing a schedule's firing window rather than leaving it open-ended | no |
| [schedule-overlap-policies](schedule-overlap-policies) | A decision guide for `overlap:` naming all six policies and why each is right where it is right — `cancel_other` here, and `buffer_all`/`terminate_other`/`allow_all` as three minimal sibling schedules beside it | no |
| [exclusive-cluster-drain](exclusive-cluster-drain) | `concurrency:` — at most one run of this workflow per key, decided at submit: one drain per cluster, `on_conflict: reject` naming the run that already holds it, and why the block cannot queue and cannot sit beside a webhook or a schedule — with `join` and `terminate_other` as two minimal sibling workflows beside it | no |
| [webhook-trigger](webhook-trigger) | `triggers:` as a list of call sites — a `webhook:` binding a delivery's payload to `inputs:` through `with:`, checked against that signature by `flow validate`, and replayed offline from a stored delivery by `flow test` (including the delivery that does not verify) | no |
| [webhook-respond](webhook-respond) | A `webhook:` with `respond_within:` holding the delivery open for the run's declared outputs — `completed`, `failed` and `running` documents under a 2xx that keeps its delivery meaning, and `flow test` rehearsing each with `expect.response:` | no |
| [webhook-approval-bridge](webhook-approval-bridge) | The other half of the same block: a `webhook:` whose `signal:` *answers* a `wait_for_signal:` instead of starting a run — `correlate:` naming the run by its entity key, the `signals:` rule that has to name the trigger before the file will compile, and one click delivered twice approving exactly one stage | no |
| [trigger-context](trigger-context) | `trigger.kind`, `trigger.name`, `trigger.principal` and `trigger.delivery_id` read in a step's `if:` so a scheduled sweep does not page anyone, `manual:` narrowing who may start a run by hand and requiring a recorded reason, and `flow test` setting the context directly so both sides of a trigger-guarded branch are exercisable with no real trigger | no |
| [manual-denied](manual-denied) | `manual: denied` — refusing a hand start outright rather than narrowing who may make one, for a workload whose only honest caller is its own webhook | no |
| [observability](observability) | The docker-compose observability lab: one trace id from `flow run` through Grafana Tempo to the Temporal UI | no |
| [embedding](embedding/README.md) | Flowstate as a Go library — `pkg/flowstate/embed`: compiling `flowfile/workflow.yaml` from bytes, a custom Go task registered with no `.proto` descriptor, and running it locally or (with `--durable`) against a real Temporal server. A Go program, not a `flow run`able Flowfile alone — read its README | no |
| [operations/tenant-routing](operations/tenant-routing/) | Per-tenant worker routing — `flow server --task-queue-prefix` and `flow worker --tenant`, one fleet per tenant with that tenant's own secrets and egress policy, why the composed queue name cannot be forged, and the two half-configured command lines refused at startup. A two-process demo rather than a Flowfile, so read its README | no |
| [operations/worker-versioning](operations/worker-versioning/) | `flow worker --temporal-deployment-name --build-id` — a run pinned to the interpreter it started on, upgraded at Continue-As-New, and the refusals for half a version and for none. Also a two-process demo, so read its README | no |
| [plugins/greet](plugins/greet/) | A task a plugin provides, written `example.greet:` and type-checked against the plugin's own schema — needs a built plugin and a worker, so read its README | no |
| [plugins/vcs](plugins/vcs/) | `vcs.log` and `vcs.diff` — version-control tasks (go-git) that clone in memory, per invocation, and return content rather than a workspace path — needs a built plugin and a worker, so read its README | yes |
| [plugins/github](plugins/github/) | `github.pull_request_get` (read) and `github.issue_comment` (a mutation, in a separate parameterized file so it cannot run by accident), plus a read/audit tier (`github.pull_request_list`, `github.pull_request_files`, `github.issue_get`, `github.issue_list`) in a review-triage example — needs a built plugin, a worker, and for the comment file a credential, so read its README | yes |
| [plugins/git](plugins/git/) | `git.ls_remote` (read) and `git.commit_push` (a mutation, in a separate parameterized file so it cannot run by accident) — one activity, compare-and-swapped against `base_ref`, never forced — needs a built plugin, a worker, and for the write file a credential, so read its README | yes |
| [plugins/sql](plugins/sql/) | `sql.query` (bounded, typed rows a later step filters with CEL, parameters bound and never spliced into query text, `max_rows:` required with no default) and `sql.exec` (a transfer's four statements as one transaction inside one activity, idempotent on retry, in a separate file) — needs a built plugin, a worker, and a real database, so read its README | yes |
| [plugins/anthropic](plugins/anthropic/) | `anthropic.decide` — typed questions put to a Claude model, with the answer routed by an `if:` that pages only on a self-reported confidence above a threshold the file states, and sends an answer with no confidence to a person; replayed offline by `flow test` — needs a built plugin, a worker and an API key, so read its README | yes |
| [plugins/slack](plugins/slack/) | `slack.post` — one bounded outbound notification for practical approval flows, followed by an authenticated Flowstate signal and an outcome in the same Slack thread; production only, with whole-secret credentials and operator-owned egress policy — needs a built plugin and a worker, so read its README | yes |
| [plugins/codex](plugins/codex/) | `codex.exec` — one bounded agentic turn over the OpenAI Codex CLI, sandboxed `SANDBOX_MODE_READ_ONLY` and written out rather than left to the default, so the file names its own sandbox — needs a built plugin, a worker, and the `codex` CLI, so read its README | yes |
| [plugins/oci](plugins/oci/) | `oci.resolve`, `oci.referrers` and `oci.blob` — a supply-chain gate: a tag pinned to the digest a registry serves now, and whether an attestation is attached to those exact bytes, before a human approves the digest rather than the tag; and a statement read by its layer digest and refused unless it hashes to it — needs a built plugin and a worker, so read its README | yes |
| [plugins/scim](plugins/scim/) | `scim.user_list`, `scim.user_get` and `scim.user_deactivate` — the quarterly access review as a durable workload: a bounded directory read, a week-long wait for a compliance reviewer, and a deactivation made conditional on the version the reviewer's evidence was read at — needs a built plugin, a worker and a SCIM directory, so read its README | yes |
| [plugins/ssh](plugins/ssh/) | `ssh.run` — a service restarted on a host after an approval, naming an operator's host grant and command grant rather than an address and a command line; the grants file beside it is the whole of the plugin's authority — needs a built plugin, a worker and a reachable host, so read its README | yes |
| [plugins/docker](plugins/docker/) | `docker.run` — a digest-pinned test container run as a gated step, with the image, mounts, network and resource bounds in the operator's grants file rather than in the workflow; read the plugin's trusted-computing-base note before running it — needs a built plugin, a worker and a container runtime, so read its README | yes |
| [plugins/jose](plugins/jose/) | `jose.verify` — a token the run received turned into claims it can act on, against issuers an operator trusts, with the authorization decision left to CEL over those claims: the second case is a token that verifies and is declined anyway — needs a built plugin and a worker, so read its README | yes |
| [plugins/oidc](plugins/oidc/) | No task at all: `${secret('oidc:billing-api')}` is an access token minted for that one call and resolved worker-side, so the credential is a reference in the file and in history — needs a built plugin, a worker and an authorization server, so read its README | yes |
| [plugins/webhook](plugins/webhook/) | `webhook.send` — one signed outbound delivery, with the key a whole secret reference and the signature computed by the engine's own signer, paired with the receiver whose `verify:` trigger accepts it (replayed offline by `flow test`, including a forged body and a wrong key) — needs a built plugin and a worker for the sender, so read its README | yes |
| [plugins/agentic-fix](plugins/agentic-fix/) | An agent given one bounded, durable try at a broken build and then a person: `codex.exec` produces a patch, `git.commit_push` lands it compare-and-swapped against the commit it was computed on, a `call:` verifies at that exact sha, and a tolerated `max_iterations:` exhaustion is what reaches the `wait_for_signal:` handoff. Two plugins in one file, both stubbed in its `.test.yaml`, and its README says why the budget is one rather than five | yes |
| [agentic-loop](agentic-loop) | A bounded agentic turn, a cost ceiling read off what it spent, a human gate crossed only when the ceiling was, and the write that lands it — with a README walking the loop an agent performs over `flow mcp` (`flowstate_get_catalog` → `flowstate_validate` → `flowstate_test` → `flowstate_run_local` → `flowstate_run`/`flowstate_get`), transcripts included | yes |
| [enterprise-fund-transfer](enterprise-fund-transfer) | A role-authorized `signals:` approval gate over a threshold, an idempotency key carried into every ledger call, and `undo:` reversing credit then debit if settlement fails after both applied | yes |
| [enterprise-access-review](enterprise-access-review) | Bounded `for_each` fan-out gathering evidence per access grant, tolerating one bad grant, closed only by a `compliance-reviewer` signal — with the grantee PII output `sensitive:` and the header naming what that does and does not do | yes |
| [enterprise-incident-response](enterprise-incident-response) | A `wait_for_signal:` page with an escalation on timeout, `parallel:` evidence gathering while it waits, and two distinct `signals:` claims separating who may claim an incident from who may authorize remediation | yes |
| [enterprise-customer-onboarding](enterprise-customer-onboarding) | `call:` into four reusable per-resource sub-workflows, each provisioner's own task step carrying `undo:` that composes back onto the run's undo stack across the `call:` boundary, a `wait_until:` grace period sized per plan, and an account-manager `signals:` confirmation gate — see [docs/USE_CASES.md](../docs/USE_CASES.md) for how the compensation composes | yes |

A directory that holds more than a `workflow.yaml` carries a `README.md` saying what
the rest of it is for. The reasons a directory needs one are few, and they are the
thing worth knowing rather than the membership: a secret- or credential-using example
ships the policy that authorizes what its step does; `task-shape-policy` ships the
deployment-side policy that refuses one; anything under `plugins/` needs a plugin
built and a worker told where to find it; `observability` is a whole docker-compose
lab; a few name the one durability property they
demonstrate alongside the two-command local-then-durable contrast;
`approval-escalation` has a hazard its own grammar cannot name — `max_iterations:` is
the engine's whole-loop ceiling and reads exactly like the reminder budget beside it,
which is a policy — so its README teaches the difference the file can only imply;
`embedding` is a Go
program rather than a Flowfile `flow` runs on its own, so its README says how to run
it instead; `agentic-loop`'s subject is the sequence of MCP tool calls an agent makes
while authoring the file beside it, which is not something the file itself can say;
and `operations/` holds walkthroughs of capabilities no Flowfile can express at all.

Everywhere else the workflow's own comments are the documentation, and a README
repeating them would be one more thing to leave stale. Which is also why
[call-a-workflow](call-a-workflow), [progressive-rollout](progressive-rollout) and
[fan-out-calls](fan-out-calls) each hold two Flowfiles and have none: the second one is
called by the first, and its own comments are exactly as much documentation as any
other example's.

### Plugin examples

Everything under `plugins/` sits a directory deeper than the rest, which is
deliberate. Everything matching `examples/*/workflow.yaml` is checked with the
built-in task registry alone, and a file naming a plugin's task is refused by a
process that has not loaded that plugin, with a diagnostic that says so rather
than a silent pass. Whether a plugin is installed is a deployment's decision, so
the checker says what it does not know instead of growing an exception.

You tell it what is installed, and the file is then checked against the plugin's
real input schema:

```console
$ flow validate --plugin-dir ./plugins examples/plugins/greet/workflow.yaml   # launches the plugins
$ flow validate --plugin-catalog plugins.lock.json examples/plugins/greet/workflow.yaml   # starts nothing
```

[greet](plugins/greet) walks through both. CI checks the whole tree against the
reviewed catalog in `plugins.lock.json` (`make plugin-examples`, which
`make plugin-example-catalog-update` regenerates), and each plugin's `reachable`
test builds the real binary and proves its example files are refused before the
plugin is registered and accepted after. None of those tests run a task that
reaches the network.
`embedding/flowfile/workflow.yaml` follows the same convention for the same reason: it
names `greet`, a task only `examples/embedding`'s own program registers, so it sits at
`embedding/flowfile/workflow.yaml` rather than `embedding/workflow.yaml` to stay out of
that single-level glob — `flow fix --check examples/` and `flow test examples/` still
walk the whole tree and reach it.

`operations/` sits a directory deeper too, for a related but distinct reason: it holds
no `workflow.yaml` at all. Its two walkthroughs are about what a *worker process* does,
which nothing in a Flowfile can express or observe, so each one runs an existing example
rather than shipping a file of its own that CI would not check. Its README argues the
placement.

Where a directory holds an `inputs.json` beside its `workflow.yaml`, that file is what
the example is run with — by you and by CI, through the same flag:

```console
$ flow run local examples/parameterized-deploy/workflow.yaml \
    --input-file examples/parameterized-deploy/inputs.json
```

Every other example runs as written, with no arguments, which is the rule: an example is
something to paste and watch work. Only an example whose subject *is* a required input
needs a file saying what it requires.

Where a directory holds a `debug.script` — `loop-accumulate/` does — that file is a
recorded debugging session about this example, and it plays back against the workflow
beside it:

```console
$ flow debug replay examples/loop-accumulate/debug.script \
    examples/loop-accumulate/workflow.yaml
```

It is a *reproduction* rather than a check: the commands a session accepted, so replaying
them holds the run in the same places and asks the same questions somebody asked while
working out why the loop's last term never lands in its sum. `cmd/flow`'s own tests replay
it, which is what keeps it from rotting into a file describing a workflow that has moved
on.

The examples marked as needing network are two kinds, and which kind decides whether running
one shows you anything.

- Pointed at `httpbin.org`, the only live service a `url:` names anywhere in this split.
  These run as written, and need internet access. A `vault:`, `op:` or `command:` reference
  reaches its own backend and no `url:` shows it, so that sits outside the split too —
  `vault-secret`'s README names the address it contacts, and the others name the tool that
  holds the credential.
- Pointed at a name beneath one RFC 2606 reserves for documentation: under `example.com`,
  `example.net` or `example.org` ([§3](https://www.rfc-editor.org/rfc/rfc2606#section-3)),
  or under the `.example` top-level domain
  ([§2](https://www.rfc-editor.org/rfc/rfc2606#section-2)). *Beneath* is
  load-bearing — those three domains and their `www` are reserved and also served, and a
  bare `example` is a single label a resolver expands against its search list — so what is
  left is the set of spellings nobody publishes a record for. These files are written to be
  read, validated, and exercised with `flow test`: a local run reaches the step pointed
  there and stops with a name-resolution error rather than showing you that request. A
  secret backend does resolve its reference before the step it feeds fails, and says so.

That second kind is a convention this repository keeps, not a property of your resolver.
Nothing here looks a name up, and a split-horizon resolver that answers for
`api.example.com` would make a local run reach it — so `cmd/flow`'s
`TestExamplesREADMENetworkClaims` enforces which spellings an example may name, and holds
the Network column's `no` to the same tree. An example pointed somewhere new fails there
rather than going stale here, and the reason each permitted spelling is permitted is written
beside it in `documentationOnlyHost`.

`plugins/` sits outside the split, because a plugin's own task decides where it goes: the Git
and VCS examples read a public repository on `github.com`, and the rest reach whichever
service the credential you supply belongs to. Those directories' READMEs say which.

The `http` task's egress policy denies internal addresses by default — see
[Flowstate's governance capabilities](../README.md#what-you-can-build-today) before
pointing one at a service on `localhost`.
