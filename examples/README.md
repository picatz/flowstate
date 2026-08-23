# Examples

Each directory holds one `workflow.yaml` demonstrating a feature. Run any of them locally,
without Temporal:

```console
$ flow run local examples/hello-world/workflow.yaml
```

Check one without running it — worth preferring for the network examples, since running
them makes real requests:

```console
$ flow validate examples/hello-world/workflow.yaml
```

| Example | Shows | Network |
| --- | --- | --- |
| [hello-world](hello-world) | The smallest possible workflow: one `log:` step | no |
| [hello-world-multi-step](hello-world-multi-step) | Several steps in order, each reading a value named once at the top | no |
| [logging](logging) | `log:` — a message for a person to read, with `level:` and `fields:`, and no outputs | no |
| [string-formatting](string-formatting) | `format()` from the profile, building a message from a var | no |
| [conditional-and-retry](conditional-and-retry) | `if:`, `timeout:`, `retry:` and `continue_on_error:` per step, tolerating a step that really does fail | no |
| [webhook-routing](webhook-routing) | `switch:` dispatching a webhook's action field — literal cases, a shared list case, written-down ignoring with `steps: []`, and a `default:` whose run is recorded | no |
| [fan-out-and-parallel](fan-out-and-parallel) | `for_each` fan-out over a computed list, and concurrent `parallel:` branches | no |
| [crossing-dependencies](crossing-dependencies) | `async:` — the N-graph, where each later step waits only for what it names, with the two-barrier version it replaces written in the file's own comment | no |
| [loop-accumulate](loop-accumulate) | `loop:` carrying state between iterations until a condition holds, bounded by `max_iterations:`, reporting `results` and `state` | no |
| [loop-poll-until](loop-poll-until) | `loop:` in its stateless mode — a bounded poll that repeats a check until the body reports ready, or gives up at `max_iterations:` | yes |
| [paged-fan-out](paged-fan-out) | The batch shape — a `loop:` walking a cursor API to exhaustion with a `for_each` inside it fanning out over each page under `max_parallel:`, and the file honest about the window draining at every page boundary | yes |
| [entity-order](entity-order) | An entity — `loop:` + `wait_for_signal:`, addressable, mutated by repeated signals, surviving Continue-As-New, closing on a terminal event rather than by exhausting its loop | no |
| [renewal-reminder](renewal-reminder) | The same two nodes as `entity-order` with the polarity reversed — a `loop:` around a `wait_for_signal:` whose *lapse* is the work (send the reminder, go round again) and whose delivered signal is the stop. Temporal's `sleep-for-days`, and the shape drift detection and certificate rotation take | no |
| [ops-healthcheck](ops-healthcheck) | `for_each` over a list of services, `continue_on_error:` tolerating the one that is down, and structured outputs shaped for a pager | yes |
| [matrix-fan-out](matrix-fan-out) | The matrix shape: two axes crossed into every combination in `items:`, one combination filtered out, and the trip-count ceiling that governs a product | no |
| [data-enrichment](data-enrichment) | `for_each` over a worklist with bounded `max_parallel`, per-item `retry:`, and which records could not be enriched named in `outputs:` | yes |
| [fan-out-calls](fan-out-calls) | `call:` inside `for_each` — a worklist where each item is handled by a reusable called workflow, bounded by `max_parallel:`, each callee's outputs read back per iteration, and one item's call failing tolerated without touching the others | yes |
| [workflow-vars](workflow-vars) | `vars:` at the top of a file, read as `vars.<name>`, beside a loop's bare binding | no |
| [step-vars](step-vars) | `vars:` on a step and on a loop, bare and private to what declares them | no |
| [expressions](expressions) | Expressions as values: a step's own `vars:`, and one dialect an `if:` reaches too | no |
| [optional-dispatch](optional-dispatch) | Why `.orValue(false)` on a three-way dispatch is a bug — `hasValue()` keeping "nobody answered" apart from "answered no" through a signal's payload, dispatched with `switch:` | no |
| [string-utilities](string-utilities) | `trim()`, `startsWith()`, `substring()`, `lowerAscii()` and `split()` decomposed across named steps to strip a reply prefix and derive a routing key | no |
| [list-comprehensions](list-comprehensions) | `all`, `exists`, `filter`, `map` and the `lists` library's `sort` classifying a batch of health checks as healthy, degraded, or down | no |
| [feature-flags](feature-flags) | Map comprehension over a caller's flags (`filter` ranging over keys) beside `.?` reading one named key that might not be sent at all | no |
| [usage-billing](usage-billing) | `math.greatest`, and `double()` before dividing so CEL's int-truncating division does not silently undercharge a partial block | no |
| [interpolation](interpolation) | Text and expressions in one value: several `${...}` in a message, the `$${` escape, and the whole-value fence that keeps its type | no |
| [approval-gate](approval-gate) | `wait_for_signal:` as a human approval gate, shaping its own `outputs:` so the gate is stated once and every branch and report reads one name | no |
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
| [task-shape-policy](task-shape-policy) | A deployment-side `--task-policy` refusing a step whose own `if:` and `signals:` have already been stripped out — #187, the author-proof complement to `approval-gate`'s in-file gate | no |
| [simple-http-multi-step](simple-http-multi-step) | Using a response status code in a later step | yes |
| [edition-and-descriptions](edition-and-descriptions) | `description:` as a property of the step, and the required `edition:` naming the grammar the file is written in | no |
| [parameterized-deploy](parameterized-deploy) | `inputs:` — typed arguments with defaults and a required one, read from an `if:`, a step's `vars:`, and a task input | yes |
| [saga-provisioning](saga-provisioning) | `undo:` — saga compensation: three steps, a failure on the third, and the first two taken back in reverse order. The one example that ends in a failed run, on purpose | yes |
| [order-fulfillment](order-fulfillment) | The same compensation over a business transaction — reserve stock, charge a card, undo both when the carrier step is asked to fail | yes |
| [progressive-rollout](progressive-rollout) | `loop:` + `call:` + `undo:` together — traffic shifted 5% → 25% → 50% by a loop carrying the percentage, each stage a reusable called workflow with its own compensation, and every stage unwound newest-first when the canary is asked to fail | yes |
| [computed-outputs](computed-outputs) | `outputs:` — what the run answers with, computed from its steps and its arguments | no |
| [call-a-workflow](call-a-workflow) | `call:` — running another Flowfile as a step, isolated from the caller, with `with:` binding its declared inputs and its `outputs:` read back under the step id | no |
| [pinned-call](pinned-call) | `digest:` on a `call:`, pinning the callee to the bytes the caller reviewed and verified when the file compiles, so a callee that changed since cannot reach a run without somebody reading the change | no |
| [scheduled-report](scheduled-report) | `triggers:` — the cadence a file declares, which `flow schedule create` turns into a schedule and `flow run` ignores | no |
| [webhook-trigger](webhook-trigger) | `triggers:` as a list of call sites — a `webhook:` binding a delivery's payload to `inputs:` through `with:`, checked against that signature by `flow validate`, and replayed offline from a stored delivery by `flow test` (including the delivery that does not verify) | no |
| [trigger-context](trigger-context) | `trigger.kind`, `trigger.name`, `trigger.principal` and `trigger.delivery_id` read in a step's `if:` so a scheduled sweep does not page anyone, `manual:` narrowing who may start a run by hand and requiring a recorded reason, and `flow test` setting the context directly so both sides of a trigger-guarded branch are exercisable with no real trigger | no |
| [observability](observability) | The docker-compose observability lab: one trace id from `flow run` through Grafana Tempo to the Temporal UI | no |
| [embedding](embedding/README.md) | Flowstate as a Go library — `pkg/flowstate/embed`: compiling `flowfile/workflow.yaml` from bytes, a custom Go task registered with no `.proto` descriptor, and running it locally or (with `--durable`) against a real Temporal server. A Go program, not a `flow run`able Flowfile alone — read its README | no |
| [operations/tenant-routing](operations/tenant-routing/) | Per-tenant worker routing — `flow server --task-queue-prefix` and `flow worker --tenant`, one fleet per tenant with that tenant's own secrets and egress policy, why the composed queue name cannot be forged, and the two half-configured command lines refused at startup. A two-process demo rather than a Flowfile, so read its README | no |
| [operations/worker-versioning](operations/worker-versioning/) | `flow worker --deployment-name --build-id` — a run pinned to the interpreter it started on, upgraded at Continue-As-New, and the refusals for half a version and for none. Also a two-process demo, so read its README | no |
| [plugins/greet](plugins/greet/) | A task a plugin provides, written `example.greet:` and type-checked against the plugin's own schema — needs a built plugin and a worker, so read its README | no |
| [plugins/vcs](plugins/vcs/) | `vcs.log` and `vcs.diff` — version-control tasks (go-git) that clone in memory, per invocation, and return content rather than a workspace path — needs a built plugin and a worker, so read its README | yes |
| [plugins/github](plugins/github/) | `github.pull_request_get` (read) and `github.issue_comment` (a mutation, in a separate parameterized file so it cannot run by accident), plus a read/audit tier (`github.pull_request_list`, `github.pull_request_files`, `github.issue_get`, `github.issue_list`) in a review-triage example — needs a built plugin, a worker, and for the comment file a credential, so read its README | yes |
| [plugins/git](plugins/git/) | `git.ls_remote` (read) and `git.commit_push` (a mutation, in a separate parameterized file so it cannot run by accident) — one activity, compare-and-swapped against `base_ref`, never forced — needs a built plugin, a worker, and for the write file a credential, so read its README | yes |
| [plugins/sql](plugins/sql/) | `sql.query` (bounded, typed rows a later step filters with CEL, parameters bound and never spliced into query text, `max_rows:` required with no default) and `sql.exec` (a transfer's four statements as one transaction inside one activity, idempotent on retry, in a separate file) — needs a built plugin, a worker, and a real database, so read its README | yes |
| [plugins/codex](plugins/codex/) | `codex.exec` — one bounded agentic turn over the OpenAI Codex CLI, sandboxed `SANDBOX_MODE_READ_ONLY` and written out rather than left to the default, so the file names its own sandbox — needs a built plugin, a worker, and the `codex` CLI, so read its README | yes |
| [agentic-loop](agentic-loop) | A bounded agentic turn, a cost ceiling read off what it spent, a human gate crossed only when the ceiling was, and the write that lands it — with a README walking the loop an agent performs over `flow mcp` (`flowstate_get_catalog` → `flowstate_validate` → `flowstate_test` → `flowstate_run_local` → `flowstate_run`/`flowstate_get`), transcripts included | yes |
| [enterprise-fund-transfer](enterprise-fund-transfer) | A role-authorized `signals:` approval gate over a threshold, an idempotency key carried into every ledger call, and `undo:` reversing credit then debit if settlement fails after both applied | yes |
| [enterprise-access-review](enterprise-access-review) | Bounded `for_each` fan-out gathering evidence per access grant, tolerating one bad grant, closed only by a `compliance-reviewer` signal — with the grantee PII output `sensitive:` and the header naming what that does and does not do | yes |
| [enterprise-incident-response](enterprise-incident-response) | A `wait_for_signal:` page with an escalation on timeout, `parallel:` evidence gathering while it waits, and two distinct `signals:` claims separating who may claim an incident from who may authorize remediation | yes |
| [enterprise-customer-onboarding](enterprise-customer-onboarding) | `call:` into four reusable per-resource sub-workflows, each provisioner's own task step carrying `undo:` that composes back onto the run's undo stack across the `call:` boundary, a `wait_until:` grace period sized per plan, and an account-manager `signals:` confirmation gate — see [docs/USE_CASES.md](../docs/USE_CASES.md) for the composition gap this file found and, once #225 closed it, the composed shape it now demonstrates | yes |

A directory that holds more than a `workflow.yaml` carries a `README.md` saying what
the rest of it is for. The reasons a directory needs one are few, and they are the
thing worth knowing rather than the membership: a secret- or credential-using example
ships the policy that authorizes what its step does; `task-shape-policy` ships the
deployment-side policy that refuses one; anything under `plugins/` needs a plugin
built and a worker told where to find it; `observability` is a whole docker-compose
lab; the examples charter (#165) asks a few to name the one durability property they
demonstrate alongside the two-command local-then-durable contrast; `embedding` is a Go
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

This paragraph used to prove its own point. It opened "Sixteen of these", went on to
list seventeen, and by then twenty directories on disk actually had one — so it was
wrong in three different ways at once about a fact anybody could have counted. It no
longer counts or enumerates, for exactly the reason it gives.

`plugins/greet`, `plugins/vcs`, `plugins/github`, and `plugins/git` also sit a directory
deeper than the rest, which is deliberate: everything matching `examples/*/workflow.yaml`
is checked with the built-in task registry, and a file naming a plugin's task is meant to
be refused by a process that has not loaded that plugin. Their READMEs say more.
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

The examples marked as needing network reach `httpbin.org`. They will fail without internet
access, and the `http` task's egress policy denies internal addresses by default — see
[Governed network access](../README.md#what-it-can-do) if you point one at a
service on `localhost`.
