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
| [fan-out-and-parallel](fan-out-and-parallel) | `for_each` fan-out over a computed list, and concurrent `parallel:` branches | no |
| [loop-accumulate](loop-accumulate) | `loop:` carrying state between iterations until a condition holds, bounded by `max_iterations:`, reporting `results` and `state` | no |
| [loop-poll-until](loop-poll-until) | `loop:` in its stateless mode — a bounded poll that repeats a check until the body reports ready, or gives up at `max_iterations:` | yes |
| [paged-fan-out](paged-fan-out) | The batch shape — a `loop:` walking a cursor API to exhaustion with a `for_each` inside it fanning out over each page under `max_parallel:`, and the file honest about the window draining at every page boundary | yes |
| [entity-order](entity-order) | An entity — `loop:` + `wait_for_signal:`, addressable, mutated by repeated signals, surviving Continue-As-New, closing on a terminal event rather than by exhausting its loop | no |
| [renewal-reminder](renewal-reminder) | The same two nodes as `entity-order` with the polarity reversed — a `loop:` around a `wait_for_signal:` whose *lapse* is the work (send the reminder, go round again) and whose delivered signal is the stop. Temporal's `sleep-for-days`, and the shape drift detection and certificate rotation take | no |
| [ops-healthcheck](ops-healthcheck) | `for_each` over a list of services, `continue_on_error:` tolerating the one that is down, and structured outputs shaped for a pager | yes |
| [data-enrichment](data-enrichment) | `for_each` over a worklist with bounded `max_parallel`, per-item `retry:`, and which records could not be enriched named in `outputs:` | yes |
| [fan-out-calls](fan-out-calls) | `call:` inside `for_each` — a worklist where each item is handled by a reusable called workflow, bounded by `max_parallel:`, each callee's outputs read back per iteration, and one item's call failing tolerated without touching the others | yes |
| [workflow-vars](workflow-vars) | `vars:` at the top of a file, read as `vars.<name>`, beside a loop's bare binding | no |
| [step-vars](step-vars) | `vars:` on a step and on a loop, bare and private to what declares them | no |
| [expressions](expressions) | Expressions as values: a step's own `vars:`, and one dialect an `if:` reaches too | no |
| [approval-gate](approval-gate) | `wait_for_signal:` as a human approval gate, and branching on `payload` versus `timed_out` | no |
| [wait-timeout](wait-timeout) | The same gate going unanswered: `timeout:` lapses, `timed_out` is true, and the run carries on rather than failing | no |
| [wait-until-a-moment](wait-until-a-moment) | `wait_until:` a computed moment, with `now` and the duration builders | no |
| [expense-approval](expense-approval) | Two `wait_for_signal:` gates in sequence — a manager approval that escalates to finance ops on timeout, fail-closed if neither ever answers | no |
| [callback-address](callback-address) | `run.workflow_id` and `run.run_id` — a run telling an external system where to send the answer, then waiting for the signal that address carries back | yes |
| [headers-and-nested](headers-and-nested) | Request headers, and selecting into a nested result | yes |
| [http-json](http-json) | Parsing a JSON body with `json_parse`, named once in a step's `vars:` | yes |
| [http-query-and-json](http-query-and-json) | `query:` parameters, a structured `json:` body, and `parse_json:` | yes |
| [http-form](http-form) | A url-encoded `form:` body, as OAuth token endpoints expect | yes |
| [http-expect](http-expect) | `expect:` — accepting a 404, and rejecting a 200 with an error in the body | yes |
| [http-output-shaping](http-output-shaping) | Returning only chosen fields from a response via `outputs` | yes |
| [http-secret](http-secret) | Resolving an authorized bearer reference only inside the HTTP task | yes |
| [vault-secret](vault-secret) | `vault:` — the regulated-deployment backend, HashiCorp Vault or OpenBao, with a KV v2 path and token or Kubernetes auth | yes |
| [keychain-secret](keychain-secret) | `keychain:` — the macOS-only local-development backend, and the platform check that refuses it elsewhere with a clear message | yes |
| [onepassword-secret](onepassword-secret) | `op:` — a password manager shared across a team, through the 1Password CLI | yes |
| [command-secret](command-secret) | `command:` — the escape hatch that reaches any external tool (`sops`, `age`, `aws kms`, `doppler`, …) with no shell involved | yes |
| [http-federated](http-federated) | Exchanging the workload identity for a short-lived API credential inside the task | yes |
| [task-shape-policy](task-shape-policy) | A deployment-side `--task-policy` refusing a step whose own `if:` and `signals:` have already been stripped out — #187, the author-proof complement to `approval-gate`'s in-file gate | no |
| [simple-http-multi-step](simple-http-multi-step) | Using a response status code in a later step | yes |
| [edition-and-descriptions](edition-and-descriptions) | `description:` as a property of the step, and the required `edition:` naming the grammar the file is written in | no |
| [parameterized-deploy](parameterized-deploy) | `inputs:` — typed arguments with defaults and a required one, read from an `if:`, a step's `vars:`, and a task input | yes |
| [saga-provisioning](saga-provisioning) | `undo:` — saga compensation: three steps, a failure on the third, and the first two taken back in reverse order. The one example that ends in a failed run, on purpose | yes |
| [order-fulfillment](order-fulfillment) | The same compensation over a business transaction — reserve stock, charge a card, undo both when the carrier step is asked to fail | yes |
| [progressive-rollout](progressive-rollout) | `loop:` + `call:` + `undo:` together — traffic shifted 5% → 25% → 50% by a loop carrying the percentage, each stage a reusable called workflow with its own compensation, and every stage unwound newest-first when the canary is asked to fail | yes |
| [computed-outputs](computed-outputs) | `outputs:` — what the run answers with, computed from its steps and its arguments | no |
| [call-a-workflow](call-a-workflow) | `call:` — running another Flowfile as a step, isolated from the caller, with `with:` binding its declared inputs and its `outputs:` read back under the step id | no |
| [scheduled-report](scheduled-report) | `triggers:` — the cadence a file declares, which `flow schedule create` turns into a schedule and `flow run` ignores | no |
| [observability](observability) | The docker-compose observability lab: one trace id from `flow run` through Grafana Tempo to the Temporal UI | no |
| [embedding](embedding/README.md) | Flowstate as a Go library — `pkg/flowstate/embed`: compiling `flowfile/workflow.yaml` from bytes, a custom Go task registered with no `.proto` descriptor, and running it locally or (with `--durable`) against a real Temporal server. A Go program, not a `flow run`able Flowfile alone — read its README | no |
| [plugins/greet](plugins/greet/) | A task a plugin provides, written `example.greet:` and type-checked against the plugin's own schema — needs a built plugin and a worker, so read its README | no |
| [plugins/vcs](plugins/vcs/) | `vcs.log` and `vcs.diff` — version-control tasks (go-git) that clone in memory, per invocation, and return content rather than a workspace path — needs a built plugin and a worker, so read its README | yes |
| [plugins/github](plugins/github/) | `github.pull_request_get` (read) and `github.issue_comment` (a mutation, in a separate parameterized file so it cannot run by accident), plus a read/audit tier (`github.pull_request_list`, `github.pull_request_files`, `github.issue_get`, `github.issue_list`) in a review-triage example — needs a built plugin, a worker, and for the comment file a credential, so read its README | yes |
| [plugins/git](plugins/git/) | `git.ls_remote` (read) and `git.commit_push` (a mutation, in a separate parameterized file so it cannot run by accident) — one activity, compare-and-swapped against `base_ref`, never forced — needs a built plugin, a worker, and for the write file a credential, so read its README | yes |
| [enterprise-fund-transfer](enterprise-fund-transfer) | A role-authorized `signals:` approval gate over a threshold, an idempotency key carried into every ledger call, and `undo:` reversing credit then debit if settlement fails after both applied | yes |
| [enterprise-access-review](enterprise-access-review) | Bounded `for_each` fan-out gathering evidence per access grant, tolerating one bad grant, closed only by a `compliance-reviewer` signal — with the grantee PII output `sensitive:` and the header naming what that does and does not do | yes |
| [enterprise-incident-response](enterprise-incident-response) | A `wait_for_signal:` page with an escalation on timeout, `parallel:` evidence gathering while it waits, and two distinct `signals:` claims separating who may claim an incident from who may authorize remediation | yes |
| [enterprise-customer-onboarding](enterprise-customer-onboarding) | `call:` into four reusable per-resource sub-workflows, each provisioner's own task step carrying `undo:` that composes back onto the run's undo stack across the `call:` boundary, a `wait_until:` grace period sized per plan, and an account-manager `signals:` confirmation gate — see [docs/USE_CASES.md](../docs/USE_CASES.md) for the composition gap this file found and, once #225 closed it, the composed shape it now demonstrates | yes |

Sixteen of these hold more than a `workflow.yaml`, and those sixteen have a `README.md`
saying what the rest of the directory is for: [http-secret](http-secret),
[vault-secret](vault-secret), [keychain-secret](keychain-secret),
[onepassword-secret](onepassword-secret), [command-secret](command-secret), and
[http-federated](http-federated) ship the policy that authorizes what their step does,
[task-shape-policy](task-shape-policy) ships the deployment-side policy that refuses
one, [plugins/greet](plugins/greet/), [plugins/vcs](plugins/vcs/),
[plugins/github](plugins/github/), and [plugins/git](plugins/git/) each need a plugin
built and a worker told where to find it, [observability](observability) is a whole
docker-compose lab, [expense-approval](expense-approval),
[ops-healthcheck](ops-healthcheck), [data-enrichment](data-enrichment), and
[order-fulfillment](order-fulfillment) each carry a README naming the one durability
property the example demonstrates and the two-command local-then-durable contrast, per
the examples charter (#165), and [embedding](embedding/README.md) is a Go program rather than a
Flowfile `flow` runs on its own, so its README says how to run it instead. Everywhere
else the workflow's own comments are the documentation, and a README repeating them
would be one more thing to leave stale — which is also why
[call-a-workflow](call-a-workflow), [progressive-rollout](progressive-rollout) and
[fan-out-calls](fan-out-calls) each hold two Flowfiles and have none: the second one is
called by the first, and its own comments are exactly as much documentation as any
other example's.

`plugins/greet`, `plugins/vcs`, `plugins/github`, and `plugins/git` also sit a directory
deeper than the rest, which is deliberate: everything matching `examples/*/workflow.yaml`
is checked with the built-in task registry, and a file naming a plugin's task is meant to
be refused by a process that has not loaded that plugin. Their READMEs say more.
`embedding/flowfile/workflow.yaml` follows the same convention for the same reason: it
names `greet`, a task only `examples/embedding`'s own program registers, so it sits at
`embedding/flowfile/workflow.yaml` rather than `embedding/workflow.yaml` to stay out of
that single-level glob — `flow fix --check examples/` and `flow test examples/` still
walk the whole tree and reach it.

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
