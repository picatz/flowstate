# Use cases: the enterprise portfolio

The index to the four examples under `examples/enterprise-*/`: moving money,
reviewing access, responding to an incident, onboarding a customer. Each entry
links its workflow, names the primitives it composes and why those are the right
ones, and states what it does not do.

Every example has a `*.test.yaml` beside it with at least one refusal-path case —
a rejection, a timeout, an unattested signal, or a failure that triggers saga
compensation — not only the happy path. Run them all with
`flow test examples/enterprise-*`.

## Not a fit

A list of fits without misfits reads as marketing. The shapes below are the
ones a reader in this portfolio's audience most often brings, and each has a
better home; [the comparison](COMPARISON.md) says why in one row each.

- A workflow that is mostly code: the Temporal SDK directly.
- Container-per-step batch on Kubernetes: Argo Workflows.
- A fully managed service inside one cloud: Step Functions.
- Builds and test suites: your CI system.
- A visual editor and a broad connector catalog: Windmill, n8n, or Airflow.

## [Financial settlement](../examples/enterprise-fund-transfer/workflow.yaml)

**Business problem:** move money between two accounts, requiring a
role-authorized human sign-off above a threshold, with a real answer for what
happens if the settlement record fails to write after both legs of the
transfer have already applied.

**Primitives:** `signals:` (a `treasury-approver` role claim gates who may
answer at all, not one named individual), `wait_for_signal` with a timeout,
`undo:` (saga compensation reversing credit then debit in that order), and an
idempotency key carried as a header into every ledger call.

**What it does not do:** the idempotency key reaching the ledger is real; a
ledger that actually *honors* it - keyed storage, a guard on the write - is a
property of the service this file calls, not of the orchestration layer.
Pair this file's approval-and-compensation shape with
[`examples/plugins/sql/transfer.yaml`](../examples/plugins/sql/transfer.yaml)'s
transactional idempotency pattern (claim-and-guard inside one database
transaction) for the real thing. This file also collapses three distinct
refusal reasons - unattested sender, wrong approver, self-approval - into one
`refused_unauthorized` output, where
[`approval-gate`](../examples/approval-gate/workflow.yaml) reports each
separately; that granularity was traded for a shorter file, not lost by
accident.

## [Compliance access review](../examples/enterprise-access-review/workflow.yaml)

**Business problem:** review a batch of access grants, gather last-used
evidence for each without one bad grant stalling the rest, and require a
`compliance-reviewer`-role signal before the review is considered closed.

**Primitives:** `for_each` with bounded `max_parallel` and
`continue_on_error:` per grant (the same shape
[`data-enrichment`](../examples/data-enrichment/workflow.yaml) uses for a
single record, here for a compliance batch), `signals:` + `wait_for_signal`
for the attestation gate, and `sensitive:` on the output carrying grantee
identities.

**What it does not do:** `sensitive:` is display etiquette, not containment -
the workflow's own header comment says so at length, because reaching for it
as if it were encryption or an access-control boundary is the natural
mistake. A grantee's email travels through Temporal history in the clear
exactly like any other output; marking it `sensitive:` only makes `flow get`
and `flow watch` redact it by default in a terminal someone happens to be looking
at. It does not restrict who may fetch the run.

## [Incident response runbook](../examples/enterprise-incident-response/workflow.yaml)

**Business problem:** page an on-call responder, keep gathering evidence
while waiting rather than after someone answers, escalate automatically if
the first page lapses, and record who authorized remediation - never the
person who opened the incident.

**Primitives:** `wait_for_signal` with a timeout for the page and a second
one for the escalation, `parallel:` branches for logs/metrics/recent-deploys
gathered concurrently with nothing waiting on them to start, and two
distinct `signals:` claims (`on-call-responder` for claiming the page,
`incident-commander` for authorizing remediation) enforcing a real
separation between who may acknowledge and who may sign off.

**What it does not do:** separation of duties here compares the remediation
signer's attested identity against `run.identity` - whoever's caller
started this run - the same check
[`approval-gate`](../examples/approval-gate/workflow.yaml) makes for a
deploy. Like the fund-transfer example, this file collapses several refusal
reasons (explicit "no," unattested sender, self-authorization) into one
`refused` output rather than approval-gate's three-way split.

## [Customer onboarding saga](../examples/enterprise-customer-onboarding/workflow.yaml)

**Business problem:** check quota, provision a tenant's database, billing
account, and access grant as reusable per-resource sub-workflows, hold
activation for a configurable grace period, and undo what already succeeded
if going live fails.

**Primitives:** `call:` into four reusable Flowfiles under `workflows/` (a
quota precondition and three provisioners, each independently runnable and
independently testable), `undo:` on each provisioner's own task step -
compensation registered *inside* the callee, reversed in order (access, then
billing, then database) across each `call:` boundary when `activate` fails -
and `wait_until: ${now + hours(inputs.activation_grace_hours)}` for a
grace-period timer sized per plan rather than hardcoded.

**Compensation across `call:`:** each provisioner carries `undo:` on its own
task step, and what it registers joins the run-level undo stack, reversed across
the `call:` boundary exactly as within one level
([Compensation composes through a call](DSL.md#compensation-composes-through-a-call)).
Each provisioner is a self-contained unit: it provisions one resource and knows
how to take it back.

**What its test cannot prove:** `check-quota.yaml`'s `expect:` line is real
production behavior - a task fails closed on an out-of-quota answer, exactly
like a 4xx - but `flow test`'s stub boundary cannot exercise it directly. A
stub replaces the whole `http` task, including its own `expect:`
evaluation, so no `returns:` value can make that specific line run under a
test; the sibling test file proves the equivalent failure with `fails:` on
the stub instead. Worth knowing before assuming a green test file proves a
task's own `expect:` line does what it says - it proves the workflow's
*reaction* to that class of failure, not the line itself.
