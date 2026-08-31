# Approval gate

This production-shaped example parks a run for a human decision without holding a
worker. It combines a durable `wait_for_signal`, sender authorization, a prompt,
output shaping, and an exhaustive `switch`. The Flowfile keeps only comments that
explain a nearby trust boundary or surprising semantic; this page owns the longer
operational explanation.

## Rehearse it locally

From the repository root:

```console
$ go run ./cmd/flow test examples/approval-gate/
$ go run ./cmd/flow run local examples/approval-gate/workflow.yaml --input-file examples/approval-gate/inputs.json --signal deploy-approved='{"approved":true}' --signal-as-subject sre-lead@example.com --signal-as-issuer https://issuer.example.com --signal-as-claim team=release-managers
```

The local sender is an assertion supplied by the person running the command, not
an authenticated production identity. Local rehearsal executes the same signal
rule, but `sender.local` marks that distinction and the durable driver refuses a
locally asserted sender. Use this path to test policy decisions, not to claim that
authentication was exercised.

The test suite covers approval, rejection, missing decisions, timeout, and sender
refusals with a virtual clock and no external effects.

## Run it durably

After configuring a server, worker, authentication, and deployment policy as
described in the [deployment guide](../../docs/DEPLOYMENT.md):

```console
$ flow run examples/approval-gate/workflow.yaml --input-file examples/approval-gate/inputs.json
$ flow signal <workflow-id> deploy-approved --data '{"approved":true}'
```

The server evaluates `signals.deploy-approved.allow` against the authenticated
sender before Temporal receives the signal. The fixed `team: release-managers`
claim is the grant; `expected_approver` only narrows that grant for this run. The
`distinct_from_starter` check prevents self-approval. The payload says what the
approver decided; it does not say who the approver is. Audit output reads the
server-attested sender instead.

The `debug` policy is deliberately separate. Holding a run for inspection and
approving a release are different authorities, so membership in the `sre` team
does not imply membership in `release-managers`.

## Why the gate shapes its outputs

`wait_for_signal.outputs` records one `outcome`: `deployed`, `rejected`, or
`undecided`. Every branch and public output reads that value. A missing
`approved` field is not treated as rejection, and a timeout remains observable
through `timed_out`. Because the switch consumes the finite literal domain, the
validator can reject an impossible or missing case.

This in-file policy is not a deployment boundary against an author who can edit
the Flowfile. Use deployment-owned authentication, task policy, egress policy,
and tenant isolation for that boundary; the [task-shape policy
example](../task-shape-policy/) demonstrates one such author-independent check.
