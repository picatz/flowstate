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

## Run an authenticated approval

This journey crosses the durable Temporal driver and the same bearer-token
middleware a deployed server uses. It needs two terminals, `flow`, `jq`, and a
repository checkout. It deliberately creates separate requester and approver
credentials: the signal payload carries the decision, while the server-attested
token supplies who made it.

In the first terminal, start the stack and keep its machine-readable connection
contract. Use a database so the generated signing key and credential paths stay
stable until you remove them:

```console
$ flow server dev --auth --identity-claim team --db /tmp/flowstate-approval.db -o json > /tmp/flowstate-approval-stack.json
```

`--identity-claim team` is deliberate: verified tokens can carry many claims,
but the server persists only the names policy evaluation needs. Omitting it
means a `signals:` rule cannot read `sender.identity.claims.team`, and this gate
fails closed.

In the second terminal, read the fields the running stack emitted and mint two
short-lived local credentials. The private key and tokens remain beneath the
stack's mode-0700 sidecar directory; neither token is passed in argv.

```sh
STACK=/tmp/flowstate-approval-stack.json
ADDRESS=$(jq -r .flowstateAddress "$STACK")
ISSUER=$(jq -r .authIssuer "$STACK")
AUDIENCE=$(jq -r .authResource "$STACK")
NAMESPACE=$(jq -r .authNamespace "$STACK")
KEY=$(jq -r .authKeyFile "$STACK")
TOKEN_DIR=$(dirname "$KEY")

flow jwt sign --key "$KEY" --id flowstate-dev --issuer "$ISSUER" \
  --subject release-requester@example.com --audience "$AUDIENCE" \
  --claim namespace="$NAMESPACE" > "$TOKEN_DIR/requester.jwt"
flow jwt sign --key "$KEY" --id flowstate-dev --issuer "$ISSUER" \
  --subject sre-lead@example.com --audience "$AUDIENCE" \
  --claim namespace="$NAMESPACE" --claim team=release-managers \
  > "$TOKEN_DIR/approver.jwt"
chmod 600 "$TOKEN_DIR/requester.jwt" "$TOKEN_DIR/approver.jwt"
```

Start as the requester and detach while the run waits. The explicit issuer
input binds this run to the local issuer that actually attested the approver;
production supplies its organizational issuer instead.

```sh
RUN=$(flow run --detach examples/approval-gate/workflow.yaml \
  --input-file examples/approval-gate/inputs.json \
  --input expected_approver_issuer="$ISSUER" \
  --address "$ADDRESS" --token-file "$TOKEN_DIR/requester.jwt" -o json)
WORKFLOW_ID=$(printf '%s\n' "$RUN" | jq -r .workflowId)
flow get "$WORKFLOW_ID" --address "$ADDRESS" \
  --token-file "$TOKEN_DIR/requester.jwt"
```

The requester cannot approve their own request. This command must fail with
`permission_denied`, and the run remains parked:

```sh
if flow signal "$WORKFLOW_ID" deploy-approved --data '{"approved":true}' \
  --address "$ADDRESS" --token-file "$TOKEN_DIR/requester.jwt"; then
  echo "unexpected self-approval" >&2
  exit 1
fi
```

Approve with the distinct release-manager credential, then follow the run and
check its public result. `approver_subject` comes from the signal sender the
server attested, not from the payload or the requester.

```sh
flow signal "$WORKFLOW_ID" deploy-approved --data '{"approved":true}' \
  --address "$ADDRESS" --token-file "$TOKEN_DIR/approver.jwt"
flow watch "$WORKFLOW_ID" --address "$ADDRESS" \
  --token-file "$TOKEN_DIR/requester.jwt" --reveal-sensitive -o json \
  | jq -e '.outputs.runOutputs.decision == "deployed" and
           .outputs.runOutputs.approver_subject == "sre-lead@example.com"'
```

The final read opts into `--reveal-sensitive` because it is asserting exact
output values. That flag is display etiquette, not additional authorization;
only use it where stdout has an appropriate destination.

Stop the first terminal with Ctrl-C, then remove
`/tmp/flowstate-approval.db`, `/tmp/flowstate-approval.db.flowstate-auth`, and
the stack JSON. The local issuer has no login, refresh, revocation, or automatic
rotation; this journey rehearses authenticated control-plane behavior and
separation of duties, not production identity operations.

## Run it durably

After configuring a server, worker, authentication, and deployment policy as
described in the [deployment guide](../../docs/DEPLOYMENT.md):

```console
$ flow run examples/approval-gate/workflow.yaml --input-file examples/approval-gate/inputs.json
$ flow signal <workflow-id> deploy-approved --data '{"approved":true}'
```

From a CI job, a cron entry, or anything else that must not hold a process open
while the gate waits on a person, start it detached and come back to it. The
first command returns as soon as the run has started and prints its ids; the
second follows the run exactly as `flow run` would have without `--detach`.

```console
$ flow run --detach examples/approval-gate/workflow.yaml --input-file examples/approval-gate/inputs.json -o json | jq -r .workflowId
$ flow watch <workflow-id>
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
