# One Flowstate calling another, with nothing shared between them

[`examples/http-federated`](../http-federated) shows a workload obtaining a
credential for a partner: the worker mints an assertion and *exchanges* it for
whatever the partner's identity provider hands back. That is the right shape
when the far side speaks STS, or Google's security token service, or RFC 8693.

Some relying parties speak none of those and need none of them, because they
already verify OIDC. For those the exchange is a hop that returns a token with
the same properties the assertion already had. This directory is that case:

```yaml
      credential: peer-flowstate
```

```yaml
    - name: peer-flowstate
      assertion:
        audience: https://flowstate.peer.example.com
```

`assertion` is a target kind that performs no exchange. What the step presents
is the assertion this deployment signed for itself, bound to that one audience,
expiring when the assertion expires. The peer's server fetches this deployment's
key set from the discovery document its issuer publishes, verifies the
signature, and reads the run's identity off the claims — the same path B's
authenticator follows for any caller, which is why nothing here is specific to
one RPC.

The step calls one: `WorkflowService/Get`, whose request is a single
`workflowId` string, so the body is a real `GetRequest` a peer would accept
rather than a shape it would answer 400 to. Get is chosen only because it has
the lightest request to write out; the point on show is the identity B's
authenticator admits, which runs before any handler and is the same for every
RPC. A production call would more often be `Run`, whose request carries a whole
compiled workflow — out of scope for an example about the identity, not the run
API.

## Two files, because federation is two configurations

[`auth-policy.yaml`](auth-policy.yaml) is deployment A's — the caller's. Its
`federation:` block names the target, the audience, and the CEL rule deciding
which workloads may assume it.

[`trust.yaml`](trust.yaml) is deployment B's — the callee's. Its `issuers:`
entry names A's issuer, the audience it requires, the claims it insists on, and
the claim it reads the tenant from. Nothing in it is Flowstate-specific: it is
the same entry shape that admits a GitHub Actions token.

Neither side can grant itself anything. A decides which of its workloads may
ask; B decides which of A's workloads it admits, and under what role and in
which namespace. A run that either side refuses does not happen, and A's refusal
comes first — nothing signed by A exists for a workload A would not have let
ask.

## What it costs

A directly presented assertion is a bearer token, which is the one thing an
exchange normally takes away. Anyone who obtains it — from a compromised peer, a
proxy that logs headers, a downstream that stores what it was sent — can replay
it until `exp`, against anything that accepts its `aud`.

Two things bound that, and both are visible in `auth-policy.yaml`. The audience
is required and is the only place the assertion works, so a replay anywhere else
fails verification. And `assertion_lifetime:` is the replay window; it is the
issuer's single knob, capped at an hour, deliberately not repeated per target
where the two could disagree. Two minutes here is long enough to reach a peer
and short enough that a captured assertion is worth little.

Prefer an exchange where the far side supports one. Reach for this where it
does not, and where the alternative is a long-lived token in a secret store.

## Running it

The hosts are `example.com`, so this is a file to read and adapt. It takes three
processes, because A has to *publish* its identity as well as *present* it.

Deployment A's **server** is what mounts the discovery document and JWKS at A's
issuer URL — the worker mints but exposes no HTTP, so without this server B has
nowhere to fetch A's keys from (`cmd/flow/routing.go` mounts the identity
handlers only when a broker is configured, which needs the federation policy and
the key):

```console
$ flow server --auth-policy examples/federation-flow-to-flow/auth-policy.yaml \
    --identity-key /etc/flowstate/identity.pem
```

Deployment A's **worker** runs the workflow and mints the assertion, with the
same policy and the same key so the assertion it signs verifies against the keys
the server publishes:

```console
$ flow worker --auth-policy examples/federation-flow-to-flow/auth-policy.yaml \
    --identity-key /etc/flowstate/identity.pem \
    --deployment-name prod --build-id "$(git rev-parse --short HEAD)"
```

Deployment B's **server** authenticates callers against `trust.yaml`, which
names A's issuer, and fetches A's keys from A's server above:

```console
$ flow server --auth-policy examples/federation-flow-to-flow/trust.yaml
```

`--deployment-name prod` on A's worker is what makes the assertion carry
`deployment: prod`, which B's `trust.yaml` requires. A rehearsal has to supply
the same identity by hand, because `flow run local` defaults `--as-namespace` to
empty and `--as-deployment` to `local` — and A's own allow rule requires
namespace `acme` (a mismatch is `ErrAssumeDenied`, before anything is minted)
while B requires deployment `prod`:

```console
$ flow run local examples/federation-flow-to-flow/workflow.yaml \
    --auth-policy examples/federation-flow-to-flow/auth-policy.yaml \
    --identity-key /etc/flowstate/identity.pem \
    --as-namespace acme --as-deployment prod
```

## What the test here proves, and what it does not

[`workflow.test.yaml`](workflow.test.yaml) pins the half a Flowfile owns: the
step names a target and holds no credential material — no bearer, no
`Authorization` header, nothing resolved. An edit that embedded a token fails
it.

`TestEveryNetworkedExampleRuns`, and its durable twin `TestEveryExampleRunsDurably`,
then *run* this file on both drivers against a stand-in with a broker holding a
real `assertion` target. That stand-in does two things a bare echo would not: it
refuses a request with no bearer token, so the example only passes if the worker
really minted an assertion and applied it (deleting `credential:` fails it); and
it decodes the body as a real `GetRequest` and rejects one that is not shaped
like one, so the example cannot pass on a request a real peer would answer 400
to. It answers a canonical proto-JSON `GetResponse`, which is why the workflow
reads the status back under the wire field name.

What no example harness can do is stand up two deployments, so none proves the
peer admits what this step sends. That is `TestFlowstateToFlowstateFederation`
in `pkg/flowstate/v1/auth`, which runs both halves in one process: A's issuer
serves its real discovery document and key set over HTTP, B's authenticator
verifies against them, and the assertion is admitted, named, and landed in a
namespace — and refused when its audience names somebody else.
