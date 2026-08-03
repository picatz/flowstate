# A credential the deployment mints, not the workflow

[`workflow.yaml`](workflow.yaml) has one line that is not about HTTP:

```yaml
      credential: partner-api
```

`partner-api` is not a token and not a reference to a stored one. It names a
*target*, and what happens at the step is an exchange: the worker proves who the
workload is with a short-lived assertion Flowstate signs, the partner's identity
provider gives back an access token for that workload, and the task applies it. No
long-lived credential for the partner exists anywhere in this system — not in the
file, not in the run's history, not in a secret store — so there is nothing to leak
and nothing to rotate.

Which is why the directory has a second file: the exchange is entirely a
deployment's business.

## `auth-policy.yaml`

```yaml
federation:
  issuer: https://flowstate.example.com
  allow:
    - 'target == "partner-api" && workload.step == "health"'
  targets:
    - name: partner-api
      token_exchange:
        token_url: https://identity.partner.example.com/oauth2/token
        audience: https://identity.partner.example.com
        target_audience: https://api.example.com
```

Three things, and they are separable on purpose.

`targets:` is where a name in a Flowfile becomes an endpoint. The workflow says
`partner-api` and nothing about OAuth, so re-pointing an integration at a different
provider is a change to this file and not to any workload.

`allow:` decides who may ask. The rules are CEL over the target and the workload
asking for it — here, only the step named `health`, and only for this target — and
they fail closed: a target no rule allows is refused, an errored rule denies, and
the rules compile when the policy loads rather than when a step runs.

`issuer:` is the identity Flowstate asserts as. Asserting requires a key, so a
worker with federation configured needs `--identity-key` (a PKCS#8 PEM private key,
`flow keys` generates one) and refuses to start without it — it cannot issue an
assertion it cannot sign.

The `issuers:` block above it is inbound trust, read by `flow server` when it
verifies the tokens *callers* present. One reviewed file, two directions.

## Running it

The endpoints here are `example.com`, so this is a file to read and adapt. Against
real ones:

```console
$ flow worker --auth-policy examples/http-federated/auth-policy.yaml \
    --identity-key /etc/flowstate/identity.pem \
    --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"
```

`flow run local` takes the same two flags, so a rehearsal exchanges credentials the
way production does rather than skipping the part most likely to be misconfigured.
