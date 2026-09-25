# flowstate-plugin-jose

One task, `jose.verify`: check a JWT against the issuers an **operator** trusts,
and return the verified claims.

It is [`pkg/flowstate/v1/auth`](../../pkg/flowstate/v1/auth)'s verifier — the
one the engine authenticates its own callers with — with a task in front of it.
There is no second implementation of signature checking, key fetching, clock
skew or algorithm pinning in this tree, and this plugin exists partly so there
never is.

An example lives at [`examples/plugins/jose`](../../examples/plugins/jose).

## Why this is not the generic JWT plugin the audit refused

[#1344](https://github.com/picatz/flowstate/issues/1344)'s candidate audit
declined a generic JWT plugin, precisely: a generic verifier "must choose trust
roots, algorithms, claim policy, time/replay semantics, and key refresh", and
generic signing "would put private-key use and bearer outputs into history".

Both refusals hold here. What is left when they do is this:

| The objection | Where the answer lives |
| --- | --- |
| trust roots | the operator's policy file, never a task input |
| algorithms | that file, per issuer |
| claim policy | that file (`require:`), plus the workflow's own CEL over verified claims |
| time semantics | the engine's verifier: clock skew, `exp`, `nbf`, `iat` |
| key refresh | the engine's verifier: cache TTL, refresh floor, bounded fetches |
| signing | **not here at all** — no key custody, no bearer output |

## Configuring

```console
$ flow worker --plugin-dir /path/to/plugins \
    --plugin-env jose=FLOWSTATE_JOSE_TRUST=/etc/flowstate/trust-policy.yaml
```

The file is **the same document `flow server --auth-policy` reads**. A
deployment that already trusts an issuer for its own API points both at one
file, and a reviewer learns one spelling:

```yaml
issuers:
  - name: github-actions
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    require:
      - claim: repository
        any_of: [acme/api]
```

With no file this plugin verifies nothing and says so, and `flow plugins`
reports it unhealthy with the reason. There is no default trust root, because
the only available default would be "whatever the token says about itself" —
the failure verification exists to prevent.

Fetching an issuer's key set goes through the deployment's egress policy, so a
JWKS URL is a destination like any other. Where the grant cannot be used, the
verifier is built with a deny-by-default policy rather than none: a policy whose
issuers all use `jwks_file` needs no network, and one that needs the network
fails closed.

## Verification is not authorization

`jose.verify` answers *is this token real, from a trusted issuer, for this
audience, unexpired*. It does not answer *may this subject do this* — that is a
CEL decision over the claims it returns, made in the workflow where the
workflow's own policy lives. The example shows the split.

`trust:` and `audience:` inputs **narrow and never widen**: a token the policy
would refuse is refused whatever a step says, and naming them is how a workflow
says "this must be the build system's token, for us" rather than "any issuer we
happen to trust".

## Replay

This task says nothing about whether a token was seen before, because "before"
is a scope only the workflow knows — a run, a day, a tenant. A workflow needing
one-time use records the `jti` claim this task returns and refuses a repeat
itself.

## Tokens as inputs

`token` may be written as a secret reference and is **not required** to be one —
the only input rule in this tree looser than the credential-carrying tasks. A
token to be verified usually arrives *in* the run (a webhook body, a callback,
a prior response) rather than from the deployment's secret store, and requiring
a secret reference would make that case unwritable.

The cost is worth stating: a token passed as a literal is written into durable
history like any other input. A token that arrived in the run is already there;
one that came from a secret store should be passed as `${secret(...)}`.

## Bounds

| What | Limit |
| --- | --- |
| a token | 64 KiB (the verifier bounds it again) |
| claims returned | 128, sorted, keys truncated at 128 B |
| `trust` / `audience` inputs | 128 B / 512 B |
| the trust policy file | 1 MiB |

## Classification

Every refusal is permanent — an expired token does not become valid by waiting,
and a token from an untrusted issuer is not a transient condition — except
reaching the issuer's key set, which is `Unavailable` and retryable. A token
that cannot be parsed is `InvalidInput` rather than a refusal: one is fixed by
sending a token, the other by being trusted.

## What was left undone, and why

- **Signing.** It needs private-key custody and produces a bearer credential,
  which must not enter workflow history. Where a workflow needs a token to
  *call* something, that belongs behind a secret provider that resolves one
  worker-side at the point of use.
- **`jose.decode` without verification.** A task that returns unverified claims
  is a task whose output looks exactly like this one's and means nothing. If a
  workflow needs to see a token it cannot verify, that is a trust policy that
  has not been written yet.
- **JWE (encrypted tokens).** No workload has asked, and the decryption key
  would be key custody this plugin deliberately does not have.
