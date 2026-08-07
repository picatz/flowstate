# A password manager shared across a team: 1Password

[`workflow.yaml`](workflow.yaml) writes `${secret('op:github#token')}` where a
credential would go, resolved the same way every other secret reference in this
engine is: worker-side, as late as possible, and never written into workflow
history. See [`http-secret`](../http-secret) for the fuller explanation of that
mechanism.

Like `keychain:`, `op:` is a local-development convenience — but unlike the
keychain, it works on any platform the 1Password CLI runs on and is shared across a
team, which makes it a reasonable way to give several developers the same
development credentials without passing them around. See
[`pkg/flowstate/v1/secrets`](../../pkg/flowstate/v1/secrets)'s `OnePasswordProvider`
doc for the full story.

## Reference syntax

```
op:github          reads the "password" field of the item "github"
op:github#token    reads the "token" field of that item
```

## Running it

Authentication is the CLI's own business — `op` must already be signed in, through
the desktop app's integration, a service account token, or `op signin` — before this
runs:

```console
$ flow run local examples/onepassword-secret/workflow.yaml \
    --secret-op \
    --auth-policy examples/onepassword-secret/auth-policy.yaml
```

This directory has no `*.test.yaml`: CI has no signed-in `op` CLI to run against,
and a stub standing in for one would prove nothing about the actual configuration
surface below. What CI does check is that `op` being absent refuses at startup
rather than mysteriously — `TestSecretRegistryOnePasswordFailsClosedWithoutTheCLI`
in [`cmd/flow/secrets_test.go`](../../cmd/flow/secrets_test.go) pins exactly that —
plus `flow fix --check` and `flow validate` holding this file to the grammar, which
is what the rest of CI does with it.

## Configuration surface

| Flag | Environment variable | Purpose |
| --- | --- | --- |
| `--secret-op` | `FLOWSTATE_SECRET_OP` | Register the `op:` scheme. Unset, no scheme is registered and `${secret('op:...')}` fails as unknown rather than resolving empty. Refused at startup if the `op` CLI is not on `PATH`. |
| `--secret-op-vault` | `FLOWSTATE_SECRET_OP_VAULT` | The vault read when a run has no namespace, in place of `flowstate`. |
| `--secret-op-namespaced` | — | Give each tenant its own vault, named after the namespace, including an unforgeable segment for the unnamespaced tenant. Off by default: a worker configured for one tenant must not become multi-tenant because an identity happened to carry a namespace. |
