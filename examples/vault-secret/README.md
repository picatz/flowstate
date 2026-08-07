# The regulated-deployment backend: Vault or OpenBao

[`workflow.yaml`](workflow.yaml) writes `${secret('vault:apps/api#token')}` where a
credential would go. Like every secret reference in this engine, that stays a
*reference* through compilation, submission, and workflow history — it is resolved
once, inside the activity that applies it, and nowhere else. See
[`http-secret`](../http-secret) for the fuller explanation of the mechanism; this
example is about `vault:`'s own configuration.

## Reference syntax

A reference names a path within the configured KV v2 mount, optionally followed by
`#` and the field to read:

```
vault:apps/api#token   the "token" field of the secret at apps/api
vault:apps/api         the only field of the secret at apps/api
```

## Running it

The example points at `api.example.com`, so it is meant to be read and adapted
rather than run as written — and unlike `env:` or `command:`, `vault:` needs a real
Vault or OpenBao server to talk to, which is why this directory has no
`*.test.yaml`: there is nothing this repository's CI can stand a real vault up as
and still be testing what an operator's vault does. `flow fix --check` and `flow
validate` still hold this file to the grammar, which is what CI does run.

Pointed at a real instance with a static development token:

```console
$ export FLOWSTATE_SECRET_VAULT_TOKEN=s.development-token-value
$ flow run local examples/vault-secret/workflow.yaml \
    --secret-vault-addr https://127.0.0.1:8200 \
    --auth-policy examples/vault-secret/auth-policy.yaml
```

Or, for a worker in a Kubernetes cluster, using the Kubernetes auth method instead of
a static token — the credential a long-running worker should prefer, since it
re-authenticates on its own before its lease expires rather than failing every
resolution the moment a static token's lease ends:

```console
$ flow worker \
    --secret-vault-addr https://vault.internal:8200 \
    --secret-vault-kubernetes-role flowstate-worker \
    --auth-policy examples/vault-secret/auth-policy.yaml
```

## Configuration surface

| Flag | Environment variable | Purpose |
| --- | --- | --- |
| `--secret-vault-addr` | `FLOWSTATE_SECRET_VAULT_ADDR` | The vault's address. Required; nothing else here does anything until this is set. `https` is required except to a loopback address. |
| `--secret-vault-token-file` | `FLOWSTATE_SECRET_VAULT_TOKEN_FILE` | A file holding a static client token, re-read on every login. |
| — | `FLOWSTATE_SECRET_VAULT_TOKEN` | A static token read directly, when no token file is configured. For a development vault or a test; a long-running worker should prefer the file form or Kubernetes auth. |
| `--secret-vault-kubernetes-role` | `FLOWSTATE_SECRET_VAULT_KUBERNETES_ROLE` | The Vault role to authenticate as via the Kubernetes auth method. Exactly one of this or a token must be configured — configuring both, or neither, refuses to start. |
| `--secret-vault-kubernetes-mount` | `FLOWSTATE_SECRET_VAULT_KUBERNETES_MOUNT` | Where the Kubernetes auth method is mounted, when it is not the default. |
| `--secret-vault-mount` | `FLOWSTATE_SECRET_VAULT_MOUNT` | Where the KV v2 engine is mounted, when it is not `secret`. |
| `--secret-vault-path-prefix` | `FLOWSTATE_SECRET_VAULT_PATH_PREFIX` | A path prefix inside the mount, above the namespace segment — useful for keeping Flowstate's secrets in one subtree of a mount other systems also use. |
| `--secret-vault-namespace` | `FLOWSTATE_SECRET_VAULT_NAMESPACE` | The Vault Enterprise or OpenBao namespace header. This is the *vault's* namespace, unrelated to the tenant namespace a run authenticates with. |
| `--secret-vault-ca-file` | `FLOWSTATE_SECRET_VAULT_CA_FILE` | A PEM CA bundle to verify the vault's certificate against, for a private CA. There is no flag to skip verification, in any form. |

Every one of these fails closed: an unset `--secret-vault-addr` means no `vault:`
scheme is registered at all, so `${secret('vault:...')}` fails as an unknown scheme
rather than resolving empty, and a bad address, a missing CA bundle, or a
misconfigured auth method refuses at startup rather than on the first workflow that
needs a secret. See
[`pkg/flowstate/v1/secrets/vault`](../../pkg/flowstate/v1/secrets/vault)'s package
doc for the full authentication, path-layout, and error-classification story —
including why there is no redirect following and no way to skip TLS verification.

`flow worker` takes the same flags `flow run local` does above, and means the same
thing by them: the rehearsal is governed by the rules production applies.
