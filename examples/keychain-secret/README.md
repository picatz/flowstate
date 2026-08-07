# The local-development backend: the macOS keychain

[`workflow.yaml`](workflow.yaml) writes `${secret('keychain:github-token')}` where a
credential would go, resolved the same way every other secret reference in this
engine is: worker-side, as late as possible, and never written into workflow
history. See [`http-secret`](../http-secret) for the fuller explanation of that
mechanism.

`keychain:` is a development convenience, not a production backend — see
[`pkg/flowstate/v1/secrets`](../../pkg/flowstate/v1/secrets)'s `KeychainProvider`
doc for why. It exists for the developer running a workflow locally who already has
a credential in the keychain and would rather not paste it into a shell profile or a
checked-out file.

## Storing the secret

```console
$ security add-generic-password -s flowstate -a github-token -w
```

That stores the entry `flowstate`'s `github-token` account, which is what
`keychain:github-token` reads by default.

## Running it

```console
$ flow run local examples/keychain-secret/workflow.yaml \
    --secret-keychain \
    --auth-policy examples/keychain-secret/auth-policy.yaml
```

macOS may prompt for authorization to read the entry the first time; that is the
keychain doing its job; there is nothing this provider does to suppress it.

This is the one backend genuinely unavailable off its platform, so it has no
`*.test.yaml`: this repository's CI runs on Linux, where `--secret-keychain` refuses
at startup with a message naming the platform rather than the generic "tool
missing" a machine without `security` would otherwise report —

```
--secret-keychain only works on macOS (this worker is running on linux); the
security tool the keychain provider shells out to does not exist here
```

— which is exercised directly in
[`cmd/flow/secrets_test.go`](../../cmd/flow/secrets_test.go)'s
`TestSecretRegistryKeychainOnNonDarwinFailsWithAClearMessage`, on every platform CI
actually runs on. `flow fix --check` and `flow validate` still hold this file to the
grammar on every platform, which is what the rest of CI does with it.

## Configuration surface

| Flag | Environment variable | Purpose |
| --- | --- | --- |
| `--secret-keychain` | `FLOWSTATE_SECRET_KEYCHAIN` | Register the `keychain:` scheme. Unset, no scheme is registered and `${secret('keychain:...')}` fails as unknown rather than resolving empty. |
| `--secret-keychain-service` | `FLOWSTATE_SECRET_KEYCHAIN_SERVICE` | The keychain service entries are stored under, in place of `flowstate`. |
| `--secret-keychain-namespaced` | — | Give each tenant its own keychain service, `<service>/<namespace>`, including the unnamespaced tenant's own segment. Off by default: a worker configured for one tenant must not become multi-tenant because an identity happened to carry a namespace. |
