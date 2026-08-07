# The escape hatch: any external command

[`workflow.yaml`](workflow.yaml) writes `${secret('command:github-token')}` where a
credential would go, resolved the same way every other secret reference in this
engine is: worker-side, as late as possible, and never written into workflow
history. See [`http-secret`](../http-secret) for the fuller explanation of that
mechanism.

`command:` is what keeps a long tail of secret backends out of this tree. Point it
at `aws kms decrypt`, `sops -d`, `age -d`,
`aws secretsmanager get-secret-value`, `doppler run`, or anything else that prints a
secret to standard output, and it is reachable — no dependency, no auth mode, no
review added to `pkg/flowstate/v1/secrets`. None of those tools get an in-tree
provider for exactly this reason.

## Configuring the command

`--secret-command` is repeatable; each occurrence is one argument of the command to
run, executable first, so there is never a shell to parse and nothing a secret name
could inject:

```console
$ flow run local examples/command-secret/workflow.yaml \
    --secret-command sops --secret-command -d --secret-command --extract \
    --secret-command '["{{name}}"]' --secret-command /etc/flowstate/secrets.enc.yaml \
    --auth-policy examples/command-secret/auth-policy.yaml
```

`"{{name}}"` is replaced, literally, with the reference's name — `github-token`
here — inside that one argument; `"{{namespace}}"` is available the same way with
`--secret-command-namespaced`, for a command whose secret store is itself
multi-tenant.

## Running it end to end

Unlike `vault:`, `keychain:`, or `op:`, this backend needs no external service or
signed-in CLI to demonstrate — any command that prints something to standard output
is a working `command:` backend, which makes the mechanism itself fully runnable
here, with `printf` standing in for the real tool:

```console
$ flow run local examples/command-secret/workflow.yaml \
    --secret-command printf --secret-command 'ghp_%s' --secret-command '{{name}}' \
    --auth-policy examples/command-secret/auth-policy.yaml
```

The example still points `url:` at `api.example.com`, which is why this directory
has no `*.test.yaml` of its own: `flow test` stubs a workflow's tasks *before* the
activity that would resolve a secret ever runs, so a stubbed `http` step proves
nothing about `command:` — the resolution this example is about never happens on
that path, for any of the four backends this issue wires up. What actually keeps the
mechanism honest in CI is
[`TestSecretRegistryWiresCommandProvider`](../../cmd/flow/secrets_test.go), which
runs this exact CLI-flag-to-provider path with `printf` for real, on every platform
CI runs on, and asserts the resolved value. `flow fix --check` and `flow validate`
hold this file to the grammar, which is what the rest of CI does with it.

## Configuration surface

| Flag | Environment variable | Purpose |
| --- | --- | --- |
| `--secret-command` | `FLOWSTATE_SECRET_COMMAND` (`$PATH`-list-separated) | The argv of the command to run, repeatable, executable first. Unset, no `command:` scheme is registered and `${secret('command:...')}` fails as unknown rather than resolving empty. Refused at startup if the executable is not on `PATH`. |
| `--secret-command-namespaced` | — | Substitute `"{{namespace}}"` with the tenant's namespace, including an unforgeable segment for the unnamespaced tenant. Off by default: a worker configured for one tenant must not become multi-tenant because an identity happened to carry a namespace — a namespaced request is refused, not silently run with an empty substitution. |
