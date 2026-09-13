# A credential that is never written down

Read [`workflow.yaml`](workflow.yaml) and notice what is missing: there is no
step that fetches a token, and no output that holds one.

The credential is `${secret('oidc:billing-api')}` — a reference in the file and
a reference in durable history. The host resolves it worker-side, at the moment
the step runs, under this run's own namespace, and hands the value to the `http`
task alone. It exists in one process for one call.

That is why the `oidc` plugin has **no tasks**. A task that returned a token
would put a bearer credential into every run's record.

```console
$ mkdir -p ./plugins
$ go build -o ./plugins/flowstate-plugin-oidc ./plugins/oidc
$ flow worker --plugin-dir ./plugins \
    --plugin-env oidc=FLOWSTATE_OIDC_PROVIDERS=$PWD/examples/plugins/oidc/providers.yaml
$ flow run examples/plugins/oidc/workflow.yaml --input invoice_id=inv_2026_0917
```

## `bearer:`, not a concatenated header

The step writes `bearer: ${secret('oidc:billing-api')}` rather than building an
`Authorization` header from it. A secret reference has to be the **whole** value
of an input: the value does not exist until the worker running the step resolves
it, so nothing workflow-side can combine it with anything. The engine refuses
the concatenation rather than letting it half-work.

## Testing it without an authorization server

```console
$ flow test examples/plugins/oidc/
```

[`workflow.test.yaml`](workflow.test.yaml) supplies the reference's value
directly. In a run it would be a token the plugin minted for that call; in a
test it is a string, and neither ends up in the outputs.
