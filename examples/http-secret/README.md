# A secret the workflow never holds

[`workflow.yaml`](workflow.yaml) writes `${secret('env:API_TOKEN')}` where a token
would go. That expression is not resolved when the file is compiled, nor when the
step is scheduled, nor when it is written into workflow history — it stays a
*reference* until the activity that sets the header applies it, in the worker
process, and the value is never anything that gets persisted.

Which is why the directory has a second file.

## `auth-policy.yaml`

A reference names a secret; it does not authorize reading one. Whether *this*
workload may resolve `env:API_TOKEN` is a deployment's decision, so it is written
down where a deployment can review it:

```yaml
secrets:
  allow:
    - 'secret.scheme == "env" && secret.name == "API_TOKEN"'
```

The rules are CEL, compiled and type-checked when the policy loads rather than when
a step runs, so a malformed rule is a startup error. There is no implicit allow: a
process with a secret provider configured and no policy refuses to start, and a
reference no rule matches is denied — the fail-closed posture the whole secrets
substrate is built on.

The `issuers:` block above it is the other half of the same file, and belongs to a
different process: it is what `flow server` accepts inbound tokens from. A real
deployment keeps inbound trust and worker-side secret rules in one reviewed file,
which is why the example ships them together even though the two sections are read
by different commands.

## Running it

The example points at `api.example.com`, so it is meant to be read and adapted
rather than run as written. Pointed at something real, it needs the value in the
environment, the name allowed as an `env:` secret, and the policy above:

```console
$ export FLOWSTATE_SECRET_API_TOKEN=...
$ flow run local examples/http-secret/workflow.yaml \
    --secret-env API_TOKEN \
    --auth-policy examples/http-secret/auth-policy.yaml
```

`flow worker` takes the same two flags, and means the same thing by them: the
rehearsal is governed by the rules production applies.
