# Restarting a service, without handing a workflow a shell

Three files here, and the split between them is the point:

- [`workflow.yaml`](workflow.yaml) — what a **workflow author** writes. It names
  a host grant and a command grant, and fills one parameter.
- [`grants.yaml`](grants.yaml) — what an **operator** writes. It is the whole of
  this plugin's authority: the address, the user, the key, the pinned host keys,
  the argv, and the pattern each parameter must match.
- [`egress-policy.yaml`](egress-policy.yaml) — the operator's **second**
  statement: which hosts may be reached at all, over the `ssh` scheme.

Read `workflow.yaml` and notice what is *not* in it: no address, no user, no
port, no key, no program. A Flowfile selects within an operator's authority. It
never composes it.

Run it:

```console
$ mkdir -p ./plugins
$ go build -o ./plugins/flowstate-plugin-ssh ./plugins/ssh
$ flow worker --plugin-dir ./plugins \
    --plugin-env ssh=FLOWSTATE_SSH_GRANTS=$PWD/examples/plugins/ssh/grants.yaml \
    --egress-policy examples/plugins/ssh/egress-policy.yaml
$ flow run examples/plugins/ssh/workflow.yaml \
    --input host=web-prod \
    --input service=nginx.service \
    --input expected_approver=sre-oncall@example.com
```

The run reads the unit's status, then stops and waits for an SRE to approve:

```console
$ flow signal <run> restart-approved --payload approved=true
```

## Why the restart step sets `attempts: 1`

A restart is not safely repeatable on its own. If the connection drops after the
command is sent, `ssh.run` returns an unknown outcome — the command may be
running right now — and the engine does not retry it. The step says so
explicitly rather than leaving a default retry policy to make that decision.

The two status reads around it are a different matter: reading a unit's state
has no side effect, and a grant for it is one an operator can hand out freely.

## What the operator's grant is protecting

`service` is passed straight from a workflow input, and two things stand between
it and the remote shell:

1. The grant's pattern, `[a-z0-9-]{1,64}\.service`, anchored to the whole value.
   `nginx.service; rm -rf /` fails it.
2. Quoting. Even if the pattern were `.*`, the value arrives as exactly one
   argument — the plugin's own tests assert this against a real SSH server for
   separators, substitutions, backticks, redirections and newlines.

One of those should be redundant. That is the point of having both.

## Testing it without a host

[`workflow.test.yaml`](workflow.test.yaml) stubs the `ssh.run` steps, so the
approved path and the refused path both run with no network, no plugin process
and no server:

```console
$ flow test examples/plugins/ssh/
```
