# flowstate-plugin-ssh

One task, `ssh.run`: run an **operator-defined command** on an
**operator-defined host**. A Flowfile names two grants and fills their
placeholders; it cannot name an address, a user, a port, a key, or a program,
because none of those are inputs.

Built on [`golang.org/x/crypto/ssh`](https://pkg.go.dev/golang.org/x/crypto/ssh)
- no `ssh` binary, no `known_hosts`, no agent.

An example lives at [`examples/plugins/ssh`](../../examples/plugins/ssh),
including the operator's own
[`grants.yaml`](../../examples/plugins/ssh/grants.yaml); read that first.

## The refusal this is an answer to

This repository declined to build a generic SSH task, and the reasons were
right: host-key policy, private-key custody, and an outcome nobody can resolve
when a connection drops mid-command.

What is wrong with that shape is not SSH — it is that **the Flowfile names the
authority**. Every part of the refusal dissolves when the operator names it
instead, which is the same move `#1348` makes for a container's mounts (grant
identifiers resolved by operator configuration, never arbitrary host paths from
a workflow) and the same one `plugins/sql` makes for a query (every value bound,
never interpolated into SQL text).

| The objection | What this plugin does |
| --- | --- |
| host-key policy | Grants pin the public keys a host may present. No trust-on-first-use, no `known_hosts`, and the handshake is refused **before the command is sent**. |
| private-key custody | A key never crosses this plugin's boundary as data. No task input carries one; the grant names a file the worker reads. `ssh.run` declares no `secret_inputs` at all. |
| arbitrary execution | The operator writes the argv. A workflow fills declared placeholders, each checked against the grant's own pattern and quoted as a single argument. |
| unknown outcomes | A failure before the exec request is a definite no-run and retryable. A failure after it is `OutcomeUnknown` and is never retried automatically. |

## Building and configuring

```console
go build -o /path/to/plugins/flowstate-plugin-ssh ./plugins/ssh
```

A plugin inherits nothing of the worker's environment, so the grants file is
named to the worker:

```console
$ flow worker --plugin-dir /path/to/plugins \
    --plugin-env ssh=FLOWSTATE_SSH_GRANTS=/etc/flowstate/ssh-grants.yaml \
    --egress-policy /etc/flowstate/egress.yaml
```

**Two independent operator statements are required.** The grants file names the
host; the egress policy permits reaching it. This plugin **refuses the worker's
built-in default egress policy** — the posture `plugins/sql` takes, sharpened:
a default policy is what a deployment runs under when nobody has decided
anything about destinations, which is not a decision to permit executing
commands on a machine. The policy must also list the `ssh` scheme, exactly as a
database policy must list `postgres`.

Without either, `ssh.run` refuses every call and `flow plugins` reports the
plugin unhealthy with the reason — discovery and validation keep working, so a
deployment can install the plugin before it configures it.

## The grants file

See [`examples/plugins/ssh/grants.yaml`](../../examples/plugins/ssh/grants.yaml)
for a commented one. In short:

```yaml
hosts:
  web-prod:
    address: web1.prod.example.com:22
    user: runbook
    identity_file: /etc/flowstate/ssh/runbook_ed25519
    host_keys: ["ssh-ed25519 AAAA…"]     # pinned; no TOFU
    commands: [service-status, restart-service]
    namespaces: [platform]               # optional tenant scoping
commands:
  restart-service:
    argv: ["/usr/bin/systemctl", "restart", "${service}"]
    parameters:
      service: {pattern: '[a-z0-9-]{1,64}\.service', max_bytes: 64}
    timeout: 60s
```

Everything checkable is checked **when the worker starts**: a program that is
not an absolute path (so which binary runs never depends on the remote `PATH`),
a placeholder with no parameter, a parameter that is never used, a pattern that
does not compile, a host permitting a command nobody granted, a host with no
pinned key, a timeout or output limit over this plugin's ceiling. An operator
learns from the worker's logs rather than from a runbook at three in the
morning.

Patterns are anchored to the whole value (`^(?:…)$`), so a pattern that "appears
somewhere in" a value never constrains one.

## The session

One session channel, one `exec` request. **No PTY, no agent forwarding, no port
or X11 forwarding, no subsystem, no shell.** Those are not defaults left alone;
nothing here requests them and no grant can ask for them. The test suite asserts
it against a real in-process SSH server, by inspecting the channel requests the
far side received.

An `exec` request carries a command *line*, which the remote account's shell
parses — that is the protocol, not a choice. So every argument is single-quoted
before it is joined, **the operator's own argv included**: quoting only what a
workflow filled would make the guarantee depend on remembering which half a
string came from. A parameter is checked against its pattern *and* quoted, on
the principle that one of the two should be redundant.

## Bounds and ceilings

| What | Limit |
| --- | --- |
| grants file | 1 MiB |
| command timeout | grant's, default 60s, ceiling 10m |
| connect timeout | grant's, default 15s, ceiling 2m |
| stdout and stderr, each | grant's, default 64 KiB, ceiling 1 MiB |
| a parameter value | grant's `max_bytes`, default 256 B |
| parameters per call | 32 |
| addresses one host name may resolve to | 8 |
| private key file | 64 KiB |

Output over the limit sets `truncated`, so a cut-off stream is never readable as
a complete one. Output that is not valid UTF-8 comes back empty rather than as
mojibake — a step output is not where arbitrary binary belongs.

## Exit status

`exit_code` is always reported. Whether a status is a *failure* is the
operator's call, not the workflow's: a grant's `success_exit_codes` (default
`[0]`) decides, because "does this command exit 3 when the unit is inactive" is
a fact about the command that whoever granted it knows. A status outside that
set fails the step **and still returns stdout and stderr**, so a runbook
debugging it is not left guessing.

## Tenancy

A host grant may list `namespaces`. The namespace compared is the one the server
established for the calling workload, never one the workload declared, so a
namespaced grant is one another tenant's workflows cannot spend. A grant naming
no namespaces is available to every namespace, which is what a single-tenant
deployment has.

## What was left undone, and why

- **File transfer (SFTP/SCP).** A different capability with its own bounds
  (path traversal, size, partial writes). Not a flag on this one.
- **A workflow-supplied command.** That is the shape the audit refused and this
  plugin exists not to be.
- **Jump hosts and agent forwarding.** Both widen what a single grant reaches,
  and neither has a contract yet that says what the far end may do with it.
- **Reusing one connection across steps.** Each call connects, runs and
  disconnects, so a session cannot outlive the activity that opened it — the
  same rule `plugins/sql` applies to a transaction.
