# Running a test container as a gated step

Three files, and the split between them is the point:

- [`workflow.yaml`](workflow.yaml) — what a **workflow author** writes. It names
  a run grant and one parameter.
- [`grants.yaml`](grants.yaml) — what an **operator** writes: the daemon, the
  digest-pinned image, the argv, the mounts, the resource bounds.
- [`workflow.test.yaml`](workflow.test.yaml) — both paths, with no daemon.

Read `workflow.yaml` and notice what is *not* in it: no image, no mounts, no
network, no memory limit. That is the whole difference between this and a CI
system's `image:` key — a Flowfile selects within an operator's authority and
cannot compose it.

**Before running this, read the trusted-computing-base note in
[`plugins/docker/README.md`](../../../plugins/docker/README.md).** A worker with
this plugin installed holds ambient daemon authority, and the grants file bounds
what the plugin will ask for, not what it could.

```console
$ mkdir -p ./plugins
$ go -C plugins/docker build -o ../../plugins/flowstate-plugin-docker .
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --plugin-env docker=FLOWSTATE_DOCKER_GRANTS=$PWD/examples/plugins/docker/grants.yaml &
$ flow server --plugin-dir ./plugins --auth-policy /path/to/auth-policy.yaml \
    --rpc-resource https://flowstate.example.com/rpc --identity-claim team &
$ flow run examples/plugins/docker/workflow.yaml \
    --input suite=smoke \
    --input expected_approver=release-manager@example.com \
    --token-file /path/to/starter.token
```

The server takes `--plugin-dir` too, because it checks each task the file names,
and its `plugins:` block, against the plugins it launched itself. It takes
an `--auth-policy` trusting a real issuer, with the `--rpc-resource` its tokens
are minted for, rather than `--insecure-no-auth`, because the approval below is
a signal only an attested release manager other than the starter may send, and
without authentication every caller is the same anonymous one.
It keeps the `team` claim (`--identity-claim team`), because the signal's rule
reads it and a server persists only the claims it names. Every client command
below authenticates with `--token-file` (or `--credential-source`): an
authenticated server refuses an anonymous caller.

`flow run` refuses this file today (#1548): it checks the file against its own
build's task registry, takes no `--plugin-dir`, and so reports `docker.run` as a
task nothing registered before the server sees it. Until that is fixed,
`flow run local` with the worker's `--plugin-dir` and `--plugin-env` runs it in
one process, answering the gate up front with `--signal` and the
`--signal-as-*` flags, and an agent host running
`flow mcp --plugin-dir ./plugins --token-file /path/to/starter.token` submits
it to this server with `flowstate_compile` then `flowstate_run`.

The run executes the container, then waits — durably, for up to a day — for
someone to read the result and decide:

```console
$ flow signal <run> release-approved --data '{"approved": true}' \
    --token-file /path/to/approver.token
```

The approver's token must be issued by `https://issuer.example.com` to the
subject `expected_approver` names, with `team: release-managers`;
`sender.identity.principal != run.identity.principal` refuses the starter's own.

## Why a failing suite is not a failing step

The grant says `success_exit_codes: [0, 1]`, because pytest exits 1 when tests
fail and that is a *result* this workflow reads rather than an error. A
container that could not start at all still fails the step, and the approval
prompt says which happened.

That decision lives in the grants file rather than in the workflow, because
"does this image exit 1 when tests fail" is a fact about the image — known to
whoever granted it, not to whoever writes a Flowfile against it.

## What the operator's grant is protecting

`suite` comes straight from a workflow input, and two things stand between it
and the container:

1. The grant's pattern, `[a-z0-9-]{1,32}`, anchored to the whole value.
2. The absence of a shell. The daemon takes an argv, so the value is one
   element of it and cannot become a second command — proved by the plugin's own
   test, which reads the create request back as a daemon would parse it.

## Testing it without a daemon

```console
$ flow test examples/plugins/docker/
```

Both cases stub the `docker.run` step: the passing suite that is released, and
the failing one that reaches the same human and is not.
