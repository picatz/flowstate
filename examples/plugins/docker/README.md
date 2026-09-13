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
$ go build -o ./plugins/flowstate-plugin-docker ./plugins/docker
$ flow worker --plugin-dir ./plugins \
    --plugin-env docker=FLOWSTATE_DOCKER_GRANTS=$PWD/examples/plugins/docker/grants.yaml
$ flow run examples/plugins/docker/workflow.yaml \
    --input suite=smoke \
    --input expected_approver=release-manager@example.com
```

The run executes the container, then waits — durably, for up to a day — for
someone to read the result and decide:

```console
$ flow signal <run> release-approved --payload approved=true
```

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
