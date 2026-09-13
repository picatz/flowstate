# flowstate-plugin-docker

One task, `docker.run`: run an **operator-defined container** to completion and
return its bounded output. A Flowfile names a run grant and fills its
placeholders; it cannot name an image, a mount, a network, a user, or a resource
limit, because none of those are inputs.

Built on the [Engine API](https://docs.docker.com/reference/api/engine/) over
`net/http` — no Docker SDK, no `docker` binary. The requests this plugin makes
are five, and every field it sets is one the contract below names; a dependency
that could set the others is a dependency whose next version might.

An example lives at [`examples/plugins/docker`](../../examples/plugins/docker),
including the operator's own
[`grants.yaml`](../../examples/plugins/docker/grants.yaml).

## Read this first: what this is not

**This is not a sandbox, and a worker running it is inside the trusted computing
base.**

A Docker Engine socket is ambient daemon authority. A process holding one can
create privileged containers, bind-mount any host path, join the host network
namespace, and read every other container's output — regardless of what this
plugin's task schema says. This plugin declines to ask for those, and declining
is *vetted code*, not a boundary: the daemon would have honoured the request.

`THREAT_MODEL.md`'s three verbs name it exactly. Process separation **bounds**
what crosses the socket; admission decides what **runs**; and the **confine**
column here is the *substrate's* — the runtime's own isolation, which this
plugin requests and does not enforce. Nothing here is called sandboxing, and
what this file bounds is what the plugin will ask for, not what it could.

If that trade is not one a deployment wants, the honest alternatives are a
remote daemon whose authority is not this host's (`address:` plus mutual TLS),
or not installing this plugin.

## The contract

Issue [#1348](https://github.com/picatz/flowstate/issues/1348) asks a container
task for a specific set of guarantees. Each is a grant field or a fixed
behavior, and each is checked when the worker starts or asserted in a test
against the create request a real daemon would parse:

| The requirement | Here |
| --- | --- |
| digest-pinned image identity, tags refused | `image: …@sha256:…`; a tag fails to load, naming digests |
| explicit argv, no shell | `argv:` is a list; parameters become single elements |
| environment assembled from nothing | `env:` only; the worker's environment never reaches a container |
| read-only root filesystem by default | `ReadonlyRootfs: true` unless `writable_root_filesystem` |
| non-root user required | defaults to `65534:65534`; root has to be written as `user: "0:0"` |
| mounts from operator grant identifiers | `mounts: [name]` resolved through the file's own `mounts:`; read-only unless `writable` |
| network none by default | `NetworkMode: none`; **host networking is refused at every grant level** |
| CPU, memory, pids, wall time, output bytes all bounded | required in the grant, each under a plugin ceiling |
| cancellation stops and removes before returning | a deferred remove with its own deadline, on every path |
| no runtime credentials in the request or result | there are no registry credentials here at all |

Beyond the list: every capability is dropped (`CapDrop: ["ALL"]`, and a grant
cannot add one back), `no-new-privileges` is set, and no TTY or stdin is
attached — so the streams stay separable and a container waiting for input
cannot hold a call open.

## Building and configuring

```console
go build -o /path/to/plugins/flowstate-plugin-docker ./plugins/docker
```

A plugin inherits nothing of the worker's environment, so the grants file is
named to the worker:

```console
$ flow worker --plugin-dir /path/to/plugins \
    --plugin-env docker=FLOWSTATE_DOCKER_GRANTS=/etc/flowstate/docker-grants.yaml
```

With no grants file there is no daemon and no run: every call is refused naming
the variable, and `flow plugins` reports the plugin unhealthy with the reason.
Discovery and validation keep working, so a deployment can install the plugin
before it configures it.

## Ceilings

A grant narrows within these and can never raise past them:

| What | Ceiling |
| --- | --- |
| run timeout | 30m (default 5m) |
| stdout and stderr, each | 4 MiB (default 256 KiB) |
| memory | 16 GiB (required) |
| CPU | 8 cores in nanocpus (required) |
| pids | 4096 |
| mounts per run | 8 |
| environment variables per run | 32 |
| parameters per call | 32 |
| grants file | 1 MiB |

Output over the limit sets `truncated`. A run over its timeout is removed and
returns `OutcomeUnknown` — the container is gone, but what it had already done
is not knowable from here, so it is never retried automatically.

## Exit status

`exit_code` is always reported. Whether a status is a *failure* is the
operator's call: `success_exit_codes` (default `[0]`) decides, because "does
this image exit 1 when tests fail" is a fact about the image that whoever
granted it knows. A status outside the set fails the step **and still returns
stdout and stderr**.

## Output framing

The daemon multiplexes stdout and stderr over one stream in eight-byte frames
when no TTY is allocated, which is always here. This plugin demultiplexes them.
Asking for a TTY instead would merge the streams at the source and lose the
distinction between what a container reported and what it complained about —
and allocating one is a capability, not a formatting choice.

## Tenancy

A run grant may list `namespaces`, compared against the namespace the server
established for the calling workload rather than one the workload declared.

## Why there is no `docker.pull`, `docker.exec` or `docker.build`

Each is a different authority with a different contract. Pulling is the
daemon's, and a grant naming a digest is what makes the image this plugin runs
the image an operator reviewed. `exec` attaches to a container somebody else's
grant created. `build` turns a workflow's input into an image, which is the
supply chain [`plugins/oci`](../oci) exists to *check* rather than to
manufacture. None of them is a flag on this task.

There are also no registry credentials. An image the daemon cannot pull is a run
that fails, and the fix is the operator's: pre-pull it, or configure the
daemon's own credentials. A registry credential in a task input would put the
deployment's pull authority into workflow history.

## What was left undone, and why

- **A `container.run` runtime interface with a containerd or remote-sandbox
  adapter.** #1348's larger shape. This plugin is the Docker adapter that shape
  would have; naming the abstraction before a second backend exists would be
  guessing at what it has to be.
- **Streaming output while the container runs.** The output is read after the
  wait, because reading it during the run races the exit status this result is
  about. A progress-reporting version is a different design.
- **Long-lived containers, `docker.stop`, `docker.logs` against something this
  plugin did not create.** Each needs a way to name a container across steps,
  which is state this plugin deliberately does not keep: a container cannot
  outlive the activity that created it, the same rule `plugins/sql` applies to
  a transaction.
