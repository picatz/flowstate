# deployment-reconciler

The problem: something in your system has a desired state and a real state, and
they drift apart. A spec is edited. A node dies. Somebody scales a deployment by
hand at 2am and forgets. You want a thing that notices and fixes it — forever,
without being restarted into correctness.

This is the loop people install an operator framework to get, written as one
file you can read top to bottom.

## The durability property

**A run that is asleep between passes, and answerable while it sleeps.**

Nothing holds a worker, a thread, or a connection during a resync interval. The
run is a durable timer plus the state it carries — the replica count it is
responsible for and how many passes it has made — and a signal can reach it at
any point in the interval. A reconciler doing one pass a minute for a year costs
a timer per pass, not a process for a year, and a worker deployment rolling
underneath it does not interrupt the watch.

That is also why the interval is a `wait_for_signal:` with a `timeout:` rather
than a `sleep:`: a sleep cannot be interrupted, so an announcement arriving one
second into a thirty-second interval would sit unread for twenty-nine of them.

## Why it is shaped this way

Every pass reads the world (`observed`) and compares it against the state the
run carries (`target.replicas`). Nothing in the body branches on *which* event
woke it. That is the level-triggered discipline, and it is the difference
between a reconciler and an event handler:

| | edge-triggered | level-triggered |
| --- | --- | --- |
| a notification is dropped | that change is lost, permanently | the next pass finds it anyway |
| the same one arrives twice | acted on twice | second pass observes a converged workload and does nothing |
| the process was down when it fired | missed | the resync interval catches it |

An edge-triggered reconciler is correct exactly until the first missed edge, and
the failure is silent — which is why the signal here changes only *when* the next
pass happens, never what it decides. The payload of a `spec-changed` carrying
`desired_replicas: 7` updates what this run is responsible for; it is still the
next pass's own observation that decides whether anything needs writing.

Two smaller decisions are worth copying. The comparison is named once
(`drift`) and read by both the step that acts and the output that counts, rather
than written twice. And the act is behind an `if:`, so a converged workload is
never written to — a reconciler whose steady state is a stream of no-op writes
is one operators learn to ignore.

## Local, then durable

```console
$ flow test examples/deployment-reconciler/
```

Four cases on a virtual clock: drift nobody announced, a spec change mid-interval,
a workload already matching its spec, and a refused input. The intervals are
thirty seconds each and the whole file runs in milliseconds. The scripted control
plane answers differently on the first pass than on the ones after it, which is
what lets a test assert convergence rather than assert that a loop ran.

```console
$ flow run local examples/deployment-reconciler/workflow.yaml \
    --signal spec-changed='{"retired": true}'
```

The same file with a real clock and no stubs — it reaches for a control plane at
`control-plane.internal.example.com`, which does not exist, so this is the command
that shows you what a failing step says rather than a working reconciler.

Durably, on a server and a worker, it is the same file again and the sleeping is
real:

```console
$ flow run examples/deployment-reconciler/workflow.yaml
$ flow signal <run-id> spec-changed --payload '{"desired_replicas": 7}'
$ flow signal <run-id> spec-changed --payload '{"retired": true}'
```

## What this file is still waiting for

Two things in [#179](https://github.com/picatz/flowstate/issues/179)'s sketch of
this workflow do not exist yet, and neither is faked here:

- **An n-way `select:`** ([#166](https://github.com/picatz/flowstate/issues/166)
  shape 6). `pace` waits for one signal *or* a deadline, which is a two-way
  select spelled with the grammar that exists — `timed_out` names which won. A
  third arm (a second signal, a cancellation) has no spelling today. The loop and
  the level-triggering, which is what makes this a reconciler, do not depend on
  it.
- **A `k8s.get` / `k8s.scale` task.** The control plane is reached over `http:`,
  the same choice `enterprise-incident-response` makes, so this file runs without
  a plugin process. The shape does not change when a typed task arrives — which is
  itself the useful finding: a reconciler is not made of special verbs.
