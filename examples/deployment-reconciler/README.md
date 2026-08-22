# deployment-reconciler

The problem: something in your system has a desired state and a real state, and
they drift apart. A spec is edited. A node dies. Somebody scales a deployment by
hand at 2am and forgets. You want a thing that notices and fixes it — until it is
told to stop — without being restarted into correctness.

This is the loop people install an operator framework to get, written as one
file you can read top to bottom.

## The durability property

**A run that is asleep between passes, and answerable while it sleeps.**

Nothing holds a worker, a thread, or a connection during a resync interval. The
run is a durable timer plus the state it carries — the replica count it is
responsible for and how many passes it has made — and a signal can reach it at
any point in the interval. A reconciler waiting out a thirty-second interval
costs a durable timer, not a running process, and a worker deployment rolling
underneath it resumes the watch exactly where it was.

That is also why the interval is a `wait_for_signal:` with a `timeout:` rather
than a `sleep:`: a sleep cannot be interrupted, so an announcement arriving one
second into a thirty-second interval would sit unread for twenty-nine of them.

What it is *not* is unbounded. `max_iterations` caps the total passes a single
run may make — a cumulative bound across every Continue-As-New segment, not a
fresh budget per segment — so a genuinely perpetual reconciler is one a
retirement signal ends, or one an operator re-creates. The ceiling is the safety
net that turns "the retirement never came" into a named failure rather than a
loop that runs forever.

## Why it is shaped this way

Every pass reads the world (`observed`) and compares it against the state the
run carries (`goal.desired`). Nothing in the body branches on *which* event
woke it. That is the level-triggered discipline, and it is the difference
between a reconciler and an event handler:

| | edge-triggered | level-triggered |
| --- | --- | --- |
| a notification is dropped | that change is lost, permanently | the next pass finds it anyway |
| the same one arrives twice | acted on twice | a later pass observes a converged workload and does nothing |
| the process was down when it fired | missed | the resync interval catches it |

An edge-triggered reconciler is correct exactly until the first missed edge, and
the failure is silent — which is why the signal here changes only *when* the next
pass happens, never what it decides. The payload of a `spec-changed` carrying
`desired_replicas: 7` updates what this run is responsible for; it is still the
next pass's own observation that decides whether anything needs writing. And
because a signal is an untrusted input the input constraint never saw, a payload
count is adopted only when it is an int in the same `(0, 100]` range — a wrong
type or an out-of-range value is ignored rather than POSTed or allowed to crash
the run.

**The wait comes first, and that ordering is load-bearing.** The scheduler — the
`wait_for_signal:` — is the *first* step of each pass, not the last, because it
is the only place a `retired: true` is consumed. A retirement can arrive while
the run is suspended (buffered across a Continue-As-New, delivered before the
body reaches the wait); if the mutation ran before the wait consumed it, a
workload already declared retired would get one last scale on its way out. With
the wait first and every mutation gated on the pass not being a retirement, a
delayed or buffered retirement ends the run before anything is observed or
written. The cost is that the very first pass waits up to one resync interval
before its first reconcile — a caller wanting an immediate first pass sends a
`spec-changed` carrying the current desired count.

Two smaller decisions are worth copying. The comparison is named once
(`drift`) and read by both the step that acts and the output that counts, rather
than written twice. And the act is behind an `if:`, so a converged workload is
never written to — a reconciler whose steady state is a stream of no-op writes
is one operators learn to ignore.

## Local, then durable

```console
$ flow test examples/deployment-reconciler/
```

Seven cases on a virtual clock: drift nobody announced and corrected by the
resync, a spec change mid-interval converged on, a workload already matching its
spec left untouched, a `retired: true` buffered before the first pass ending the
run with *no* scale (the ordering fix), an out-of-range and a wrong-type signal
count both ignored, and a refused input. The intervals are thirty seconds each
and the whole file runs in milliseconds. The scripted control plane answers on
`goal.passes` — the loop's own carried counter — so it can report one thing on
the first pass and another after this reconciler wrote to it, which is what lets
a test assert convergence rather than assert that a loop ran.

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
