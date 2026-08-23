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
retirement ends, or one an operator re-creates. The ceiling is the safety
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

**The stop condition is read from the world, not from the event.** Retirement is
`observed.retired`, taken from the control plane every pass — not a `retired: true`
payload. A `wait_for_signal:` consumes exactly one queued delivery, oldest first,
so a run suspended while a spec change *and* a retirement were both sent consumes
the spec change and finds no retirement in it; a file reading retirement from the
payload would then scale a workload whose retirement was already declared. Asking
the world makes the queue's order irrelevant, which is the same discipline as the
table above applied to the one decision that can do damage.

The wait still comes first in the body, for a smaller reason: a pass is a wake-up
followed by a fresh reading, so nothing acts on an observation taken before it
went to sleep. The cost is that the first pass waits up to one resync interval; a
caller wanting an immediate first pass sends a `spec-changed` with the current
desired count.

Two smaller decisions are worth copying. The comparison is named once
(`drift`) and read by both the step that acts and the output that counts, rather
than written twice. And the act is behind an `if:`, so a converged workload is
never written to — a reconciler whose steady state is a stream of no-op writes
is one operators learn to ignore.

## Local, then durable

```console
$ flow test examples/deployment-reconciler/
```

Eight cases on a virtual clock: drift nobody announced and corrected by the
resync with no signal at all, a spec change mid-interval converged on, a workload
already matching its spec left untouched, a retired workload never scaled even
when drifted and with a spec change queued behind it (the ordering case), an
out-of-range and a wrong-type signal count both ignored, a bounded watch ending on
its own pass budget, and a refused input. The intervals are thirty seconds each
and the whole file runs in milliseconds. The scripted control plane answers on
`goal.passes` — the loop's own carried counter — so it can report one thing on
the first pass and another after this reconciler wrote to it, which is what lets
a test assert convergence rather than assert that a loop ran.

```console
$ flow run local examples/deployment-reconciler/workflow.yaml --input max_passes=1
```

The same file with a real clock and no stubs — it reaches for a control plane at
`control-plane.internal.example.com`, which does not exist, so this is the command
that shows you what a failing step says rather than a working reconciler.

Durably, on a server and a worker, it is the same file again and the sleeping is
real:

```console
$ flow run examples/deployment-reconciler/workflow.yaml
$ flow signal <workflow-id> spec-changed --data '{"desired_replicas": 7}'
```

Retirement is not a signal: this reconciler stops when the control plane reports
the workload retired, which the next pass picks up on its own.

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
