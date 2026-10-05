# An approval that escalates instead of hanging

An expense report has to be approved by a manager before it is reimbursed — and a
manager who is on leave, or who simply misses the message, cannot be allowed to leave
the request stuck forever. A real policy answers that with an escalation: if nobody
in the first role responds, ask someone in a second one, and only give up if neither
does.

## The durability property

Both gates are `wait_for_signal:` steps. A wait like this is not a worker sitting in
a loop asking "has anyone answered yet" — it is state on the execution substrate
itself, so it survives however many times the worker fleet gets redeployed while it
is open, and it costs nothing while it is waiting. A cron job or a queue consumer
polling for an answer would have to persist which report it is waiting on, which gate
it is at, and re-derive all of that after every restart; here there is nothing to
re-derive, because the run itself already *is* that state.

## Two commands

Zero infrastructure, no Temporal, no server — and nobody answers either gate, so both
lapse and the report ends up denied:

```console
$ flow run local examples/expense-approval/workflow.yaml \
    --input-file examples/expense-approval/inputs.json
```

The same file, unchanged, is what a worker executes durably against Temporal; the
only difference is that a real deployment's gates would carry a `timeout:` of hours
or days rather than the few seconds this example ships with, and an answer arriving
three days later would resolve it exactly as fast as one arriving three seconds later
does here. Answer the first gate to see the ordinary path instead of the lapse:

```console
$ flow run local examples/expense-approval/workflow.yaml \
    --input-file examples/expense-approval/inputs.json \
    --signal manager-approved='{"approved": true}'
```

And the same file, run durably instead of in this process. `flow server dev` starts
Temporal, the API server and a worker on loopback with no sign-in, and keeps
everything in memory (the first launch downloads the Temporal CLI):

```console
$ flow server dev
```

In another terminal, start the report with `--detach` so the command returns while
the run waits, and give the first gate a timeout long enough to answer
(`timeout: 10m` in place of `4s`):

```console
$ flow run --detach examples/expense-approval/workflow.yaml \
    --input-file examples/expense-approval/inputs.json
$ flow list
```

Then address the id `flow list` shows, and read the result:

```console
$ flow signal <workflow-id> manager-approved --data '{"approved": true}'
delivered manager-approved to <workflow-id>
$ flow get <workflow-id>
COMPLETED workflow <workflow-id> ...
outputs
  amount 482.5
  approved_by anonymous
  outcome approved_by_manager
```

Without a sign-in, `approved_by` is `anonymous`; a production deployment names the
verified approver and can refuse an answer from anyone else, which
[`approval-gate`](../approval-gate) shows.

`flow signal` is the durable spelling of `--signal`, addressed to a workload
already waiting rather than answered before it starts — which is the part a local
run cannot show at all, since a local run is a process with nobody left to send
anything to once it has started.

## The interesting lines

- **`timeout: 4s` on `manager_review`.** Short because this is an example to run
  rather than to wait on; the shape at 48 hours is identical. A wait with no
  `timeout:` at all is the right choice for an approval that must genuinely block
  until a person acts — it waits as long as the run lasts, see `wait_for_signal:`
  in [docs/LANGUAGE.md](../../docs/LANGUAGE.md#wait_for_signal) — and a timeout is
  right whenever the workload has somewhere sensible to go when nobody does.
- **`escalate` is a real step, not a comment.** `if: ${steps.manager_review.timed_out}`
  runs it only on the lapsed path, and it is what turns "eventually times out" into
  "escalates": a person reading the run's log sees that the report was asked about
  twice, not that it silently vanished after a day.
- **`finance_review` also guards on `steps.manager_review.timed_out`.** An approved or
  explicitly rejected report has nothing left to escalate, and without the guard this
  step would run — and wait — on every path, not just the one where the first gate
  lapsed.
- **Each gate shapes its own answer, and nothing downstream nests.** A gate's
  `outputs:` name `decision`, `approved_by` and `timed_out` where `payload` and
  `sender` are actually in scope, and the four endings are then assembled by four
  one-question `value:` steps — `direct_outcome`, `escalation_decision`,
  `escalation_outcome`, `outcome` — rather than by one ternary holding another.
  That is `docs/STYLE.md` R5, and `flow lint` reports the nested form.
- **Every read of the second gate is guarded by the first one's lapse.** A skipped
  step produces no outputs, so reading `steps.finance_review.decision`
  unconditionally would fail the run rather than evaluate to false. Stating
  `if: ${steps.manager_review.timed_out}` on the steps that read it says that once,
  instead of every expression re-testing it.
- **`outputs.outcome` names which gate decided it.** `"denied"` and
  `"denied_no_response"` are both refusals, and a caller deciding whether it is worth
  asking again needs to know which one happened — a bare `approved: bool` would have
  thrown that away.
