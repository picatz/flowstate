# subscription

A customer's subscription as a long-lived entity: one run per subscription, alive
for months, changed by the events other systems send it. This is the service-shaped
counterpart to the request-shaped examples; compare [entity-order](../entity-order),
which is the same loop around a single accumulating number.

```
trial ──payment_succeeded──▶ active ──payment_failed──▶ past_due ──(3rd failure)──▶ cancelled
  │                            ▲  └─────────────────────────┘ payment_succeeded         ▲
  └──────── cancel, or a month of silence ─────────────────────────────────────────────┘
```

## What to notice

- **State is a value the loop carries.** `sub` holds the status, plan and failure
  count. Each pass waits for one `event` signal, computes the next state once, and
  `update:` carries it. The run survives restarts and Continue-As-New in between.
- **A decision table, not nested conditionals.** `outcomes` lists what each event
  kind does to the state, and `next` indexes it by kind. An unknown kind changes
  nothing, so a sender that gets ahead of the file cannot wedge the run.
- **Silence is an event.** The wait's 30-day timeout reads as `quiet`: it ends a
  trial or an overdue account and leaves an active one alone.
- **The ending state is the last pass's `next`.** `update:` never runs once
  `until:` holds, so `state` is the state going into the final pass; `ended_as`
  reads the last result instead.

## Try it

```console
$ flow test examples/subscription
$ flow run local examples/subscription/workflow.yaml \
    --signal event='{"kind":"payment_succeeded"}' \
    --signal event='{"kind":"cancel"}'
```

Durably, `flow run --detach` prints the workflow id, and billing then sends each
event with `flow signal <id> event --data '{"kind":"payment_failed"}'`. Which
senders may signal it is deployment policy; add a `signals:` rule for `event`
before exposing it beyond a rehearsal.
