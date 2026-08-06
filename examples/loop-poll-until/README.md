# loop-poll-until

The `loop:` primitive's **stateless mode** — no `as:`, `init:` or `update:`.

A loop does not have to carry a value. This one carries nothing: it repeats a
health check and stops on what the check reports, which is the shape a poll takes.
`until:` reads the body step's own output (`${steps.probe.ready}`), and
`max_iterations:` is the give-up bound — a dependency that never comes ready is not
a loop that should run forever.

```
flow run local examples/loop-poll-until/workflow.yaml
flow test examples/loop-poll-until/
```

## The honest shape of a stateless loop

A stateless loop only makes progress when the thing it checks changes on its own —
an external endpoint, here. Over purely deterministic tasks a stateless loop would
report the same thing on every iteration, so it would either stop on the first one
or run all the way to its bound; there is nothing to move it in between. Carrying
that kind of progress yourself — a cursor, a counter, an accumulator — is exactly
what `as:`/`init:`/`update:` are for (see `examples/loop-accumulate`).

`workflow.test.yaml` drives both endings without a network, by stubbing the probe:

- **ready** — the check reports ready, so the loop stops after one iteration.
- **never ready** — the check never reports ready, so the loop runs its whole
  budget of five and then **fails distinctly**, saying it exhausted its budget
  rather than returning as though the endpoint had come up. That distinct failure
  is the whole point of the bound.

For the stateful counterpart — a loop that carries a value between iterations — see
`examples/loop-accumulate` (a running total) and
`examples/plugins/git/log-paginate` (a pagination cursor).
