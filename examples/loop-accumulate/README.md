# loop-accumulate

A bounded `loop:` that carries state between iterations, with no plugin dependency.

`for_each` maps a body over a list it already holds. `loop:` is the other shape: it
repeats a body, threads a value from one iteration into the next, and decides for
itself when it is done — the shape a cursor-paged API or a converging computation
has, where the length is not known before you start.

```
flow run local examples/loop-accumulate/workflow.yaml
flow run local examples/loop-accumulate/workflow.yaml --input target=8
flow test examples/loop-accumulate/
```

## What it shows

- **Carried state** — `as: acc` names the value the body reads bare, `init:` is what
  it holds first, `update:` computes the next value from the current one. Here `acc`
  is a small map (`{n, sum}`), which is how one carried value holds several fields
  without a block of named accumulators.
- **Do-while** — the body runs, then `until:` is checked, so `until:` reads what the
  body (and `update:`) produced. A loop that carries nothing but drives `until:` from
  its own state, like this one, needs no task that returns a value.
- **A mandatory bound** — `max_iterations:` is the ceiling. A loop that could run
  forever is one whose runaway the engine must be able to stop, so hitting the
  ceiling is a *distinct* failure, not a silent stop. The second test case
  (`target: 200`, past the loop's `max_iterations: 100`) proves it fails saying so.
- **Loop outputs** — a loop reports its body's per-iteration outputs as `results`,
  and, when it carries state, its final value as `state`. Both are read here in the
  workflow's declared `outputs:`.

For the same primitive driving a real plugin's pagination cursor to exhaustion, see
`examples/plugins/git/log-paginate.yaml`.
