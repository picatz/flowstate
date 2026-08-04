# A health check that survives one of its targets being down

Somebody has to probe checkout, payments, and search on a schedule, and one of them
being down should not stop the other two from being checked, or from being reported.
The output an on-call human or a pager integration needs is not "the check crashed" —
it is which services are unhealthy right now.

## The durability property

This one is less about a long wait and more about a scheduled run finishing honestly.
A shell script probing three services in a loop with `set -e` dies on the first
failure and reports nothing about the other two; taking `set -e` out fixes that at
the cost of nobody being able to tell "one thing failed" from "the script itself
broke" from the log. Here, every probe runs — `continue_on_error: true` says a
failed probe is data, not a reason to stop — and the run finishes and reports a
result either way. Pointed at with `triggers:` (see `examples/scheduled-report`), a
firing that starts before a worker fleet redeploys finishes on whatever worker
replaces it, with the same three answers it would have produced without the
redeploy.

## Two commands

```console
$ flow run local examples/ops-healthcheck/workflow.yaml -o json | jq .runOutputs
```

Nothing else changes to run this durably: the same file, submitted to a worker
instead of executed in this process, checks the same three services and answers with
the same shape — a schedule created with `flow schedule create` is what turns "run
this once" into "run this every five minutes forever," not a rewrite of the file.

## The interesting lines

- **Three branches under one `parallel:`, each with its own `continue_on_error`.**
  Branches merge their outputs into the enclosing scope under their own step id once
  the block completes, which is what makes `steps.checkout`, `steps.payments`, and
  `steps.search` each addressable afterward by name rather than needing to be found
  inside a loop's anonymous `results`.
- **`search` is shipped pointed at a permanent 503.** An example whose every probe
  passes demonstrates nothing about tolerance; this one always has exactly one
  service down, the same way `conditional-and-retry`'s `notify` step always fails on
  purpose, so the interesting path runs every time rather than only sometimes.
- **`has(steps.<id>.error)` is the whole classification.** A probe that failed
  carries `error` and no `status_code`; one that succeeded carries the reverse. There
  is no separate `healthy:` flag to compute per probe — the presence of the field
  `continue_on_error` attaches on failure is the signal, read three times in the
  `outputs:` block below.
- **`unhealthy_services`, not just a count.** A pager wants to know *what* to page
  about. Built as three conditional single-element lists concatenated together
  because outputs cannot reference each other — each of `unhealthy_services`,
  `healthy_count`, and `page_required` re-derives the same three `has()` checks
  independently, which is a little repetitive and exactly what the schema allows.
