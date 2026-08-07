# A health check that survives one of its targets being down

Somebody has to probe a list of services on a schedule, and one of them being down
should not stop the others from being checked, or from being reported. The output an
on-call human or a pager integration needs is not "the check crashed" — it is which
services are unhealthy right now.

## The durability property

This one is less about a long wait and more about a scheduled run finishing honestly.
A shell script probing services in a loop with `set -e` dies on the first failure and
reports nothing about the rest; taking `set -e` out fixes that at the cost of nobody
being able to tell "one thing failed" from "the script itself broke" from the log.
Here, every probe runs — `continue_on_error: true` says a failed probe is data, not a
reason to stop — and the run finishes and reports a result either way. Pointed at with
`triggers:` (see `examples/scheduled-report`), a firing that starts before a worker
fleet redeploys finishes on whatever worker replaces it, with the same answers it
would have produced without the redeploy.

## Two commands

```console
$ flow run local examples/ops-healthcheck/workflow.yaml -o json | jq .runOutputs
```

Nothing else changes to run this durably: the same file, submitted to a worker
instead of executed in this process (needs a Temporal dev server, `flow worker`, and
`flow server` — see the main README's Quickstart), checks the same services and
answers with the same shape:

```console
$ flow run examples/ops-healthcheck/workflow.yaml -o json | jq .runOutputs
```

A schedule created with `flow schedule create examples/ops-healthcheck/workflow.yaml`
is what turns "run this once" into "run this every five minutes forever," not a
rewrite of the file — see `examples/scheduled-report` for the `triggers:` block that
would sit above `steps:` here to declare the cadence.

## The interesting lines

- **`for_each` over a list of services, not `parallel:` branches unrolled by hand.**
  `parallel:` is for branches that are genuinely different work — see
  `fan-out-and-parallel`'s `check_config` and `check_quota` for that shape. These
  probes are the same operation, repeated: adding a fourth service is one entry in
  `checks.for_each.items` and one entry in `vars.service_names`, not a fourth
  hand-written branch plus a fourth re-derivation in every output.
- **The list is written twice, on purpose, and the header says why.** A `for_each`'s
  `items:` has to be a literal in the file, not an expression reading `vars:`, for
  the example corpus's own test harness (`PointAtStandIn`) to find every request in
  it and point it off the real network. `vars.service_names` — just the names, no
  urls — is what `outputs:` reads, so the harness constraint costs this file a
  second list rather than a compromise on what the output reports.
- **`search` is shipped pointed at a permanent 503.** An example whose every probe
  passes demonstrates nothing about tolerance; this one always has exactly one
  service down, the same way `conditional-and-retry`'s `notify` step always fails on
  purpose, so the interesting path runs every time rather than only sometimes.
- **`continue_on_error: true` inside the loop body, and `has(r.probe.name)` as the
  classification.** A probe that failed never reaches its own `outputs:` expression
  — `expect`'s default rule rejects the response first — so a failed iteration's
  result carries no name, only whatever `continue_on_error` recorded as its error.
  `unhealthy_services` is exactly the names in `vars.service_names` that are missing
  from the names `checks` reported back.
- **`unhealthy_services`, not just a count.** A pager wants to know *what* to page
  about. `outputs:` cannot reference another output, so `unhealthy_services`,
  `healthy_count`, and `page_required` each re-derive the same filter over
  `steps.checks.results` independently, which is a little repetitive and exactly
  what the schema allows.
