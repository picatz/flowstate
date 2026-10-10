# Routing on a typed decision

[`triage.yaml`](triage.yaml) asks a Claude model two questions about a support
ticket with `anthropic.decide` (one choice, one yes/no) and then routes on the
answer with an ordinary `if:`. There is no `decide:` or `judge:` keyword: the
answer is a typed step output like any other.

What it is about is the gate. A model's stated confidence is its own claim, not
a probability, so the file pages on-call only when the model answered `urgent`,
said how sure it was (`calibration == CALIBRATION_SELF_REPORTED`), and was at least as
sure as `page_threshold`, a number a person chose in the file. An answer that
carries no confidence, or one below the threshold, goes to a person. Swapping in
a provider that reports none therefore queues every ticket for review instead of
paging on a number that means nothing.

`flow test --plugin-catalog examples/plugins/plugins.lock.json examples/plugins/anthropic/` runs the three cases with no plugin
process, no model and no network: a confident urgent ticket pages, an urgent one
below the threshold is queued, and an urgent one with no confidence is queued
however it is answered.

Running it for real needs a built plugin, a worker, and a key:

- `ANTHROPIC_API_KEY` must be admitted by the configured `env:` secret backend
  (`--secret-env ANTHROPIC_API_KEY` on the worker, which reads
  `FLOWSTATE_SECRET_ANTHROPIC_API_KEY`), with an `--auth-policy` whose
  `secrets:` section allows it. `api_key` is the plugin's credential, bound once under
  `plugins:` as a whole secret reference (a step may write its own to override
  it), and a literal is rejected.
- [`egress-policy.yaml`](egress-policy.yaml) authorizes only
  `api.anthropic.com` over HTTPS. A plugin declaration is not destination
  authority.

See [`plugins/anthropic`](../../../plugins/anthropic) for the contract, what
each failure looks like, and how a question set maps onto the request.
