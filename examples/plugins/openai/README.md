# Routing on a typed decision

[`triage.yaml`](triage.yaml) asks an OpenAI model two questions about a support
ticket with `openai.decide` (one choice, one yes/no) and then routes on the
answer with an ordinary `if:`. There is no `decide:` or `judge:` keyword: the
answer is a typed step output like any other.

What it is about is the gate. The Decisions API returns probabilities, so the
file pages on-call only when the probability of `true` in the `urgent`
answer's `distribution` is at least `page_threshold`, a number a person chose in
the file, and only when `calibration == CALIBRATION_MODEL_PROBABILITY`. It reads the distribution rather than the
boolean because the boolean is only "at least one half". The team is taken from
the `category` answer only when its confidence reaches `team_threshold`;
otherwise the ticket is `unassigned`. An answer with no probability, from any
provider, therefore queues the ticket for a person instead of paging on a
number that means nothing.

`flow test examples/plugins/openai/` runs the three cases with no plugin
process, no model and no network: a probably urgent ticket pages, an urgent one
below the threshold is queued, and an answer with no probability is queued
however it is phrased.

Running it for real needs a built plugin, a worker, and a key:

- `OPENAI_API_KEY` must be admitted by the configured `env:` secret backend
  (`--secret-env OPENAI_API_KEY` on the worker, which reads
  `FLOWSTATE_SECRET_OPENAI_API_KEY`), with an `--auth-policy` whose
  `secrets:` section allows it. `api_key` is the plugin's credential, bound once under
  `plugins:` as a whole secret reference (a step may write its own to override
  it), and a literal is rejected.
- `model` must name a model the Decisions API accepts; the plugin has no
  default.
- [`egress-policy.yaml`](egress-policy.yaml) authorizes only `api.openai.com`
  over HTTPS. A plugin declaration is not destination authority.

See [`plugins/openai`](../../../plugins/openai) for the contract, what each
failure looks like, and how a question set maps onto the request.
