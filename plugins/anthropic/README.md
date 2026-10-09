# flowstate-plugin-anthropic

`anthropic.decide` puts a set of typed questions to a Claude model about some
evidence and returns one validated, provider-neutral answer per question. It is
the Anthropic half of the decision contract in
[`proto/flowstate/decision/v1/decision.proto`](../../proto/flowstate/decision/v1/decision.proto)
(#2376): the questions and answers are `flowstate.decision.v1.QuestionSet` and
`flowstate.decision.v1.Answer`, so a Flowfile that routes on an answer reads the same
under any provider plugin. It adds no Flowfile keyword; a decision is an
ordinary plugin task whose output an `if:` or `switch:` reads.

## Contract

Inputs are `api_key`, `model`, `evidence`, `question_set`, and optional
`report_confidence` and `max_tokens`; the output is `answers`, a list of
`flowstate.decision.v1.Answer` in the question set's order.

- `question_set` is a `flowstate.decision.v1.QuestionSet` written as a mapping: each
  question has a `name`, optional `instructions`, and one of `predicate: {}`,
  `choice: {options: [...]}` or `score: {levels: [...]}` (levels lowest to
  highest). It is a typed input, so `flow validate` checks it against the
  schema's rules at the line that wrote it (a repeated option, a name that is
  not an identifier, an empty list of levels), and the plugin checks it again
  before any request is made. Question names are further limited to 64 bytes, the longest tool-schema
  property name the Messages API accepts.
- `evidence` is text, up to 256 KiB. It is framed to the model as data to be
  judged, not instructions to follow, and `&` and `<` are escaped in it so that
  untrusted text cannot close the `<evidence>` element it is wrapped in.
- `model` is passed through unchanged and never defaulted.
- `max_tokens` defaults to 1,024 and may not exceed 8,192. The reply is only a
  tool call, so this bounds it tightly.

## How a question set becomes a request

The Messages API has no decisions endpoint, so the plugin sends one request
whose only tool, `record_decisions`, takes one required property per question,
and forces it with `tool_choice`. A choice question is an enum of its options, a
predicate is a boolean, and a score is an enum of its levels in order. Nothing
is parsed out of prose: the answer is the tool call's JSON input.

## Calibration

Claude exposes no token probabilities, so the plugin cannot report a calibrated
probability and never invents one:

- With `report_confidence: true` each question also asks for a `confidence`
  between 0 and 1. An answer whose tool input carries one is
  `CALIBRATION_SELF_REPORTED`: a claim by the model about itself, not a
  measurement.
- Otherwise, including when confidence was asked for and the model left it out,
  the answer is `CALIBRATION_NONE` and carries no `confidence`.
- `distribution` is never filled in.

A gate such as `answer.calibration == CALIBRATION_MODEL_PROBABILITY &&
answer.confidence >= 0.9` therefore denies on this provider, which is the
point: swapping providers cannot silently weaken it. The output encoding spells
an enum as its number (`2` is `CALIBRATION_SELF_REPORTED`, `3` is
`CALIBRATION_NONE`) and an absent `confidence` as `0`, so test `calibration`
before trusting a confidence; the example does.

## Failure handling

Every provider answer is validated before it is returned: each answer must name
a question that was asked, be of that question's kind, select one of its options
or levels, and satisfy the `Answer` rules, all through `flowstate.v1.Validate`
over a `flowstate.decision.v1.Decision`. A missing or extra question, a value of the
wrong JSON type, an option the question did not offer, a confidence outside 0 to
1 or one nobody asked for, a reply with no `record_decisions` call or more than
one, a reply cut off at `max_tokens`, a body that is not JSON, and a body over
the response limit all fail the task with a typed `Failed` error. Nothing is
repaired and no partial result is returned.

A decision writes nothing, but a call that reached the provider is paid for and
may still be answered, so only failures that prove it was not processed are
retried automatically:

- HTTP 429 returns `UnavailableAfter` with the `Retry-After` delay, capped at
  five minutes, and HTTP 5xx (including the 529 overloaded response) returns
  `Unavailable`, because a response arrived.
- A failure before any byte of the request was written (dial, DNS, TLS) returns
  `Unavailable`. A reset or a timeout after the request was written, or a
  response that could not be read, returns `OutcomeUnknown` and is not retried
  automatically.
- HTTP 401 and 403 are `PermissionDenied`; 400, 404, 413 and 422 are
  `InvalidInput`; anything else is `Failed`.
- There are no hidden retries inside the plugin. The step's `retry:` policy is
  the one retry mechanism.

## Security boundary

`api_key` is the plugin's `api_key` credential, so a Flowfile binds it once under
`plugins:` (a step may override it), and it is both `secret_inputs` and
`required_secret_inputs`. The host must
receive `${secret('provider:name')}`, resolve it under the run namespace on the
worker, and scrub it from plugin errors and outputs; a literal is refused before
plugin invocation. The plugin sends it only in the `x-api-key` header, caps it
at 4 KiB, refuses one containing a line break, and never repeats it: an error
reports the provider's error type and status code, not its message, which can
quote the request.

Credential release is not destination authorization. The host forwards the
operator's `--egress-policy` (or the built-in HTTP task's default when none was
configured) as an immutable launch-time snapshot, and every request goes through
the governed client built from it, so DNS, address, port, redirect, TLS and
credential rules apply on the actual dial path. The default grant is accepted,
because it permits public HTTPS and requiring a policy file to get back what the
worker already does would be a worse install; a deployment that wants
`api.anthropic.com` narrowed or stopped writes one. A grant that cannot be read
fails closed. Responses are read under this plugin's own 256 KiB ceiling and the
operator's `max_response_bytes`, whichever is lower, and one call is bounded to
two minutes end to end.

Unlike `slack.post`, a decision writes nothing, so it runs under `flow run
local` as well as in production; it still spends real credentials and tokens.

## Build and use

From the repository root:

```console
$ go -C plugins/anthropic build -o ../../bin/flowstate-plugin-anthropic .
$ flow plugins --plugin-dir ./bin
$ flow validate --plugin-dir ./bin examples/plugins/anthropic/triage.yaml
$ flow worker --plugin-dir ./bin --plugin anthropic \
  --temporal-deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
  --egress-policy examples/plugins/anthropic/egress-policy.yaml \
  --task-policy /path/to/task-policy.yaml \
  --secret-env ANTHROPIC_API_KEY --auth-policy /path/to/auth-policy.yaml
```

`--secret-env ANTHROPIC_API_KEY` turns on the `env:` secret provider for the
example's `${secret('env:ANTHROPIC_API_KEY')}`, read from the worker's own
`FLOWSTATE_SECRET_ANTHROPIC_API_KEY`, and a worker holding a secret provider
refuses to start without an `--auth-policy` whose `secrets:` section decides
which workloads may read it. The task policy must admit `anthropic.decide`. See
[`examples/plugins/anthropic`](../../examples/plugins/anthropic) for a ticket
triage that routes on an answer.
