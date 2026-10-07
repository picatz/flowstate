# flowstate-plugin-openai

`openai.decide` puts a set of typed questions to an OpenAI model about some
evidence through the Decisions API (public beta) and returns one validated,
provider-neutral answer per question. It is the OpenAI half of the decision
contract in
[`proto/flowstate/decision/v1/decision.proto`](../../proto/flowstate/decision/v1/decision.proto)
(#2376): the questions and answers are `flowstate.decision.v1.QuestionSet` and
`flowstate.decision.v1.Answer`, so a Flowfile that routes on an answer reads the same
under any provider plugin. It adds no Flowfile keyword; a decision is an
ordinary plugin task whose output an `if:` or `switch:` reads.

## Contract

Inputs are `api_key`, `model`, `evidence` and `question_set`; the output is
`answers`, a list of `flowstate.decision.v1.Answer` in the question set's order.

- `question_set` is a `flowstate.decision.v1.QuestionSet` written as a mapping: each
  question has a `name`, optional `instructions`, and one of `predicate: {}`,
  `choice: {options: [...]}` or `score: {levels: [...]}` (levels lowest to
  highest). It is validated against the schema before any request is made.
- `evidence` is text, up to 256 KiB, sent as the request's `input`. The plugin
  sends no images.
- `model` is passed through unchanged and never defaulted: which models support
  decisions is the API's to say, not this plugin's.
- There is no output-size knob. The API's reply is at most one answer per
  question with one probability per option, and the plugin bounds what it reads
  instead.

## How a question set becomes a request

The Decisions API is the neutral contract's native shape, so the mapping is a
rename and nothing else: one `POST /v1/decisions` whose body is `model`, the
evidence as `input`, and `questions` in the author's order. A predicate is
`{type: predicate, name, instructions}`; a choice adds
`choices: [{value, description}]`; a score adds `levels: [{label, description}]`.

The neutral question has no per-option description, so every `description` is
sent empty. The API accepts richer options, and that gap is a follow-up to
#2376, not something this plugin invents.

## Calibration

The API returns model-derived probabilities, so every answer carries
`CALIBRATION_MODEL_PROBABILITY`:

| API answer | Neutral answer |
| --- | --- |
| `predicate` with `probability` p | the value is `p >= 0.5`; `distribution` is `{"true": p, "false": 1-p}`; no `confidence` |
| `choice` with `choice`, `confidence`, `probabilities` | the selected option; the API's `confidence`; `distribution` keyed by option |
| `score` with `score`, `confidence`, `probabilities` | the level with the highest probability (the lowest on a tie); the API's `confidence`; `distribution` keyed by level label |

The API's `score` is a probability-weighted average over the levels, which is a
number and not a level, and the neutral answer selects a level, so the average
is not carried. Each level's `value` must be an integer one more than the level
before it in the order offered, and the average must lie between the first and
last of those values; it is then dropped.
A Flowfile that needs it can derive an expected position from `distribution`
and the question's level order.

The output encoding spells an enum as its number (`1` is
`CALIBRATION_MODEL_PROBABILITY`, `3` is `CALIBRATION_NONE`) and an absent
`confidence` as `0`, so test `calibration` before trusting a number; the example
does.

## Failure handling

Every provider answer is validated before it is returned, and the whole task
fails with a typed `Failed` error, with no partial result, on any of:

- a `refusal` answer for any question;
- a question with no answer, an answer for a question that was not asked, or two
  answers for one;
- an answer whose `type` is not its question's kind, or that carries fields its
  kind does not have;
- a choice or score level the question did not offer;
- a score whose level values are missing, null, fractional, repeated, out of order or enormous, or whose average lies outside them;
- a probability, confidence or predicate probability outside 0 to 1;
- probabilities that name a value the question did not offer, repeat one, leave
  one out, or do not sum to 1 within 1e-3 (the decision schema's own tolerance);
- an answer that does not pass `flowstate.v1.Validate` as a `flowstate.decision.v1.Decision`
  with its question;
- a body that is not JSON, and a body over the response limit.

Nothing is repaired: an option the API omitted because its probability was zero
is a refusal to answer, not an assumed zero. Unknown fields in the reply are
ignored, so a field the beta adds does not stop every call; the fields the
plugin reads are held to the rules above.

A decision writes nothing, but a call that reached the provider is paid for and
may still be answered, so only failures that prove it was not processed are
retried automatically:

- HTTP 429 returns `UnavailableAfter` with the `Retry-After` delay, capped at
  five minutes, and HTTP 5xx returns `Unavailable`, because a response arrived.
  A 429 for `insufficient_quota` is `PermissionDenied`: no wait fixes it.
- A failure before any byte of the request was written (dial, DNS, TLS) returns
  `Unavailable`. A reset or a timeout after the request was written, or a
  response that could not be read, returns `OutcomeUnknown` and is not retried
  automatically.
- HTTP 401 and 403 are `PermissionDenied`; 400, 404, 413 and 422 are
  `InvalidInput`; anything else is `Failed`.
- There are no hidden retries inside the plugin. The step's `retry:` policy is
  the one retry mechanism.

## Security boundary

`api_key` is both `secret_inputs` and `required_secret_inputs`. The host must
receive `${secret('provider:name')}`, resolve it under the run namespace on the
worker, and scrub it from plugin errors and outputs; a literal is refused before
plugin invocation. The plugin sends it only as the `Authorization` bearer token,
caps it at 4 KiB, refuses one containing a line break, and never repeats it: an
error reports the provider's error type and status code, not its message, which
can quote the request. No text from a reply is repeated in an error except a
question name this request itself sent.

Credential release is not destination authorization. The host forwards the
operator's `--egress-policy` (or the built-in HTTP task's default when none was
configured) as an immutable launch-time snapshot, and every request goes through
the governed client built from it, so DNS, address, port, redirect, TLS and
credential rules apply on the actual dial path. The default grant is accepted,
because it permits public HTTPS and requiring a policy file to get back what the
worker already does would be a worse install; a deployment that wants
`api.openai.com` narrowed or stopped writes one. A grant that cannot be read
fails closed. Responses are read under this plugin's own 256 KiB ceiling and the
operator's `max_response_bytes`, whichever is lower, and one call is bounded to
two minutes end to end.

The evidence is sent to OpenAI. Zero Data Retention, HIPAA eligibility and
similar terms are properties of the OpenAI account behind the key, not something
this plugin sets or can verify; an operator who needs them arranges them with
the account that owns the key.

Unlike `slack.post`, a decision writes nothing, so it runs under `flow run
local` as well as in production; it still spends real credentials and tokens.

## Build and use

From the repository root:

```console
$ go -C plugins/openai build -o ../../bin/flowstate-plugin-openai .
$ flow plugins --plugin-dir ./bin
$ flow validate --plugin-dir ./bin examples/plugins/openai/triage.yaml
$ flow worker --plugin-dir ./bin --plugin openai \
  --temporal-deployment-name flowstate --build-id "$(git rev-parse --short HEAD)" \
  --egress-policy examples/plugins/openai/egress-policy.yaml \
  --task-policy /path/to/task-policy.yaml \
  --secret-env OPENAI_API_KEY --auth-policy /path/to/auth-policy.yaml
```

`--secret-env OPENAI_API_KEY` turns on the `env:` secret provider for the
example's `${secret('env:OPENAI_API_KEY')}`, read from the worker's own
`FLOWSTATE_SECRET_OPENAI_API_KEY`, and a worker holding a secret provider
refuses to start without an `--auth-policy` whose `secrets:` section decides
which workloads may read it. The task policy must admit `openai.decide`. See
[`examples/plugins/openai`](../../examples/plugins/openai) for a ticket triage
that routes on an answer's probability.
