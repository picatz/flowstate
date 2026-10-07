# flowstate-plugin-webhook

`webhook.send` signs one body and POSTs it once to a receiver. It is the sending
half of the `verify:` block a Flowstate webhook trigger declares: it speaks the
same schemes (`hmac_sha256`, the default, plus `github`, `slack`, `shopify`, `linear` and `stripe`), and computes the
signature with the engine's own signer (`SignWebhookDelivery` in
`pkg/flowstate/v1/webhookverify.go`), so a delivery from this task verifies at a
Flowstate receiver holding the same key. One table of schemes serves both
directions, and a repository test signs and verifies every scheme the engine
declares, so a scheme added to one side without the other fails CI.

## Contract

Inputs are `url`, `body`, `signing_key`, and optional `scheme`,
`idempotency_key` and `headers`; outputs are the receiver's `status`, a bounded
`response`, and `response_truncated`.

- `body` is the text that is signed and sent, byte for byte. The task never
  re-encodes it, because a receiver verifies the bytes on the wire; build JSON
  with `json.encode(...)`, which renders compact text with sorted keys. It is
  sent as `application/json` unless `headers` sets a Content-Type. At most 1 MiB.
- `signing_key` is both `secret_inputs` and `required_secret_inputs`: it must be
  a whole `${secret('provider:name')}`, resolved by the host worker-side, and a
  literal is refused before it can enter durable history. The key is only the
  HMAC key. It is never sent, returned, or put in an error, and a receiver that
  echoes the key or the signature in its response gets `[redacted]` in the
  `response` output instead.
- `scheme: hmac_sha256` sends `X-Flowstate-Signature: <hex HMAC-SHA256 of the
  body>`. `github`, `shopify` and `linear` send the provider's own header over the same body; `scheme: slack` sends `X-Slack-Signature: v0=<hex>` and `X-Slack-Request-Timestamp`, signed over `v0:<t>:<body>`. `scheme: stripe` sends `Stripe-Signature: t=<unix seconds>,v1=<hex>`,
  signed over `<t>.<body>`; a Flowstate receiver enforces a five-minute replay
  window on it. An unknown scheme is refused.
- `idempotency_key` is sent as `Idempotency-Key`. It is not covered by either
  signature, so a receiver that must dedupe should key on something in the signed
  body (the example's receiver does). Reuse it unchanged across retries of one
  logical delivery.
- `headers` is at most 16 headers of at most 1 KiB each. `Authorization`,
  `Proxy-Authorization`, `Cookie`, the signature headers, `Idempotency-Key`, and
  framing headers are refused: a value here is recorded in durable history. A URL
  carrying userinfo is refused for the same reason.

## Outcomes

There are no hidden retries; the workflow's retry policy is the one mechanism.

- A 2xx answer succeeds. Any other status, including a 3xx (redirects are not
  followed, so a signed delivery goes only to the URL the author named), fails
  the step permanently, and the error carries the status and nothing the
  receiver said.
- A 429 and an operator rate-bucket refusal before the request is sent are
  definite no-delivery outcomes and return a retryable error with the delay
  capped at five minutes.
- A lost response, a timeout, a 5xx, or an unreadable response after the request
  was written is an unknown outcome and is not retried automatically.
- The response is read under the operator policy's `max_response_bytes` and this
  plugin's 64 KiB output cap, and one request is bounded to 30 seconds. A receiver
  that accepted the delivery with an oversized response still succeeds, with
  `response_truncated: true`.

## Security boundary

Send to an `https://` receiver. An `http://` one is accepted, as `slack` accepts it
where the operator's egress policy allows it, but the body, the signature and the
idempotency key then cross the network in the clear, and for `hmac_sha256`, which
signs the body alone, a signature seen on the wire is replayable.

The deployment's egress policy decides where a delivery may go, taken as `slack`
takes it: the deployment default (public HTTPS, internal ranges and loopback
denied) is accepted, an operator `--egress-policy` narrows it, and a grant that
cannot be read fails closed before inputs are decoded. Every delivery is marked
as carrying a credential, because a signature is what the receiver
authenticates by, so a `credentials && ...` rule sees it. Task policy and secret
release are separate controls the deployment also owns. The plugin manifest is
a declaration, not authority.

Unlike `slack.post`, this task does not require the production execution mode:
a rehearsal that is allowed to reach a receiver by operator policy may send.

## Build and use

```console
$ go -C plugins/webhook build -o ../../bin/flowstate-plugin-webhook .
$ flow plugins --plugin-dir ./bin
$ flow validate --plugin-dir ./bin examples/plugins/webhook/sender.yaml
$ flow worker --plugin-dir ./bin --plugin webhook \
  --egress-policy examples/plugins/webhook/egress-policy.yaml \
  --secret-env PEER_WEBHOOK_KEY --auth-policy /path/to/auth-policy.yaml
```

See [`examples/plugins/webhook`](../../examples/plugins/webhook) for the sender
and the receiver it delivers to.
