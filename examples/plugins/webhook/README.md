# One Flowstate telling another, with a signed webhook

Two deployments, one shared key, and the two halves of a webhook:

- [`sender.yaml`](sender.yaml) is deployment A. `webhook.send` signs the body it
  built with `json.encode` and POSTs it to B's webhook route.
- [`receiver.yaml`](receiver.yaml) is deployment B. An ordinary `webhook:`
  trigger with `verify: hmac_sha256` starts a fulfilment run from a delivery that
  verifies, and nothing from one that does not.

Both name the same scheme (`hmac_sha256`; `stripe` is the other) and the same
secret, `env:PEER_WEBHOOK_KEY`. The key is a whole secret reference on each side:
the sender's worker resolves it inside the plugin at the point of use, it is never
a value in an expression, and neither the step's recorded input nor its output
carries it. The signature is computed by the engine's own function, the one the
receiver checks against, so the two cannot disagree about what a scheme is.

What is signed is exactly what is sent. That is why `body:` is text built in the
step: a receiver verifies the bytes on the wire, and a task that re-encoded a
structure could produce bytes the author never saw. The receiver's dedupe key is
read from the signed body (`id`), because the `Idempotency-Key` header the sender
also sets is not covered by an `hmac_sha256` signature.

## What the tests prove

`flow test examples/plugins/webhook/` runs both files with no plugin process and
no network:

- [`receiver.test.yaml`](receiver.test.yaml) replays
  [`testdata/order-paid.json`](testdata/order-paid.json), whose body is the exact
  text `sender.yaml` renders for order `ord_H1x9` and whose signature is the HMAC
  of those bytes under the fixture key. The receiver's own arithmetic accepts it.
  The same signature over an edited body, and the same delivery under another
  key, start nothing.
- [`sender.test.yaml`](sender.test.yaml) stubs `webhook.send` and checks the
  workflow around it.

The signing itself is proved where the plugin lives: `plugins/webhook` signs with
the task and verifies with the engine for every scheme, refuses a tampered body,
timestamp or key, and runs the real plugin binary end to end.

## Running it

```console
$ go -C plugins/webhook build -o ../../bin/flowstate-plugin-webhook .
$ flow validate --plugin-dir ./bin examples/plugins/webhook/sender.yaml
$ flow worker --plugin-dir ./bin --plugin webhook \
    --egress-policy examples/plugins/webhook/egress-policy.yaml \
    --secret-env PEER_WEBHOOK_KEY --auth-policy /path/to/auth-policy.yaml
```

On deployment B, serve the trigger with
`flow server --webhook examples/plugins/webhook/receiver.yaml` and the same
`PEER_WEBHOOK_KEY`. [`egress-policy.yaml`](egress-policy.yaml) is deployment A's
operator file: it allows the one receiver and keeps a credentialed request from
going anywhere else. Replace `flowstate.peer.example.com` with B's host. The
sender allows no loopback, so rehearse locally by adding `allow_loopback: true`
to a policy of your own, not by widening this one.

[`examples/federation-flow-to-flow`](../../federation-flow-to-flow) is the other
way for one Flowstate to call another, over the RPC with an exchanged identity;
this is the one for a receiver that is, or wants to be, a webhook.
