# vendor-bids

Ask several vendors for a price, wait out a deadline, and award the cheapest valid
bid if enough vendors answered. It is the multi-party shape: one run, many senders,
each attested separately, none of them the person who started it.

## What to notice

- **Fan-in over signals.** Each vendor sends a `bid` signal whenever they like. The
  run sleeps out the bidding window, then drains everything that arrived with one
  `wait_for_signals:`. Draining first would take only the earliest bidders, because
  that step returns on the first delivery and collects what is already buffered.
- **Identity is attested, not claimed.** The `signals:` rule admits only
  `role: vendor`, and a bid's vendor is `d.sender.identity.subject`, never a name in
  the payload.
- **A payload is whatever the sender wrote.** Each field is checked for its type as
  well as its presence, so a bid with a missing or wrongly typed price, or a negative
  lead time, is dropped rather than crashing the award.
- **Late is judged by when the server accepted a bid**, `sender.accepted_at`, against a
  cutoff fixed when the run began, not by when the run got round to reading it. A
  rehearsal leaves `accepted_at` unset on purpose, so `flow test` cannot show a late
  bid; the durable driver can.
- **A quorum counts vendors, not bids.** One vendor bidding twice is still one vendor
  (an identity is issuer plus subject), and its cheapest bid is the one that can win.
- **Cheapest first, lead time only to break a tie**, written as two small steps rather
  than a weighted sum, which a very long lead time could game.
- **Too few vendors is an outcome.** Below `min_bids` the run logs why and awards
  nothing; silence reads the same way.
- **At most 50 bids are read**, the first 50 accepted, which is why `min_bids` stops
  at 50. A larger tender would drain again in a `loop:`.

## Try it

```console
$ flow test examples/vendor-bids
```

A rehearsal with `flow run local` needs bids delivered with `--signal`; the tests
script them with a `sender:` each, which is the easier way to see every path.

Compare [signal-batch-drain](../signal-batch-drain) for the draining rules and
[refund-request](../refund-request) for a single decider rather than many bidders.
