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
- **A payload is whatever the sender wrote.** Fields are read with `.?` and default
  to `0`, so a bid with no price is dropped rather than crashing the award.
- **Too few bids is an outcome.** Below `min_bids` valid bids the run logs why and
  awards nothing; silence reads the same way.
- **Ties are broken in the expression**, cheapest first and then shorter lead time.

## Try it

```console
$ flow test examples/vendor-bids
```

A rehearsal with `flow run local` needs bids delivered with `--signal`; the tests
script them with a `sender:` each, which is the easier way to see every path.

Compare [signal-batch-drain](../signal-batch-drain) for the draining rules and
[refund-request](../refund-request) for a single decider rather than many bidders.
