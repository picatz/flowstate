# A checkout that cannot keep money for an order it never shipped

An order reserves stock, charges a card, and hands the package to a carrier. If the
carrier step fails — no capacity, an address it will not deliver to, whatever the
reason — the business must not be left holding a charge for goods that were never
sent and a stock hold nobody will ever release. That reversal has to happen every
time, not only when someone remembers to write the refund by hand after a support
ticket shows up.

## The durability property

This is the same mechanism `examples/saga-provisioning` shows over infrastructure,
here over the transaction a customer actually experiences. Each step that has a
`undo:` registers its compensation the moment it succeeds — not written down for
someone to run later, already in effect — so a failure on the very next step, or the
one after, unwinds exactly what had already happened, in the reverse of the order it
happened in, and does it whether the failure comes from a real error, from
`flow cancel`, or (in this rehearsal) from an input that says to simulate one. A
process that crashes after charging the card and before booking the carrier would
need its own retry logic to even notice the mismatch; here the compensation is
already registered by the time that crash could happen.

## Two commands

The ordinary path — everything succeeds, and the run reports what it did:

```console
$ flow run local examples/order-fulfillment/workflow.yaml
```

The path this example exists for — the carrier cannot take the order, so the last
two steps' effects come back:

```console
$ flow run local examples/order-fulfillment/workflow.yaml --input carrier_outage=true
```

Both are the same file. Run durably, a failure partway through this saga unwinds the
same way, from whichever worker happens to be running it when `arrange_shipment`
fails — nothing about the compensation depends on it being the same worker that ran
`reserve_inventory` in the first place.

## The interesting lines

- **`undo:` on `reserve_inventory` and `charge_payment`, none on `arrange_shipment`.**
  A step's compensation exists to take back *that step's own* effect, registered on
  success; the last step with an effect worth reversing does not need one, because
  there is nothing after it that depends on undoing something *else's* mistake.
- **`${steps.reserve_inventory.id}` inside `reserve_inventory`'s own `undo:`.**
  Everywhere else in the file that would be a forward reference and `flow validate`
  would refuse it; inside a step's own `undo:` it is the ordinary case, because by
  the time the compensation runs the step has already finished and the value is
  settled.
- **The charge is refunded before the reservation is released.** Steps depend
  forwards — the charge only happened because the reservation already existed — so
  undoing runs in reverse: `charge_payment`'s compensation first, then
  `reserve_inventory`'s.
- **`carrier_outage` decides through `expect:`, not through the URL.** `arrange_shipment`'s
  `url:` stays a literal `https://httpbin.org/status/200` regardless of the input, and
  the workflow's own `expect: ${!inputs.carrier_outage && response.status_code == 200}`
  is what fails the step. A URL built from an input would have to be an expression,
  and this portfolio's networked examples are pointed at a stand-in host during
  testing by rewriting a literal — an expression cannot be rewritten that way, so it
  would either be untestable here or would reach the real internet from inside a
  test run.
- **The run still reports FAILED.** Compensation changes what the world looks like
  afterward, not what the run answers — a saga that cleaned up after itself has
  still not done what it was asked to do, and what it cleaned up is named in the
  failure text rather than hidden by a success.
