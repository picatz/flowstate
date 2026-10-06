# refund-request

A customer asks for a refund. Small ones are paid straight away. Larger ones wait
for someone in finance, who must not be the person who started the run, and a
payout that has to be taken back is.

It is the example to read after [approval-gate](../approval-gate) and
[order-fulfillment](../order-fulfillment): the same gate and undo, in a business
process, with a typed input.

## What to notice

- **A typed request.** `Refund` and `Line` are declared once under `types:`. A bad
  `reason`, an empty `lines`, or a field the type does not name is refused before
  the first step runs, with the path of what is wrong.
- **Policy is the file's, not the caller's.** `signals:` says who may decide
  (`team: finance`) and a sender-differs-from-starter clause keeps a requester
  from deciding their own refund. Neither is an input.
- **No answer is not a yes.** The gate's `outcome` is `approved`, `declined` or
  `undecided`; only `approved` pays, and a lapsed 72 hours reads `undecided`.
- **An optional field, read safely.** `inputs.refund.?note` is used only when
  present.
- **An idempotent payout with an undo.** The payout carries a key derived from the
  order, so a retry converges, and `undo:` voids it if a later step fails.

## Try it

```console
$ flow test examples/refund-request
$ flow run local examples/refund-request/workflow.yaml \
    --input 'refund={"order_id":"o-1","reason":"late","lines":[{"sku":"kb-01","cents":1500}]}'
```

The local run makes a real request to `payments.example.com`, which does not
exist; use `flow test` to see every path without one. Approving over a durable
run uses `flow signal` as a finance user; see
[approval-gate](../approval-gate/README.md) for the walkthrough.
