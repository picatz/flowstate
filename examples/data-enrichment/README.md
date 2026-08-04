# Fanning out over a worklist without losing the ones that fail

A batch of records needs to be looked up against a slow, occasionally flaky
enrichment service — too many to do one at a time, too many to hit all at once
either. Some will fail for reasons worth retrying (the service was briefly
overloaded) and at least one will fail for a reason no retry fixes (the record itself
is bad); whatever runs this needs to say, afterward, exactly which records are which.

## The durability property

`max_parallel` bounds concurrency the way a worker pool would, and `retry` gives a
record a second chance before this run gives up on it — both are ordinary code in a
script too. What is not ordinary in a script is surviving a crash partway through:
a `for` loop's position, which records already succeeded, and which attempt a retry
is on all live in variables that vanish with the process. Here that state belongs to
the execution substrate rather than to any one worker, so a crash mid-batch resumes
on whatever worker replaces the one that died, at the record it was on, on the
attempt it was on — nothing repeats and nothing is skipped.

## Two commands

```console
$ flow run local examples/data-enrichment/workflow.yaml -o json | jq .runOutputs
```

Run durably against Temporal, the same file processes the same worklist with the
same bound on concurrency; the only thing that changes is that a crash between two
records is now something the run recovers from instead of something a caller has to
notice and restart by hand.

## The interesting lines

- **`max_parallel: 2` on a four-record list.** Local execution still runs iterations
  one after another regardless of this number — see `fan-out-and-parallel`'s
  README-adjacent note in the main README — so the two drivers process the same
  requests in the same order either way; only the durable driver's concurrency is
  actually bounded by it.
- **`retry` on `lookup`, and why "flagged" is not what it rescues.** The policy is
  for the ordinary transient case — an upstream that was briefly unavailable — not
  for the record that fails on principle. `expect` decides "flagged" is unacceptable
  the same instant every attempt returns, so retrying it three times before giving
  up costs nothing beyond the interval and proves the policy is not selectively
  applied.
- **`expect` deciding failure instead of the URL.** Every request goes to the same
  literal `https://httpbin.org/get`; what makes "flagged" fail is
  `record != 'flagged'` inside `expect`, evaluated per iteration. A URL built from
  the record — `${'.../record/' + record}` — would need to be an expression, and an
  expression is not something the request can be pointed anywhere else by after the
  fact, which is exactly why none of the http examples in this portfolio build a URL
  that way.
- **`failed` recovers identity by subtraction.** A failed iteration's own result
  carries no identifier — the step never reached its `outputs:` expression, because
  `expect` decided before that point — so there is nothing to read back off it
  directly. What there is instead is `inputs.records`, the list this run started
  with, and `enriched`, what actually finished; `failed` is the first with the
  second's members filtered back out, using CEL's `in` on a list built from the same
  `filter`+`map` that produced `enriched` a few lines above.
