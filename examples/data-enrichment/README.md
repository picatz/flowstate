# Fanning out over a worklist without losing the ones that fail

A batch of records needs to be looked up against a slow, occasionally flaky
enrichment service — too many to do one at a time, too many to hit all at once
either. Some calls fail for reasons worth retrying (the service was briefly
overloaded) and at least one fails for a reason no retry fixes (the record itself is
bad); whatever runs this needs to say, afterward, exactly which records are which,
and treat the two failures differently rather than lumping them into one "it
failed" bucket.

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

And the same file run durably instead of in this process (needs a Temporal dev
server, `flow worker`, and `flow server` — see the main README's Quickstart):

```console
$ flow run examples/data-enrichment/workflow.yaml -o json | jq .runOutputs
```

Both process the same worklist with the same bound on concurrency and answer with
the same shape; the only thing that changes is that a crash between two records is
now something the run recovers from instead of something a caller has to notice and
restart by hand.

## The interesting lines

- **`max_parallel: 2` on a four-record list.** Local execution still runs iterations
  one after another regardless of this number — see `fan-out-and-parallel`'s
  README-adjacent note in the main README — so the two drivers process the same
  requests in the same order either way; only the durable driver's concurrency is
  actually bounded by it.
- **"flagged" fails once, on purpose, and `retry` never sees it.** `expect` rejects
  it the instant a response comes back — `record != 'flagged' && ...` — and that
  kind of failure is permanent: the request was never going to be accepted, so
  trying it again would answer the same way every time. `retry` is declared on
  `lookup` because *other* failures on this same call are worth another attempt;
  "flagged" is here to show what `retry` correctly does nothing for, not what it
  fixes.
- **`vendor_status` is where `retry` is actually exercised.** A 503 is a status the
  http task classifies as transient, so this step's three attempts are real —
  proven by timing it: pin the interval to something obvious and the run visibly
  takes that much longer, or read a durable run's activity log, which reports
  `Attempt 1`, `Attempt 2`, `Attempt 3` before the step is tolerated. "flagged" and
  `vendor_status` are the same policy over two different failure shapes, and only
  one of them is a shape `retry:` can do anything about.
- **`vendor_status` needed an address of its own.** It is not one more record in the
  worklist above because every request `lookup` makes shares one literal URL — the
  only kind an example's networked tests can safely repoint elsewhere — and a
  request that must answer 503 regardless of which record asked for it cannot share
  that URL with the ones that must answer 200.
- **`expect` deciding "flagged" instead of the URL.** Every `lookup` request goes to
  the same literal `https://httpbin.org/get`; what makes "flagged" fail is
  `record != 'flagged'` inside `expect`, evaluated per iteration. A URL built from
  the record — `${'.../record/' + record}` — would need to be an expression, and an
  expression is not something a test harness can safely repoint, which is exactly
  why none of the http examples in this portfolio build a URL that way.
- **`failed` recovers identity by subtraction.** A failed iteration's own result
  carries no identifier — the step never reached its `outputs:` expression, because
  `expect` decided before that point — so there is nothing to read back off it
  directly. What there is instead is `inputs.records`, the list this run started
  with, and `enriched`, what actually finished; `failed` is the first with the
  second's members filtered back out, using CEL's `in` on a list built from the same
  `filter`+`map` that produced `enriched` a few lines above.
