# The quarterly access review, as a durable workload

An access review is the workload this engine is shaped for and most tools are
not: a bounded read, a wait measured in days, and a write that must reflect what
the reviewer actually looked at. Losing a worker in the middle of it should
change nothing.

Run it:

```console
$ mkdir -p ./plugins
$ go build -o ./plugins/flowstate-plugin-scim ./plugins/scim
$ export FLOWSTATE_SECRET_SCIM_TOKEN=...   # the worker's own env: provider
$ flow worker --plugin-dir ./plugins --egress-policy examples/plugins/scim/egress-policy.yaml
$ flow run examples/plugins/scim/workflow.yaml \
    --input directory=https://example.okta.com/scim/v2 \
    --input user_id=2819c223-7f76-453a-919d-413861904646 \
    --input expected_approver=compliance-lead@example.com
```

The run stops at `decision` and waits - durably, for up to a week - until a
compliance reviewer answers:

```console
$ flow signal <run> review-decided --payload keep=false
```

## What each step is protecting

- **`in_scope`** is bounded by construction. A directory with fifty thousand
  accounts pages through `next_start_index`; a `count` over the task's ceiling
  is refused rather than quietly lowered, so nobody discovers at audit time that
  a review covered the first five hundred accounts.

- **`under_review`** is read for two things: the evidence a person judges, and
  the `version` - the provider's ETag - that makes the write below conditional.

- **`decision`** is bounded by `signals:`. Only a subject holding a
  `compliance-reviewer` claim can answer, and `distinct_from_starter: true`
  means whoever *requested* the review cannot approve it. That constraint is
  enforced by the server, not by this file's good intentions.

- **`revoke`** carries `expected_version`. If the user changed between the
  evidence and the approval - a transfer, a rename, another review - the step
  fails as a conflict rather than acting on stale evidence. And it runs only on
  an explicit refusal: **a timeout changes nothing**, because a review nobody
  answered is an unanswered review, not a decision to revoke.

## Testing it without a directory

[`workflow.test.yaml`](workflow.test.yaml) stubs all three plugin steps, so both
paths - the refusal that writes, and the timeout that does not - are exercised
with no network and no plugin process:

```console
$ flow test examples/plugins/scim/
```

## The egress policy

[`egress-policy.yaml`](egress-policy.yaml) is the operator's half: the plugin
has no allowlist of its own, so this file is what decides which directory a
worker may read and write.
