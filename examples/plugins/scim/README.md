# The quarterly access review, as a durable workload

An access review is the workload this engine is shaped for and most tools are
not: a bounded read, a wait measured in days, and a write that must reflect what
the reviewer actually looked at. Losing a worker in the middle of it should
change nothing.

Run it:

```console
$ mkdir -p ./plugins
$ go -C plugins/scim build -o ../../plugins/flowstate-plugin-scim .
$ export FLOWSTATE_SECRET_SCIM_TOKEN=...   # the worker's own env: provider
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --egress-policy examples/plugins/scim/egress-policy.yaml \
    --secret-env SCIM_TOKEN --auth-policy /path/to/auth-policy.yaml &
$ flow server --plugin-dir ./plugins --auth-policy /path/to/auth-policy.yaml \
    --rpc-resource https://flowstate.example.com/rpc &
$ flow run examples/plugins/scim/workflow.yaml \
    --input directory=https://example.okta.com/scim/v2 \
    --input user_id=2819c223-7f76-453a-919d-413861904646 \
    --input expected_approver=compliance-lead@example.com
```

`--secret-env SCIM_TOKEN` is what turns on the `env:` provider that reads
`FLOWSTATE_SECRET_SCIM_TOKEN`, and a worker holding a secret provider refuses to
start without an `--auth-policy` that has a `secrets:` section allowing
`env:SCIM_TOKEN`. The server takes `--plugin-dir` too, because the file declares
`plugins:`, and an `--auth-policy` trusting a real issuer, with the
`--rpc-resource` its tokens are minted for, rather than `--insecure-no-auth`,
because the decision below is a signal only an attested reviewer other than the
starter may send.

`flow run` refuses this file today (#1548): it checks the file against its own
build's task registry, takes no `--plugin-dir`, and so reports the `scim.*`
tasks as ones nothing registered before the server sees it. Until that is
fixed, `flow run local` with the worker's plugin, secret and policy flags runs
it in one process, and an agent host running `flow mcp --plugin-dir ./plugins`
submits it to this server with `flowstate_compile` then `flowstate_run`.

The run stops at `decision` and waits - durably, for up to a week - until a
compliance reviewer answers:

```console
$ flow signal <run> review-decided --data '{"keep": false}'
```

## What each step is protecting

- **`in_scope`** is bounded by construction. It reads one page of fifty
  (`count: 50`) and reports `total_results`; a directory with fifty thousand
  accounts is paged by a caller following `next_start_index`, which this file
  does not do. A `count` over the task's ceiling is refused rather than quietly
  lowered, so nobody discovers at audit time that a review covered the first
  five hundred accounts.

- **`under_review`** is read for two things: the evidence a person judges, and
  the `version` - the provider's ETag - that makes the write below conditional.

- **`decision`** is bounded by `signals:`. Only the subject
  `https://issuer.example.com#<expected_approver>` holding the claim
  `team: compliance-reviewers` can answer, and `distinct_from_starter: true`
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
