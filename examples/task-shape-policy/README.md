# A control the workflow's author cannot delete

[`workflow.yaml`](workflow.yaml) has already lost every in-file safeguard
`examples/approval-gate` demonstrates: no `wait_for_signal:`, no `signals:`, and
`deploy`'s own `if:` has been weakened all the way to `true`. That is #206 gap 3,
named in `approval-gate`'s own header and tracked at #187: a control written in a
Flowfile is a control the Flowfile's author can delete.

## `task-policy.yaml`

A deployment-side task-shape policy is the fix, because it is not something this
file carries at all:

```yaml
deny:
  - 'task == "log" && identity.namespace != "platform"'
```

CEL, compiled and type-checked when the policy loads rather than when a task
tries to dispatch, evaluated at the one seam both execution drivers funnel every
task through (`Task.EvalInScope`) — before the task's own code runs, and before
any secret it would need resolves. With nothing configured, every task dispatches
exactly as it does today; once a policy is loaded, an errored rule denies and a
denied dispatch produces none of the side effects a permitted one would have.

## Running it

```console
$ flow run local examples/task-shape-policy/workflow.yaml
$ flow run local examples/task-shape-policy/workflow.yaml \
    --task-policy examples/task-shape-policy/task-policy.yaml
```

The first runs `deploy` unrestricted — the zero case. The second refuses it, and
`flow worker` takes the identical flag and means the same thing by it: the
rehearsal is governed by the rules production applies.

What a Flowfile cannot rehearse is a *real* attested identity — a local run's
`identity.namespace` always reads empty, which this policy's rule happens to deny
regardless (`"" != "platform"` is true). The durable half, with a genuinely
attested identity on both sides of the rule — denied outside the platform team's
namespace, permitted inside it — is `engine.TestTaskPolicyIdentityNamespaceDenial`
in `pkg/flowstate/v1/engine`, run under the durable test harness as part of
`make check`.
