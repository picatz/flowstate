# Testing a policy, not just writing it

`flow test` runs workflows and `flow signals check` rehearses a workflow's own
gates. `flow policy test` puts cases to the policy files a deployment hands a
worker, and says which rule denied each refusal.

```sh
flow policy test examples/egress-policy.yaml examples/policy-test/egress-cases.yaml
flow policy test examples/task-shape-policy/task-policy.yaml examples/policy-test/task-cases.yaml
```

Each cases file names the policy surface once (`egress`, `task` or `exec`) and
lists cases: who asks (`principal`), what is asked (`request`), and what the
policy must say (`expect: allow` or `deny`). A denial can name the rule that must
make it with `rule:`, the deny rule's source text or, for a denial no deny rule
made, the reason (`allow rules`, `scheme`, `port`, `address`, `rule error`).

The cases here are mostly refusals, on purpose. "team-a reaches its own partner"
proves almost nothing; "team-b does not" and "a run with no attested identity does
not" prove the tenant boundary. Loosen `identity.namespace == "team-a"` in the
egress policy to `true` and `team-b is refused team-a's partner API` fails; the
verb exits 1 and prints what it expected and what it got.

Each case is decided by the evaluator the worker enforces that policy with, so
there is no second copy of the rules to drift. Egress is decided before DNS: the
`ip:` field says what a hostname resolves to, for the address checks (loopback,
private, link-local, metadata) a hostname alone cannot exercise. An `exec` suite
resolves programs and directories against the machine it runs on, so it is not
portable between machines, and this directory does not ship one; see the
`exec` tests in `cmd/flow/policytest_test.go` for the shape.

`-o json` writes the report as a document for a job to read.
