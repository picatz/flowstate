# Handoff

Where the work stands and what to pick up next. Delete this file once it is stale
— it describes a moment, not the design. [ARCHITECTURE.md](./ARCHITECTURE.md) is
the durable document.

Branch: `harden-and-expand-engine`, seven commits, pushed. A clean checkout builds,
vets clean, and passes tests in every package except the two noted below.

## Resolved late in the day

**The Flowfile parser now bounds alias expansion.** It previously bounded only how
far an alias *chain* was followed, not how many nodes an alias *expanded to*, and
expansion is what explodes:

```yaml
a: &a ["x","x","x","x","x","x","x","x","x","x"]
b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a,*a]
c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b,*b]
```

Ten per level, ten levels, 10^9 nodes, every chain one hop long. A `flowfile.test`
process reached 23 GB resident and 32 GB of swap on this and had to be killed.

`maxNodes` and `maxDepth` in `flowfile/parse.go` now bound it, reported as an
ordinary diagnostic with a position:

```
7:22: steps[0].task.inputs.m5[5][6][6][6][8]: holds more than 100000 values once
      aliases are expanded, which is more than a Flowfile is meant to hold
```

Verified against the document above under a 512 MiB cap: rejected immediately.

Still worth doing tomorrow: confirm the four parser tests that were failing
(`TestParseReportsPositions`, `TestParsePositionPaths`, `TestParseExpressionContexts`,
`TestParseAnchorsAndMerge`) now pass, with a bounded run —
`GOMEMLIMIT=512MiB go test -timeout 30s ./pkg/flowstate/v1/flowfile/`. Fuzzing that
package is reasonable again now that the bound exists, but run it with
`-parallel 1`, a small `-fuzztime`, and `GOMEMLIMIT` set.

## Also unfinished

**`pkg/flowstate/v1/secrets` is mid-refactor and uncommitted.** It builds; its
tests do not. `Resolve` is moving from taking a `Ref` to taking a `Request` so
resolution can be scoped by namespace, and `NewRef`, `RefString`, `ValidateRef`,
`ErrPermission`, and `ErrUnavailable` are in flux. A Vault provider and a provider
registry exist in partial form. Get it compiling and passing before adding anything.

**`pkg/flowstate/v1/sensitive` does not exist yet.** It is the agreed home for one
secret-bearing value type shared by `secrets` and `auth`, which today have two with
different guarantees. It has to be a leaf package depending only on the standard
library: `auth` will need `secrets` (an OAuth exchanger reading a stored client
secret from a file), so the type cannot live in either. Material belongs in a
closure, because `fmt` cannot call a method on a value it reaches through an
unexported field and so prints the fields instead — that is how a private signing
key was found printable through `%+v` on an enclosing struct.

## Not started, roughly in order of value

1. **Wire secrets into a task end to end.** The schema, the resolver, and the
   providers exist; nothing connects them. A string input should accept a
   `SecretRef`, resolved inside the activity. The DSL needs syntax for it —
   `${secret('env:API_KEY')}` compiling through `ParseRef`, so a malformed
   reference is a compile error. Scrub the response body before evaluating an
   `outputs` expression, or a server echoing a token back lands it in step outputs,
   and step outputs go to history.

2. **Mount the JWKS and discovery handlers.** `broker.Issuer().Handler()` at
   `auth.DiscoveryPath` and the JWKS path, and they must sit **outside** the authn
   middleware — a relying party fetches them before it has any credential. Worth a
   test asserting that, because it is exactly the kind of thing that regresses
   silently. Also needs an `--identity-key` flag for the signing key.

3. **Enforce tenancy.** `auth/tenancy.go` and `auth/secretpolicy.go` exist. The
   rule to preserve: a workload's namespace comes from the authenticated caller,
   never from the workload. Optional mapping onto Temporal namespaces needs
   something from `temporalclient`.

4. **Durable waiting.** `sleep:` and `wait_until:` as durable timers, and
   `wait_for_signal:` with `flow signal` for human approval gates. This is the
   capability that most distinguishes the engine from a task runner, and the frame
   stack now makes it tractable.

5. **Sub-workflows.** A `workflow:` step as a Temporal child workflow, which also
   relieves payload pressure on fan-out by giving each one its own history.

6. **Lifecycle RPCs.** List, cancel, terminate, signal, query — thin wrappers over
   what Temporal already provides. The service is still only `Run` and `Get`.

## What is done and verified

- Zero reachable vulnerabilities, including standard library, via a pinned toolchain.
- Bounded CEL evaluation: the verified attack expression now fails in ~60 ms instead
  of running seconds past its deadline.
- Governed egress, verified by hand: a workflow targeting a loopback metadata
  endpoint is denied with a reason, and reaching localhost needs an explicit opt-in.
- Fail-closed authentication; the server refuses to start unconfigured.
- Real specification validation, replacing 3,408 generated lines that enforced
  nothing.
- Correct retry classification, so a non-idempotent request is not repeated because
  its failure was deterministic.
- Per-step timeout, retry, and `continue_on_error`; conditionals; `for_each` with
  bounded concurrency; `parallel` branches; frame-stack Continue-As-New.
- Execution parity: the same cases run against both drivers and agree.
- A language server providing diagnostics, hover, completion, go-to-definition, and
  document symbols, verified over the real protocol rather than only in tests.
- All ten examples validate and run.

## Two bugs worth remembering

Both were found by writing something ordinary, not by looking for them.

Restoring the `http` task's documented `body` output revealed that **output shaping
never worked under Temporal at all** — the engine pre-resolved every expression in
workflow code, but `outputs` references response variables that do not exist yet.
The registry now declares which inputs a task evaluates itself.

Writing a plain `printf` example revealed that a list mixing a step reference with
a literal was rejected, because the literal and expression paths had diverged. And
input resolution mutated the task in place, which would have made a loop's second
iteration read the first iteration's values.
