---
name: flowstate-verify
description: Choose and report targeted, gate, or full Flowstate verification.
---

# Flowstate verification

Verification is evidence, not a ritual. Select the cheapest check that can
falsify the changed behavior, then broaden according to the diff's reach and the
handoff being prepared.

## 1. Determine the affected surface

Inspect the diff and identify changed packages, generated artifacts, public
interfaces, both-driver behavior, and external integrations. Do not run the
largest suite merely to avoid making that judgment.

## 2. Inner loop

Run focused tests with explicit time and memory bounds. A common bounded tier is:

```sh
GOMEMLIMIT=1GiB go test -short -timeout 120s ./...
```

For one package or test, narrow the package and `-run` pattern further.

The packages that share a Temporal dev server (`engine`, `server`,
`temporalclient`, `cmd/flow`) each boot one in `TestMain`, about eleven seconds
before the first test runs. When iterating on one of them, start a server once
and let every run attach to it:

```sh
make dev-temporal                       # prints the export line, stays up
export FLOWSTATE_TEST_TEMPORAL_ADDRESS=127.0.0.1:PORT
GOMEMLIMIT=1GiB go test -timeout 120s -run TestOne ./pkg/flowstate/v1/engine/
```

Unset, nothing changes. Stop the server you started when you are done.

Bound a fuzzer by time, memory, and parallelism:

```sh
GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 \
  -run=XXX -fuzz FuzzName -fuzztime 60s ./path/
```

## 3. Normalize and derive

Use `make fmt`, not a bare `gofmt` — a `gofmt` from `PATH` may be a different
binary from the pinned toolchain's and can disagree on formatting. Run
generation and drift checks when schemas or generated surfaces may have
changed. Do not edit generated files directly.

## 4. Before a PR handoff

Run the diff-scoped repository gate unless the user explicitly requested a
narrow draft that is not ready for review:

```sh
go run ./tools/gate
# equivalent: make gate
```

Use `make check` for a full CI-parity rehearsal when the task, risk, or requested
handoff warrants the full repository cost.

## 5. Report honestly

For every attempted leg, report the command and one of: passed, failed, timed
out, unavailable, or not run. Name the first actionable failure. Do not translate
a skipped leg or silent tool absence into green. Check for stray processes and
the final diff before declaring completion.

`make test` and the gate's test legs pipe `go test -json` through
`tools/testsum`, which prints the failing tests with their `file.go:NN`, the
shuffle seed to rerun them when `-shuffle` is on, and a count of what passed.
Quote that block rather than the log; for one package,
`go test -json ./path/ | go run ./tools/testsum` prints the same shape.

## Historical field notes

Read the archived [full CI](../../../.agent-history/commands/ci-check.md) or [fast test](../../../.agent-history/commands/test-fast.md) command only when a prior rationale is relevant. They are evidence and history, not a second current procedure.
