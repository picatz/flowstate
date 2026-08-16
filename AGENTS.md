# Flowstate — agent instructions

Flowstate is a durable, policy-governed workload engine: you declare a workload
in a YAML+CEL DSL (a `Flowfile`), it compiles to a typed protobuf specification,
and it executes on Temporal's durable-execution engine. It is not a CI system —
the target is anything that must finish correctly despite crashes, network
failures, and long waits.

This file is for an agent working *on* this repository (Codex, Claude Code, or
any other tool that reads `AGENTS.md`). It intentionally says almost nothing on
its own: **read [CLAUDE.md](CLAUDE.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
before changing anything.** CLAUDE.md is the canonical guidance for this repo;
this file exists only as a pointer to it for tools that look for `AGENTS.md`
specifically, not as a second copy — if something here ever disagrees with
CLAUDE.md, CLAUDE.md wins, and the difference is a bug in this file.

(If you're looking for how to configure an editor's or agent's *MCP client* to
talk to `flow mcp`, that's a different audience and a different document — see
[docs/CLI.md](docs/CLI.md#flow-mcp-the-same-surface-for-an-agent). This file is
about developing Flowstate itself.)

## Before you change anything

Read CLAUDE.md's invariants — proto-first types, bounding untrusted input,
fail-closed policy, secrets never entering workflow history, both execution
drivers agreeing, and the rest. A change that violates one is a bug even when
the tests pass.

## The verification gate

The diff-scoped gate is the default before pushing a PR branch; PR CI runs the
same diff-scoped computation and skips what the diff cannot reach, the merge
queue runs the full list, and `make check` is the same full list run locally:

    go run ./tools/gate    # diff-scoped tier (or: make gate)
    make check             # full CI-parity rehearsal

Bound test runs individually if you're not running a gate tier:

    GOMEMLIMIT=1GiB go test -timeout 120s ./pkg/flowstate/v1/...
    GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzName -fuzztime 60s ./path/

See CLAUDE.md's "The gate" section for what each check catches and the
toolchain pins (`GOTOOLCHAIN=go1.26.6`) `govulncheck` and `staticcheck` need.

## Working alongside other agents

- **Never edit a file you do not own.** Report the problem to whoever owns
  that package instead.
- **A build error in someone else's file is probably a stale snapshot** —
  re-read the file and rebuild before reacting, especially to an "undefined"
  generated type: `buf generate` rewrites `.pb.go` files in place.
- **Verify from a clean clone before believing the tree is green.** A shared
  working tree reflects everyone's unsaved work; only a fresh clone of the
  pushed branch shows what a colleague will actually get.
- **Verify claims rather than relaying them**, and **leave a green stopping
  point** — a smaller change that passes beats a larger one that doesn't.

See CLAUDE.md's "Working alongside other agents" for the full version of each
of these.
