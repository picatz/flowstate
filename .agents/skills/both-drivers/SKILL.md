---
name: both-drivers
description: Use when a change touches behavior observable through both `flow run local` and Temporal execution (step execution, expression timing, retries, timeouts, tolerance, loops, compensation, cancellation, run results), so shared conformance cases prove the drivers agree.
---

# Both execution drivers

Use this whenever a change touches step execution, expression timing, retries,
timeouts, tolerance, loops, compensation, cancellation, run results, or another
behavior observable through both `flow run local` and Temporal.

## Procedure

1. Put shared cases under `pkg/flowstate/v1/internal/conformance`, not in one
   driver's package.
2. Confirm both drivers call the case set you added or extended. Compilation is
   not proof that both paths execute it:

   ```sh
   grep -rn '<CaseSetName>' --include='*.go' .
   ```

3. If the drivers diverge, look first for a fact written twice. Move shared
   semantics into `pkg/flowstate/v1` or the existing common execution mechanism,
   then let both drivers read it.
4. Test the direction that distinguishes the implementations. A case that only
   proves each driver can succeed independently does not prove agreement.
5. Run the specific case against both drivers, without `-short`: this step
   exists to prove the durable driver agrees with the local one, and a
   durable test that dials a live Temporal server (through
   `newTemporalNamespace`) skips outright under `testing.Short()` — the same
   flag `flowstate-verify`'s inner loop reaches for would certify nothing
   about Temporal for that test. Not every `engine` test is gated this way —
   one built on `testsuite.WorkflowTestSuite` runs a real `engine.Run` against
   a simulated Temporal SDK and never skips — but `-short` still has to be
   left off, since which kind a given test is isn't always obvious from its
   name:

   ```sh
   GOMEMLIMIT=1GiB go test -count=1 -timeout 120s -v \
     -run '<LocalTestName>|<DurableTestName>' \
     ./pkg/flowstate/v1/ ./pkg/flowstate/v1/engine/
   ```

   `-count=1` bypasses the test cache — without it, an unchanged package can
   report a cached pass instead of actually re-executing.

   The local and durable tests for one case rarely share a literal name —
   `TestStepTimeoutReachesTheTaskLocal` and
   `TestStepTimeoutReachesTheTaskDurable` are one case, not two — and `-run`
   is a regexp applied independently to each package: a single name that
   only one package has still exits 0 in the other with "no tests to run",
   silently certifying only one driver. Give `-run` an alternation naming
   both (or run each package with its own `-run`), and confirm both actually
   ran by finding `--- PASS: <LocalTestName>` and `--- PASS: <DurableTestName>`
   by name in the `-v` output — two `PASS` lines is not enough, since an
   unmatched package still prints "no tests to run" followed by its own
   package-level `PASS`.

   If the durable test does dial a live server, the engine package's
   `TestMain` boots and tears one down automatically for the run above, at a
   cost of roughly eleven seconds. For faster iteration across several runs,
   `make dev-temporal` starts a server that stays up and prints an `export`
   line; paste that into the terminal running `go test` first and the same
   package's tests start in about a second instead:

   ```sh
   # terminal 1 — stays running, Ctrl-C when done
   make dev-temporal

   # terminal 2 — paste the export line make dev-temporal printed, then run
   # the go test command above
   export FLOWSTATE_TEST_TEMPORAL_ADDRESS=127.0.0.1:PORT
   ```

6. Use the `flowstate-verify` skill for the broader gate before PR handoff.

Report which two call sites exercised the shared case and which observable result
the test forces to agree.

## History

[The archived both-drivers command](../../../.agent-history/commands/both-drivers.md)
is evidence and history, not a second current procedure.
