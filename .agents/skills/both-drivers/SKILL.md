---
name: both-drivers
description: Use for behavior that must agree between local and Temporal execution.
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
5. Run the specific case against both drivers. `-short` is wrong here: this
   step exists to prove the durable driver agrees with the local one, and
   `engine`'s own tests exit early under `testing.Short()` — the same flag
   that `flowstate-verify`'s inner loop reaches for would make this step
   certify nothing about Temporal. `make dev-temporal` is a foreground
   recipe that stays running, so start it in one terminal and paste its
   printed `export` line into a second one before running the test:

   ```sh
   # terminal 1 — stays running, Ctrl-C when done
   make dev-temporal

   # terminal 2 — paste the export line make dev-temporal printed, then:
   export FLOWSTATE_TEST_TEMPORAL_ADDRESS=127.0.0.1:PORT
   GOMEMLIMIT=1GiB go test -timeout 120s -run '<LocalTestName>|<DurableTestName>' ./pkg/flowstate/v1/ ./pkg/flowstate/v1/engine/
   ```

   The local and durable tests for one case rarely share a literal name —
   `TestStepTimeoutReachesTheTaskLocal` and
   `TestStepTimeoutReachesTheTaskDurable` are one case, not two — and `-run`
   is a regexp applied independently to each package: a single name that
   only one package has still exits 0 in the other with "no tests to run",
   silently certifying only one driver. Give `-run` an alternation naming
   both (or run each package with its own `-run`), and confirm both actually
   ran — `go test -v` printing both names, or two `PASS` lines, not one.

6. Use the `flowstate-verify` skill for the broader gate before PR handoff.

Report which two call sites exercised the shared case and which observable result
the test forces to agree.

## Historical field notes

Read [the archived both-drivers command](../../../.agent-history/commands/both-drivers.md) only when a prior incident or rationale is relevant. It is evidence and history, not a second current procedure.
