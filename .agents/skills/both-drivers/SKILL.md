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
5. Run the bounded shared suite:

   ```sh
   GOMEMLIMIT=1GiB go test -timeout 120s ./pkg/flowstate/v1/...
   ```

6. Use the `flowstate-verify` skill for the broader gate before PR handoff.

Report which two call sites exercised the shared case and which observable result
the test forces to agree.

## Historical field notes

Read [the archived both-drivers command](../../../.agent-history/commands/both-drivers.md) only when a prior incident or rationale is relevant. It is evidence and history, not a second current procedure.
