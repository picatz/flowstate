---
description: Checklist for any behavior change — verify both execution drivers agree
---

Local execution (`flow run local`) and durable execution through Temporal are
two drivers over one execution model, and anything observable between them
must match. Use this checklist whenever a change touches step execution,
retries, tolerance, loops, or run results:

1. Add the new case(s) to `pkg/flowstate/v1/tests`, not to one driver's own
   package — a case that lives only where one driver can see it can't prove
   agreement.
2. Confirm **both** drivers actually call the case set you added or extended,
   not just that it compiles. Grep for two callers:

   ```
   grep -rn '<CaseSetName>' --include='*.go' .
   ```

   Expect one call site in the local-driver path and one in the durable path.
   `ZeroValueCases` had exactly one caller for months — it compiled, passed,
   and proved only half of what it was written to prove.
3. If the case reveals a divergence, check whether it's one of the two
   recurring shapes from CLAUDE.md: a value with one meaning written down
   twice (put it in `pkg/flowstate/v1`, which both drivers import, and let
   each read it), or a bound that's only exercised once a second attempt/path
   exists (a bound nothing reaches is a bound nothing tests — look for a
   sibling bound immediately behind the one you just fixed).
4. Re-run the shared suite bounded, per CLAUDE.md:

   ```
   GOMEMLIMIT=1GiB go test -timeout 120s ./pkg/flowstate/v1/...
   ```
