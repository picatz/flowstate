# Flowstate custom Ship procedure

Ship means prepare, review, and merge a pull request; it never means bypassing
evidence because the change looks small.

1. Fetch `origin/main`, inspect the actual base/head diff, run the narrowest
   falsifying tests and the diff-scoped gate, and report any unavailable leg.
2. Push a focused branch and open a pull request **without auto-merge**. Never
   use `gh pr merge --auto` or enable GitHub auto-merge.
3. Finish all edits before requesting independent review. Record the exact
   40-character head, then obtain at least one distinct independent AI review
   covering both code and security on that head. The evidence must identify the
   reviewer, full head SHA, `code-security` scope, and PASS/no-actionable-findings
   verdict. A clean exact-head summary is valid evidence. Codex and Copilot are
   optional additional channels: request each at most once and do not wait on
   quota, unavailability, or a missing vendor-specific artifact.
4. Read every review, suppressed suggestion, check, and review thread. Reply
   with one visible disposition for every finding: fixed with evidence, false
   positive with evidence, obsolete, or linked to a searched, scoped follow-up
   issue. Resolve a thread only after its disposition is visible.
5. Any pushed fix invalidates prior review evidence. Obtain a new independent
   code-and-security review on the new exact head. Post its evidence and this
   machine-readable owner attestation in one PR comment:

   `<!-- flowstate-independent-review:v1 {"headSha":"FULL_SHA","reviewer":"REVIEWER_IDENTITY","scope":"code-security","status":"pass"} -->`

   Wait for every applicable check, including non-required checks—not merely
   GitHub's required checks. Run expensive CI only once per candidate final
   head. A stale review, unresolved or undispositioned finding, semantic
   uncertainty, or red/pending check still blocks. Optional-provider
   unavailability is never represented as PASS.
6. Run `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER`. It must
   pass on the unchanged final head.
7. Merge manually with exact-head protection, for example
   `gh pr merge NUMBER -R picatz/flowstate --squash --match-head-commit SHA`.
   Fetch `origin/main`, prove the merged commit is reachable, and wait for all
   applicable post-merge checks on `main`. Report the PR, final head, merge
   commit, review evidence, checks, and post-merge result.

An exception requires explicit human authorization that names the skipped gate
and applies to this PR. Never infer an exception from urgency, permissions,
change size, prior practice, or a green required-check subset. Record the
authorization and residual risk in the PR; do not enable auto-merge.

If this procedure misses a failure, do not rely on a reminder. Before the next
autonomous merge, add a regression check when feasible, update the owning
guidance or Ship procedure, or file a searched, scoped issue when the durable
fix requires a product/admin decision. Link that artifact from the incident.
