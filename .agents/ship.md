# Flowstate custom Ship procedure

Ship means prepare, review, and merge a pull request; it never means bypassing
evidence because the change looks small.

1. Fetch `origin/main`, inspect the actual base/head diff, run the narrowest
   falsifying tests and the diff-scoped gate, and report any unavailable leg.
2. Push a focused branch and open a pull request **without auto-merge**. Never
   use `gh pr merge --auto` or enable GitHub auto-merge.
3. Finish all edits before requesting independent review. Record the exact
   40-character head, then request all configured channels: `@codex review`,
   `@codex security review`, and Copilot review. Make at most two requests per
   channel and wait at most ten minutes total for each provider on one head;
   retries after that are churn, not stronger evidence.
4. Read every review, suppressed suggestion, check, and review thread. Reply
   with one visible disposition for every finding: fixed with evidence, false
   positive with evidence, obsolete, or linked to a searched, scoped follow-up
   issue. Resolve a thread only after its disposition is visible.
5. Any pushed fix invalidates prior approval. Request every review channel again
   on the new exact head. Wait for every applicable check, including non-required
   checks and external review channels—not merely GitHub's required checks.
6. A review that returns findings must be dispositioned and re-reviewed; the
   fallback below never waives findings. When a named provider instead remains
   unavailable or quota-limited after step 3's bounded attempts and wait, use a
   distinct available AI reviewer for an independent code **and** security
   review of the exact head. Post its PASS/no-actionable-findings evidence on
   the PR, link the provider's outage response, and link the one searched
   tooling-incident issue. Then post this machine-readable owner disposition:

   `<!-- flowstate-review-fallback:v1 {"provider":"codex-security","headSha":"FULL_SHA","unavailableUrl":"PR_COMMENT_URL","evidenceUrl":"PR_COMMENT_URL","incidentUrl":"ISSUE_URL","reviewer":"DISTINCT_REVIEWER","scope":"code-security","status":"pass"} -->`

   Provider unavailability remains recorded as unavailable, never PASS. A stale
   review, unresolved or undispositioned finding, red/pending check, missing
   exact-head fallback review, or absent outage evidence still blocks.
7. Run `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER`. It must
   pass on the unchanged final head.
8. Merge manually with exact-head protection, for example
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
