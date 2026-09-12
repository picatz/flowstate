# Flowstate custom Ship procedure

Ship means prepare, review, and merge a pull request; it never means bypassing
evidence because the change looks small.

1. Fetch `origin/main`, inspect the actual base/head diff, run the narrowest
   falsifying tests and the diff-scoped gate, and report any unavailable leg.
2. Push a focused branch and open a pull request **without auto-merge**. Never
   use `gh pr merge --auto` or enable GitHub auto-merge.
3. Finish all edits before requesting review. Record the intended final
   40-character head and obtain one provider-neutral AI review covering both
   code and security on it: a fresh-context review that carries this
   repository's rubrics, such as the `flowstate-reviewer` subagent on Claude
   Code. That review is the evidence, and it must identify the reviewer, full
   head SHA, `code-security` scope, and PASS/no-actionable-findings verdict.
   Do not request a vendor review bot. One that reviews on its own is input,
   not a gate: read what it says, disposition it once for the head it reviewed,
   and never wait, retry, or treat its silence, quota, or absence as either a
   defect or a PASS.
4. Before merging, read every response that has arrived, including suppressed
   suggestions, checks, and review threads. Give every substantive suggestion
   one visible classification: fixed with evidence, false positive with
   evidence, obsolete, or deferred to a searched, non-duplicate scoped issue.
   Correctness and security findings block until fixed and independently
   re-reviewed. Resolve a thread only after its disposition is visible. A late
   response invalidates the final attestation below; if one arrives after merge,
   triage it promptly into a focused fix or issue rather than ignoring it.
5. Only a material defect in this change earns a new head: something wrong in
   correctness, security, durable state, or a documented claim the code does
   not support. Fix that and review the new head, where the review covers the
   fix rather than auditing the whole diff again unless the fix changed the
   design. An advisory, stylistic, or out-of-scope finding is dispositioned on
   the head where it arrived, or becomes a scoped follow-up issue; it does not
   restart these gates, and neither does a bot repeating a finding already
   dispositioned. Post the review evidence and this machine-readable owner
   attestation in one PR comment, authored by the repository owner, since
   `shipcheck` reads the comment's `OWNER` association and a comment posted
   under a bot identity does not carry it:

   `<!-- flowstate-independent-review:v1 {"headSha":"FULL_SHA","reviewer":"REVIEWER_IDENTITY","scope":"code-security","status":"pass"} -->`

   Wait for every applicable check, including non-required checks—not merely
   GitHub's required checks. Run expensive CI only once per intended final
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
