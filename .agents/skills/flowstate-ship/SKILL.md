---
name: flowstate-ship
description: Use when asked to land, ship, or merge a Flowstate pull request, or to take one through the exact-head review, disposition, shipcheck, and manual-merge gates that an autonomous merge requires.
argument-hint: "[pr-number]"
---

# Shipping a pull request

[`.agents/ship.md`](../../../.agents/ship.md) is the procedure and the
authority; read it first. This skill says what each gate looks like from a
coding host, and where the loop stops. Pull request: $ARGUMENTS (resolve it from
the current branch when empty).

## Gates, in order

1. **Final head.** Finish every edit, push, and record the full 40-character
   head SHA on the pull request before requesting review. Any later push
   restarts from here.
2. **Independent review on that head.** One provider-neutral AI
   code-and-security review, and it is the evidence. On Claude Code, delegate
   to the `flowstate-reviewer` subagent with the PR number or base/head; its
   fresh context is the independence. Do not request a vendor review bot. One
   that reviews on its own is input: read it, disposition it once for the head
   it reviewed, and never wait on it.
3. **Disposition.** Give every finding one visible disposition: fixed with
   evidence, false positive with evidence, obsolete, or deferred to a searched,
   scoped issue. Resolve a thread only after its disposition is visible on it.
   A material defect (correctness, security, durable state, or a documented
   claim the code does not support) blocks until fixed, and the fix is a new
   head: return to gate 1, where the review concentrates on the fix but
   verdicts the whole base-to-head diff, since that head is what is attested.
   Everything else is dispositioned here and does not restart the gates, so a
   round that yields only advisory findings ends the
   review. Do not answer an AI reviewer with AI prose; the disposition is the
   reply. On Claude Code, delegate the thread resolution and the hiding of
   decision-free bot comments to the `flowstate-pr-tidy` subagent so human
   reviewers see only decisions.
4. **Attestation.** After the last review activity, post one owner comment that
   states the reviewer, the full head SHA, the `code-security` scope, and
   "PASS: no actionable findings", carrying the machine-readable line from
   `.agents/ship.md`. `tools/shipcheck` rejects an attestation that any later
   comment, review, or thread update follows, so post it last, and it reads the
   comment's `OWNER` association, so post it as the owner: on Claude Code the
   GitHub tools carry that identity, while `gh` with the session token posts as
   a bot and the attestation will not count.
5. **Checks.** Every applicable check terminal and acceptable on that head,
   including non-required ones; run expensive CI once per intended final head.
6. **Shipcheck.** `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER`
   must pass on the unchanged head. It needs `gh` and a token.
7. **Merge, pinned.** Use the exact-head invocation from `.agents/ship.md`
   (`--match-head-commit SHA` with the CLI, or the GitHub merge tool with
   `expectedHeadSha`). The squash message follows `comms-commit`. Claude's
   `mergeguard` hook denies auto-merge, `--admin`, an unpinned merge, a merge
   without thread evidence, and unresolved threads.
8. **Prove it landed.** Fetch `origin/main`, confirm the merge commit is
   reachable, wait for the post-merge checks on `main`, and report the PR, head,
   merge commit, review evidence, and checks.

An exception needs explicit human authorization naming the gate; record it and
the residual risk on the pull request. Never infer one from urgency, size,
permissions, or a green subset of checks.

## Where the loop stops

Fix material findings; classify the rest visibly and move on. A round that
yields only optional or stylistic findings is a passing round, and its findings
become follow-ups rather than another head. Review is bounded per head, not per
reviewer: a bot that re-reviews a new head does not reopen a gate already
satisfied, and a finding does not become new by being repeated while its
disposition still holds. A repetition after a push that reintroduced the defect,
or that the fix did not cure, is a finding again. Two rounds that keep producing
new material findings mean the change needs rethinking: stop, summarize the
recurring root cause, and ask rather than iterate.
