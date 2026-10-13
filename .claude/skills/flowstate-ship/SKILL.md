---
name: flowstate-ship
description: Use when asked to land, ship, or merge a Flowstate pull request, or to take one through the exact-head review, disposition, shipcheck, and manual-merge gates that an autonomous merge requires.
argument-hint: "[pr-number]"
---

# Shipping a pull request

[`.agents/ship.md`](../../../.agents/ship.md) is the procedure and the
authority, including the exception rule; read it first. This skill says what
each gate looks like from a coding host. Pull request: $ARGUMENTS (resolve it
from the current branch when empty).

## Gates, in order

1. **Final head.** Finish every edit, push, and record the full 40-character
   head SHA on the pull request before requesting review. Any later push
   restarts from here. Before every push to an open pull request, run
   `go run ./tools/mergecheck`; a push without its `clean against origin/main`
   line is unverified, and conflicting paths are resolved first.
2. **Independent review on that head.** On Claude Code, delegate to the
   `flowstate-reviewer` subagent with the PR number or base/head, first on the
   local base..head diff before the first push; its fresh context is the
   independence. Compare the SHA its verdict names with the real head before
   relying on it.
3. **Disposition.** Disposition every finding the way `comms-review` says, on
   the head where it arrived; only a material defect earns a new head and a
   return to gate 1. On Claude Code, delegate the thread resolution and the
   hiding of decision-free bot comments to the `flowstate-pr-tidy` subagent so
   human reviewers see only decisions.
4. **Attestation.** After the last review activity, print the comment with
   `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER --attest
   REVIEWER --head SHA` (SHA as the reviewer's verdict names it) and post it
   unchanged as a new comment. `tools/shipcheck` rejects an attestation that any
   later comment, review, or thread update follows, so post it last, and it
   reads the comment's `OWNER` association, so post it as the owner: on Claude
   Code the GitHub tools carry that identity, while `gh` with the session token
   posts as a bot and the attestation will not count. When it is refused,
   shipcheck names the reason.
5. **Checks.** Every applicable check terminal and acceptable on that head,
   including non-required ones; run expensive CI once per intended final head.
6. **Shipcheck.** `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER`
   must pass on the unchanged head. It needs `gh` and a token.
7. **Merge, pinned.** Write the squash message to a file (it follows
   `comms-commit`), then run
   `go run ./tools/shipmerge --repo picatz/flowstate --pr NUMBER -F FILE` as a
   command of its own. It checks the message, reruns shipcheck, and merges over
   REST pinned to the head shipcheck read, so it works where GraphQL is
   refused. Fall back to the exact-head invocation in `.agents/ship.md` only
   when it cannot run; Claude's `mergeguard` hook denies auto-merge, `--admin`,
   an unpinned merge, a merge without thread evidence, and unresolved threads.
8. **Prove it landed.** Fetch `origin/main`, confirm the merge commit is
   reachable, wait for the post-merge checks on `main`, and report the PR, head,
   merge commit, review evidence, and checks.
