---
name: comms-pr
description: Use when drafting or updating a Flowstate pull request title and body; the body follows the template's six sections and the commit-conventions check reads it.
---

# Pull request communication

A PR body makes the change reviewable without restating the diff. Gather the
base/head diff, commits, the linked issue or decision, generated artifacts, and
the command results, and treat them as authoritative over recollection. If the
branch moved, refresh them before editing the body.

## Default shape

1. **Why** — the problem, decision, or missing capability this change addresses.
2. **What changed** — the behavioral shape and the important boundaries, not a
   list of files.
3. **Design choices** — only non-obvious choices a reviewer must evaluate,
   including alternatives deliberately refused.
4. **Verification** — exact checks that completed and the behavior they prove.
5. **Risk, compatibility, and rollout** — where the change can fail, what remains
   compatible, and any migration or operational consequence.
6. **Remaining uncertainty** — checks not run, known limitations, or follow-up
   work that is intentionally outside this PR.

These are the `##` headings of `.github/PULL_REQUEST_TEMPLATE.md`; a test keeps
the two lists equal. A small change has a small body; use tables, examples, or
mutation evidence only when they materially reduce the reviewer's work.

## What the conventions check reads

`tools/commitcheck` runs on the title and body (warning-only until 2026-09-21,
then strict) and on the squash message at merge time:

- Title: `scope: lowercase imperative`.
- Body: an issue reference (`Refs #N`, `Closes #N`) or a `No-Issue: <reason>`
  trailer, and a `## Verification` heading or `Verification:` line, or an
  `Unverified: <reason>` trailer.
- "fully tested", "safe", "backward-compatible", and "no impact" must sit on a
  line that also carries their evidence.
- No hand-written attribution footer and no `claude.ai/code/session_…` link.

## Repository specifics

- Do not hard-wrap GitHub prose; keep each paragraph and list item on one source
  line and let the browser wrap it.
- Distinguish a local pass from CI, and a targeted check from the full gate. A
  skipped or unavailable check is named as such, not omitted.

## Attribution

The host and the forge append their own attribution to a body they post. One
written by hand lands above theirs rather than instead of it, and two whose
wordings differ do not collapse into one: #1976 said it three times. Write none.
The post's author is the claim, and the way to keep attribution quiet is to post
less often, not to shrink the marker.

A session link belongs to no surface at all. It resolves for the agent that
produced it and for no other reader, so in a pull request body it is a dead
reference, and a body pasted into a squash message carries it into `git log` for
good. This history already carries them; `git log
--grep='claude.ai/code/session_'` counts how many, which is a number worth
measuring rather than quoting, since a shallow clone sees only part of it.

The check knows whose is whose: it reads a pull request body back as the forge
stores it, discounts the one footer appended after your last line, and reports
whatever is left. So the forge's own marker is never a finding and a second one
always is.

Review comments and replies are the exception, and the only one: nothing appends
a footer there, so those carry it explicitly.

## History

[Archived comms-pr guidance](../../../.agent-history/skills/comms-pr/SKILL.md)
is evidence and history, not a second current procedure.
