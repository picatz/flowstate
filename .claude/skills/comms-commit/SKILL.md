---
description: Commit messages that say what changed, plus why when non-obvious
---

Audience: the future reader in `git log` or `git blame`, years out, with no
session context. Of every surface in this set, the commit message is the
one guaranteed to survive tool migrations, so the why lives here rather
than only in a PR body.

The governing rule: minimum receiver effort at the required fidelity; think
as much as the task deserves, publish what the recipient needs.

## Shape

Subject: `area: what changed`, specific enough to find in `git log
--oneline`. Body: the why when it is not obvious from the diff, the
mechanism when it is subtle, the proof when the change claims one. Footers
per CLAUDE.md (Co-Authored-By, Claude-Session). A truly obvious change
earns a subject and nothing more.

Not a diary, not a changelog: the body explains, it does not enumerate.

## Examples

#480's body carries exactly the why the diff cannot:

> stepkeys: reserve `async` while structured concurrency is designed (#418)
>
> Reserving it now means landing the construct later is a change to this
> package rather than a break for a plugin that registered a task under
> the name in the meantime

#479's review-fix commit: the subject states the semantic change; the body
gives the finding, the mechanism, and the re-proof:

> tests: make the reverse-completion forcing deterministic, not probable
>
> The hold is now a `sleep:` opening the slow branch. The durable test
> environment advances virtual time only once no activity is runnable, so
> the quick branch's step has completed and registered before the slow
> branch starts any work at all
>
> Mutation re-proof under the deterministic forcing: reversing the durable
> driver's merge loop fails the case.

## Failure modes

- **The diary**: narrating the session ("first tried X, then discovered
  Y"). History wants the result and its reason, not the route.
- **The changelog echo**: listing files and hunks. `git show` does that.
- **The bare subject on a non-obvious change**: "fix test" over a change
  whose whole value is its reasoning. The why is the payload; carry it.

## Self-check

Will `git log --oneline` find this commit by its subject? Does the body
say anything `git show` does not? If the change is non-obvious, is the
reason in the message itself rather than only in the PR?
