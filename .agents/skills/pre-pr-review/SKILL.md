---
name: pre-pr-review
description: Self-review a Flowstate branch and its shipped artifacts before a pull request.
---

# Pre-PR review

Run this over the actual base/head diff and every shipped artifact: code, tests,
generated output, documentation, commit messages, and the proposed PR body.
`PASS` is valid; manufacturing findings creates churn and trains reviewers to
ignore the next real one.

## Lenses

1. **Scope** — every changed line contributes to the requested outcome; unrelated
   cleanup and speculative abstractions are absent.
2. **Behavior** — the implementation satisfies the intended semantics on the
   concrete paths the change makes reachable.
3. **Boundaries** — empty, absent, maximum, retry, cancellation, cleanup, and
   fail-closed directions are covered where relevant.
4. **Source of truth** — the change derives shared facts instead of writing a
   second copy that can drift.
5. **Driver agreement** — behavior observable in local and Temporal execution is
   covered by shared conformance tests and actually called by both drivers.
6. **Regression evidence** — tests fail for the defect they claim to catch. Use a
   mutation proof when the test's connection to the mechanism is not otherwise
   obvious or the risk justifies it; do not turn mutation into ceremony.
7. **Generated and public surfaces** — generation, examples, CLI reference, and
   user-facing docs agree with the implementation.
8. **Verification honesty** — the final report and PR body name exactly what ran,
   what passed, and what did not run.
9. **Receiver effort** — remove repetition, file tours, unsupported absolutes,
   and context the reviewer already has in the issue or diff.

Use the `flowstate-verify` skill to select the appropriate checks. Inspect the
resulting diff again after formatting or generation.

## Output

Return only material findings with location, consequence, evidence, and a
concrete fix. If none remain, return `PASS` followed by the checks that support
that conclusion and any explicitly unverified leg.

## Historical field notes

Read [the archived pre-pr-review guidance](../../../.agent-history/skills/pre-pr-review/SKILL.md) only when a prior incident, exemplar, or host-specific rationale is relevant. It is evidence and history, not a second current procedure.
