---
name: comms-commit
description: Use when writing or reviewing a Flowstate commit message or squash subject; the shape is a scope prefix, a colon, and a lowercase imperative, with a body that says why, and tools/commitcheck holds it to that shape.
---

# Commit communication

Write for the future reader deciding what changed, why, and whether the commit
belongs in the history. Read the staged diff and the verification that actually
completed; do not write from the conversation's memory when the tree can answer.

## Shape

- Subject: `scope: lowercase imperative`, the scope an area of the tree
  (`engine: bound the walker's depth`), specific enough to distinguish the
  commit from its neighbors.
- Body only when the motivation, a compatibility or security choice, a
  non-obvious mechanism, or the verification cannot fit honestly in the subject.
  Explain the decision and its consequence, not a file-by-file tour of the diff
  or a diary of how the agent got there.
- Name a limitation or an unverified leg when it changes how the commit should
  be reviewed or used. Do not claim a test, review, or generated check ran unless
  its result is in hand.
- A squash message is held to the subject shape only: the pull request body
  carries the issue reference and the verification, so the message needs
  neither trailer. `tools/commitcheck` reports a subject that is not
  `scope: lowercase imperative`.

## Attribution

A trailer the host adds (`Co-Authored-By`, a session link) is permitted and
never required; `tools/commitcheck` does not read it. A `Co-Authored-By` trailer
is for crediting a second contributor, a person who paired on the change or
another agent that wrote part of it, rather than the model that happened to
serve the turn.
