---
name: flowfile-style
description: Write and review Flowfiles in the canonical style and route language proposals to the rule that decides them.
---

# Flowfile style

The rule text lives in [`docs/STYLE.md`](../../../docs/STYLE.md). Read the relevant
rule before citing it; this skill is an index and procedure, not a second style
charter.

## When this applies

- Writing or editing a Flowfile in examples, tests, documentation, comments, or
  issue and PR sketches.
- Reviewing a diff that touches a Flowfile.
- Proposing a keyword, step property, policy key, or another spelling for a concept
  the language may already express.

## Rule index

| The question in front of you | Rule |
| --- | --- |
| Should this be a keyword at all, or a name in scope? | R1. The admission test for surface syntax |
| Should this policy surface take a key, or a predicate? | R2. One predicate language for policy |
| Two spellings both work; does one have to die? | R3. Two spellings for one meaning: one dies, and tooling leverage picks the survivor |
| Fenced `${...}` or bare, in this position? | R4. Fencing is uniform per position class |
| This expression is getting long, or repeats | R5. Decompose rather than nest, with thresholds a machine can check |
| This key parses and does nothing on one driver | R6. No key is accepted and ignored |
| Somebody wants a formatter option | R7. Canonical form has no options |
| Is this snippet fit to be shown? | R8. Shown is a subset of canonical, and the subset is enforced |
| This rule has no enforcement path | R9. The charter enforces itself or shrinks |

Anti-goals are Part IV of the same file. A proposal that hits one is refused with
that reference rather than a new parallel argument.

## Procedure

1. Read the relevant section of `docs/STYLE.md` and the current implementation.
2. Search for the repository's existing spelling before sketching a new one; cite
   the current `file:line` or state that no spelling exists.
3. Validate parser claims instead of reasoning from memory:

   ```sh
   go run ./cmd/flow validate <file>
   ```

4. Run the style linter before leaving a style review comment:

   ```sh
   go run ./cmd/flow lint <file-or-directory>
   ```

   Findings are advisory unless `--strict` is used. When a mechanical tier-4 check
   could have made the comment but did not, treat that as a tooling gap rather than
   repeatedly spending reviewer attention on it.
5. Walk the decided-spellings table for any Flowfile an author will copy. What
   validates is wider than what Flowstate should generate or teach.
6. Do not repair a recorded charter/tree disagreement inside unrelated work; keep
   the tracked migration and the current task separate.

## Editing the charter

`docs/STYLE.md` owns the rule text. Keep the rule index above synchronized with its
`### R<n>.` headings in the same change; the repository test checks that coupling.
Do not duplicate the full rules here.

## Historical field notes

Read [the archived flowfile-style guidance](../../../.agent-history/skills/flowfile-style/SKILL.md)
only when a prior incident, exemplar, or detailed rationale is relevant. It is
history and evidence, not a second current charter.
