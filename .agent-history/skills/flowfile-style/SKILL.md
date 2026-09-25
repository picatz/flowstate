---
name: flowfile-style
description: Write and review Flowfiles in the canonical style, and route a language proposal to the rule that decides it
---

The rules live in [`docs/STYLE.md`](../../../docs/STYLE.md). This skill does not
restate them: it says when to open that file, which rule answers the question in
front of you, and what an agent specifically gets wrong here. Read the rule before
citing it. A cited rule number with the wrong text is worse than no citation.

The governing rule: minimum receiver effort at the required fidelity; think as much
as the task deserves, publish what the recipient needs.

## When this applies

- Writing or editing a Flowfile, anywhere: `examples/`, a test fixture, a snippet in
  a comment, a file in an issue body.
- Reviewing a diff that touches one.
- Proposing a keyword, a step property, a policy key, or a second way to spell
  something the language already spells.

## The index: which rule decides what

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

Anti-goals are Part IV of the same file, numbered 1 to 10. A proposal that hits one
is refused with a link rather than an argument.

## What an agent gets wrong here specifically

1. **Writing what validates rather than what is canonical.** The validator accepts
   far more than the style guide shows, and generated-looking Flowstate is the
   dominant failure mode of an agent writing this language. Before shipping a
   Flowfile, walk the decided-spellings table in `docs/STYLE.md` and check the file
   against each row. That table exists to be walked, not admired.

2. **Reasoning from memory about what the parser accepts.** Every claim about the
   language in a PR body, an issue, or a review comment is checkable in seconds:

       go run ./cmd/flow validate <file>

   Write the scratch file, run it, quote the diagnostic. CLAUDE.md's rule about
   reading the tree rather than recalling it applies with full force to the DSL,
   because the surface moves and the recollection does not.

3. **Leaving a style opinion as a review comment.** If a tier-4 check could have made
   the comment, the comment is a missing check. File it, and say so in the review
   rather than only in your head. This is Part II's closing line and it is the one
   most often skipped.

   Tier 4 is `flow lint` now, so the first move is to run it rather than to reason
   about it:

       go run ./cmd/flow lint <file-or-directory>

   It exits 0 on every finding — the findings are advice, not refusals — and
   `--strict` is what makes one a failure. If it is silent on something you were
   about to comment on, that is the missing check to file.

4. **Proposing a spelling the repository already has.** Grep before sketching, and
   cite `file:line`. Two spellings of one concept arriving as a *new feature* is the
   expensive failure this repository has paid for repeatedly; CLAUDE.md's design
   section has the worked example.

5. **Fixing the tree while landing a rule, or the reverse.** `docs/STYLE.md` Part III
   records where the tree disagrees with the charter, and each disagreement is
   somebody's tracked issue. Recording a disagreement is in scope. Silently repairing
   it inside an unrelated diff is not.

## Reviewing a Flowfile

In order, because the cheap checks eliminate most findings:

1. Does it validate? Run it, do not read it.
2. Does it lint? `flow lint` makes R5's three mechanical findings for you, with a
   position and a remedy, which is the cheapest half of step 3 already done.
3. Walk the decided-spellings table. Each row is a yes or no about this file.
4. For anything left, name the rule that condemns it. If no rule does, it is taste
   and belongs in the comment thread only if you can say what to write instead.
5. If the file is one an author will copy (`examples/`, `README.md`, `docs/`), R8
   applies: one thing demonstrated, runnable, nothing pushed past what it is good at.

## Editing the charter itself

`docs/STYLE.md` holds the rule text. This file holds the index and nothing else that
could disagree with it, and `TestStyleSkillIndexMatchesTheCharter` enforces exactly
that: every `### R<n>.` heading in the document appears here verbatim, and nothing
here claims a rule the document does not have. Adding or renaming a rule means
editing both files in the same commit. That is the whole of the coupling, on purpose.
