# Flowstate style

`docs/DSL.md` says what a Flowfile may contain. This document says what a good one
contains, which is a much smaller set, and it says why each choice was made so that
the reasoning survives the next argument about it.

It exists because taste that lives in a reviewer's head is taste that gets applied
unevenly and then forgotten. The charter here was drafted on
[#543](https://github.com/picatz/flowstate/issues/543) and landed in the repository
by [#646](https://github.com/picatz/flowstate/issues/646) for exactly that reason: a
rule nobody can find is a rule that gets relitigated every few months.

Audience: anyone writing a Flowfile, and anyone reviewing a proposal to change the
language. Human or agent. The agent-facing companion is
[`.claude/skills/flowfile-style`](../.claude/skills/flowfile-style/SKILL.md), which
cites this file rather than restating it (see [Keeping this document
honest](#keeping-this-document-honest)).

## Three sets, and the mistake of conflating them

- **Legal** is what the grammar and the validator accept. It is the widest set, it
  stays wide, and every corner of it is tested including the corners nobody should
  write by hand. Generators and agents produce shapes no person would type, and the
  parser must take them.
- **Canonical** is what `flow fmt` produces: one form per legal construct, no
  options, no negotiation.
- **Shown** is what `examples/`, `README.md` and `docs/DSL.md` teach. It is the
  narrowest set and the one that propagates, because people copy examples.

The intended containment is `shown ⊆ canonical ⊂ legal`. Style narrows what the
toolchain *produces* and what the documentation *teaches*. It never narrows what the
grammar accepts, because a language that refuses its own generators is a language
with a workaround culture.

Part III records where the tree does not yet satisfy that containment, measured
rather than asserted.

## The decided spellings

One obvious spelling per construct, with the reason the loser lost. Everything in
this table was checked against the tree at `c4ead7c`, and the rules the entries
descend from are in Part I.

| Construct | Write this | Not this | Why this one |
| --- | --- | --- | --- |
| Dispatch on one value with three or more outcomes | `switch:` with a `default:` that means something | a ternary tree, or sibling `if:` steps each testing the same value for equality | only the keyword lets the validator check the branches against the value's domain, which it does: an unreachable `default:` is refused by name |
| Reading a field that may be absent | `x.?y.orValue(d)` | `has(x.y) ? x.y : d` | one traversal instead of a presence test and a separate read that can drift apart; `flow fix` already performs this rewrite (`fixoptional.go:22`) |
| Asking whether a field was sent at all | `x.?y.hasValue()`, or `has(x.y)` | `x.?y.orValue(false)` | a default cannot tell "absent" from "present and false", so it collapses two answers into one |
| A fact read more than once | a `value:` step, read as `${steps.<id>.value}` | the same subexpression written out at each site | the sites drift; one of them gets edited and the file still validates |
| A constant used across steps | workflow `vars:` | repeating the literal | one place to change, and the name says what it is |
| Splicing a value into a string | `${...}` interpolation | `format()` | interpolation is the plain case; `format()` is for width, precision and positional reuse, and the two are not mechanically interconvertible (`%d` on a double truncates where `string()` does not) |
| An input constraint expressible as a bound | `min:`, `max:`, `min_len:`, `max_len:`, `min_items:`, `max_items:`, `unique:` | `must:` saying the same thing | `flow breaking` reads these structurally and can name a raised floor as a narrowing; a `must:` predicate is opaque text it must conservatively call a break |
| Any other input constraint | `must:` | a retired `pattern:` | a regex is as opaque to compatibility analysis as CEL is, which is why `pattern:` died and `min:` lives |
| Naming a computed value | a `value:` step | the retired `cel:` step | a value is not an effect, and the key named the evaluator instead of the role |
| Printing a line | `log:` | the retired `echo:` / `printf:` | the capability already existed under another name |
| Reading a step's scalar output | `${steps.<id>.value}` | a bare `${steps.<id>}` | the six characters buy uniformity in every tool that reads outputs, and this is permanent (anti-goal 7) |
| Bounding or re-attempting work | `timeout:` / `retry:` on the task step that does the work | the same keys on `for_each:`, `parallel:`, `call:`, `loop:`, `switch:`, a wait, or a `value:` | on those kinds the keys bind nothing, so the parser refuses them with a position and points at where they do work (`parse_wait.go:397`) |
| An expression in `if:`, or in a loop's `items:` | the fenced form, `${...}` | the bare form, which also parses | one spelling per position class; the fence is what tells data from code everywhere else in the file, so the fenced form is the one that reads the same way in every position |

The last row is the one place where the canonical spelling is not yet the only legal
one. `compiler.exprValue` (`pkg/flowstate/v1/flowfile/value.go:128`) documents the
fence as optional for expression-typed fields, and one shipped example takes it up
(`examples/task-shape-policy/workflow.yaml:57` writes `if: true`). R4 condemns that
third state and #545 decides how it goes away. Until then, write the fence.

## Part I: the rules

Each rule is a test a reviewer applies to a concrete proposal, and each names the
tier that enforces it. A rule that names no tier is deleted rather than kept as
prose, which is R9.

### R1. The admission test for surface syntax

A construct earns a keyword (a node kind, a step property, a policy key) only if at
least one of these holds:

1. **It causes the engine to do something.** Expressions are pure and free of I/O,
   so anything that schedules, waits, retries, branches, fans out or compensates
   cannot be an expression. `sleep:`, `undo:`, `for_each:` pass here.
2. **The word buys checks, not behavior.** `switch:` landed on exactly this ground:
   sibling `if:` steps could always express the branching, but nothing could check
   that the branches partition the value or that the default means something. The
   check is real and it fires today; see the worked example below.
3. **CEL can express it only by transcribing the engine's own bookkeeping.** If the
   author would have to re-implement state the engine already holds (retry counters,
   loop-carried state, the compensation stack), the construct belongs in the grammar.

The reviewer's procedure: **write the proposal as a CEL expression over a scope.** If
what is missing is a *name in scope*, add the name and refuse the keyword. If what is
missing is *behavior*, it is a node kind. If nothing is missing, it is a tier-4
suggestion or nothing at all.

Condition 2 is worth seeing rather than taking on faith. Given a `switch:` whose
cases already cover every value the dispatched expression can produce, the validator
refuses the `default:` by name:

```
step "settle" input "default": `default:` can never run: the cases already handle
every value `steps.outcome.value` can produce ("approved", "rejected"); remove it,
or remove the case whose value it was meant to catch
```

Nothing a chain of `if:` steps can say makes that diagnostic possible, which is the
whole argument for the keyword.

Enforcement: review, against this text. Admission cannot be machine-checked, which is
precisely why the test is written down.

### R2. One predicate language for policy

Anywhere the system answers "may this happen" (egress, secret access, task shape,
signal authorization, and every policy surface not yet built) the answer is a CEL
predicate over an explicitly injected, closed scope, evaluated fail-closed. No
boolean keywords encoding one relationship. No bespoke match-list grammars with their
own semantics for equality and membership.

The test: *could this proposed policy key be written as a clause in a predicate over
a scope the surface already has, or could name?* Then it is a clause, and the key is
refused. The next relationship somebody needs ("not the same team as the starter",
"not someone who already approved another step in this run") composes with `&&`
instead of becoming another keyword, which is the whole difference between a policy
language and a policy menu.

Enforcement: review via R1, plus the migration named in Part III for the one surface
that shipped the other way.

### R3. Two spellings for one meaning: one dies, and tooling leverage picks the survivor

When two spellings mean the same thing, one is retired at an edition boundary with a
`flow fix` rewrite. No deprecation windows: the repository's own history (`cel:`,
`echo:`, `printf:`, `iterator:`, `pattern:`, the fenced-map output shaping #533
replaced) shows the mechanism works and costs a second of somebody's time.

Exactly two exemptions, each with a test a reviewer can apply.

1. **Not mechanically interconvertible.** `flow fix` must never exercise judgment, so
   two spellings whose equivalence is a per-argument judgment both survive.
   `format()` beside interpolation is the standing example: `%d` on a double
   truncates where `string()` does not, so no rewrite is provably safe. The test:
   *could `flow fix` rewrite one into the other with byte-level confidence?* If yes,
   one dies. If no, both stay and the documentation shows the split.
2. **The redundant spelling is consumed as data by a shipped tool answering a
   question the general spelling cannot.** The declarative constraints (`min_len:`,
   `max_len:`, `min:`, `max:`, `min_items:`, `max_items:`, `unique:`) duplicate what
   `must:` can say, and they stay, because `flow breaking` reads them structurally:
   a raised floor is a narrowing it can name, while any change to a `must:`
   expression is opaque text it must conservatively treat as a break
   (`cmd/flow/breaking.go:460`). `pattern:` failed this same test, because a regex is
   as opaque to compatibility analysis as CEL is, and that is why it died while
   `min:` lives. The test: **name the shipped tool and the question it answers from
   the structure.** No tool, no second spelling.

Enforcement: review for admission; tier 3 (`flow fix` plus an edition) for retirement.

### R4. Fencing is uniform per position class

The language holds both positions in the #545 argument at once. `must:` takes bare
CEL and *refuses* a fence, because it is read when the workflow is compiled:

```
inputs.amount.must: cannot be an expression; it is read when the workflow is
compiled, so write the value out
```

while `if:` accepts the fenced and the bare form alike (`value.go:128`). Whatever
#545 decides, this rule constrains the resolution:

- It applies to **every** expression-typed field, at **one** edition boundary, with a
  `flow fix` rewrite. Never field by field as each is touched.
- There is **no optional fence.** A position either requires it or refuses it. A
  third state where both parse is two spellings for one meaning, and it fails R3 with
  no exemption available.
- `${...}` keeps interpolation regardless. The fence's job of telling data from code
  where a field can hold either is not up for renegotiation; #545 is only about
  fields that can hold nothing but code.

Enforcement: tier 1 for whichever rule each position ends up with, tier 3 for the
migration.

### R5. Decompose rather than nest, with thresholds a machine can check

- **A ternary inside a ternary** is a `switch:`, a `value:` step or a `vars:` entry.
  Mechanical shape, mechanical suggestion (tier 4).
- **A `has(x)` guard followed by a read of `x`** is an optional. Already tier 3:
  `fixoptional.go` rewrites the three exact shapes, and `has()` keeps the presence
  question, which is a different question.
- **A structurally repeated subexpression appearing three or more times** is a
  `value:` step waiting for a name. Structural identity is already implemented, as
  `exprEqual` (`pkg/flowstate/v1/flowfile/negation.go:268`), and `flow audit` already
  counts occurrences with it. Tier 4 *suggests* and never rewrites, because the
  rewrite would have to invent a name, and a rewriter that guesses names is the bug
  class `flow fix` exists never to be.
- **A chain of sibling `if:` steps that all compare one value for equality** against
  different literals is a `switch:`. Tier 4 suggests. It never fires on siblings
  whose conditions are unrelated, or that partition a complement into named cases:
  both are legal and good style.
- **A comment explaining what an expression computes** is a decomposition smell,
  where a comment explaining *why* the workflow does something is the point of
  comments. Taste, with a proxy: in a shown file, an expression that needed such a
  comment is a review rejection.

Enforcement: tier 4 for the first, third and fourth; tier 3 for the second (shipped);
review for the last.

The positive shape, compiled by this document's own test:

```yaml
edition: v2026.3
name: refund-dispatch
description: Settle a refund on the outcome a reviewer sent.
inputs:
  amount:
    type: int
    must: this > 0
steps:
  - id: review
    wait_for_signal:
      name: refund-reviewed
      timeout: 24h
      outputs:
        decision: ${payload.?decision.orValue("rejected")}
        timed_out: ${timed_out}
  # The outcome is named once, so each branch below reads a fact rather than
  # recomputing it.
  - id: outcome
    value: '${steps.review.timed_out ? "no_response" : steps.review.decision}'
  - id: settle
    switch:
      value: ${steps.outcome.value}
      cases:
        - case: approved
          steps:
            - id: pay
              log:
                message: paying the refund
        - case: rejected
          steps:
            - id: decline
              log:
                message: declining the refund
      default:
        steps:
          - id: chase
            log:
              level: warn
              message: nobody reviewed the refund before the deadline
```

Three things there are the rule rather than decoration. The wait's `outputs:` name
the two facts that a later step needs, so nothing downstream reaches into a payload.
`outcome` is a single ternary over those names rather than a nested one over raw
fields. And the `switch:` dispatches on the named value, so the `default:` catches
the one case the two `case:` arms do not, which is what makes it reachable and
therefore legal.

Not this, for the same workflow:

```yaml
# Not this: the presence test and the read can drift apart, the outcome is
# recomputed at every site that needs it, and three sibling steps test one
# value for equality where a switch would let the validator check the set.
- id: pay
  if: ${has(steps.review.payload.decision) && steps.review.payload.decision == "approved"}
  log:
    message: paying the refund
- id: decline
  if: ${has(steps.review.payload.decision) && steps.review.payload.decision == "rejected"}
  log:
    message: declining the refund
```

### R6. No key is accepted and ignored

Every key an author can write either binds to behavior on **both drivers** or is
refused with a position and a remedy. Accept-and-ignore is the worst of the three
outcomes, because it teaches the author that their `retry:` protects them when it
protects nothing. This is CLAUDE.md's "a misspelled key must be reported" rule
generalized: a key that parses and does nothing is a misspelling the parser happens
to know how to spell.

The capability rule runs in both directions. Just as a capability is not done until a
Flowfile can express it, a spelling is not *accepted* until an engine executes it, on
both drivers, with a shared case in `pkg/flowstate/v1/internal/conformance`.

Enforcement: tier 1. This rule is already load-bearing in the tree, cited by name at
`pkg/flowstate/v1/flowfile/parse_wait.go:389`.

### R7. Canonical form has no options

`flow fmt` never gains a style flag. Not line width, not quote style, not key order.
Two files that mean the same thing are byte-identical after formatting, and a
reviewer never spends a comment on layout. `gofmt`'s one transferable lesson is that
the value is the absence of the argument, and a single option destroys it.

`--check`, `--stdout` and `-o json`/`-o jsonl` are I/O plumbing rather than style,
and they stay. As of `c4ead7c` those are the only flags the command has
(`cmd/flow/fmtcmd.go:123`).

Enforcement: review of `cmd/flow/fmtcmd.go` itself, plus the standing test that
formatting is idempotent.

### R8. Shown is a subset of canonical, and the subset is enforced

Every example, every snippet in `docs/DSL.md`, every snippet in `README.md`, and
every snippet here:

- passes `flow fmt --check` byte-identically;
- produces zero tier-4 findings;
- demonstrates one thing, runs, and never shows a construct pushed past what it is
  good at.

`legal` stays wide underneath all of that.

Enforcement: a CI leg over `examples/`, `docs/DSL.md` and the README's fenced
Flowfiles, once tier 4 exists. Until then, review against this text, plus the
compile-time half that already runs: `TestREADMEWorkflowsCompile` compiles every
complete workflow shown in `README.md`, `docs/DSL.md`, `docs/ARCHITECTURE.md` and
this file.

**The byte-identical clause is not satisfied anywhere in the repository today**, and
the measurement is in Part III. It is stated here as the target because that is what
the rule is for, and recorded there as unmet because a charter that describes a tree
it has not looked at is worth nothing.

### R9. The charter enforces itself or shrinks

A rule that has named no tier within one release cycle of being written down is
deleted from this document rather than kept as prose. Prose rules decay between
sessions, and that decay is the failure mode this whole document exists to prevent.

The standing exceptions are R1's admission test and R5's taste call, which are review
rules by nature. Even those name the artifact review checks against, which is this
file, so the argument is "does the proposal pass the test" and never "what do we
think good looks like".

## Part II: the tiers

Four tiers over one idea: severity is decided by *whose problem it is*.

| Tier | Tool | Contract | Carries |
| --- | --- | --- | --- |
| 0. Measure | `flow audit` | counts, no severity, no judgment; evidence for language decisions, deliberately **not** a lint | R3's corpus evidence, R5's repeat counts |
| 1. Refuse | `flow validate` and the parser | position, problem, remedy; wrong everywhere rather than merely ugly; properties of the file only, never of a deployment | R4's fence rules, R6's no dead keys |
| 2. Normalize | `flow fmt` | one form per construct, no options, idempotent, comments preserved | R7, and the byte-level half of R8 |
| 3. Migrate | `flow fix` plus editions | byte-safe, exact-match, refuses rather than guesses, tested by bytes or by compiling the result and never by "still validates" | R3's retirements, R4's sweep, R5's guarded-read rewrite (shipped) |
| 4. Suggest | does not exist yet | warns, never blocks; every check has a mechanical shape *and* a mechanical or name-shaped replacement; a check that fires on legitimate generated output gets fixed or deleted, because a disabled lint teaches nothing | R5's ternary, repeat and dispatch checks; the tooling half of R8 |

Wrong-everywhere is tier 1. Same-meaning-two-spellings is tier 2 or tier 3.
Legal-but-there-is-a-better-idiom is tier 4 and only tier 4, because promoting a
taste rule to a refusal is how a language starts refusing its own generators.

Tier 4 is the gap, and it is the highest-leverage build in this charter. It is what
would have caught #540's stale example without a human reading it, and it is where
every future style argument gets discharged. **A style comment in review that tier 4
could have made is not a review comment. It is a missing check, and the review action
is to file it.**

## Part III: the rules applied to what shipped

A charter that only blesses the present is worthless. Everything in this section was
measured against `c4ead7c` with the commands named beside it, not recalled.

### Condemned, and worth migrating

**`distinct_from_starter:` and the `allow:` match-list grammar.** Fails R1 (a keyword
where a name in scope was missing) and R2 (a bespoke match grammar beside three CEL
policy surfaces). This is #326, and the charter's contribution is to say that the
answer follows from the rules rather than being open: signal authorization becomes a
CEL predicate over a scope holding the attested `sender`, `run.started_by` (landed by
#514) and `inputs`; `distinct_from_starter: true` rewrites mechanically to
`sender.subject != run.started_by.subject`; the migration is an edition plus `flow
fix`. Worth changing rather than grandfathering: it is three examples and one server
check today (`examples/approval-gate`, `examples/enterprise-fund-transfer`,
`examples/enterprise-access-review`, and `pkg/flowstate/v1/server`), and every month
it stands it teaches the match-list idiom to more files.

**The fencing split.** `must:` refuses a fence, `if:` accepts either. The language has
shipped both answers to #545, which means the status quo is not the conservative
position but the inconsistent one. Condemned by R4, which binds the resolution without
pre-empting #545's measurement of what YAML permits.

**R8's byte-identical clause, which nothing in the tree satisfies.** Measured by
running `flow fmt --stdout` over each shown Flowfile and comparing bytes:

| Corpus | Canonical today |
| --- | --- |
| `examples/*/workflow.yaml` (62 files) | 0 byte-identical; 2 differ only by a trailing newline; 60 differ in content |
| the one complete workflow in `README.md` | not canonical |
| the one complete workflow in `docs/DSL.md` | `flow fmt` **refuses** it, because a comment inside it cannot be carried back |

The direction of the failure is the finding. `flow fmt` today writes sequences at
zero indentation, unfolds a folded block scalar into one long line, normalizes `24h`
to `24h0m0s`, and re-quotes a single-quoted string with backslash escapes. Making the
corpus byte-canonical would therefore mean rewriting every teaching file in the
repository into a shape no teaching file currently uses, and in one case into a shape
whose comment the formatter cannot keep at all. So the gap is not that the examples
are sloppy. It is that the formatter is not yet good enough to be the canon for
hand-written teaching files, and **that is the first thing the tier-4 slice has to
resolve**, before R8's CI leg can exist.

**Nested ternaries in the shown corpus.** R5's first threshold fires on 12
expressions across 7 example files today, `examples/optional-dispatch/workflow.yaml`
included, which is the example most recently rewritten *for* style. That is not an
argument against the rule. It is the corpus the check has to be tried against before
it is turned on, and the reason the tier-4 leg lands advisory for its first 48 hours
the way every new check in this repository does.

### Grandfathered or kept, with the reason on the record

- **`format()` beside interpolation.** R3 exemption 1, and `flow fix`'s refusal to
  rewrite between them is load-bearing rather than lazy.
- **The declarative constraints beside `must:`.** R3 exemption 2, earned by `flow
  breaking`'s structural reasoning. On notice in one direction: any *future*
  declarative twin of something `must:` can say has to name its consuming tool at
  proposal time.
- **Sibling `if:` steps beside `switch:`.** Both stay legal forever. `if:` composes
  on every node kind, and a partitioned complement is good style. Canonical for
  *equality dispatch on one value* is `switch:`; tier 4 suggests and never refuses.
- **`${steps.<id>.value}` over a whole-step scalar.** Decided, permanent, and
  recorded as anti-goal 7 so it is not re-argued.
- **`has()`.** Keeps the presence question. The guarded-read idiom it enabled is
  already migrated by tier 3. Correctly split, nothing to change.

### Settled since the charter was drafted

**`retry:` and `timeout:` accepted and ignored on composites.** The charter condemned
this under R6 and asked for an interim refusal. It has landed: `checkPolicyPlacement`
(`pkg/flowstate/v1/flowfile/parse_wait.go:397`) refuses both keys with positioned
advice on every kind that schedules nothing, which is `wait`, `value`, `for_each`,
`parallel`, `call`, `loop` and `switch`. The last two were beyond the original list.
#286 is closed; whether a composite should ever *carry* those semantics is a feature
question and not this rule's business.

### Confirmed: where the language already obeyed the charter

The retirements of `cel:` (a value is not an effect), `echo:` and `printf:` (the
capability existed under another name), `pattern:` (R3 exemption 2's test applied
before it was written down), and #533's one spelling for output shaping are all
decisions these rules would have produced. That matters, because it is the evidence
that the rules describe this language's grain rather than imposing a foreign one.

## Part IV: anti-goals

What this language will not become, written down before the pressure arrives, so that
each refusal costs one link rather than one argument.

1. **No control flow in strings, ever.** A fence holds one CEL expression. No
   `${if ...}`, no `${for ...}`, no pipelines, no filters. The moment a scalar
   contains a second evaluation order, the file is a template and the bet that the
   language cannot express nondeterminism dies in the parser.
2. **No second expression language, and no evaluator names in the grammar.** `cel:`
   answered "which evaluator" when the author asked "what role does this value play".
   Nothing gets to make that mistake again, including a future "jq-flavored" or
   "regex-typed" field.
3. **No vendor names in built-ins.** The built-in registry is what every deployment
   trusts, and a product name in it is a product inside the trust boundary.
4. **No per-relationship policy booleans.** The next `distinct_from_starter:` is
   refused at review by R1 and R2, and written as a clause.
5. **No deprecation windows.** One edition, one rewriter, one spelling per concept.
   Carrying two taxes the parser, the validator, the language server, the marshaller
   and every test matrix that crosses them, for as long as the window lasts, and
   windows do not close on schedule.
6. **No options on `flow fmt`, and no style debate in review.** A layout opinion is a
   formatter change proposal. An idiom opinion is a tier-4 check proposal. Both are
   code, and neither is a comment thread.
7. **No whole-step scalar special case**, in any tool, ever. Decided with `value:`.
   The two forms are not interconvertible later, so this is permanent.
8. **No style diagnostics drawn from a deployment.** Every tier reports properties of
   the file. An editor keystroke never consults an egress policy and never resolves a
   host.
9. **No judgment in `flow fix`.** Exact-match rewrites, or refusal with a position. No
   name invention, no "probably equivalent", no reflowing an author's file beyond the
   edit that was asked for.
10. **The DSL does not become a general-purpose language.** No user-defined functions
    in the file, no recursion, no unbounded anything an outside party can grow. Reuse
    composes through `call:`, a unit with a contract resolved at compile time, rather
    than through macros or includes. Expressiveness grows in CEL's scope and in the
    standard library, where it stays pure, cost-bounded and identical on both drivers.

## Keeping this document honest

**The skill cites this document and holds only what is specific to an agent's
workflow.** That is the choice, stated here so nobody adds a second copy later. Two
alternatives were considered and rejected: generating the skill from this file, which
produces prose written for one audience and read by another, and asserting that two
differently worded documents agree, which no test can actually check.

What is checked is the part that can be. The skill carries the rule *index*, because
routing to the right rule is the thing an agent needs and the thing that goes stale
when a rule is added or renamed. `TestStyleSkillIndexMatchesTheCharter` extracts every
`### R<n>.` heading from this file and requires the skill to name each one, exactly,
with no extras. Adding a rule here and not to the skill fails; renaming one fails;
deleting one fails. The rule *text* lives here alone, so there is nothing else that
can drift.

### Conventions for the snippets in this document

- A **positive** example is a complete Flowfile, fenced ```` ```yaml ```` and opening
  with `edition:` at the margin. Every one of them is compiled and validated by
  `TestREADMEWorkflowsCompile`, which is the same harness the README and
  `docs/DSL.md` already run through.
- A **negative** example is always a fragment rather than a whole file, and it opens
  with a `# Not this:` comment. Fragments are never compiled, which is deliberate:
  some of them are legal but poor style and would pass, and others are refused
  outright, and a checker that could not tell the halves apart would either reject
  this document or teach nothing.
- `TestStyleGuideShowsBothKinds` guards both halves, so a document that lost its
  examples cannot pass by having nothing to check.

Byte-level canonicity is deliberately *not* asserted over these snippets, for the
reason measured in Part III: `flow fmt` currently rewrites or refuses every shown
Flowfile in the repository, so an assertion here would force this file alone into a
shape the rest of the corpus contradicts. When the tier-4 slice resolves that, this
paragraph and the test beside it are the first things to change.

## See also

- [`docs/DSL.md`](DSL.md): what a Flowfile may contain. This document never restates
  it.
- [`docs/reference/`](reference/): generated, and never hand-edited.
- [`.claude/skills/flowfile-style`](../.claude/skills/flowfile-style/SKILL.md): the
  agent-facing companion.
- [`CLAUDE.md`](../CLAUDE.md): how to work on this repository at all.
