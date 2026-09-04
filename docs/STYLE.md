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
| Splicing a value into a string | `${...}` interpolation | `format()` for the plain case | both spellings stay, because they are not mechanically interconvertible (`%d` on a double truncates where `string()` does not), so the split is by job: interpolation for splicing, `format()` when width, precision or positional reuse is the point |
| An input constraint expressible as a bound | `min:`, `max:`, `min_len:`, `max_len:`, `min_items:`, `max_items:`, `unique:` | `must:` saying the same thing | `flow breaking` reads these structurally and can name a raised floor as a narrowing; a `must:` predicate is opaque text it must conservatively call a break |
| Any other input constraint | `must:` | a retired `pattern:` | a regex is as opaque to compatibility analysis as CEL is, which is why `pattern:` died and `min:` lives |
| Naming a computed value | a `value:` step | the retired `cel:` step | a value is not an effect, and the key named the evaluator instead of the role |
| Printing a line | `log:` | the retired `echo:` / `printf:` | the capability already existed under another name |
| Reading a step's scalar output | `${steps.<id>.value}` | a bare `${steps.<id>}` | the six characters buy uniformity in every tool that reads outputs, and this is permanent (anti-goal 7) |
| Bounding or re-attempting work | `timeout:` / `retry:` on the task step that does the work | the same keys on `for_each:`, `parallel:`, `call:`, `loop:`, `switch:`, a wait, or a `value:` | on those kinds the keys bind nothing, so the parser refuses them with a position and points at where they do work (`parse_wait.go:397`) |
| An expression in `if:`, or in a loop's `items:` | the fenced form, `${...}` | the bare form, which also parses | one spelling per position class; the fence is what tells data from code everywhere else in the file, so the fenced form is the one that reads the same way in every position |

The last row is the one place where the canonical spelling is not yet the only legal
one. `compiler.exprValue` (`pkg/flowstate/v1/flowfile/value.go:128`) documents the
fence as optional for expression-typed fields, and it is: a step written
`if: inputs.amount > 1`, with no fence anywhere, validates. Every `if:` in
`examples/` that holds an expression writes the fence anyway, which is the corpus
agreeing with this row before it was written down. R4 condemns the third state where
both parse, and #545 decides how it goes away. Until then, write the fence.

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
   (`constraintNarrowed`). `pattern:` failed this same test, because a regex is
   as opaque to compatibility analysis as CEL is, and that is why it died while
   `min:` lives (`cmd/flow/breaking.go:475`). The test: **name the shipped tool and
   the question it answers from the structure.** No tool, no second spelling.

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

Enforcement: tier 4 for the first, third and fourth — all three shipped, as
`R5/nested-conditional`, `R5/repeated-expression` and `R5/equality-dispatch` in
`flow lint`; tier 3 for the second (shipped); review for the last.

The first check is now the real one rather than the approximation Part III measured
with: a conditional is a `_?_:_` call in the parsed expression, and an optional
traversal is not a conditional there at all, so nothing counts `?` characters. The
third and fourth carry a narrowing each that the prose above leaves implicit and a
checker cannot. A repeat is only reported where every name it reads is one a
`value:` step elsewhere could read — a loop's item, a step's own `vars:` key and
`now` inside a wait are bound where they are written, and advising an author to
hoist one of those is advice that breaks the file. A dispatch is only reported where
each condition is exactly one equality against a literal, all on one value, all
literals distinct: a guard conjoined onto the equality has nowhere to go in a
`switch:`, and two steps on one literal are two things that both happen, which a
`switch:` cannot express at all.

All three are also silent wherever no step could hold the answer. Every remedy this
rule offers names something else in the file, and two of the three name a step, so a
position evaluated before any step has run — a workflow `vars:` entry, a webhook
trigger's expressions, a signal policy's computed `subject:` — cannot take the
advice however simple the expression is. The validator is explicit about the first:
a var may read neither a step nor another var, which removes the third remedy too.
A check owing a mechanical replacement and having none owes silence instead.

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
fields. And the `switch:` dispatches on the named value, so the `default:` carries
what the two `case:` arms do not, the lapse included. That is what makes it reachable
and therefore legal: put a third `case:` in for every value the expression can
produce and the validator refuses the `default:` by name.

Not this, for the same workflow, with the wait left unshaped so that every reader
reaches into the raw payload:

```yaml
# Not this: the presence test and the read are two spellings of one question and
# can drift apart, the decision is recomputed at every site that needs it, and
# sibling steps test one value for equality where a switch would let the
# validator check the branches against the set of values it can take.
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
and they stay. As of `c4ead7c` those are the only flags the command declares
(`cmd/flow/fmtcmd.go:123`), which is the state to keep it in.

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

Enforcement: split, because the two clauses are not equally reachable today.

The zero-tier-4-findings clause is enforced. `TestShownWorkflowsAreLintClean` runs
`flow lint` over every complete workflow shown in `README.md`, `docs/DSL.md`,
`docs/ARCHITECTURE.md` and this file — the same block set
`TestREADMEWorkflowsCompile` compiles, read from one list so a document cannot be
compiled and not linted — and all of them are clean today. A snippet in prose is
invisible to `flow fix --check`, `flow test` and `flow breaking`, which is what made
it the half most worth having. Over `examples/` the same check runs as a CI leg, and
that leg is **enforcing** since #646's corpus slice: `flow lint --strict examples/`,
no `continue-on-error`. It landed advisory with 21 findings, measured below, because
a check is tried against a corpus before it is turned on; the corpus is clean now,
so the trial is over.

**The byte-identical clause is enforced, over `examples/` and over the snippets in
prose alike.** `flow fmt` writes what the corpus already spelled — short durations,
plain-first quoting, indented sequences, and the mapping spelling of a structure
whose entries hold expressions (#850) — and
`TestEveryExampleIsAlreadyWhatTheFormatterWrites` holds every workflow under
`examples/` to those bytes, alongside a second pass proving idempotence and a
`proto.Equal` proving the reformat kept the workflow each file compiles to.

The snippets in prose are held by the same two claims, in
`TestShownWorkflowsAreCanonical`, over the same list of documents the compile and
lint checks read. Every complete workflow shown is canonical today and that test's
`notYetCanonical` is empty, so a snippet is held to bytes from the day it lands.
The map stays as the way a future exemption is written down rather than left
silent — an entry asserts the shape it claims to fail in, so a fix cannot leave a
stale exemption behind — and with nothing exempt,
`TestTheCanonicalClaimFailsOnANonCanonicalSnippet` is what keeps the claim
falsifiable: it drives both failure shapes over blocks written by hand, so a green
run means the check can still fail rather than that it stopped looking.

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
| 4. Suggest | `flow lint` | warns, never blocks; every check has a mechanical shape *and* a mechanical or name-shaped replacement; a check that fires on legitimate generated output gets fixed or deleted, because a disabled lint teaches nothing | R5's ternary, repeat and dispatch checks; the tooling half of R8 |

Wrong-everywhere is tier 1. Same-meaning-two-spellings is tier 2 or tier 3.
Legal-but-there-is-a-better-idiom is tier 4 and only tier 4, because promoting a
taste rule to a refusal is how a language starts refusing its own generators.

Tier 4 was the gap, and it is the highest-leverage build in this charter. It is what
would have caught #540's stale example without a human reading it, and it is where
every future style argument gets discharged. **A style comment in review that tier 4
could have made is not a review comment. It is a missing check, and the review action
is to file it.**

`flow lint` is that tool, landed by [#646](https://github.com/picatz/flowstate/issues/646).
It carries R5's three mechanical checks and nothing else, because those are what this
table says tier 4 carries; each check's doc comment in
`pkg/flowstate/v1/flowfile/lint.go` names the rule it descends from, and every finding
names it too, so `R5/nested-conditional` is a heading to read here rather than a number
to look up somewhere. It exits zero on every finding — that is what "warns, never
blocks" means as an exit code — and `--strict` is the opt-in a corpus held to R8 uses.
`examples/` is that corpus, and its CI leg passes `--strict` since #646's corpus
slice; nothing else in the repository does, so the tier is still a suggestion
everywhere an author's own file is concerned.

Two properties of it are worth knowing before adding a fifth check. It is a *verb*
rather than a mode of `flow validate`, because a tier-4 finding travelling in the
stream a tier-1 refusal travels in is one consumer's mistake away from being a
refusal, and `Diagnostic` has no severity field to tell them apart with. And a
check that cannot say what to write instead does not land: R5's repeat check stays
silent where the expression reads a name bound where it sits, because "name this in
a `value:` step" is advice that does not compile inside a loop body or a wait's
output shaping.

## Part III: the rules applied to what shipped

A charter that only blesses the present is worthless. Everything in this section was
measured against `c4ead7c`, with the measurement named beside the claim rather than
recalled from a draft. Two claims the draft carried did not survive that: see the
commit history of this file.

### Condemned, and worth migrating

**`distinct_from_starter:` and the `allow:` match-list grammar.** Fails R1 (a keyword
where a name in scope was missing) and R2 (a bespoke match grammar beside three CEL
policy surfaces). This is #326, and the charter's contribution is to say that the
answer follows from the rules rather than being open: signal authorization becomes a
CEL predicate over a scope holding the attested `sender`, a name for the run's
starter, and `inputs`, after which `distinct_from_starter: true` is one clause and
the next relationship somebody needs is another one.

R1 also says what has to exist first, and a grep says it does not. The server already
knows who started a run, because that is what the current keyword is checked against
(`pkg/flowstate/v1/server/signalauth_internal_test.go:318`), but no `started_by` name
is exposed anywhere an author can write one: nothing in `proto/flowstate/v1/` or the
server declares it, and #514, which landed a run reading how it started, landed
`trigger.kind` rather than the starter's identity. So the missing piece is a name in
scope, exactly as R1 predicts, and the keyword was wrong the day it shipped.

Worth changing rather than grandfathering: three example workflows carry the key
today (`examples/approval-gate`, `examples/enterprise-fund-transfer`,
`examples/enterprise-access-review`), beside the server check and the CLI and MCP
surfaces that spell it, and every month it stands it teaches the match-list idiom to
more files.

**The fencing split.** `must:` refuses a fence, `if:` accepts either. The language has
shipped both answers to #545, which means the status quo is not the conservative
position but the inconsistent one. Condemned by R4, which binds the resolution without
pre-empting #545's measurement of what YAML permits.

**R8's byte-identical clause, held over the corpus and over prose, with nothing
exempt.** This was a table of which shown workflows were canonical, and deleting it
is the point rather than a tidy-up: its rows restated in prose, which somebody has
to remember to update, exactly what two tests decide on every run —
`TestEveryExampleIsAlreadyWhatTheFormatterWrites` over `examples/`, and
`TestShownWorkflowsAreCanonical` over every complete workflow shown in `README.md`,
`docs/DSL.md`, `docs/ARCHITECTURE.md` and this file. A second declaration of facts a
test already decides is the same defect as a value written down twice, and this one
failed in exactly that way: the hand-measured version stood stale for the four months
between `c4ead7c` and #850's re-measurement, describing a formatter that had moved
under it.

So those tests are the authority, and what is written here is what they cannot say:
every one of them passes today, and `notYetCanonical` — the map that names an
exemption in writing — is empty. `flowfile/showncanonical_test.go` is where to read
which workflows are covered, and it draws that list from the same place the compile
and lint checks draw it, so it cannot fall behind a document somebody adds.

The corpus half was `0 byte-identical` when this section was first written, and the
direction of that failure was the finding: `flow fmt` wrote sequences at zero
indentation, normalized `24h` to `24h0m0s`, and re-quoted a single-quoted string with
backslash escapes — so making the corpus canonical would have meant rewriting every
teaching file into a shape no teaching file used. The gap was never that the examples
were sloppy; it was that the formatter was not canon for anything.

#850 closed it in that direction rather than this one, in two passes. Three defaults
were decided against what the corpus already spelled — the shortest exact duration,
plain-then-single-quoted scalars, indented sequences — and then the fold below was
decided the same way, each pass reformatting `examples/` once against the result.

**The fold, and what closing it cost.** `compiler.composite` collapses any mapping or
sequence holding a `${...}` anywhere inside it into a single `Value_Expr` — a CEL map
literal — because that is the only way a per-key expression can be evaluated. `Marshal`
wrote that value back the one way it knew, as a fenced string on one line, so the keys
the author wrote were not in the document the formatter produced: a comment anchored to
one of them had nowhere to go, which is what refused `docs/DSL.md`'s worked example, and
the first reformat flattened 59 authored mappings and sequences into one-line literals
on the way to being canonical — 20 `json:`, 19 `query:`, 12 `fields:`, 4 `headers:`,
4 others — of which `examples/plugins/sql/transfer.yaml:82` reached 778 characters.

`Marshal` now offers the mapping spelling back as a candidate and verifies it by
re-compiling and comparing the value with `proto.Equal`, the way `scalarStyles` and
`scalarSurvives` already choose between renderings of a scalar; `unfoldedStructure`
is that chooser. All 59 sites are authored mappings and sequences again, the longest
line under `examples/` is 436 characters of format-string prose rather than 778 of
flattened structure, and the two `notYetCanonical` holdouts closed with it.

Three things keep the fenced one-line form, and each is a verification failure rather
than a rule written down twice: a key that is not a constant string, an expression
written with a macro (which cel-go cannot unparse), and an all-constant structure,
whose mapping spelling compiles to a *literal* rather than to the expression the
value holds. A shaping task's `outputs:` keeps it too, and that one is a real
distinction rather than a shortfall — a mapping there means a shaped set of names,
so unfolding one would change what the file says.

**A long expression may take a line of its own.** `value: |` or `value: >` followed
by a single `${...}` is that expression, typed as it is written — the newline the
block's default chomping appends is YAML's, not text the author wrote, so it does
not turn the value into a string. Writing `|-` says the same thing and stays
correct. A block scalar holding a fence *and* other text is still interpolation, and
`|+` keeps the newlines it was explicitly asked to keep.

**Folding is the other, smaller question, and stays parked.** `flow fmt` unfolds a
folded block scalar into a single line. Re-folding is a re-wrapping decision — a
width, and a rule for where a break may fall that no string can be corrupted by —
and it stays undecided rather than guessed at, since a formatter that folds wrongly
changes what a file says. The unfold above removed most of its motivation: the lines
that made folding look necessary were the flattened structures, not authored prose.
`README.md`'s `approval-gate` was the one shown workflow it left non-canonical, and
that was answered in the README — the `outcome:` expression is written as the single
line the formatter writes, with the prose above it still naming each piece of the
idiom.

**What tier 4 found in the shown corpus, and what it finds now.** `flow lint
examples/` reported 21 findings across 12 of the 86 files it read, measured at the
commit that landed it:

| Check | Findings on landing | Where |
| --- | --- | --- |
| `R5/nested-conditional` | 12 | 6 files; `examples/expense-approval/workflow.yaml` held five, and `examples/optional-dispatch/workflow.yaml:52` — the example most recently rewritten *for* style — was one |
| `R5/repeated-expression` | 9 | 8 files, including three of the plugin examples |
| `R5/equality-dispatch` | 0 | nothing in this corpus dispatches by sibling equality; the shapes that look like it are the partitioned complements the rule is written to stay silent on |

The earlier draft of this paragraph counted 13 nested ternaries by scanning `${...}`
spans for `?` characters and discounting optional traversals, and said in as many
words that it was an approximation. The real count was 12, and the difference is the
point: an approximation of a style rule is a number nobody can act on.

**The corpus lints clean today, and the leg is enforcing.** #646's corpus slice
resolved all 21 and flipped the CI leg in the same diff, so it cannot drift from the
tree it describes: `flow lint --strict examples/`, no `continue-on-error`, and the
identical `--strict` in `tools/gate` and `make check`.

Two of the twenty-one were the check's own defect rather than a file's, and both
were fixed in the check:

- **The `string()` a fence desugars to was counted.** Interpolation rewrites every
  `${x}` to `string(x)`, so a value spliced into three sentences read as three
  statements of a call nobody typed — and R5's remedy did not converge: hoisting it
  into a `value:` step and reading the step back leaves the file stating
  `string(steps.n.value)` three times instead. Measured, by applying that rewrite and
  watching the finding come back. `flow lint` now looks *through* a conversion:
  `string(<name>)` is nothing, `string(<computation>)` is reported as the
  computation, which does converge.
- Nothing else needed the rule bent. The remaining nineteen took the remedies R5
  already names — a second `value:` step for a three-way answer, one name for a
  repeated element or count — and each file still teaches what it taught, which the
  examples' own tests and `--coverage-required` are what check.

One thing the slice is honest about costing: a three-way answer is now two steps
wherever it appears, because the rule admits no single-expression spelling of one.
`examples/expense-approval` gained four `value:` steps and
`examples/list-comprehensions` one. That reads better in the enterprise files, where
each name is a fact a reader wanted anyway, and is a real tax in the small ones,
where the file's subject is the comprehension rather than the branching. It is
recorded here rather than argued away: if a later slice adds a table-lookup or
`switch:`-valued spelling for a small closed mapping, this is the evidence for it.

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
produces prose written for one audience and read by another, and keeping two
differently worded copies with a test that they agree, which can compare wordings and
not meanings.

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
  examples cannot pass by having nothing to check. It looks for the marker inside a
  fenced block rather than anywhere in the file, because this very paragraph names
  the marker in prose, and a search over the whole document would stay green with
  every negative example deleted.

Byte-level canonicity is deliberately *not* asserted over these snippets, for the
reason measured in Part III: `flow fmt` currently rewrites or refuses every shown
Flowfile in the repository, so an assertion here would force this file alone into a
shape the rest of the corpus contradicts. When the tier-4 slice resolves that, this
paragraph is the first thing to delete, and the assertion belongs beside the
convention it enforces, in `TestStyleGuideShowsBothKinds`.

## See also

- [`docs/DSL.md`](DSL.md): what a Flowfile may contain. This document never restates
  it.
- [`docs/reference/`](reference/): generated, and never hand-edited.
- [`.claude/skills/flowfile-style`](../.claude/skills/flowfile-style/SKILL.md): the
  agent-facing companion.
- [`CLAUDE.md`](../CLAUDE.md): how to work on this repository at all.
