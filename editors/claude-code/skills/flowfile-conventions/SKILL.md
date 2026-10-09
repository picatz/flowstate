---
name: flowfile-conventions
description: Use while editing a Flowstate Flowfile; canonical spellings, secret references, and CEL pitfalls. Loads on its own when a Flowfile is open.
paths:
  - "**/Flowfile"
  - "**/Flowfile.y*ml"
  - "**/workflow.y*ml"
  - "**/*.flow.y*ml"
---

# Flowfile conventions

Plugins cannot ship path-scoped rules, so this skill carries them: `paths`
loads it when a Flowfile is in play. `flowfile-author` has the loop (guide,
catalog, validate, fmt, test); this is what to get right while typing. The rule
text is in [docs/STYLE.md](https://github.com/picatz/flowstate/blob/main/docs/STYLE.md)
and [docs/DSL.md](https://github.com/picatz/flowstate/blob/main/docs/DSL.md);
read the row before arguing with it.

## Spell it the canonical way

- `${...}` fences expressions in `if:` and a loop's `items:`. Quote the whole
  value when it holds `: ` (a ternary): `'${a ? b : c}'`.
- A step's scalar output is `${steps.<id>.value}`, never `${steps.<id>}`.
- Name a computed value with a `value:` step, print with `log:`. The `cel:`,
  `echo:`, and `printf:` keys are retired.
- Three or more outcomes on one value is `switch:` with a meaningful
  `default:`, not nested ternaries or sibling `if:` steps.
- A value read more than once is a `value:` step; a constant is a workflow
  `vars:` entry; a repeated computation is a `functions:` entry.
- `timeout:` and `retry:` belong on the task step doing the work, not on
  `for_each:`, `parallel:`, `call:`, `loop:`, `switch:`, or a wait.
- Input limits: `min_len:`, `max_len:`, `min_items:`, `max_items:`; anything
  else is `must:`. `pattern:`, `min:`, `max:`, and `unique:` are retired.
- A webhook's `idempotency_key:` names the event (`${event.body.id}`), never a
  signature header; to decline a delivery use the trigger's `when:`.
- `flow fmt` output is canonical. Do not hand-format against it.

## Secrets

Write `${secret('scheme:name')}` (for example `${secret('env:API_TOKEN')}`) on
the task input that uses it, never a literal, and never in `vars:`: a secret
there is refused because it would reach durable history. The plugin's guard is
a heuristic net, not proof: it catches known token shapes and credential-named
keys only, so an edit that goes through does not show that no literal secret
was written.

## CEL pitfalls

- A field that may be absent: `x.?y.orValue(d)`, not `has(x.y) ? x.y : d`. To
  ask whether it was sent at all, `x.?y.hasValue()` or `has(x.y)`;
  `orValue(false)` cannot tell absent from false.
- Expressions are pure: no I/O, no randomness, no clock outside a wait. Anything that waits,
  retries, branches, or fans out is a step kind.
- Do not guess a function or step key. If `flow validate` rejects it, read the
  guide (`flowstate://docs/language`) instead of trying spellings.

## Before you say done

Run `flow validate <path>` (or `flow test` when a `*.test.yaml` exists) and
report the result. A Flowfile you did not validate is not finished.
