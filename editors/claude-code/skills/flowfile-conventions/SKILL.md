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
  `vars:` entry; a repeated computation is a `functions:` entry (a `call:` is
  a whole durable run, too heavy for a one-line computation). A shape or a
  scalar with a rule is a `types:` entry, and any of these shared across files
  is a module taken with `use:`; see `flowfile-compose`.
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

## Local versus shared servers

This is the one place the rule is stated; the agents link here.

- **Act without asking** against `flow run local` and a local dev server
  (`flow server dev` on a loopback address: `localhost`, `127.0.0.1`, `[::1]`,
  or one the user said is on their own machine; any other address, or any
  doubt, is shared). Find a working target yourself, start runs, send
  signals, replay, and verify end to end, then report what you did. A probe
  that fails against an unreachable or denied endpoint is yours to repoint only
  when it is a data-free, non-mutating probe (a GET with no body, no secret or
  credential header): try a public HTTPS endpoint that the egress policy
  allows. Any request that carries a body, a secret-backed header or a
  credential, or that mutates, is stubbed instead, with a `flow test` stub or a
  local echo task; never aim it at an unrelated public service. Do not ask the user for an
  endpoint. Do not propose `--auth` or a server restart for a laptop dev
  server unless the task is about auth. "Without asking" means no extra
  question in chat: the host's permission prompt for `flow run` and similar
  commands may still appear, so proceed through it, never around it.
- **Ask first** for a shared or remote server, for anything that cannot be
  undone (deleting data, terminating production-like runs, publishing), and for
  any change that loosens policy: an egress allow list,
  `FLOWSTATE_ALLOW_LOOPBACK_EGRESS`, auth turned off. Never loosen policy
  without asking, even on a dev server.

## Before you say done

Run `flow validate <path>` (or `flow test` when a `*.test.yaml` exists) and
report the result. A Flowfile you did not validate is not finished.
