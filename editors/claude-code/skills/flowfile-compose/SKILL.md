---
name: flowfile-compose
description: Use before adding a repeated expression, constant, shape, or block to a Flowstate Flowfile, or when a Flowfile repeats itself; picks vars, functions, types, a module (use:), or call:.
---

# Composing a Flowfile

Copies drift and the file still validates. Pick the smallest tool that
answers the question; the decision is in
[DSL.md, composition end to end](https://github.com/picatz/flowstate/blob/main/docs/DSL.md#composition-end-to-end-choosing-a-mechanism-and-evolving-it-safely-landed)
and [STYLE.md, the decided spellings](https://github.com/picatz/flowstate/blob/main/docs/STYLE.md#the-decided-spellings).

| The question | Write | A run sees |
| --- | --- | --- |
| A value read more than once, from steps | a `value:` step, `${steps.<id>.value}` | the value |
| A constant | workflow `vars:` | the value |
| A computation or predicate, used in several places | `functions:`, `${slug(inputs.title)}` | its body, inlined |
| A shape (record) or a scalar with a rule | `types:` (`fields:`, or `type:` and `must:` over `this`) | the base type and plain rule |
| A name for a way to fail | `errors:` and `fail:` | the error name |
| Any of the last three across files | a module, imported with `use:` | the same, inlined |
| A whole process with its own history | `call:` | a child run |

A function sees only its parameters, may call another but not itself, and is
callable in any expression, `must:`, and `allow:`.

## A module

A file with no `steps:` that declares only `types:`, `functions:`,
`errors:`, and optionally `use:` is a module: validated, formatted, and linted, never run. Importers
name its declarations through the alias, never bare. Quote a whole value that
holds `: ` (a map literal does); `flow fmt` keeps the quotes only where YAML
needs them.

```yaml
# lib/slack.yaml
edition: v2026.4
name: slack
description: Block Kit shapes the notify workflows share.
types:
  Channel:
    type: string
    must: this.startsWith("#")
functions:
  section:
    description: One mrkdwn section block.
    params:
      text: string
    returns: map(string, dyn)
    body: '${{"type": "section", "text": {"type": "mrkdwn", "text": text}}}'
```

```yaml
# notify.yaml, beside lib/
edition: v2026.4
name: notify
use:
  slack:
    path: ./lib/slack.yaml
inputs:
  channel:
    type: slack.Channel
    required: true
  title:
    type: string
    required: true
steps:
  - id: blocks
    value: ${[slack.section("*" + inputs.title + "*"), slack.section("done")]}
```

Copy a starter from
[examples/lib](https://github.com/picatz/flowstate/tree/main/examples/lib)
(`ids`, `numbers`, `errors`). The path is local; nothing is fetched.

## Pin, repin, and what changed

- `digest: sha256:<64 hex>` on a `use:` entry pins the module's bytes. A changed
  module then fails validation with `module-pin-mismatch`, naming the new
  digest. Pin a module whose change should be a decision; leave the rest
  unpinned.
- Read what changed, then adopt it with `flow fix --repin <path>...`. It
  rewrites stale pins only: it never adds a pin and never touches a matching
  one. Plain `flow fix` (the edition migration) never re-stamps a pin.
- `flow breaking --against <git ref> <paths>` compares a module's interface
  (types, function signatures, errors) and lists the importers it would break.
  It does not compare function bodies, so review a body edit yourself.

## Test a module

A `*.test.yaml` whose `workflow:` is the module states `expect.check:` for
functions and `expect.types:` for scalar types, and nothing else:

```yaml
edition: v2026.4
defaults:
  workflow: ./slack.yaml
tests:
  - name: section wraps text
    expect:
      check:
        - section("hi").text.text == "hi"
  - name: Channel needs a hash
    expect:
      types:
        Channel:
          admits: ["#ops"]
          refuses: ["ops", 3]
```

Run `flow test lib/`. `refuses:` shows the rule is not vacuous. A record type
cannot be tested this way.

## Dedupe checklist

1. Repeated expression? `flow audit <path>` counts them with line numbers.
   Same fact from steps: a `value:` step. Pure computation: a function.
2. Repeated literal? `vars:`. Repeated shape or constrained scalar? `types:`.
3. Repeated block of data (a Slack block, a payload)? A function that takes the
   varying parts and returns the map, in a module if two files need it. There
   is no template or macro for a repeated sequence of steps; that is a `call:`.
4. Used in two files? Move the declaration to a module and `use:` it.
5. `flow validate`, then `flow fmt`, then `flow test`; for a module,
   `flow breaking` before you merge.

## What stays out

The language is not general-purpose: no recursion, no closures over `inputs`
or `vars`, no generics, no private declarations, no re-exports, no bare or
wildcard import, no remote or registry source (vendor the file, then pin it),
and no values or steps in a module. Plugins evaluate no CEL. Do not work
around a missing piece with a second mechanism; say what is missing.
