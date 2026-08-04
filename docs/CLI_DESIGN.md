# The CLI/TUI design language

This is the document a future call — a new view, a new symbol, a new colour — is
checked against instead of re-argued. `docs/CLI.md` states the philosophy: two
audiences, colour as a capability, symbols not emoji. This document is the layer
under that: the actual tokens, the actual views, and which of today's surfaces
already speak them and which do not yet.

Nothing here is aspirational about the machinery. `cmd/flow/internal/ui` already
implements the token system this document describes — detection, palette, symbols,
pills — landed and tested in #164. What is aspirational is *coverage*: several
render paths in `cmd/flow` predate that package or were added beside it without
being routed through it, and section 7 names them precisely, file by file, as the
work that brings them onto the charter. Section 6 names a larger, wholly new
capability — a workflow graph — on the same terms: nothing in it exists yet, and
it is designed in layers precisely so each layer is buildable and checkable before
the next is started.

## 1. Audience tiers, detected and overridable

A command's output has exactly one reader at a time, and the three readers want
incompatible things from the same bytes:

- **Human-interactive.** A person at a terminal, watching. Wants a status findable
  before the line is read, spacing that groups related facts, and a summary —
  never the whole of what happened, because the whole of it is what `-v` is for.
- **Agent.** A program — a script calling `flow` as a tool, an MCP client, CI — that
  addresses a field by name rather than recognizing a shape. Wants structure with a
  schema behind it and nothing decorative in the way of parsing it.
- **Script.** The oldest of the three and the easiest to forget: `awk`, `grep`,
  a Makefile. Wants plain, stable, line-oriented text — not JSON, because a shell
  script does not carry a JSON parser, and not colour, because an escape sequence
  in the middle of a field is corruption, not decoration.

These are not three renderers. They are one computation — the same protobuf
message every RPC already returns — read by three different tools. See section 4.

**Detection**, in order, each step winning over the ones before it:

1. **TTY** is the default signal. `ui.Detect` asks the stream, not the process: a
   piped stdout and a terminal stderr are two different answers in one invocation,
   which is what lets `flow get x | jq` hand a script its outputs on stdout while a
   person watching still sees status on stderr.
2. **An explicit flag wins over detection.** `--output json` or `--output jsonl` is
   a caller stating which of the three they are, and asking again by checking the
   terminal after they have said so would be answering a question they did not
   ask. `-o` is available on every command that produces an answer worth
   addressing — see `addOutputFlag` — and refused, not silently ignored, on every
   command that only produces an account (`flow cancel`, `flow terminate`).
3. **`NO_COLOR` and `--no-color` win over the terminal's own colour capability**,
   folded into the same `colorprofile.Detect` computation `ui.Detect` already runs
   rather than checked separately — see `environForSurface` in `cmd/flow/output.go`.
   `--no-color` is spelled as an environment variable appended after the process's
   own, specifically so an explicit flag on *this* invocation outranks a variable
   exported for every invocation, which is the same "more specific wins" rule
   `--output` obeys.

**The invariant from #164**: every surface's machine twin is *computed once and
read twice*, never recomputed. `writeJSON` marshals the same `proto.Message` a
text renderer walks field by field; `ui.Capabilities` is detected once per stream
and every style, every symbol, and every pill downstream of it reads that one
value. A second code path that reimplements "what would the JSON say" — even one
that agrees today — is a fork that will disagree the day one side changes and the
other is not touched, which is the exact shape CLAUDE.md's "both drivers must
agree" section describes for the engine and this document asserts for output: one
value, several readers, never several computations of it.

**Verbosity** is tiered, and a tier only *adds*:

- The default is quiet on success. A run that finished cleanly says so in one
  pill-and-id line; it does not narrate every step it took to get there.
- `-v` adds detail a person can ask for — pending-activity phases, the full step
  transcript instead of the tail — and never changes what a bare invocation
  *means*. A workflow that succeeded is reported as succeeded at every verbosity;
  `-v` cannot turn a quiet success into a louder failure, because that would make
  the exit code and the prose disagree depending on a flag nobody scripting
  against the exit code thought to check.
- **The budget**: a run's success answer fits one screen. `flow get`'s default
  text form is a pill line, an optional position line, and the declared outputs —
  never the full transcript, which is why `writeStepOutputs` writes the *outputs*
  document to stdout and leaves the transcript to `-o json`, where a consumer who
  wants all of it can ask for all of it by name. `flow watch`'s live view applies
  the same budget continuously: `visibleSteps` caps the step list to what the
    terminal's own height allows and states the count it cut, rather than growing
  the screen past what a person can read at a glance.

## 2. The token system

Everything below already exists in `cmd/flow/internal/ui` (`theme.go`,
`symbols.go`). This section names it as the vocabulary the rest of the CLI is
required to use, per section 5's refusal of bespoke per-command styling.

### Semantic colours, never raw ones

A call site says `theme.Success`, `theme.Danger`, `theme.Tone(statusTone(status))`
— never a hex code, never `lipgloss.Color("2")`. `Theme` (`ui/theme.go`) exposes:

| Role      | Meaning                                             |
|-----------|------------------------------------------------------|
| `Muted`   | secondary text — a placeholder, a hint, a unit       |
| `Strong`  | emphasis within a line — the token being scanned for |
| `Accent`  | the product's own voice — a heading, an example      |
| `Success` | the outcome role for "it worked"                     |
| `Warning` | the outcome role for "working, but something's off"  |
| `Danger`  | the outcome role for "it failed"                     |
| `Info`    | the outcome role for "in progress, no valence yet"   |
| `Header`  | a table's column row                                 |

`Tone` is the four outcome roles (plus `ToneNeutral`) as a value, for the surfaces
that pick one at runtime rather than at the call site — `statusTone` in
`cmd/flow/output.go` is the single mapping from a `RunResponse.Status` to a tone,
used by `flow get`, `flow list`, and `flow watch` alike, so the three cannot
disagree about whether a `TERMINATED` run reads as a failure.

**"Pending" and "skipped"** are represented, but not as colour roles of their own —
and that is deliberate rather than an oversight this document should paper over.
`ToneNeutral` (rendered `Muted`) is the *absence* of an outcome, which is what a
pending or skipped step actually is: neither a success nor a failure, a fact with
no valence. Giving pending or skipped its own hue would imply a fifth thing can go
right or wrong, and nothing does. What *does* distinguish them is the symbol
(`○` waiting, `—` skipped, both muted) — see below — which is the right layer for
a distinction that is about *shape*, not about *outcome*.

**Light/dark** is resolved once, at `ui.NewTheme(dark, caps)`, against
`charmtone`'s paired values (`NewPalette`) — the darker half of each pair for a
light background, the lighter half for dark, because Charm's set is already built
to hold contrast against both and re-deriving that by eye is not worth doing
twice. `dark` itself comes from the OSC 11 background query (`terminalIsDark`,
memoized per process) unless `FLOWSTATE_BACKGROUND` or a `< ANSI` colour profile
settles it for free — see `ui.go`'s `settledBackground` for the cost this avoids
(4.02s → 0.02s against a pty that answers nothing).

**Below ANSI, every role loses its styling entirely — this is what the code does,
not a description of graceful weight-only degradation.** `styleIf(plain, s)`
returns a bare `lipgloss.NewStyle()` and discards `s` outright when the profile
can carry no colour (`theme.go`), so a plain-text stream gets no bold and no
faint either, not "the same word, still emphasised." That is true even of the two
roles that carry weight on an ANSI-capable stream — `Strong` and `Header` are
`Bold(true)`, but `styleIf` strips that along with everything else the moment
`plain` is true — and it was never true of the outcome roles in the first place:
`Success`, `Warning`, `Danger`, and `Info` are `Foreground`-only at every colour
depth, with no bold of their own to fall back to. So there is no intermediate
"colour is gone but weight remains" tier for any role. This is exactly why the
no-colour-alone rule in section 5 is load-bearing rather than a belt-and-braces
extra: at the bottom of the profile ladder, the word and the symbol are the
*entire* carrier of meaning, with nothing else backing them up. A future change
that wanted a real weight-only tier would be a deliberate addition to `styleIf` —
named here as an option, not assumed to exist.

### Symbols: a fixed set, non-emoji, with ASCII twins

`ui.SymbolSet` (`symbols.go`) is the complete inventory. No call site invents a
mark; a new one is added here or not added.

| Role     | Unicode | ASCII | Notes                                          |
|----------|:-------:|:-----:|-------------------------------------------------|
| Success  | `✓`     | `+`   | outcome mark                                    |
| Failure  | `✗`     | `x`   | outcome mark                                    |
| Warning  | `△`     | `!`   | outcome mark                                    |
| Waiting  | `○`     | `o`   | outcome mark — pending, not yet run             |
| Running  | `▶`     | `>`   | outcome mark — in progress                      |
| Skipped  | `—`     | `-`   | outcome mark — deliberately not run             |
| Bullet   | `•`     | `*`   | structure mark                                  |
| Arrow    | `→`     | `>`   | structure mark                                  |
| Ellipsis | `…`     | `...` | prose only — see below, never a column mark     |
| Divider  | `─`     | `-`   | fills a horizontal rule                         |

Every mark except `Ellipsis` is exactly one display column in both sets — checked
by `TestSymbolsDegradeButKeepTheirWidth` — so a table rendered with one set reads
correctly against a terminal running the other, and a resize or an
`FLOWSTATE_SYMBOLS` override never shifts a column. `Ellipsis` is the deliberate
exception: three ASCII dots cannot be one column, so it belongs in prose (`… 12
earlier steps`) and never in a position anything else is aligned against.

Unicode is selected only on positive evidence — a TTY carrying at least an ANSI
colour profile (`wantsUnicode`) — because a mark that renders as a replacement
glyph is worse than one that never had the chance to. `FLOWSTATE_SYMBOLS=unicode`
or `=ascii` overrides in either direction for the person who can see their own
terminal and the detection cannot.

**The tree-structure marks this charter's step view needs** (`│`, `└`, and their
ASCII twins `|`, `` `- ``) are not yet in `SymbolSet` — see the gap inventory,
slice 5.

### Pills vs. inline glyphs

Two ways to show a status, used for two different jobs, and mixing them up is the
single most common way a view stops reading as this product's:

- **A pill** (`Theme.Pill(tone, label)`) is a filled background carrying an
  upper-cased label — `theme.Pill(ToneDanger, "FAILED")` renders as a solid block
  a reader's eye lands on before anything else on the line. It exists for **the
  one value on a line worth finding at a glance**: a run's status opening `flow
  get`'s summary, a run's status opening `flow watch`'s live view. A line with
  three pills has none, because a filled background's whole value is that
  everything else on the screen is unfilled.
- **An inline glyph** (`symbols.Mark(tone)`) is a single character beside a label
  that already carries the meaning in words — `✓ deploy`, `✗ upload`. It is for
  **a list of things being scanned together**, where a reader's eye is meant to
  travel down a column rather than land on any one row: the step list in `flow
  watch`, a future tree view.

Rule of thumb: **pills for terminal states in summaries, glyphs inline in lists.**
A summary reports one outcome and wants it found; a list reports many and wants
them compared.

Padding: a pill is `Padding(0, 1)` — one space each side inside the fill — which
is enough for the label not to touch the fill's edge and no more, since a wider
pad only pushes whatever follows it further right for no reason.

### Spacing, indent, and tabular-numeric alignment

- **Two-space indent** for a line subordinate to the one above it — the pending-
  activity lines under `flow get`'s status line, a wrapped note's continuation
  under `flow watch`'s marked block (`watchModel.note`). Not four: the content is
  usually one sentence, and four spaces of indent for one sentence reads as a
  nesting deeper than one level.
- **A blank line separates sections, never a rule.** `flow watch`'s view puts one
  blank line before the step block and one before a warning or failure note — see
  `View()` in `watchmodel.go` — because a printed divider is one more thing a
  screen reader announces and a rule of dashes competes with `Divider` for what
  a horizontal line means.
- **Durations and counts are right-aligned and monospaced-width**, so a column of
  them lines up on the ones place rather than the tens. `roundedDuration` already
  rounds to whole seconds for exactly this reason — `1m23s` and `47s` read as a
  column; `1m23.4917s` and `47.0012s` do not, and the extra digits are a fact
  about the clock the command ran at rather than about the run. Where a view lays
  out several rows (the table view, a future tree view), the width is measured
  from the *widest* rendered value in the column before any row is emitted, the
  same way `tabwriter` already does it for `flow list` — never assumed at a fixed
  column count, because a run that took `2h14m` in a listing of ones that took
  `3s` would otherwise blow the alignment of every row after it.

## 3. The views

Four views cover every render path in the CLI today. Each is sketched exactly —
the mockups below are the byte-for-byte shape a golden test can assert against,
not an impression of one.

### Step/timeline view (`get`, `watch`)

One line per step: glyph, id, duration right-aligned, phase appended only while
running. Nesting for `call:` and loop bodies uses box-drawing guides and **never
flattens** — a step inside a loop stays visually inside it rather than being
listed alongside its parent's siblings, which is the rule the tree view is bound
to as soon as the schema can carry the shape.

**This mockup is a target, not a description of anything running today, and it
is blocked on two schema prerequisites named precisely rather than assumed
solvable at render time.** Nesting depends on slice 1/2's graph schema and
model — reusing the same spec-to-tree join the graph gets, rather than a second
implementation of it. Per-step duration and per-step terminal status (the
`✓`/`✗`/`—` on a *finished* step, not only the one currently running) depend on
slice 3's run-telemetry schema addition, because neither exists on the wire
today: see the gap inventory for exactly what `RunProgress` and `StepOutputs`
carry instead. Both are additive, per-invariant-10 schema changes, gated on
being proposed and landed before slice 9 builds this renderer — see the gap
inventory for the ordering.

```
✓ RUN  deploy-frontend                         a3f9c21e  (took 2m14s)

✓ fetch-artifact                                    3s
✓ run-checks                                       41s
▶ call: promote
  │
  ├─○ each-region                                  ...
  │  ├─✓ us-east-1                                 12s
  │  ├─✓ eu-west-1                                 14s
  │  └─▶ ap-south-1                    calling the plugin
  └─○ notify

  … 4 earlier steps
```

ASCII fallback, same information, no unicode:

```
+ RUN  deploy-frontend                         a3f9c21e  (took 2m14s)

+ fetch-artifact                                    3s
+ run-checks                                       41s
> call: promote
  |
  +- each-region                                  ...
  |  +- us-east-1                                 12s
  |  +- eu-west-1                                 14s
  |  `- ap-south-1                    calling the plugin
  `- notify

  ... 4 earlier steps
```

What each part is: the top line is a pill-equivalent for the whole run (bold
status word rather than a filled pill, because it sits above a list rather than
opening a summary paragraph — see the pill/glyph rule above), the workflow id
muted, and the elapsed-or-took time muted at the far right. Each step line below
is an inline glyph (never a pill — this is a list), the step id, and either a
right-aligned duration for a finished step or a muted phase word in the duration's
place for the one step currently running. A loop's or call's children are indented
one guide level per nesting level, matching the `path` a `RunProgress` already
carries for the *live* position; a finished loop collapses to its own line with a
duration once every iteration has reported, the same way a finished call does.

Today's `flow get` and `flow watch` render a subset of this: a pill-opened summary
line, a muted position line (`positionPath`, joined with `>` rather than drawn as
a tree), and a flat list of completed step ids with no duration and no nesting
(`stepLines` in `watchmodel.go`). The gap between that and the mockup above is
slice 9 of the work plan, blocked on slices 1–3 as stated above.

### Diagnostics view (`validate`, `fix --check`)

Already the standard the rest of the CLI is held to, and it is `path:line:col:
message`, one per line, colour mapped onto exactly two of its parts: the path is
`Muted` (context, not the point), and the word carrying the outcome — `ok`, or the
diagnostic's own message — carries the tone. Nothing else in the line is coloured,
because a diagnostic is read left to right as prose and a rainbow of a position, a
step name, and a remedy would fight the reader's eye rather than guide it.

```
workflow.yaml:14:3: step "deploy" references unknown step "depoy" — did you mean "deploy"?
workflow.yaml:22:1: "retry.max_attempts" must be at least 1
examples/basic/workflow.yaml: ok
```

The middle line above, un-styled, is exactly what a shell, an editor's problem
matcher, and a screen reader receive; the styling on the path and on `ok` is
everything colour adds. `flow fix`'s refusals (a shape it will not guess at) use
the same shape and the same rule; they are diagnostics, not a different kind of
message, which is why `refusalDiagnostics` widens them into `*v1.Diagnostic`
rather than formatting a bespoke line.

### Progress view (`flow watch`'s live shape)

```
■ RUNNING  deploy-frontend
run a3f9c21e-...
on deploy > each-region > ap-south-1
watching for 47s

△ retrying, attempt 3: connection reset (next attempt in 8s), calling the plugin

✓ fetch-artifact
✓ run-checks
… 4 earlier steps
6 step(s) done

q stops watching, not the run
```

Structurally: a pill-opened identity block (status, run id, position, elapsed —
one fact per line, muted below the pill because the pill is the one thing this
screen wants found first), a warning block for anything Temporal is retrying
(`Warning` tone, `symbols.Warning` mark, wrapped rather than truncated — the one
place this view reflows, because a failure message is the reason somebody is
looking), the step summary described above, and a footer stating what quitting
does and does not do. This is drawn today; the gap is only the step block's shape,
covered in section 3's first view and slice 9 of the gap inventory.

### List/table view (`flow list`)

```
WORKFLOW_ID                            STATUS       STARTED               FINISHED
deploy-frontend-a3f9c21e-...           COMPLETED    2026-08-03T14:02:11Z  2026-08-03T14:04:25Z
deploy-frontend-b71ac0a2-...           RUNNING      2026-08-03T14:05:00Z
deploy-frontend-c02fe991-...           FAILED       2026-08-03T13:58:02Z  2026-08-03T13:58:44Z
```

Header row in `Header` (muted, bold); each `STATUS` cell in its tone (`Theme.Tone`
via `statusTone`) — coloured text, not a pill, because this is the list the
pill/glyph rule calls out by name: many rows, compared against each other, so
nothing here should be a block the eye is meant to land on once. Columns are
tabwriter-aligned and the header is withheld until the first row is known to
exist, so a listing that fails before returning anything prints no header at all
— an empty stdout, not a header over nothing, per `docs/CLI.md`'s rule that a
header with no rows is indistinguishable from a listing that succeeded and found
none.

This view already matches the charter as specified. It is the one place today's
code and this document fully agree, which is worth stating because the other
three sections say where they do not.

## 4. Other surfaces derive from the same structure

The protobuf messages under `proto/flowstate/v1/flowstate.proto` **are** the view
model. `flow get`'s text form and `flow get -o json`'s document are two renderings
of one `GetResponse`; a web dashboard, a VS Code extension, or an MCP-served UI
rendering the same run is a third, fourth, and fifth renderer of the identical
struct — never a reason to grow a second schema. `docs/CLI.md`'s "the machine
surface" section already states the RPC-projection half of this; the point here is
that the CLI is not even privileged among the renderers. It is the first one that
existed, not the definition the others approximate.

That has a concrete consequence for this document's step/timeline view: the tree
needs fields the schema does not carry today — see the gap inventory's slices 1
and 3 — and the fix is a schema field every renderer gains at once, per the
proto-first rule in `CLAUDE.md`, rather than a CLI-only lookup that a future web
UI would have to reimplement or do without. The graph model in section 6 is
built the same way from the start, for the same reason: see 6.1.

**Where this is easy for Flowstate specifically**: the RPCs are already Connect
services with protobuf request and response types, which is exactly the shape
both of the surfaces below want to consume. Nothing about serving a second
renderer requires inventing a second contract.

### MCP Apps / UI extensions

As of the 2026-01-26 MCP extension release — folded into the base MCP
specification's extensions framework as of the 2026-07-28 spec — MCP servers can
declare interactive UI resources under a `ui://` scheme, associate them with a
tool via metadata, and have the host render them in a sandboxed iframe with
bidirectional JSON-RPC communication back to the server. It is a merge of the
community MCP-UI project and OpenAI's Apps SDK, jointly authored by Anthropic,
OpenAI, and MCP-UI, and clients including ChatGPT, Claude, Goose, and VS Code have
shipped support for it (source: the MCP Apps announcement post at
`blog.modelcontextprotocol.io/posts/2026-01-26-mcp-apps/`, and the specification
itself at `github.com/modelcontextprotocol/ext-apps`). This is confirmed as
current, shipped protocol surface, not a proposal — but the extent of client-side
support beyond the four named clients is not something this audit verified beyond
the announcement's own claim, and should be treated as unconfirmed until checked
against whatever MCP client the next slice targets.

What this makes easy for `flow mcp`: the nine RPC-projected tools already return
protojson built from the same messages this document's views render, so a `ui://`
resource for, say, a run's progress would consume `GetResponse` exactly as
`writeRun` does today — no new data path, only a new renderer subscribing to data
that already exists. The token system does not travel (an iframe is not a
terminal and has no `colorprofile` to detect), but the *tokens*-as-concept do: the
same six roles, the same rule that a status is a word before it is a colour,
translate directly into a small CSS custom-property set keyed to the same names
this document uses (`--tone-success`, `--tone-danger`, …), which is a design
decision for whichever slice builds it rather than one this document needs to
settle now.

### VS Code extension surfaces

VS Code extensions compose a `TreeView` (for a hierarchical, always-visible list —
the natural home for "runs in this namespace" or, per this document's step view,
"steps in this run") with a `Webview` opened from a tree item for a richer detail
view, which is the pattern GitLab's own CI/CD extension uses for pipelines
(source: `code.visualstudio.com/api/ux-guidelines/views` and
`code.visualstudio.com/api/ux-guidelines/webviews`; the GitLab extension as a
worked example, not something this audit inspected directly — unconfirmed beyond
the search summary describing it). Flowstate's shape maps onto this almost
directly: a `TreeView` provider backed by `flow list -o jsonl` or a direct
Connect-RPC call for the namespace's runs, expanding into the nested step tree
this document's view 1 specifies, with a `Webview` for a single run's full detail
— outputs, diagnostics, the pending-activity account — built from the identical
`GetResponse` `flow get -o json` already emits. Nothing here is implemented; it is
named because the IR and RPC model make it a thin client rather than a new
backend, which is the same argument `docs/CLI.md` makes for `flow mcp`.

## 5. What we refuse

- **Emoji.** Inconsistent width, unpredictable screen-reader behaviour, and tone
  where a fact was asked for. Already law (`docs/CLI.md`, `symbols.go`'s package
  doc); restated here because it is the rule new contributors reach for first and
  the one a design review should refuse fastest.
- **Colour as the only carrier of meaning.** A glyph and a word accompany every
  colour, always — `Tone` alone never appears without `Mark` or a label rendered
  beside it. This is what makes every view in section 3 legible with colour
  stripped, and it is checked mechanically: `TestEveryRoleSurvivesLosingItsColour`
  and `TestNoColorKeepsTheWords` assert exactly this for the token layer, and any
  new view keeps that property or is not done.
- **Decoration in machine modes.** `-o json` and `-o jsonl` carry no styling ever
  — not degraded styling, none, because a byte of ANSI in a stream a program
  indexes into by field name is corruption rather than emphasis. `writeJSON` never
  touches `Theme`.
- **Verbosity that changes semantics.** `-v` widens what is shown; it does not
  change whether a run is reported as succeeded, and it does not change the exit
  code. A flag that made "quiet" and "loud" disagree about the outcome would make
  scripting against one and reading the other two different programs.
- **Per-command bespoke styling.** No command builds a `lipgloss.Style` from a raw
  colour or invents a symbol local to itself. Every visual decision routes through
  `Theme`, `SymbolSet`, or `Pill` — precisely so that adding a token widens every
  surface built from it at once, and so that "how does a warning look" has one
  answer rather than six commands' worth of almost-the-same answers that drift
  the day one of them is touched and the other five are not.

## 6. Workflow graph visualization

Section 3's step/timeline view answers "where is this run and what has it done."
A graph answers a different question — "what *could* this workflow do, and how do
its steps relate" — and it is worth its own section rather than a fifth view
because it is not a rendering of a run at all. Its base layer is a rendering of
the *spec*: a compiled `Workflow` has a shape whether or not anything has ever run
it, and that shape is what a graph draws. A run overlays outcome onto that shape;
it does not define it. Getting that ordering right is what keeps this feature from
turning into a second, competing definition of what a step is — the exact failure
mode CLAUDE.md's "one vocabulary" and proto-first sections warn about.

The design is four layers, and the layering is the point stated explicitly rather
than left to be inferred: **each layer is independently testable, and a defect is
caught at the layer it was introduced in, before it ever reaches a screen.** A
renderer bug in the mermaid exporter cannot be a graph-model bug, because the
model has its own goldens and the exporter is tested against a fixed model value,
never against a live compile. A keybinding bug in the TUI cannot be an exporter
bug, for the same reason one level up. An agent iterating on any one layer runs
that layer's tests — seconds, no terminal, no eyeballing a screen — and only
climbs a layer once the one below is green.

### 6.1 The graph model, proto first

**Corrected from an earlier draft of this section, which proposed a hand-written
Go package as the graph's home.** That draft violated CLAUDE.md's proto-first
invariant on its own terms: this model is explicitly for CLI, MCP, and web
consumers, which is precisely the shape the invariant means by "describes things
that travel," and a type built as a Go struct first guarantees the later
migration the invariant exists to avoid — a second, competing definition of what
a graph is the moment a non-Go consumer needs one, with every field renamed by
hand to match. The graph shape is schema from the start, exactly like
`RunProgress` and `Diagnostic` already are.

`proto/flowstate/v1/flowstate.proto` gains three additive messages (new
messages, no changes to anything existing — safe under `buf breaking` the way
every other addition in this schema is):

- **`GraphNode`**: `id` (the deterministic, path-qualified identifier — see
  6.2's node-id fix below for why a bare step id is not unique), `label` (the
  authored step id, for display), `kind` (an enum: `TASK`, `FOR_EACH`,
  `PARALLEL`, `CALL`), and `depth` (the nesting level, per the depth rule
  below). What this message does **not** yet carry: a status or a duration.
  Those are run facts, and no schema field for them exists anywhere in this
  proto today — see the run-state prerequisite below, which is a fenced-off,
  separately landed addition rather than something this message grows silently.
- **`GraphEdge`**: `from`, `to` (both `GraphNode.id` values), and `kind` — an
  enum of `SEQUENCE` (this step follows that one), `CALL_EXPANSION` (this
  step's body is the callee's own graph, computed recursively from the callee's
  compiled `Workflow`), `LOOP_BODY` (this step is inside a `for_each`, once,
  regardless of how many iterations a run performed — the *spec's* shape, with
  an iteration count layered on separately if the run-state prerequisite below
  ever adds one), and `PARALLEL_BRANCH` (this step is one of several siblings
  under one parallel node rather than a sequence).
- **`Graph`**: `repeated GraphNode nodes`, `repeated GraphEdge edges`, and the
  workflow id or path the graph was built from, so a `Graph` value is
  self-describing rather than needing to be handed back to whoever produced it
  to be identified.

**Depth is a level, not a coordinate**: top-level steps are depth 0, a `call:`'s
callee is a subgraph rooted one level down, a loop or parallel body is a
subgraph one level down from the step that declares it. This is the same
never-flatten rule section 3 states for the step view, generalized: depth *is*
the zoom control, and expanding or collapsing a subgraph is moving a step
between being drawn as one summary node and being drawn as its own depth level
— never a step quietly reappearing at its parent's level, which is the
flattening #172 forbids.

**Go behaviour attaches to the generated types**, per CLAUDE.md's rule that the
shape comes from the schema and behaviour is hand-written methods on it: a
`NewGraph(spec *v1.Workflow) *v1.Graph` function, in a hand-written file beside
the rest of `pkg/flowstate/v1`'s IR-adjacent behaviour (`eval.go`,
`authority.go`) rather than in a package of its own — there is no longer a
"pure package" to justify separately once the shape itself is generated code
that any importer of `pkg/flowstate/v1` already has. `NewGraph` walks a
compiled `Workflow`'s nodes exactly the way `runForEach`/`runParallel` in
`eval.go` and the authority walk in `authority.go` already do, and produces a
`*v1.Graph` value — pure function of its input, deterministic, golden-testable
with no terminal, no bubbletea, and no server in the loop.

**The run-state overlay is out of scope for this slice, named as its own
prerequisite rather than glossed over.** An earlier draft of this section
proposed folding a run's status and duration onto `Graph`'s nodes via a second,
optional argument to the same constructor — `NewGraph(spec, run
*RunProgress)`. Checked against what the schema actually carries, that cannot be
built: `RunProgress` holds only the current top-level `step_id`, a partial
`path` into it, and a segment-local `completed_steps` count — nothing about any
*other* step's status or how long it took — and `GetResponse` for a finished run
is a oneof between `RunOutputs` (values only, no per-step status or timing) and
an `Error` (which is the whole run's outcome, not a per-step account). There is
today no schema path to "step X succeeded in 12s while step Y is still running,"
which is exactly what an overlay needs and exactly what the failed-run
post-mortem this document sketches in 6.2 requires. That telemetry — per-step
status, per-step duration, per-step terminal outcome — is therefore its own
additive schema slice (gap inventory slice 3), landed and reviewed on its own
before any overlay code is written, and every overlay-producing path in this
document (6.2's `--run` variant, 6.3's outcome colouring, section 3's
step/timeline view) is blocked on it. `NewGraph` in this slice takes a spec and
nothing else.

**This is what makes the feature programmable, not merely drawable.** An MCP
client, a VS Code extension, or a future web UI wanting "what does this
workflow look like" reads the identical `*v1.Graph` message the CLI's own
exporters read in 6.2 — the same "one structure, several renderers" argument
section 4 makes for the rest of the schema, extended to this feature from its
first line rather than arrived at after a migration.

### 6.2 Exports: `flow graph`, mermaid and dot

A new command, `flow graph <path-or-workflow-id>`, derives its output from the
model in 6.1 and from nothing else — never re-walking the spec — which is the DRY
rule stated as an architecture: one derivation from the IR, N renderers reading
its output, so a nesting rule fixed in the model is fixed for every export at
once rather than fixed once per format and drifting the second time someone
touches only one of them.

**Mermaid and dot are `--output` values, not a second flag.** An earlier draft of
this section introduced `--format mermaid` beside `--output`/`-o` in the same
document that names per-command bespoke vocabulary a thing this charter refuses
(section 5) — two flags spelling "how should this be rendered" is exactly that
kind of duplication, and `resolveOutputFormat` today reads only the `output`
flag, so `--format` would not even be wired to it. The fix is to extend what
`--output` accepts for this command specifically: `mermaid` (the default), `dot`,
and `json` (the `Graph` message itself, protojson, for a consumer building its
own renderer against the identical schema section 6.1 defines — the same
argument section 4 makes generally). `flow graph` has no sensible `text` or
`jsonl` form — a graph is one document, not a table or a stream of records — so
this is the first command whose accepted `--output` values are not the global
three. `addOutputFlag`/`resolveOutputFormat` need a small, explicit change to
support that: a per-command accepted-value list rather than the single package
global `outputFormats` every command currently validates against, so that `flow
list -o mermaid` is refused by the command that has no mermaid rendering, per
`resolveOutputFormat`'s own existing rule that a format a command does not
accept is refused rather than silently mishandled. This is a small, contained
piece of the exporter slice, named here so it is not discovered mid-implementation.

**Node ids are path-qualified, not the bare step id — verified necessary, not
assumed.** `examples/call-a-workflow/workflow.yaml` has a step named `provision`
that calls `workflows/provision-tenant.yaml`, whose own steps include one also
named `provision` — a real, checked-in file, not a hypothetical. Emitting the
bare step id as the mermaid or dot node id would merge or silently overwrite
one of those two nodes, since both would render as `provision`. The scheme:
`GraphNode.id` is the full chain of enclosing step ids from the graph's root to
this node, joined by a separator no step id can itself contain: `/`, which a
step id's own schema pattern (`Node.id`, `^[A-Za-z0-9-_]+$` in
`flowstate.proto`) excludes outright, so the join can never collide with a
step id that happens to contain the separator — there is no such step id. The
caller's `provision` node id is `provision` and the callee's is
`provision/provision`. This is deterministic (the same spec always produces the
same ids, since it is derived from position in the compiled tree, never
generated) and stable under reordering elsewhere in the file, since it is
anchored to the calling chain rather than to appearance order — which is what
keeps a diff between two exports of the same workflow a diff of what changed
rather than a diff of every id shifting. `GraphNode.label` stays the bare
authored id — `provision`, not `provision/provision` — because that is what a
person reads on the node; the qualified id is the wire identity, never the
display text.

Both mermaid and dot draw nesting (`call:` bodies, loop bodies, parallel
branches) as their own grouping construct — mermaid's `subgraph`/`end`, dot's
`subgraph cluster_<qualified-id>` (graphviz requires the `cluster_` prefix for a
subgraph to render as a bounded box) — one per depth-1 grouping the model
produced, never flattened into the parent's node list, per 6.1's depth rule.
Mermaid styles nodes with a `classDef` per semantic token (`classDef success`,
`classDef failure`, `classDef pending`, `classDef running`) applied via `class
<qualified-id> success` rather than an inline `style` line, which is what lets
mermaid's own theme directive carry light/dark instead of this exporter
hard-coding one background's contrast and being wrong on the other; dot uses
`style=filled` and a fixed colour attribute sourced from the same token names.
Both formats get golden-file tests, byte-stable: `flow graph
examples/basic/workflow.yaml -o mermaid` produces the same bytes today and after
an unrelated change, checked the way `flow fix`'s and `flow docs generate`'s own
outputs are pinned elsewhere in this repo's CI.

Both exports accept `--depth N` (draw no deeper than N, collapsing anything below
it to a single summary node reporting how many steps it contains — the same
"state what was cut" rule section 1's verbosity budget and `stepLines`'s "…N
earlier steps" already apply) and `--expand <step-id>` (draw one named subgraph's
contents regardless of depth, for "show me inside this one loop and nothing
else"). Both flags are shared vocabulary with the TUI navigator in 6.3 — depth
and expansion are the same two operations whether they are typed once on a
command line or driven interactively, which is what keeps `flow graph --depth 1`
and pressing "collapse" twice in the navigator from being two different mental
models of the same feature.

**A run-state overlay variant is designed here and blocked, not built.** `flow
graph <id> --run <run-id>` (or `--run` reading the current run the way `flow
get`'s `--run-id` does), producing the identical export with per-node status and
duration folded in — nodes styled by outcome, the form worth having for a
post-mortem: "show me the shape of this workflow, coloured by how the failed run
actually went." This is blocked on the same run-telemetry schema prerequisite
6.1 names (gap inventory slice 3) for the identical reason: there is no field
today carrying a finished run's per-step status or duration for this flag to
read. The flag and its rendering are designed now so the exporter slice does not
have to be revisited when the telemetry lands; the flag itself does not ship
until slice 3 does.

### 6.3 TUI navigator

Recommendation from the audit: **a mode of `flow watch`, not a fourth standalone
command.** `flow watch` already owns the one bubbletea program this CLI runs, the
polling loop that keeps a run's state current, and the split between a styled
live view on stderr and a plain answer on stdout that section 1's "interactive
surfaces are optional" rule requires of any TUI here. A second, separately
constructed `tea.Program` in a `flow graph --interactive` command would duplicate
all of that plumbing — the poller, the outage handling `watchmodel.go` already
gets right, the ctx-cancellation-on-quit behaviour — for no reason the audit could
find.

**Corrected from an earlier draft, which claimed the navigator builds its graph
from the same `RunProgress`/`GetResponse` the plain view already reads. Checked
against the schema, that claim does not hold**: neither message carries the
compiled `Workflow` a graph is built from (`GetResponse`'s oneof is `Error` or
`StepOutputs`, and `RunProgress` is the position summary section 6.1 already
describes — no spec anywhere in either), and no RPC in
`WorkflowService` returns a run's specification. `flow watch [workflow-id]`
today has never had the source file in hand; a run knows only its id.

Two ways to fix that, and this document picks one rather than leaving both live.
**(b) — adding an RPC that hands a compiled spec back by workflow id — is
rejected here, not merely deferred**, because a workflow spec is not a small,
low-stakes value to add a new authorized read path for: it is CLAUDE.md's own
example of what a schema type carries when it is *everything* — task inputs,
egress rules, secret references — and "add a way to read a run's full spec back
out" is exactly the kind of capability that needs its own authorization and
bounding argument (who may read whose spec, and what about it is safe to serve
to a caller who only holds a workflow id) *before* a graph feature's UI
convenience earns it a reason to exist. That argument does not belong bolted
onto this section as a side effect of wanting a nicer flag.

**(a) is the design this document takes: the navigator requires the spec as an
explicit input.** `flow watch --graph --source <path>` (mnemonic: the same word
`flow run local` and `flow compile` already use to point at a file) — `--graph`
without `--source` is refused at the flag-parsing stage, per section 1's
fail-closed principle, rather than silently drawing an empty or partial graph.
The navigator's model is `NewGraph` (6.1) applied to compiling `--source`
locally — the same compile `flow validate`/`flow compile` already perform, no
new RPC, no new server-side capability — joined against the *position* fields
`RunProgress` already provides for colouring the live step, and against nothing
further until the run-telemetry schema (gap inventory slice 3) lands. A
workflow that has never run works identically: `flow graph <path>
--interactive` is the entry point that needs no run at all, and it is the same
code path with `run` simply absent, per 6.1's "run-state overlay is optional"
design. `flow watch --graph` without `--source` on a workflow whose file the
caller does not have on disk is, honestly, not yet servable — named here as a
real limitation of choice (a) rather than hidden by choice (b)'s unresolved
authorization question.

**Keyboard**, in the vocabulary bubbletea v2 already delivers as `tea.KeyMsg`:

| Key                | Action                                              |
|--------------------|------------------------------------------------------|
| `↓` / `j`, `↑` / `k`| move the selection to the next / previous sibling    |
| `→` / `l`, `enter`  | expand the selected subgraph (`call:`, loop, parallel branch) — descend one depth level |
| `←` / `h`, `esc`    | collapse the current subgraph — ascend one depth level |
| `g`                 | jump to a step by id (opens a filter prompt over the same step ids the model already has, so a typo is refused the way an unresolved reference is refused elsewhere in this CLI) |
| `+` / `-`           | widen / narrow `--depth` for the whole tree at once, the batch form of `→`/`←` |
| `q`                 | stop watching, exactly as it does in the plain view — the footer line in section 3's mockup is unchanged |

**Mouse**, using bubbletea v2's actual event model rather than v1's — verified
against `charm.land/bubbletea/v2 v2.0.8` (the version already vendored in
`go.mod`) rather than assumed from the older API: a `tea.View` sets
`MouseMode: tea.MouseModeCellMotion` (click, release, and wheel events; drag is
covered too since v2 reports motion while a button is held) rather than a
`tea.WithMouseCellMotion()` program option the way v1 read. Events arrive as
`tea.MouseClickMsg`, `tea.MouseReleaseMsg`, and `tea.MouseWheelMsg` (each a
`tea.Mouse` under the hood, carrying `X`, `Y`, `Button`), handled either through
the model's `Update` via a type switch or through the `OnMouse` hook `tea.View`
exposes directly. Mapped as: **click** a node to select it (equivalent to
navigating there with `j`/`k`), **click a collapsed subgraph's summary node** to
expand it in place (equivalent to `enter`), and **wheel** to scroll a graph taller
than the terminal — the same `visibleSteps`-style height budget section 1's
verbosity rules require of the plain view, applied to a graph instead of a flat
list.

Every part of this view is styled entirely through section 2's tokens — the
selected node is `Strong`, an expandable-but-collapsed subgraph's summary node is
`Muted` with the count section 1's budget rule requires stated, and where outcome
colouring is available at all, it reads `Theme.Tone`/`symbols.Mark` exactly as
section 3's step view does, because this is a second shape for the same facts and
a third palette invented for it would be precisely the bespoke-per-view styling
section 5 refuses.

Mockup, a `promote` call two nodes deep, `each-region` collapsed — **this depicts
the state once slice 3's run-telemetry schema has landed; against a spec-only or
pre-slice-3 graph, the finished-step marks and durations below are not yet real
data and would not yet be drawn**:

```
flow watch --graph  deploy-frontend (a3f9c21e)             RUNNING

  ✓ fetch-artifact                                              3s
  ✓ run-checks                                                 41s
▶ ┬ call: promote                                    calling the plugin
  ├─▶ ○ each-region  (3 steps, 1 running)                       ...
  └─  notify

↑↓ move   →/enter expand   ←/esc collapse   g jump   +/- depth   q quit
```

Before slice 3, the same screen still works — the navigator draws structure
(nodes, nesting, the selection cursor) from the spec alone, per 6.1's optional
overlay — it simply has no `✓`/duration to show on a finished step yet, the
identical limitation section 3 states for the plain step/timeline view.

The selection marker (`┬`/`▶` at the line the cursor is on — ASCII fallback `+`)
is a fifth kind of mark alongside section 2's existing set, needed only here, and
belongs in `SymbolSet` beside the tree-structure marks the gap inventory's
symbol slice already adds — named here so it is not lost between the two
sections.

### 6.4 Testing at every layer — the development loop this design buys

Stated as a property, not left implicit, because it is the reason the layering in
6.1–6.3 is worth the extra indirection rather than a single TUI built straight
against a live compile:

- **Model tests** (6.1) are plain Go unit tests plus golden files, comparing a
  `*Graph` built from a fixed `*v1.Workflow` — no terminal, no bubbletea, no
  server — against a checked-in expected value. Milliseconds. This is where a
  nesting-depth bug or a mis-typed edge is caught, and it is caught before either
  exporter or the TUI is even asked to run.
- **Export tests** (6.2) are golden-file comparisons of `flow graph`'s stdout
  against checked-in `.mmd`/`.dot` files, built from the *same* fixed model
  values the 6.1 tests already validated — an exporter test never re-derives a
  model from a spec, so a failure here is unambiguously an exporter defect.
  Seconds, still no terminal.
- **TUI tests** (6.3) drive the model directly, the way `watchmodel_test.go`
  already tests the plain view — and specifically **not** via
  `teatest.RequireEqualOutput`, a byte-stream golden. An earlier draft of this
  section recommended exactly that, and it is wrong on `watchmodel_test.go`'s
  own precedent: that file deliberately avoids pinning bubbletea's emitted byte
  stream, because bubbletea coalesces and differentially repaints frames, which
  makes the literal bytes on the wire scheduler-dependent — a passing test today
  and a flaking one tomorrow with no code change, exactly the class of test
  CLAUDE.md's testing sections warn against trusting. `TestWatchViewShowsThePositionAdvancing`
  is the named example this is checked against. The mechanism the existing test
  actually uses, and the one the graph navigator's tests should copy: a `fold`
  helper (`cmd/flow/watchmodel_test.go`) that threads a model value through a
  sequence of `tea.Msg`s via `Update` — `current, _ = current.Update(msg)`,
  since a bubbletea model is a value and `Update` returns a new one each time —
  and a `viewOf` helper that renders the *resulting* model's `View().Content` as
  a plain string, asserted against directly (`require.Contains`,
  `require.Equal`) or via `require.True(t, ok)` on a type-asserted
  `tm.FinalModel(t).(watchModel)` for a test that needs the program's own event
  loop rather than a hand-folded sequence. This still uses
  `github.com/charmbracelet/x/exp/teatest/v2` (`teatest.NewTestModel`, `.Send`,
  `.Type`, `.WaitFinished`, `.FinalModel`) for the handful of tests that need a
  real running program — the graph navigator's would too, for the same reason
  `TestWatchViewShowsProgressAsItArrives` is the one test in that file that
  drives the real program rather than folding messages by hand: establishing
  that the poll loop, the renderer, and the terminal state actually agree is not
  something a folded sequence can prove on its own. What changes from the
  earlier draft is only the assertion at the end — a rendered `View()` or a
  final model's fields, never the raw escape-sequence stream. No real terminal,
  no pty — seconds, not minutes, and no flake from a scheduler this test does
  not control.
- **The capability matrix rides the existing token tests.** `NO_COLOR` and the
  ASCII-fallback assertions the graph's marks need are not a new test category:
  they are `TestNoColorKeepsTheWords` and `TestSymbolsDegradeButKeepTheirWidth`
  from `cmd/flow/internal/ui`, run once the new selection marker (6.3) and any
  new subgraph-drawing marks are added to `SymbolSet`. A mark that fails that
  matrix fails at the token layer, before a TUI test ever renders it.

The property this buys: **an agent iterating on any one layer never needs a human
eyeball to know it is right**, and a defect in a lower layer fails fast at that
layer's own tests, in the seconds a model or export test takes, rather than
surfacing forty minutes later as "the TUI draws the wrong thing" with three
layers between the symptom and the cause.

## 7. Gap inventory → sliced work plan

Ordered so each slice is buildable and checkable on its own, and each leaves the
tree green per CLAUDE.md's "leave a green stopping point."

**What is already on the charter**, from the audit in sections 2 and 3: the token
system itself (`cmd/flow/internal/ui/theme.go`, `symbols.go`, with tests in
`ui_test.go` and `background_test.go`), `flow list`'s table view, `flow
validate`'s diagnostics view, and `flow get`/`flow watch`'s pill-opened summary
line. What is not: everything below.

1. **The graph schema** (section 6.1): additive `Graph`, `GraphNode`, `GraphEdge`
   messages in `proto/flowstate/v1/flowstate.proto` — node kind and edge kind
   enums, the path-qualified `id`/display `label` split section 6.2 requires,
   `depth`. No run-state fields yet — see slice 3. `buf generate`, `buf
   breaking` (additive messages are safe by construction, verified rather than
   assumed), and schema-level tests the way `Diagnostic`'s own shape is tested.
   This is the first slice of the entire plan: proto-first per CLAUDE.md's
   invariant 1, and everything else in this section is a consumer of what it
   defines.

2. **The graph model, Go construction** (section 6.1): `NewGraph(spec
   *v1.Workflow) *v1.Graph`, a hand-written method attached to the generated
   types slice 1 produced, living beside `eval.go`/`authority.go` in
   `pkg/flowstate/v1` rather than in a package of its own. Spec-only — no run
   argument, since the run-state overlay is blocked on slice 3. Golden-tested
   against fixed `*v1.Workflow` values, no terminal, no bubbletea, no server.
   The dependency every other graph slice below sits on; nothing in 4 or 6 can
   start correctly before this one is settled and reviewed.

3. **The run-telemetry schema.** Additive fields — per-step status, per-step
   duration, per-step terminal outcome — landing wherever the schema review
   decides they belong (extending `GetResponse`'s finished-run shape, a new
   message, or something else `buf breaking` accepts as additive; this document
   deliberately does not pre-decide the exact message shape, only that the
   fields do not exist anywhere today and must be proposed and reviewed on
   their own). Verified necessary, not assumed: `RunProgress` carries only the
   current top-level step, a partial path, and a segment-local completed count;
   `GetResponse` for a finished run is a oneof between output values and an
   error, neither of which is a per-step account. This slice blocks the
   run-state overlay in 6.1/6.2/6.3 *and* the step/timeline tree's per-step
   duration in section 3 — both are named as blocked on it rather than
   re-solved independently, since it is one gap with two consumers.

4. **The mermaid and dot exporters** (section 6.2): `flow graph`, extending
   `--output`/`-o` with `mermaid` (default), `dot`, and `json` values for this
   command specifically — including the small, explicit change to
   `resolveOutputFormat`/`addOutputFlag` needed to let one command accept a
   different value set than the other three, so `flow list -o mermaid` is
   refused rather than mishandled. Reads only slice 2's model — never re-walks
   a spec. `classDef`/`style=filled` token styling, `subgraph`/`cluster_`
   nesting, `--depth`/`--expand`. The `--run` overlay variant is designed but
   does not ship until slice 3 lands. Golden-file tests, byte-stable. No TUI
   risk: this slice is exporters and golden files only.

5. **Add the tree-structure and selection symbols to `ui.SymbolSet`.** `│`, `└`,
   `├`, and the navigator's selection mark (`┬`/`▶`, section 6.3), with ASCII
   twins `|`, `` `- ``, `+-`, `+`. Needed by both the step/timeline tree (slice 9
   below) and the graph navigator (slice 6); defined once, with the same
   single-column-width guarantee `TestSymbolsDegradeButKeepTheirWidth` already
   holds the rest of the set to. File: `cmd/flow/internal/ui/symbols.go`, with a
   widened version of that test.

6. **The TUI navigator** (section 6.3): `flow watch --graph --source <path>` —
   `--source` required, refused without it per section 1's fail-closed rule,
   since no RPC returns a run's spec and this document deliberately does not
   add one (see 6.3's reasoning). The second `View()` mode on the existing
   `watchModel`, built from slice 2's model compiled from `--source` locally
   and coloured against `RunProgress`'s existing position fields; outcome
   colouring for finished steps is blocked on slice 3 the same way the exporter
   overlay is. Never a second poller or a second `tea.Program`. Keyboard first,
   then mouse (`tea.MouseModeCellMotion`, `OnMouse`, per 6.3's verified v2 API).
   Tested by driving the model through `Update` and asserting `View()` or the
   final model's fields — never a `teatest.RequireEqualOutput` byte-stream
   golden, per 6.4's correction and `watchmodel_test.go`'s own precedent. This
   is the one slice in the whole plan with real TUI risk, which is why it is
   sequenced after the schema, the model, and the exporters have already proven
   the underlying structure is right.

7. **Give `flow fix` and `flow fmt` outcome-tone parity with `flow validate`.**
   Verified against the current code, not the stale claim an earlier draft of
   this slice made: `runFmt`/`runFix` already call `newSurface` and `fmtOne`/
   `fixOne` already render every line through `theme.Muted` (`cmd/flow/fmtcmd.go`
   lines 152, 215 onward; `cmd/flow/fix.go` lines 138, 211 onward) — that
   migration is done, and an earlier draft's "route through Theme" framing was a
   no-op this rewrite removes. What remains, checked line by line: every outcome
   word in both files — a refusal, `"already formatted"`/`"already current"`, a
   changed-file report — renders in the same `Muted` regardless of whether it is
   good or bad news, unlike `runValidate`'s `ok` (`Success`) and diagnostic
   lines. File: `cmd/flow/fmtcmd.go`'s `fmtOne`, `cmd/flow/fix.go`'s `fixOne`.
   Give a refusal `theme.Danger`, a clean file `theme.Success`, and a changed
   file (found something to do, under `--check`) `theme.Warning` — the same
   word-plus-tone pairing section 2 requires elsewhere — with a golden-output
   test asserting the `NO_COLOR` form stays byte-identical to today's.

8. **Give `flow list` an outcome glyph, not only a coloured word.** Per section
   2's colour-is-never-alone rule, `STATUS` in the table today is `Theme.Tone`
   applied to the bare word (`lifecycle.go`'s `listRendering.add`) with no
   `symbols.Mark` beside it — every *other* status-bearing surface (`get`,
   `watch`) pairs a mark with the word. File: `cmd/flow/lifecycle.go`. Add a
   `SYM` or leading-glyph column ahead of `STATUS`; update the golden tests in
   `runlocal_output_test.go`'s siblings that pin this table's shape.

9. **Build the step/timeline tree renderer.** Blocked on slice 2 (nesting —
   reuse the same spec-to-tree join `NewGraph` already performs rather than a
   second implementation of it) and slice 3 (per-step duration and terminal
   status for a *finished* step, not only the one currently running — neither
   exists on the wire before that slice lands). Once both are available:
   replace `watchmodel.go`'s flat `stepLines` with the nested form from section
   3's mockup, reusing `positionPath`'s existing path-join logic for the live
   line. Files: `cmd/flow/watchmodel.go`, `cmd/flow/get.go` (for the non-live
   `flow get` equivalent), with golden tests for both the unicode and ASCII
   forms and for a loop deep enough to need two guide levels.

10. **Consider whether `flow get`'s and `flow watch`'s prose deserve the same
    audit `flow list` already passed.** Both already route status through
    `Pill` correctly; the remaining question — out of scope for this document
    to resolve, and named so it is not lost — is whether `pendingActivityLines`'
    phase sentences and `writeRunOutputs`' output list should also carry inline
    glyphs for skipped/pending step outputs, once slice 9 gives the tree a
    place to put them.

None of the above is blocking for a person reading `flow get` or `flow list`
today — both already read as one program, which is the audit's headline finding
alongside the gaps. The load-bearing order is 1 before 2 before 4 and 6 (schema,
then model, then its two consumers), 3 before the overlay portions of 4 and 6
and all of 9 (the telemetry gap has three dependents), and 5 before 6 and 9
(both need the symbols it adds). 7 and 8 depend on nothing above and can run at
any point in the sequence; 10 depends only on 9.
