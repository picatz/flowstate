# Flowstate — the whole system reviewed: form and function across every surface (2026-09-02)

> [!NOTE]
> **Internal process, not product documentation.** This file is part of
> `docs/plans/`: how agent work is dispatched here, and what past waves
> measured. Nothing in it describes Flowstate to someone using it — the map of
> the documentation that does is [docs/README.md](../README.md).

## Status and standing rule

Written 2026-09-02 against main at `40cc365` (merge of #1424), the day after
[2026-09-roadmap.md](2026-09-roadmap.md)'s week-one slate landed and the
morning the reference-model audit ([2026-09-reference-model.md](2026-09-reference-model.md),
#1421) and the type-system audit (#1459) were filed. Those two passes each
took one axis — names, and types — through the whole system. This pass takes
every axis at once and asks one question of each: is the thing beautiful in
form and function for a person and for an agent, and where it is not, what is
the one change that makes it so.

It is a step back, so it repeats nothing the two audits proved. It cites them,
adds what six fresh evidence passes found beyond them, takes the decisions the
records had left open where the tree now decides them, and sequences the
whole into a program. Read the tree and the open issues for current state,
never this file; when an item here lands, its entry moves to the document that
owns the shipped thing, per [VISION.md](../VISION.md)'s rule.

## Method, and what it cost

Six evidence agents at the Opus tier, one lead. One pass per dimension: the
language (DSL, type system, CEL profile, IR); the runtime (engine, both
drivers, server, conformance, Temporal surface, bounds); identity and policy
(OIDC/OAuth 2.1/WIF, the five CEL surfaces, secrets, audit, the threat model);
the developer surfaces (CLI, LSP, MCP, DAP, `flow test`, embedding, docs);
the extension model (protocol, SDK, six plugins, registry, isolation, supply
chain, the agentic direction); and the project's own machinery (gate, CI,
factory, codebase health, release, deployment, observability). Every agent
built `flow` from `40cc365`, probed under a scratch directory, cited
`file:line` or a reproduced invocation, and marked inference. The lead read
the primary documents and the umbrella records first, re-read every
load-bearing citation that reached a decision below, and kept the decisions.

Cost: ~1.6M subagent tokens across the six passes, ~650 tool calls, 12 minutes
of wall time each in parallel. The lead's own spend was held to reading and
synthesis; no second wave was dispatched, and the decisions below were taken
by the lead against the evidence rather than by a debate pair. Not verified by
anyone: any Temporal cluster (the sandbox has none; durable claims are from
code, `testsuite` tests and the replay corpus), a real editor window, and
`make check`.

## Where we have been

The project's own map (#336, 2026-08-08) names four corrective arcs:
engine-first, then the composition correction (`call:`, `inputs:`/`outputs:`,
`flow test`), then the identity correction (attested senders, WIF, one identity
vocabulary across the policy surfaces), then the honesty correction, "still
running" — docs verified against code, review findings verified against the
tree. The work-queue record (#641) added the ordering principle that has held
since: shipped-but-broken beats correctness debt beats structure debt beats
new surface, and design records are consulted, not scheduled. By 2026-08-22 it
recorded the queue as decision-bound rather than work-bound; #1103 indexed the
twelve owner decisions and all twelve were answered by 2026-08-27.

August's plan closed on "finished as an engine for one run, empty as a
platform for many"; the September roadmap opened on "close to finished as a
platform for one team, empty as a fabric for many" and its week-one slate —
secret admission, the plugin token, the gate's base channel, the egress grant
to every plugin, the worker audit, typed outputs, the webhook→signal bridge —
landed in one night (#1391, #1389, #1387, #1390, #1411, #1394, #1392, #1412).

Measured at the snapshot:

| | |
| --- | --- |
| Pull requests merged, all time | 803 (78 closed unmerged, 8.9%); 148 in Aug 25–31 (~21/day), 31 on Sep 1–2 |
| Issues | 247 open, 335 closed; the open set is ≈78 design records, ≈23 umbrellas, ≈22 decisions, roughly forty bugs after this week's two audits |
| Go | 628 non-test files, 920 test files; `pkg/flowstate/v1` root 54,677 lines of which 19,928 generated; `flowfile` 28,131; `cmd/flow` 31,908; six plugin modules 26,084 |
| Schema | 16 proto files, 9,743 lines; `workflow.proto` 2,117; `service.proto` 17 RPCs; plugin protocol at version 6 |
| Language | 8 node kinds, 5 roots, 2 built-in tasks, 16 plugin tasks, 12 CEL libraries in profile `2026.1`, edition `v2026.3` |
| Surfaces | 33 top-level `flow` verbs; 21 MCP tools; LSP with 7 capabilities; DAP; ~82 CI-validated examples; 54 conformance case-set files enforced two-sided by an AST walk |
| Docs | 18,699 lines under `docs/` plus the threat model; 7 generated references pinned by drift tests; a tested-complete index |
| Health probes, run by the passes at `40cc365` (not re-run by the PR that lands this file) | `go build ./...` clean, `go vet` clean, gate and agent-config tests green; 1,910 of 1,940 exported declarations under `pkg/flowstate` documented |

The velocity is real and the discard rate is low, which argues the dispatch
discipline works. What the speed cost is the subject of the next section.

## Where we stand: six findings that cut across every surface

The per-surface strong/thin tables in the September roadmap are still
accurate a day later and are not repeated. What a whole-system read adds is
that the thin edges are not scattered: they are six shapes, each visible on
several surfaces at once, and each with one remedy.

### F1. The grammar is finished; the type system stops at its joints

The language, judged as a language, is in good shape where it was designed:
one value shape (`Value`, five arms), one fence, eight node kinds, one
composition primitive resolved at compile time, effects by grammar, written
order with one opt-out, and a constitution of twelve principles that most
proposals can be checked against without argument. The corpus is written to
teach choice, not syntax — `examples/enum-input`'s header explains *why*
`type: enum` beats `string` + `must:` — and the diagnostics name the fix.

The type story holds exactly where #1459 said it does and breaks exactly at
the joints, all reproduced again at `40cc365`. Three vocabularies say "what
shape is this" — `InputDeclaration.Type` (now shared by `OutputDeclaration`),
`TaskField.type` as prose, and `*cel.Type` — joined by four hand-written
switches (`constraintCELType`, `declaredTypeOfCEL`, `inputTypeOf`,
`scalarTypeName`), and a dead fourth (`Value.Type`, zero non-generated readers)
that is now decidable. The consequences are the ones an author meets first:
`type: list` cannot say a list of what; `type: struct` says nothing about its
keys; a timestamp output is a raw `Any` in the run document; `${1 == 1.0}` is
refused at validate while `${response.json.count + 1}` validates and fails at
run; `0777` is 511. Two decoders in one binary disagree about the same bytes
(`--input-file` uses `UseNumber` and lets the declaration decide;
`json_parse` and the http task's `parseJSONResponse` decode into `any`), so
whether a 64-bit id survives depends on which door the JSON came through. The
canonical reconciler example spends six steps testing "is this a whole
number" because of it (`examples/deployment-reconciler/workflow.yaml:78-98`).

Two things the audits did not have. **`must:` is pinned to the build's
profile, not the run's**: `constraints.go:163` resolves
`ProfileLibraries(CurrentProfile)` while every other evaluation site resolves
`scope.GetProfile()`. Unobservable while one profile exists; the day a second
ships, an input constraint in a spec pinned to `2026.1` evaluates under
`2026.2`, a breach of "one dialect per file, pinned per run" on the submit
path. And the IR has **no validation entry point for a `Workflow` message**:
`ValidateRequest` takes `files[].source` only, and the `Run` path applies
protovalidate, size, depth, plugin pins, capabilities and input binding but
none of `flowfile`'s semantic rules (`server/server.go:1631-1660`). That is
#1430 restated from the other side: a Go builder or an agent emitting proto
gets a materially weaker gate than a Flowfile author, which is the single
largest obstacle to a second frontend and a live fail-open at a trust
boundary.

### F2. Built, tested, reachable from nothing — and green when it should not be

A class of defect this repository's own verification culture created: a
capability implemented and unit-tested that no configuration, flag, or caller
reaches. Found on four surfaces this pass:

- The RFC 8693 **delegated exchange** (`auth/exchange_oauth.go:130-138`,
  `:584-585`, arrow correctly inverted per §2.1) has no `FederationPolicy`
  field and no `cmd/flow` path — S8's client half exists as a library
  capability with no operator surface.
- `ProtectedResource.Digest()`/`.Revision()` have zero non-test callers; the
  `Flowstate-Policy-Revision` header is served only when a config field no
  flag sets is non-zero; the two trace attributes
  `AUTHORIZATION_FRESHNESS.md` names do not exist. The document describes an
  enforcement half an operator cannot reach (#1380, understated).
- `netpolicy.WithControlPlane` (#1381) and `Issuer.Rotate`/`RevokeKey`
  (#1019): the same shape, already filed.

And its mirror, green-when-wrong, reproduced on the test layer: a stub whose
`returns:` the task cannot produce passes (#1295); a misspelled signal name is
delivered into the void and the case passes (#1443); `flow test` has no
plugin flag at all, so `make check` runs every shipped plugin example's suite
blind (#1294) — and that is how four of sixteen first-party plugin tasks
shipped unable to return a non-empty result (`git.log`, `git.ls_remote`,
`vcs.log`, `vcs.diff`; #1456 lists three). The coverage gap is narrower than this pass
first wrote it: `reachable_test.go`'s "the run reaches the plugin process"
subtest and `TestPluginTaskEndToEndRespectsTenantBoundary` do execute a plugin
through its real process. What no test covers is any of the four broken tasks,
and what nothing checks at all is a plugin's outputs against its declared
`output_message` (Codex, on #1479; #1476 narrowed to what survives).

### F3. One mechanism, many copies

Invariant 2 is honored at the runtime and violated one layer up, on every
axis the passes looked at:

| Concept | The one source | The copies |
| --- | --- | --- |
| The reference model | `v1.Scope`, one bucket per root, both drivers | eighteen hand-kept lists (#1437): the LSP offers two roots, `taskrun.go:671` omits `trigger`, `celReservedIdentifiers` exists twice with different contents |
| The type vocabulary | none yet | three spellings, four switches (#1452) |
| Proto → outputs | none yet | two bridges with different coverage, one nulls silently (#1440) |
| The plugin substrate | `sdk` | `installEgressPolicy` ×5, `resolveSecret` ×3, `isNetworkUnavailable` ×3, `packBoundedStorer` ×2, and `requestTimeout` as a `Duration` in two modules and an `int` of seconds in a third (#1333) |
| The caller identity | one meaning (#565) | three registered CEL native types (`netpolicy.Identity`, `v1.taskPolicyIdentity`, `auth.callerIdentity`) |
| The step address | none | five surfaces spell "which step, how nested" five ways (#1439) |
| "One executor" | ~40 shared `v1.*` rule functions | two tree-walkers (`eval.go`, `engine/execute.go`); the `StepExecutor` type ARCHITECTURE.md names does not exist. AGENTS.md never names it: its invariants 2 and 3 are already accurate as written |

The one duplication in the tree that has a guard is the step-key vocabulary
(`grammarStepKeys` vs `stepPropertyKeys`, held by `TestEveryGrammarKeyIsReserved`).
That is the pattern: where a second copy cannot be deleted, a test that
enumerates both. Where it can, delete it.

### F4. The agent is the product's second-class user

The README says "for humans and agents alike"; the tree is closer to that than
most, and the gap is measurable. `tools/list` is 757,619 bytes across 21 tools,
95.7% of it three tools that embed the whole `WorkflowSpec` schema (#1288,
up 6% since filed). `prompts/list` is empty — the six-step authoring loop is
prose in CLI.md, in a document no host can surface. There is no tool that
answers what a name means at a position (#1438), none that renders a
workflow's own signature (#1455) — `flowstate_run` advertises `inputs` as
`additionalProperties: object`, so an agent learns argument names from a
refusal — none that applies the machine-applicable `edits` a diagnostic
already carries, and no `flowstate_fix`, while three diagnostics tell the
agent to run `flow fix`. `flowstate_validate` takes base64 where
`flowstate_run_local` takes YAML (#1290); one malformed stdin line kills the
session (#1289); a composed workflow cannot be validated from bytes at all
(#1376, with the best refusal sentence in the system). The generated
`diagnostics.md`, written so "a program can decide what a failure is", is not
an MCP resource. Meanwhile the human at a terminal has `flow docs generate`
and raw roff from `flow man` (#426): each audience is better served by the
other's documentation system.

### F5. Governance reads truer than it runs

The September roadmap's P1 closed most of its list — the egress grant reaches
every first-party plugin, the worker audits its decisions, secrets cannot be
literals at admission. What still reads as governance and behaves as advice,
each confirmed at `40cc365`: `scopes_supported` is published and no code reads
a token's scopes (#1014 — an authorization server told that list mints a
narrowed token this deployment admits identically to an unnarrowed one);
task-shape rules see `task` and `identity` and no `inputs` (#1378), and a
deny keyed on a plugin's task name fails open on a rename (#1450); pinning is
per-name opt-in with no completion switch, so a new binary in a pinned
directory launches (#1327), and the digest to pin is printed nowhere a person
looks (#1326); `--audit-required` means two different things on the two sides
of the plugin socket (#1399); no CEL policy surface can be tested against
cases — there is no `flow policy` verb; and two manifest string lists that
decide which inputs are credentials are never cross-checked against the
descriptor's fields, so a typo in `required_secret_inputs` lets the real
field accept a literal into durable history (recorded as a "known limitation"
in PLUGINS.md and owned by nothing). The `--egress-policy` flag reads as
confinement and is a grant to a cooperating process; the one plugin that runs
an AI agent does not consume it, and the confine column of #721 has no code
behind it anywhere in the tree.

### F6. The factory's own controls are advisory

The gate is one computation for three tiers, the both-drivers rule and the
generated-derived rule are enforced structurally, the deep tier files its own
issues, and the agent-configuration layers are tested for drift. Around that
core, three controls the documents describe as mechanisms are conventions:
`plan` and `verdict` are required by no ruleset (#489, fired twice on
record), the deep tier publishes working fuzz reproducers while SECURITY.md
forbids reporters from doing the same and private vulnerability reporting is
off (#965). Two more are unfiled: the six plugin modules — carrying
go-git, pgx, sqlite, go-github and an OpenAI client — are scanned by neither
`govulncheck` nor `staticcheck` anywhere, because every invocation runs
`./...` from the root; and the factory's receiver-cost numbers are the only
hand-counted metrics in a repository that computes everything else, which is
how the ledger stopped for ~900 PRs without anything noticing.

### The surfaces, one line each

| Surface | Strong | The one change |
| --- | --- | --- |
| **DSL / IR** | constitution, one fence, editions with a rewriter, typed inputs and outputs | one `Type` message with a CEL type-expression spelling (`list(string)`, `map(string, int)`, `timestamp`), and one numeric model, as one edition |
| **CEL** | one evaluator, one profile per run, cost-bounded | pin the remaining eight libraries; `must:` reads the run's profile |
| **Runtime** | shared rules, structural two-sided conformance, bounds on every path but one | weigh `parallel:` blocks before dispatch; then `call: execution: child` |
| **Server** | 17 RPCs, holds no run state, refuses indistinguishably | run the semantic checks on a submitted `Workflow` |
| **Identity** | RFC-precise, fail-closed, one identity vocabulary | enforce the scopes it publishes; reach the delegation it built |
| **Secrets** | reference until the activity, closure-held, structurally redacted audit | nothing; hold the line |
| **Plugins** | process boundary, host-resolved secrets, pins, grant | one total proto→outputs bridge, outputs verified against the manifest, `flow test` sees plugins |
| **CLI** | a written contract, held (exit codes, streams, `-o json`) | a directory form for `run local`, a workflow inventory, SARIF/JUnit |
| **LSP** | derived task shape, diagnostics with edits, test-file support | the five roots from `v1`'s constants; then semantic tokens |
| **MCP** | descriptor-derived roster, six server-less tools, resources | a 20 KB `tools/list`, prompts, a signature tool, a fix tool |
| **flow test** | fixtures, tables, coverage, seeds, virtual time | load-time refusal of every name it can already check |
| **Debugger / DAP** | scope, inspect, conditional breakpoints, replay | a read-only account of a finished durable run |
| **Docs** | generated references pinned, a tested index | `flow docs` renders what the binary carries; a `values.md`; a `scope.md` |
| **Process** | one gate, three tiers, drift-tested skills | the ruleset; reproducers off the public repo; scan the plugin modules |

## Decisions taken

The owner's standing instruction (2026-09-01, [orchestration.md](orchestration.md))
is that decisions, sequencing and the merge of a green reviewed PR are the
lead's; what stays with the owner is the materially irreversible or externally
consequential act. Every decision below was taken against the evidence above
and the invariants, names the shape refused, and names what would reopen it.
Each is recorded on its issue in one line so a builder is dispatched from the
record, never from this file. The project is super-alpha and the owner has
said so twice this week: where a break buys coherence, the break is taken,
with `flow fix` carrying files across it and the compiled contract untouched.

**D1. One type vocabulary: a house `flowstate.v1.Type` message, spelled in
the Flowfile as a CEL type expression.** (#1452; amended 2026-09-02 on the
owner's steer, from a bracket spelling) The message has six kinds — `scalar`
(string, int, double, bool, bytes, timestamp, duration, `null_type` — not
`uint`,
which D2 decides is not a Flowfile type and which a plugin's unsigned
descriptor field binds as a range-constrained `int`), `list` of a `Type`, `map` with string keys and a value `Type`, `enum` as the
existing closed string set, `message` by full name reserved for #177, and
`dyn`. The Flowfile spells it the way CEL already spells its own types:
`type: list(string)`, `type: map(string, int)`, `type: timestamp`. cel-go's
`Type.String()` prints exactly that form in every checker error an author
already reads, so any other spelling is R3's two-for-one-meaning. A type
expression is parsed by the one CEL parser and checked and evaluated in a
*type environment* distinct from every run environment: there `list` and
`map` are functions from types to types (the precedent is CEL's own `type`,
an identifier and a function at once); `timestamp`, `duration` and `dyn` are
declared identifiers, since the standard environment names them only as
functions; `enum` is a bare identifier whose closed set stays in the sibling
`values:`, where per-value documentation lives; and the result is a `Type`
message at compile time. Verified on stock cel-go v0.31 in this session:
`list(string)`, `map(string, list(int))` and `list(list(bytes))` compile and
evaluate with two function declarations and no parser change, and `list(1)`
and `foo` are check errors with a column — the diagnostic the LSP wants,
from the parser it already has. The wire is the message, so the CLI, server,
LSP and MCP never parse a string; the house printer prints the CEL spelling,
and one property test holds print-then-parse to the identity. Equality with
cel-go's own `Type.String()` is asserted per kind rather than blanket,
because two spellings are deliberate translations and the test should say so:
the null type is `null_type`, cel-go's name and the one `celcheck.go:462,490`
already parses out of checker errors, because bare `null` is the null
*literal* and could never be a type identifier (Codex, on #1479); and
`timestamp`/`duration` are the house's brief names for types cel-go prints as
`google.protobuf.Timestamp` and `google.protobuf.Duration`, declared as
identifiers in the type environment because the standard environment gives
those words to conversion functions. *The rule
that keeps it honest:* a type value never enters a run environment. CEL
erases parameters at run time — `type(xs)` on a list of strings is
`list(dyn)` — so `type(inputs.tags) == list(string)` would be false and read
as a bug; `list(…)` and `map(…)` are declared in the type environment and
nowhere else. One derivation per source (a declaration, a descriptor, a
checked expression) and one projection per consumer (the checker, `must:`,
the catalog rendering, JSON Schema, `flow breaking`); the four switches are
deleted, not joined by a fifth. *Refused:* `google.api.expr.v1alpha1.Type`,
which the IR's `ParsedExpr` already imports and which cel-go converts
natively — it carries seven kinds the DSL must refuse (function, type param,
wrapper, abstract, error, type, any) and lacks the one it needs (a closed
string set), so adopting it is a subset-with-refusals, a second vocabulary
in disguise; adding values to the enum, which can never say `list(string)`;
and the bracket spelling `list[string]` this decision first chose — a third
spelling beside CEL's and Go's, and `[` is a YAML flow indicator, so
`{ type: list[string] }` is a parse error where `{ type: list(string) }`
is not. *Also refused:* keeping `list` and `struct` as bare spellings — R3
says two spellings for one meaning, one dies; `flow fix` rewrites `list` →
`list(dyn)` and `struct` → `map(string, dyn)` at the edition boundary, which
loses nothing because today's `struct` is an open map with no fields. The
enum field itself is *not* reserved at the edition boundary: the `Type`
message takes a new field number and both are carried until a drain or a
documented migration retires the old one, because `RunState.workflow` puts
every declaration into durable history — see the retirement list for the
whole argument.
*One YAML corner:* inside a flow mapping the comma in `map(string, int)`
splits the value into two keys, which the decoder refuses as the unknown key
`int)`; #1466 gains the rule that names the quoted form and `flow fix`
writes it. All 441 type declarations in `examples/` are block style.
*Points forward:* in this spelling a closed record is sayable only by name,
`list(Customer)`, which is how CEL names a message type — that is #177's
`types:` block, recorded there, and explicitly not this edition. *Reopens
if:* cel-go publishes a stable type proto with an enum kind, or #177 lands
descriptors-in-spec and the `message` arm turns out to want the whole
descriptor rather than a name.

**D2. One numeric model, decided by the declaration and one decoder.**
(#1432, #1436, #1462) An integral JSON number decodes to `int` and a
fractional one to `double`, everywhere in the binary — `--input-file`,
`json_parse`, the http task's response, a plugin's outputs — through one
`UseNumber` path; an integral value past `int64` is refused at the boundary
it arrived through, naming the value. Profile `2026.2` enables cross-type
numeric comparison so `1 == 1.0` type-checks and evaluates the same way. A
YAML plain scalar past `int64` is a compile diagnostic; `uint` is not a
Flowfile type — a plugin's unsigned descriptor field renders and binds as
`int` with its range stated as a constraint, and a value past `int64` is
refused at the bridge. Timestamps and durations are declarable scalars and
travel in the run document as RFC 3339 and Go duration strings, which the
generated values reference states. `optional.none()` reaching an output is a
validate-time refusal. Plain-scalar typing follows YAML 1.2 core with one
documented exception (`0o777` for octal; `0777` is refused as ambiguous).
This is edition `v2026.4` and profile `2026.2` together — the first real use
of the machinery the third round built for exactly this. *Refused:* leaving
"decided by the digits" in place with a workaround in the corpus. *Reopens
if:* the corpus shows a real workload that needs `uint` semantics rather than
a range.

**D3. The reference model is derived from one table.** (#1437) A root
registry and two functions — what a node contributes after it, what it binds
inside itself — exported from `pkg/flowstate/v1`, consumed by both drivers'
scope construction, the validator, the LSP's `refScope`, `flow fix`, `flow
lint`, the debugger, flowtest and `flow task run`; the reserved-word lists in
one package pinned once against cel-go; a test that fails when a surface
declares a list of its own. #1425 is decided inside it: a `switch:` arm or a
nested `parallel:` inside a branch **merges** its ids out, as a top-level
`switch:` already does — the validator wins, one recursive `MergedStepIDs`
replaces four loops, and the refusal option (the runtime wins, one-line
narrowing) is refused because it makes the language smaller for a reason
nobody argued. #1428 is decided inside it too: the five bare system names
(`error`, `item`, the wait shaping names, `event`) stay bare because they are
lexically local like `now`, and the compiler **refuses** an author-chosen name
that shadows one in the scope where it is bound — the standing-guard rule
`now` already pays, not an edition sweep that roots what nobody misreads.

**D4. `call:` gains an execution mode; the child mode is the first at-scale
slice, and Update follows it.** (#133, #272) `execution: child` as a sibling
key on the call step, default `inline` so no file changes meaning. The four
correctness requirements #133's third comment already enumerated are
non-negotiable in the implementation: `PARENT_CLOSE_POLICY_REQUEST_CANCEL`
with `WaitForCancellation` (the SDK's `TERMINATE` default skips `undo:`);
the callee compensates as a unit ordered at the call's structural position;
the engine writes the tenant, signal-policy and starter memos into
`ChildWorkflowOptions.Memo` explicitly; the parent waits on the child
future's own result-bearing `Get` before returning. That last one is stated
here against what #133's third comment says, and the correction matters:
`GetChildWorkflowExecution().Get()` resolves when the child has *started*, so
a parent that waited on it would return with the child still running, and
`REQUEST_CANCEL` would then cancel the very work `undo:` is ordered around
(Codex, on #1479). Update, with
update-with-start in the same decision, follows — it is an absence a signal
plus a poll routes around, where the inline call is a ceiling nothing routes
around. *Refused:* Nexus ahead of either (design note only), and a literal
`workflow:` node kind beside `call:` (a second spelling of composition).

**D5. `workflow.SideEffect` is a recorded no.** (#908) A marker per
condition reintroduces the history cost `cel:` was retired for, and markers do
not survive Continue-As-New, so it cannot fix the one seam pinning does not
cover. The "Leaning into Temporal" table gains the row as a refusal with that
sentence.

**D6. A `parallel:` block is weighed before dispatch, over the whole atomic
segment.** `CheckAtomicBlockActivities` runs on `for_each` only; the schema
admits 100 branches × 100 steps, and 10,000 activities in one atomic segment
were reproduced at `40cc365` — ~70,000 history events against the 51,200 cap,
the termination-that-skips-compensation ending `atomicblock.go` exists to
prevent. #771 called a branch list "author-shaped"; `call:` expansion to
100,000 nodes has made that premise weaker than when it was written. The
bound is static, pre-dispatch, over the enclosing segment (nested `for_each`
blocks inside a branch count toward one total), identical sentence on both
drivers, ceiling-exact case in conformance. *Refused:* lowering the schema's
per-list bound (crude, and it breaks files that would have fit).

**D7. A whole-run timeout carries `ErrorKindRunTimeout` on both drivers.**
(#1310) The local driver's objection — a classification claims the driver
knows why a workload it stopped went wrong — does not survive the fact that
it does know: it stopped it. One conformance case pins the kind.

**D8. A Flowfile names a callee by a path relative to itself, and nothing
else.** (#1447) No alias syntax; the module story is paths, a lockfile
carrying the transitive digest set (#1448), a shared types document expanded
at compile (#637's reference form is `./path.yaml#Name`), and a **bundle**
for byte surfaces. That bundle is a prerequisite, not an existing capability:
`ValidateRequest` takes `files[]` but processes them as independent files
rather than a root and its dependencies, and `CompileRequest` takes exactly
one required `file` (`service.proto:1280-1285`), so a composed workflow is
unresolvable over Compile and its MCP projection today. The additive
root-and-bundle request shape plus resolver wiring lands before or with the
callee work (Codex, on #1479). The resolver contract (#1376) is that a bundle
names a root and resolves relative paths inside it, and a monorepo `lib/` above the calling file is reached only
by a person-passed flag, never repository content. R1 decides it: an alias
buys nothing at the call site a path does not, for a reader or a generator.
*Reopens if:* a remote module registry resolves names to bytes, which is
explicitly not now (VISION's "do not foreclose" paragraph stands).

**D9. The test format is a schema type, and the decided path is being
executed.** (#923) Path B was answered by the owner on 2026-08-23 and step 1
landed (#1179, #1185, #1193); this pass found one report calling it
unanswered, which is the cost of a decision living in a comment. Step 2 — the
`TestSuite` message swap — is sequenced after #1273, per the owner's
2026-08-30 note, and the load-time name checks (#1441, #1443, #1295) land on
the message, not on the interim structs.

**D10. Plugins: the SDK converts, the host verifies, the launch is a message.**
(#1456, #1440, #1393) One total proto→`Node.Outputs` bridge in the schema
layer, called by built-ins and the SDK, with a table test over every
`protoreflect.Kind` × {singular, repeated, map, nested, `Timestamp`,
`Duration`}, no silent `null`, enums by name — so `git`/`vcs` keep their typed
schemas rather than retreating to `any` (which would make "the schema is the
contract" false in writing). The host checks a plugin's returned outputs
against its declared `output_message` and refuses a mismatch, and the SDK and
host both cross-check the manifest's input-name lists against the input
descriptor's fields, fail-closed. Launch inputs become a post-handshake
`LaunchRequest` message (protocol 7, the last version a grant change costs),
because `netpolicy.ParseConfig` is strict and every new grant key is
otherwise a protocol break — three of six versions were spent that way in
48 hours. `Registry.Register` refuses a duplicate and enforces the grammar;
`Replace` is explicit.

**D11. The agentic loop advances by one input, not one platform.** (#1343,
#200) `codex.exec` gains a `base_ref` that materializes the working context at
a named sha through the git plugin's governed go-git fetch, turning
`agentic-fix`'s `max_iterations: 1` into a real loop; the workspace substrate
(#200) follows on evidence from that loop, not before it. The next plugin
after the substrate gates (#1333, #1393) is the **MCP bridge** (#108) — an MCP
server as a task provider is one plugin that makes every MCP server a
capability, and it is exactly the consumer that needs D10's total bridge
first. `container.run` waits for the confine column it needs (#721). *Refused:*
#1344's container-run-first order, on the isolation-model dependency it
names itself.

**D12. Governance: the submit boundary runs the semantic checks; scopes are
enforced where the bindings already are; the freshness document says what is
enforced.** (#1430, #1014, #1380) A `ValidateSpec(*Workflow)` — the
position-free subset of `flowfile.Validate` — runs at `validateSpecification`
and `RunWithInputs`, which is also the gate a second frontend needs. Scope
enforcement lives at the per-RPC and per-tool action bindings that already
exist (`authorization.go:24`), with `scope=` in the 403 from one challenge
builder; `scopes_supported` stays published, because unpublishing breaks the
authorization servers already configured against it for the week the
enforcement takes. `AUTHORIZATION_FRESHNESS.md` is rewritten to the checks
that exist, the `--protected-resource-revision` flag lands as the one piece
that was a flag away, and the descriptor-identity comparison is queued behind
scope enforcement because both live at the same decision point. The
delegation proto shape (#567 D2) is the one decision in this dimension left
where it is: it is the nearly-irreversible move the track itself flagged, and
the recommendation on the issue stands for the owner.

**D13. The process controls stop being conventions.** The gate declines the
wide leg by name under the no-scope fallback (#1388); `govulncheck` and
`staticcheck` walk
the plugin modules, added to `ciDecisions` so `verdict` sees them; the deep
tier stops filing reproducers in public issues (#965, #1187) — but *not* into
a run artifact, which is what this pass first wrote and what Codex refuted on
#1479: this repository is public, so an `actions/upload-artifact` upload is
downloadable by anyone with read access, which is everyone. `deep.yml:180`
already tells readers the opposite ("visible to collaborators only"), a live
misstatement in a security-relevant workflow that is fixable ahead of the home
decision. What is actually private is a draft security advisory or
access-controlled storage; #965 stays the owner's, and this narrows what it
chooses between; #1386 closes on the ledger's own
Wave 2 and 3 entries; #1103 closes as an index in favour of the
`kind/decision` label. The two repository settings — the ruleset and private
vulnerability reporting — stay the owner's, verified unapplied 2026-08-31, and
this pass changes nothing about their urgency except to say it again.

## The program

Sequenced so each wave's first PR is the one that makes the next wave's
claims checkable. Sizes: S under a day, M one to three days, L a week or
more. One owner per package per wave, merge order decided up front, at most
two full gates concurrently, targeted package tests otherwise and PR CI as the
gate — the Wave 2 rules.

### Wave A — bugs both drivers agree on, and the reach fixes (this week)

Everything here is S or M, reproduced, and lands with the negative-direction
test.

1. **#1426** sort the two input loops and the wait-shaping binding loop
   (`nodes.go:527`, `call.go:51`, `wait.go:462`) — the only live
   nondeterminism in replay-path code. *S*
2. **#1422** assign `Trigger` in `CallScope`; **#1449** the
   `NamespaceReachCases` set that stops the next `Scope` field repeating it. *M*
3. **#1299** read the describe response `Cancel` already holds. *S*
4. **#1457** the enum switch reads declared inputs. *S*
5. **#1442** the `[]byte` arm and a round-trip table. *S*
6. **#1358** pin the eight libraries. *S*
7. **#1294** `--plugin-catalog` on `flow test`, refusing a stub or step naming
   a task nothing knows. *M* — alone in this wave: D9 sequences the three
   load-time name checks (**#1441**, **#1443**, **#1295**) onto the `TestSuite`
   message after #1273, so scheduling them here would build them against the
   interim structs D9 rejects. They move to the wave that lands #1273's swap,
   as one set of three (Codex, on #1479).
8. **#1288** an opaque spec field in the three tools' schemas, with a
   byte-bound test. *M*
9. **#1434 + #1463** the five roots from `v1`'s constants in completion and
   hover. *M*
10. **#1289**, **#1290** the stdio session survives a bad line; one input
    convention. *S*
11. **#1327**, **#1326** `--require-plugin-pins`; digests in the human output. *S*
12. **#1450** load-time warning on a deny-only task policy; **#1341** the
    concurrency sentence. *S*
13. The doc-truth items this pass found (one PR): ARCHITECTURE.md's "one
    `StepExecutor`" (lines 72, 81, 151, 718, 721) names a type that does not
    exist, and becomes a description of the mechanism that does — one set of
    shared rules, two drivers walking them, proved equal by a shared corpus.
    AGENTS.md is *not* in this rewrite: it never names `StepExecutor`, and
    invariants 2 and 3 stay exactly as written, since relaxing an invariant to
    match an implementation is the failure mode rather than the fix (Codex,
    on #1479); STYLE.md's
    decided-spellings row that still names `min:`/`max:`/`unique:` as
    canonical after DSL.md retired them; THREAT_MODEL's stale line cites and
    §5's heading; DSL.md's missing fifteenth round from the type audit's
    branch. *S* — and **#1382** closes, its sweep having landed in #1402.

### Wave B — the type system, as one edition (weeks 2–3)

D1 and D2 as one coherent change: `flowstate.v1.Type`, the CEL type-expression
spelling and its type environment, one decoder, profile `2026.2`, edition `v2026.4`, `flow fix` across
the boundary, the generated `values.md` (#1453) whose every cell is a
conformance case, and then the projections that were waiting — `env.Check`
with declared types (#1383, the slot-type half first), the workflow signature
as JSON Schema on `flow describe` and an MCP tool (#1455), `flow breaking`
seeing a narrowing structurally. #1445 (`|` vs `|-`) and #1458 (a bare read
of an optional input is a diagnostic naming the `.?` rewrite) ride the same
edition. This is the wave that turns "a workflow has a typed, semver-able
signature an agent can call blind" from a sentence into a projection. *L,
one owner for the schema, a second for the compiler, a third for the
projections; the schema PR lands alone.*

### Wave C — the reference model derived once, and the loops that read it (weeks 3–4)

D3: the registry (#1437), the merged-ids rule (#1425), the shadowing refusal
(#1428), one identifier grammar at every declaration position (#1427) with
the schema patterns tightened on `Node.id`, `vars` keys and `Signal.outputs`,
one step-address grammar (#1439), `unresolved-reference` that says where a
name lives (#1433), the `scope.md` reference and the `flow scope`/MCP tool
(#1438), rename and references on the `flow fix` engine (#1435), and
semantic tokens so `${…}` is highlighted in every editor without a
tree-sitter grammar (#135). The agent loop's cheap wins ride here: two MCP
prompts from text that exists, `diagnostics.md` as a resource, a
`flowstate_fix` tool as a third caller of one rewriter, a workflow inventory
on all three surfaces over one walker. *M and L, serial on `flowfile/` and
`lsp/`.*

### Wave D — plugins: one bridge, verified outputs, a launch message (weeks 2–4, parallel to B on disjoint files)

D10 in order: the total bridge with end-to-end execution tests for the four
broken tasks (#1440 + #1456 together, L); the host output check and the
manifest cross-check (M, security-adjacent, the `flowstate-security-review`
pass); `plugintoolkit` extraction and the `requestTimeout` unit fix (#1333,
M); the `LaunchRequest` message (#1393, L, protocol 7); `Register` soundness
(#1431, S). Then D11's `base_ref` (M) and the #108 bridge spike (L, after the
total bridge).

### Wave E — governance as true as it reads (weeks 2–4, parallel)

D12: `ValidateSpec` at submit (#1430, M); scope enforcement (#1014, M/L);
the freshness rewrite plus the revision flag (#1380, M + S); `inputs` in the
task-policy activation (#1378, M); a `flow policy test` verb over the
existing policy activations (new, M); the delegated exchange reaching
`FederationPolicy` (new, M); #1170's assurance projection (M); the
plugin-process audit dial-back in the same protocol bump as #1393 (#1399, L).

### Wave F — scale (weeks 4–6)

D4: `call: execution: child` (L, `testsuite`-testable, no DSL beyond one
key) — on *both* drivers, since invariant 3 makes a mode that only the
Temporal driver honors a defect rather than a slice: `eval.go` runs the
callee under the same result, cancellation and compensation semantics, and a
shared conformance case set proves the two agree, without which
`flow run local` would rehearse a mode it does not have (Codex, on #1479); then Update with update-with-start (L); D6's `parallel:` bound (M);
D7's timeout kind (S); the structured compensation record on `RunState`,
`Get` and the timeline (#1385, L) — which is what remains of the
durable-account idea once #1478 is withdrawn, since `GetTimeline` already
reads history and `flow timeline` and the MCP tool already render it, so the
work is a new field on an existing answer rather than a new reader;
search-attribute pushdown with a two-configuration equivalence test
(#1384, L).

### Wave G — the factory (this week, S items, one builder)

D13 in full: #1388, plugin-module scanning in `ciDecisions`, #965's
disposition once the owner picks the reproducer home — which #1187 now waits
behind rather than leading, since D13 refuses the run-artifact route and the
deep tier's existing upload plus its "visible to collaborators only" sentence
are what that decision has to correct, #1386 and #1103 closed, the AGENTS.md line routing
capacity questions to `tools/fleet` (#1307), #1311's toolchain sentence, the
CDP wall-clock wait replaced with a poll (#1324), and one computed metric:
bot findings accepted versus refuted per merged PR, read from the API into
the ledger by a tool rather than by hand.

### Not now, and why

Nexus in either direction (D4 sequences it behind child mode and Update);
`container.run` and the confine column (#721 first, on its own design
round); WASM (#242, refused for now on its own record); remote plugin
distribution and signing (#1325 decides whether there is anything to sign);
the entity loop beyond design (#105); the model-provider family (#192)
ahead of the MCP bridge; a `flow` context/profile mechanism (#371, wanted,
but every fleet ergonomic and none of the correctness items waits on it);
splitting `pkg/flowstate/v1` (the safe seam is horizontal — `eval.go`,
`celenv.go`, `protoliterals.go` as a `celcore` the root re-exports — and the
unsafe one is by driver, which is how a second executor is born; #528 owns
the call and nothing in this program forces it); and the first tagged release,
which #1216 rehearses and whose flip is the owner's, sequenced after the two
settings and #965 because a tag while the intake path is dark is the wrong
order.

## Remove, retire, refactor

Named explicitly, because a greenfield project deletes rather than deprecates
and the tree has been good at that (`cel:`, `echo:`, `printf:`, `pattern:`,
`min:`, `max:`, `unique:`, `iterator:`, `libs:` are all gone, not carried).

- **Delete** `Value.Type` (#1285): zero readers, and the live vocabulary now
  has two consumers of the other enum.
- **Retire** `InputDeclaration.Type`'s enum behind the `Type` message —
  *additively*, and this is the one place the delete-rather-than-deprecate
  posture does not apply. `RunState.workflow` is a whole `Workflow`
  (`run.proto`), so every declaration is serialized into durable Temporal
  history, which is what makes a run self-describing under invariant 10;
  `proto/buf.yaml` declares `FILE` and `WIRE`, and CI runs `buf breaking`
  against `origin/main`. Replacing field 2 with a message and reserving the
  number is therefore a wire break that CI would fail, and worse would strand
  an in-flight run: a worker on the new schema decoding an old history would
  drop the enum into unknown fields and lose the declaration. So the `Type`
  message takes a *new* field number, both are carried while any history can
  hold the old one, the compiler writes only the new field, and the enum's
  numbers are reserved in a later change gated on a drain or a documented
  migration — not in the edition PR (Codex, on #1479). The Flowfile spelling
  still changes in one edition; the wire does not.
- **Retire** bare `type: list` and `type: struct`; `flow fix` writes
  `list(dyn)` and `map(string, dyn)`.
- **Retire** `json_parse` for `json.parse` (#1454, R3); `flow fix` rewrites it.
- **Delete** the four type switches, the eighteen reference-model lists, the
  two proto→outputs bridges (replaced by one), `sdk/values.go:274-423`, the
  five copies of `installEgressPolicy` and their siblings.
- **Delete** `internal/schemaifacepilot` once #1268 has taken what it needs
  (#1269).
- **Retire** the environment-variable launch channel behind `LaunchRequest`.
- **Rewrite** `AUTHORIZATION_FRESHNESS.md` to the enforced subset; move the
  rest to VISION under its own rule.
- **Close** #1382 (landed), #1386 (the ledger is alive), #1103 (the label is
  the index), and re-title #580 to its residue.
- **Fix in place**, not delete: the `flow fix` refusal that sends a `v2026.1`
  typo to "a newer flow", the `run local` directory refusal written in bounds
  terms, `flow man`'s raw roff, and the two YAML traps that `flow lint` should
  name (`|` vs `|-`; a fence inside flow style).

## The issue slate

**Filed by this review** (each dup-checked against the open queue and this
week's audits, cross-linked into the record that constrains it; numbers are
real):

- #1464 — `parallel:` has no atomic-block bound; 10,000 activities in
  one segment reproduced (D6).
- #1465 — `must:` evaluates under the build's profile, not the run's.
- #1466 — `flow lint` names neither YAML trap: a `|` block scalar that
  turns a whole-value expression into a string, and a fence inside flow style
  that fails in goccy's voice.
- #1467 — the RFC 8693 delegated exchange is built and unreachable from
  any configuration.
- #1468 — no CEL policy surface can be tested against cases; a
  `flow policy test` / `explain` verb over the existing activations.
- #1469 — `flow mcp` serves no prompts, no `diagnostics` resource, and
  no `fix` tool; the agent's own documentation and repair loop.
- #1470 — nothing on any surface answers "what workflows are in this
  tree": a CLI verb, `workspace/symbol`, and an MCP tool over one walker.
- #1471 — `flow validate` speaks no SARIF and `flow test` no JUnit,
  while `Diagnostic` already carries everything both need.
- #1472 — no watch mode on `test`, `validate` or `fmt`.
- #1473 — two refusals in the wrong voice: `run local` on a directory,
  and `flow fix` on a `v`-typo of the oldest edition.
- #1474 — `govulncheck` and `staticcheck` never scan the six plugin
  modules.
- #1476 — the host never checks a plugin's outputs against its
  declared `output_message`; narrowed after review to drop its false
  no-end-to-end-coverage half.
- #1477 — manifest input-name lists are never cross-checked against
  the descriptor's fields, so a `required_secret_inputs` typo fails open.

**Advanced by comment, not re-filed:** #1426 (the `wait.go:462` sibling),
#1456 (`git.ls_remote` is the fourth), #1380 (the revision flag and the
non-existent trace attributes), #1429 (a gate cannot record which claim
admitted it), #1340 (`lint`, `schedule`, `dap` take no plugin flags), #135
(semantic tokens as the cheap route), #1216 (a changelog mechanism is a
seventh criterion), #1011/#548 (three CEL identity types to converge).

**Filed and then withdrawn, by this PR's own review:** #1475 (the gate
already prints `appearance: NOT VERIFIED locally (… absent)` and counts it on
the verdict line — `tools/gate/main.go:524-530`, `:1112`) and #1478
(`GetTimeline` reads `GetWorkflowHistory` at `server/timeline.go:232`,
`TimelineEntry` carries step, attempt, timer, signal and ending kinds, and the
surface is reachable from `flow timeline` and from the `GetTimeline` MCP tool
at `cmd/flow/internal/mcp/mcp.go:634`). Both premises came from a subagent
grep scoped to one package and generalized in the write-up, and both survived
the lead's read; the review bots caught them. Recorded here rather than
quietly deleted, because a slate that never reports a withdrawal is not being
checked.

**Decisions recorded on their issues:** #1452 (D1), #1432 + #1436 + #1462
(D2), #1437 + #1425 + #1428 (D3), #133 (D4), #908 (D5), #1310 (D7), #1447
(D8), #923 (D9, a pointer to the existing answer), #1456 + #1440 + #1393 +
#1431 (D10), #1343 + #1344 (D11), #1430 + #1014 + #1380 (D12).

**To close:** #1382, #1386, #1103, and the two withdrawn above; #580 re-titled to defect 5 and the
`--secret-*-namespace` asymmetry.

## PR candidates, by size

| Size | Candidate | Files |
| --- | --- | --- |
| S | #1426 three sorted loops | `nodes.go`, `call.go`, `wait.go` |
| S | #1299 `Cancel` reads its own describe | `server/lifecycle.go` |
| S | #1422 `CallScope.Trigger` | `call.go` |
| S | #1457 enum switch domain | `flowfile/validate_switch.go`, DSL.md |
| S | #1442 `[]byte` arm | `eval.go`, `sdk/sdk_test.go` |
| S | #1358 eight pins | `celenv.go` |
| S | #1285 delete `Value.Type` | `value.proto`, regenerate |
| S | #1327/#1326 pins completion + digests shown | `plugin/admission.go`, `cmd/flow/plugins.go` |
| S | #1289/#1290 stdio resilience, one input convention | `cmd/flow/mcp.go`, `internal/mcp/` |
| S | `--protected-resource-revision` | `cmd/flow/protectedresource.go` |
| S | #1386/#1103/#1382 closes; #1307/#1311 AGENTS.md lines | `AGENTS.md`, skills |
| S | `deep.yml`'s false "visible to collaborators only" line, ahead of #965 | `.github/workflows/deep.yml:180` |
| S | Doc-truth pass (the `StepExecutor` wording, STYLE.md row, THREAT_MODEL cites, DSL.md fifteenth round). Not `AGENTS.md`: its invariants stand | `docs/ARCHITECTURE.md`, `docs/STYLE.md`, `THREAT_MODEL.md`, `docs/DSL.md` |
| M | #1425 `MergedStepIDs` + conformance | `eval.go`, `engine/execute.go`, `flowfile/validate.go`, `conformance/` |
| M | D6 `parallel:` atomic bound | `atomicblock.go`, both `runParallel`s, `conformance/` |
| M | #1449 `NamespaceReachCases` + one rooted replay history | `conformance/`, `engine/testdata/replay/` |
| M | #1294/#1295/#1443 `flow test` load-time refusals | `cmd/flow/test.go`, `flowtest/stub.go`, `run.go`, `file.go` |
| M | #1288 opaque spec field | `cmd/flow/mcp.go`, `mcp_test.go` |
| M | #1434+#1463 five roots in the LSP | `lsp/completion.go`, `lsp/hover.go`, `lsp/callinputs.go` |
| M | #1430 `ValidateSpec` at submit | `pkg/flowstate/v1/`, `server/server.go`, `eval.go` |
| M | #1378 `inputs` in the task-policy activation | `taskpolicy.go`, `conformance/` |
| M | #1380 freshness rewrite | `docs/AUTHORIZATION_FRESHNESS.md`, `docs/VISION.md` |
| M | manifest cross-check + host output check | `sdk/sdk.go:874`, `plugin/task.go:31,193` |
| M | #1333 `plugintoolkit` | `plugins/*`, a new shared package |
| M | #1388 gate declines the wide leg; plugin-module scanning in `ciDecisions` | `tools/gate/main.go`, `ci.go`, `Makefile`, `ci.yml` |
| M | SARIF/JUnit; workflow inventory; MCP prompts + resource + fix tool | `cmd/flow/`, `internal/mcp/` |
| M | `codex.exec base_ref` | `plugins/codex`, `plugins/git` |
| L | D1+D2 the type edition | `workflow.proto`, `flowfile/`, `eval.go`, `celenv.go`, `rundoc.go`, `docs/reference/values.md` |
| L | #1437 the reference-model registry | `pkg/flowstate/v1/`, `flowfile/`, `lsp/`, `flowdebug/`, `flowtest/`, `cmd/flow/taskrun.go` |
| L | #1440+#1456 one bridge + e2e plugin tests | `protoliterals.go`, `sdk/values.go`, `internal/pluginreachtest` |
| L | #1393 `LaunchRequest`, protocol 7 | `plugin.proto`, `plugin/launch.go`, `sdk/` |
| L | D4 `call: execution: child`, both drivers | `workflow.proto`, `flowfile/`, `engine/execute.go:594`, `eval.go`, `server/`, a shared conformance case set |
| L | #1385 compensation record | `run.proto`, `service.proto`, `engine/workflow.go`, `server/timeline.go` |
| L | #1014 scope enforcement | `authorization.go`, `auth/challenge.go`, `server/`, `cmd/flow/mcpserve.go` |
| L | #1435 rename/references; #1384 pushdown; #108 MCP bridge spike | `lsp/`, `flowfile/fix*`; `server/list.go`; a new plugin |

## What only the owner decides

Short, because this pass decided everything else it could:

1. **The two repository settings**, verified unapplied on 2026-08-31 and still
   the highest-leverage five minutes in the project: PATCH ruleset `20890404`
   with `pull_request` + `required_status_checks` naming `plan` and `verdict`
   + `merge_queue` (docs/CI.md has the parameters), and turn private
   vulnerability reporting on so SECURITY.md stops pointing at a button that
   does not exist.
2. **Where fuzz reproducers go** (#965): a draft security advisory,
   access-controlled storage outside Actions, or the status quo. A run
   artifact is not on the list: this repository is public, so anyone with
   read access can download one. The code follows the choice in one PR, and
   `deep.yml:180`'s claim that the artifact is "visible to collaborators
   only" is wrong today and can be corrected ahead of the choice.
3. **The delegation proto shape** (#567 D2): the recommendation on the issue
   stands; it is the one nearly-irreversible move in the identity track.
4. **The versioning scheme and the first tag** (#1216 criterion 1): what
   `flow version` prints and what `go install …@v` resolves, after items 1
   and 2.

Everything else in this file is the lead's under the standing instruction and
is recorded on its issue; the owner reopens by commenting.

## Risks

- **The type edition is the one schema change that touches everything.**
  It lands as one wave with one schema owner and `buf breaking` clean —
  which is achievable only because the change is additive: the `Type` message
  takes a new field number and the enum keeps its own, since the spec travels
  in durable history and reserving field 2 would strand an in-flight run (see
  the retirement list). Every `examples/` file is carried by `flow fix` in the
  same PR that changes the grammar. The risk is
  overreach — the numeric model and the boundary kinds are enough; message
  types and the `types:` block the CEL spelling points at (#177) are
  explicitly not this edition.
- **Trust-boundary work under velocity.** The submit checks, the scope
  enforcement, the manifest cross-check and the host output check each take
  the `flowstate-security-review` pass and land with the negative-direction
  test, or they wait.
- **Plugin protocol 7 is the last cheap bump.** After `LaunchRequest`, a grant
  change is additive; before it, every one is a version. Do not land a grant
  change between now and then.
- **Decisions in comments decay.** D9 was found "unanswered" by a careful
  reader because the answer lived four comments down. Each decision above is
  recorded as the first line of a comment that starts with the word
  *Decided*, so a grep finds it.
- **Token budget.** The account's weekly window was at 44% on Wednesday with
  this pass's six agents already spent. Waves A and G are Sonnet-routable
  almost entirely; B, D and F are Opus builders from settled specs; nothing in
  the program needs the top tier except the security passes. The lead does
  not dispatch a second review wave this month.

## Verification

- Every reproduced claim above was run against a `flow` built from `40cc365`
  under the session scratch directory: validate, run and test probes per
  dimension, LSP and MCP sessions driven by hand over stdio, one plugin built
  and executed against a real remote, one 649 KB generated `parallel:` file.
  The two claims this file inherits from the reference-model audit's durable
  runs (#1422, #1425) were not re-run on the durable driver here.
- Targeted tests run green by the passes: the conformance callers walk, the
  one-sided allowlist, `TestTheCallerHasNoRunContext`,
  `TestEveryRPCHasExactlyOneAuthorizationAction`, the SDK's structured-output
  tests, `tools/gate` and `tools/agentconfig`.
- This file and the ledger entry were checked with
  `go test ./cmd/flow -run 'TestInternalDocumentsSayTheyAreInternal|TestTheDocsIndex'`
  before the push. No generated surface changed; no `make check` was run
  because nothing in the tree changed but two plan files.
- Not verified: any Temporal cluster; VS Code or Zed in a real window; the
  `flow fmt` alias-erasure claim that D8 inherits from #1447; the durable
  driver's rendering of a timestamp output.
