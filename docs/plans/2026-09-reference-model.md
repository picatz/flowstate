# The reference model, audited (2026-09-02)

> [!NOTE]
> **Internal process, not product documentation.** This file is part of
> `docs/plans/`: how agent work is dispatched here, and what past waves
> measured. Nothing in it describes Flowstate to someone using it — the map of
> the documentation that does is [docs/README.md](../README.md).

## Status and standing rule

Written 2026-09-02 against main at `faf09e8` (merge of #1416). Grounded in
seven evidence passes taken that day over one question: how a name written
in a Flowfile is declared, resolved, validated, completed, tested, addressed
and documented, from the YAML through the protobuf IR into both drivers and
back out through the LSP, MCP, CLI, `flow test`, plugins and the policy
surfaces. The slate is filed under
[#1421](https://github.com/picatz/flowstate/issues/1421); this file records
what was measured, what was corrected in earlier records, and what the
factory learned. It decays like the other plans: read the tree and the
issues for current state, never this file.

## Method

Seven agents, one lead. Five at the deep tier (scope census, modules memo,
IR-and-runtime coherence, plugins/policy/ingress naming, test-format naming)
and two at the mechanical tier (foot-gun probes against a binary built from
the snapshot, tooling-surface census). Every agent was told to cite
`file:line` or a reproduced `flow` invocation and to mark observation
against inference. The lead re-read every load-bearing citation in the tree
before a sentence reached an issue: `CallScope`, both parallel merge loops,
`mergedStepNodes`, `ResolveTaskInputs`, `validateNested`, `Registry.Register`,
`postRunScope`, `positionPath`, the wait's identity renderer, the LSP's
`refScope`, `certainNames`, `AttachIterationBinding`, and the flowtest
universes. Twenty-three issues were filed, each with a reproducer or a
citation, and none duplicates an open record; sixteen existing records were
advanced by cross-link rather than re-filed.

## What is strong, measured

- `v1.Scope` is one bucket per root and both drivers fill it; every
  expression resolves through one activation. References are CEL ASTs
  resolved by name, so the IR is frontend-neutral and a rename is a source
  edit.
- Rooting holds exactly as `docs/DSL.md` says: five roots refused as step
  ids, twelve plausible collisions (`results`, `state`, `value`, `payload`,
  `has`, `size`, `map`, `now`, `response`, `this`, `secret`, `timed_out`)
  legal and unambiguous; a `call:` refuses undeclared and callee-internal
  names with one sentence; `inputs:`/`outputs:` names are grammar-checked
  with a message that names the unreadable spelling.
- `plugins:` has landed as a declaration, resolved at submit and through
  callees, pinned as a replay contract.
- The four CEL policy surfaces share one identity vocabulary, held by
  discipline and `auth/vocabulary_test.go` (#548's residue is the extraction
  into one package, unchanged by this pass).
- `expect.check:` runs through the workflow's evaluator and profile.

## What the pass found

Grouped as #1421 groups them. The numbers are real.

| Group | Issues |
| --- | --- |
| Silent wrong answers on both drivers | #1422 `trigger.*` empty in a callee; #1425 nested block in a parallel branch (validator vs both drivers); #1426 map order picks the reported failure in workflow code |
| Names that mean two things, names nothing checks | #1427 identifier grammar at three of seven declaration positions; #1428 `error`/`item`/wait shaping names/`event` bare in an author namespace; #1429 `sender.identity` vs `run.identity`, `trigger.principal`; #1431 `Register` name grammar and uniqueness |
| Trust boundary | #1430 submit runs protovalidate only; per-selector precedence |
| Diagnostics and editor | #1433 `unresolved-reference` cannot say where a name lives; #1434 LSP: no `call:` outputs, two of five roots; #1435 rename/references |
| One mechanism, eighteen copies | #1437 derive from one table; #1438 scope reference, CLI verb, MCP tool; #1439 one step-address grammar; #1449 conformance set over every root and position |
| Test format | #1441 five step universes; #1443 signal names unchecked; #1444 check scope narrower than the run; #1446 coverage blind to callees |
| Modules and policy | #1447 the callee-naming decision; #1448 transitive digests; #1450 deny-shaped task policy on plugin rename |
| Docs | #1451 the drift, enumerated |

The recommended order is the table's order: the three both-driver bugs and
the name bugs are one PR each; the LSP `call:` arm is the cheapest large
win; the single-table refactor (#1437) is what prevents the next copy, and
#1439 and #1441 stand on it; #1447 is the owner's decision and reorders
nothing above it.

## Corrections to earlier records, found on the way

- `docs/plans/2026-09-roadmap.md`'s language row says outputs are untyped;
  #1377 closed via PR #1392 the day after it was written. Struck below.
- #172's observability section claims path addressing is "the one spelling
  everywhere"; `flow get` renders step ids only, no callee file or digest,
  and two same-named steps render as `provision > provision` (#1439 and
  #1451 carry the correction).
- `docs/DSL.md` still calls `plugins:` Phase 3, and its `vars:` round says
  "start-time derivations" where a var may not read `inputs.*` (#1451).
- `docs/DSL.md`'s record of the vars-on-block-step disagreement is
  historical: both walks now derive step vars before the node kind is known.
- #1110's "no positions, first error wins" is stale: the flowtest loader
  reports every problem with `line:col` and the LSP attaches to
  `**/*.test.yaml`.
- The `CallScope` isolation sentence in #172 ("its bound arguments and the
  profile, and nothing else") is narrower than the shipped rule, which
  carries `run.*`, `local` and the address across; #1422 follows the shipped
  rule and adds `trigger`.

## Deliberately not filed

- Egress declaration in the file (`requires:`): `docs/DSL.md` records it as
  held; STYLE R1 limb 2 is the gate it would pass on, the same argument
  `plugins:` won. Advance when the owner reopens it.
- Signal authorization as CEL: #326, condemned by STYLE already.
- `flow test` and `flow mcp serve` blind to plugins: #1294, #1340, reproduced
  unchanged and left where they are.
- Tree-sitter or TextMate highlighting for `${…}`: #135, confirmed current.
- Fencing (`if:` accepts bare and fenced; `where:` refuses the fence): #545
  and STYLE R4 own it; the `where:` row is recorded on #1441.
- A third identity type extraction across the policy packages: #548.

## Verification

- 43 `flow validate` probes and 12 `flow run local`/`flow test` probes
  against a binary built from `faf09e8`, bounded by `timeout 60`, outputs
  quoted verbatim in the issues. Two claims were also run on the durable
  driver through `testsuite.TestWorkflowEnvironment` from temporary
  in-package tests that were deleted before any report (#1422, #1425).
- The map-order measurement (#1426) is 200 local runs; the durable path
  calls the identical shared function and was not run.
- No `go test`, `tools/gate` or `make check` for the audit itself: nothing
  in the tree changed. This file was checked with
  `go test ./cmd/flow -run 'TestInternalDocumentsSayTheyAreInternal|TestDocsIndex'`.
- Not verified: the LSP against a real editor window (#585), any Temporal
  cluster (the sandbox has none), and the `flow fmt` alias-erasure claim in
  #1447, which is inference from `marshal.go`.

## What the factory learned

- Report files: the harness refuses a subagent's `Write` of a report; four
  of seven agents returned the report as their final message instead. A
  brief should say "return the report as text" rather than name a path.
- Two agents wrote probe tests into the tracked tree despite the brief;
  both cleaned up, but the stop hook saw untracked files twice. A brief
  names the scratch directory *and* says probes go there or in `-run`
  filters over existing fixtures.
- Sonnet did the mechanical passes well when the brief enumerated the
  probes; Opus found the both-driver bugs, which no probe list would have
  named. The routing rule held.
- Receiver-cost: zero clarification turns; the owner's one redirect widened
  scope to plugins, policy and the test framework mid-flight, which two
  more agents absorbed without re-briefing the first five.
