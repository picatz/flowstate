# The program model and its frontends, audited (2026-09-02)

> [!NOTE]
> **Internal process, not product documentation.** This file is part of
> `docs/plans/`: how agent work is dispatched here, and what past waves
> measured. Nothing in it describes Flowstate to someone using it — the map of
> the documentation that does is [docs/README.md](../README.md).

## Status and standing rule

Written 2026-09-02 against main at `dc9429d` (merge of #1479), the evening
after the reference-model audit ([2026-09-reference-model.md](2026-09-reference-model.md),
#1421) and the whole-system review
([2026-09-whole-system-review.md](2026-09-whole-system-review.md)). One
question: the Flowfile is one frontend over a Protobuf program model that two
drivers execute, so what do the model, the compiler and the toolchain still
assume about *that* frontend and *that* backend, and what are the smallest
slices that let a program, a canvas, an agent or another language produce the
same program and let the program travel as a signed, digested, deployable
artifact. The slate is filed under
[#1562](https://github.com/picatz/flowstate/issues/1562); this file records
what was measured and what the factory learned. It decays like the other
plans: read the tree and the issues for current state, never this file.

## Method

One lead, no subagents. The primary documents (ARCHITECTURE.md, VISION.md,
DSL.md's constitution and versioning rounds, the two September plans), the
twelve records that already claim the territory (#528, #1232, #1231, #106,
#713, #172, #102, #242, #715, #1331, #114, #112), and every open issue title
were read before a sentence was written. Load-bearing claims were then checked
against the schema and a binary built from the snapshot: `Workflow`'s fields
classified from `workflow.proto`, `Marshal`'s callers grepped, `flow compile`
run on two files differing only in expression whitespace, the output weighed
with and without one expression's `source_info`, and `flow run local` and
`flow validate` handed a compiled specification. Probes lived in the scratch
directory; nothing in the tree changed.

## What is strong, measured

- The IR is frontend-neutral where it counts: references are CEL ASTs resolved
  by name, and `normalizeExpr` (`flowfile/value.go:432`) re-parses every
  expression to a fixed point, so two files differing only in expression
  whitespace compile to byte-identical protojson.
- The versioning layers are explicit and a second frontend inherits them:
  edition at parse, profile per run, interpreter pinned per run, plugins
  resolved at submit.
- The projections a second frontend needs exist as functions and are absent
  as surfaces: `flowfile.Validate` takes a `*Workflow`, `flowfile.Marshal`
  writes one back, `v1.NewExpr` parses against the profile.
- WASM is a compile fact (#242) and the plugin protocol is a schema (#713).

## What the pass found

Grouped as #1562 groups them. The numbers are real.

| Group | Issues |
| --- | --- |
| The program as a value: classify, digest, normalize | #1563 (fields of `Workflow` classified; `AuthoredProgram`), #1564 (`ProgramDigest` over a canonical form), #1569 (`v1.Normalize`) |
| A second frontend and its proof | #1567 (Go builder), #1576 (`workflow.spec.json` goldens beside every example), #1577 (a second textual syntax refused, with the admission rule) |
| The IR as a surface | #1565 (`flow decompile`; `Marshal` made total or the IR narrowed), #1566 (every verb accepts a compiled spec), #1568 (schema-owned `SourceMap`; IR-addressed diagnostics) |
| Producers in other languages | #1575 (a parse RPC for producers without cel-go), #1574 (publish the Buf module; generation check; canonical fixture) |
| The program as a contract and an artifact | #1571 (the workflow signature derived as a descriptor), #1572 (the webhook trigger answers with declared outputs), #1570 (spec admission policy as CEL over the compiled `Workflow`), #1573 (the bundle and `flow build`) |

The recommended order is #1563, #1564, #1569 first, because the digest, the
goldens, the builder's equality test and #1331's signature all stand on the
authored subset and the normalizer; then #1567 and #1576 together, since each
is the other's test; the surfaces (#1565, #1566, #1568) as they earn slices;
#1577 is a decision and reorders nothing.

## Corrections to what this pass first wrote

- "Reformatting a Flowfile changes the compiled bytes" was the planned lead
  finding for #1564 and is false: the compiler already normalizes expression
  text. The finding became the opposite one, that the noise left in a spec is
  structural (`source_info` positions, `Call.source`, the `resolved_*`
  records) and has to be excluded by a defined subset rather than by a
  formatter.
- "The IR has no validation entry point" (the whole-system review's F1) is
  narrower than it reads: `flowfile.Validate(*Workflow)` exists at
  `validate.go:245`; what is missing is the package it lives in and the
  boundary it runs at (#1430), which #1566 and #1567 make concrete.
- `source_info` cannot simply be stripped as provenance: `Marshal`, lint and
  `flow fix` read `macro_calls` and `positions` from it. #1563 carries the
  measurement (2,119 bytes with, 875 without, on a one-expression probe) and
  the decision.

## The second pass, same day

The other axes the question has: what the IR carries for a program that
outgrows one execution, for policy that has to see the program and not only
one call, and for a run that has to change under an operator. One new
measurement, taken the same way as the first pass: a three-step callee called
four times compiles to 44,398 bytes against 11,341 for one call, because
`Call.workflow` embeds a copy per site (#172 states the design; nothing had
weighed it).

| Group | Issues |
| --- | --- |
| The program that outgrows one execution | #1582 (callees content-addressed inside the spec), #1583 (one `Dependencies(wf)` graph replacing four walkers), #1585 (sizing over the graph; ranked cut candidates for D4 and #156) |
| The program as a review surface | #1584 (`flow explain`, and `--against` as `breaking` for effects) |
| The run that changes under an operator | #1587 (`flow migrate`, `flow rerun --from`; `Frame.next_node` is positional), #1592 (`Workflow.budget` with `loop:`'s three properties) |
| Policy that sees the program | #1588 (pre-flight of task-shape, secret and assumption policy at submit), #1589 (resource-aware server authorization from attested facts), #1590 (`flowstate.policy.v1`; files as a projection), #1591 (`needs` narrowing claims on task descriptors) |

Recommended order inside the pass: #1583 first, because #1584 and #1585 read
it and #1484 lands underneath it either way; #1582 with #1564; #1590 before
#1589 and #1588, so the rules they add are written once against a schema;
#1587 and #1592 are decisions the owner takes and reorder nothing.

Deliberately not filed on this pass: a tuple-store authorization system or a
second policy language (#1589 binds facts the server already attests into the
one CEL vocabulary); per-step inline policy in the file (#104's by-reference
rule stands); reopening the held Flowfile egress declaration (DSL.md holds it;
#1591 is the narrowing half #721 already says is safe).

## The third pass: testing, analysis and debugging over the program

The IR as the thing a test generates from, a mutant edits, an analyzer
walks, and a history reproduces. Two facts from probes against the same
binary: a `call:` step cannot be stubbed (`flow test` answers "runs no task
... and so cannot be stubbed"), and a caller's coverage reads `1/1 steps
reached` over a three-step callee. One fact from the tree: `flowtest`'s
package doc scopes it to the local driver on purpose, while 37 engine test
files run the durable interpreter in-process through the SDK's
`TestWorkflowEnvironment` with no server.

| Group | Issues |
| --- | --- |
| One contract for every check | #1597 (an `Analyzer` over the program, in-process and as a plugin capability) |
| The author's own file, both drivers | #1598 (`flow test --driver durable`, sub-second, no server) |
| Tests the contract writes | #1594 (`--fuzz` from the typed contract), #1600 (`--mutate`, one-field mutants of the message), #1599 (a `step:` stub on a `call:` answers at the callee's declared outputs) |
| The run as evidence | #1595 (`flow test init --from-run`: a durable run as a fixture and the post-mortem debugger), #1596 (`flow diff` between runs, fixtures and seeds) |

Recommended order inside the pass: #1598 first, because it is the cheapest
and turns the product's central promise into a test an author runs; #1599
with #1441's load-time universe; #1597 after #1583, since the graph is its
first shared fact; #1594 and #1600 after #1295 and D1; #1595 after #1550's
redaction decision; #1596 after #1439.

Deliberately not filed on this pass: durable attach and a `flow debug replay`
verb (#928 and #1111 hold both with the owner's ledger; #1595 is the
post-mortem answer that needs neither); embedding the Temporal dev server
(#377 decided the child process and named when to revisit); a run profile
verb (#1485 surfaces measured CEL cost and #1585 the static sizes).

## The analysis framework, expanded

Owner steer (2026-09-02, evening): the analyzer contract is the center of
the testing and analysis work, in the shape `go/analysis` and Buf's check
plugins give it, and it should be extensible, pluggable and dogfooded. #1597
became the umbrella; its record maps each `go/analysis` piece onto the tree
(the package is the root program plus its callee table; `TypesInfo` is the
scope table and the checked expressions; `passes/inspect` is #1501's one
traversal; `buildssa` is the flow graph; facts are keyed by program digest;
`unitchecker` is a plugin capability) and carries the migration ledger of
every existing check. The count that motivated it: the validator is one
1,900-line file with 18 `validate*`/`check*` functions, and at least seven
other one-consumer walks over the same message exist beside it.

| Slice | Issue |
| --- | --- |
| the second IR: a control-flow graph joined to #1583's def-use edges, built once | #1601 |
| `# want` fixtures and `.golden` fixes, one `Run` for every analyzer | #1602 |
| facts across `call:` and module boundaries, cached by digest | #1603 |
| the out-of-process protocol and `CAPABILITY_ANALYSIS` | #1604 |
| one checker for five hosts, a narrowing project file, tiers, the ledger | #1605 |

Three questions were left open on the record rather than decided: in-file
suppression (recommended none; disables live in the project file), severity
narrowing, and whether the package lives beside the IR or importable without
the engine (#406 decides).

## Deliberately not filed

- A WASM plugin ABI, remote plugin distribution and browser execution: #102,
  #715, #242 and #1546 hold them with a decided order.
- `call:` by remote reference or alias: D8 in the whole-system review closes
  it; #1573 names the artifact a registry would serve without reopening it.
- Update and a child execution mode for `call:`: #133 and D4; #1572 is shaped
  so update-with-start changes no Flowfile when it lands.
- A `metric:` task, `SideEffect`, a third driver: refused or held in DSL.md,
  #908 and #1231.

## Verification

- Two `flow compile` probes, one `flow run local` probe and one
  `flow validate` probe against a binary built from `dc9429d`, bounded by
  `timeout 60`, outputs quoted in the issues.
- Second pass: two more `flow compile` probes (one call site, four call sites
  to one callee), bounded the same way, numbers quoted in #1582 and #1562.
- Third pass: two `flow test` probes (a `step:` stub naming a call step, and
  `--coverage-required` on a caller), bounded the same way, output quoted in
  #1599 and #1562; the 37-file count is `grep -rl TestWorkflowEnvironment
  pkg/flowstate/v1/engine/*_test.go | wc -l` at the snapshot.
- No `go test`, `tools/gate` or `make check` for the audit itself: nothing in
  the tree changed. This file was checked with
  `go test ./cmd/flow -run 'TestInternalDocumentsSayTheyAreInternal|TestDocsIndex'`.
- Not verified: any Temporal cluster, a browser, or a second-language
  toolchain; the claim that no TypeScript or Python CEL parser ships the
  profile's macros is from knowledge of those ecosystems, not from running
  them, and #1575 says so.

## What the factory learned

- Running the binary before filing changed three of fifteen issues' shapes
  (the corrections above). A pass that files from reading alone would have
  filed a false lead finding under the digest issue.
- The umbrella records (#528, #1232, #1231) are complete as contracts and were
  empty as slices; every acceptance criterion they carry that this pass could
  make concrete now has an issue with a reproducer, and each is cross-linked
  from the record it advances rather than re-argued in a new one.
