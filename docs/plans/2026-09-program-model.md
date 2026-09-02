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
