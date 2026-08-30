# Issue #1227 schema-derived interface pilot

> [!NOTE]
> **Internal research record, not product documentation.** This report records
> a bounded experiment against `origin/main` at `aa734bac` (2026-08-30). The
> package under `internal/schemaifacepilot` is evidence, not a supported CLI
> framework and not authorization to retrofit commands or annotate schemas.

## Decision

**Narrow the proposal.** Keep built-in Cobra binding explicit and hand-written.
Use descriptors to project contract facts into read-only documentation and to
run the existing Protovalidate validator early, but only after a command-owned
allowlist has selected a field. Do not adopt runtime reflective request binding
or add a static-binding generator for built-in commands from this pilot.

The generated/static pilot is materially cheaper than reflection once built,
but it replaces nine straightforward lines in `runGet` with a generator,
generated artifact, drift test, and compatibility policy. The reflective pilot
removes little code, shifts descriptor initialization into command construction,
and still needs the same explicit selection and surface metadata. Neither earns
production ownership for this command. The useful reusable slice is the
descriptor-backed reference row: requiredness, UUID constraints, and optional
presence are contract facts and render consistently without moving usage copy
or exposure policy into Protobuf.

Dynamic, read-only plugin catalog rendering remains the strongest candidate for
reflection. It is a different decision from mutating built-in CLI input and
should be evaluated separately with untrusted-descriptor byte/count/depth/text
bounds.

## Why `get`

The candidates were inspected before choosing:

- `compile` is a local file/stdin/compiler surface. Its Cobra input is a path,
  while the `Compile` RPC takes compiled request bytes; binding RPC fields would
  model the wrong interface and its Flowfile/CEL diagnostics dominate behavior.
- schedule commands are mutating and carry nested workflow, input, trigger,
  overlap, pause, backfill, tenancy, and authorization semantics. They are too
  high-risk for a first mechanics pilot.
- `get` has one required string positional and one optional UUID string flag,
  already validates the request before transport, and has no CEL behavior. It
  exercises requiredness and presence without identity, credential, secret, or
  policy input.

The explicit allowlist is therefore only:

| Command surface | Request field | Eligibility owner |
|---|---|---|
| `[workflow-id]` | `GetRequest.workflow_id` | `GetSelections` in Go |
| `--run-id` | `GetRequest.run_id` | `GetSelections` in Go |

No descriptor walk discovers additional inputs.

## Fact inventory

### `workflow_id`

| Fact | Owner | Projection decision |
|---|---|---|
| Field number, string type, requiredness | `service.proto` and `(buf.validate.field).required` | Schema contract; safe to render and validate from the descriptor. |
| Google `REQUIRED` behavior annotation | `service.proto` | Language-neutral API contract; informative, not CLI exposure authority. |
| Positional spelling and exact one-argument arity | Cobra `Use` and `cobra.ExactArgs(1)` in `cmd/flow/main.go` | Explicit command-owned exposure and hierarchy; do not derive. |
| Assignment to `GetRequest.WorkflowId` | `runGet` in `cmd/flow/get.go` | Executable CLI behavior; the pilot compares reflection with generated direct assignment. |
| Empty-ID rejection | Protovalidate interceptor plus `authorizeRunDecision`'s explicit empty check | Server/request authority; early CLI validation does not replace it. |
| Tenant routing, ownership check, indistinguishable not-found response, audit | `authorizeRun` / `authorizeRunDecision` in `pkg/flowstate/v1/server/lifecycle.go` | Server authority only; no descriptor metadata can grant or weaken it. |
| “Report what a run is doing” and examples | Cobra `Short`, `Long`, and `Example` | Presentation copy and runnable journey; do not move to schema field prose. |

### `run_id`

| Fact | Owner | Projection decision |
|---|---|---|
| Optional presence and UUID format | `service.proto` Protovalidate option | Schema contract; safe to render and validate. |
| Unset means current run; set pins one attempt | CLI flag usage, RPC/service prose, and Temporal lookup behavior | Cross-surface behavior, but the concise flag wording remains presentation copy. |
| Empty flag stays absent rather than becoming present empty string | `runGet` and both pilots | CLI binding behavior required to preserve schema presence. |
| `--run-id` spelling, no short alias, default `""` | Cobra flag declaration | Explicit command-owned exposure and compatibility; do not derive from `run_id`. |
| UUID refusal before network I/O | `v1.Validate(request)` in `runGet` | Early feedback from the same schema rules; not a second validator. |
| UUID refusal at the served API boundary | Protovalidate Connect interceptor installed by `flow server` and `server dev` | Server authority remains unchanged. Direct embedders remain responsible for installing an interceptor or validating, as existing server docs state. |
| Temporal attempt selection | `authorizeRunDecision` and Temporal `DescribeWorkflowExecution` | Runtime/server behavior, not descriptor UI configuration. |

### Other `flow get` facts that are not request fields

| Fact | Owner | Why it stays there |
|---|---|---|
| `--output`, `--raw`, status/output stream split | CLI renderer | Presentation and scripting contract, not `GetRequest` schema. |
| `--reveal-sensitive` and fail-closed output redaction | CLI output containment | A response-handling security decision; never inferred from input shape. |
| `--address`, TLS, token file, credential source and environment precedence | shared client/server flags | Transport, identity, credentials, and environment policy; explicitly excluded from request projection. |
| Cobra hierarchy, help grouping, examples, aliases | Cobra tree | The executable CLI remains the source used by `docsgen/cli.go`. |
| Response authorization and tenant ownership | server lifecycle code | Authoritative runtime policy. Descriptive metadata would carry no authority. |
| CEL evaluation, function cost, determinism, cancellation | none on `Get`; shared CEL registries elsewhere | No CEL behavior exists to derive for this command. Descriptors may describe types, never evaluator behavior. |

The generated CLI reference must therefore continue to walk the actual Cobra
tree for command and flag inventory. The pilot reference augments two selected
rows with schema contract facts; it does not construct a parallel command tree.

## Pilot shape

Both pilots consume the same `GetSelections` slice. Every selection names its
protobuf field, Go field (static generation only), surface spelling, usage copy,
positional status, and command-owned exposure classification.

- **Runtime reflection:** validates only the named fields, creates only selected
  non-positional flags, and writes through `protoreflect.Message.Set`.
- **Generated/static:** validates the same selection at generation time and
  emits direct assignments to `GetRequest.WorkflowId` and `RunId`.
- **Reference generation:** lives in a separate package so importing a binding
  does not pull in the 487,312-byte source-bearing descriptor artifact. It
  combines selected surface metadata with linked descriptor shape,
  the shared `FieldConstraints` vocabulary (extended with its missing UUID
  phrase), and source-bearing prose.
- **Validation:** both pilots call `v1.Validate`; the server code and Connect
  interceptor are untouched.

The generated reference is
[`internal/schemaifacepilot/testdata/get-fields.md`](../../internal/schemaifacepilot/testdata/get-fields.md).
It truthfully shows that `GetRequest` has message prose but its selected fields
have no field comments. Normal linked Go descriptors return no source prose;
only `protodoc.Files()` does.

## Fail-closed and evolution results

The fixture tests establish these outcomes:

| Change or shape | Result |
|---|---|
| A new request field, including identity, credential, policy, server-owned, or output-only names | Not exposed because it is absent from `GetSelections`. |
| Unknown, unspecified, or server-owned exposure classification | Construction/generation error. No custom schema exposure option was introduced. |
| Selected field renamed, moved, or removed | Construction/generation error. This is a CLI compatibility event despite wire compatibility. |
| Surface-name collision | Construction/generation error. |
| Optional scalar | Supported; unset and explicitly empty remain absent, matching `runGet`; a changed non-empty value is present, then validation decides validity. |
| Real oneof | Rejected. Proto3 optional's synthetic oneof is accepted. |
| Repeated, map, message, enum, numeric, or other non-string shape | Rejected. The pilot does not flatten or invent parsers. Enum symbol evolution therefore cannot silently alter CLI behavior. |
| Schema default | Rejected; command defaults stay explicit. |
| Deprecated selected field | Rejected until the command owns an alias/deprecation/migration decision. The pilot generates no aliases. |
| UUID/bounds violation | Refused by `v1.Validate` before transport. Numeric bounds are outside this selected shape and numeric fields are rejected. |
| Unknown descriptor options on unselected fields | No effect: descriptor options do not grant eligibility. A future security classification option must reject unknown enum values before selection. |

These are deliberately stricter than a general-purpose `protoflags` walk. A
supported protobuf shape is not evidence that a command should expose it.

## Measurements

Environment: Linux amd64 orb, Intel Xeon 2.60 GHz, Go 1.27.0, two logical CPUs.
Binaries used `-trimpath -ldflags='-s -w'`. Benchmarks ran five times with
`GOMEMLIMIT=1GiB`; values below are medians. Cold construction ran each helper
in 25 fresh processes and measures the construction call inside `main`.

### Construction and application

| Measure | Runtime reflection | Generated/static |
|---|---:|---:|
| Cold construction latency | 478,895 ns | 4,845 ns |
| Cold construction allocations | 1,743 | 7 |
| Cold construction allocated bytes | 282,712 B | 776 B |
| Warm construction | 2,022 ns, 11 allocs, 1,256 B | 809 ns, 7 allocs, 776 B |
| Warm apply (validation excluded equally) | 400 ns, 4 allocs, 64 B | 52 ns, 1 alloc, 16 B |

The reflective cold result includes first-use initialization of linked
descriptors. The steady-state difference is tiny in human CLI terms, but the
reflection approach is not free and does not eliminate selection metadata.

### Size and generation

| Artifact | Bytes |
|---|---:|
| Stripped static helper binary | 15,765,767 |
| Stripped runtime helper binary | 15,778,055 (+12,288) |
| Stripped runtime + source-info reference binary | 16,281,863 (+516,096 vs static) |
| Existing source-bearing descriptor set | 487,312 |
| Generated static binding | 1,066 |
| Generated selected-field reference | 541 |
| Pilot Go + generated-reference sources measured by `wc` | 31,552 |

Warm reference generation took 6.16 µs, 32 allocations, and 3,488 B. `go
generate ./internal/schemaifacepilot` took 0.34 seconds with a warm Go cache and
62.74 seconds / 324,712 KiB maximum RSS after `go clean -cache`; almost all of
the cold cost is compiling the generator's existing Flowstate/Protobuf
dependency graph. That is real CI/developer toolchain cost for 1,607 generated
bytes, even though incremental cost is small.

## Testability and authority

The tests compare both pilots' messages and usage, mutate the UUID input to
prove early validation fails, preserve optional absence, reject every listed
unsupported/evolution case, verify linked-versus-source descriptor prose, and
regenerate both artifacts in memory to detect drift. Existing server tests
remain the proof of tenant authorization and served-boundary validation; this
pilot neither replaces nor mocks them.

No schema, server, Cobra hierarchy, generated CLI reference, CEL registry,
authorization policy, billing setting, or executable request behavior changed.
The one durable shared change is that `FieldConstraints` now renders the UUID
rule as `a UUID`; the pilot consumes that source instead of maintaining a
second rule inspection.

## Follow-up boundary

Do not merge the pilot into `flow get`. Keep this report and test fixture as the
reproducible evidence for #1227, then remove the experimental package when the
issue is resolved if maintainers do not need it for comparison.

A focused next experiment, if desired, is **read-only plugin descriptor
rendering with explicit untrusted-input budgets**: cap descriptor bytes, files,
messages, fields, nesting, and rendered comment length; reject unknown security
classifications; and measure catalog/docs rendering independently from CLI
input binding. It should not share exposure policy with built-in commands.
