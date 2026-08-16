# Flowstate — One-Week Plan (2026-08-03 → 2026-08-09): bugs, features, examples, tests, docs

## Status: executed, then continued (2026-08-03)

The week's plan shipped as PR #123 — thirty-one commits, each a green stopping
point. The follow-on session then closed the three items #123 deferred, took
staticcheck to zero, and cleared the dependency backlog. The workstream sections
below are kept as written so the plan and its execution can be compared; this
ledger is what actually happened.

**Shipped in #123**: Z1–Z11 (all eleven verified bugs, plus two found wider
during the fix: the ASCII whole-line fallback in `rootResponseScalar`, and the
cancelled local run that tolerated its way to success); G1 and G2 (corrections
sweep, then `docs/reference/` generated and CI-pinned, with the env-var
enumeration held to the tree by an AST walk in both directions); A1–A5; B1–B3
(`inputs:`/`outputs:` end-to-end: schema, both drivers, CLI, MCP, two CI-run
examples); C1–C3 (versioning gate with `--allow-unversioned-interpreter`, the
durable example harness forcing Continue-As-New over all 23 examples with exact
driver agreement, nested secret references via `Value.Structure`); D1–D3
(client-side traces with flush on every exit path, the Temporal tracing
interceptor and activity spans with containment tests, the observability lab);
E1 and E2 (LSP code actions and formatting, `flow fmt`); F1–F3
(`flowstate_run_local`, MCP resources, client docs); three new fuzz targets.

**Shipped after it, as separate PRs**, closing everything #123 deferred:

| PR | What |
|---|---|
| #129 | **E3** — `flow compile`, the verb for "what does this file become" |
| #132 | **E4** — `flow lsp --plugin-dir`, opt-in plugin awareness at startup |
| #131 | **E5** — the watch TUI consuming the progress the server already answered |
| #130 | staticcheck to zero and made required, a day before its advisory window closed |
| #127 | `actions/checkout` and `actions/setup-go`, across all five jobs rather than the three dependabot could see |
| #128 | eleven module bumps, including the `connectrpc.com/validate` v0.6.0 break |

Two Codex review findings on #123 were fixed before it merged: the MCP result cap
was bypassable through a run's declared outputs (measured at 2 MB against a
256 KiB bound), and `telemetryConfigured` named two of the three endpoint
variables the OTLP exporters read, so a traces-only deployment got silence.

**Still open**, now tracked as issues rather than only here: #133 (Update, child
workflows, heartbeats, per-step queues and priorities), #134 (the two spellings
of caller identity, the three shapes of a task), #135 (saga compensation —
unblocked now that `inputs:`/`outputs:` exist and probably the highest-value item
left — streaming Watch, Nexus, remote plugin distribution, `flow test`, the
`Value`/literal TODO, tree-sitter grammar, Windows CI, and the smaller carried
items).

**Then the session kept going**, and the deferral list above is most of what it
spent itself on:

| PR | What |
|---|---|
| #136 | this ledger, caught up with the branch |
| #137 | the three examples the house rule was owed — plugin, wait-timeout, saga — and the harness honesty to match, after one of them reached the real httpbin.org from inside a suite that believed everything was pointed at a stand-in |
| #138 | **logs over OTLP**, the third signal, with a step's line carrying the trace it belongs to |
| #139 | **schedules** — `triggers:` in a file, seven RPCs, `flow schedule`, tenancy doubled |
| #140 | **saga compensation** — `undo:`, the failure half |
| #141 | **cancellation compensation** — the other half, plus two Codex P1 findings |

Three of those deserve their reasoning recorded, because in each case the
interesting decision is not the feature.

`undo:` is spelled `undo:` and not `on_failure:`, which was the working name for
months and is in this document above. A step's compensation runs when *that step
succeeded* and something later failed — so `on_failure:` names the one case it
never runs in. A name that is wrong in the common case is a name that teaches the
feature backwards.

The cancellation half was deferred out of #140 deliberately and turned out to be
the more urgent one, because it was already promised. `flow cancel --help` said "a
workload that has to release a lock or undo a partial change still does", the
stdout said "it runs its cleanup before finishing", and `docs/DSL.md` said the
opposite of both while the engine did neither. A capability three surfaces
describe and no code performs is worse than an absent one: it is the sentence that
makes an operator stop looking for what is still allocated.

And the second Codex finding on #141 is the best bug of the day, because nothing
in the diff was wrong. `WaitForCancellation` is false by Temporal's default, which
resolves an activity future when cancellation is *requested* rather than when the
activity has stopped — so compensation could start while the forward work was
still in flight, and `delete` could complete and be reported as "undid" while the
`create` it was undoing was still on its way to succeeding. Correct compensation,
correct ordering, correct summary, and a resource left allocated under a sentence
saying it was not. The lesson generalises past sagas: a run that reports CANCELED
while its effects are still happening is making the same class of claim as a run
that reports cleanup it never did.

## What is next, and why it is not more features

An audit of the *surface* rather than the backlog, taken at the end of this
session:

- `Node.kind` has exactly four shapes — `task`, `for_each`, `parallel`, `wait`.
  There is no composition primitive at all: no `call:`, no `uses:`, no import.
  Every Flowfile is a flat, standalone list of steps.
- The built-in task registry has two entries, `log` and `http`. Everything else is
  a plugin.
- There is no `flow test`, no `flow list --filter`, and nothing heartbeats.

Which says something the issue list does not: **Flowstate is finished as an engine
for one run and empty as a platform for many.** Everything a single run does is
expressible, durable, governed, and observable. Nothing about an organisation's
*second* workflow is — it copies the first.

That is a structural gap rather than a missing feature, so it gets more expensive
with every feature that encrusts the flat step list. It is also unblocked: typed
`inputs:`/`outputs:` landed in #123 and are exactly the signature a call needs,
and a called workflow has the same driver split `task` already has — a nested run
locally, a nested executor durably.

So the next flagship is **`call:` — a step that runs another workflow**, resolved
at compile time and carried whole (a run's specification is frozen at submit, so
resolving by name at runtime would make a durable run depend on a mutable external
definition), bound by `with:` against the callee's declared inputs, seeing nothing
of the caller's scope, and bounded by total expanded nodes rather than by
resolution depth — a diamond of includes multiplies breadth, which is the same
shape as the billion-laughs bound the YAML reader already carries.

After it, in order: **`flow test`** (a reusable workflow nobody can test is one
nobody will reuse), **operability at scale** (search attributes and `flow list
--filter`, which a memo cannot give; activity heartbeats, the missing half of
#131), and then the debt in #134 before it compounds.

Explicitly not next, though each is real: Nexus, remote plugin distribution,
WASM, entity workflows, payload encryption. None of them is what stops a second
workflow being written today.

## Context

Flowstate is ~50 PRs old: a durable, policy-governed workflow engine (YAML+CEL DSL → protobuf spec → Temporal) with two agreeing execution drivers, a connect-go control plane, LSP, MCP, TUI, plugin system, and an identity/secrets substrate. The owner asked for a step-back plan for the coming week covering every surface (DSL, engine, Temporal, OTel, TUI, CLI, API/RPC/proto-first, LSP, MCP) plus agentic self-improvement — and, explicitly, checking everything so the plan contains concrete bug fixes, not just features, and fixing doc bit-rot.

This plan is grounded in: three code-exploration passes, two adversarial bug hunts (engine/drivers; parser/fix/LSP/auth/secrets/netpolicy), a systematic doc-vs-code drift audit, and a full local CI-parity run.

**Verified baseline (2026-08-02, branch tip = main)**: `go build ./...`, `go vet ./...`, `gofmt -l` all clean; `GOMEMLIMIT=2GiB go test -race -timeout 900s ./...` passes every package (~3.5 min wall; `pkg/flowstate/v1` 70s and `server` 57s dominated by ungated Temporal dev-server tests). The tree is green — every bug below is a latent defect the current tests cannot see, which is itself the lesson: most are one-direction tests (A→A) missing the negative or cross-driver direction.

This file is the week's working roadmap: sessions executing an item should check it off here (or strike it with a pointer to the PR) so the file stays a live record rather than becoming the next stale document. When the week ends, what shipped moves into the documents that own shipped things — ARCHITECTURE.md, DSL.md — per VISION.md's own rule, and what didn't ship either rolls into the next plan or is dropped with a sentence saying why.

## Definition of done for the week (pragmatism guard)

Parallel agent sessions make ~25–30 PRs feasible, but the week succeeds or fails on the **must-land set**; everything else is upside. Must land: Z1–Z3 and Z6–Z8 (the corruption + driver-agreement + compaction bugs), G1 (doc corrections), A1–A4 (agent force multipliers), B1–B3 (`inputs:`/`outputs:` end-to-end), D1 (OTel core), F1 (MCP local-run). If the week goes sideways, cut from the bottom of each wave, never from this set — and per CLAUDE.md, back out a half-landed stream to a green stopping point rather than leaving both halves.

## Security posture of the new surfaces (gating, not follow-up)

Every new capability this week is reviewed against fail-closed before merge; PRs touching auth/secrets/netpolicy/plugin run the security-review pass in addition to `make check`.

- **`flowstate_run_local` (F1)** hands agents in-process execution: it must inherit `flow run local`'s exact deny-by-default posture — no egress without the same explicit opt-ins, no secret scheme registered unless the flags say so, bounded output size on the tool result (an agent-facing surface is an untrusted-consumer surface too).
- **`flow lsp --plugin-dir` (E4)** makes an editor launch plugin executables: opt-in flag only, never auto-discovery from workspace config (a cloned repo must not be able to make an editor execute a binary); reuse the existing discovery hardening (world-writable refusal, handshake, `AllowInsecureSearchPath` gate) unchanged.
- **`inputs:` (B)** is new untrusted input into the spec: inputs count against `CheckSpecSize`/`CheckRunStateSize`, are validated against declarations at submit (fail closed on undeclared/mistyped), and are values, never expressions evaluated server-side.
- **Z10 (signal carry)** is a security fix, not hygiene: byte-bound the payload in the schema/server (not the caller's transport option) and GC delivered/expired carries.
- **Observability lab (D3)**: loopback-only bindings, no real credentials in compose files, and telemetry attributes covered by the invariant-7 containment tests so the lab cannot become the leak path.

## Extensibility notes (so this week's schema survives next month)

- **B1's input declarations carry a name, type, optionality, and default** — not a bare string map. That is what LSP type-checking (`env.Check`), `flow run --input` coercion, Schedules' static argument binding, saga compensation values, and a future Nexus operation signature all read from; a stringly map would force a breaking rework for each.
- **Run-level outputs get their own message**, not a widening of an existing one (the "streaming is not unary with more elements" lesson): adding fields later is free, widening is breaking.
- **G2's doc generators read the same descriptors/registry/cobra tree the code runs** — layer-3 single-source — so every future task, flag, or RPC is documented by existing machinery rather than a new obligation.

## Workstream Z — Verified bug fixes (Days 1–2, before feature work touches the same files)

Eleven traced, code-verified defects. Fix order within each package follows the hunter's ranking; each fix lands with the missing-direction test.

### Z1. `flow fix` / flowfile (owner: flowfile package)
1. **HIGH — `fixexpr.go:131,154,174`**: CEL `SourceInfo.Positions` are code-point offsets, spliced as byte offsets. Non-ASCII text before a bare reference → false "inside a macro" refusal (fails closed, wrong diagnostic); crafted case (`${'日本a'+ a}`) → **silent corruption**: string literal rewritten, real reference left bare, `flow fix` exits 0 on a file `flow validate` rejects. Fix: index `[]rune(src)` as `secret.go:250` (`markerSpan`) already does. Test by byte-comparison per CLAUDE.md.
2. **HIGH — `fix.go:1393-1396,1398-1399`**: `boundBareNames` doesn't unwrap anchors/aliases for `as:` (falls back to `item`, subtracting the wrong binding → body refs to a same-named step get rewritten; corruption) and `vars: *alias` yields nil. Note ordering: `f.expressions()` runs before `collectAnchors()` (`fix.go:180,184`), so alias resolution needs the anchor pass hoisted. No test covers anchored/aliased `as:`/`vars:` today.
3. **MEDIUM — `fix.go:1374-1406,1453-1461`**: workflow-level `vars:` names are subtracted as if bound bare, but workflow vars are rooted under `vars.` — a top-level var sharing a step's name leaves legacy references unrooted; `flow fix` exits 0, validator rejects.
4. **MEDIUM-LOW — `value.go:310-317`**: nested block scalars skip the `fenceError` interpolation check — `${...}` inside a block scalar in a mapping ships as literal text with zero diagnostic ("silently doing nothing gives the author no reason to doubt the file").
5. **LOW — `fix.go:1646-1656` (+ `rootResponseScalar` :1573)**: fence located by code-point column into a byte-indexed line; defeats the comment's stated protection against rewriting a fence in a same-line comment.

### Z2. Engine / driver agreement (owner: v1 + engine packages; shared tests in `pkg/flowstate/v1/internal/conformance`)
6. **HIGH — `eval.go:739-742` vs `execute.go:150-153`**: a step-`vars:` failure bypasses `continue_on_error` locally (whole run aborts) but is tolerated durably. Rehearsal stricter than production — invariant 3 violation. Small fix; wants a shared-corpus case: tolerated *non-task* failure at the *outermost* step.
7. **HIGH — error-text divergence**: durable prefixes `step "<id>": ` at the failure point (`execute.go:244,256`, `engine/wait.go`), local only on the propagating path (`eval.go:747-749`) — `${steps.<id>.error}` differs across drivers for any non-TaskError at the tolerating step itself. `ErrorTextCases`/`NestedErrorTextCases` miss exactly this direction.
8. **HIGH — `workflow.go:578-581`**: Continue-As-New compaction walker ignores CEL **map-key** expressions (`Expr_CreateStruct_Entry.map_key`), so a `steps.<id>` reference in key position is pruned — the resumed segment fails a step that already succeeded. Durable-only, invisible to examples CI (local driver never compacts).
9. **HIGH (facts) — local retry/timeout divergence**: (a) deterministic input-resolution failures (`nodes.go:341` plain error → `ErrorKindInternal` → retryable) are retried 5× with backoff locally but fail instantly durably, because local resolves inputs inside the retry loop (`eval.go:1025-1031`) and durable resolves in workflow code pre-scheduling (`execute.go:191`); (b) local has no default per-attempt/overall timeouts vs durable's 2m/10m (`engine/policy.go:25,32`) — a plugin task hangs locally where production fails.
10. **MEDIUM — `engine/wait.go:190-223`, `MaxPendingSignals=128`**: pending-signal carry is count-bounded, not byte-bounded (schema has no payload size cap; the only byte bound is the caller's `WithReadMaxBytes` — the exact "caller's configuration" pattern `CheckSpecSize` exists to avoid), and never GC'd: 128 signals to an already-passed gate permanently poison `drainSignals`, dropping every later early signal for every other name.
11. **LOW — `wait.go:287-302`**: `ValidateWait` refuses `timeout:` on `sleep:` but silently accepts it on `wait_until:`, where both drivers ignore it (unreachable from a Flowfile; reachable via the Run RPC with a hand-built spec).

### Adversarial passes that came back clean (recorded so nobody re-litigates)
auth/netpolicy/secrets fail-closed paths (deny-on-error everywhere, redirect re-checking in the dialer, alg/claims hardening, scrubber closure design); `server/list.go` paging (cursor cannot skip; both bounds live); `CheckRunStateSize`/`CheckSpecSize` on all paths incl. MCP; retry defaults single-sourced; all twelve shared case sets now have two driver callers; HTTP body caps on all paths below the library.

## Workstream G — Doc de-rot (corrections Day 1; durable fix Day 5)

### G1. Corrections sweep (12 verified-false items, one PR)
- `cmd/flow/watch.go:29-42` **and** `README.md:1109-1113`: server *does* return `Progress`/`PendingActivities` while RUNNING (`server/server.go:428`, since #90).
- `docs/EDITORS.md:109-118` + `docs/DSL.md:70-74` + `lsp/hover.go:669`: http deferred-input names are rooted under `response.*` (`eval.go:288`), not bare — DSL.md's own table at :1139 is the correct one.
- `docs/EDITORS.md:99-104`: "no task consumes a secret yet" — false since `http.bearer`.
- `cmd/flow/client.go:112-116`: comment claims client tracing works; nothing initializes a client tracer/propagator (VISION.md:120-124 is correct — fix the code comment; D1 below fixes the code).
- `docs/VISION.md:109-112`: JIT federation described as unwired; it landed (#115, `taskruntime.go:19`, `examples/http-federated/`) — move the entry per VISION's own rule.
- `docs/DSL.md:1179-1183`: waits also produce outputs (`timed_out`, `payload` — `wait.go:17-66`).
- `CLAUDE.md:79-89`: add `buf lint` + `buf breaking --against .git#branch=origin/main` to the CI-parity list (the only two CI checks missing from it).
- `flowfile/doc.go:92-99`: header-secret refusal now deliberate, and `bearer:` unmentioned (README fixed in `ac36c89`; package doc left behind).
- `README.md:637-647`: env-var table omits ten variables the code reads (FLOWSTATE_AUTH_POLICY, _IDENTITY_KEY, _SECRET_ENV_ALLOW, _SECRET_DIR, _PLUGIN_DIR, _MAX_STEPS_PER_RUN, _INSECURE_PLAINTEXT_TOKEN, _SYMBOLS, _BACKGROUND, OTEL_EXPORTER_OTLP_*).
- `docs/ARCHITECTURE.md:226`: search-attributes/memo row is half-landed (tenant memo shipped and load-bearing; `flow list --filter` not) — split/mark partial.
- Preserve verified-true claims (do NOT "fix"): CLAUDE.md's connect-go line refs; VISION's client-propagation claim; DSL.md's versioning-unenforced correction paragraphs; EDITORS.md's not-implemented list; plugin-blindness claim (with the nuance that `flow server --plugin-dir` *does* see plugin tasks for Validate/GetCatalog).

### G2. The durable fix (Day 5)
Generated reference docs pinned in CI, per VISION's "generated ecosystem": task reference from registry/descriptors, CLI reference from the cobra tree (`newRootCommand()` is testable by design), MCP tool list from the service schema, env-var table from one registration point — each pinned with `git diff --exit-code`, the same mechanism keeping `buf generate` honest. Verify SourceCodeInfo availability first (VISION's warning). docs/CLI.md becomes generated reference + philosophy preamble. End of week: move shipped VISION entries out, per its own rule.

## Workstream A — Agentic self-improvement (Day 1, non-negotiable)
1. `.gitignore`: narrow the final `.claude/` rule to `.claude/worktrees/`; commit `.claude/commands/` skills encoding the CI-parity loop (now incl. buf lint/breaking), bounded-test/fuzz recipes, both-drivers checklist.
2. `Makefile`/`scripts/ci.sh`: `make check` = the full CI-parity list verbatim (incl. `GOTOOLCHAIN=go1.26.5` govulncheck pin); `make test-fast`.
3. Fast tier: gate the three `testsuite.StartDevServer` sites (`server/main_test.go:86`, `engine/versioning_test.go:130`, `engine/workflow_e2e_test.go:41`) behind `testing.Short()` — measured: this is ~2 min of every agent's inner loop.
4. Remove vestigial `pkg/flowstate/v1/worker/main.go` (24-line worker with no versioning/secrets/egress/plugins, zero tests).
5. CI hardening (may slip to Day 2): staticcheck/golangci-lint; `flow fix --check examples/`; bounded fuzz smoke job (`GOMEMLIMIT=512MiB -parallel 1 -fuzztime 30s`); new jobs advisory 48h before required. New fuzz targets Day 3: CEL compile path, MCP protojson args, plugin protocol framing (one target exists today vs the "bound every untrusted parser" principle).

## Workstream B — DSL flagship: `inputs:` / `outputs:` (Days 2–4, single owner, critical path)
DSL.md's own Phase 2. Today runs can't be parameterized (`RunRequest` carries only a `Workflow`) and nothing carries a computed value out of a run (the `cel:`/`echo:` retirement removed it). Unblocks: `flow run --input`, MCP agents passing arguments, LSP expression type-checking, saga/runbook example corpus.
- **B1 (Day 2) schema first**: `Workflow.inputs`, run-level outputs; `buf breaking` clean; all week's proto changes concentrate here; design note in DSL.md.
- **B2 (Day 3) compile + execute**: flowfile spelling; CEL root `inputs` grouped under an object (invariant 2's reserved-word lesson); shared-corpus cases with **both drivers verified as callers**; size checks (invariant 9); `RunState` add-only/absent-tolerant with an explicit old-writer/new-reader test (invariant 10).
- **B3 (Day 4) reachable**: `flow run --input k=v`/`--input-file`; outputs in `flow get`; MCP tools accept inputs; **two examples in `examples/`**. House gate: Flowfile expresses it, `flow validate` accepts it, CI-run example exercises it.

## Workstream C — Engine/Temporal (beyond Z fixes)
1. **Enforce the worker-versioning precondition** for workflow-side CEL eval (DSL.md:387+ documents required-but-unenforced; today `flow worker` only warns, `main.go:257`). Fail closed, scoped so zero-config `start-dev` still works (invariant 8); update DSL.md to "enforced."
2. **Durable example coverage**: examples run only via the local driver, so compaction/CAN paths escape example CI (self-documented at `engine/compactvars_internal_test.go:27` — and bug Z8 proves the point). Add a `-short`-gated dev-server harness running CAN-relevant examples durably.
3. **Secret refs nested in lists/maps**: generalize `flowfile/secret.go:65-89` rather than more one-off fields like `bearer:`; containment tests per CLAUDE.md (`%v/%+v/%#v/%s` on value, struct, slice).

## Workstream D — OpenTelemetry (weakest subsystem; VISION wrote the spec)
1. **(Day 2) core**: stop discarding the shutdown/flush func (`cmd/flow/main.go:180`); set `service.name` resource; initialize client-side tracer + `otel.SetTextMapPropagator` so traces start at the person running `flow run` (fixes the `client.go` comment lie for real).
2. **(Day 3) Temporal TracingInterceptor** (contrib — "theirs to keep working") on client + worker; first-party spans for compile, activity-side step execution, secret resolution — attributes never carry secret values (invariant 7, containment-shape tests); no spans minted in the replay path outside Temporal's interceptor contract (invariant 4).
3. **(Day 5, likeliest slip, non-blocking) observability lab**: docker-compose under `examples/observability/` — Collector, Tempo, Loki, Prometheus, Grafana + Temporal dev server + flowstate server/worker; provisioned dashboard; README proving one trace id from `flow run` through Grafana to the Temporal UI. CI validates `docker compose config`; full smoke best-effort.

## Workstream E — CLI / LSP / TUI (cheap wins on existing machinery)
1. **LSP code actions** wired to the `flow fix` library (after Z1–Z3 land — do not ship the rewriter into editors while it can corrupt): quickfix + `source.fixAll`; byte-comparison tests.
2. **`flow fmt`** exposing `flowfile.Marshal` (+ `--check`); LSP formatting (opt-in — Marshal rewrites whole documents).
3. **`flow compile`** verb (RPC exists; currently MCP-only).
4. **`flow lsp --plugin-dir`** (command takes zero flags today): opt-in plugin awareness at startup, never on keystroke, so editor/validator/worker agree when asked.
5. ~~**TUI live progress**~~ — **shipped**: `watchState.absorb` folds `Progress` and `PendingActivities` in, a position change counts as a change in both shapes, and both render through the helpers `flow get` uses (`runPosition`, `pendingActivityLines`) so the two surfaces cannot drift. The join is covered by `TestWatchFollowsARealRunningExecution`, which watches a genuinely running execution through the real server and asserts it is observed moving between steps — dev-server-gated behind `-short` like the rest.

## Workstream F — MCP (close the agent loop)
1. **`flowstate_run_local` tool**: compile → execute via the local driver in-process → structured result; agents can author *and verify* with no server/Temporal. Same fail-closed egress/secret posture and opt-ins as `flow run local`; it *is* the local driver (invariant 3).
2. **MCP resources**: DSL.md + task catalog; consider deriving the ~110-line hand-written dispatch table from the service descriptor.
3. **Docs parity**: `flow mcp` client-config snippets (LSP has five editors' worth; MCP has zero).

## Explicitly NOT this week
Nexus; remote plugin distribution; child workflows / Update / heartbeats / per-step queues & priorities; ~~`on_failure:` saga (wants inputs/outputs — design note only)~~ — **shipped as `undo:`**, both halves, in #140 and #141; the rename is argued in the ledger above; streaming Watch RPC; Principal↔WorkloadIdentity unification and the three task shapes (written issues); tree-sitter grammar; Windows CI. ~~**Stretch if B lands early: Schedules**~~ — **shipped** in #139.

## Sequencing

| Wave | Lands |
|---|---|
| Day 1 | A1–A4, G1 doc-corrections sweep, Z1 fixes 1–3 (flow fix corruption class), Z2 fixes 6–7 (small driver-agreement fixes + the missing shared-corpus direction) |
| Day 2 | B1 schema, Z8 compaction map-key fix, Z9 local retry/timeout parity, D1 OTel core, F1 MCP local-run, A5 CI hardening |
| Day 3 | B2 both-driver execution, C1 versioning enforcement, Z10 signal-carry bounds/GC, D2 tracing interceptor + spans, E2 fmt, new fuzz targets, Z4/Z5/Z11 small fixes |
| Day 4 | B3 reachable + examples, C2 durable example coverage, C3 nested secrets, E1 code actions (rewriter now safe), E3–E5, F2/F3 |
| Day 5 | G2 generated docs + CI pins, D3 observability lab, stretch: Schedules |
| Days 6–7 | Buffer; clean-clone verification of pushed branches (CLAUDE.md); ~~examples debt (plugin-task example, `timed_out` example, per-example READMEs)~~ **shipped**; written issues for every deferral; VISION entries moved for shipped items |

The examples debt closed as three things rather than the three it named.
`examples/wait-timeout/` runs a gate to its deadline on both drivers, which also
gave the local harness its first unattended path through a `wait_for_signal:` step.
`examples/plugins/greet/` is the plugin surface's worked example, deliberately one
directory outside `examples/*/workflow.yaml` — a file naming `example.greet` is
*meant* to be refused by the eight glob-driven checks that judge that corpus with
the built-in registry, so it is enumerated by name from the one package that can
build a plugin and launch it, and the package fails rather than skips if a run that
could have executed it did not. READMEs went only where a directory holds something
the Flowfile cannot explain: the plugin example, and the two `auth-policy.yaml`
files. Everywhere else the workflow's own comments already say it, and a second copy
is a thing to leave stale.

## Risks
- **Schema churn**: all proto changes concentrate in B1; one schema owner; `buf generate` transiently breaks every importer (CLAUDE.md warns).
- **Invariant 10**: `RunState` additions add-only/absent-tolerant; test the cross-version read.
- **Invariant 3**: every behavior change lands as shared-corpus cases with two verified driver callers — Z6/Z7/Z9 exist precisely because a direction was missing.
- **Fix-before-feature ordering**: E1 (LSP code actions) must not ship before Z1–Z3; C2's durable harness should land before/with Z8 so the fix has a test that can see it.
- **Parallel-agent hygiene**: one owner per package (Z1/B: flowfile+proto+tests; Z2/C: engine+v1; D: telemetry; E: lsp+cmd); report cross-package findings, never cross-edit; leave green stopping points.
- **New CI jobs advisory for 48h** (the govulncheck "advisory arrived, not your bug" lesson generalizes).
- **The lab slips easiest** — deliberately last, non-blocking.

## Verification
- Per-PR: `make check` (full CI-parity incl. buf lint/breaking); capability PRs pass the house gate (Flowfile + validate + CI-run example); PRs touching auth/secrets/netpolicy/plugin/MCP additionally get a security-review pass.
- Bug-fix PRs: each carries the test for the direction that was missing (cross-driver, negative, or join), not a re-assertion of the passing direction.
- Driver agreement: new case sets grepped for two callers.
- Doc pins: `git diff --exit-code` after generation.
- End of week: clean-clone build/vet/test of pushed branches; re-run the doc-drift audit spot-checks against the corrected files.
