# Flowstate — the step-back review and September direction (2026-08-31)

> [!NOTE]
> **Internal process, not product documentation.** This file is part of
> `docs/plans/`: how agent work is dispatched here, and what past waves
> measured. Nothing in it describes Flowstate to someone using it — the map of
> the documentation that does is [docs/README.md](../README.md).

## Status and standing rule

Written 2026-08-31 against main at `39798d4` (PR #1374). Grounded in five
evidence passes taken that day — engine/drivers, security/identity/policy,
language/toolchain, the GitHub issue and PR landscape, and
verification/docs/process — plus the primary documents. Every load-bearing
claim cites the tree at that snapshot.

The August plan's own lesson applies to this file from the day it is written:
it decays, and several of its claims will be false of the code within weeks at
current velocity. Read the tree and the open issues for current state, never
this file. When an item here lands, its entry moves to the document that owns
the shipped thing, per [VISION.md](../VISION.md)'s rule; what does not land
rolls into the next plan or is dropped with a sentence saying why.

Updated the same day, after a reconciliation pass: every "file after
dup-check" candidate was searched against the open design records, the
anchors were read, and the slate below now carries real issue numbers —
eleven filed (#1376–#1386), the rest advanced on records that already
claimed the territory. Three of this file's first-draft claims were
corrected by that pass, and the corrections are visible where they apply:
per-step priority is a recorded refusal (not an open gap), the entity-loop
bounds belong to #105, and the task-policy zero case was decided when #187
closed.

## August, measured

**Velocity.** 688 PRs merged in the month (182 in Aug 1–10, 190 in 11–20, 316
in 21–31 — the last window is ~29/day, nearly double the earlier pace). The PR
queue is effectively empty (4 open, 3 of them dependabot). 207 issues are open,
and their shape matters: ≈78 `kind/design-record`, ≈23 `kind/umbrella`, ≈22
`kind/decision`, and only ≈29 `kind/bug`. The backlog is deliberately-filed
design debt, not defect debt. There has never been a release or tag; #1216
(distribution rehearsal) is the in-flight exception, with publishing
deliberately hard-disabled in `release.yml`.

**The plan against the outcome.** The August plan closed with an audit that
read: four node kinds, two built-in tasks, no composition primitive, no
`flow test`, nothing heartbeats — *"Flowstate is finished as an engine for one
run and empty as a platform for many."* Every clause of that sentence is now
false in the good direction:

| August's audit line | The tree today |
| --- | --- |
| four node kinds, no composition | eight kinds — `task`, `for_each`, `loop`, `parallel`, `wait` (5 arms), `call`, `value`, `switch` — with `call:` compile-time resolved, input-bound, depth- and expansion-bounded, optionally digest-pinned (`workflow.proto:1183`, `flowfile/call.go`) |
| two built-in tasks, nothing else | still two built-ins by design (`log`, `http` — the admission test held), plus six plugins shipping ~14 tasks: `codex`, `git`, `github`, `slack`, `sql`, `vcs` |
| no `flow test` | `flowtest` with stubs, virtual time, per-step and per-arm coverage with `--coverage-required`, seeded schedule exploration, a redaction floor, and 31 CLI verbs around it |
| nothing heartbeats | ten-second heartbeats carrying a closed `v1.Phase` vocabulary, doubling as the cancellation-delivery path (`engine/heartbeat.go`) |

Beyond the plan's own list, the month also shipped: editions with a
comment-preserving rewriter (`flow fix`), webhook triggers with per-scheme
signature verification, `labels:` and CEL `flow list --filter`, concurrency
keys, per-tenant task queues and fairness keys, schedules through backfill,
payload-codec seam, in-process TLS/ACME/mTLS, a server-side audit trail,
workload identity federation verified in CI against GitHub's live issuer, DST
with a seeded scheduler, a replay corpus, a DAP debugger, `flow mcp` with 21
tools, ~80 CI-validated examples, and a conformance suite of 52 shared case
sets whose both-drivers property is enforced structurally — an AST walk fails
any case set not called from both a local and a durable test
(`internal/conformance/callers_test.go:63`).

**What the speed cost.** Three things, all measurable:

1. **The factory stopped measuring itself.** `factory.md`'s retro ledger ends
   at Wave 1 (2026-08-12); main advanced ~900 PRs past it with no recorded
   wave, so the receiver-cost metrics the ledger exists for (clarification
   turns, owner edits, findings accepted vs noise) have one data point.
2. **Docs drifted where code outran them** — in *both* directions, which is the
   interesting failure: VISION.md still calls `flow mcp` "in flight" while its
   own line 178 describes it shipped; THREAT_MODEL.md claims cleartext-only
   HTTP (`:162`), unrecorded allows, and no codec seam, all three now behind
   the tree (`cmd/flow/tls.go`, `cmd/flow/acme.go`, `audit.Recorder.Allow`,
   `pkg/flowstate/v1/payloadcodec`); ARCHITECTURE.md says schedule
   backfill/bounds/catchup are "not surfaced yet" and all three ship;
   DEBUGGING.md says DAP is "the front after MCP" and `flow dap` shipped; and
   AUTHORIZATION_FRESHNESS.md describes per-decision freshness checks whose
   functions have no non-test caller (`auth/protectedresource.go:309,319`) —
   the exact "three surfaces describe it, no code performs it" failure the
   August ledger warned about.
3. **Advisory windows lapsed.** The `appearance` job's `continue-on-error` was
   due out on 2026-08-12 and was still set at the snapshot (#1319; dropped on
   this branch), and the gate's own hazards under parallel waves are open: #1306 (a worktree agent
   cannot tell the gate its merge-base; a nine-PR wave merged unverified) and
   #489 (back-to-back merges can break main with every check green, blocked 19
   days).

## Where the tree stands

One line of strength and the honest thin edge, per subsystem. Evidence is the
snapshot's; the five passes hold the detail.

| Subsystem | Strong | Thin |
| --- | --- | --- |
| **Engine & drivers** | Full step model on both drivers; structural both-callers conformance; bounds on essentially every engine path; pinned interpreter versioning; DST + replay corpus | `call:` is inlining, not a child workflow — one history, one CAN budget; no Temporal Update; `flow watch` polls; search attributes write-only, so listing is a bounded scan (#1384); no entity/unbounded loop (#105); per-step *priority* is a recorded refusal (#657, via #133), with capability routing the open caveat (#156, #1271) — and the owner's expressiveness census (#133, 2026-08-22) calls the Update/child-execution-mode pair "the single highest-leverage open decision in the tracker" |
| **Language & toolchain** | 31 verbs; editions + rewriter; 9 stable diagnostic codes with machine-appliable edits; `flow test` coverage/seeds; DAP debugger; `flow breaking`; generated references pinned in CI | Outputs are untyped (`parse.go:81`) so a callee has no semver-able contract; `call:` can't climb above the caller's directory and can't resolve from bytes (MCP/RPC/unsaved buffers refuse it); CEL type checking stops at `dyn` for reference-only expressions; no `exec:` task yet (decided in DSL.md, unbuilt); no tree-sitter; only Neovim is CI-verified |
| **Governance & identity** | Five CEL policy surfaces sharing one identity vocabulary; OIDC/JWT/mTLS/ACME; WIF with live CI verification; secrets as refs with closure-held values; server-side audit with allow recording | Worker-side decisions (task/secret/egress) reach no audit trail (`main.go:1241` wires the server only); `--egress-policy` is forwarded to exactly two plugins by name (`plugins.go:58,63`) while `github`/`vcs`/`git` compile their own defaults; task-shape policy sees no inputs and its zero case permits everything; scopes are published and enforced by nothing; delegation (`act` chains) refused rather than represented; single issuer key, no revocation |
| **Ingress & the human loop** | Webhooks with HMAC/Stripe verification, idempotency, bounded and timing-levelled; schedules; manual triggers with reasons; `slack.post` outbound | A webhook can only start a run (`server/webhook.go:780`) — it cannot answer a `wait_for_signal:` gate, so the Slack-approval loop is one bridge short; no outbound webhook signing; no event→signal provider capability |
| **Ecosystem** | Plugin protocol with handshake, path hardening, digest pins, host-resolved secrets, caller identity on the wire | Local `--plugin-dir` is the only distribution; no scaffolder or conformance harness (#713); ~1000 duplicated lines across five plugins (#1333); no CEL policy *on* plugin calls; no sandbox tier below the process (T3 documented, not built) |
| **Verification & process** | Diff-scoped gate mirrored into CI's plan/verdict; weekly deep tier (fuzz, soak, 2000-schedule DST, dual-driver examples); hooks; skills with drift-proof mirrors | Factory ledger stalled; #1306/#489 gate hazards; fuzz reproducers self-published against SECURITY.md (#965); merge-queue enforcement unverifiable from the tree; docs/CI.md omits `editors.yml` and `release.yml` |

### By surface

The same tree read the way a person or an agent meets it. One line of strength
and the thin edge per surface; the numbers are the language/toolchain pass's.

| Surface | Strong | Thin |
| --- | --- | --- |
| **CLI** (31 verbs) | A written design language (CLI.md, CLI_DESIGN.md): two audiences, stable exit codes, `-o json` everywhere, printed output pinned by recorded goldens; `init` scaffolds a workflow and its test that pass `validate`/`test`/`fix --check` immediately | The terminal debug session cannot record itself for `flow debug replay` — only the MCP tool can (#928) |
| **MCP** (21 tools) | 18 derived from the service descriptor so a new RPC is a tool on regeneration; six answer with no server (validate, compile, catalog, run_local, test, debug); embedded DSL/catalog/examples resources; an authorized HTTP subset | Cannot validate a composed workflow (#1376); stdio authenticates the process, not the request (#337, #350); validate/compile take a different input convention from the rest (#1290) |
| **LSP** | Diagnostics with machine-appliable `edits`, scope-accurate completion three levels deep, hover, definition, symbols, formatting, code actions; five editors documented; Neovim verified in CI byte-for-byte | No rename, references, semantic tokens or signature help; type checking stops at `dyn` (#1383); VS Code unverified in a real window (#585); no tree-sitter grammar (#135) |
| **API / RPC** | Connect over HTTP/1.1 and HTTP/2, proto-first, every server-backed workflow and schedule verb a projection of an RPC (the process-local verbs, `server`, `worker`, `lsp`, `mcp`, `dap`, `init`, `fix` and the other local-only commands, have none by design), reference docs generated from the descriptors and pinned | Scopes published and enforced by nothing (#1014); no Update or streaming Watch (#133); `list --filter` is a bounded scan (#1384) |
| **DAP** | `flow dap` serves the terminal debugger's session to an editor over stdio, with function breakpoints on step ids; scripted sessions replay | Local driver only (#928 slice 2); line breakpoints are accepted unverified; no VS Code debug-type contribution (#585) |
| **Embedding** | A four-call Go API (`NewTasks`/`Register`/`Install`, `Compile`, `RunLocal`, `RunDurable`) with two registries by design so concurrent embedders do not collide | `Compile` is parse-only, so validate-grade diagnostics need a second call; a Go task registered with a nil descriptor silently opts out of validation, docs and completion |

## The thesis for September

The August gap was between one *run* and one *team*: the organisation's second
workflow copied the first. `call:`, `inputs:`/`outputs:`, and `flow test`
closed it. The same gap has now moved up a level, and it should be named the
same way: **Flowstate is close to finished as a platform for one team, and
empty as a fabric for many.** A second *team* today copies the first —
vendoring files by relative path inside one subtree, with no module identity,
no typed contract to depend on, no way to route a step to a different fleet,
no way to install capability a server fleet didn't build, and no durable call
across a namespace boundary.

That gap is expensive to close and cheap to *sequence*, because every piece
has a contract-level prerequisite that is small now and breaking later. So
September builds contracts and closes perimeter, and designs the rest:

**P1 — Make governance as true as it reads.** The policy *language* story is
essentially complete: one CEL machinery, five decision surfaces, one identity
vocabulary. The *enforcement* story has open seams, each of which reads today
like governance and behaves like advice: the egress snapshot three plugins
never receive, the worker that audits nothing (#1379), the invocation-shape
rules #187 designed and slice 1 left unbuilt (#1378), the scopes nothing
reads (#1014), the freshness doc nothing enforces (#1380). Every one of these
is more urgent than any new capability, because each is a sentence an
operator already believes. Fixing them is mostly small, already-issue-tracked
work (#1332 decides the egress-grant contract before #1321/#1322/#1323
execute it; #1352 and #1336 are open invariant-7-adjacent bugs; the audit
recorder already exists and wants a second wiring point).

**P2 — Close the human loop where humans already live.** The engine's
distinctive primitive is the durable wait; the systems humans answer from are
Slack and webhooks. Outbound landed (`slack.post`, policy-governed). The
missing half is one bridge: a verified inbound delivery answering a
`wait_for_signal:` gate instead of only starting a run — VISION's "webhooks in
both directions", the #350 approval story's transport, and the single
highest-leverage feature of the month because it turns every existing gate
into something a person can answer from where the question was asked. It is
trust-boundary work: signature verification, sender identity shape, and
idempotency are the design, not the garnish.

**P3 — One team to many, contract-first.** In order, because each unlocks the
next and only the first is cheap: (a) **typed workflow outputs** (#1377) —
`type:` on output declarations, the semver-able contract a shared workflow
needs, add-only in the schema and `flow breaking`-aware; (b) **`call:` beyond
the calling file** (#1376) — a resolver decision so MCP, the Compile RPC, and
a monorepo `lib/` can compose (today every byte-based surface refuses); (c)
**module identity** — already mapped by #172 (composition keystone: vendoring
not linking, registry-as-distribution) plus DSL.md's Phase-3 paragraphs;
advance that record rather than open a second one; (d) **capability routing**
— per-step *priority* is a recorded refusal (#657, argued in #133), but
plugins are per-worker, so "steps needing this plugin run there" is capability
routing, not priority; #156's `runs_on:` sketch is the recorded landing zone
and #1271 owns the fleet-homogeneity proof; (e) **the at-scale execution
decisions** — the #133 census elevates Update (with update-with-start) and a
child-execution mode for `call:` as the tracker's highest-leverage open
decision, ahead of Nexus, which stays a design note behind them. None of
(c)–(e) should ship code in September; each should end it with a decided
design.

**P0 — The factory at its own new speed.** ~29 merges/day through a gate with
two known correctness gaps is the tail risk to everything above. #1306 and
#489 first, #1319 and #965 as the cheap signal restorations, #1307 so parallel
waves stop seeing phantom failures, and one honest catch-up entry in
`factory.md` — either resume per-wave measurement or explicitly retire the
ledger to a monthly cadence; a ledger that silently stopped is the worst of
both.

**Explicitly not September**, though each is real and recorded: Nexus
implementation, remote plugin distribution and hosted plugins, WASM, entity
workflows beyond the design round, an `llm` task ahead of #341's design
settling, multi-region, and flipping #1216's release publishing on (rehearse
it; the flip is the owner's call, and "super-alpha" is currently an honest
label). The registry stays two built-ins; `exec:` is the one candidate with a
decided design (DSL.md: built-in, denied by default) and should be scheduled
the week the security-review capacity exists, not squeezed in.

## The week-one slate (2026-09-01 → 09-07)

Bugs and perimeter first, one flagship, contracts opened, designs filed.
Must-land is marked; cut from the bottom, never from must-land.

### Fix (existing issues, in this order)

1. **#1352** — secret material bypassing RPC admission into durable history.
   Invariant 7 is the invariant; this lands before anything else. *(must)* —
   PR #1391, **merged**.
2. **#1336** — plugin handshake token readable in `/proc/<pid>/environ` for
   the process lifetime. *(must)* — PR #1389 (protocol v4), **merged**.
3. **#1306** — let a worktree agent hand the gate its merge-base; the gate is
   the thing 29 merges/day stand on. *(must)* — **merged**, PR #1387; the
   no-scope fallback's other half is #1388.
4. **#1319** — drop the lapsed `continue-on-error` so `appearance` fails
   loudly again; 19 days overdue by its own comment. *(must, trivial)* —
   **landed on this branch**, on the evidence that the job passed on the
   latest `main` runs that exercised it.
5. **#965** — stop the deep tier self-publishing fuzz reproducers in public
   issues, per SECURITY.md's own embargo posture.
6. **#489** — decide the back-to-back-merge answer (merge queue or
   re-verification), even if the fix lands later. — **Decided** (recorded on
   #489): the merge queue. The CI half already shipped in #688 (`merge_group`
   trigger, queue-safe concurrency, the full job set forced); the one step
   left is the ruleset flip in the repository settings, per docs/CI.md.

### Build (PR candidates)

7. **The doc-truth sweep** *(must; one PR, G1-style)* — #1382 enumerates and
   bounds it: THREAT_MODEL.md's three stale gaps (TLS, allow-audit, codec
   seam) plus its narrow egress-coverage list; VISION.md's `flow mcp` line
   moved per its own rule and its plugin wish-list reconciled with the real
   roster; ARCHITECTURE.md's schedule row; DEBUGGING.md's DAP note;
   docs/CI.md's two missing workflows. AUTHORIZATION_FRESHNESS.md is
   deliberately outside the sweep — #1380 owns its implement-or-correct
   decision, since the answer may be code. The claims that are *true* (and
   the audit found many) stay untouched. — **Landed on this branch**, every
   enumerated item re-verified against the merged tree before editing, plus
   one stale code comment the sweep turned up on the way (`cmd/flow/tls.go`
   still said "no ACME here" beside `acme.go`).
8. **Egress to every plugin** — land the #1332 decision (a generic,
   immutable, explicit grant rather than per-plugin env constants), then
   execute #1321/#1322/#1323 so `github`/`vcs`/`git` obey the operator's
   `--egress-policy` exactly as `sql`/`slack` do. *(must)* — decision recorded
   on #1332 (with the default-grant amendment). PR A, #1390, **merged**
   2026-09-02 after three security passes: protocol v5, the SDK installing
   the attested identity and marking header-visible credentials (sticky
   across redirect hops), proxy inputs granted only under
   `proxy_from_environment`, the grant built at `NewHost`. PR B, #1411,
   **merged** 2026-09-02: the deployment default forwarded and marked
   `deployment_default: true`, `git`/`vcs`/`github`/`slack`/`sql` on the SDK
   constructor, `flow mcp`'s deny-all forwarded from the same document its
   own task is built from, protocol v6 because a key a strict parser refuses
   is not additive — the second bump #1393 anticipated. Closes
   #1321/#1322/#1323; the schema-owned launch-input question is #1393.
9. **Worker-side audit** (#1379) — wire the existing `audit.Recorder` through
   `runWorker` so task-policy, secret, egress and assumption decisions
   (allows included) leave the same trail server RPCs already do; #1018's
   decided contract at the second of its two homes, #353's principle 2 made
   true past the RPC boundary. — PR #1394, **merged** 2026-09-02: one record
   per dispatch attempt on both drivers, an interrupted evaluation or an
   unresolved dial recorded as no decision, `THREAT_MODEL.md` gap 7 rewritten
   inside it. Its review left two gaps deliberately outside it: #1397 (a
   permitted redirect chain leaves one record, not one per hop) and #1399 (a
   plugin enforcing the same policy in its own process leaves none).
10. **Typed outputs, schema first** (#1377) — `type:` on output declarations
    (mirroring `inputs:`), compile + validate + both drivers + `flow breaking`
    awareness; the B1-style opener for everything in P3. — PR #1392,
    **merged**; the container-key hole its review found (#1404) is closed by
    #1409, **merged**, with the literal walk bounded at `MaxStructureDepth`.
11. **The webhook→signal bridge** *(flagship; #96 is the design record, and
    its thread now carries the promotion proposal)* — a verified delivery
    answering a declared `wait_for_signal:`/`wait_for_signals:` gate: reuses
    the existing verification schemes and `signals:` authorization, needs a
    sender-identity shape and idempotency across redelivery, conformance
    cases with both drivers as callers, and the `flowstate-security-review`
    pass before merge. **Promoted** (recorded on #96); built as PR #1412,
    which at the time of writing is in its last review round: the security
    pass found and the branch closed a cross-workflow gate answer (a
    trigger's key holder could answer an unpoliced gate in another workflow
    of the tenant) and header-addressed correlation under schemes that do
    not sign headers, and both drivers consume one `ConsumeDeliveryID`. If
    it is not green by the morning it waits, unmerged — a half-bridge at a
    trust boundary is worse than none.
12. **Factory catch-up** — #1386 (the ledger cadence decision), #1307's
    `tools/fleet` reachability, and the docs/CI.md workflow-inventory fix if
    not already inside (7).

### The issue slate, reconciled (dup-checked against the open design records,
then filed or advanced on 2026-08-31; the numbers below are real)

**Filed by this review**, each cross-linked into the records that constrain
it: #1376 (`call:` unresolvable from bytes, and the monorepo `lib/`
boundary), #1377 (output declarations carry no type — half a signature),
#1378 (task-shape rules read `task`+`identity` but not `inputs` — #187's
unbuilt second half), #1379 (worker-side decisions unaudited), #1380
(AUTHORIZATION_FRESHNESS.md: implement or correct), #1381 (netpolicy's
control-plane capability has zero callers), #1382 (the doc-truth sweep,
enumerated), #1383 (thread task descriptors into `env.Check`), #1384
(search-attribute pushdown for `list --filter`), #1385 (compensation
observability through `Get`/timeline), #1386 (the factory ledger cadence).

**Filed by the wave itself** (2026-09-01, each from a review finding or a
measured friction rather than from the census): #1388 (the gate's no-scope
fallback), #1393 (launch inputs as a schema-owned shape; #1398 was a
duplicate filed after a context reset and is closed into it), #1395 (the
signal-carry-bound test trips the deadlock detector on a loaded runner;
fixed by #1410 at the production worker's own budget), #1396 (a sensitive
output's out-of-set value echoed unredacted; fixed inside #1392), #1397
(per-hop egress records), #1399 (plugin-process enforcement decisions
unrecorded), #1404 (a `struct` output keyed by non-strings passed validation
and reached callers in tagged JSON; fixed by #1409).

**Found already claimed — advance, never re-file:** the webhook→signal
bridge is #96 (promotion proposed in its thread); scope enforcement is
#1014; module identity and the catalog are #172 plus DSL.md's Phase-3
paragraphs; the sandbox/exec story is #100/#239 (with #721); the entity
loop and its `state:` byte bound are #105 (DSL.md's sixth-round deferral
names it); capability routing is #156/#1271 under #133's census; the
task-policy zero case is *decided* — opt-in per deployment, argued in
#187's closing design record — and needs no issue.

**Advance:** #337 (delegation/`act` chains, OAuth 2.1 alignment, per-tenant
issuers and key custody), #341 (the agent-loop design; `llm`/`mcp:` stay
behind it), #350 (approvals; the bridge in (11) is its transport), #353
(refresh A.1's status against `payloadcodec` reality), #146 (plugin
vetting/signing), #713 (plugin scaffold + conformance harness), #1333
(plugin SDK dedup before a seventh plugin), #928 (durable debugging; also
owns session recording for terminal runs), #585 (the VS Code extension and
its missing debug-type contribution), #923, #477 (DST depth), #113/#271
(payload offload), #1216 (release rehearsal), #641 (the expressiveness
census — the language-side companion to this review).

**Re-triage:** #133 and #135 turned out to be *actively maintained* — #133
retitled with two refusals recorded and the owner's census on top, #135
corrected in-thread (this review added the one stale line: `flow test`
shipped, #155) — so the real re-triage residue is #134 (both debts appear
still live) and the Jul-31 design wave (#95–#108) for supersession
(#102 vs #715, #104 vs #548).

### Day two (2026-09-02), from the night's state

Landed on day one: #1387, #1402, #1391, #1389, #1372, #1281, #1401, #1392,
#1410, #1409, #1390, #1394, #1411, #1413; #1412 in its last round. In order:

1. **Owner, five minutes:** the merge-queue ruleset flip (#489 is decided;
   the CI half shipped in #688). Then the #1216 rehearsal can run against a
   queue that exists. Publishing stays interlocked as the section above and
   docs/CI.md say: the release flip is a separate, deliberate owner decision
   taken on the rehearsal's evidence, never a step that follows it.
2. **#1393** — the schema-owned launch shape, now that two bumps (5, 6) have
   shown the environment contract moving twice in a day; one builder, the
   third bump is the last one the environment takes.
3. **#1397 + #1399** — the two audit gaps #1394 left on purpose: per-hop
   egress records (with the `UndecidedError` doc nit noted there) and
   plugin-process decisions reaching the worker's recorder, which needs a
   host-facing RPC and so a design memo pair before a builder.
4. **#965** (fix list item 5) and **#1307** — the two day-one items that did
   not start.
5. **Process, one commit:** the container's `PATH` gofmt is the base image's
   build, not the pinned toolchain's (filed from the night); a setup hook or
   the Makefile targets as the only spelling of "formatted".

Not tomorrow: `accepts:` on the bridge (deferred on #96), manifest network
intent (#239), and anything the owner has not seen land yet.

### Definition of done for the week

Items 1–4, 7, and 8 merged; 11 merged *or* explicitly slipped with its design
note filed; the ledger entry written whichever way the ledger question is
decided. Anything else is upside. Success is also measured by what did not
happen: no new capability that skipped its conformance direction, no doc
claim ahead of its code, no growth in the built-in registry.

## Risks

- **Trust-boundary work under velocity.** (8) and (11) both touch enforcement
  paths; each takes the `flowstate-security-review` pass and lands with the
  negative-direction tests, or it waits. The August rule stands: fail-closed
  is reviewed before merge, not after.
- **Schema changes concentrate.** (10) is the only proto change in the slate;
  one owner, `buf breaking` clean, `RunState` rules untouched.
- **The gate's own gaps.** Until #1306/#489 land, parallel waves keep the
  wave-1 discipline: one owner per package, merge order decided up front,
  clean-clone verification on anything that merged behind a suspect gate.
- **The sweep's failure mode is overcorrection.** The doc-truth pass fixes
  only claims verified false against the snapshot; the audit's
  verified-true list ships in the PR description so the reviewer can hold the
  line cheaply.

## Verification

Per slice: the diff-scoped gate (`go run ./tools/gate`) before push, PR CI as
the full gate; conformance case sets carry both driver callers or an
`oneSidedByDesign` entry with its reason; trust-boundary slices add the
security-review pass; doc changes re-run the generation checks
(`go run ./cmd/flow docs generate`, `go generate ./cmd/flow/internal/reference`)
when they touch generated surfaces. Report what ran and what it said; a
skipped leg is not a green one.
