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
   due out on 2026-08-12 and is still set (#1319, `ci.yml:621-631`), and the
   gate's own hazards under parallel waves are open: #1306 (a worktree agent
   cannot tell the gate its merge-base; a nine-PR wave merged unverified) and
   #489 (back-to-back merges can break main with every check green, blocked 19
   days).

## Where the tree stands

One line of strength and the honest thin edge, per subsystem. Evidence is the
snapshot's; the five passes hold the detail.

| Subsystem | Strong | Thin |
| --- | --- | --- |
| **Engine & drivers** | Full step model on both drivers; structural both-callers conformance; bounds on essentially every engine path; pinned interpreter versioning; DST + replay corpus | No per-step routing or priority (tenant-level only); `call:` is inlining, not a child workflow — one history, one CAN budget; no Temporal Update; `flow watch` polls; search attributes write-only, so listing is a bounded scan; no entity/unbounded loop |
| **Language & toolchain** | 31 verbs; editions + rewriter; 9 stable diagnostic codes with machine-appliable edits; `flow test` coverage/seeds; DAP debugger; `flow breaking`; generated references pinned in CI | Outputs are untyped (`parse.go:81`) so a callee has no semver-able contract; `call:` can't climb above the caller's directory and can't resolve from bytes (MCP/RPC/unsaved buffers refuse it); CEL type checking stops at `dyn` for reference-only expressions; no `exec:` task yet (decided in DSL.md, unbuilt); no tree-sitter; only Neovim is CI-verified |
| **Governance & identity** | Five CEL policy surfaces sharing one identity vocabulary; OIDC/JWT/mTLS/ACME; WIF with live CI verification; secrets as refs with closure-held values; server-side audit with allow recording | Worker-side decisions (task/secret/egress) reach no audit trail (`main.go:1241` wires the server only); `--egress-policy` is forwarded to exactly two plugins by name (`plugins.go:58,63`) while `github`/`vcs`/`git` compile their own defaults; task-shape policy sees no inputs and its zero case permits everything; scopes are published and enforced by nothing; delegation (`act` chains) refused rather than represented; single issuer key, no revocation |
| **Ingress & the human loop** | Webhooks with HMAC/Stripe verification, idempotency, bounded and timing-levelled; schedules; manual triggers with reasons; `slack.post` outbound | A webhook can only start a run (`server/webhook.go:780`) — it cannot answer a `wait_for_signal:` gate, so the Slack-approval loop is one bridge short; no outbound webhook signing; no event→signal provider capability |
| **Ecosystem** | Plugin protocol with handshake, path hardening, digest pins, host-resolved secrets, caller identity on the wire | Local `--plugin-dir` is the only distribution; no scaffolder or conformance harness (#713); ~1000 duplicated lines across five plugins (#1333); no CEL policy *on* plugin calls; no sandbox tier below the process (T3 documented, not built) |
| **Verification & process** | Diff-scoped gate mirrored into CI's plan/verdict; weekly deep tier (fuzz, soak, 2000-schedule DST, dual-driver examples); hooks; skills with drift-proof mirrors | Factory ledger stalled; #1306/#489 gate hazards; fuzz reproducers self-published against SECURITY.md (#965); merge-queue enforcement unverifiable from the tree; docs/CI.md omits `editors.yml` and `release.yml` |

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
never receive, the worker that audits nothing, the task-shape zero case that
permits everything, the scopes nothing reads, the freshness doc nothing
enforces. Every one of these is more urgent than any new capability, because
each is a sentence an operator already believes. Fixing them is mostly small,
already-issue-tracked work (#1332 decides the egress-grant contract before
#1321/#1322/#1323 execute it; #1352 and #1336 are open invariant-7-adjacent
bugs; the audit recorder already exists and wants a second wiring point).

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
next and only the first is cheap: (a) **typed workflow outputs** — `type:` on
output declarations, the semver-able contract a shared workflow needs, add-only
in the schema and `flow breaking`-aware; (b) **`call:` from bytes** — a
resolver decision so MCP, the Compile RPC, and hosted authoring can compose
(today every byte-based surface refuses); (c) **module identity** — the
Phase-3 design DSL.md already records (`name@vMAJOR.MINOR.PATCH`, content
digests, a catalog; leaning OCI for distribution) promoted from a paragraph to
a design round; (d) **per-step routing/priorities** — the per-tenant queue
mechanism one level down, already sketched in ARCHITECTURE.md; (e) **Nexus,
consuming first** — the durable cross-namespace boundary, as a design note
only. None of (c)–(e) should ship code in September; all three should end it
with a decided design.

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
   Invariant 7 is the invariant; this lands before anything else. *(must)*
2. **#1336** — plugin handshake token readable in `/proc/<pid>/environ` for
   the process lifetime. *(must)*
3. **#1306** — let a worktree agent hand the gate its merge-base; the gate is
   the thing 29 merges/day stand on. *(must)*
4. **#1319** — drop the lapsed `continue-on-error` so `appearance` fails
   loudly again; 19 days overdue by its own comment. *(must, trivial)*
5. **#965** — stop the deep tier self-publishing fuzz reproducers in public
   issues, per SECURITY.md's own embargo posture.
6. **#489** — decide the back-to-back-merge answer (merge queue or
   re-verification), even if the fix lands later.

### Build (PR candidates)

7. **The doc-truth sweep** *(must; one PR, G1-style)* — THREAT_MODEL.md's
   three stale gaps (TLS, allow-audit, codec seam) refreshed against the tree;
   VISION.md's `flow mcp` line moved per its own rule and its plugin wish-list
   reconciled with the real roster (grpc/discord/docker unbuilt; sql/vcs/codex
   shipped unlisted); ARCHITECTURE.md's schedule row; DEBUGGING.md's DAP note;
   docs/CI.md's two missing workflows; AUTHORIZATION_FRESHNESS.md either
   implemented or corrected to what is enforced — the claims that are *true*
   (and the audit found many) stay untouched.
8. **Egress to every plugin** — land the #1332 decision (a generic,
   immutable, explicit grant rather than per-plugin env constants), then
   execute #1321/#1322/#1323 so `github`/`vcs`/`git` obey the operator's
   `--egress-policy` exactly as `sql`/`slack` do. *(must)*
9. **Worker-side audit** — wire the existing `audit.Recorder` through
   `runWorker` so task-policy, secret and egress decisions (allows included)
   leave the same trail server RPCs already do; #353's principle 2 applied at
   the second of its two homes.
10. **Typed outputs, schema first** — `type:` on output declarations
    (mirroring `inputs:`), compile + validate + both drivers + `flow breaking`
    awareness; the B1-style opener for everything in P3.
11. **The webhook→signal bridge** *(flagship)* — a verified delivery answering
    a declared `wait_for_signal:`/`wait_for_signals:` gate: reuses the
    existing verification schemes and `signals:` authorization, needs a
    sender-identity shape and idempotency across redelivery, conformance
    cases with both drivers as callers, and the `flowstate-security-review`
    pass before merge. If the week runs short, this slips whole rather than
    shipping unverified — a half-bridge at a trust boundary is worse than
    none.
12. **Factory catch-up** — the `factory.md` entry (or its explicit
    retirement), #1307's `tools/fleet` reachability, and the docs/CI.md
    workflow-inventory fix if not already inside (7).

### Design rounds (issues to file — each after a `search_issues` dup-check
against the ≈78 open design records — or to advance where a number exists)

- **File: module identity and the catalog** — promote DSL.md's Phase-3
  paragraphs to a design record: naming, digests, version resolution, and the
  distribution posture (OCI-leaning, per VISION), with the explicit non-goal
  of remote fetch in v1.
- **File: `call:` beyond the subtree** — the resolver contract for bytes-based
  and cross-directory callees; decides what MCP/hosted authoring may compose.
- **File: task-shape policy sees inputs, and the zero case** — today a rule
  cannot say "deny `sql.query` against prod" and an absent file permits every
  task; both halves need a decided posture (the input-visibility half has a
  secrets-adjacency question the design must answer).
- **File: scope enforcement** — scopes are minted and published
  (`authorization.proto`) and read by nothing; enforce at the one place
  actions are already enumerated, or stop publishing them.
- **File: per-step routing and priority** — the ARCHITECTURE.md sketch
  ("the same mechanism one level down") as a decidable design.
- **File: sandbox-provider plugin shape** — the T3 story with `plugins/codex`
  as the worked precedent for fail-closed confinement ceilings.
- **File: entity-loop bounds** — the one acknowledged unbounded engine path
  (carried `state:` byte bound, history-size-aware suspend cadence).
- **Advance:** #337 (delegation/`act` chains, OAuth 2.1 alignment, per-tenant
  issuers and key custody), #341 (the agent-loop design; `llm`/`mcp:` stay
  behind it), #350 (approvals; the bridge in (11) is its transport), #353
  (refresh A.1's status against `payloadcodec` reality), #146 (plugin
  vetting/signing), #713 (plugin scaffold + conformance harness), #1333
  (plugin SDK dedup before a seventh plugin), #928 (durable debugging),
  #923, #477 (DST depth), #113/#271 (payload offload), #1216 (release
  rehearsal).
- **Re-triage:** #133/#134/#135 against a tree ~700 merges newer than their
  text, and the Jul-31 design wave (#95–#108) for supersession (#102 vs
  #715, #104 vs #548).

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
