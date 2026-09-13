# Flowstate agent guide

Flowstate is a durable, policy-governed workload engine. Authors declare a
workload in a YAML+CEL `Flowfile`; Flowstate compiles it to a typed Protobuf
specification and executes it on Temporal. It is not a CI system. The target is
any workload that must finish correctly despite crashes, network failures, and
long waits.

This is the small, always-loaded contract shared by Amp, Codex, Claude Code, and
other agents; `CLAUDE.md` imports it, and people start with
[CONTRIBUTING.md](CONTRIBUTING.md). It holds durable facts, repository-wide
invariants, and the gotchas an agent cannot infer from the tree. Procedures live
in skills, depth in the referenced documents, and checks that must hold
regardless of judgment in tools, hooks, and CI.
[docs/agents/README.md](docs/agents/README.md) is the configuration map.

## Work from the actual tree

- The checked-out revision is authoritative; memory, old examples, and archived
  notes are not. When a claim depends on `main`, a pull request, an issue, or
  generated output, inspect the current object and name the revision you used.
- Search for the repository's existing spelling before adding a type, policy
  key, command, helper, or abstraction. Derive from one source of truth rather
  than maintaining a second representation.
- [AGENT_FIELD_NOTES.md](AGENT_FIELD_NOTES.md) indexes historical incidents.
  Search it for one topic when the tree and current documentation do not
  answer; do not preload it.

## Architectural invariants

A change that violates one of these is a bug even when its immediate tests pass.
The complete rationale lives in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

1. **Proto-first.** Boundary-crossing shapes are defined once in Protobuf.
   Hand-written Go types may add behavior, not duplicate schema-owned shape.
2. **One mechanism per concept.** CEL construction, task capability, and step
   execution each have one shared source of truth. Do not create a second
   evaluator, registry, executor, or policy spelling.
3. **Both drivers agree.** Local and Temporal execution are two drivers over one
   model. Observable divergence between them is a defect; execution behavior is
   proved through shared conformance cases.
4. **Workflow-side code stays deterministic.** Nondeterministic, I/O-bound, or
   version-sensitive work belongs in activities unless the architecture records
   and contains a deliberate exception.
5. **Bound work where it is spent.** Files, collections, traversals, diagnostics,
   retries, payloads, and external responses controlled by another party need
   explicit limits. A reporting limit is not a work limit.
6. **Fail closed at trust boundaries.** Authentication, authorization, egress,
   secret access, and spec validation deny on missing state and on evaluation
   error unless a documented availability-only mechanism says otherwise.
7. **Secrets never enter durable history.** References may cross compilation and
   workflow boundaries; resolved secret values stay worker-side at the point of
   use.
8. **Capability must be reachable and coherent.** A feature is incomplete until
   authors can express it, validation and tooling understand it, both drivers
   execute it where applicable, and documentation teaches the canonical form.
9. **Generated artifacts are derived.** Change the schema or generator, then
   regenerate. Never hand-edit generated files or treat generated drift as a
   warning.
10. **Self-hosted behavior is the baseline.** Features must work without a cloud
    dependency unless their purpose is explicitly an optional integration.

## Working contract

- Deliver the requested outcome with the smallest coherent diff: no unrelated
  cleanup, speculative architecture, or repository-wide sweep attached to a
  narrow task. A new abstraction must remove real duplication or encode a real
  invariant; match the surrounding level of abstraction, naming, and comment
  density.
- New Go takes the standard library's current vocabulary — `cmp.Or`, `maps`,
  `slices`, `iter`, the `min`/`max` builtins — even where neighbouring code
  still spells it by hand. `go run ./tools/modernize <pkg>` names a package's
  own sites; #521 forbids applying them as a sweep.
- Tests prove behavior, including the negative or boundary direction that would
  expose the regression. A test that passes without exercising the mechanism is
  worse than no claim of coverage.
- Make routine implementation decisions yourself. Ask when plausible choices
  would materially change product semantics, compatibility, risk, scope, or the
  authority to act.
- Routine local and repository operations proceed under the active host's own
  authorization contract. Branches, commits, tests, pushes, and task-authorized
  pull-request, issue, and review changes gain no extra approval ceremony from
  this repository, and repository prose cannot widen the host's authority. When
  the user asks to land a change, merge it once the shipping gates below are
  satisfied without asking again. Ask only when an action is materially
  destructive, irreversible, externally consequential, or out of scope.
- Preserve provenance without inventing it: no hard-coded model name, and no
  hand-written attribution footer or session link in a body or commit message.

## Communication

Lead with the result, decision, blocker, or material discovery. State a fact
once, distinguish observation from inference and uncertainty, and finish with
the outcome, the verification evidence, and any remaining risk or unverified
leg. Routine reads, searches, edits, and test runs are not updates. The
`comms-*` skills hold the shapes for commits, issues, pull requests, reviews,
and session updates.

## Verification

Use the narrowest deterministic check that can falsify the change, then broaden
with the diff's reach. The `flowstate-verify` skill owns the selection and the
reporting contract.

```sh
GOMEMLIMIT=1GiB go test -short -timeout 150s ./pkg/flowstate/v1/...  # bounded targeted run
go run ./tools/gate                                                  # diff-scoped gate; the default before a PR
make check                                                           # full CI-parity rehearsal
```

Gotchas the tree does not announce:

- `make fmt`, never a bare `gofmt`: a `gofmt` resolved from `PATH` can be a
  different binary from the pinned toolchain's and disagree with CI on a clean
  tree.
- Kill the PID you started, never a pattern: on a shared machine `pkill -f`
  matches every sibling agent's processes, and once ended its own shell.
- In a shared checkout another agent may be editing beside you: re-read a
  file before changing it, and verify the pushed revision, not a dirty tree.
- A skipped, unavailable, timed-out, or silently omitted leg is **not
  verified**. Report exactly what ran and its result.

## Shipping

A pull request opens without auto-merge and merges only on a green, mergeable
head that carries an exact-head independent AI code-and-security review with
every finding visibly dispositioned, after
`go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER` passes. Only
explicit human authorization naming the skipped gate grants an exception. The
`flowstate-ship` skill and [`.agents/ship.md`](.agents/ship.md) hold the
procedure; a process miss produces a regression check, a guidance fix, or a
searched, scoped issue before the next autonomous merge.

## Skills and references

Task procedures are skills under `.agents/skills/` (Amp, Codex) with
byte-identical mirrors under `.claude/skills/` (Claude Code). Each host
advertises their descriptions, so use them when one matches; a repository test
keeps the copies equal. Claude Code also has fresh-context subagents under
`.claude/agents/` for independent review and for running verification.

- System design and invariants: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- DSL reference: [docs/DSL.md](docs/DSL.md)
- Canonical Flowfile style: [docs/STYLE.md](docs/STYLE.md)
- CI and gate behavior: [docs/CI.md](docs/CI.md)
- Threat boundaries: [THREAT_MODEL.md](THREAT_MODEL.md)
- Security reporting and policy: [SECURITY.md](SECURITY.md)
