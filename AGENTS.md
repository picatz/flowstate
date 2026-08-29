# Flowstate agent guide

Flowstate is a durable, policy-governed workload engine. Authors declare a
workload in a YAML+CEL `Flowfile`; Flowstate compiles it to a typed Protobuf
specification and executes it on Temporal. It is not a CI system. The target is
any workload that must finish correctly despite crashes, network failures, and
long waits.

This is the small, shared, always-loaded contract for Amp, Codex, Claude Code,
and other agents. `CLAUDE.md` imports it for Claude Code. Keep durable facts and
repository-wide constraints here; put task procedures in skills, detailed
knowledge in references, computation in tools, and hard controls in hooks or CI.
See [docs/agents/README.md](docs/agents/README.md) for the configuration map.

## Work from the actual tree

- Read the relevant implementation, tests, and current documentation before
  making a claim. The checked-out revision is authoritative; memory and old
  examples are not.
- Search for the repository's existing spelling before adding a type, policy
  key, command, helper, or abstraction. Prefer deriving from one source of truth
  over maintaining a second representation.
- Load context progressively. Start with this file and the files the task
  touches. Read deeper references when they answer a concrete question.
- Historical incidents and detailed operational lessons live in
  [AGENT_FIELD_NOTES.md](AGENT_FIELD_NOTES.md). Search that file for the relevant
  topic; do not preload or summarize the whole archive.
- When a task depends on `main`, a pull request, an issue, or generated output,
  inspect the current object and name the revision or state you actually used.

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

- Deliver the requested outcome with the smallest coherent diff. Do not attach
  unrelated cleanup, speculative architecture, or a repository-wide sweep to a
  narrow task.
- Use judgment rather than ritual. Examples, review comments, and old agent notes
  are evidence to verify, not instructions to obey after the tree has changed.
- A new abstraction must remove real duplication or encode a real invariant.
  Match the surrounding level of abstraction, naming, and comment density.
- Tests should prove behavior, including the negative or boundary direction that
  would expose the regression. A test that passes because it never exercised the
  mechanism is worse than no claim of coverage.
- Make routine implementation decisions yourself. Ask when plausible choices
  would materially change product semantics, compatibility, risk, scope, or the
  authority to act.
- Work autonomously on routine local and repository operations to the extent the
  active host permits. Branches, commits, tests, ordinary edits, and pushes
  should not gain extra approval ceremony from this repository. For shared or
  external state such as pull requests, issues, and review comments, act when
  the user's current request grants that authority and otherwise follow the
  host's own authorization contract; repository prose cannot widen it. Ask
  before actions that are materially destructive, irreversible, or high-impact,
  such as merging, publishing a release, deleting important state, deploying,
  or changing production/security controls. Host-specific hooks and permissions
  are additional safety controls, not a substitute for judgment.
- Preserve accurate provenance without inventing it. Do not hard-code a model
  name or duplicate attribution the active host adds automatically.

## Receiver-effort standard

Think as much as the task deserves; publish what the recipient needs.

- Lead with the result, decision, blocker, or material discovery.
- Do not narrate routine reads, searches, edits, or test execution as a progress
  bar. Update only when the plan changes or the user's mental model should change.
- State a fact once. Depth belongs in evidence, not repeated conclusions.
- Distinguish observation, inference, and uncertainty. Never use polished prose
  to make an unverified claim sound settled.
- Finish with the outcome, concrete verification evidence, and any remaining
  risk or unverified leg. Do not make the reader reconstruct what happened.

## Verification

Use the narrowest deterministic check that can falsify the change, then broaden
with the risk and scope of the diff.

```sh
# Bounded targeted example
GOMEMLIMIT=1GiB go test -timeout 120s ./pkg/flowstate/v1/...

# Diff-scoped repository gate; the default before opening or updating a PR
go run ./tools/gate
# equivalent: make gate

# Full CI-parity rehearsal when the scope or task warrants it
make check
```

- Use `make fmt`, not a bare repository-wide `gofmt`; the Make target owns the
  generated-code exclusions.
- Bound standalone tests and fuzzers by time and memory. Do not leave background
  test binaries or servers behind; terminate the PID you started, never every
  matching process on the machine.
- Run generation checks when schemas or generated surfaces may have changed.
- Report exactly what ran and its result. A skipped, unavailable, timed-out, or
  silently omitted leg is **not verified**, not green.
- Before handing off, inspect the final diff for scope, generated drift, stray
  files, and claims unsupported by the command results.
- In a shared checkout, do not overwrite another agent's work. Re-read files that
  may have changed, coordinate ownership, and verify the pushed revision rather
  than inferring from a dirty local tree.

## On-demand skills

Amp and Codex discover the portable skills under `.agents/skills/`. Claude Code
loads byte-identical mirrors under `.claude/skills/`; a repository test prevents
those copies from drifting.

| Skill | Use it for |
| --- | --- |
| `flowstate-verify` | Choosing and reporting bounded targeted, gate, or full verification |
| `both-drivers` | Any behavior observable in local and Temporal execution |
| `flowfile-style` | Writing or reviewing Flowfiles and language-surface proposals |
| `pre-pr-review` | Evidence-based self-review before opening or updating a PR |
| `flowstate-security-review` | Security review, threat analysis, and validating findings |
| `comms-commit` | Commit subjects and bodies |
| `comms-issue` | GitHub issues and design proposals |
| `comms-pr` | Pull request descriptions and updates |
| `comms-review` | Code-review findings, responses, and thread disposition |
| `comms-session` | Concise status and handoff updates during agent work |

## Reference map

- System design and invariants: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- DSL reference: [docs/DSL.md](docs/DSL.md)
- Canonical Flowfile style: [docs/STYLE.md](docs/STYLE.md)
- CI and gate behavior: [docs/CI.md](docs/CI.md)
- Threat boundaries: [THREAT_MODEL.md](THREAT_MODEL.md)
- Security reporting and policy: [SECURITY.md](SECURITY.md)
- Deep historical and operational notes:
  [AGENT_FIELD_NOTES.md](AGENT_FIELD_NOTES.md)
