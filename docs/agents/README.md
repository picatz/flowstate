# Agent configuration

Flowstate supports Claude Code, Codex, and Amp without making any one host's
prompt format the repository's source of truth. The design is layered so agents
receive a small durable contract, load procedures only when relevant, and rely on
software rather than prose for checks that can be deterministic.

## The layers

| Artifact | Role | Loading behavior |
| --- | --- | --- |
| `AGENTS.md` | Shared repository-wide facts, invariants, and working contract | Always loaded by Amp and Codex; imported by Claude Code |
| `CLAUDE.md` | Thin Claude Code adapter | Always loaded by Claude Code |
| `.agents/skills/*/SKILL.md` | Portable task workflows for Amp and Codex | Name and description are advertised; the body loads when selected |
| `.claude/skills/*/SKILL.md` | Claude Code mirrors of the portable skills | The body loads when selected |
| `.claude/settings.json` | Claude-specific hooks, the allow list that stops the repository's own verification commands from prompting, and the two official security plugins the repository enables for every clone and cloud session, pinned to the Anthropic-owned marketplace they resolve from | Enforced by Claude Code |
| `.claude/agents/*.md` | Fresh-context subagents carrying the review, verification, and review-hygiene rubrics (`flowstate-reviewer`, `flowstate-verifier`, `flowstate-pr-tidy`, `flowstate-scout`), each pinned to a model and effort by the `flowstate-delegate` skill | Claude Code delegates on the description; the body and preloaded skills load in the subagent's own context |
| `.claude/rules/*.md` | Path-scoped procedures, currently only the agent-configuration maintenance rule | Loaded when Claude Code reads or edits a file matching the rule's `paths` |
| `REVIEW.md` | The `comms-review` rubric in the one form Anthropic's hosted Code Review can read: what Important means here, the nit cap, the generated surfaces to skip, and the disposition rule that keeps a re-review from relitigating | Read by Code Review on a pull request |
| `.claude/claude-security-guidance.md` | Flowstate's trust boundaries and the four invariants a security reviewer most often gets wrong here, for the `security-guidance` plugin's model-backed reviews | Loaded by the plugin's model-backed reviews |
| `.amp/settings.json` | Amp workspace skill selection; no repository-specific permission prompts | Applied by Amp in this repository |
| `.agents/ship.md` | Versioned Amp Custom Ship procedure | Copied into the Amp project setting; it is not read automatically after edits |
| `.agents/setup` / `.agents/resume` | Amp Orb provisioning and wake behavior | Run by the Orb lifecycle |
| `tools/agentconfig` | Structural tests for this configuration | Run by the full Go test suite and selected by the diff-scoped gate for repository-owned agent configuration |
| `tools/citations` | Checks every `path.go:NNN` citation in the hand-written documentation against the tree: the file exists, the line is inside it, and the cited lines hold the symbol the sentence names | Run by the full Go test suite and selected by the diff-scoped gate for a change under `docs/`, `README.md` or `AGENTS.md` |
| `tools/doclinks` | Checks every doc link (`[Name]`, `[pkg.Name]`, `[import/path.Name]`) in Go package and exported-declaration comments resolves the way pkg.go.dev and gopls render it, rather than showing literal brackets | Run by the full Go test suite |

## Why skills are mirrored

Amp and Codex discover skills in `.agents/skills/`; Claude Code in
`.claude/skills/`. Every skill exists in both with identical bytes, so no host
needs a forwarding instruction or an extra read. Edit the portable copy first,
copy it to the Claude location, and run `go test ./tools/agentconfig`, which
rejects a one-sided edit, a name that does not match its directory, and an empty
description. The diff-scoped gate selects that package for `.agents/`,
`.claude/`, `.amp/`, and the root adapters.

## What belongs where

Use this decision rule when an agent or reviewer proposes another instruction:

| Kind of information or behavior | Put it here |
| --- | --- |
| Non-obvious fact needed in nearly every task | `AGENTS.md` |
| Package- or subtree-specific invariant | A nested `AGENTS.md`; add a Claude-scoped rule only when the same behavior cannot be expressed portably |
| Repeatable procedure, rubric, or artifact contract | A skill |
| Detailed architecture, examples, or historical incident | A referenced document |
| Exact parsing, discovery, validation, or transformation | A script or typed tool |
| A text or command sequence agents keep getting wrong (an attestation body, a pinned merge) | A tool that prints or performs it, such as `tools/shipcheck --attest` and `tools/shipmerge` |
| Action that must be blocked or checked regardless of model judgment | Hook, permission rule, sandbox, or CI |
| An independent judgment, or a procedure whose output should not share the main context | A Claude subagent under `.claude/agents/` preloading the relevant skill; other hosts run the skill inline |
| A procedure that matters only while particular files are open, and only on Claude Code | `.claude/rules/*.md` with `paths:`; a portable equivalent is a nested `AGENTS.md` |
| A learning from one session that is not repository policy | The host's auto memory, not a repository file |
| One task's outcome, scope, and acceptance criteria | The current prompt |
| Personal tone, model, verbosity, or account preference | User-level host configuration, not this repository |

Do not add an instruction merely because one session failed. First ask whether
the failure is recurring, whether the current tree already expresses the answer,
and whether a deterministic mechanism can prevent it more reliably.

## Host-specific notes

### Amp

- `.agents/ship.md` is the versioned Custom Ship prompt. Amp stores a copy
  rather than reading the file at Ship time, so synchronize every accepted edit:

  ```sh
  amp projects update picatz/flowstate --ship-behavior custom \
    --custom-ship-prompt-file .agents/ship.md
  ```

- `.amp/settings.json` disables Claude-skill fallback so Amp uses the portable
  copy only. Flowstate adds no repository-specific Amp approval prompts.

### Claude Code

- `.claude/settings.json` hooks guard generated files, process cleanup, merge
  review state, and formatting; SessionStart builds them once into the ignored
  `.claude/hooks/.bin`. Enabled plugins and the hook binaries are trusted
  without a pin or a digest, by decision:
  [THREAT_MODEL.md](../../THREAT_MODEL.md#agent-host-to-checkout) records why.
  Other hosts do not run these tool events.
- `permissions.allow` pre-approves the build, test, format, gate, and
  regeneration commands the repository prescribes; `tools/agentconfig` rejects a
  wildcard, push, merge, commit, or destructive entry. It assumes a checkout the
  operator trusts: on a head you do not trust, review by reading and leave
  running its code to CI.
- `.claude/agents/` holds the fresh-context subagents (`flowstate-reviewer`,
  `flowstate-verifier`, `flowstate-pr-tidy`, `flowstate-scout`); the
  `flowstate-delegate` skill holds the routing and `tools/agentconfig` pins it.
- Auto memory lives outside the repository; session learnings go there, not in
  `CLAUDE.md`.

### Codex

- `AGENTS.md` is the instruction source; skills load by description. The
  repository claims no Claude hook parity in Codex: port a guard only after its
  tool name, input shape, and blocking behavior are covered by an end-to-end
  test.

## Maintaining the system

1. Reproduce the process failure and identify the smallest layer that owns it.
2. Before the next autonomous merge, add a regression check where feasible,
   update the owning guidance, or file a searched, scoped issue.
3. Prefer deletion or movement over repeating the instruction in another file.
4. Update both skill mirrors and run `go test ./tools/agentconfig`.
5. After a major model or harness upgrade, audit old instructions for ritualized
   verification, mandatory narration, and assumptions the new host no longer
   needs.

The success metric is not how much guidance the repository contains. It is
whether agents produce correct, reviewable work while consuming less context.
