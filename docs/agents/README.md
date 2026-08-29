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
| `.claude/settings.json` | Claude-specific hooks and permission-time controls | Enforced by Claude Code |
| `.amp/settings.json` | Amp workspace permissions for high-impact or destructive actions | Applied by Amp in this repository |
| `.agents/setup` / `.agents/resume` | Amp Orb provisioning and wake behavior | Run by the Orb lifecycle |
| `AGENT_FIELD_NOTES.md` | Small index of historical guidance | Never imported; used to locate one relevant archive |
| `AGENT_FIELD_NOTES_LEGACY.md` and `.agent-history/` | Byte-preserved legacy guidance and skill bodies | Historical reference, loaded only for a concrete question |
| `tools/agentconfig` | Structural tests for this configuration | Run by the Go test suite and repository gate |

This follows each host's current discovery contract:

- Amp loads repository `AGENTS.md` files and project skills from
  [`.agents/skills/`](https://ampcode.com/docs/customize/skills). It recommends a
  general top-level `AGENTS.md` and more specific guidance closer to a subtree.
  See [Amp's AGENTS.md documentation](https://ampcode.com/docs/customize/agents-md).
- Codex loads `AGENTS.md` from the repository hierarchy and uses skills for
  reusable workflows. See [Codex AGENTS.md](https://developers.openai.com/codex/agent-configuration/agents-md)
  and [Build skills](https://developers.openai.com/codex/build-skills).
- Claude Code loads `CLAUDE.md` and project skills from `.claude/skills/`.
  Anthropic recommends keeping always-loaded instructions concise and moving
  procedures to skills. See [Claude memory](https://code.claude.com/docs/en/memory)
  and [Claude skills](https://code.claude.com/docs/en/skills).

## Why skills are mirrored

Amp and Codex prefer `.agents/skills/`; Claude Code discovers project skills in
`.claude/skills/`. Every Flowstate skill therefore exists in both locations with
identical bytes. `tools/agentconfig/agentconfig_test.go` verifies:

- both directories contain the same skill names;
- every `name` matches its directory;
- every skill has a useful `description`;
- the corresponding `SKILL.md` files are byte-identical.

The mirror avoids host-specific forwarding instructions and an extra read when a
skill activates. Edit the portable copy first, copy it to the Claude location,
and run:

```sh
go test ./tools/agentconfig
```

The test is the mechanism that prevents the two copies from becoming competing
sources of truth.

## What belongs where

Use this decision rule when an agent or reviewer proposes another instruction:

| Kind of information or behavior | Put it here |
| --- | --- |
| Non-obvious fact needed in nearly every task | `AGENTS.md` |
| Package- or subtree-specific invariant | A nested `AGENTS.md`; add a Claude-scoped rule only when the same behavior cannot be expressed portably |
| Repeatable procedure, rubric, or artifact contract | A skill |
| Detailed architecture, examples, or historical incident | A referenced document |
| Exact parsing, discovery, validation, or transformation | A script or typed tool |
| Action that must be blocked or checked regardless of model judgment | Hook, permission rule, sandbox, or CI |
| One task's outcome, scope, and acceptance criteria | The current prompt |
| Personal tone, model, verbosity, or account preference | User-level host configuration, not this repository |

Do not add an instruction merely because one session failed. First ask whether
the failure is recurring, whether the current tree already expresses the answer,
and whether a deterministic mechanism can prevent it more reliably.

## Host-specific notes

### Amp

- Inspect active guidance with `agents-md list` from the command palette.
- Inspect skill precedence with `amp skills list`; `.agents/skills/` wins over the
  `.claude/skills/` compatibility location for repository skills. Workspace
  settings disable Claude-skill fallback so Amp uses the portable copy only.
- `.agents/setup` provisions the pinned toolchain and dependencies for a fresh
  Orb. `.agents/resume` stays fast because Flowstate has no persistent backing
  service or authentication state to repair on wake.
- Flowstate intentionally lets Amp perform routine reversible repository work
  autonomously, including pushing branches and creating or updating pull
  requests, issues, and review comments. `.amp/settings.json` reserves approval
  prompts for high-impact actions such as merging, publishing releases, or
  destructively rewriting local state. Broader permission examples can be kept
  in documentation if a future project needs a stricter posture; they are not
  the default here.
- Amp-specific controls do not make Claude hooks portable; add an Amp control
  only when a behavior genuinely needs enforcement on that host.

### Claude Code

- Verify the loaded memory files and skills with `/context`.
- Use `/doctor` when always-loaded guidance grows or becomes inconsistent.
- The repository's `.claude/settings.json` hooks guard generated files, process
  cleanup, merge review state, and formatting. Other hosts do not run them.
- Legacy `.claude/commands/ci-check.md` and `test-fast.md` remain only as short
  compatibility aliases for the `flowstate-verify` skill. New procedures belong
  in skills.

### Codex

- `AGENTS.md` is the repository instruction source; do not configure Codex to
  fall back to the large historical notes file.
- Invoke project skills explicitly when useful or let their descriptions route
  the task. Keep personal model effort and visible verbosity in user-level Codex
  configuration.
- The repository does not currently claim Claude hook parity in Codex. The
  existing generated-file, formatting, process, and merge guards were written and
  tested against Claude Code's tool events. Port a guard only after its Codex tool
  name, input shape, and blocking behavior are covered by an end-to-end test;
  pretending a hook is portable would be worse than naming the gap.
- Repository CI, not an agent's summary, is the final authority for mergeability.

## Maintaining the system

1. Reproduce the recurring failure and identify the smallest layer that owns it.
2. Propose the change before allowing an agent to persist a new rule.
3. Prefer deletion or movement over repeating the instruction in another file.
4. Update both skill mirrors and run `go test ./tools/agentconfig`.
5. Test the behavior on representative coding, review, and communication tasks.
6. After a major model or harness upgrade, audit old instructions for ritualized
   verification, mandatory narration, fixed reasoning sequences, and assumptions
   the new host no longer needs.

The migration preserves the former root `CLAUDE.md` as
`AGENT_FIELD_NOTES_LEGACY.md` and the replaced Claude skills and commands under
`.agent-history/`. They remain searchable evidence without occupying every
session or silently acting as current policy.

The success metric is not how much guidance the repository contains. It is
whether agents produce correct, reviewable work while consuming less context and
leaving less verification and interpretation work for the next person.