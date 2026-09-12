---
paths: ["AGENTS.md", "CLAUDE.md", ".agents/**", ".claude/**", "docs/agents/**", "tools/agentconfig/**"]
---

# Editing the agent configuration

- `AGENTS.md` is the shared contract; `CLAUDE.md` names only what Claude Code
  adds. `tools/agentconfig` bounds both and fails a change that grows them past
  the bound, so a new always-loaded line displaces an old one.
- Edit a skill in `.agents/skills/<name>/SKILL.md` first and copy it byte for
  byte to `.claude/skills/<name>/SKILL.md`; the test rejects a one-sided edit.
  Frontmatter that changes how a skill runs on Claude Code (`context`, `agent`,
  `hooks`, `allowed-tools`, `model`) does not belong in a mirrored skill;
  `argument-hint` and `$ARGUMENTS` are fine, since a host that lacks them
  ignores the hint and reads the placeholder as blank. Claude-only behavior
  goes in `.claude/agents/`, `.claude/rules/`, or `.claude/settings.json`.
- Run `go test ./tools/agentconfig` before handing off. The diff-scoped gate
  selects it for any of these paths.
- Before adding a rule, ask whether the tree already answers it, whether the
  failure recurs, and whether a hook or test can enforce it instead. A learning
  that is not repository policy goes to auto memory. `docs/agents/README.md`
  has the decision table.
