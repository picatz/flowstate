@AGENTS.md

# Claude Code adapter

`AGENTS.md` is the contract. This file names only what Claude Code adds.

- Skills under `.claude/skills/` mirror `.agents/skills/` byte for byte; edit
  the portable copy first. `.claude/rules/agent-config.md` loads the
  maintenance procedure when you touch either tree.
- Subagents under `.claude/agents/`: delegate to `flowstate-reviewer` for a
  fresh-context code-and-security review of a diff or pull request head (the
  provider-neutral review the shipping gate needs), to `flowstate-verifier`
  to run verification legs without their output filling this context, and to
  `flowstate-pr-tidy` to resolve dispositioned AI review threads and hide
  decision-free bot comments before the attestation.
- Hooks in `.claude/settings.json` are controls, not advice: they refuse edits
  to generated files, pattern kills, and merges without thread evidence, and
  report formatting drift. Treat their output as evidence; other hosts do not
  run them.
- A learning about this codebase that is not repository policy belongs in auto
  memory, not in this file or `AGENTS.md`; a repository rule needs a recurring
  failure or a mechanism (`docs/agents/README.md` has the decision table).
- Source comments that cite a rule in `CLAUDE.md` predate the split into
  `AGENTS.md`; the archived text is indexed by `AGENT_FIELD_NOTES.md`.
