@AGENTS.md

# Claude Code adapter

`AGENTS.md` is the contract. This file names only what Claude Code adds.

- Subagents under `.claude/agents/` carry the independent review
  (`flowstate-reviewer`, the review the shipping gate needs), verification,
  lookups, and pull-request tidying; their descriptions say when to delegate.
- Hooks in `.claude/settings.json` are controls, not advice: they refuse edits
  to generated files, pattern kills, and merges without thread evidence, and
  report formatting drift. Treat their output as evidence; other hosts do not
  run them.
- A learning about this codebase that is not repository policy belongs in auto
  memory, not in this file or `AGENTS.md`; a repository rule needs a recurring
  failure or a mechanism (`docs/agents/README.md` has the decision table).
