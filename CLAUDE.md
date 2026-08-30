@AGENTS.md

# Claude Code adapter

The shared repository contract is `AGENTS.md`. Keep this file as a thin Claude
Code adapter rather than a second instruction manual.

- Project workflows live in `.claude/skills/`. They are byte-identical mirrors
  of the portable skills in `.agents/skills/` so Claude, Amp, and Codex receive
  the same task-specific guidance.
- `.claude/settings.json` contains Claude-specific hooks. Treat those hooks as
  controls and their output as evidence; do not replace them with prose or assume
  another host ran them.
- Use the receiver-effort standard from `AGENTS.md`: no routine progress
  narration, no repeated recap, and no inflated certainty.
- Existing source comments that cite a detailed rule in `CLAUDE.md` predate this
  split; follow those citations through to `AGENT_FIELD_NOTES.md`. Do not import
  that archive wholesale. Search and read only the relevant section when the
  current tree and primary documentation do not answer the question.
