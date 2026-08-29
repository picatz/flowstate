# Agent field notes

This is an index of historical agent guidance and incident-derived lessons. It is
**reference material, not active policy**. `AGENTS.md`, the current source tree,
current documentation, deterministic tools, hooks, and CI take precedence when
an archived note conflicts with them.

Search and read only the section relevant to the current question. Do not load the
archive wholesale merely because it exists. Paths inside archived files are
written relative to the repository layout they originally occupied.

## Repository-wide archive

- [Legacy Claude guidance](AGENT_FIELD_NOTES_LEGACY.md) — the former root
  `CLAUDE.md`, preserved byte-for-byte when the always-loaded configuration was
  reduced.

## Archived task guidance

The former Claude-specific skill and command bodies remain at matching directory
depths so their relative paths still resolve. They preserve incidents and
exemplars; the active skills define the current host-neutral workflow.

- [Commit communication](.agent-history/skills/comms-commit/SKILL.md)
- [Issues and design passes](.agent-history/skills/comms-issue/SKILL.md)
- [Pull request communication](.agent-history/skills/comms-pr/SKILL.md)
- [Review communication](.agent-history/skills/comms-review/SKILL.md)
- [Session updates](.agent-history/skills/comms-session/SKILL.md)
- [Pre-PR review](.agent-history/skills/pre-pr-review/SKILL.md)
- [Flowfile style](.agent-history/skills/flowfile-style/SKILL.md)
- [Both-driver command](.agent-history/commands/both-drivers.md)
- [Full CI command](.agent-history/commands/ci-check.md)
- [Fast test command](.agent-history/commands/test-fast.md)
