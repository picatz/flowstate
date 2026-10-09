---
max_turns: 15
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit]
---

`workflow.yaml` in this directory fails `flow validate`. Fix it so it is a
valid Flowstate workflow, keeping what it does: ping the URL, require a 200,
then log how the ping answered.
