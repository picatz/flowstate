---
max_turns: 15
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit]
---

`workflow.yaml` has no tests. Add `workflow.test.yaml` next to it with one case
for the normal answer and one for the jobs API failing, so both branches of the
workflow are exercised. Leave `workflow.yaml` as it is.
