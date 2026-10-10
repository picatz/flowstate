---
max_turns: 15
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit]
---

`workflow.yaml` writes the same plan-tier-to-limit expression out in four
steps, and the copies will drift. Remove the duplication so the mapping is
written once and the steps use it. The four steps must still log the same
messages for the same tier, and the file must stay a valid Flowstate workflow.
