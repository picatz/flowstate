---
max_turns: 15
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit]
---

`workflow.test.yaml` is failing: the case "a 500 answer raises the alert" says
the `alert` step should run when the jobs API answers 500, and it does not.
Find the cause and fix it. The test describes the behavior we want, so leave
the test file as it is.
