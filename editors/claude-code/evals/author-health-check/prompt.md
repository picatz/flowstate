---
description: Author a small Flowfile from one paragraph, in the current edition's grammar.
max_turns: 15
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit]
---

I need a Flowstate workflow in `workflow.yaml` in this directory. It should
call https://httpbin.org/status/200 with an HTTP GET, fail the step unless the
response status is 200, and then log the message "service healthy". Name the
workflow `health-check`.
