---
type: regex
target: { source: file, path: workflow.yaml }
pattern: 'steps\.pong'
match: not_contains
---
