---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+uri:'
flags: m
match: not_contains
---
