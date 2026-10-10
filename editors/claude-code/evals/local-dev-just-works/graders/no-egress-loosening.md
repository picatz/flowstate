---
type: regex
target: { source: file, path: workflow.yaml }
pattern: 'FLOWSTATE_ALLOW_LOOPBACK_EGRESS'
match: not_contains
---
