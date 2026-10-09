---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+- id: (?:fetch|alert)\s*$'
flags: m
match: "count:2"
---
