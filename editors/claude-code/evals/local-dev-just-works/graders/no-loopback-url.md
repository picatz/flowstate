---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '(?:localhost|127\.0\.0\.1|\[::1\])'
flags: i
match: not_contains
---
