---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '"team"[\s\S]*"team"|"5000"[\s\S]*"5000"'
match: not_contains
---
