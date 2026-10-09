---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+log:\s*\n\s+message: .*service healthy'
flags: m
---
