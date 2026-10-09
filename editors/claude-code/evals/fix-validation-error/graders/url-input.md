---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+url: https://httpbin\.org/status/200\s*$'
flags: m
---
