---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+http:\s*\n(?:\s+.*\n)*?\s+url: .*httpbin\.org/status/200'
flags: m
---
