---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+url: https://(?!(?:localhost|127\.|\[?::1))\S+\s*$'
flags: m
---
