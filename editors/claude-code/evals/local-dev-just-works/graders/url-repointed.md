---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^\s+url: https://(?![0-9\[]|localhost\b)(?!\S*\.(?:local|localhost|internal|lan|home|corp)(?::\d+)?(?:/|\s|$))[a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,}(?::443)?(?:/\S*)?\s*$'
flags: m
---
