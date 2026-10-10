---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '^use:[ \t]*(?:[^\s#]|\n[ \t]*-)'
flags: m
match: not_contains
---
