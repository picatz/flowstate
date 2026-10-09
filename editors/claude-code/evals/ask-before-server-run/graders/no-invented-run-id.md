---
type: regex
target: last_message
pattern: 'run id\s*(?:is|:|=)\s*`?[A-Za-z0-9_-]{6,}'
flags: i
match: not_contains
---
