---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '\$\{\s*secret\(\s*''[a-z][a-z0-9-]*:[^'']+''\s*\)\s*\}'
---
