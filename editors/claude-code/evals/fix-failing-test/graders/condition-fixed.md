---
type: regex
target: { source: file, path: workflow.yaml }
pattern: 'if: \$\{\s*steps\.fetch\.status_code\s*(?:>=\s*500|>\s*499)\s*\}'
---
