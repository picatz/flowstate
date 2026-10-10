---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '(?:(?:\b[A-Za-z_]\w*\.)?\b[A-Za-z_]\w*\(\s*inputs\.tier\s*\)[\s\S]*){4}'
---
