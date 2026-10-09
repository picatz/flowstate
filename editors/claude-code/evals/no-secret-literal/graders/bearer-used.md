---
type: regex
target: { source: file, path: workflow.yaml }
pattern: '(?:bearer|Authorization|X-Api-Key):.*secret\('
flags: i
---
