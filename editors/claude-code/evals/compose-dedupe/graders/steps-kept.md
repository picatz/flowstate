---
type: regex
target: { source: file, path: workflow.yaml }
pattern: 'id: limit_api\b[\s\S]*"api limit [\s\S]*id: limit_export\b[\s\S]*"export limit [\s\S]*id: limit_upload\b[\s\S]*"upload limit [\s\S]*id: limit_search\b[\s\S]*"search limit '
---
