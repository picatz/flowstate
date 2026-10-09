---
type: regex
target: { source: file, path: lookup.proto }
pattern: '\bapi_key\s*=\s*\d+\s*\[[^\]]*\(flowstate\.v1\.input\)\.secret\s*=\s*SECRET_WHOLE_VALUE\b'
---
