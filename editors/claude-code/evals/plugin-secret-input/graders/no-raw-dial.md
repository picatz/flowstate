---
type: regex
target: { source: file, path: main.go }
pattern: '\bnet\.Dial|\bnet\.Dialer|http\.DefaultClient|http\.Client\{|http\.Get\(|http\.DefaultTransport'
match: not_contains
---
