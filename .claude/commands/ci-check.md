---
description: Run the full CI-parity loop locally before pushing (mirrors .github/workflows/ci.yml)
---

Run what CI runs, in CI order. Prefer `make check` once it exists; otherwise run
the commands verbatim, in order, and stop at the first failure:

```
go build ./...
go vet ./...
gofmt -l ./cmd ./pkg                       # must print nothing
GOMEMLIMIT=2GiB go test -race -timeout 900s ./...
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 lint
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#branch=origin/main'
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 generate && git diff --exit-code
GOTOOLCHAIN=go1.26.5 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
```

Notes, per CLAUDE.md:

- `gofmt -l` must print nothing — any output is a failure, not a warning.
- `buf generate` followed by `git diff --exit-code` catches committed generated
  code that has drifted from `proto/flowstate/v1/flowstate.proto`.
- `govulncheck` reports reachability against a database fetched at run time, so
  it can go red on a tree nobody touched. Before treating a finding as yours,
  run it against `main` too — if `main` also fails, the advisory arrived rather
  than the code changed, and the fix is a dependency bump owned by everyone,
  not the diff in front of you.
- The `GOTOOLCHAIN=go1.26.5` pin matches CI's toolchain; without it, a machine
  honouring a newer `toolchain` directive in `go.mod` can fail with a spurious
  "file requires newer Go version" error from files in the module cache.
