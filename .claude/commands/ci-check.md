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
go run ./cmd/flow fix --check examples/*/workflow.yaml
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 lint
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#branch=origin/main'
go run github.com/bufbuild/buf/cmd/buf@v1.72.0 generate && git diff --exit-code
GOTOOLCHAIN=go1.26.6 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
GOTOOLCHAIN=go1.26.6 go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...
```

Advisory, run separately, not part of the block above (bounded per CLAUDE.md's
fuzz recipe — a fresh finding is a real defect to triage, not something the
advisory status excuses ignoring):

```
GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzRoundTrip -fuzztime 30s ./pkg/flowstate/v1/flowfile/
```

Notes, per CLAUDE.md:

- `gofmt -l` must print nothing — any output is a failure, not a warning.
- `flow fix --check` on every example is not advisory: the examples are already
  in the current edition, so a failure here is a real regression, not noise.
- `buf generate` followed by `git diff --exit-code` catches committed generated
  code that has drifted from `proto/flowstate/v1/flowstate.proto`.
- `govulncheck` reports reachability against a database fetched at run time, so
  it can go red on a tree nobody touched. Before treating a finding as yours,
  run it against `main` too — if `main` also fails, the advisory arrived rather
  than the code changed, and the fix is a dependency bump owned by everyone,
  not the diff in front of you.
- `staticcheck` is advisory in CI until 2026-08-04 (48h after landing), for the
  same reason: a check run against this tree for the first time can find
  something pre-existing that isn't yours. The fuzz smoke job carries the same
  advisory window, to absorb flake rather than to excuse ignoring a real
  crasher.
- The `GOTOOLCHAIN=go1.26.6` pin matches CI's toolchain; without it, a machine
  honouring a newer `toolchain` directive in `go.mod` can fail with a spurious
  "file requires newer Go version" error from files in the module cache. This
  applies to both govulncheck and staticcheck — each resolves its own
  toolchain via its own `go.mod` otherwise.
