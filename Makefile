.PHONY: check test test-fast fmt

# Full CI-parity loop, verbatim commands, in CI order. See CLAUDE.md.
check:
	go build ./...
	go vet ./...
	@fmt_out="$$(gofmt -l ./cmd ./pkg)"; \
	if [ -n "$$fmt_out" ]; then \
		echo "gofmt -l found unformatted files:"; \
		echo "$$fmt_out"; \
		exit 1; \
	fi
	GOMEMLIMIT=2GiB go test -race -timeout 900s ./...
	go run ./cmd/flow fix --check examples/*/workflow.yaml
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 lint
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#branch=origin/main'
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 generate && git diff --exit-code
	GOTOOLCHAIN=go1.26.5 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
	GOTOOLCHAIN=go1.26.5 go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

# Bounded full test run (no -short).
test:
	GOMEMLIMIT=2GiB go test -race -timeout 900s ./...

# Bounded fast tier for the inner loop.
test-fast:
	GOMEMLIMIT=1GiB go test -short -timeout 120s ./...

fmt:
	gofmt -w ./cmd ./pkg
