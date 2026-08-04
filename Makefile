.PHONY: check test test-fast fmt docs

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
	@# The plugins are separate modules, so `./...` above does not reach them.
	@# A plugin that does not compile would leave every check above green.
	@for module in plugins/*/; do \
		[ -f "$$module/go.mod" ] || continue; \
		echo "==> $$module"; \
		( cd "$$module" && go build ./... && go vet ./... && \
			GOMEMLIMIT=2GiB go test -race -timeout 300s ./... ) || exit 1; \
		fmt_out="$$(gofmt -l $$module)"; \
		if [ -n "$$fmt_out" ]; then echo "gofmt: $$fmt_out"; exit 1; fi; \
	done
	go run ./cmd/flow fix --check examples/*/workflow.yaml
	docker compose -f examples/observability/docker-compose.yaml config -q
	go run ./cmd/flow docs generate && git diff --exit-code -- docs/reference/
	go generate ./cmd/flow/internal/reference && git diff --exit-code -- cmd/flow/internal/reference/
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

# Regenerate the reference documentation under docs/reference/ from the registry,
# the cobra tree, the MCP tool table and the env-var table. CI pins the result
# with `git diff --exit-code`, so this is what to run when that pin fails.
docs:
	go run ./cmd/flow docs generate
