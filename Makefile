.PHONY: check test test-plugins test-fast fmt docs

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
	$(MAKE) test
	$(MAKE) test-plugins
	go run ./cmd/flow fix --check examples/
	go run ./cmd/flow test examples/
	docker compose -f examples/observability/docker-compose.yaml config -q
	go run ./cmd/flow docs generate && git diff --exit-code -- docs/reference/
	go generate ./cmd/flow/internal/reference && git diff --exit-code -- cmd/flow/internal/reference/
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 lint
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#branch=origin/main'
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 generate && git diff --exit-code
	GOTOOLCHAIN=go1.26.5 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
	GOTOOLCHAIN=go1.26.5 go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

# Bounded full test run (no -short). CI's `test` step runs this target rather
# than its own copy of the command, so the bound cannot drift between the two —
# a copy of a command list is a thing that drifts, and the whole point of this
# file is that it is what CI runs.
test:
	GOMEMLIMIT=2GiB go test -race -timeout 900s ./...

# The plugins are separate modules, which is the point of them: `./...` above
# does not reach them, and a plugin that does not compile would leave every
# other check green. Bounded on the same reasoning as `test` — a fuzz-adjacent
# or runaway plugin test should fail with a diagnosable timeout naming its
# package, not consume the job's whole budget and leave an operator guessing
# which module hung.
test-plugins:
	@for module in plugins/*/; do \
		[ -f "$$module/go.mod" ] || continue; \
		echo "==> $$module"; \
		( cd "$$module" && go build ./... && go vet ./... && \
			GOMEMLIMIT=2GiB go test -race -timeout 300s ./... ) || exit 1; \
		fmt_out="$$(gofmt -l $$module)"; \
		if [ -n "$$fmt_out" ]; then echo "gofmt: $$fmt_out"; exit 1; fi; \
	done

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
