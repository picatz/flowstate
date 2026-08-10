.PHONY: check test test-plugins test-ordering test-fast fuzz-smoke fmt docs appearance appearance-update

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
	$(MAKE) test-ordering
	go run ./cmd/flow fix --check examples/
	go run ./cmd/flow test --coverage-required examples/
	$(MAKE) fuzz-smoke
	$(MAKE) appearance
	docker compose -f examples/observability/docker-compose.yaml config -q
	go run ./cmd/flow docs generate && git diff --exit-code -- docs/reference/
	go generate ./cmd/flow/internal/reference && git diff --exit-code -- cmd/flow/internal/reference/
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 lint
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#branch=origin/main'
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 generate
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 build --exclude-imports -o pkg/flowstate/v1/protodoc/flowstate.descriptorset.binpb
	git diff --exit-code
	GOTOOLCHAIN=go1.26.5 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
	GOTOOLCHAIN=go1.26.5 go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

# The four bounded fuzz smokes CI's fuzz-smoke job runs, verbatim, so the local
# gate cannot pass a commit the required job rejects. Time-bounded, single
# worker, memory-bounded: a fuzzer's purpose is to find the input that
# explodes, and these bounds are what make it safe to run on every push.
fuzz-smoke:
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzRoundTrip -fuzztime 30s ./pkg/flowstate/v1/flowfile/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzCELCompile -fuzztime 30s ./pkg/flowstate/v1/flowfile/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzMCPToolArguments -fuzztime 30s ./cmd/flow/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzMessageDescriptor -fuzztime 30s ./pkg/flowstate/v1/plugin/

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

# The packages whose correctness is an *ordering* claim, run under a schedule
# that interleaves differently rather than one that runs harder.
#
# `-cpu=1` is the whole point, and it is not a smaller version of `-count`.
# GOMAXPROCS=1 forces goroutines to interleave only at yield points instead of
# running truly in parallel, which reaches orderings a multi-core run reaches
# rarely or never. The local test harness's virtual clock decides when time
# moves from how many participants are parked, so every claim it makes is an
# ordering claim: a defect there shows up as a *wrong answer* — a gate that
# should have lapsed reporting that it did not — rather than as a crash a race
# detector would catch.
#
# Sized to be cheap enough to keep: seconds, not minutes. It exists because
# `-race -count=3` at the default GOMAXPROCS ran clean against a defect that
# `-cpu=1` reproduced three times in ten (#278).
test-ordering:
	GOMEMLIMIT=1GiB go test -race -cpu=1 -count=20 -timeout 300s ./pkg/flowstate/v1/flowtest/

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

# Record the CLI's styled surfaces with charmbracelet/vhs and compare them
# against the goldens under cmd/flow/internal/appearance/testdata. Needs vhs,
# ttyd and ffmpeg on PATH; without them the test skips and says which is
# missing, which is also why this is not part of `check`: a gate that reports
# green by not running is worse than one that is honestly somewhere else. CI's
# `appearance` job installs all three and runs this command.
appearance:
	GOMEMLIMIT=2GiB go test -timeout 900s -count=1 -run TestAppearance ./cmd/flow/internal/appearance/

# Re-record every golden. Run this when a styled surface changed on purpose,
# read the diff as the review of that change, and commit the goldens alongside
# the change that moved them. Never hand-edit a golden: it would record an
# appearance the CLI has never produced.
appearance-update:
	GOMEMLIMIT=2GiB go test -timeout 900s -count=1 -run TestAppearance ./cmd/flow/internal/appearance/ -update
