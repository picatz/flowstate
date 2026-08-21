.PHONY: check gate test test-plugins test-ordering test-fast fuzz-smoke fmt docs docs-preview appearance appearance-update coverage

# Diff-scoped local gate (#482): build, gofmt on changed files, vet and
# bounded -race tests for the packages the diff touches plus their reverse
# dependencies, and the conditional legs (buf, docs drift, examples, flowtest
# ordering) only when their inputs changed. The default before pushing a PR
# branch; PR CI runs the full list as the gate that decides. See CLAUDE.md.
gate:
	go run ./tools/gate

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
	go run ./cmd/flow breaking --against origin/main examples/
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
	GOTOOLCHAIN=go1.26.6 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
	GOTOOLCHAIN=go1.26.6 go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

# The six bounded fuzz smokes CI's fuzz-smoke job runs, verbatim, so the local
# gate cannot pass a commit the required job rejects. Time-bounded, single
# worker, memory-bounded: a fuzzer's purpose is to find the input that
# explodes, and these bounds are what make it safe to run on every push.
fuzz-smoke:
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzRoundTrip -fuzztime 30s ./pkg/flowstate/v1/flowfile/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzCELCompile -fuzztime 30s ./pkg/flowstate/v1/flowfile/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzMarshalRoundTrip -fuzztime 30s ./pkg/flowstate/v1/flowfile/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzMCPToolArguments -fuzztime 30s ./cmd/flow/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzMessageDescriptor -fuzztime 30s ./pkg/flowstate/v1/plugin/
	GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzWebhookEventBinding -fuzztime 30s ./pkg/flowstate/v1/

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
			GOMEMLIMIT=2GiB go test -race -timeout 300s ./... ) || \
			{ echo "==> $$module failed; if it says \"updates to go.mod needed\", run \`make tidy-plugins\` — a root dependency bump moves shared versions out from under these modules' own pins"; exit 1; }; \
		fmt_out="$$(gofmt -l $$module)"; \
		if [ -n "$$fmt_out" ]; then echo "gofmt: $$fmt_out"; exit 1; fi; \
	done

# The other half of `test-plugins`, and the reason that target now names it.
#
# A bump to the root module moves shared dependencies — protobuf, cel-go, the
# generated protovalidate module — out from under pins the plugin modules carry
# separately, and `test-plugins` then fails with `updates to go.mod needed`.
# That is a correct failure about a stale file, not about the bump, and it has
# now arrived twice on Dependabot pull requests whose own diffs were fine
# (#605, #611). A red check on a correct diff is how people learn to merge past
# a failing job, so the fix is one command rather than five `cd`s.
#
# Deliberately not run by `test-plugins` itself: a check that repairs what it is
# checking cannot fail, and the staleness is a fact about committed files that
# somebody has to commit.
tidy-plugins:
	@for module in plugins/*/; do \
		[ -f "$$module/go.mod" ] || continue; \
		echo "==> $$module"; \
		( cd "$$module" && go mod tidy ) || exit 1; \
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

# Preview how this module's godoc renders, the way pkg.go.dev and gopls-on-hover
# will show it, by serving the working tree locally at http://localhost:8080.
# This is a preview tool, not a gate: it is deliberately NOT part of `check` or
# CI, it just closes the read-it-back loop for a doc PR the way `docs` closes it
# for reference docs. Ctrl-C to stop.
#
# Two rendering gotchas worth knowing before you rely on the preview:
#
#   - A [Symbol] doc link only resolves when the identifier is importable from
#     the package the comment lives in. A link to something unimportable renders
#     as literal brackets, not a link, so cross-package links need the full
#     import path (`[github.com/picatz/flowstate/pkg/flowstate/v1.Workflow]`), not
#     a bare local name or a slash-qualified fragment (a name with slashes is
#     read as an import path, so `[pkg/flowstate/v1.Workflow]` links to a package
#     that does not exist).
#   - A code block needs the blank-comment-line-then-indent shape: an empty `//`
#     line, then lines indented under it. Without the blank line first, the
#     indented text renders as an ordinary paragraph rather than as code.
docs-preview:
	go run golang.org/x/pkgsite/cmd/pkgsite@latest -http localhost:8080 .

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

# Coverage across the process boundary `go test -cover` cannot see (#519).
#
# At least seven test files drive the `flow` binary or a plugin as a real
# subprocess — exec.Command on a separately compiled binary — rather than
# calling into the package under test directly: cmd/flow/execute_test.go,
# nocolor_test.go, mcp_plugin_test.go, cmd/flow/internal/appearance's
# appearance_test.go, and pkg/flowstate/v1/plugin/example_test.go among them.
# `go test -cover` only instruments the package it is compiling, so every one
# of those lines is invisible to it: a CLI verb whose only coverage comes
# from a subprocess test looks identical to one with no test at all.
#
# GOCOVERDIR exported below is read two ways at once. Go's own toolchain
# reads it directly: `go test -cover -args -test.gocoverdir=...` makes this
# process's own in-process test binaries write their counters there. And
# internal/covbuild reads the same variable to decide, for every test file
# above, whether to build its subprocess binary with -cover and to carry
# GOCOVERDIR into the environments built from scratch that would not
# otherwise inherit it — so an instrumented `flow` or example-plugin binary,
# run as a real subprocess, writes its counters into the same directory.
# `go tool covdata` then merges every process's counters, however many ran,
# into one profile — that merge is the whole point of the mechanism; nothing
# here computes coverage from a single process the way -coverprofile does.
#
# This is a map, not a gate, per #519: nothing in CI or `make check` reads
# .coverage/, and no percentage is enforced anywhere, here or anywhere else.
# Run it locally, open .coverage/coverage.html, and look for a path nothing
# reached — that reading is the deliverable, not the number.
#
# Scope: this covers everything `go test ./...` reaches, including every
# subprocess-driven test file named above. It does NOT cover plugins/* — git,
# github, sql, vcs and codex are separate Go modules (make test-plugins builds
# and tests them one module at a time) outside this module's build graph, so
# there is nothing from them for covdata to merge here. Extending this
# mechanism to the plugin modules, and wiring a merged report into the weekly
# deep tier per #519's own suggestion, are tracked as follow-ups rather than
# landed in this target.
coverage:
	rm -rf .coverage
	mkdir -p .coverage/raw
	( GOCOVERDIR=$(CURDIR)/.coverage/raw GOMEMLIMIT=2GiB go test -cover -timeout 1800s ./... -args -test.gocoverdir=$(CURDIR)/.coverage/raw ; echo $$? > .coverage/status ) 2>&1 | tee .coverage/test.log; \
	status=$$(cat .coverage/status); \
	go tool covdata percent -i=.coverage/raw | tee .coverage/percent.txt; \
	go tool covdata textfmt -i=.coverage/raw -o .coverage/coverage.out; \
	go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html; \
	echo "coverage HTML: .coverage/coverage.html"; \
	echo "per-package summary: .coverage/percent.txt"; \
	echo "raw counters: .coverage/raw/ (merge more processes into it with: go tool covdata merge -i=... -o=.coverage/raw)"; \
	if [ $$status -ne 0 ]; then \
		echo "go test exited non-zero; see .coverage/test.log. The merge above still ran — a failing run's coverage is still worth reading, since it shows what the failure itself reached."; \
	fi; \
	exit $$status
