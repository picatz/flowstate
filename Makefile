.PHONY: check gate test test-plugins plugin-examples plugin-example-catalog-update test-ordering test-fast fuzz-smoke fmt modernize vacuity docs docs-preview appearance appearance-update coverage coverage-plugins release-artifacts

# gofmt from the toolchain go.mod pins, rather than whichever build sits on
# PATH (#1061).
#
# `go` re-execs into the pinned toolchain and `gofmt` beside it does not, so
# `/usr/local/go/bin/gofmt` can be an older build while the `go version` next
# to it prints the pin — and the obvious sanity check therefore confirms the
# wrong thing. An older gofmt disagrees with the pinned one about real files
# (1.24.7 and 1.27.0 indent a composite literal in a multi-value return
# differently), so it reports as unformatted what CI — which installs the pin
# through `go-version-file: go.mod`, making its PATH gofmt the right one —
# considers clean. Three agents investigated that false positive before it was
# written down.
#
# Resolved by asking for the version go.mod names, exactly, rather than by
# asking `go env GOROOT` what is selected. The `go` directive is a *minimum*:
# toolchain selection keeps a local default that is new enough, so on a machine
# whose Go is newer than the pin, plain `go env GOROOT` answers with that newer
# toolchain — while CI's `go-version-file: go.mod` installs the directive's
# version exactly. That would put the newer gofmt here and the pinned one there,
# which is this whole problem again with the versions swapped.
#
# The version is read out of go.mod rather than written down here, so it is the
# same single value CI reads and cannot drift from it. `GOTOOLCHAIN` naming an
# exact release is what makes the answer that release and not merely something
# at least as new; it downloads the toolchain if this machine lacks it, which is
# what CI does too.
#
# `tools/gate` needs none of this: its gofmt leg calls `go/format`, the library
# face of the same printer, compiled with the toolchain that builds the gate.
GOVERSION := $(shell awk '$$1 == "go" { print $$2; exit }' go.mod)
GOFMT := $(shell GOTOOLCHAIN=go$(GOVERSION) go env GOROOT)/bin/gofmt

# Refuse to run rather than refuse to check.
#
# `$(shell ...)` discards the exit status of what it ran, so a toolchain that
# cannot be resolved — offline, or a version that does not exist — leaves GOFMT
# as the bare suffix `/bin/gofmt`. A command substitution around a binary that
# is not there yields the empty output an "is anything unformatted?" test reads
# as "nothing is", and on a host that does have a `/bin/gofmt` it is silently
# the wrong formatter. Either way the check reports success because it never
# ran, which is the one failure this repository's gate design exists to make
# impossible. So every recipe that formats asserts the resolved path first.
define require-gofmt
@[ -x "$(GOFMT)" ] || { \
	echo "make: cannot resolve the gofmt go.mod pins (go$(GOVERSION)): resolved to \"$(GOFMT)\"" >&2; \
	echo "make: \`GOTOOLCHAIN=go$(GOVERSION) go env GOROOT\` has to answer — the toolchain must be installed or fetchable" >&2; \
	exit 1; \
}
endef

# Diff-scoped local gate (#482): build, gofmt on changed files, vet and
# bounded -race tests for the packages the diff touches plus their reverse
# dependencies, and the conditional legs (buf, docs drift, examples, flowtest
# ordering) only when their inputs changed. The default before pushing a PR
# branch; PR CI runs the full list as the gate that decides. See CLAUDE.md.
gate:
	go run ./tools/gate

# Build the complete release payload locally without publishing it. VERSION is
# deliberately required rather than inferred from a moving branch; the release
# workflow supplies the immutable tag after checking that it names this commit.
release-artifacts:
	go run ./tools/release -version "$(VERSION)"

# Full CI-parity loop, verbatim commands, in CI order. See CLAUDE.md.
check:
	go build ./...
	go vet ./...
	$(require-gofmt)
	@fmt_out="$$("$(GOFMT)" -l ./cmd ./pkg)" || exit 1; \
	if [ -n "$$fmt_out" ]; then \
		echo "gofmt -l found unformatted files:"; \
		echo "$$fmt_out"; \
		exit 1; \
	fi
	$(MAKE) test ARTIFACT_SWEEP=1
	$(MAKE) test-plugins
	$(MAKE) plugin-examples
	$(MAKE) test-ordering
	go run ./cmd/flow fix --check examples/
	go run ./cmd/flow lint --strict examples/
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
	go run github.com/bufbuild/buf/cmd/buf@v1.72.0 build --exclude-imports -o pkg/flowstate/v1/plugin/examples/flowstate-plugin-example/schema.descriptorset.binpb pkg/flowstate/v1/plugin/examples/flowstate-plugin-example/proto
	git diff --exit-code
	GOTOOLCHAIN=go1.27.0 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
	GOTOOLCHAIN=go1.27.0 go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...

# The bounded fuzz smokes CI's fuzz-smoke job runs — and it runs *this target*
# rather than its own copy of them, so the local gate cannot pass a commit the
# required job rejects, for the same reason `test` below is shared that way.
# The targets themselves come from tools/fuzztargets/targets.txt, the one
# written source of that list (#857): a new target lands there and every runner
# picks it up. Time-bounded, single worker, memory-bounded: a fuzzer's purpose
# is to find the input that explodes, and these bounds are what make it safe to
# run on every push.
#
# The list is captured before the loop rather than piped into it: a pipeline's
# status is the status of its right-hand side, so `list.sh | while read` would
# report success on a run that fuzzed nothing at all if the list could not be
# read — a check passing by not running, which is the failure this file
# legislates against elsewhere. list.sh itself refuses to print an empty tier.
fuzz-smoke:
	@targets="$$(tools/fuzztargets/list.sh smoke)" || exit 1; \
	echo "$$targets" | while read -r target dir; do \
		echo "==> $$target ($$dir)"; \
		GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz "$$target" -fuzztime 30s "./$$dir/" || exit 1; \
	done

# Bounded full test run (no -short). CI's `test` step runs this target rather
# than its own copy of the command, so the bound cannot drift between the two —
# a copy of a command list is a thing that drifts, and the whole point of this
# file is that it is what CI runs.
# ARTIFACT_SWEEP=1 additionally cross-builds one program per entry in `go tool
# dist list`, to confirm `tools/artifacts` recognises every executable format
# Go can emit. `check` sets it; the PR lane does not, because it costs a couple
# of minutes on a cold cache to guard a thing that changes when Go adds a port.
# The tracked-file gate that check exists for runs either way, in milliseconds.
test:
	GOMEMLIMIT=2GiB $(if $(ARTIFACT_SWEEP),FLOWSTATE_ARTIFACT_SWEEP=1 ,)go test -race -timeout 900s ./...

# The plugins are separate modules, which is the point of them: `./...` above
# does not reach them, and a plugin that does not compile would leave every
# other check green. Bounded on the same reasoning as `test` — a fuzz-adjacent
# or runaway plugin test should fail with a diagnosable timeout naming its
# package, not consume the job's whole budget and leave an operator guessing
# which module hung.
test-plugins:
	$(require-gofmt)
	@for module in plugins/*/; do \
		[ -f "$$module/go.mod" ] || continue; \
		echo "==> $$module"; \
		( cd "$$module" && go build ./... && go vet ./... && \
			GOMEMLIMIT=2GiB go test -race -timeout 300s ./... ) || \
			{ echo "==> $$module failed; if it says \"updates to go.mod needed\", run \`make tidy-plugins\` — a root dependency bump moves shared versions out from under these modules' own pins"; exit 1; }; \
		fmt_out="$$("$(GOFMT)" -l $$module)" || exit 1; \
		if [ -n "$$fmt_out" ]; then echo "gofmt: $$fmt_out"; exit 1; fi; \
	done

# Build the first-party plugins into an isolated directory, compare their
# descriptors and security claims with the portable reviewed catalog, then
# validate every plugin example against the complete native catalog from that
# same build without executing plugin tasks. A plugin
# module changing its task contract, a stale catalog, and an invalid example are
# therefore one failing gate rather than three ways CI can stay silent (#1342).
plugin-examples:
	@generated="$$(mktemp "$${TMPDIR:-/tmp}/flowstate-plugin-contracts.XXXXXX")"; \
	validation="$$(mktemp "$${TMPDIR:-/tmp}/flowstate-plugin-validation.XXXXXX")"; \
	trap 'rm -f "$$generated" "$$validation"' EXIT HUP INT TERM; \
	tools/pluginexamples/catalog.sh "$$generated" "$$validation"; \
	diff -u examples/plugins/plugins.lock.json "$$generated" || { \
		echo "plugin example catalog drifted; run 'make plugin-example-catalog-update' and review the result" >&2; \
		exit 1; \
	}; \
	go run ./cmd/flow validate --plugin-catalog "$$validation" examples/plugins/

# The explicit write side of plugin-examples. The check above never repairs the
# artifact it judges; an author runs this target and reviews the portable
# descriptor and claims-digest changes like any other generated contract.
plugin-example-catalog-update:
	tools/pluginexamples/catalog.sh examples/plugins/plugins.lock.json

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
	$(require-gofmt)
	"$(GOFMT)" -w ./cmd ./pkg

# Report what Go's `go fix` modernizers would change, and change nothing
# (#521). Note which `fix` this is: Go's `go fix` rewrites Go source, this
# repository's own `flow fix` rewrites Flowfiles, and the two are unrelated.
#
# This is a map, not a gate, on the same reasoning as `coverage` below:
# nothing in `check` or the PR lane runs it, and no count is enforced
# anywhere. #521's decision is that the modernizers are applied
# opportunistically — a package at a time, inside a diff a reviewer is
# already reading closely for another reason — and never as a sweep, because
# a mechanical diff thousands of lines long is the shape in which a real
# defect hides, and none of what the fixers propose fixes one. So the useful
# invocation is the narrow one, run when you are already in the package:
#
#     make modernize PKGS=./pkg/flowstate/v1/engine/
#     go run ./tools/modernize -sites ./pkg/flowstate/v1/engine/
#
# The weekly deep tier (.github/workflows/deep.yml) runs the wide one and
# files a single advisory issue, so the number stays visible without a tool
# committing on anyone's behalf.
#
# Scope: the root module. The plugin modules under plugins/* are separate
# modules outside this module's build graph — exactly as they are for
# `coverage` — and nothing here reaches them, tracked as a follow-up rather
# than landed here.
modernize:
	go run ./tools/modernize $(if $(PKGS),$(PKGS),./...)

# Report tests that pass without proving anything.
#
#     make vacuity            # the counts, and every unasserted site
#     make vacuity SITES=1    # every site, conditional ones included
#
# Two checks, enforced differently because the tree stands in two different
# places. `unasserted` — a test that reaches no assertion at all — is at zero,
# with the two deliberate sites carrying a `//vacuity:ignore` naming their
# reason, so a finding is one a diff introduced. `conditional` — every claim
# inside a loop over something nothing says is non-empty — stands in the
# hundreds and is reported only; enforcing a number that size would mean a
# sweep this repository has twice paid for, or an allowlist that rots. The
# current figure is what the command prints, which is where a number belongs.
#
# Nothing needs to run this for the gate to hold: `tools/vacuity`'s own
# TestTheRepositoryHasNoUnassertedTest walks the tree under `go test ./...`,
# so the enforcing venue is the one everybody already watches, and no CI
# workflow had to learn a new job. This target is for reading the report.
#
# Unlike `modernize` and `coverage`, it does reach plugins/*: it parses rather
# than builds, so a separate module is just another directory — and a plugin's
# containment tests are where a vacuous claim costs the most.
vacuity:
	go run ./tools/vacuity $(if $(SITES),-sites,)

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
# Two variables below, naming one directory, because two mechanisms read it.
# Go's own toolchain reads GOCOVERDIR: `go test -cover -args
# -test.gocoverdir=...` makes this process's own in-process test binaries
# write their counters there. internal/covbuild reads FLOWSTATE_COVERDIR —
# deliberately *not* GOCOVERDIR, see that package's doc for the scratch
# directory `go test -cover` points GOCOVERDIR at and then discards — to
# decide, for every test file above, whether to build its subprocess binary
# with -cover and to carry a real GOCOVERDIR into environments built from
# scratch that would not otherwise inherit one. `go tool covdata` then merges
# every process's counters, however many ran, into one profile — that merge is
# the whole point of the mechanism; nothing here computes coverage from a
# single process the way -coverprofile does.
#
# This target exported only GOCOVERDIR until #404, which is the failure #519
# is about wearing the mechanism's own clothes: covbuild saw no
# FLOWSTATE_COVERDIR, built every subprocess binary uninstrumented, and the
# report came back with the subprocess tier contributing nothing — silently,
# since a package with no counters and a package whose counters never merged
# read identically. Measured on `-run TestExitCodeGoldenPaths` alone: two
# files in the raw directory and 0.0% for cmd/flow with GOCOVERDIR only,
# against six files and 11.2% with both.
#
# This is a map, not a gate, per #519: nothing in CI or `make check` reads
# .coverage/, and no percentage is enforced anywhere, here or anywhere else.
# Run it locally, open .coverage/coverage.html, and look for a path nothing
# reached — that reading is the deliverable, not the number.
#
# Scope: this covers everything `go test ./...` reaches, including every
# subprocess-driven test file named above, plus the plugin modules under
# plugins/* by way of `coverage-plugins` below (#761). What it still does not
# cover is CI: nothing in ci.yml or deep.yml runs this target, so the report
# is a local, on-demand read rather than an automated one — tracked as a
# follow-up on #761 rather than landed here.
coverage:
	rm -rf .coverage
	mkdir -p .coverage/raw
	( GOCOVERDIR=$(COVERAGE_RAW) FLOWSTATE_COVERDIR=$(COVERAGE_RAW) GOMEMLIMIT=2GiB go test -cover -timeout 1800s ./... -args -test.gocoverdir=$(COVERAGE_RAW) ; echo $$? > .coverage/status ) 2>&1 | tee .coverage/test.log; \
	( $(MAKE) coverage-plugins ; echo $$? > .coverage/plugin-status ) 2>&1 | tee -a .coverage/test.log; \
	status=$$(cat .coverage/status); \
	plugin_status=$$(cat .coverage/plugin-status); \
	go tool covdata percent -i=.coverage/raw | tee .coverage/percent.txt; \
	go tool covdata textfmt -i=.coverage/raw -o .coverage/coverage.out; \
	$(MAKE) --no-print-directory .coverage/go.work; \
	GOWORK=$(CURDIR)/.coverage/go.work go tool cover -html=.coverage/coverage.out -o .coverage/coverage.html; \
	echo "coverage HTML: .coverage/coverage.html"; \
	echo "per-package summary: .coverage/percent.txt"; \
	echo "raw counters: .coverage/raw/ (merge more processes into it with: go tool covdata merge -i=... -o=.coverage/raw)"; \
	if [ $$status -ne 0 ]; then \
		echo "go test exited non-zero; see .coverage/test.log. The merge above still ran — a failing run's coverage is still worth reading, since it shows what the failure itself reached."; \
	fi; \
	if [ $$plugin_status -ne 0 ]; then \
		echo "make coverage-plugins exited non-zero; see .coverage/test.log. The merge above still ran, and it still holds whatever the plugin modules reached before the failure."; \
		if [ $$status -eq 0 ]; then status=$$plugin_status; fi; \
	fi; \
	exit $$status

# The plugin half of `coverage`, and the reason that target now names it (#761).
#
# plugins/{git,github,sql,vcs,codex} are separate Go modules, which is the
# point of them: `go test ./...` in the root module does not reach them, so
# until this target existed there was simply nothing from them for `go tool
# covdata` to merge — the boundary #519 called the worst blind spot of the
# lot, since a plugin is exactly the kind of code whose only end-to-end test
# runs it as a subprocess.
#
# There is no second mechanism here. Each module's own test binaries write
# their counters into the same COVERAGE_RAW directory the root run used, via
# the same `-cover ... -args -test.gocoverdir=` shape, and covdata unions the
# lot: the counters are keyed by package import path, so one directory holding
# several modules' meta files merges exactly as one module's do. And
# FLOWSTATE_COVERDIR is exported for the same reason `coverage` exports it —
# plugins/*/reachable builds this plugin's real binary and launches it through
# a plugin.Host, and internal/covbuild is what makes that subprocess
# instrumented and points its GOCOVERDIR at a directory the merge reads back.
#
# COVERAGE_RAW is absolute because the loop below cds into each module: a
# relative -test.gocoverdir would scatter counters into five directories
# nothing merges. Sequential for the same reason `test-plugins` is: a failure
# should name the module it came from.
COVERAGE_RAW ?= $(CURDIR)/.coverage/raw

coverage-plugins:
	@mkdir -p "$(COVERAGE_RAW)"
	@for module in plugins/*/; do \
		[ -f "$$module/go.mod" ] || continue; \
		echo "==> $$module"; \
		( cd "$$module" && FLOWSTATE_COVERDIR="$(COVERAGE_RAW)" GOMEMLIMIT=2GiB \
			go test -cover -timeout 900s ./... -args -test.gocoverdir="$(COVERAGE_RAW)" ) || \
			{ echo "==> $$module failed; if it says \"updates to go.mod needed\", run \`make tidy-plugins\`"; exit 1; }; \
	done

# The workspace `go tool cover -html` is pointed at, and nothing else (#761).
#
# The HTML report is rendered from source, so the cover tool has to turn each
# import path in the profile back into a directory on disk — and asked from
# the root module it cannot, because plugins/* are not in its module graph:
# `cover: no required module provides package .../plugins/codex`, and *no*
# report at all rather than one missing a section. A workspace naming all six
# modules answers exactly that question.
#
# Generated under .coverage/ (gitignored, and rebuilt by `coverage` after its
# `rm -rf`) rather than checked in at the repository root on purpose: a
# committed go.work would put every plugin module into every ordinary build's
# module graph, which is precisely the separation these modules exist to keep
# — go-git and the rest stay out of the root module's dependencies. It is read
# only by the one command that needs it, through GOWORK on that command.
#
# Absolute paths in the `use` block because they resolve relative to the
# go.work file's own directory, not the working directory.
.coverage/go.work:
	@mkdir -p .coverage
	@{ echo "go $$(go list -m -f '{{.GoVersion}}')"; \
		echo; \
		echo "use ("; \
		echo "	$(CURDIR)"; \
		for module in plugins/*/; do \
			[ -f "$$module/go.mod" ] || continue; \
			echo "	$(CURDIR)/$$module"; \
		done; \
		echo ")"; } > $@
