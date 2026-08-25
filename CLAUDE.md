# Working on Flowstate

Guidance for anyone — human or agent — making changes here. It exists because each
item below cost real time or nearly cost a machine.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for what the system is and the
invariants that constrain changes. Read the invariants before a structural change;
a change that violates one is a bug even when the tests pass.

## Proto-first

Types describing the system live in the schema under `proto/flowstate/v1/`, not as
hand-written Go structs. Regenerate with `buf generate`. Behavior attaches to
generated types as methods in hand-written files; the shape comes from the schema.

The exception is a type defined by a boundary it refuses to cross. A value that must
never be serialized cannot be a schema type, because the schema exists to describe
things that travel. Say so in the package doc when you write one, or someone will
"fix" it into the proto.

## Bound anything that consumes untrusted input

Every parser, evaluator, and reader in this repo handles input an outside party
chooses. Each one needs an explicit bound, and the bound has to match the shape of
the attack:

- CEL evaluation is bounded by cost, not by time (`DefaultCostLimit`).
- HTTP responses are bounded by bytes before being read into memory.
- Alias expansion in YAML must be bounded by *total nodes*, not by chain depth — a
  billion-laughs document has a depth of one per alias and multiplies breadth at
  every level.
- Recursive resolution is bounded by depth (`maxActivationDepth`).
- A paged listing is bounded by executions read *and* by requests made
  (`maxListScan`, `maxListRequests`).

Depth bounds do not stop breadth explosions, and time bounds do not stop memory
explosions. Ask which resource the attacker controls, then bound that resource.

Bounding one resource does not bound another the peer controls the ratio to.
`List` reads executions until it has filled a page or spent its scan budget, and
both of those only advance when executions come back — how many come back per
request is the *peer's* choice. Temporal's visibility store can legitimately answer
with an empty page carrying a next-page token, so against a peer that answers that
way every time, a loop bounded only by executions read never terminates: nothing it
checks ever changes. Requests therefore have their own bound. Whenever a loop's
progress is measured in units the far side decides, count the round trips too.

And check that a bound covers the path an attacker would actually take, rather than
the one a cooperative peer takes. `connect.WithReadMaxBytes` bounds a *successful*
response: connect-go v1.20.0 builds a separate unmarshaler for a non-200 body
(`protocol_connect.go:541`) without carrying the limit over, and the check at `:1119`
is gated on it being greater than zero. A hostile peer answers with an HTTP 500 and
an arbitrarily large body. The cap therefore belongs on the `http.RoundTripper`,
below the RPC library, where no path the library treats specially can miss it — see
`plugin/transport.go`. A bound configured through a library option is only as good as
that library's coverage of its own error paths.

## Running tests

Always bound test runs:

    GOMEMLIMIT=1GiB go test -timeout 120s ./pkg/flowstate/v1/...

Fuzzing needs more care, because a fuzzer's purpose is to find the input that
explodes:

    GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzName -fuzztime 60s ./path/

`-fuzztime` bounds time, not memory. Eight parallel workers on a parser with an
unbounded expansion path consumed 23 GB and 32 GB of swap in one afternoon.

### `gofmt` on `PATH` is not the pinned toolchain's

`go` re-execs into the toolchain `go.mod` pins; the `gofmt` sitting beside it
does not. So `/usr/local/go/bin/gofmt` can be an older build while the
`go version` next to it prints the pin — and the obvious sanity check ("which
Go is this? the same as CI") confirms the wrong thing. The two disagree about
real files: 1.24.7 and 1.27.0 indent a composite literal in a multi-value
return differently, which is why three separate agents reported
`pkg/flowstate/v1/auth/vocabulary_test.go` as unformatted on a green `main`,
one of them concluding CI would be red for every PR until somebody fixed it
(#1061). CI installs the pin through `go-version-file: go.mod`, so its `gofmt`
is the right one and it was never wrong.

Reach for `make fmt` or `make check`, which resolve the right one through the
`GOFMT` variable. By hand it is:

    "$(GOTOOLCHAIN=go$(awk '$1 == "go" { print $2; exit }' go.mod) go env GOROOT)"/bin/gofmt -l ./cmd ./pkg

which is a mouthful because two separate things have to be right. `go env
GOROOT` alone answers with the *selected* toolchain, and the `go` directive is
a minimum rather than a pin — selection keeps a local default that is new
enough — so on a machine whose Go is newer than the directive it answers with
that newer toolchain, while CI's `go-version-file: go.mod` installs the
directive's version exactly. That is this same disagreement with the versions
swapped, and it is why `GOTOOLCHAIN` names an exact release. And the version is
read out of `go.mod` rather than typed, because a version typed anywhere else
is a second copy of a number CI already reads. Run it from inside the module:
outside it there is no `go.mod`, `go env GOROOT` answers with the host default,
and that is the original false positive again — which is how one of those
investigations confirmed the wrong answer twice.

`tools/gate` never had the problem: its gofmt leg calls `go/format`, the
library face of the same printer, compiled with the toolchain that builds the
gate. Reach for a bare `gofmt` and you are the only thing in the loop without
the pin.

A `go test` command that returns does not mean the test binary exited. If a run
behaves oddly, check:

    ps -Ao pid,rss,args | grep -E '\.test|-fuzz' | grep -v grep

### Coverage across a subprocess (`make coverage`)

`go test -cover` only instruments the package it is compiling. At least seven
test files drive the `flow` binary or a plugin as a real subprocess —
`cmd/flow/execute_test.go`, `nocolor_test.go`, `breaking_test.go`,
`browser_test.go`, `mcp_plugin_test.go`,
`cmd/flow/internal/appearance/appearance_test.go`, and
`pkg/flowstate/v1/plugin/example_test.go` — and every line those exercise is
invisible to it, because it runs in a process the harness never instrumented.
A CLI verb whose only coverage comes from a subprocess test looks identical to
one with no test at all (#519).

    make coverage

builds every subprocess binary those tests launch with Go's `-cover`
instrumentation, runs the full suite, and merges every process's counters —
however many ran — with `go tool covdata` into `.coverage/coverage.html`
(browsable) and `.coverage/percent.txt` (a per-package summary). Read the
HTML output and look for a path nothing reached; that reading is the point.

This is a map, not a gate: nothing in CI or `make check` reads `.coverage/`,
and no percentage is enforced anywhere. A percentage rewards a test that
executes lines without asserting anything, which is the "green by not
running" failure this file already legislates against elsewhere, wearing a
different hat.

The mechanism is `internal/covbuild`, keyed off `FLOWSTATE_COVERDIR` rather
than Go's own `GOCOVERDIR` — deliberately a different name. `go test -cover
-args -test.gocoverdir=X` points the running test process's own `GOCOVERDIR`
at a scratch directory of its own and copies only the counters *it* wrote
into `X` afterward; a subprocess that merely inherited that scratch
`GOCOVERDIR` writes real counters into a directory the merge then discards,
which is coverage that silently vanishes rather than coverage that is
visibly missing. `covbuild.Env()` threads the real destination through every
subprocess's `Cmd.Env` explicitly instead.

The plugin modules are in that report too, by way of `make coverage-plugins`,
which `make coverage` invokes (#761). `plugins/*` (`git`, `github`, `sql`,
`vcs`, `codex`) are separate Go modules outside this module's build graph, so
`go test ./...` never reaches them and there used to be nothing from them to
merge — the worst of the blind spots, since a plugin's end-to-end test runs it
as a real subprocess. It is not a second mechanism: each module's tests run
with the same `-cover ... -args -test.gocoverdir=` shape into the same
`.coverage/raw` the root run used, and `covdata` unions counters keyed by
import path, so several modules' meta files in one directory merge exactly as
one module's do. `FLOWSTATE_COVERDIR` is exported into each module as well,
which is what makes `plugins/*/reachable` build the plugin binary it launches
with `-cover` and name a `GOCOVERDIR` the merge reads back. Run `make
coverage-plugins` on its own to refresh just that half; `COVERAGE_RAW` names
the destination, and it has to be absolute because the target `cd`s into each
module — a relative one would scatter counters into five directories nothing
merges.

The remaining gap, tracked as a follow-up rather than landed here: `make
coverage` is not wired into CI (`ci.yml` or `deep.yml`) — running it is a
local, on-demand read, not an automated one, on the same "leave CI wiring as
a named follow-up" reasoning that keeps `make check` itself the full local
rehearsal rather than something CI second-guesses.

## Go modernizers (`go fix`): weekly awareness, per-package adoption, never a sweep

Go 1.26 made `go fix` the home of the modernizers — `strings.SplitSeq`,
`maps.*`, `slices.Contains`, `min`/`max`, `new(expr)` and the rest — and this
repository's toolchain is pinned past that, so they are available today.

Say which `fix` you mean, every time. Go's `go fix` rewrites **Go source**.
This repository's own `flow fix` rewrites **Flowfiles**. A commit message, job
name or comment that says "fix" near this work and does not disambiguate
teaches the next reader that `flow fix` grew modernizers.

    make modernize                                    # the whole module
    make modernize PKGS=./pkg/flowstate/v1/engine/    # one package
    go run ./tools/modernize -sites ./pkg/...         # every site's position

That command reports and changes nothing; it has no apply mode, deliberately.
The weekly deep tier's `modernize` job runs the wide one and files a single
advisory issue (deduplicated by title like every other job in `deep.yml`), so
the number stays visible without a tool committing on our behalf. It is a map,
not a gate: nothing in `make check` or the PR lane runs it, and no count is
enforced anywhere.

**Apply them a package at a time, when that package is already open for another
reason** — so the conversion rides in a diff a reviewer is reading closely — and
**never as a standalone sweep**. Measured on `main`, one sweep is roughly 11,000
mechanical lines across 91 files, none of it fixing a defect. That is precisely
the shape in which a real defect hides from review, and this repository has paid
for it twice already (two `flow fix` corruptions landed inside changes that
looked mechanical; #513's review found four textual-search bugs in a change
everyone would have called routine). #521 has the decision and the numbers.

Two properties of the report worth knowing. The fixer list is not written down
anywhere here — it comes from the diagnostics the pinned toolchain actually
produces, so a toolchain bump that adds a modernizer shows up without anyone
editing a list (the fifteen analyzers #521 measured were twenty-three by
go1.26.6, and twenty-six by go1.27.0). And sites inside generated files are
counted separately and excluded from every total, because a generated file is
never hand-edited: a modernization there could only ever arrive through its
generator.

A third, and the only thing that can make the weekly job go red: the report is
complete or it is not printed. When a package fails to load, `go fix -json`
exits non-zero but *still* writes well-formed diagnostics for every package
that did analyse — so accepting that output yields a plausible report, short by
an unknown amount, that reads exactly like a clean tree. A non-zero exit
therefore refuses the report and names the packages that were not analysed, and
the job files an issue saying the report could not be produced rather than
quietly filing a small number. Findings themselves never fail it.

## The gate: diff-scoped before a push, diff-scoped on PR CI, full in the queue

Three tiers over one list of checks, and — this is the part worth holding on to
— **one computation of what a diff can reach**, in `tools/gate`. The local tier
runs it before a push. PR CI runs the same computation in its `plan` job and
skips the jobs the diff cannot reach. The merge queue runs everything, because
it is the last gate before main and the one place where being wrong about the
plan is unrecoverable. `make check` remains the full local rehearsal.

CI does not have a second opinion, and must never grow one. `paths:` filters in
YAML are the obvious way to do this and the wrong one: they are the same value
written down twice, in the venue where the copy cannot be unit tested, cannot
see the import graph, and cannot see that `pkg/flowstate/v1/flowfile`'s tests
read `examples/` off disk with no import anywhere (#589).

Two GitHub semantics decide the shape of the PR lane, and both fail in a
direction someone has to know about:

- A **required** job skipped by an `if:` reports the conclusion `skipped`, and
  a required status check counts `skipped` as satisfied. Make the conditional
  jobs required and a wrongly-skipped `test` shows a green tick on a pull
  request nothing tested. That is a gate passing on something it never looked
  at, and it is the *default* behaviour.
- A required check that never reports at all — what a workflow-level `paths:`
  filter produces, since the whole workflow is skipped — stays pending forever
  and blocks the merge with no way to clear it.

So none of the conditional jobs is required. **`plan` and `verdict` are the two
required checks.** `verdict` runs `if: always()`, re-reads the plan, and fails
unless every job the plan selected succeeded, every job it did not select was
skipped, and the two sets of job names match the workflow's. A skip can
therefore only ever be a decision the plan made in writing.
`tools/gate/verdict_test.go` executes that script — read out of the workflow
file, not copied — against every one of those failure shapes.

See `docs/CI.md` for the whole design, the measured numbers, and the repository
settings it depends on.

Before pushing a PR branch, run the diff-scoped tier:

    go run ./tools/gate        # or: make gate

It computes the changed files against the merge-base with origin/main, maps
them to packages, expands to every package whose build or tests can see a
changed one, then runs the build, gofmt on the changed files, and vet,
staticcheck and bounded `-race` tests for the affected set. The staticcheck
leg is the same analyser, release and `GOTOOLCHAIN` pin the required CI job
runs, narrowed to the affected packages — CI's own job is the same check over
`./...` — because a gate missing a required check passes commits that check
rejects (#878, #879). It narrows only where CI narrows: a change to the
harness (a workflow, the Makefile, `tools/gate`, the fuzz target list) or to
the module graph forces the *job* wide through `ciForceReason`, and the leg
takes the same answer and analyses `./...` too. A workflow-only diff affects
no Go package at all, so a leg reading the affected set alone would skip
exactly where the required job runs. The `vet` leg reads that same forcing,
through the same two functions (`forcedWide`, `scopedLegRuns`), because CI's
`test` job vets the module on exactly those diffs and vetting the module
costs seconds (#887).

The `test` leg is the one place the two tiers deliberately disagree, and it
is a priced decision rather than the same gap left open: a full bounded
`-race` run is the better part of ten minutes, this tier's value is that it
answers in seconds to minutes, and a gate slow enough that people stop
running it protects nothing. So on a harness diff it still runs the affected
set — and its own printed line names the residual and cites #887, so a
narrow test leg beside a wide CI job is something a reader can tell apart
from a bug. `tools/gate/scope_test.go` pins both answers over the diffs
where they could differ, the way `TestTheStaticcheckLegAndJobShareATrigger`
pins staticcheck's. Conditional legs fire only when
their inputs changed: the buf trio and the descriptorset pin on `proto/`, the
docs mirror and reference drift checks on `docs/DSL.md` and on anything that
reaches the binary generating them, example fix and coverage checks on
`examples/`, and the `-cpu=1` ordering line when the flowtest package is
affected. Every leg prints one line saying it ran or why it was skipped, so a
skip is a decision you can read rather than a gap.

Two things that gate leg earns its keep by getting right, both of them ways a
gate can pass when it should fail. A generate-then-verify leg checks for
*untracked* output as well as drift, because `git diff` answers a question
about tracked files only: a generator that creates a new artifact leaves it
untracked, and a diff-only pin reports success while the artifact is missing
from the commit. And the schema is a documentation source, not only a code
one: `flow docs generate` reads a task's field names, types and
required-ness from the protovalidate rules on the schema, and builds the MCP
tool list by walking the service descriptor, so a proto-only edit fires the
docs leg too. More generally the docs leg fires whenever `cmd/flow` is in the
affected set, because that command *is* the generator and its real source set
is its own dependency closure.

The gate finds its own merge-base, which is not a detail — it is the difference
between a gate that runs and one that does not. A clone made with
`--single-branch` has no `origin/main` ref; one made with `--depth` has the ref
and none of the history it shares with the branch under review. The old code
exited with an error naming the fix, `git fetch origin main`, which is correct
advice that a CI job or an agent handed a checkout cannot act on: seven pull
requests in one wave reported exactly that refusal, and all seven were opened
with nothing verified. `resolveBase` now uses the ref, else fetches the branch,
else deepens the clone — each step reached only because the one before produced
no merge-base, so an ordinary checkout pays for one `git merge-base`.

With no network and no remote it runs anyway, treating every tracked file as
changed. That answer goes through `buildPlan` like any other, which sets
`moduleWide` and `ciWide` and selects every conditional leg, so the widest plan
comes out of the one computation rather than a second code path meaning
"everything". The direction is the point: overrunning the true scope costs time,
underrunning it passes commits the checks reject, and a gate that refuses to
start protects nothing at all. Every tier prints which of those happened, so a
wide run is never mistaken for a diff that touched everything.

This inverts the old default of this section, which told everyone to run the
full list locally before every push because a CI round trip bought nothing.
That reasoning predates a six-minute parallel CI and a standing red-to-green
driver: in the last wave, five agents each re-ran the full list serially on
one contended machine at 30 to 60 minutes per gate, to predict an answer CI
returns in six (#482). What survives is the bound, not the venue: nothing
merges red, and every check still runs on every change whose diff can reach it.

That last clause used to read "every required check still runs on every PR",
and it was doing two jobs. As a claim about coverage it is now stated more
precisely by `verdict`. As a claim about *enforcement* it was, until #489's
branch-protection fix was written up and never shipped, a convention the owner
was applying by hand — there was no required-status-checks ruleset on this
repository at all. `docs/CI.md` records the ruleset that makes it a mechanism.

Two habits the last wave paid for, now written down. Editing `docs/DSL.md`
requires `go generate ./cmd/flow/internal/reference`, because that package
holds a generated mirror of the document and `TestTheMirrorMatchesTheRepository`
fails on drift; the gate's docs leg runs this for you. And generated files
(`*.pb.go`, `docs/reference/`, the reference mirror, the descriptorset) are
never edited directly: edit the source they derive from and regenerate. The
hooks under `tools/hooks/` enforce both at edit time.

### `make check`: the full rehearsal

Every check in `.github/workflows/ci.yml`, run locally, in CI order. Run it
when verifying what main composes to, before release-shaped changes, when a
CI failure needs local reproduction, or whenever the diff-scoped answer is
not the whole answer you want:

    go build ./...
    go vet ./...
    $(GOFMT) -l ./cmd ./pkg                    # must print nothing; see GOFMT in the Makefile
    GOMEMLIMIT=2GiB go test -race -timeout 900s ./...
    make test-plugins                          # the plugin modules ./... cannot reach
    GOMEMLIMIT=1GiB go test -race -cpu=1 -count=20 -timeout 300s ./pkg/flowstate/v1/flowtest/
    go run ./cmd/flow fix --check examples/
    go run ./cmd/flow lint --strict examples/  # tier 4 over the shown corpus, enforcing since #646
    go run ./cmd/flow test --coverage-required examples/
    go run ./cmd/flow breaking --against origin/main examples/
    make fuzz-smoke
    make appearance
    docker compose -f examples/observability/docker-compose.yaml config -q
    go run ./cmd/flow docs generate && git diff --exit-code -- docs/reference/
    go generate ./cmd/flow/internal/reference && git diff --exit-code -- cmd/flow/internal/reference/
    go run github.com/bufbuild/buf/cmd/buf@v1.72.0 lint
    go run github.com/bufbuild/buf/cmd/buf@v1.72.0 breaking --against '.git#branch=origin/main'
    go run github.com/bufbuild/buf/cmd/buf@v1.72.0 generate
    go run github.com/bufbuild/buf/cmd/buf@v1.72.0 build --exclude-imports -o pkg/flowstate/v1/protodoc/flowstate.descriptorset.binpb
    go run github.com/bufbuild/buf/cmd/buf@v1.72.0 build --exclude-imports -o pkg/flowstate/v1/plugin/examples/flowstate-plugin-example/schema.descriptorset.binpb pkg/flowstate/v1/plugin/examples/flowstate-plugin-example/proto
    git diff --exit-code
    go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
    go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...

`make check` in the repo root runs exactly that list, in that order, with the
toolchain pins below already applied. Prefer it, and keep it and this section
saying the same thing — a copy of a command list is a thing that drifts, and the
whole point of the list is that it is what CI runs.

The `-cpu=1` line is not a smaller version of the one above it, and reaching for
more `-count` instead will not substitute. `GOMAXPROCS=1` makes goroutines
interleave only at yield points rather than running truly in parallel, so it
reaches orderings a multi-core run reaches rarely or never — it schedules
*differently*, not *harder*. It is scoped to `flowtest` because that package's
virtual clock decides when time moves from how many participants are parked,
which makes every claim it makes an ordering claim. The failure mode is the
reason it earns a line: a defect there is a **wrong answer** — a gate that
should have lapsed reporting that it did not — rather than a crash the race
detector would catch, so nothing else in the list can see it. #278's follow-on
ran clean under `-race -count=3` and reproduced three times in ten under
`-cpu=1`.

Four of those repay the trouble in ways that are not obvious.

The two `buf` checks that are not `generate` guard a contract rather than a build.
`buf lint` enforces the whole default rule set, with nothing suppressed. `buf
breaking` compares against `origin/main`, which is why it needs the base branch
fetched, and why it is the one check that can fail on a diff that compiles and tests
perfectly. The schema is public — plugins are separate processes compiling against
these descriptors — so a break here is not a compile error somebody sees, it is every
plugin in the wild.

`buf generate` followed by `git diff --exit-code` is the one people skip, and it is
the one that fails for someone else rather than for you: committed generated code
that disagrees with its schema builds perfectly until the next person regenerates.

The `buf build` line between them writes a second generated artifact under that same
pin: `pkg/flowstate/v1/protodoc/flowstate.descriptorset.binpb`, the descriptor set
the schema's own comments travel in. It is a separate command because `buf generate`
cannot produce one: protoc strips `SourceCodeInfo` from what a `.pb.go` embeds, so
protoreflect over the linked-in registry finds shape and no prose, and `buf build`
is what keeps the comments. `--exclude-imports` keeps it to this repository's schema
rather than googleapis and protovalidate as well. The `git diff --exit-code` that
follows covers both artifacts, for the identical reason it covers the first: a
checked-in descriptor set that disagrees with the schema it describes is a set of
sentences about a file that has moved on.

The line after it is the same command pointed at the example plugin's own schema,
which is a separate buf module and therefore needs its own build. That artifact is
what carries a *plugin author's* field comments to an editor's hover: the SDK
attaches it to the descriptors a manifest already ships (`sdk.Plugin.SchemaProse`,
#723), because a plugin's compiled-in descriptor has its comments stripped by
protoc exactly as the engine's does. It is opt-in for a plugin and pinned here for
this one, since prose built from a `.proto` that has since moved is worse than no
prose — a sentence attached to the wrong field.

`flow docs generate` followed by the same `git diff --exit-code` is that mechanism
pointed at prose. `docs/reference/` is derived from the task registry, the cobra
tree, the MCP tool table and one hand-kept env-var table — the four surfaces the doc
audit found had drifted — so adding a task, a flag, an RPC or a variable and not
regenerating fails here. Never hand-edit a file under `docs/reference/`; edit what it
is derived from (`cmd/flow/internal/docsgen/envvars.go` for the env-var prose) and
run `make docs`.

`govulncheck` reports *reachability* against a database fetched when it runs, so it
can go red on a tree nobody touched — a new advisory is not a new bug in your diff.
Before assuming a finding is yours, run it against `main`. If `main` fails too, the
advisory arrived rather than the code changed, and the fix is a dependency bump that
belongs to everyone. Say so plainly wherever you report it, because a scan naming
your file makes it look like yours.

It also has a failure that is not a finding at all. `go run …/govulncheck@v1.6.0`
builds govulncheck using *its* `go` directive, then type-checks your tree against
whatever toolchain `go.mod` selected — so on a machine honouring this module's
`go 1.27.0` it reports `file requires newer Go version go1.27 (application built with
go1.25)` on files in the module cache and exits 1. CI does not see this, because
`go-version-file: go.mod` installs the one version it then uses for everything.
Pin the run to match and it scans clean:

    GOTOOLCHAIN=go1.27.0 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

`staticcheck` builds the same way — its own `go.mod` selects a toolchain, so it
needs the identical pin, for the identical reason:

    GOTOOLCHAIN=go1.27.0 go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...

The staticcheck *release* is pinned to the toolchain as well as beside it, and that
direction is the one that bites. staticcheck type-checks with its own copy of
`go/types`, which reads the export data the toolchain's compiler wrote, and export
data has a format version that rises with the toolchain. Run a release older than
the toolchain and it does not report findings and it does not say the version is
unsupported — it fails per standard-library package with `internal error in
importing "math/bits" (cannot decode …, export data version 4 is greater than
maximum supported version 2); please report an issue (compile)`, which reads like a
bug in the tool rather than a pin that needs moving. 2026.1 (v0.7.0) fails exactly
that way under go1.27.0; 2026.2.1 (v0.8.1) is the release that reads it. So a
toolchain bump moves `STATICCHECK_VERSION` too, and the two move together or the
required job goes red for a reason that names nothing in your diff.

staticcheck is required in CI, and the tree is at zero findings. It landed
advisory (`continue-on-error: true`) for the 48-hour window every newly-added
check gets, because the govulncheck lesson above applies just as well to a linter
nobody has run against this tree before — a finding on landing is not necessarily
a finding in your diff. That window closed with the 22 it found settled on their
merits, so a finding now *is* one your diff introduced.

Where a finding is the point of the code, silence it in place with
`//lint:ignore <check> <reason>` and a real reason — not `//nolint:`, which
nothing here reads, and which sat uselessly over two nil-context tests for
exactly as long as nobody ran staticcheck. The reasons that survive are all one
shape: the check describes a mistake, and the test is *making* that mistake on
purpose to prove it is handled — marshaling a struct with nothing exported,
passing a nil context, spelling `%s` rather than calling `String()` so that the
verb an operator's log line uses is the one under test. Never quiet one of those
by changing what it asserts.

The bounded fuzz smoke job graduated to required on 2026-08-09, its advisory
window long closed, and `make fuzz-smoke` (run by `make check`) runs the same
target list CI runs — CI's job *is* `make fuzz-smoke`, and the list it loops
over is `tools/fuzztargets/targets.txt`, the one place a target is written down
— and the local gate therefore cannot pass a commit the required job rejects. A
crasher it finds is a real defect with a corpus entry to triage, never flake to
re-run away.

That file is the whole list, tiers and all: 30s per target on every push for
the ones tagged `smoke`, 10m per target weekly in `deep.yml`'s `fuzz-deep` job
for every target, and the package set `tools/gate` uses to decide whether a diff
can reach a fuzz target at all. Adding a target means adding a line there and
nothing else; `tools/fuzztargets`' test walks the tree for
`func Fuzz…(f *testing.F)` and fails when the file and the tree disagree, which
is how the deep tier came to be running four of ten targets before #857.

## Both execution drivers must agree

Local execution (`flow run local`) and durable execution through Temporal are two
drivers over one execution model. Anything observable — whether a step is skipped,
retried, tolerated, or how a loop reports results — must match, because local runs
exist to tell an author what production will do.

Shared cases live in `pkg/flowstate/v1/internal/conformance`; both drivers run them. Add cases
there rather than in one driver's package — and check that both drivers actually
*call* the set you added. Every function in that package had two callers, one per
driver, except `ZeroValueCases`, which had one; it sat there for months proving
half of what it was written for.

The disagreements found so far were all one shape: a value with one meaning,
written down twice. The default attempt count was `1` in `eval.go` and `5` in
`engine/policy.go`; the answer document was rendered through `marshalJSON` on one
side and a bare `protojson.Marshal` on the other. Neither pair was ever compared,
because nothing imported both. So put the value in `pkg/flowstate/v1`, which both
drivers already import, and let each read it — one constant cannot disagree with
itself.

And when you fix one of these, look immediately behind it. Local execution
defaulted to a single attempt, which hid two more disagreements that could not
show until there was a second attempt to get wrong: no maximum retry interval,
and `Retry-After` ignored where the durable driver passes it to Temporal as
`NextRetryDelay`. A bound nothing reaches is a bound nothing tests.

## Fail closed

Authentication, egress policy, secret access, and specification validation all deny
by default and deny on error. A component that allows when it cannot decide will
eventually allow everything. When adding a policy surface: deny beats allow, an
errored rule denies, and rules compile and type-check when configuration loads
rather than when a request arrives.

## Secrets never enter workflow history

Temporal history is durable and broadly readable. A secret reaches a worker as a
reference and is resolved only inside the activity that needs the value.

Two leak classes, both found the hard way:

- **Reflection through unexported fields.** `fmt` cannot call a method on a value it
  reaches through an unexported field, so it prints the fields instead. A redacting
  `String` method protects a value printed directly and does nothing when it sits in
  another struct. Hold material in a closure; reflection cannot reach a captured
  variable.
- **Unwrapping into persisted failures.** Temporal's default failure converter walks
  the `errors.Unwrap` chain and writes every level's message into the failure it
  persists, so a scrubbed error that wraps the original leaks anyway.

Test the containment shapes, not just the value: `%v`, `%+v`, `%#v`, and `%s` on the
value, on a struct holding it, and on a slice of those.

## A capability is not done until it is reachable from a Flowfile

Twice now a subsystem has been complete, tested, and impossible to use.

Secrets resolved, cached, scoped by tenant, and refused to be printed — and no workflow
could reference one, because `${secret(...)}` did not compile. Durable waiting executed
correctly on both drivers, with the schema, the engine, and Continue-As-New handling all
in place — and no Flowfile could express it, because the parser had no spelling for
`sleep:` or `wait_for_signal:`.

Both times the tests passed, because they built the value or the node directly in Go.
That proves an executor works and says nothing about whether anyone can reach it. A test
that constructs `&v1.Node{Kind: &v1.Node_Wait{...}}` is a test of the engine; the feature
is the path from a file someone writes.

So a capability lands when a Flowfile can express it, `flow validate` accepts it, and an
example in `examples/` exercises it — those run in CI, which is what keeps them honest.
Until then it is scaffolding, however green the package is.

## Test that A cannot reach B, not that A can reach A

A tenant boundary can be present, checked, and covered by passing tests, and still
leak — because the encoding is ambiguous rather than the check missing.

The env provider derived a variable name as `prefix + NAMESPACE + "_" + name`. Its
tenancy tests passed, because each asserted that a tenant reads its own secret.
Probing the other direction found the default tenant reading `TEAM_A_API_KEY`, and
namespace `team` reading `A_API_KEY`, both resolving
`$FLOWSTATE_SECRET_TEAM_A_API_KEY` — team-a's secret, from two other tenants. The
file provider had the same shape.

No separator fixes it, because every character legal in a prefix is legal in a name.
Namespacing is therefore explicit and fail-closed per backend, and where the file
provider is namespaced *every* tenant gets a segment including the default one
(`_default`, unforgeable because `ValidateNamespace` forbids underscores).

So: an isolation test asserting that each party reaches its own resource is a
functionality test wearing a security test's clothes. Write the negative direction.

## Test the traversal, not just the step

The same mistake has a second shape. Where the tenancy tests covered one direction
of a boundary, `List`'s tests covered one *page* of a walk — and a paging bug does
not live in a page.

`List` asked Temporal for a hundred executions, stopped appending once the page held
fifty, and advanced the cursor past the whole batch anyway. Temporal's page token
addresses a batch, so the other fifty ended up behind a cursor that had already moved
past them: runs the caller owned, absent from every later page rather than delayed.
Walking a namespace of 23 owned runs five at a time returned 5 and then reported the
listing *complete* — not short, but claiming to be the whole of it.

Two tests covered `List` and both stayed green through it. One asserted a page holds
the number of runs asked for; the other that the scan stops. Both are page-shaped,
and neither can see a cursor that skips. What fails is walking to exhaustion and
checking the *set* — every item reached, exactly once, and nothing belonging to
anyone else (`TestListPagingReachesEveryRun`,
`TestListPagingReachesEveryRunAmongOtherTenants`).

Two habits follow. Test the join of two features and not only each half: paging was
tested, filtering was tested, and the bug lived precisely where a filter decides
where in a batch a page fills. And when a bound exists, assert it was *reached* as
well as not exceeded — `scanned <= maxListScan` is also satisfied by a listing that
gave up after one batch, and under-scanning hides a caller's own runs just as
effectively as over-scanning costs the server.

## Diagnostics are a feature

The authoring experience is a product surface. A diagnostic should name the position
(line and column), what is wrong, and what to do instead — see
`flowfile/validate.go` for the standard. A misspelled key must be reported, not
ignored: silently doing nothing gives the author no reason to doubt the file.

False diagnostics are worse than missing ones. Some task inputs are evaluated by the
task itself against a scope the validator cannot see; check `ResolvableInputs` before
reporting a reference as unresolved.

The rule that decides the rest of them: **report what is a property of the file, and
stay silent about what a deployment decides.** A validator runs in an author's
editor and the worker runs somewhere else, so a diagnostic drawn from an egress
policy, a scheme allowlist, a port rule or a CEL rule tells an author their file is
wrong on the strength of configuration the machine they are typing on may not
share. Ask the *task* what it can never do — the http task speaks HTTP, so `ftp://`
is wrong everywhere — and leave the deployment's answers to the deployment. The
same rule keeps I/O out: `netpolicy.Policy.CheckURL` resolves the host when a proxy
is configured, which would put DNS on the editor's keystroke path.

## A rewriter has to know what the grammar binds

`flow fix` corrupting a valid file is the worst thing this repo can do, because the
whole promise of the command is that it is safe to run on anything. It has managed
it twice, and both times the rewriter knew less about scope than the language does.

The first was a name CEL binds — a macro's iteration variable — fixed by parsing
with the profile's environment. The second was the four names the *grammar* binds
bare: a loop's `as:`, the `item` a loop binds when it writes no `as:`, a step's own
`vars:` keys, and `now` inside a wait. All four are deliberately legal alongside a
step of the same id, and each was rewritten into a reference to that step.

Two things make this class hard to see. Every corrupted file still passes
`flow validate`, because a whole-step reference with no output name is legal — so
the file simply computes something else. And the scope of a binding is not the step:
a loop's item is bound for the body only, and a step's `vars:` are not in scope for
its own `if:`, which `runNodes` evaluates first. Subtracting a name too widely fails
the other way — the reference is *left* bare while the edition is stamped, and
`flow fix` exits zero on a file the validator then rejects.

So when the rewriter needs to know a scope, take it from where the engine evaluates
the thing rather than from where it is written. And test by comparing bytes or by
compiling the result: asserting the output still validates is what let all of this
through.

## `errors.AsType` in new code, and never as a sweep

Go 1.26 added `errors.AsType[E error](err error) (E, bool)`, the generic form of
`errors.As` that returns the value instead of filling in a pointer you declared a
line earlier. **New code uses it.** That is the only unconditional part of this
section.

The existing 121 `errors.As` call sites across 65 files are deliberately left
alone, and #499 is where that was decided rather than a gap nobody noticed. A
mechanical rewrite of all of them is churn with real regression surface bought for
a readability gain: the risk per site is small but not zero, and a diff that large
is one nobody reads carefully. So an existing site converts only when its file is
already open for another reason, so the conversion rides in a diff a reviewer is
looking at anyway — never as a standalone sweep PR, and never as a task picked up
on its own.

When one does ride along, three shapes do not convert mechanically, and the first
of them is a compile error rather than a silent one:

- **An interface target that does not itself implement `error`.** `errors.As`
  accepts any interface target; `AsType` constrains `E` to `error`, so
  `cmd/flow/execute.go:207` — where the target is `commandSuggester`, an interface
  whose only method is `nextCommands()` — cannot be converted at all. Leave it.
- **A `switch { case errors.As(...): }` chain.** `AsType` binds by assignment, so a
  case chain becomes an `if`/`else if` chain: a restructure to read, not a swap.
  Five sites are this shape, and three of them sit in the two densest clusters —
  `pkg/flowstate/v1/engine/workflow.go:165`, the file #499 names as the natural
  first candidate, and `pkg/flowstate/v1/plugin/sdk/errors.go:191` and `:195`.
- **A target whose value has to survive a failed match.** `errors.As` leaves the
  target untouched when it does not match, so a variable reused across branches
  still holds whatever the last successful call put there; `AsType` hands back a
  fresh zero. No site in the tree relies on that today — every reused *name* is a
  separate function's own variable — but it is the one difference that changes
  behaviour rather than shape, so check the scope before assuming a rename is all
  that happened.

The shape that actually pays is the boolean-only test, where the declaration
existed solely to be passed by address: `errors.As(err, new(*netpolicy.DenyError))`
(`plugins/git/errors.go:81`, `plugins/vcs/errors.go:61`) says what it means as
`_, ok := errors.AsType[*netpolicy.DenyError](err)`.

Temporal's `serviceerror` types are detected with `errors.As` on purpose — that is
how a `*serviceerror.NotFound` is told apart from a transport failure, and getting
it wrong turns "the run is gone" into "the server is broken". `AsType` matches
identically, but those sites carry a decision, not just a type assertion. Convert
one only with its tests in front of you.

## A design sketch names the spelling it already has

The most expensive mistakes in this repository have not been wrong code. They have
been *proposals written without reading the thing they propose to change* — and they
are expensive because a sketch that looks coherent gets discussed, refined, and
sometimes built before anybody notices it re-invents something three files away.

The shape is always the same. Someone reasons from the domain rather than from the
tree, produces a design that is internally sensible, and lands it beside an existing
answer to the same question. The result is invariant 1's violation arriving as a
*new feature* instead of as legacy debt: two hand-maintained shapes of one thing,
both current, both defensible.

Worked example, because the general statement is too easy to nod at. A sketch on
#726 proposed a `ClaimRequirement` message — `{claim, one_of_values}` — to annotate
which claims gate an RPC. It reads well. It is also the *fourth* spelling of "this
claim must carry this value" in a tree that already had three, one of them in the
schema and gating an RPC:

- `SignalPolicyRule.claims` (`proto/flowstate/v1/signal.proto:95`) — a structured
  `map<string, string>` of exact-match claim requirements, checked against the
  sender the server attested, and the thing that decides who may signal a run.
- `auth.ClaimRule` (`auth/policy.go:215`) — the same idea at token admission,
  hand-written rather than schema-defined.
- CEL, where the rest of policy lives: `SecretAccessPolicy` takes CEL strings
  (`auth/secretpolicy.go:61`), and `netpolicy` evaluates CEL over an identity
  activation already exposing `subject`, `issuer`, `namespace` and `claims`
  (`netpolicy/identity.go:27-30`).

Note what the grep changes, and that this section was itself corrected by one
(#730). Without `SignalPolicyRule` the sketch looks like it mirrors a lone legacy
struct, and "just use CEL" is the obvious answer. With it, the repository already
has a schema-defined structured claims map gating an RPC — so the live question is
whether the new surface should *be* that message rather than a fourth shape beside
it, and the strongest argument against the sketch is not "CEL exists" but "this
message exists, five files away, doing exactly this". Five minutes of grep, before
the sketch rather than after it, would have produced a better design and no
discussion.

So, before proposing a schema addition, a config surface, a policy shape, or a new
keyword:

- **Find how the repo already spells this, and cite it with `file:line`.** If the
  answer is "it doesn't", say that explicitly — that is a finding, and a reviewer
  can check it. An uncited sketch is a claim of novelty nobody can falsify.
- **Check the neighbours.** If three surfaces answer one question, the odd one out is
  usually the oldest, not the best. Do not mirror the odd one out.
- **State the cost you are choosing to pay.** Every real design loses something. A
  sketch with no stated cost has not been compared against anything.
- **Prefer deriving to duplicating.** A view computed from the source of truth cannot
  drift; a parallel declaration of the same facts always eventually does.

The rule generalizes past design. It is the same failure as a "confident, wrong
finding" from a stale checkout (#647), and the same failure as a review comment that
describes code the author has already changed: **reasoning about this repository from
memory or from first principles, when the file is right there.** Read it first. The
tree is the only thing that is authoritative about the tree.

## Opening pull requests and issues

Use `gh` — `gh pr create`, `gh issue create` — rather than an MCP or API call that
posts on a human's behalf. The two are not interchangeable in the one way that
matters to anyone reading the repository later: work an agent did should say so.
A PR authored by `claude[bot]` is legible as agent work at a glance, in the list,
in `git log`, and in a blame six months from now; the same PR posted through a
maintainer's credential is indistinguishable from something they wrote, and the
person it misleads most is the maintainer themselves, reading their own history.

`gh` is not installed by default in every environment. Install it once:

    GOBIN=/usr/local/bin go install github.com/cli/cli/v2/cmd/gh@latest

**Do not hand-write an attribution footer in a PR or issue body.** The server
appends one. Adding your own produces two, and they do not deduplicate when the
wordings differ — a body ending in "Generated with Claude Code" plus a session
link, followed by the server's "Generated by Claude Code", is what shipped on
#589 before anyone noticed. Review comments and replies are the exception: those
carry the footer explicitly, because nothing appends one for them.

**A `codex`-labelled pull request was authored by Codex, not by the account
that opened it.** Codex commits through a maintainer's credential, so its
commits carry that human as git author and a squash merge credits them —
while Codex, which wrote the diff, is credited nowhere. `41baccf` shipped
exactly that, crediting the maintainer and Claude for a change Codex wrote.

The squash commit message is ours to write, so the trailers get fixed there.
The `codex` label is the signal; do not infer authorship from a branch name.

    Co-authored-by: chatgpt-codex-connector[bot] <199175422+chatgpt-codex-connector[bot]@users.noreply.github.com>
    Co-authored-by: claude[bot] <209825114+claude[bot]@users.noreply.github.com>
    Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>

Credit Codex when the label is present. Credit Claude additionally when
Claude did work on it — a rebase, a fix for a finding, a conflict
resolution, a test — and not when Claude only pressed merge, because merging
is not authorship. Credit Copilot only where a finding of its materially
shaped the diff; a review that was read and dismissed is not co-authorship.
Do not credit the human account on a `codex` PR unless they hand-wrote part
of it: relaying a tool's output through your credential is not authorship,
and that is the whole correction.

None of this loosens the rule above. A commit already authored as
`claude[bot]` still takes no trailer naming Claude.

The point is not politeness. These numbers are what the project's own
metrics are read from, and a contribution graph that attributes an agent's
work to whoever holds the credential describes a team that does not exist.

**A commit authored as `claude[bot]` gets no `Co-authored-by` trailer naming
Claude.** It is already the author, and GitHub adds its own co-author line when
a branch is squash-merged — so a second one credits Claude twice for one commit.
On the repository's front page that renders as "claude[bot] and claude", two
avatars for one contributor, which is the same double-attribution mistake as the
footer above wearing different clothes. `13e928f` shipped carrying both
`Co-authored-by: claude[bot]` and `Co-authored-by: Claude Opus 5`.

So: commit as `claude[bot]`, and let that be the whole claim. A co-author
trailer is for a *second* contributor — a human who paired on the change, or
another agent that genuinely wrote part of it. Naming yourself in one is not
attribution, it is an echo.

Two `gh` habits worth having from the start. Every listing takes `--paginate`, and
`per_page` defaults to 30 without it, so a bare listing on a busy PR silently
returns a prefix — see the API budget section of `.claude/skills/comms-review`.
And `gh api graphql` is refused in this environment; REST is the whole surface
available, which is enough for everything except resolving a review thread.

## Working alongside other agents

When several agents edit interlocking packages:

- **Never edit a file you do not own.** Report the problem to whoever owns it. Every
  cross-package finding today was fixed faster by reporting it than it would have
  been by two agents editing one file.
- **A build error in someone else's file is probably a stale snapshot.** Several
  "urgent broken build" reports today were the tree caught mid-edit. Re-read the file
  before diagnosing, and verify with a fresh `go build ./<their package>/`.

  The loudest version of this is a generated type appearing undefined —
  `undefined: v1.Node_ForEach`, `undefined: flowstatev1.PluginManifest`. `buf
  generate` rewrites each `.pb.go` in place, so for a moment every type in the file
  is gone and every package importing it looks catastrophically broken. It reads
  like someone deleted the schema. Re-run the build before reacting; the alarming
  reading is almost always the wrong one.

- **Verify from a clean clone before believing the tree is green.** A working tree
  shared by several agents is a snapshot of everyone's unsaved work, and its
  greenness says nothing about what is committed. Clone the pushed branch to a
  scratch directory and build, vet, and test there. That is the only view a
  colleague pulling tomorrow will actually get.

  The same tree tells the same lie about *code*, and that version is quieter.
  The shared checkout sits on whatever branch somebody left it on, which after a
  wave of merges is behind `main` by every one of them. An analysis that reads it
  is not confused and does not fail — it produces findings that are internally
  consistent, specific, and describe code that stopped existing hours ago. One
  such pass reported that `flow breaking` ignored an enum's `values:` and that
  `typeName` was a hand-kept switch missing `TYPE_ENUM`, citing line numbers for
  both. Both were true of the branch it read and false of `main`, where the fix
  had landed that morning — and the second one's replacement carries a doc
  comment naming the exact defect being reported.

  So: any task whose output is a claim about the code starts with
  `git fetch origin main` and reads `origin/main`, or works in a worktree created
  from it. Cite the commit the claim is against. A finding with no revision
  attached is a finding about an unknown tree, and it costs more to disprove than
  it did to make.
- **Verify claims rather than relaying them.** Reproduce a coverage number or a
  failure before acting on it.
- **Kill processes by PID, never by pattern.** `pkill -f 'go test'` on a shared
  machine matches every sibling agent's suite, and once it matched the compound
  command that contained it and killed its own shell. Three agents re-learned this
  independently in one night, each apologizing to whoever's `make check` they had
  just ended. Record the PIDs of what you start (`$!`, a pidfile, `ps` filtered by
  your own worktree path) and kill exactly those. The corollary for the victim:
  a test run that dies with SIGTERM and no failure output was probably somebody's
  pattern, not your diff; re-run before diagnosing.
- **A dispatched agent's own background command does not wake it back up.** An
  agent that starts `go run ./tools/gate` or a CI-poll loop with a backgrounded
  shell and then ends its turn to "wait for the notification" is not paused, it
  is finished — the notification for a background job fires into whoever
  dispatched it, not back into the agent that started it, and nothing resumes a
  turn that has already returned. In one session, five separate dispatches
  stalled this way in a row, each needing a human to notice a "waiting for the
  monitor" hand-back with no monitor behind it and manually resume the agent
  with the same task. Poll a long-running command inline, in the same turn,
  and capture its real exit status rather than only whether the process is
  still alive — `kill -0` after backgrounding answers "has it exited," not
  "did it pass," and inverting it with `!` turns a failed gate into a loop
  that ends the same way a passing one does. `wait "$PID"` after the loop, or
  just run the command in the foreground and skip the backgrounding
  entirely, so a broken gate cannot look like a stopping point.
- **Commit before a checkpoint you don't control, not after.** A container
  restart during one of those stalls killed two agents mid-task and discarded
  everything they had not yet committed — one of them a finished, reviewed fix
  sitting only in the working tree because the polling loop above never got to
  the commit step. Nothing here schedules a restart for you to plan around, so
  treat every long wait (a gate run, CI, a review round) as one: commit whatever
  is correct and complete before starting it, not after it returns.
- **Ask the machine how many lanes it can hold, rather than picking a number.**
  `go run ./tools/fleet` prints how many agent lanes fit right now, which
  resource decides it, and the environment each lane must be given. Read it
  before dispatching a wave, and again before adding to one.

  The number is not a constant, and treating it as one is how this goes wrong.
  A lane is not one process: `go test ./...` builds with `-p` workers and each
  package's binary then runs `-parallel` tests inside itself, both defaulting to
  the core count — so a single unbounded lane can saturate a small box, and four
  of them on a four-core machine produced a load average above 20, with lanes
  reporting link failures they reasonably mistook for defects in their own
  diffs. That is why the tool prints an appetite as well as a count: the fleet
  size is capacity divided by what a lane is allowed to spend, and the division
  only means something if the lane is actually held to it.

      eval "$(go run ./tools/fleet -env)"     # what one lane may spend
      go run ./tools/fleet -n                 # how many fit, for a script

  It emits `export` statements, and that matters: bare assignments evaluated in
  a shell are shell-local, so a lane given them runs at exactly the unbounded
  defaults this exists to prevent while the shell shows the value and `go test`
  never sees it.

  When disk is the bound, it can fix it rather than only naming it:

      go run ./tools/fleet -prune             # give back what a lane needs

  This exists because Go has no size budget for its build cache and cannot be
  given one. `go help cache` offers exactly two levers, and on a busy day
  neither works: a sweep of entries unused for *five days*, run at most once a
  day, and `go clean -cache`, which discards everything. A machine that fills
  twenty-three gigabytes between breakfast and lunch has nothing five days old
  to sweep, so the first is a no-op — and the second charges a cold rebuild to
  every lane, which is itself load enough to hold the fleet at zero for as long
  as it runs. That is not hypothetical: it happened here, and the tool sat
  correctly reporting "dispatch nothing" until somebody noticed.

  `-prune` is the missing middle — cmd/go's own `trimSubdir` with a *size*
  cutoff where it has a *time* one. It takes oldest entries first until a lane
  fits and stops, so the hot entries survive. Its rules are Go's, because the
  cache is Go's format: only `-a` and `-d` names are entries, an entry may be a
  *directory* (an executable cache entry) needing `RemoveAll`, and mtime is a
  real last-used signal because Go refreshes it on use. One rule is ours: the
  fuzz corpus is never touched — those inputs cost machine-hours to rediscover
  and `go help cache` says plainly that removing them makes fuzzing less
  effective.

  Safe to run while builds are in flight, and that safety is the same property
  the disk floor protects: the cache is content-addressed, so a removed entry
  is a miss and a miss is a rebuild. What is *not* safe is running out of disk
  mid-write, which leaves a partial object that surfaces later as a corrupt
  cache entry and reads like a compiler bug.

  It reads cores, memory, disk and the current load, and the smallest bound
  wins. Load matters as much as capacity: it counts work this process cannot
  see — a sibling session's suite, a lane that has not reported — and a box
  thrashing on disk reads high there even when its cores look idle, which is
  exactly when adding a lane hurts most. Disk is reserved rather than divided,
  because a build that meets ENOSPC halfway leaves a partial object the next
  build reports as a corrupt cache entry, which reads like a compiler bug; this
  machine hit that twice in one session behind a 13 GiB build cache.

  The point is that it adapts. On a bigger machine it says a bigger number
  without anybody editing this file, and on a full one it says zero and names
  what to prune. A lane count written down here would have been right once.

- **Leave a green stopping point.** A package with fewer features that compiles and
  passes beats a half-migrated one. If a migration cannot finish, back it out and
  document it rather than leaving both halves.
