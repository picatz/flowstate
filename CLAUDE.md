# Working on Flowstate

Guidance for anyone — human or agent — making changes here. It exists because each
item below cost real time or nearly cost a machine.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for what the system is and the
invariants that constrain changes. Read the invariants before a structural change;
a change that violates one is a bug even when the tests pass.

## Proto-first

Types describing the system live in `proto/flowstate/v1/flowstate.proto`, not as
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

A `go test` command that returns does not mean the test binary exited. If a run
behaves oddly, check:

    ps -Ao pid,rss,args | grep -E '\.test|-fuzz' | grep -v grep

## The gate: diff-scoped before a push, full on PR CI

Two tiers over one list of checks. The diff-scoped tier runs locally before a
push and covers what your diff can reach. PR CI runs the full list as seven
parallel jobs in about six minutes and is the gate that decides; the
orchestrator's webhook loop watches the PR and drives red back to green.
`make check` runs the same full list locally, and remains the rehearsal for
main-composition verification and for anyone who wants the whole answer on
their own machine.

Before pushing a PR branch, run the diff-scoped tier:

    go run ./tools/gate        # or: make gate

It computes the changed files against the merge-base with origin/main, maps
them to packages, expands to every package whose build or tests can see a
changed one, then runs the build, gofmt on the changed files, and vet plus
bounded `-race` tests for the affected set. Conditional legs fire only when
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

This inverts the old default of this section, which told everyone to run the
full list locally before every push because a CI round trip bought nothing.
That reasoning predates a six-minute parallel CI and a standing red-to-green
driver: in the last wave, five agents each re-ran the full list serially on
one contended machine at 30 to 60 minutes per gate, to predict an answer CI
returns in six (#482). What survives is the bound, not the venue: nothing
merges red, and every required check still runs on every PR.

Two habits the last wave paid for, now written down. Editing `docs/DSL.md`
requires `go generate ./cmd/flow/internal/reference`, because that package
holds a generated mirror of the document and `TestTheMirrorMatchesTheRepository`
fails on drift; the gate's docs leg runs this for you. And generated files
(`*.pb.go`, `docs/reference/`, the reference mirror, the descriptorset) are
never edited directly: edit the source they derive from and regenerate. The
helper programs under `tools/hooks/` can check both when invoked from trusted,
user-local editor configuration; the repository does not automatically run
code from the mutable checkout.

### `make check`: the full rehearsal

Every check in `.github/workflows/ci.yml`, run locally, in CI order. Run it
when verifying what main composes to, before release-shaped changes, when a
CI failure needs local reproduction, or whenever the diff-scoped answer is
not the whole answer you want:

    go build ./...
    go vet ./...
    gofmt -l ./cmd ./pkg                       # must print nothing
    GOMEMLIMIT=2GiB go test -race -timeout 900s ./...
    make test-plugins                          # the plugin modules ./... cannot reach
    GOMEMLIMIT=1GiB go test -race -cpu=1 -count=20 -timeout 300s ./pkg/flowstate/v1/flowtest/
    go run ./cmd/flow fix --check examples/
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
    git diff --exit-code
    go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
    go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

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
whatever toolchain `go.mod` selected — so on a machine honouring `toolchain
go1.26.6` it reports `file requires newer Go version go1.26 (application built with
go1.25)` on files in the module cache and exits 1. CI does not see this, because
`go-version-file: go.mod` installs the one version it then uses for everything.
Pin the run to match and it scans clean:

    GOTOOLCHAIN=go1.26.6 go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

`staticcheck` builds the same way — its own `go.mod` selects a toolchain, so it
needs the identical pin, for the identical reason:

    GOTOOLCHAIN=go1.26.6 go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

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
window long closed, and `make fuzz-smoke` (run by `make check`) is the same four
commands CI runs, so the local gate cannot pass a commit the required job
rejects. A crasher it finds is a real defect with a corpus entry to triage,
never flake to re-run away.

## Both execution drivers must agree

Local execution (`flow run local`) and durable execution through Temporal are two
drivers over one execution model. Anything observable — whether a step is skipped,
retried, tolerated, or how a loop reports results — must match, because local runs
exist to tell an author what production will do.

Shared cases live in `pkg/flowstate/v1/tests`; both drivers run them. Add cases
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
- **Leave a green stopping point.** A package with fewer features that compiles and
  passes beats a half-migrated one. If a migration cannot finish, back it out and
  document it rather than leaving both halves.
