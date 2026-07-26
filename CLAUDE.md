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

Depth bounds do not stop breadth explosions, and time bounds do not stop memory
explosions. Ask which resource the attacker controls, then bound that resource.

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

## Both execution drivers must agree

Local execution (`flow run local`) and durable execution through Temporal are two
drivers over one execution model. Anything observable — whether a step is skipped,
retried, tolerated, or how a loop reports results — must match, because local runs
exist to tell an author what production will do.

Shared cases live in `pkg/flowstate/v1/tests`; both drivers run them. Add cases
there rather than in one driver's package.

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

## Diagnostics are a feature

The authoring experience is a product surface. A diagnostic should name the position
(line and column), what is wrong, and what to do instead — see
`flowfile/validate.go` for the standard. A misspelled key must be reported, not
ignored: silently doing nothing gives the author no reason to doubt the file.

False diagnostics are worse than missing ones. Some task inputs are evaluated by the
task itself against a scope the validator cannot see; check `ResolvableInputs` before
reporting a reference as unresolved.

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
- **Verify claims rather than relaying them.** Reproduce a coverage number or a
  failure before acting on it.
- **Leave a green stopping point.** A package with fewer features that compiles and
  passes beats a half-migrated one. If a migration cannot finish, back it out and
  document it rather than leaving both halves.
