package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The checks, exercised against source written to say one thing each.
//
// Fixtures rather than the tree, because a test that asserts "the repository
// currently has two of these" fails the day somebody writes a third and says
// nothing about whether the check is right. What the tree is worth is asserted
// once, at the bottom, and it is the only claim here that mentions a number.

// analyzed is a fixture package on disk, run through [Analyze].
//
// On disk because the analysis walks directories: pointing it at a temporary
// one is the same code path CI runs, where handing it a parsed file would test
// a function nothing calls that way.
func analyzedFixture(t *testing.T, source string) []Finding {
	t.Helper()

	findings, _ := analyzedFile(t, "fixture_test.go",
		"package fixture\n\nimport (\n\t\"fmt\"\n\t\"testing\"\n\n\t\"github.com/stretchr/testify/require\"\n)\n\nvar _ = require.NotEmpty\nvar _ = fmt.Sprint\n\n"+source)

	return findings
}

// analyzedFile writes one complete file under a chosen name and analyses it,
// for the claims that are about the *file* rather than about its contents.
func analyzedFile(t *testing.T, name, source string) ([]Finding, int) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600))

	findings, tests, err := Analyze(dir)
	require.NoError(t, err)

	return findings, tests
}

// checksFound is the findings' checks, for comparing against what a fixture
// was written to produce.
func checksFound(findings []Finding) []string {
	found := make([]string, 0, len(findings))
	for _, finding := range findings {
		found = append(found, string(finding.Check))
	}

	return found
}

func TestATestThatAssertsNothingIsFound(t *testing.T) {
	t.Parallel()

	findings := analyzedFixture(t, `
func TestNothing(t *testing.T) {
	value := 1 + 1
	_ = value
}
`)

	require.Len(t, findings, 1, "the one test in the fixture asserts nothing and was not found")
	assert.Equal(t, CheckUnasserted, findings[0].Check)
	assert.Equal(t, "TestNothing", findings[0].Test)
	assert.True(t, findings[0].Fatal(), "an unasserted test has to fail the command or nothing changes")
}

// TestHandingOverTheTestHandleCountsAsAsserting is the conservative direction,
// and the one that decides whether anybody can believe this tool.
//
// A test that gives its `t` to a conformance helper is asserting whatever that
// helper asserts. This analysis has no types with which to follow the call, so
// counting it as an assertion is a false negative traded for the false positive
// that would otherwise land on the shape this tree uses most — eleven of the
// first thirteen findings were exactly this, before the rule existed.
func TestHandingOverTheTestHandleCountsAsAsserting(t *testing.T) {
	t.Parallel()

	assert.Empty(t, analyzedFixture(t, `
func TestDelegates(t *testing.T) {
	conformance.AssertTheOutcome(t, 1)
}
`), "a test that handed its handle to something was called vacuous")

	// And a method *on* the handle is not handing it over: `t.Context()` gives
	// away nothing, which is what separates the case above from a test that
	// really does assert nothing.
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(analyzedFixture(t, `
func TestOnlyUsesTheHandle(t *testing.T) {
	t.Parallel()
	_ = t.Context()
	_ = t.TempDir()
}
`)), "calling methods on the handle was mistaken for delegating to something that asserts")
}

// TestAnAssertingHelperMakesItsCallersAssert follows the same-package chain,
// and repeats because one pass over it is sometimes enough by luck.
//
// The propagation is a fixpoint over a map, and Go randomizes map iteration
// per run — so a single pass resolves a two-deep chain whenever it happens to
// visit the helpers before the test, and does not when it does not. A
// single-shot version of this test survived the mutation that cuts the
// iteration to one pass. Repetition is what makes the claim about the
// algorithm rather than about the ordering it drew.
func TestAnAssertingHelperMakesItsCallersAssert(t *testing.T) {
	t.Parallel()

	const chain = `
func check(value int) {
	require.NotEmpty(nil, value)
}

func alsoCheck(value int) {
	check(value)
}

func thenCheck(value int) {
	alsoCheck(value)
}

func TestThroughThreeHelpers(t *testing.T) {
	thenCheck(1)
}
`

	for attempt := range 40 {
		require.Empty(t, analyzedFixture(t, chain),
			"attempt %d: a test asserting through three helpers was called vacuous, so the "+
				"propagation resolved on some map orderings and not others", attempt)
	}
}

// TestMutuallyRecursiveHelpersResolveToTheHonestAnswer is the reason the
// propagation is iterated rather than recursed.
//
// Two helpers that call each other is legal Go, and a naive walk of that graph
// does not come back — which in a check that runs in CI is not a wrong answer
// but no answer at all. The bounded loop cannot hang, so what is asserted here
// is the answer rather than the termination: neither helper asserts, so the
// test that calls them does not either.
func TestMutuallyRecursiveHelpersResolveToTheHonestAnswer(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(analyzedFixture(t, `
func ping(n int) {
	if n > 0 {
		pong(n - 1)
	}
}

func pong(n int) {
	if n > 0 {
		ping(n - 1)
	}
}

func TestBouncing(t *testing.T) {
	ping(4)
}
`)), "mutual recursion between helpers did not resolve to the honest answer")
}

// TestAMarkerWithAReasonExcusesATestAndAnEmptyOneDoesNot is the escape hatch,
// and the thing that stops it being a way to make the report go quiet.
func TestAMarkerWithAReasonExcusesATestAndAnEmptyOneDoesNot(t *testing.T) {
	t.Parallel()

	assert.Empty(t, analyzedFixture(t, `
// TestDeliberate proves something by not crashing.
//
//vacuity:ignore unasserted a subprocess entry point, whose assertions are made by whoever launched it
func TestDeliberate(t *testing.T) {
	_ = 1
}
`), "a marker carrying a reason did not excuse the test")

	assert.Len(t, analyzedFixture(t, `
//vacuity:ignore unasserted
func TestBare(t *testing.T) {
	_ = 1
}
`), 1, "a marker with no reason silenced the check, which makes it a mute button rather than a decision")

	assert.Len(t, analyzedFixture(t, `
//vacuity:ignore conditional wrong check for this finding
func TestWrongCheck(t *testing.T) {
	_ = 1
}
`), 1, "a marker naming a different check silenced this one")
}

// TestAClaimOnlyInsideALoopOverSomethingUncountableIsFound is the second check,
// and the shape this repository keeps shipping.
func TestAClaimOnlyInsideALoopOverSomethingUncountableIsFound(t *testing.T) {
	t.Parallel()

	findings := analyzedFixture(t, `
func TestEveryCaseHolds(t *testing.T) {
	for _, one := range conformance.Cases() {
		require.NotEmpty(t, one)
	}
}
`)

	require.Len(t, findings, 1)
	assert.Equal(t, CheckConditional, findings[0].Check)
	assert.Equal(t, "conformance.Cases()", findings[0].Detail,
		"the report has to name what to assert about, not only that something is missing")
	assert.False(t, findings[0].Fatal(),
		"a conditional finding must never fail the command: the tree holds these by the "+
			"hundred, and enforcing that number would mean a sweep or an allowlist")
}

// TestATableAReaderCanCountIsNotAFinding is the clause that keeps the ordinary
// table test out of the report.
//
// Without it the check reported about twice as many sites, and the ones that
// mattered were buried among them. A literal — inline, or assigned to a name a
// few lines up — is one whoever is reading the test can count.
func TestATableAReaderCanCountIsNotAFinding(t *testing.T) {
	t.Parallel()

	assert.Empty(t, analyzedFixture(t, `
func TestInline(t *testing.T) {
	for _, one := range []int{1, 2, 3} {
		require.NotZero(t, one)
	}
}
`), "a loop over a literal written right there was reported")

	assert.Empty(t, analyzedFixture(t, `
func TestNamed(t *testing.T) {
	cases := []int{1, 2, 3}
	for _, one := range cases {
		require.NotZero(t, one)
	}
}
`), "a loop over a name assigned a literal in the same function was reported")

	// An *empty* literal is a different thing entirely: countable, and the
	// count is zero. Nothing in the tree does this, and it is the one case
	// where the loop is provably vacuous rather than possibly.
	assert.Equal(t, []string{string(CheckConditional)}, checksFound(analyzedFixture(t, `
func TestEmptyTable(t *testing.T) {
	cases := []int{}
	for _, one := range cases {
		require.NotZero(t, one)
	}
}
`)), "a loop over a literal with nothing in it was not reported")
}

// TestEveryFixTheReportAsksForSettlesIt is the other half of a check being
// usable: what it tells you to do has to work.
//
// A report that names a finding and then rejects the obvious fix is worse than
// no report, because the person does the work and the number does not move.
// The five spellings below are the ones somebody actually reaches for, and two
// clauses answer them between them — an assertion outside the loop settles the
// first four, and only the skip needs [skipsWhenEmpty]. That split is not
// visible from here on purpose: what a caller is owed is that the fix works,
// not which branch honoured it.
func TestEveryFixTheReportAsksForSettlesIt(t *testing.T) {
	t.Parallel()

	for _, guard := range []string{
		"require.NotEmpty(t, cases)",
		"require.Len(t, cases, 3)",
		"require.Greater(t, len(cases), 0)",
		"if len(cases) == 0 {\n\t\tt.Fatal(\"the corpus is empty\")\n\t}",
		"if len(cases) == 0 {\n\t\tt.Skip(\"nothing to check here\")\n\t}",
	} {
		assert.Empty(t, analyzedFixture(t, `
func TestGuarded(t *testing.T) {
	cases := conformance.Cases()
	`+guard+`
	for _, one := range cases {
		require.NotZero(t, one)
	}
}
`), "the guard %q did not settle the finding, so the report asks for a fix it does not accept", guard)
	}
}

// TestOnlyAFailureThroughTheHandleCountsIsAnAssertion closes the hole that
// would have made the fatal check unenforceable.
//
// `t.Errorf` fails a test. `err.Error()` and `fmt.Errorf(…)` do not, and a
// prefix match on the method name cannot tell them apart. Since `err.Error()`
// appears in almost every test in this tree, reading it as an assertion meant
// a test that claimed nothing at all walked past the one check that fails a
// build (Codex, #1125).
func TestOnlyAFailureThroughTheHandleCountsIsAnAssertion(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(analyzedFixture(t, `
func TestLooksLikeItAsserts(t *testing.T) {
	err := fmt.Errorf("something")
	_ = err.Error()
	_ = fmt.Errorf("wrapping: %w", err)
}
`)), "a test whose only `Error`-named calls are on an error was read as asserting")

	// The same method name, through the handle, is the real thing.
	assert.Empty(t, analyzedFixture(t, `
func TestReallyAsserts(t *testing.T) {
	if 1 != 1 {
		t.Errorf("impossible")
	}
}
`), "failing through the handle was not read as asserting")

	// And through a field of that type, read directly. `h.t.Fatalf(…)` fails
	// the test as surely as `t.Fatalf(…)` does, and the receiver is a selector
	// rather than the identifier the function was handed — so the field names
	// the package declares as handles are part of its vocabulary too.
	//
	// The harness arrives from a constructor this fixture does not hand the
	// test's own `t` to, deliberately: nothing else here can save the test, so
	// the field receiver is the whole of what is being asserted.
	assert.Empty(t, analyzedFixture(t, `
type harness struct {
	t *testing.T
}

func newHarness() *harness { return &harness{} }

func TestThroughAField(t *testing.T) {
	h := newHarness()
	if 1 != 1 {
		h.t.Fatalf("impossible")
	}
}
`), "failing through a handle held in a field was not read as asserting")
}

// TestEveryWayOfGivingTheHandleAwayCountsAsAsserting covers the four shapes one
// at a time.
//
// They overlap in real code — `cmd/flow/dap_test.go` builds `&dapConn{t: t, …}`
// *and* calls methods on it that use the handle — so a fixture written the way
// a person writes tests is saved several times over and pins none of them.
// Each case below is written so that exactly one mechanism can save it, which
// is what makes removing any one of them fail something.
//
// Kept rather than trimmed, and that is a decision worth stating: none of the
// four changes a single answer on this tree today. But this check fails a
// build, so a false positive costs more than a false negative, and each shape
// is ordinary Go that somebody will write next week. That is the opposite call
// from the dead clause deleted earlier in this package — the difference being
// that clause could never fire, where these are reachable and merely not yet
// reached.
func TestEveryWayOfGivingTheHandleAwayCountsAsAsserting(t *testing.T) {
	t.Parallel()

	// Passed to a call. The helper is in another package, so nothing here can
	// see whether it asserts.
	assert.Empty(t, analyzedFixture(t, `
func TestPassed(t *testing.T) {
	conformance.AssertTheOutcome(t, 1)
}
`), "a handle passed to a call was not read as given away")

	// Stored in a literal, and nothing is done with the result: no method to
	// propagate through, no field read.
	assert.Empty(t, analyzedFixture(t, `
type sink struct {
	t *testing.T
}

func TestStored(t *testing.T) {
	_ = &sink{t: t}
}
`), "a handle stored in a literal was not read as given away")

	// Assigned into a field, with the same isolation.
	assert.Empty(t, analyzedFixture(t, `
type slot struct {
	t *testing.T
}

func TestAssigned(t *testing.T) {
	var s slot
	s.t = t
	_ = s
}
`), "a handle assigned into a field was not read as given away")

	// Delegated to a method, where the handle is never given away in the test
	// at all — so only following the receiver's method can save it.
	assert.Empty(t, analyzedFixture(t, `
type suite struct {
	t *testing.T
}

func newSuite() *suite { return &suite{} }

func (s *suite) check(got int) {
	require.NotZero(s.t, got)
}

func TestDelegated(t *testing.T) {
	s := newSuite()
	s.check(1)
}
`), "a test delegating to an asserting method was called vacuous")
}

// TestASkipMustBeTakenWhenTheCorpusIsEmpty is the guard's direction.
//
// `if len(cases) > max { t.Skip(…) }` mentions a length and skips, and is not
// taken by an empty corpus — so the loop still runs zero times, the test still
// claims nothing, and settling the finding on it makes the report go quiet
// about exactly what it exists to say (Codex, #1125).
func TestASkipMustBeTakenWhenTheCorpusIsEmpty(t *testing.T) {
	t.Parallel()

	for _, settles := range []string{
		"if len(cases) == 0 {",
		"if 0 == len(cases) {",
		"if len(cases) < 1 {",
		"if len(cases) <= 0 {",
	} {
		assert.Empty(t, analyzedFixture(t, `
func TestGuarded(t *testing.T) {
	cases := conformance.Cases()
	`+settles+`
		t.Skip("nothing to check")
	}

	for _, one := range cases {
		require.NotZero(t, one)
	}
}
`), "%q is an emptiness test and did not settle the finding", settles)
	}

	for _, doesNot := range []string{
		"if len(cases) > 100 {",
		"if len(cases) != 0 {",
		"if len(cases) >= 1 {",
	} {
		assert.Equal(t, []string{string(CheckConditional)}, checksFound(analyzedFixture(t, `
func TestGuardedWrong(t *testing.T) {
	cases := conformance.Cases()
	`+doesNot+`
		t.Skip("not this run")
	}

	for _, one := range cases {
		require.NotZero(t, one)
	}
}
`)), "%q is not taken when the corpus is empty and settled the finding anyway", doesNot)
	}
}

// TestASkipAboutSomethingElseDoesNotSettleIt keeps the one guard that is not
// an assertion from becoming a way past the check.
//
// A skip on an empty corpus clears the finding because it reports the
// emptiness. A skip on the operating system reports nothing of the kind, and a
// test that has one is as silent about its corpus as a test that has neither —
// so the guard has to be about a *length*, not about skipping.
//
// Found by mutation: dropping the length half left every test with any skip in
// it unreported, and nothing here noticed.
func TestASkipAboutSomethingElseDoesNotSettleIt(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{string(CheckConditional)}, checksFound(analyzedFixture(t, `
func TestSkipsElsewhere(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("not on windows")
	}

	for _, one := range conformance.Cases() {
		require.NotZero(t, one)
	}
}
`)), "a skip unrelated to the corpus settled a finding about the corpus")
}

// TestAClaimOutsideTheLoopSettlesIt is the other way a test is unconditional:
// it asserts something whatever the corpus holds.
func TestAClaimOutsideTheLoopSettlesIt(t *testing.T) {
	t.Parallel()

	assert.Empty(t, analyzedFixture(t, `
func TestAssertsFirst(t *testing.T) {
	got := conformance.Cases()
	require.NotNil(t, got)

	for _, one := range got {
		require.NotZero(t, one)
	}
}
`), "a test asserting outside its loop was reported as conditional")
}

// TestBenchmarksAndFuzzTargetsAreNotTests keeps the checks off functions that
// are doing what they are for.
//
// A benchmark's job is to run the code. A fuzz target's claims live in the
// corpus and in the runtime rather than in the function. Flagging either would
// be the check misunderstanding what it is looking at.
func TestBenchmarksAndFuzzTargetsAreNotTests(t *testing.T) {
	t.Parallel()

	assert.Empty(t, analyzedFixture(t, `
func BenchmarkSomething(b *testing.B) {
	for b.Loop() {
		_ = 1 + 1
	}
}

func FuzzSomething(f *testing.F) {
	f.Fuzz(func(t *testing.T, in string) {
		_ = len(in)
	})
}
`), "a benchmark or a fuzz target was judged as a test")
}

// TestOnlyWhatTheRunnerCallsIsJudged keeps the checks off functions that look
// like tests and are not.
//
// Three ways to look like one and not be, and all three would fail a build over
// something `go test` never invokes (Copilot and Codex, #1125). A `Test…`
// function in an ordinary file is a helper for other packages to call —
// `func TestFixture(t testing.TB)` is a real shape in real repositories. And a
// `Test…` taking a `*testing.B` or a `*testing.F` is a benchmark or a fuzz
// target that has been misnamed, which is a style question rather than a
// vacuous claim.
func TestOnlyWhatTheRunnerCallsIsJudged(t *testing.T) {
	t.Parallel()

	// A reusable suite an ordinary file exports for downstream `_test.go`
	// files to call. It has the runner's exact signature and the runner never
	// calls it, because the runner only reads `_test.go` files — so the file it
	// sits in is the only thing that tells them apart. This repository has
	// `Test…`-named functions in ordinary files today
	// (`cmd/flow/internal/mcp/mcp.go:1046`), which is what makes the shape
	// worth excluding rather than hypothetical.
	const suite = `package fixture

import "testing"

func TestConformance(t *testing.T) {
	_ = 1
}
`
	findings, tests := analyzedFile(t, "suite.go", suite)
	assert.Empty(t, findings,
		"a `Test…` in an ordinary file was judged, so a build fails over a function "+
			"`go test` never invokes")
	assert.Zero(t, tests, "a `Test…` in an ordinary file was counted as a test")

	// The same declaration in a file the runner does read is a test.
	findings, tests = analyzedFile(t, "suite_test.go", suite)
	assert.Len(t, findings, 1, "the same declaration in a _test.go file was not judged")
	assert.Equal(t, 1, tests)

	// And the handle has to be the one the runner passes, wherever it lives.
	const helper = `package fixture

import "testing"

func TestFixture(t testing.TB) {
	_ = 1
}
`
	findings, tests = analyzedFile(t, "fixture.go", helper)
	assert.Empty(t, findings, "a helper in an ordinary file was judged as a test")
	assert.Zero(t, tests, "a helper in an ordinary file was counted as a test")

	findings, tests = analyzedFile(t, "fixture_test.go", helper)
	assert.Empty(t, findings, "a `Test…` taking a testing.TB was judged as a test")
	assert.Zero(t, tests, "a `Test…` taking a testing.TB was counted as a test")

	// And the real thing, in the right file, is.
	findings, tests = analyzedFile(t, "fixture_test.go", `package fixture

import "testing"

func TestReal(t *testing.T) {
	_ = 1
}
`)
	assert.Len(t, findings, 1, "a real test asserting nothing was not judged")
	assert.Equal(t, 1, tests)
}

// TestTheTestingImportIsResolvedToItsLocalName is the alias, which `go test`
// honours and a literal match does not.
//
// `import tst "testing"` is legal and the runner calls
// `func TestAliased(t *tst.T)` exactly as it calls any other test. An analysis
// insisting on the identifier `testing` misses it entirely — neither reported
// nor counted — and a count that silently omits what it could not recognise is
// the failure this whole tool is about, wearing the checker's own clothes
// (Codex, #1125).
func TestTheTestingImportIsResolvedToItsLocalName(t *testing.T) {
	t.Parallel()

	findings, tests := analyzedFile(t, "aliased_test.go", `package fixture

import tst "testing"

func TestAliased(t *tst.T) {
	_ = 1
}
`)
	require.Equal(t, 1, tests, "an aliased test was not counted")
	require.Len(t, findings, 1, "an aliased test asserting nothing was not reported")
	assert.Equal(t, "TestAliased", findings[0].Test)

	// A dot import puts the names in the file's own scope, so there is no
	// selector to match at all.
	findings, tests = analyzedFile(t, "dotted_test.go", `package fixture

import . "testing"

func TestDotted(t *T) {
	_ = 1
}
`)
	require.Equal(t, 1, tests, "a dot-imported test was not counted")
	assert.Len(t, findings, 1, "a dot-imported test asserting nothing was not reported")

	// And a file that does not import testing at all has no tests in it,
	// whatever it names its functions.
	findings, tests = analyzedFile(t, "unrelated_test.go", `package fixture

type T struct{}

func TestNotATest(t *T) {
	_ = 1
}
`)
	assert.Zero(t, tests, "a function taking an unrelated type named T was counted as a test")
	assert.Empty(t, findings)
}

// TestAQuietMethodDoesNotLaunderItsCaller is the limit of following the
// receiver.
//
// Methods participate in the propagation because a test that hands its checks
// to one — `s.check(got)` — reaches its assertion through a declaration a
// function-only walk drops on the floor (Codex, #1125). The risk in widening
// it that way is that *every* method call starts reading as an assertion,
// which would hollow the check out entirely.
func TestAQuietMethodDoesNotLaunderItsCaller(t *testing.T) {
	t.Parallel()

	// The positive direction is
	// [TestEveryWayOfGivingTheHandleAwayCountsAsAsserting]'s last case, where
	// it is isolated. This is the direction that keeps the mechanism from
	// swallowing the check whole: a method that asserts nothing does not
	// launder its caller.
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(analyzedFixture(t, `
type quiet struct{}

func (q *quiet) look(got int) {
	_ = got
}

func TestThroughAQuietMethod(t *testing.T) {
	q := &quiet{}
	q.look(1)
}
`)), "delegating to a method that asserts nothing was read as asserting")
}

// TestAManualFailureCountsAsAnAssertion covers the ways to fail a test that
// are not a call on the handle.
//
// `if got != want { panic("mismatch") }` is a manual assertion — the testing
// runner turns the panic into a failure — and it was being reported fatally
// unasserted for doing exactly what the check looks for, because `panic` is a
// builtin called on nothing and the walk wanted a selector (Codex, #1125). A
// false positive on a check that fails a build is the expensive direction.
func TestAManualFailureCountsAsAnAssertion(t *testing.T) {
	t.Parallel()

	assert.Empty(t, analyzedFixture(t, `
func TestPanics(t *testing.T) {
	got, want := 1, 2
	if got != want {
		panic("mismatch")
	}
}
`), "a test that fails by panicking was called vacuous")

	// And it propagates, so a helper written that way carries its callers.
	assert.Empty(t, analyzedFixture(t, `
func mustMatch(got, want int) {
	if got != want {
		panic("mismatch")
	}
}

func TestThroughAPanickingHelper(t *testing.T) {
	mustMatch(1, 1)
}
`), "a helper that fails by panicking did not carry its caller")
}

// TestTheAssertionPackagesAreResolvedNotGuessed keeps an unrelated name from
// silencing the check.
//
// `assert` and `require` are ordinary Go identifiers. A call on one was read as
// an assertion whatever it was bound to, so a vacuous test holding a fixture
// call like `require.Load()` walked past a check that fails builds
// (Codex, #1125). They are matched by import path now, so an alias works and a
// local variable does not.
func TestTheAssertionPackagesAreResolvedNotGuessed(t *testing.T) {
	t.Parallel()

	findings, _ := analyzedFile(t, "shadowed_test.go", `package fixture

import "testing"

type loader struct{}

func (l loader) Load() int { return 1 }

func TestShadowed(t *testing.T) {
	require := loader{}
	_ = require.Load()
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a local value named `require` silenced the check")

	// An alias of the real package is the real thing.
	findings, _ = analyzedFile(t, "aliased_testify_test.go", `package fixture

import (
	"testing"

	req "github.com/stretchr/testify/require"
)

func TestAliasedTestify(t *testing.T) {
	req.NotZero(t, 1)
}
`)
	assert.Empty(t, findings, "testify imported under an alias was not recognised")
}

// TestASkipIsAboutTheCollectionItNames matches the guard to the loop.
//
// A boolean answer settled every finding in the function, so a skip on one
// collection stood in for a claim about another: `if len(optional) == 0 {
// t.Skip(…) }` before a loop over `Cases()` silenced a finding about `Cases()`,
// which is still empty and still makes every assertion disappear
// (Codex, #1125).
func TestASkipIsAboutTheCollectionItNames(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{string(CheckConditional)}, checksFound(analyzedFixture(t, `
func TestGuardsTheWrongThing(t *testing.T) {
	optional := conformance.Optional()
	if len(optional) == 0 {
		t.Skip("nothing optional here")
	}

	for _, one := range conformance.Cases() {
		require.NotZero(t, one)
	}
}
`)), "a skip about one collection settled a finding about another")

	// Naming the loop's own subject settles it.
	assert.Empty(t, analyzedFixture(t, `
func TestGuardsTheRightThing(t *testing.T) {
	cases := conformance.Cases()
	if len(cases) == 0 {
		t.Skip("nothing to check")
	}

	for _, one := range cases {
		require.NotZero(t, one)
	}
}
`), "a skip about the loop's own subject did not settle it")
}

// TestBuildTaggedDeclarationsAreEachJudged keeps one variant from hiding
// another.
//
// This walk groups files by their `package` clause and ignores build tags on
// purpose, which is what lets it read every variant — and is also what makes
// two files declaring `TestPlatform` land on one key. Storing one per name kept
// whichever the directory listing reached last, so the count was short and a
// vacuous variant could be hidden by an asserting one depending on filename
// order (Codex, #1125).
func TestBuildTaggedDeclarationsAreEachJudged(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a_linux_test.go"), []byte(`//go:build linux

package fixture

import "testing"

func TestPlatform(t *testing.T) {
	_ = 1
}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "z_windows_test.go"), []byte(`//go:build windows

package fixture

import "testing"

func TestPlatform(t *testing.T) {
	t.Fatal("no")
}
`), 0o600))

	findings, tests, err := Analyze(dir)
	require.NoError(t, err)

	assert.Equal(t, 2, tests, "two declarations of one name were counted as %d", tests)
	require.Len(t, findings, 1,
		"the vacuous variant was hidden by its asserting sibling, or reported twice")
	assert.Contains(t, findings[0].Pos, "a_linux_test.go",
		"the finding names the wrong variant")
}

// TestALocalAliasOfTheHandleIsOne is the other spelling `go test` honours.
//
// `type T = testing.T; func TestAliased(t *T)` runs like any other test, and a
// match on the qualified name alone misses it entirely — neither reported nor
// counted, where the test-count floor cannot reveal one omission
// (Codex, #1125).
func TestALocalAliasOfTheHandleIsOne(t *testing.T) {
	t.Parallel()

	findings, tests := analyzedFile(t, "alias_test.go", `package fixture

import "testing"

type T = testing.T

func TestAliased(t *T) {
	_ = 1
}
`)
	require.Equal(t, 1, tests, "a test taking a local alias of the handle was not counted")
	assert.Len(t, findings, 1, "a vacuous test taking an alias was not reported")

	// An alias of a *different* handle is not the runner's signature: it calls
	// a `Test…` taking `*testing.T` and nothing else.
	_, tests = analyzedFile(t, "benchalias_test.go", `package fixture

import "testing"

type B = testing.B

func TestNotReally(t *B) {
	_ = 1
}
`)
	assert.Zero(t, tests, "a `Test…` taking an alias of testing.B was counted as a test")

	// And a defined type is a new type the runner will not accept, however it
	// is spelled.
	_, tests = analyzedFile(t, "defined_test.go", `package fixture

import "testing"

type Own testing.T

func TestDefined(t *Own) {
	_ = 1
}
`)
	assert.Zero(t, tests, "a defined type over the handle was counted as the handle")
}

// TestAShadowedNameIsNotThePackageItShadows keeps a local binding from
// silencing the check.
//
// `assert`, `require` and `panic` are ordinary Go names. Reading them by their
// usual meaning wherever they appear means a test can bind one to something
// harmless, call it, and pass a check that fails builds (Codex, #1126).
func TestAShadowedNameIsNotThePackageItShadows(t *testing.T) {
	t.Parallel()

	// Testify imported under an alias, then shadowed by a local of the same
	// name: `req.Load()` is an ordinary method call on a `loader`.
	findings, _ := analyzedFile(t, "shadowed_alias_test.go", `package fixture

import (
	"testing"

	req "github.com/stretchr/testify/require"
)

type loader struct{}

func (l loader) Load() int { return 1 }

var _ = req.NotZero

func TestShadowsTheAlias(t *testing.T) {
	req := loader{}
	_ = req.Load()
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a local value shadowing the testify alias silenced the check")

	// And the builtin, which returns normally once it is a local func.
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(analyzedFixture(t, `
func TestShadowsPanic(t *testing.T) {
	panic := func(any) {}
	panic("this returns")
}
`)), "a local func named panic was read as the builtin that fails a test")

	// A *parameter* binds the name too, and this one is the expensive shape:
	// the helper reads as asserting, and the propagation then marks every test
	// that calls it as asserting — so one un-shadowed parameter hides an
	// arbitrary number of vacuous tests, not just its own function.
	findings, _ = analyzedFile(t, "shadowed_param_test.go", `package fixture

import (
	"testing"

	req "github.com/stretchr/testify/require"
)

type loader struct{}

func (l loader) Load() int { return 1 }

var _ = req.NotZero

func helper(req loader) {
	_ = req.Load()
}

func TestThroughAShadowedParameter(t *testing.T) {
	helper(loader{})
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a helper whose parameter shadows the testify alias was read as asserting, so "+
			"every test calling it was too")

	// A *receiver* binds it too, which is the third place a signature does and
	// the one that took a second fixture to pin: the first called nothing
	// through the shadowed name, so it could not tell the two answers apart.
	// Here the method's body calls `require.Load()` — a method on its own
	// receiver, in a file where `require` is testify.
	findings, _ = analyzedFile(t, "shadowed_recv_test.go", `package fixture

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type loader struct{}

func (l loader) Load() int { return 1 }

var _ = require.NotZero

func (require loader) check() {
	_ = require.Load()
}

func TestThroughAShadowedReceiver(t *testing.T) {
	loader{}.check()
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a receiver shadowing the testify import was not counted as binding the name, so "+
			"a call on it read as an assertion")

	// And a *type* parameter, which is the fifth place a signature binds and
	// the one the first version of this fix was still short by: in a generic
	// helper `panic(x)` is a conversion to the type parameter, and it returns.
	//
	// Called without explicit instantiation on purpose. Written `convert[string]("x")`
	// the callee is an IndexExpr rather than an identifier, so the propagation
	// never follows it — and the fixture then reports the test unasserted
	// whether the fix is present or not, which is this tool's own subject
	// wearing its own clothes. The mutation is what said so.
	findings, _ = analyzedFile(t, "shadowed_typeparam_test.go", `package fixture

import "testing"

func convert[panic ~string](x string) {
	_ = panic(x)
}

func TestThroughAShadowedTypeParameter(t *testing.T) {
	convert("x")
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a type parameter named panic was not counted as binding the name, so a "+
			"conversion read as the builtin that fails a test")

	// And a *receiver's* type parameters, which is the sixth binding site and
	// the one that outlived two rounds of adding field lists — because it is
	// the one place a name is bound that is not a field list at all. It is
	// written in the receiver's type: `box[panic]`.
	findings, _ = analyzedFile(t, "shadowed_recv_typeparam_test.go", `package fixture

import "testing"

type box[T ~string] struct{}

func (b box[panic]) check(x string) {
	_ = panic(x)
}

func TestThroughAReceiverTypeParameter(t *testing.T) {
	b := box[string]{}
	b.check("x")
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a receiver's type parameter named panic was not counted as binding the name")

	// Several of them, and through a pointer receiver, which are the other two
	// spellings the syntax has for the same thing.
	findings, _ = analyzedFile(t, "shadowed_recv_typeparams_test.go", `package fixture

import "testing"

type pair[A ~string, B any] struct{}

func (p *pair[panic, U]) check(x string) {
	_ = panic(x)
}

func TestThroughAPointerReceiverTypeParameter(t *testing.T) {
	p := &pair[string, int]{}
	p.check("x")
}
`)
	assert.Equal(t, []string{string(CheckUnasserted)}, checksFound(findings),
		"a pointer receiver's first type parameter was not counted as binding the name")
}

// TestAnAliasReachesTheWholePackage is Go's scoping, which is not the file's.
//
// `type T = testing.T` in one file and `func TestCrossFile(t *T)` in another is
// ordinary Go, and `go test` runs it. Reading aliases only from the file that
// declares them left such a test neither counted nor reported (Codex, #1126).
func TestAnAliasReachesTheWholePackage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "alias_test.go"), []byte(`package fixture

import "testing"

type T = testing.T
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "use_test.go"), []byte(`package fixture

func TestCrossFile(t *T) {
	_ = 1
}
`), 0o600))

	findings, tests, err := Analyze(dir)
	require.NoError(t, err)
	assert.Equal(t, 1, tests, "a test using an alias declared in another file was not counted")
	assert.Len(t, findings, 1, "a vacuous test using an alias from another file was not reported")
}

// TestAGuardIsMatchedToTheExpressionItNames is why the key is the printed
// expression rather than the report's summary.
//
// [render] is for a person reading the report and is deliberately lossy: it
// drops index values and call arguments, so `corpora[0]` and `corpora[1]` are
// one string to it, and so are `Cases(optional)` and `Cases(required)`. Used as
// a guard key that let a skip about one collection settle a loop over another —
// which is the finding this whole guard was tightened for, one level down
// (Codex and Copilot, #1126).
func TestAGuardIsMatchedToTheExpressionItNames(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{string(CheckConditional)}, checksFound(analyzedFixture(t, `
func TestIndexedCorpora(t *testing.T) {
	corpora := conformance.Corpora()
	if len(corpora[0]) == 0 {
		t.Skip("the first is empty")
	}

	for _, one := range corpora[1] {
		require.NotZero(t, one)
	}
}
`)), "a guard on corpora[0] settled a loop over corpora[1]")

	assert.Equal(t, []string{string(CheckConditional)}, checksFound(analyzedFixture(t, `
func TestDifferentArguments(t *testing.T) {
	if len(conformance.Cases("optional")) == 0 {
		t.Skip("no optional cases")
	}

	for _, one := range conformance.Cases("required") {
		require.NotZero(t, one)
	}
}
`)), "a guard on Cases(\"optional\") settled a loop over Cases(\"required\")")

	// The same expression, written the same way, still settles it.
	assert.Empty(t, analyzedFixture(t, `
func TestSameIndex(t *testing.T) {
	corpora := conformance.Corpora()
	if len(corpora[1]) == 0 {
		t.Skip("empty")
	}

	for _, one := range corpora[1] {
		require.NotZero(t, one)
	}
}
`), "a guard naming the loop's own subject exactly did not settle it")
}

// TestTestdataIsNotWalked keeps fixtures for other tests out of the report.
//
// `testdata` is Go's own convention for source that is not the package's, and
// this repository has plenty — a Flowfile fixture is input to a test rather
// than a test, and a Go file there is the same.
func TestTestdataIsNotWalked(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	nested := filepath.Join(dir, "testdata")
	require.NoError(t, os.MkdirAll(nested, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(nested, "fixture_test.go"),
		[]byte("package fixture\n\nimport \"testing\"\n\nfunc TestNothing(t *testing.T) {}\n"), 0o600))

	findings, tests, err := Analyze(dir)
	require.NoError(t, err)
	assert.Empty(t, findings, "a test under testdata was judged")
	assert.Zero(t, tests, "a test under testdata was counted")
}

// TestAnotherAgentsWorktreeIsNotWalked keeps somebody else's unfinished
// checkout from deciding whether this one passes.
//
// `.worktrees/` and `.claude/` are where an isolated agent checkout
// materializes, and `.gitignore:46-53` keeps both out of the repository for the
// same reason they are kept out of this walk: each is a copy of this tree
// holding work in progress. A half-written test in one is not tracked, is not
// in CI, and must not fail the check here (Codex, #1125).
func TestAnotherAgentsWorktreeIsNotWalked(t *testing.T) {
	t.Parallel()

	const vacuous = "package other\n\nimport \"testing\"\n\nfunc TestHalfWritten(t *testing.T) {}\n"

	for _, scratch := range []string{
		filepath.Join(".worktrees", "lane-one", "pkg"),
		filepath.Join(".claude", "worktrees", "lane-two", "pkg"),
	} {
		dir := t.TempDir()
		nested := filepath.Join(dir, scratch)
		require.NoError(t, os.MkdirAll(nested, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(nested, "half_test.go"), []byte(vacuous), 0o600))

		// And one real test beside them, so a walk that reached nothing at all
		// would fail this too rather than look like a pass.
		require.NoError(t, os.WriteFile(filepath.Join(dir, "real_test.go"),
			[]byte("package fixture\n\nimport \"testing\"\n\nfunc TestReal(t *testing.T) {\n\tt.Fatal(\"no\")\n}\n"), 0o600))

		findings, tests, err := Analyze(dir)
		require.NoError(t, err)
		assert.Empty(t, findings,
			"a test inside %s decided the result for a checkout that does not contain it", scratch)
		assert.Equal(t, 1, tests,
			"the walk did not reach the checkout's own test, so a clean report means nothing")
	}
}

// TestTheRepositoryHasNoUnassertedTest is the claim that makes this a gate
// rather than a report.
//
// It is asserted here as well as by the command's exit status, so that a
// finding fails `go test ./tools/...` and not only the leg that runs the
// command — the two are different enough venues that somebody could add the
// first while only ever running the second.
func TestTheRepositoryHasNoUnassertedTest(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	findings, tests, err := Analyze(root)
	require.NoError(t, err)
	require.Greater(t, tests, 1000,
		"the walk found %d test functions, which is too few to have reached the tree — a "+
			"clean report over nothing is the failure this whole tool is about", tests)

	var unasserted []string
	for _, finding := range findings {
		if finding.Check == CheckUnasserted {
			unasserted = append(unasserted, finding.Pos+": "+finding.Test)
		}
	}

	assert.Empty(t, unasserted,
		"a test reaches no assertion, so it is green for a reason unrelated to the code. "+
			"Assert what it is for, or say why it proves nothing with `%s%s <reason>` on it:\n\n%s",
		marker, CheckUnasserted, strings.Join(unasserted, "\n"))
}

// TestTheReportSaysWhatToDo keeps the output usable by whoever it fails for.
func TestTheReportSaysWhatToDo(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	failed := writeReport(&out, []Finding{
		{Check: CheckUnasserted, Test: "TestNothing", Pos: "a_test.go:1:1"},
		{Check: CheckConditional, Test: "TestLoop", Pos: "b_test.go:2:2", Detail: "Cases()"},
	}, 2, ".", false)

	assert.True(t, failed, "a report holding an unasserted finding did not fail")

	printed := out.String()
	assert.Contains(t, printed, "TestNothing",
		"an unasserted site was counted and not located, so nobody can act on it")
	assert.Contains(t, printed, marker+string(CheckUnasserted),
		"the report does not say how to record a deliberate one")
	assert.NotContains(t, printed, "b_test.go",
		"conditional sites were listed without -sites, which buries the list that matters")

	var everything strings.Builder
	writeReport(&everything, []Finding{
		{Check: CheckConditional, Test: "TestLoop", Pos: "b_test.go:2:2", Detail: "Cases()"},
	}, 1, ".", true)
	assert.Contains(t, everything.String(), "b_test.go:2:2",
		"-sites did not list the conditional sites")
	assert.Contains(t, everything.String(), "Cases()",
		"the site does not name what to assert about")
}
