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

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fixture_test.go"),
		[]byte("package fixture\n\nimport (\n\t\"testing\"\n\n\t\"github.com/stretchr/testify/require\"\n)\n\nvar _ = require.NotEmpty\n\n"+source), 0o600))

	findings, _, err := Analyze(dir)
	require.NoError(t, err)

	return findings
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
		"the tree stands at 134 of these, and a number that large can only be a map")
}

// TestATableAReaderCanCountIsNotAFinding is the clause that keeps the ordinary
// table test out of the report.
//
// Without it the check found 253 sites and the ones that mattered were buried
// among them. A literal — inline, or assigned to a name a few lines up — is one
// whoever is reading the test can count.
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
