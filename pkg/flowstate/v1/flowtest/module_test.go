package flowtest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// moduleSource is a module with a function that calls another, a function that can
// fail at run time, a constrained string and a constrained int, and a record.
const moduleSource = `edition: v2026.4
name: lib
types:
  Slug:
    type: string
    must: isSlug(this)
    example: checkout-api
  Count:
    type: int
    must: this >= 1
    example: 3
  Pair:
    fields:
      left:
        type: string
functions:
  isSlug:
    params:
      text: string
    returns: bool
    body: ${text.matches("^[a-z0-9]+(-[a-z0-9]+)*$")}
  both:
    params:
      a: string
      b: string
    returns: bool
    body: ${isSlug(a) && isSlug(b)}
  at:
    params:
      items: list(int)
      index: int
    returns: int
    body: ${items[index]}
errors:
  NotFound:
    description: The customer does not exist.
`

// runModuleSuite writes a module and a suite over it and runs the suite.
func runModuleSuite(t *testing.T, tests string) *v1.TestReport {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/lib.yaml", moduleSource)

	return flowtest.RunFile(writeInline(t, dir, "defaults:\n  workflow: ./lib.yaml\ntests:\n"+tests))
}

func onlyCase(t *testing.T, report *v1.TestReport) *v1.TestCase {
	t.Helper()

	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	return report.GetCases()[0]
}

func moduleFailures(c *v1.TestCase) string {
	var lines []string
	for _, f := range c.GetFailures() {
		lines = append(lines, f.GetField()+": "+f.GetMessage())
	}

	return strings.Join(lines, "\n")
}

// TestAModuleCaseCallsTheModulesFunctions: a claim names the module's functions by
// their declared names, including one that calls another, and holds.
func TestAModuleCaseCallsTheModulesFunctions(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: functions answer
    expect:
      check:
        - isSlug("checkout-api")
        - '!isSlug("Checkout API")'
        - both("a", "b")
        - '!both("a", "B")'
        - at([4, 5, 6], 1) == 5
`))

	assert.True(t, c.GetPassed(), "%s %s", c.GetError(), moduleFailures(c))
	assert.Empty(t, c.GetError())
}

// TestAModuleCaseFailsWithTheValueTheFunctionAnswered: a claim that does not hold
// fails the case and names both the claim and what the function actually answered.
func TestAModuleCaseFailsWithTheValueTheFunctionAnswered(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: wrong expectation
    expect:
      check:
        - that: isSlug("ok") == false
          because: ok is a slug
`))

	assert.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 1)
	failure := c.GetFailures()[0]
	assert.Equal(t, "expect.check[0]", failure.GetField())
	assert.Contains(t, failure.GetMessage(), `check failed: isSlug("ok") == false`)
	assert.Contains(t, failure.GetMessage(), "because: ok is a slug")
	assert.Contains(t, failure.GetMessage(), `isSlug("ok") = true`)
	assert.NotZero(t, failure.GetLine(), "a failure is placed in the suite file")
}

// TestAModuleCaseReportsAFunctionThatErrors: a function whose body errors for its
// argument fails the case as an errored check, not as a false one.
func TestAModuleCaseReportsAFunctionThatErrors(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: out of range
    expect:
      check:
        - at([1], 5) == 0
`))

	assert.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 1)
	assert.Contains(t, c.GetFailures()[0].GetMessage(), "check errored: at([1], 5) == 0")
}

// TestAModuleCaseRefusesACallToAnUndeclaredFunction: a name the module does not
// declare is not evaluated as something else; the case fails and says so.
func TestAModuleCaseRefusesACallToAnUndeclaredFunction(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: typo
    expect:
      check:
        - isSlugg("a")
`))

	assert.False(t, c.GetPassed())
	assert.Contains(t, moduleFailures(c), "check errored: isSlugg")
}

// TestAModuleCaseTypeClaimsAdmitAndRefuse covers both directions for a string
// and an int type, including a value of the wrong kind for the base.
func TestAModuleCaseTypeClaimsAdmitAndRefuse(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: types
    expect:
      types:
        Slug:
          admits: [checkout-api, a1]
          refuses: [Checkout, "", 7]
        Count:
          admits: [1, 40]
          refuses: [0, -1, one]
`))

	assert.True(t, c.GetPassed(), "%s %s", c.GetError(), moduleFailures(c))
}

// TestAModuleCaseTypeClaimsFailInBothDirections: a type that refuses what the case
// says it admits, and one that admits what the case says it refuses, each fail with
// the value named.
func TestAModuleCaseTypeClaimsFailInBothDirections(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: wrong both ways
    expect:
      types:
        Slug:
          admits: [Not-A-Slug]
          refuses: [fine]
`))

	assert.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 2)
	admit, refuse := c.GetFailures()[0], c.GetFailures()[1]
	assert.Equal(t, "expect.types.Slug.admits", admit.GetField())
	assert.Contains(t, admit.GetMessage(), `type Slug must admit string "Not-A-Slug", but refused it`)
	assert.Contains(t, admit.GetMessage(), `input "Slug" must satisfy`)
	assert.Equal(t, "expect.types.Slug.refuses", refuse.GetField())
	assert.Contains(t, refuse.GetMessage(), `type Slug must refuse string "fine", but admitted it`)
	assert.NotZero(t, admit.GetLine())
}

// TestAModuleCaseNamesTheTypesItDeclares: an unknown type is a failure that lists
// what the module has, and a record is refused as untestable here rather than
// admitting everything.
func TestAModuleCaseNamesTheTypesItDeclares(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: unknown
    expect:
      types:
        Slugg:
          admits: [a]
`))
	assert.False(t, c.GetPassed())
	assert.Contains(t, moduleFailures(c), `the module declares no type "Slugg"; it declares Count, Pair, Slug`)

	c = onlyCase(t, runModuleSuite(t, `
  - name: record
    expect:
      types:
        Pair:
          admits: [a]
`))
	assert.False(t, c.GetPassed())
	assert.Contains(t, moduleFailures(c), `type "Pair" is a record`)
}

// TestAModuleCaseRefusesWhatHasNoMeaningWithoutARun: inputs, stubs and the run
// expectations name things a module does not have, so a case stating them errors
// instead of passing vacuously.
func TestAModuleCaseRefusesWhatHasNoMeaningWithoutARun(t *testing.T) {
	t.Parallel()

	c := onlyCase(t, runModuleSuite(t, `
  - name: exercises a module
    inputs: {a: 1}
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [x]
      check:
        - isSlug("a")
`))

	assert.False(t, c.GetPassed(), "a module has nothing to run, so no case against it may pass")
	assert.Contains(t, c.GetError(), "the workflow is a module, which has no steps to run")
	assert.Contains(t, c.GetError(), "expect.ran, inputs, stubs")
}

// TestAModuleCaseBoundsTheValuesItPuts: the work one case asks for is bounded at
// load, and a table entry's rows share the bound through the effective case.
func TestAModuleCaseBoundsTheValuesItPuts(t *testing.T) {
	t.Parallel()

	values := make([]string, flowtest.MaxModuleValuesPerTest+1)
	for i := range values {
		values[i] = fmt.Sprint(i)
	}
	dir := t.TempDir()
	writeFile(t, dir+"/lib.yaml", moduleSource)
	report := flowtest.RunFile(writeInline(t, dir, fmt.Sprintf(`
tests:
  - name: too many
    workflow: ./lib.yaml
    expect:
      types:
        Count:
          admits: [%s]
`, strings.Join(values, ", "))))

	assert.Contains(t, report.GetRefused(), "puts 201 values to its types, more than the limit of 200")
}

// TestAModuleCaseCapsItsFailures: a table of values that all fail reports a
// bounded number of them and says how many it left out.
func TestAModuleCaseCapsItsFailures(t *testing.T) {
	t.Parallel()

	values := make([]string, 40)
	for i := range values {
		values[i] = fmt.Sprintf("Bad%d", i)
	}
	c := onlyCase(t, runModuleSuite(t, fmt.Sprintf(`
  - name: all wrong
    expect:
      types:
        Slug:
          admits: [%s]
`, strings.Join(values, ", "))))

	assert.False(t, c.GetPassed())
	require.Len(t, c.GetFailures(), 17)
	assert.Contains(t, c.GetFailures()[16].GetMessage(), "(and 24 more failures)")
}

// TestAModuleSuiteReportsEachCaseByItsOwnName: each case is reported under its own
// field every consumer reads, so a failing module case is a failing report.
func TestAModuleSuiteReportsEachCaseByItsOwnName(t *testing.T) {
	t.Parallel()

	report := runModuleSuite(t, `
  - name: first
    expect:
      check: ['isSlug("a")']
  - name: second
    expect:
      check: ['!isSlug("a")']
  - name: third
    skip: not yet
    expect:
      check: ['!isSlug("a")']
`)

	require.Len(t, report.GetCases(), 2)
	assert.Equal(t, "first", report.GetCases()[0].GetName())
	assert.True(t, report.GetCases()[0].GetPassed())
	assert.Equal(t, "second", report.GetCases()[1].GetName())
	assert.False(t, report.GetCases()[1].GetPassed())
	require.Len(t, report.GetSkipped(), 1)
	assert.Empty(t, report.GetCoverage(), "a module has no steps to account for")
}

// TestExpectTypesAgainstAWorkflowIsNotSilentlyPassed: `expect.types` against a
// workflow is a real claim the run cannot honor, so it errors rather than passing.
func TestExpectTypesAgainstAWorkflowIsNotSilentlyPassed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: w
steps:
  - id: a
    value: 1
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: types on a workflow
    workflow: ./workflow.yaml
    expect:
      types:
        Slug:
          admits: [a]
`))

	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.False(t, c.GetPassed())
	assert.NotEmpty(t, c.GetError()+moduleFailures(c))
}

// TestAnEmptyTypeClaimIsNoClaim: naming a type with no values, or with empty
// lists, asserts nothing, so a case made only of that is refused at load rather
// than passing.
func TestAnEmptyTypeClaimIsNoClaim(t *testing.T) {
	t.Parallel()

	for name, expect := range map[string]string{
		"no values":   "types: {Count: {}}",
		"empty lists": "types: {Count: {admits: [], refuses: []}}",
		"empty check": "check: []\n      types: {Count: {}}",
	} {
		dir := t.TempDir()
		writeFile(t, dir+"/lib.yaml", moduleSource)
		report := flowtest.RunFile(writeInline(t, dir, "tests:\n  - name: vacuous\n    workflow: ./lib.yaml\n    expect:\n      "+expect+"\n"))

		assert.Contains(t, report.GetRefused(), "claims nothing", name)
		assert.Empty(t, report.GetCases(), name)
	}
}
