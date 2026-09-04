package flowtest_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A failure found while running a case is placed in the file that claimed it.
//
// Every one of these used to arrive as line 0, column 0 with an empty code, so
// `flow test -o json` handed an editor nothing to underline and a consumer
// nothing to group by (#1558). The loader had already computed where each key
// is; only the run-time half never asked.
//
// One case per failure class, because the classes are built in different
// functions and a single one passing would say nothing about the rest.

// positionWorkflow is a workflow with enough shape for every claim below to be
// about something real: a step that runs, a step that is skipped, and two
// outputs so a case can name one and miss the other.
const positionWorkflow = `edition: v2026.3
name: positions
steps:
  - id: first
    if: ${false}
    value: ${"skipped"}
  - id: after
    value: ${2}
outputs:
  total:
    value: ${steps.after.value}
  extra:
    value: ${"unnamed"}
`

func TestRunTimeFailuresArePlacedInTheTestFile(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		// suite is written verbatim, so the line numbers asserted below are the
		// ones a reader can count here.
		suite string
		field string
		code  v1.DiagnosticCode
		line  uint32
	}{
		{
			// The value the case named, underlined where it is written rather
			// than at the block that holds it.
			name: "an output whose value differs",
			suite: `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      outputs:
        total: 99
        extra: unnamed
`,
			field: "expect.outputs",
			code:  v1.DiagnosticCodeOutputMismatch,
			line:  7,
		},
		{
			// The case does not name this output, so there is no entry to
			// underline and the key it should be added to is the honest answer.
			name: "an output the case does not name",
			suite: `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      outputs:
        total: 2
`,
			field: "expect.outputs",
			code:  v1.DiagnosticCodeOutputMismatch,
			line:  6,
		},
		{
			name: "a step claimed to have run that did not",
			suite: `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      ran: [after, first]
      outputs:
        total: 2
        extra: unnamed
`,
			field: "expect.ran",
			code:  v1.DiagnosticCodeExpectationUnmet,
			line:  6,
		},
		{
			name: "an outcome the run did not reach",
			suite: `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      failed: true
      outputs:
        total: 2
        extra: unnamed
`,
			field: "expect.failed",
			code:  v1.DiagnosticCodeExpectationUnmet,
			line:  6,
		},
		{
			name: "a check that came back false",
			suite: `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      check:
        - ${1 == 2}
      outputs:
        total: 2
        extra: unnamed
`,
			field: "expect.check[0]",
			code:  v1.DiagnosticCodeExpectationUnmet,
			// The claim itself, on line 7, rather than the `check:` key above
			// it: one entry of a sequence has no key of its own, and the
			// expression is what the author would fix.
			line: 7,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, filepath.Join(dir, "workflow.yaml"), positionWorkflow)
			path := filepath.Join(dir, "workflow.test.yaml")
			writeFile(t, path, test.suite)

			report := flowtest.RunFile(path)
			require.Len(t, report.GetCases(), 1)

			failure := failureFor(t, report.GetCases()[0].GetFailures(), test.field)

			assert.Equal(t, test.line, failure.GetLine(),
				"a failure about %s must point at the line that claims it, not line 0", test.field)
			assert.NotZero(t, failure.GetColumn(),
				"a placed failure must carry a column as well as a line")
			assert.Equal(t, string(test.code), failure.GetCode(),
				"a consumer groups by code, so it must never be empty")
		})
	}
}

// TestAFailureAboutAKeyNobodyWroteCarriesNoPosition is the exact-or-nothing half.
//
// A run that fails when the case never wrote `expect.failed:` is still reported,
// and there is no such line to underline. Guessing one — the enclosing `expect:`,
// or the case — would send an author to correct text that is already right, which
// this package holds to be worse than an unplaced diagnostic.
func TestAFailureAboutAKeyNobodyWroteCarriesNoPosition(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `edition: v2026.3
name: fails
steps:
  - id: boom
    value: ${1 / 0}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      ran: [boom]
`)

	report := flowtest.RunFile(path)
	require.Len(t, report.GetCases(), 1)

	failure := failureFor(t, report.GetCases()[0].GetFailures(), "expect.failed")
	assert.Zero(t, failure.GetLine(),
		"a key the author never wrote must not be given a position")
	assert.Equal(t, string(v1.DiagnosticCodeExpectationUnmet), failure.GetCode(),
		"an unplaced failure still carries its code")
}

// failureFor returns the one failure about a field, so a case asserting on
// `expect.ran` is not quietly satisfied by a failure about something else.
func failureFor(t *testing.T, failures []*v1.Diagnostic, field string) *v1.Diagnostic {
	t.Helper()

	var found []*v1.Diagnostic
	for _, failure := range failures {
		if failure.GetField() == field {
			found = append(found, failure)
		}
	}

	require.NotEmpty(t, found, "no failure about %s; got %s", field, fieldsOf(failures))

	return found[0]
}

// fieldsOf names what a case did report, so a missing failure says what arrived
// instead of only what did not.
func fieldsOf(failures []*v1.Diagnostic) string {
	names := make([]string, 0, len(failures))
	for _, failure := range failures {
		names = append(names, failure.GetField()+": "+failure.GetMessage())
	}
	if len(names) == 0 {
		return "no failures at all"
	}

	return strings.Join(names, "; ")
}

// TestATableRowIsPlacedAtTheRowThatWroteIt is the case a flat `tests[i]` gets
// wrong.
//
// A table entry's rows expand into the flat list the runner walks, so the third
// effective case may have been written at `tests[0].cases[1]`. Deriving the path
// from the runner's index would underline a different case's text — a false
// position on a true finding, which is worse than none (Copilot, #1628).
func TestATableRowIsPlacedAtTheRowThatWroteIt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), positionWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `edition: v2026.3
tests:
  - name: table
    workflow: ./workflow.yaml
    cases:
      - name: first row
        expect:
          outputs:
            total: 2
            extra: unnamed
      - name: second row
        expect:
          outputs:
            total: 99
            extra: unnamed
`)

	report := flowtest.RunFile(path)
	require.Len(t, report.GetCases(), 2)

	// The first row is correct, so only the second reports anything.
	require.True(t, report.GetCases()[0].GetPassed(), "the first row should hold")

	failure := failureFor(t, report.GetCases()[1].GetFailures(), "expect.outputs")
	assert.Equal(t, uint32(14), failure.GetLine(),
		"the second row's wrong value is on line 14; anchoring by flat index would point into the first row")
}

// TestAnInheritedClaimIsNotPlacedOnTheCase is the arithmetic half of the same
// rule.
//
// `defaults:` prepends its check claims, so a case's own claims sit after them
// and a merged index is not an index into what the author wrote. Translating it
// is what keeps merged claim 0 — inherited — from underlining the case's own
// first claim, which is text that is already right.
func TestAnInheritedClaimIsNotPlacedOnTheCase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), positionWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `edition: v2026.3
defaults:
  check:
    - that: ${1 == 2}
tests:
  - name: c
    workflow: ./workflow.yaml
    expect:
      check:
        - that: ${true}
      outputs:
        total: 2
        extra: unnamed
`)

	report := flowtest.RunFile(path)
	require.Len(t, report.GetCases(), 1)

	// The inherited claim is the one that fails, and it is merged claim 0 while
	// the case's own `check[0]` is the claim on line 10 that holds.
	failure := failureFor(t, report.GetCases()[0].GetFailures(), "expect.check[0]")
	assert.Zero(t, failure.GetLine(),
		"an inherited claim must not borrow the case's own check[0] position")
}

// TestAWarningIsPlacedAndCodedToo covers the findings that are not failures.
//
// A task a case invoked with no stub to answer it is reported as a warning, and
// an editor has the same reason to underline it: the fix is a `stubs:` entry.
// Placing only failures left these at line 0 with an empty code, which is the
// same gap #1558 is about (Copilot, #1628).
func TestAWarningIsPlacedAndCodedToo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `edition: v2026.3
name: reaches
steps:
  - id: fetch
    http:
      url: https://example.invalid/
outputs:
  code:
    value: ${steps.fetch.status_code}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `edition: v2026.3
tests:
  - name: c
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: "inputs.url == 'https://other.invalid/'"
        returns:
          status_code: 200
    expect:
      outputs:
        code: 200
`)

	report := flowtest.RunFile(path)
	require.Len(t, report.GetCases(), 1)

	warning := failureFor(t, report.GetCases()[0].GetWarnings(), "stubs")
	assert.NotZero(t, warning.GetLine(),
		"a warning an editor should underline must carry a position")
	assert.Equal(t, string(v1.DiagnosticCodeStubUnmatched), warning.GetCode(),
		"the stub code must actually be reachable, or it is dead in the registry")
}
