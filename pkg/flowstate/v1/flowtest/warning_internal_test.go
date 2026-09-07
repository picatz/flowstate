package flowtest

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestSuiteWarningBudgetBoundsRetainedDiagnostics(t *testing.T) {
	b := newSuiteWarningBudget()
	input := make([]*v1.Diagnostic, maxSuiteWarnings+20)
	for i := range input {
		input[i] = &v1.Diagnostic{Field: "stubs", Message: strings.Repeat("x", maxWarningMessageBytes+1)}
	}

	got := b.take(input)
	require.LessOrEqual(t, len(got), maxSuiteWarnings+1, "the retained warnings, plus this case's one marker")
	for _, warning := range got[:len(got)-1] {
		assert.LessOrEqual(t, len(warning.GetMessage()), maxWarningMessageBytes)
	}
	assert.Contains(t, got[len(got)-1].GetMessage(), "warning(s) omitted")
	retainedBytes := 0
	for _, warning := range got[:len(got)-1] {
		retainedBytes += len(warning.GetMessage())
	}
	assert.LessOrEqual(t, retainedBytes, maxSuiteWarningBytes)
	assert.LessOrEqual(t, len(got[len(got)-1].GetMessage()), maxWarningMarkerBytes)
}

// TestSuiteWarningBudgetRetainsNothingSizedByWhatItDropped: the returned slice
// is what a TestCase holds for the life of the report, so a case that produced
// far more warnings than the suite can keep must not leave the report holding
// an array sized by the ones thrown away. Length is the visible bound; capacity
// is the retained one.
func TestSuiteWarningBudgetRetainsNothingSizedByWhatItDropped(t *testing.T) {
	b := newSuiteWarningBudget()
	flood := make([]*v1.Diagnostic, 100_000)
	for i := range flood {
		flood[i] = &v1.Diagnostic{Field: "stubs", Message: strings.Repeat("x", maxWarningMessageBytes)}
	}

	got := b.take(flood)
	require.NotEmpty(t, got)
	assert.LessOrEqual(t, cap(got), maxSuiteWarnings+1,
		"the retained backing array is sized by the omitted warnings, not by the budget")

	// A later case arrives with the budget already spent. It keeps no warning
	// of its own, but it does keep the one diagnostic saying so — a case that
	// warned must never come back empty.
	spent := b.take(flood)
	require.Len(t, spent, 1, "a case that warned must not be returned as a case that did not")
	assert.Contains(t, spent[0].GetMessage(), "warning(s) omitted")
	assert.LessOrEqual(t, cap(spent), maxSuiteWarnings+1)
}

// TestSuiteWarningBudgetLeavesEveryWarnedCaseWarned is the verdict half of the
// budget, and the reason the marker is per case rather than per suite. Both
// `--fail-on-warning` and the per-case PASS/FAIL line read
// `len(TestCase.Warnings)`, so a case whose warnings the budget dropped must
// still hold one: a memory bound that silently turns a warned case green is a
// wrong answer, not a smaller one (Codex, #1857).
//
// It also covers the placement fix: runSuite budgets before it places, which is
// the only reason the marker carries a code and a line rather than sitting at
// line 0 where no editor can show it.
func TestSuiteWarningBudgetLeavesEveryWarnedCaseWarned(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(dir+"/workflow.yaml", []byte(`
edition: v2026.3
name: ghost
steps:
  - id: greet
    log:
      message: hello
outputs: {}
`), 0o600))

	// Each case earns exactly one warning, and each is source-sized: a where:
	// clause that matched nothing is quoted back, which is what the per-message
	// bound cuts to 4 KiB. Enough cases to spend the 64 KiB suite budget twice
	// over, so the later ones are all dropped.
	const cases = 32
	var source strings.Builder
	source.WriteString("tests:\n")
	filler := strings.Repeat("x", maxWarningMessageBytes)
	for i := range cases {
		fmt.Fprintf(&source, `  - name: case %d
    workflow: ./workflow.yaml
    stubs:
      - task: log
        where: inputs.message == %q
        returns: {}
      - task: log
        returns: {}
    expect:
      ran: [greet]
`, i, filler)
	}
	path := dir + "/inline.test.yaml"
	require.NoError(t, os.WriteFile(path, []byte(source.String()), 0o600))

	report := RunFile(path)
	require.Empty(t, report.GetRefused(), "the file must load: %s", report.GetRefused())
	require.Len(t, report.GetCases(), cases)

	markers, retained, retainedBytes := 0, 0, 0
	for _, c := range report.GetCases() {
		require.NotEmpty(t, c.GetWarnings(),
			"case %q warned and must still say so, whatever the budget kept", c.GetName())

		for _, w := range c.GetWarnings() {
			assert.LessOrEqual(t, len(w.GetMessage()), maxWarningMessageBytes)
			assert.NotEmpty(t, w.GetCode(), "an unplaced warning carries no code")
			assert.NotZero(t, w.GetLine(), "an unplaced warning sits at line 0 and no editor can show it")

			if strings.Contains(w.GetMessage(), "warning(s) omitted") {
				markers++
				assert.LessOrEqual(t, len(w.GetMessage()), maxWarningMarkerBytes)
				continue
			}
			retained++
			retainedBytes += len(w.GetMessage())
		}
	}
	require.NotZero(t, markers, "32 source-sized warnings must outrun the suite budget")
	assert.LessOrEqual(t, retained, maxSuiteWarnings)
	assert.LessOrEqual(t, retainedBytes, maxSuiteWarningBytes)
}
