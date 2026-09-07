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
	require.LessOrEqual(t, len(got), maxSuiteWarnings)
	for _, warning := range got[:len(got)-1] {
		assert.LessOrEqual(t, len(warning.GetMessage()), maxWarningMessageBytes)
	}
	assert.Contains(t, got[len(got)-1].GetMessage(), "additional warning(s) omitted")
	retainedBytes := 0
	for _, warning := range got {
		retainedBytes += len(warning.GetMessage())
	}
	assert.LessOrEqual(t, retainedBytes, maxSuiteWarningBytes)
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
	assert.LessOrEqual(t, cap(got), maxSuiteWarnings,
		"the retained backing array is sized by the omitted warnings, not by the budget")

	// A later case arrives with the budget already spent: it retains nothing at
	// all, and the marker keeps counting for the whole suite.
	before := b.marker.GetMessage()
	assert.Nil(t, b.take(flood), "a case that keeps no warning must retain no slice either")
	assert.NotEqual(t, before, b.marker.GetMessage(), "the omitted count must keep rising")
	assert.LessOrEqual(t, len(b.marker.GetMessage()), maxWarningMarkerBytes,
		"the marker must stay inside the reservation newSuiteWarningBudget held back for it")
}

// TestSuiteWarningBudgetMarkerIsPlacedInTheFile: the marker is a diagnostic
// like any other, so an editor must be able to underline it. runSuite budgets
// before it places, which is the only reason the marker has a code and a line;
// placing first would leave the substitute unplaced at line 0.
func TestSuiteWarningBudgetMarkerIsPlacedInTheFile(t *testing.T) {
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

	// Each case earns one unbounded-source warning: a where: clause that
	// matches nothing is quoted back, which is what the per-message bound cuts
	// to 4 KiB. Enough cases to spend the 64 KiB suite budget several times
	// over.
	var source strings.Builder
	source.WriteString("tests:\n")
	filler := strings.Repeat("x", maxWarningMessageBytes)
	for i := range 32 {
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

	var marker *v1.Diagnostic
	retainedBytes, retained := 0, 0
	for _, c := range report.GetCases() {
		for _, w := range c.GetWarnings() {
			retained++
			retainedBytes += len(w.GetMessage())
			assert.LessOrEqual(t, len(w.GetMessage()), maxWarningMessageBytes)
			if strings.Contains(w.GetMessage(), "additional warning(s) omitted") {
				marker = w
			}
		}
	}
	require.NotNil(t, marker, "32 source-sized warnings must outrun the suite budget")
	assert.LessOrEqual(t, retained, maxSuiteWarnings)
	assert.LessOrEqual(t, retainedBytes, maxSuiteWarningBytes)

	assert.Equal(t, "stubs", marker.GetField(), "the marker stands in for the warnings it replaced")
	assert.NotEmpty(t, marker.GetCode(), "an unplaced marker carries no code")
	assert.NotZero(t, marker.GetLine(), "an unplaced marker sits at line 0 and no editor can show it")
}
