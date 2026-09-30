package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// coverageOf is a coverage report whose names spell two withheld values and one
// that spells nothing, each in every place a report prints a name.
func coverageOf() *Coverage {
	return &Coverage{
		Workflow:  "workflow.yaml",
		Reached:   []string{"a_secret", "plain"},
		Unreached: []string{"b_secret", "c_secret", "route"},
		Accepted: map[string]string{
			"b_secret": "b_secret is only reachable in production",
		},
		Stale:        []string{staleEntry{name: "d_secret", known: true}.message("d_secret")},
		staleEntries: []staleEntry{{name: "d_secret", known: true}},
		// Declared in an order that is not alphabetical, which is the order
		// the withheld names are numbered in.
		declared: []string{"route", "c_secret", "b_secret", "a_secret", "plain"},
		Arms: []*SwitchArm{
			{Key: "route:case[0]", Step: "route", Label: `case "a_secret"`, literal: "a_secret", decl: "0:route:case[0]"},
			{Key: "route:case[1]", Step: "route", Label: `case "plain"`, literal: "plain", decl: "0:route:case[1]"},
			{Key: "c_secret:default", Step: "c_secret", Label: "default", Reason: "for c_secret", decl: "1:c_secret:default"},
		},
	}
}

// TestACoverageReportWithholdsEveryNameThatSpellsAWithheldValue: each place a
// coverage report prints a name is withheld, the names that withhold alike are
// told apart in the order they were written, and the report still says what it
// did — the same steps counted, the same gap, the recorded reason kept.
func TestACoverageReportWithholdsEveryNameThatSpellsAWithheldValue(t *testing.T) {
	t.Parallel()

	cov := coverageOf()
	total := cov.Total()
	cov.withheldUnder(v1.SensitiveValues{}.WithValues("a_secret", "b_secret", "c_secret", "d_secret"))

	assert.Equal(t, total, cov.Total(), "withholding a name must not change what is counted")
	// Numbered as the workflow declares them: c_secret first, whatever sorts
	// first, so the numbers say nothing of the values they stand for.
	assert.Equal(t, []string{"[redacted]#3", "plain"}, cov.Reached)
	assert.Equal(t, []string{"[redacted]", "[redacted]#2", "route"}, cov.Unreached)
	assert.Equal(t, map[string]string{"[redacted]#2": "[redacted] is only reachable in production"}, cov.Accepted)
	assert.Equal(t, []string{"[redacted]", "route"}, cov.Gaps(), "the accepted step must stay accepted, and the other must stay a gap")
	assert.Equal(t, []string{`coverage.allow_unreached names "[redacted]", but a case reached it; remove the entry`}, cov.Stale)

	require.Len(t, cov.Arms, 3)
	assert.Equal(t, "route:case[0]", cov.Arms[0].Key, "an arm's key spells its step, which spells nothing withheld here")
	assert.Equal(t, "case [redacted]", cov.Arms[0].Label)
	assert.Equal(t, "route:case[1]", cov.Arms[1].Key)
	assert.Equal(t, `case "plain"`, cov.Arms[1].Label, "a label that spells nothing withheld was withheld")
	assert.Equal(t, "[redacted]:default", cov.Arms[2].Key)
	assert.Equal(t, "for [redacted]", cov.Arms[2].Reason)
	assert.Len(t, cov.ArmGaps(), 2)
}

// TestACoverageReportUnderAPostureThatWithholdsEverythingWithholdsEveryName:
// where the posture spanning the file could not be enumerated, no name can be
// told from a value, so none is printed; each is still counted and told apart.
func TestACoverageReportUnderAPostureThatWithholdsEverythingWithholdsEveryName(t *testing.T) {
	t.Parallel()

	cov := coverageOf()
	total := cov.Total()
	cov.withheldUnder(v1.WithheldSensitiveValues())

	assert.Equal(t, total, cov.Total())
	// Numbered in the order the names were written, across every list.
	assert.Equal(t, []string{"[redacted]#4", "[redacted]#5"}, cov.Reached)
	assert.Equal(t, []string{"[redacted]", "[redacted]#2", "[redacted]#3"}, cov.Unreached)
	assert.Len(t, cov.Gaps(), 2)
	assert.Equal(t, "case [redacted]", cov.Arms[1].Label)
	for _, arm := range cov.Arms {
		assert.NotContains(t, arm.Key, "route")
	}
	for _, reason := range cov.Accepted {
		assert.Equal(t, "[withheld]", reason, "a reason is prose, withheld whole as the run's own prose is")
	}
}

// TestACoverageReportWithholdsNothingWhenNothingIsWithheld: no case withholds
// anything, so the report is exactly what the accumulator built.
func TestACoverageReportWithholdsNothingWhenNothingIsWithheld(t *testing.T) {
	t.Parallel()

	cov := coverageOf()
	cov.withheldUnder(v1.SensitiveValues{})

	assert.Equal(t, coverageOf(), cov)
}

// TestACoverageReportWithholdsAShortValueWhereItIsTheWholeText: a value too
// short to match inside other text is still withheld where it is the whole of a
// name, a reason or a stale entry — the places a report quotes what the file
// wrote, not a sentence built around it.
func TestACoverageReportWithholdsAShortValueWhereItIsTheWholeText(t *testing.T) {
	t.Parallel()

	cov := &Coverage{
		Workflow:     "workflow.yaml",
		Unreached:    []string{"x"},
		Accepted:     map[string]string{"x": "x"},
		Stale:        []string{staleEntry{name: "x", known: false}.message("x")},
		staleEntries: []staleEntry{{name: "x", known: false}},
		declared:     []string{"x"},
	}
	cov.withheldUnder(v1.SensitiveValues{}.WithValues("x"))

	assert.Equal(t, []string{"[redacted]"}, cov.Unreached)
	assert.Equal(t, map[string]string{"[redacted]": "[redacted]"}, cov.Accepted)
	require.Len(t, cov.Stale, 1)
	assert.Contains(t, cov.Stale[0], `names "[redacted]", which is not a step`)
}

// TestStepsAreDeclaredInTheOrderTheWorkflowWritesThem: the order withheld
// names are numbered in is declaration order, by structural path taken
// numerically at each level, so the tenth step follows the ninth rather than the
// first, and a nested step follows the step that holds it.
func TestStepsAreDeclaredInTheOrderTheWorkflowWritesThem(t *testing.T) {
	t.Parallel()

	wc := &workflowCoverage{steps: map[string]string{
		"/10":   "tenth",
		"/2":    "third",
		"/0":    "first",
		"/2/0":  "inside_third",
		"/1":    "second",
		"/2/10": "late_inside_third",
	}}

	assert.Equal(t, []string{"first", "second", "third", "inside_third", "late_inside_third", "tenth"}, wc.declaredIDs())
}

// TestACoverageReportWithholdsAValueInsideAStepId: a step id that carries a
// withheld value inside a longer name is withheld at that place, not only where
// the whole id is the value.
func TestACoverageReportWithholdsAValueInsideAStepId(t *testing.T) {
	t.Parallel()

	cov := &Coverage{
		Workflow:  "workflow.yaml",
		Reached:   []string{"deploy_hunter2_stepid"},
		Unreached: []string{"plain"},
		declared:  []string{"deploy_hunter2_stepid", "plain"},
	}
	cov.withheldUnder(v1.SensitiveValues{}.WithValues("hunter2_stepid"))

	assert.Equal(t, []string{"deploy_[redacted]"}, cov.Reached)
	assert.Equal(t, []string{"plain"}, cov.Unreached)
}
