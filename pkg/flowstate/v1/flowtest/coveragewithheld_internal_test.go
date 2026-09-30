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
		Stale: []string{`coverage.allow_unreached names "d_secret", but a case reached it; remove the entry`},
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
	assert.Equal(t, []string{"[redacted]", "plain"}, cov.Reached)
	assert.Equal(t, []string{"[redacted]#2", "[redacted]#3", "route"}, cov.Unreached)
	assert.Equal(t, map[string]string{"[redacted]#2": "[redacted] is only reachable in production"}, cov.Accepted)
	assert.Equal(t, []string{"[redacted]#3", "route"}, cov.Gaps(), "the accepted step must stay accepted, and the other must stay a gap")
	assert.Equal(t, []string{`coverage.allow_unreached names "[redacted]", but a case reached it; remove the entry`}, cov.Stale)

	require.Len(t, cov.Arms, 3)
	assert.Equal(t, "route:case[0]", cov.Arms[0].Key, "an arm's key spells its step, which spells nothing withheld here")
	assert.Equal(t, "case [redacted]", cov.Arms[0].Label)
	assert.Equal(t, "route:case[1]", cov.Arms[1].Key)
	assert.Equal(t, `case "plain"`, cov.Arms[1].Label, "a label that spells nothing withheld was withheld")
	assert.Equal(t, "[redacted]#3:default", cov.Arms[2].Key)
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
	assert.Equal(t, []string{"[redacted]", "[redacted]#4"}, cov.Reached)
	assert.Equal(t, []string{"[redacted]#2", "[redacted]#3", "[redacted]#5"}, cov.Unreached)
	assert.Len(t, cov.Gaps(), 2)
	assert.Equal(t, "case [redacted]", cov.Arms[1].Label)
	for _, arm := range cov.Arms {
		assert.NotContains(t, arm.Key, "route")
	}
	for _, reason := range cov.Accepted {
		assert.Equal(t, sensitiveMarker, reason)
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
