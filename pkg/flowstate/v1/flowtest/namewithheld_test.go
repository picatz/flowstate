package flowtest_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The names a `flow test` report prints are the file's own words, and an author
// can spell a value a case withholds as readily as anywhere else (#2229). These
// tests spell the value one sensitive input carries as a step id, a `switch:`
// arm label, a stub's target and the case's own name.

// nameSecret is the sensitive input's value, and every name below spells it.
const nameSecret = "hunter2_stepid"

const nameWorkflow = `edition: v2026.3
name: names
inputs:
  token:
    type: string
    required: true
    sensitive: true
  action:
    type: string
    required: true
steps:
  - id: hunter2_stepid
    if: ${inputs.action == "run"}
    log:
      message: guarded
  - id: route
    switch:
      value: ${inputs.action}
      cases:
        - case: hunter2_stepid
          steps: []
        - case: public_arm
          steps: []
      default:
        steps: []
outputs: {}
`

// nameSuite is a suite of the cases given, in a file that also holds a plain
// workflow with none of the secret's spellings, and returns its path.
func nameSuite(t *testing.T, cases string) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", nameWorkflow)
	path := dir + "/names.test.yaml"
	writeFile(t, path, "edition: v2026.3\ntests:"+cases)

	return path
}

// nameCase is a case whose name, expectation and stub each spell nameSecret.
const nameCase = `
  - name: case hunter2_stepid
    workflow: ./workflow.yaml
    inputs:
      token: hunter2_stepid
      action: skip
    stubs:
      - task: log
        returns: {}
      - step: hunter2_stepid
        returns: {}
    expect:
      ran: [hunter2_stepid]
`

// TestACaseReportWithholdsANameThatSpellsAWithheldValue: the case's failure,
// its warnings and its name print the file's names as written, and a name that
// spells a value the case withholds is withheld in each, in the report and in
// its `-o json` rendering, while the field path and code the diagnostic is
// derived from are kept.
func TestACaseReportWithholdsANameThatSpellsAWithheldValue(t *testing.T) {
	t.Parallel()

	run := flowtest.RunPath(t.Context(), nameSuite(t, nameCase), flowtest.RunOptions{})
	report := run.Report
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.NotEmpty(t, c.GetFailures(), "no failure was reported, so this proves nothing")

	rendered, err := protojson.Marshal(report)
	require.NoError(t, err)
	assert.NotContains(t, string(rendered), nameSecret)
	assert.Contains(t, c.GetName(), v1.SensitiveMarker, "the case's own name was not withheld")

	failure := c.GetFailures()[0]
	assert.Equal(t, "expect.ran", failure.GetField(), "the field path is the harness's word and is kept")
	assert.Equal(t, v1.SensitiveMarker, failure.GetStep())
	assert.Contains(t, failure.GetMessage(), v1.SensitiveMarker)

	// The stub's target is quoted by the warning that says it was never
	// consulted, beside a task's name, which spells nothing withheld.
	require.Len(t, c.GetWarnings(), 2)
	assert.Contains(t, c.GetWarnings()[0].GetMessage(), `task "log"`)
	assert.Contains(t, c.GetWarnings()[1].GetMessage(), `step "`+v1.SensitiveMarker+`"`)
	for _, warning := range c.GetWarnings() {
		assert.Equal(t, "stubs", warning.GetField())
		assert.Equal(t, "stub-unmatched", warning.GetCode())
	}
}

// TestACaseThatWithholdsNothingPrintsItsNamesAsWritten is the other direction: a
// name is withheld because the case withholds the value it spells, never
// because it looks like one.
func TestACaseThatWithholdsNothingPrintsItsNamesAsWritten(t *testing.T) {
	t.Parallel()

	suite := nameSuite(t, `
  - name: case hunter2_stepid
    workflow: ./workflow.yaml
    inputs:
      token: something-else-entirely
      action: skip
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hunter2_stepid]
`)
	run := flowtest.RunPath(t.Context(), suite, flowtest.RunOptions{})
	require.Len(t, run.Report.GetCases(), 1)

	c := run.Report.GetCases()[0]
	require.NotEmpty(t, c.GetFailures())
	assert.Equal(t, "case hunter2_stepid", c.GetName())
	assert.Equal(t, "hunter2_stepid", c.GetFailures()[0].GetStep())
}

// TestCoverageWithholdsANameThatAnyCaseWithholds: coverage is one report for the
// file, so a step id or arm label that spells a value any case withholds is
// withheld, in the lists and in `-o json`, and is still counted and still a gap.
func TestCoverageWithholdsANameThatAnyCaseWithholds(t *testing.T) {
	t.Parallel()

	// The second case withholds nothing; the first is the one whose posture
	// holds the secret, and coverage is withheld under the file's.
	suite := nameSuite(t, nameCase+`
  - name: a plain case
    workflow: ./workflow.yaml
    inputs:
      token: something-else-entirely
      action: other
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
`)
	run := flowtest.RunPath(t.Context(), suite, flowtest.RunOptions{})
	require.Len(t, run.Coverage, 1)
	cov := run.Coverage[0]

	rendered, err := protojson.Marshal(run.Report)
	require.NoError(t, err)
	assert.NotContains(t, string(rendered), nameSecret)
	report, err := json.Marshal(cov.Report())
	require.NoError(t, err)
	assert.NotContains(t, string(report), nameSecret)

	// Still counted: the guarded step is one of the two steps the workflow
	// has, and it is a gap.
	assert.Equal(t, 2, cov.Total())
	assert.Equal(t, []string{v1.SensitiveMarker}, cov.Unreached)
	assert.Equal(t, []string{v1.SensitiveMarker}, cov.Gaps())
	assert.Equal(t, []string{"route"}, cov.Reached)

	labels := map[string]bool{}
	for _, arm := range cov.Arms {
		labels[arm.Label] = arm.Reached
		assert.NotContains(t, arm.Key, nameSecret)
	}
	assert.Contains(t, labels, "case "+v1.SensitiveMarker, "the arm was not withheld: %v", labels)
	assert.Contains(t, labels, `case "public_arm"`, "an arm that spells nothing withheld was withheld: %v", labels)
	assert.False(t, labels["case "+v1.SensitiveMarker], "still a gap")
	assert.Len(t, cov.ArmGaps(), 2, "withholding an arm must not close its gap")
}

// TestCoverageOfAFileWhoseCasesWithholdNothingIsAsWritten is coverage's other
// direction: with nothing withheld in any case, the names are as written.
func TestCoverageOfAFileWhoseCasesWithholdNothingIsAsWritten(t *testing.T) {
	t.Parallel()

	suite := nameSuite(t, `
  - name: a plain case
    workflow: ./workflow.yaml
    inputs:
      token: something-else-entirely
      action: other
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
`)
	run := flowtest.RunPath(t.Context(), suite, flowtest.RunOptions{})
	require.Len(t, run.Coverage, 1)

	assert.Equal(t, []string{"hunter2_stepid"}, run.Coverage[0].Unreached)
}
