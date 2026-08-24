package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow test`'s switch-arm coverage at the surface an author meets it (issue
// #801): the summary line, the positioned diagnostic, and the verdict.

// armCoverageWorkflow is the documented shape the step universe cannot measure:
// two literals sharing one body, an arm whose body is deliberately empty, and a
// default.
const armCoverageWorkflow = `edition: v2026.3
name: routing
inputs:
  action:
    type: string
    required: true
steps:
  - id: on_event
    switch:
      value: ${inputs.action}
      cases:
        - case: [closed, merged]
          steps:
            - id: archive
              log:
                message: archiving
        - case: synchronize
          steps: []
      default:
        steps:
          - id: unhandled
            log:
              message: unhandled
`

// writeArmCoverageFixture writes the workflow and a suite driving the actions
// given, and returns the directory `flow test` is pointed at.
func writeArmCoverageFixture(t *testing.T, actions []string, extra string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(armCoverageWorkflow), 0o600))

	suite := "edition: v2026.3\ntests:"
	for _, action := range actions {
		suite += `
  - name: ` + action + `
    workflow: ./workflow.yaml
    inputs:
      action: ` + action + `
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
`
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(suite+extra), 0o600))

	return dir
}

// TestAnUnreachedArmFailsCoverageRequiredWithAPosition is issue #801's
// acceptance criterion, and both halves of it matter.
//
// The suite drives `closed` and an unenumerated value, so every *step* the
// workflow has runs: step coverage says 3/3 and, before this, `--coverage-required`
// passed. The `synchronize` arm — `steps: []`, the documented spelling of
// written-down ignoring — was taken by nothing, and `merged` was never
// distinguished from `closed`. Both are now failures, each named by the position
// it was written at, which is the only name an arm has.
func TestAnUnreachedArmFailsCoverageRequiredWithAPosition(t *testing.T) {
	dir := writeArmCoverageFixture(t, []string{"closed", "labeled"}, "")

	out, err := runFlowTest(t, "--coverage-required", dir)
	require.Error(t, err, "an arm no case took must fail --coverage-required")

	// The step account is clean, which is exactly why the arm account is needed.
	assert.Contains(t, out, "3/3 steps reached")
	assert.NotContains(t, out, "never ran:")

	assert.Contains(t, out, "2/4 switch arms taken")
	// A position, in the `flowfile/validate.go` form: the workflow file, the
	// line, the column.
	assert.Contains(t, out, filepath.Join(dir, "workflow.yaml")+":17:")
	assert.Contains(t, out, `case "synchronize" of switch "on_event" was taken by no test case`)
	assert.Contains(t, out, `case "merged" of switch "on_event" was taken by no test case`)
	// And the key to record, so nobody has to derive one from the docs.
	assert.Contains(t, out, "coverage.allow_unreached: on_event:case[1]")
}

// TestArmCoverageIsAResultWithoutTheFlag keeps the posture #420 set for step
// coverage: an unreached arm is a fact worth reading on every run, and a verdict
// only where the run opted in.
func TestArmCoverageIsAResultWithoutTheFlag(t *testing.T) {
	dir := writeArmCoverageFixture(t, []string{"closed", "labeled"}, "")

	out, err := runFlowTest(t, dir)
	require.NoError(t, err, "coverage is a result, not a failure, without --coverage-required")
	assert.Contains(t, out, "2/4 switch arms taken")
	assert.Contains(t, out, "was taken by no test case")
}

// TestARecordedArmIsAnAcceptedResidual is `coverage.allow_unreached` answering
// for an arm exactly as it answers for a step: a reason turns a hole into a
// decision, and the run passes.
func TestARecordedArmIsAnAcceptedResidual(t *testing.T) {
	dir := writeArmCoverageFixture(t,
		[]string{"closed", "merged", "labeled"},
		"\ncoverage:\n  allow_unreached:\n    on_event:case[1]: the provider only sends this in production; there is nothing to rehearse.\n")

	out, err := runFlowTest(t, "--coverage-required", dir)
	require.NoError(t, err, "a recorded arm must not fail --coverage-required")
	assert.Contains(t, out, "3/4 switch arms taken")
	assert.Contains(t, out, "accepted-unreached arms: on_event:case[1]")
	assert.NotContains(t, out, "was taken by no test case")
}

// TestAWorkflowWithNoSwitchPrintsTheLineItAlwaysDid pins that this adds nothing
// to a file it has nothing to say about. A second number reading "0/0 switch
// arms taken" on every workflow in the corpus would be noise that teaches
// readers to skip the line the rest of coverage lives on.
func TestAWorkflowWithNoSwitchPrintsTheLineItAlwaysDid(t *testing.T) {
	dir := writeCoverageFixture(t, "")

	out, err := runFlowTest(t, dir)
	require.NoError(t, err)
	assert.Contains(t, out, "1/2 steps reached")
	assert.NotContains(t, out, "switch arms taken")
}

// TestJSONCarriesTheArms is the machine half: the arms ride the report as a
// schema field, with the position, so CI annotates a pull request at the line
// rather than scraping the prose.
func TestJSONCarriesTheArms(t *testing.T) {
	dir := writeArmCoverageFixture(t, []string{"closed", "labeled"}, "")

	out, err := runFlowTest(t, "-o", "json", "--coverage-required", dir)
	require.Error(t, err, "the machine form reports the same verdict as the text one")

	var doc struct {
		Files []struct {
			Coverage []struct {
				Arms []struct {
					Arm     string `json:"arm"`
					Step    string `json:"step"`
					Label   string `json:"label"`
					Reached bool   `json:"reached"`
					Reason  string `json:"reason"`
					Line    int    `json:"line"`
					Column  int    `json:"column"`
				} `json:"arms"`
			} `json:"coverage"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &doc), "output was not the expected JSON shape:\n%s", out)
	require.Len(t, doc.Files, 1)
	require.Len(t, doc.Files[0].Coverage, 1)

	arms := doc.Files[0].Coverage[0].Arms
	require.Len(t, arms, 4, "two literals of the list case, the empty case, and the default")

	byKey := map[string]bool{}
	for _, arm := range arms {
		byKey[arm.Arm] = arm.Reached
		assert.Equal(t, "on_event", arm.Step)
		assert.Positive(t, arm.Line, "every arm of a workflow parsed from a file carries a position")
	}
	assert.Equal(t, map[string]bool{
		"on_event:case[0][0]": true,
		"on_event:case[0][1]": false,
		"on_event:case[1]":    false,
		"on_event:default":    true,
	}, byKey)
}

// TestArmDiagnosticsAreOneLinePerArm guards the shape rather than the content: a
// coverage report that folded five missed arms into one sentence would be a
// summary, and the reason positions were threaded through at all is that each
// arm is somewhere different.
func TestArmDiagnosticsAreOneLinePerArm(t *testing.T) {
	dir := writeArmCoverageFixture(t, []string{"labeled"}, "")

	out, _ := runFlowTest(t, "--coverage-required", dir)

	lines := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "was taken by no test case") {
			lines++
		}
	}
	assert.Equal(t, 3, lines, "closed, merged and synchronize are three arms and three lines")
}
