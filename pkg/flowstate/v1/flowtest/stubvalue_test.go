package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestStubAimedAtAValueSaysWhatItIs pins the sentence a stub aimed at a `value:`
// step is answered with.
//
// [stepTasks] builds two maps: task steps by the task they run, and every other
// step by a word naming its kind. The second exists so that "you named a step
// that runs no task" is told apart from "you named nothing", because the fix
// differs completely: the first is a real id aimed at the wrong kind of step and
// the second is a typo.
//
// A kind missing from that walk falls into the second bucket, so a stub aimed at
// a value step was answered with "unknown step, which this workflow has no task
// step for": false about a step written three lines above it, and an instruction
// to go hunting for a misspelling that is not there.
//
// A value is also the kind most likely to be aimed at by mistake, which is why
// this is worth a test of its own rather than a line in a table. Forcing a
// computed predicate is exactly what a test author wants; the honest answer is
// that there is nothing to stub, because nothing is invoked. The expression is
// the value.
func TestStubAimedAtAValueSaysWhatItIs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `edition: v2026.2
name: stub-value
inputs:
  amount:
    type: int
    required: true
steps:
  - id: over_threshold
    value: ${inputs.amount >= 100}
  - id: announce
    log:
      message: decided
`)

	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a stub aimed at a value step
    workflow: ./workflow.yaml
    inputs: {amount: 500}
    stubs:
      - step: over_threshold
        returns: {value: false}
    expect:
      ran: [over_threshold, announce]
`))

	require.NotEmpty(t, report.Cases)

	// A stub that names nothing stubbable is refused before the run starts, so
	// it arrives as the case's own error rather than as an expectation failure.
	text := report.Cases[0].Error
	require.NotEmpty(t, text, "a stub naming a value step was accepted")

	require.Contains(t, text, `step "over_threshold"`,
		"the failure does not name the step the author wrote")
	require.Contains(t, text, "it is a value step",
		"a stub aimed at a value step is not told what kind of step it named")
	require.NotContains(t, text, "unknown step",
		"a step written in the file was reported as one the workflow does not have")
}
