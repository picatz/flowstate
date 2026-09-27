package flowdebug_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestTheDriverSpeaksThePromptsVocabularyToATarget drives a real local run
// through the command lines `flow debug attach` and the MCP sessions accept,
// which reach the target through nothing but its typed contract.
func TestTheDriverSpeaksThePromptsVocabularyToATarget(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)

	do := func(line string) *flowdebug.DriveResult {
		t.Helper()
		result, err := driver.Do(t.Context(), line)
		require.NoError(t, err, line)

		return result
	}

	assert.Contains(t, do("status").Text, "held at start")

	armed := do("break each/touch if item == 2")
	assert.Contains(t, armed.Text, "breakpoint at each/touch")
	assert.Contains(t, do("log touch saw {item}").Text, "breakpoint at touch")
	assert.Contains(t, do("break nowhere").Text, "not armed: nowhere")

	stop := do("continue")
	require.NotNil(t, stop.Snapshot)
	assert.Equal(t, "each[1]/touch", stop.Snapshot.GetOccurrence().GetAddress())
	assert.Contains(t, stop.Text, "breakpoint")
	assert.Equal(t, "2\n", do("inspect item").Text)
	assert.Contains(t, do("expand [1, [2, 3]]").Text, "list")
	assert.Contains(t, do("bt").Text, "iteration 1")
	assert.Contains(t, do("scope").Text, "vars: item")
	assert.Contains(t, do("breakpoints").Text, "each/touch  hits 1")

	out := do("finish")
	assert.Equal(t, "checks", out.Snapshot.GetOccurrence().GetAddress())

	detached := do("detach")
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, detached.Receipt.GetStatus())
	require.NoError(t, <-run.done)

	_, err := driver.Do(t.Context(), "frobnicate")
	require.Error(t, err)
}
