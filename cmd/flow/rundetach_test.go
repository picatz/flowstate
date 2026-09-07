package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `flow run --detach` (#1771): the start is the whole of the command. What a
// detached caller is owed is what a following one already receives before the
// first poll — the start line with the way back, or the run as it was started
// as a document — and nothing is asked of the server after that.

// neverFollowed is a fake whose Get fails the test: a detached run must not
// poll, and a poll that happened is the defect rather than a harmless extra.
func neverFollowed(t *testing.T) *fakeWorkflowService {
	t.Helper()
	fake := startedThenFinished()
	fake.onGet = func() { t.Error("a detached run polled the server after starting; --detach must return at the start") }
	return fake
}

func TestADetachedRunSaysTheWayBackAndReturnsAtTheStart(t *testing.T) {
	fake := neverFollowed(t)
	cmd, _, errOut := runCommandForTest(t, fake)
	cmd.Flags().Bool("detach", false, "")
	require.NoError(t, cmd.Flags().Set("detach", "true"))

	var out strings.Builder
	cmd.SetOut(&out)

	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}),
		"a start that succeeded is a detached run that succeeded")

	require.NotNil(t, fake.gotRun, "the run was never submitted")
	assert.Nil(t, fake.gotGet, "the server was polled after the start")
	assert.Contains(t, errOut.String(), "started workflow")
	assert.Contains(t, errOut.String(), "flow watch flowstate-request-3f7c",
		"the way back to the run is the one thing a detached caller has to be told")
	assert.Empty(t, out.String(), "the text shape owes stdout nothing: the run has produced no outputs yet")
}

func TestADetachedRunInJSONWritesTheRunAsStarted(t *testing.T) {
	for _, format := range []string{"json", "jsonl"} {
		t.Run(format, func(t *testing.T) {
			fake := neverFollowed(t)
			cmd, out, errOut := runCommandForTest(t, fake)
			cmd.Flags().Bool("detach", false, "")
			require.NoError(t, cmd.Flags().Set("detach", "true"))
			require.NoError(t, cmd.Flags().Set("output", format))

			require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))

			assert.Nil(t, fake.gotGet, "the server was polled after the start")

			// One document, and it is the run as started: the ids a caller
			// hands to `flow watch`, and a status that says it is going. The
			// jsonl shape is one line; the json shape is one indented document,
			// which is why the whole of stdout is decoded rather than a line.
			if format == "jsonl" {
				require.Len(t, strings.Split(strings.TrimSpace(out.String()), "\n"), 1,
					"a detached jsonl run writes exactly one event, the start:\n%s", out.String())
			}
			var document map[string]any
			require.NoError(t, json.Unmarshal([]byte(out.String()), &document), "stdout is not one JSON document: %s", out.String())
			assert.Equal(t, "flowstate-request-3f7c", document["workflowId"])
			assert.Equal(t, "0198f1e2-0000-7000-8000-000000000000", document["runId"])
			assert.Equal(t, v1.RunResponse_STATUS_RUNNING.String(), document["status"])

			assert.NotContains(t, errOut.String(), "started workflow",
				"the machine shapes carry the ids in the document, not in prose a reader has to parse past")
		})
	}
}

func TestADetachedRunStillReportsARefusedStart(t *testing.T) {
	fake := startedThenFinished()
	fake.runErr = assert.AnError
	cmd, _, _ := runCommandForTest(t, fake)
	cmd.Flags().Bool("detach", false, "")
	require.NoError(t, cmd.Flags().Set("detach", "true"))

	err := runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"})
	require.Error(t, err, "the exit code is the start's, and the start was refused")
}
