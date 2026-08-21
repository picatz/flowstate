package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The ladder itself — which rung drops what, in what order, what the floor
// keeps, and that a failed run's error survives every rung — is tested in
// cmd/flow/internal/mcp/result_test.go, where it needs no server and the tests
// run in parallel.
//
// What cannot be tested there is the *wiring*: that dispatch reaches the ladder
// at all, that the note becomes a second content block rather than a field in
// the document, and that a tool without a ladder still refuses. That is what
// these tests are for, and there are deliberately few of them: connectRemoteMCP
// stands a fake deployment up through t.Setenv, so every test here is serial,
// and serial tests in this package are the expensive ones.

// manyStepTranscript is a step transcript past [flowmcp.MaxResultBytes], which
// is the shape that matters: the size comes from what the workload computed, not
// from how many fields the schema has.
//
// Many steps rather than one enormous one, because that is what a reduction can
// actually act on — the ladder keeps whole, real steps and drops the rest, so a
// transcript of one step can only ever be kept or be too big.
func manyStepTranscript() *v1.Workflow_StepOutputs {
	steps := make(map[string]*v1.Node_Outputs, 64)
	for i := range 64 {
		steps[fmt.Sprintf("step_%02d", i)] = &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"body": v1.NewValue(strings.Repeat("x", 16<<10))},
		}
	}

	return &v1.Workflow_StepOutputs{StepValues: steps}
}

// getToolBlocks calls flowstate_get against a stand-in deployment and returns
// the answer's content blocks: the document, then any notes after it.
func getToolBlocks(t *testing.T, response *v1.GetResponse) (document string, notes []string, result *mcp.CallToolResult) {
	t.Helper()

	posture := defaultLocalRunPosture()

	// The ladder is about size, not about redaction: withholding a declared
	// output replaces it with a marker, which would shrink the very field these
	// tests are trying to make large. --reveal-sensitive keeps a fixture the
	// size the test built it.
	require.NoError(t, posture.Flags().Set(revealSensitiveFlagName, "true"))

	session := connectRemoteMCP(t, posture, &fakeWorkflowService{getResponse: response})

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("Get"),
		Arguments: map[string]any{"workflowId": "flowstate-workflow-3f7c"},
	})
	require.NoError(t, err, "the call itself must succeed; the answer is what is bounded")
	require.NotEmpty(t, result.Content)

	blocks := make([]string, 0, len(result.Content))
	for _, content := range result.Content {
		text, ok := content.(*mcp.TextContent)
		require.True(t, ok, "flowstate_get answered with a %T content block", content)
		blocks = append(blocks, text.Text)
	}

	return blocks[0], blocks[1:], result
}

// TestTheGetToolLadderOverTheWire drives both directions through a real MCP
// session, because either one alone passes while the other is broken.
//
// One serial test with two halves rather than two serial tests: connectRemoteMCP
// forbids t.Parallel, and this package's serial tests are what its wall clock is
// made of.
func TestTheGetToolLadderOverTheWire(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	// The control, and the half a carelessly written ladder breaks: a reduction
	// that also fired on answers that fit would quietly strip every run's
	// transcript.
	t.Run("an answer under the ceiling is untouched", func(t *testing.T) {
		response := &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "6b1f",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{Outputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"greet": {NamedValues: map[string]*v1.Value{"message": v1.NewValue("hello")}},
				},
			}},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{"answer": v1.NewValue("42")}},
		}

		document, notes, result := getToolBlocks(t, response)

		require.False(t, result.IsError, "a small answer must not be refused: %v", result.Content)
		assert.Empty(t, notes,
			"an answer that dropped nothing must not claim it dropped something")

		expected, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response)
		require.NoError(t, err)
		assert.JSONEq(t, string(expected), document,
			"an answer under the ceiling stopped being the protojson of its response message")
	})

	// The bound is asserted *reached*, not merely unexceeded: the untouched
	// document is measured and required to exceed the ceiling first, because a
	// fixture that quietly fit would make every assertion below pass while
	// testing nothing.
	t.Run("an oversized answer is reduced and says so", func(t *testing.T) {
		response := &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "6b1f",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind:       &v1.GetResponse_Outputs{Outputs: manyStepTranscript()},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{"answer": v1.NewValue("42")}},
		}

		untouched, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response)
		require.NoError(t, err)
		require.Greater(t, len(untouched), flowmcp.MaxResultBytes,
			"the fixture fits, so this test would pass without the ladder ever running")

		document, notes, result := getToolBlocks(t, response)

		// Not an error: a reduced answer is an answer. An error result is what
		// the refusal this replaced meant, and a model reading IsError as "ask
		// again with something different" would loop on a run it was just handed.
		assert.False(t, result.IsError, "a reduced answer must not be reported as a failure")

		assert.LessOrEqual(t, len(document), flowmcp.MaxResultBytes,
			"the reduced answer is still over the surface's ceiling")

		// Parses, and parses as the message the schema says this tool answers
		// with — not merely as some JSON object. Half a document would satisfy
		// the length assertion above and nothing else.
		var got v1.GetResponse
		require.NoError(t, protojson.Unmarshal([]byte(document), &got),
			"the reduced answer stopped being a parseable GetResponse")

		// Reduced, not cleared: `GetResponse.kind` is a required oneof, so an
		// answer with the arm removed is one v1.Validate rejects. The document
		// is checked against the schema here and not merely parsed, which is
		// the assertion the first version of this test was missing (#853).
		require.NoError(t, v1.Validate(&got),
			"the reduced answer is not a GetResponse the schema accepts")
		assert.NotEmpty(t, got.GetOutputs().GetStepValues(),
			"the transcript arm was emptied, which the schema rejects")
		assert.Less(t, len(got.GetOutputs().GetStepValues()), 64,
			"the oversized step transcript was kept whole")

		// What a reader most needs is what survives: the identity, the status,
		// and the run's declared outputs — the answer, as against the transcript.
		assert.Equal(t, "flowstate-workflow-3f7c", got.GetWorkflowId())
		assert.Equal(t, "6b1f", got.GetRunId())
		assert.Equal(t, v1.RunResponse_STATUS_COMPLETED, got.GetStatus())
		require.NotNil(t, got.GetRunOutputs(), "the declared outputs were dropped before they had to be")
		assert.Equal(t, "42", got.GetRunOutputs().GetValues()["answer"].GetLiteral().GetStringValue())

		// It says it degraded, in a block a model reads and can act on.
		require.Len(t, notes, 1, "a reduced answer has to say what left")
		assert.Contains(t, notes[0], "step transcript", "the note should name what was dropped")
		assert.Contains(t, notes[0], fmt.Sprint(flowmcp.MaxResultBytes), "the note should name the limit")
		assert.Contains(t, notes[0], "flow get", "the note should say how to read what left")

		// And the note stays *out* of the document. The first block is exactly
		// the protojson of a GetResponse — the same bytes `--output json` prints
		// — so a caller that unmarshals strictly is not broken on the day a run
		// gets large. A note added as a field beside the response's own would
		// fail here.
		require.NoError(t, protojson.UnmarshalOptions{DiscardUnknown: false}.Unmarshal([]byte(document), &got),
			"the answer carries a field the schema does not describe")

		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(document), &fields))
		for name := range fields {
			assert.NotContains(t, strings.ToLower(name), "note",
				"the degradation note was smuggled into the document as %q", name)
		}
	})
}

// TestAFailedRunWithAnOversizedReasonIsStillAnswered is the P1 finding over the
// wire.
//
// A failed run carries no transcript, and normally no carried state and no
// declared outputs either, so every rung of the ladder was a no-op and the tool
// answered with a document far over the ceiling as though it had fitted.
// `RunResponse.Error.message` has no `max_len` in the schema and carries a task's
// or an application's own error, so that was the workload-chosen resource
// escaping the bound — the one thing this surface's cap exists to stop.
//
// It has to come back as an *answer*, not a refusal: the reason a run failed is
// why anyone reads a failed run, and a shortened reason beats none.
func TestAFailedRunWithAnOversizedReasonIsStillAnswered(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	const opening = "step charge failed: upstream returned 503"

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "6b1f",
		Status:     v1.RunResponse_STATUS_FAILED,
		Kind: &v1.GetResponse_Error{Error: &v1.RunResponse_Error{
			Message: opening + " " + strings.Repeat("E", flowmcp.MaxResultBytes+(64<<10)),
		}},
	}

	untouched, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response)
	require.NoError(t, err)
	require.Greater(t, len(untouched), flowmcp.MaxResultBytes,
		"the fixture fits, so this test would pass without the cap ever running")

	document, notes, result := getToolBlocks(t, response)

	assert.False(t, result.IsError, "a run whose reason had to be shortened is still an answer")
	require.LessOrEqual(t, len(document), flowmcp.MaxResultBytes,
		"a failure-only answer escaped the surface's ceiling")

	var got v1.GetResponse
	require.NoError(t, protojson.Unmarshal([]byte(document), &got))
	require.NoError(t, v1.Validate(&got), "the bounded answer is not a valid GetResponse")

	require.NotNil(t, got.GetError(), "the reason the run failed was dropped rather than shortened")
	assert.Contains(t, got.GetError().GetMessage(), opening,
		"the reason survived but not usably: a caller cannot tell what went wrong from it")
	assert.Equal(t, v1.RunResponse_STATUS_FAILED, got.GetStatus())

	require.Len(t, notes, 1, "a shortened reason has to say it was shortened")
	assert.Contains(t, notes[0], "failure message")
}

// TestAnOversizedListingIsStillRefused pins a decision, not an omission.
//
// A listing looks like the easiest thing on this surface to shorten and is the
// one thing here that must not be shortened. ListResponse.next_page_token
// addresses where the *server's* scan stopped, and this binary cannot mint one —
// so returning fewer runs beside the server's token would leave the dropped runs
// behind a cursor already past them, absent from every later page rather than
// delayed. That is the defect server/list.go bounds its own batch size to make
// unrepresentable (see the comment on `batch :=` there), and this test is what
// stops it being reintroduced one layer up by someone extending the Get ladder
// "for consistency". Returning fewer runs with an *empty* token would be worse
// still: a truncated listing claiming to be the whole of it.
func TestAnOversizedListingIsStillRefused(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	// A page big enough to pass the ceiling on run count alone, which is the
	// only way a listing gets large: every field of a RunSummary is bounded by
	// the schema, so no single run can do it.
	runs := make([]*v1.RunSummary, 0, 1000)
	for i := range 1000 {
		runs = append(runs, &v1.RunSummary{
			WorkflowId: fmt.Sprintf("flowstate-workflow-%s-%04d", strings.Repeat("d", 64), i),
			RunId:      fmt.Sprintf("%s-%04d", strings.Repeat("r", 64), i),
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Name:       strings.Repeat("n", 128),
		})
	}

	listing := &v1.ListResponse{Runs: runs, NextPageToken: "more-to-come"}

	untouched, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(listing)
	require.NoError(t, err)
	require.Greater(t, len(untouched), flowmcp.MaxResultBytes,
		"the fixture fits, so this test says nothing about an oversized listing")

	session := connectRemoteMCP(t, defaultLocalRunPosture(), &fakeWorkflowService{
		listResponses: []*v1.ListResponse{listing},
	})

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("List"),
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	text := result.Content[0].(*mcp.TextContent).Text

	require.True(t, result.IsError,
		"an oversized listing must be refused, not silently shortened into a page that skips runs")
	assert.LessOrEqual(t, len(text), flowmcp.MaxResultBytes)

	// A refusal an agent cannot act on is a refusal it will repeat verbatim.
	assert.Contains(t, text, flowmcp.ToolName("List"), "the refusal should name the tool that overflowed")
	assert.Contains(t, text, fmt.Sprint(flowmcp.MaxResultBytes), "the refusal should name the limit")
	assert.Contains(t, text, "ask for less", "the refusal should say what to do instead")

	// And it must not be a truncated listing wearing a refusal's clothes: no
	// parseable ListResponse carrying some of the runs.
	var partial v1.ListResponse
	if protojson.Unmarshal([]byte(text), &partial) == nil {
		require.Empty(t, partial.GetRuns(),
			"the refusal came back as a listing carrying some of the runs, "+
				"which is the skipping page this test exists to forbid")
	}
}
