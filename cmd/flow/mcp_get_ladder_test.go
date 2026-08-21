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

// hugeValue is a workflow-chosen value large enough on its own to carry a
// GetResponse past [flowmcp.MaxResultBytes], which is the shape that matters:
// the size comes from what the workload computed, not from how many fields the
// schema has.
func hugeValue(t *testing.T) *v1.Value {
	t.Helper()

	huge := strings.Repeat("x", flowmcp.MaxResultBytes+(64<<10))
	require.Greater(t, len(huge), flowmcp.MaxResultBytes,
		"the fixture is not large enough to force the ladder to drop anything")

	return v1.NewValue(huge)
}

// hugeTranscript is a step transcript past the ceiling on its own.
func hugeTranscript(t *testing.T) *v1.Workflow_StepOutputs {
	t.Helper()

	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"fetch": {NamedValues: map[string]*v1.Value{"body": hugeValue(t)}},
	}}
}

// smallTranscript is a step transcript comfortably under the ceiling.
func smallTranscript() *v1.Workflow_StepOutputs {
	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"greet": {NamedValues: map[string]*v1.Value{"message": v1.NewValue("hello")}},
	}}
}

// getToolText calls flowstate_get against a stand-in deployment and returns the
// answer's content blocks.
func getToolText(t *testing.T, response *v1.GetResponse) (document string, notes []string, result *mcp.CallToolResult) {
	t.Helper()

	posture := defaultLocalRunPosture()

	// The ladder is about size, not about redaction: withholding a declared
	// output replaces it with a marker, which would shrink the very field the
	// test is trying to make large. --reveal-sensitive keeps the fixture's size
	// the size the test built.
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

// TestTheGetToolAnswerUnderTheCeilingIsUntouched is the control, and it is the
// half that a ladder breaks if it is written carelessly: a reduction that also
// fires on answers that fit would quietly strip every run's transcript.
//
// It pins both things at once — the document is byte-for-byte the protojson of
// the response, and there is no second content block, because rung 0 carries no
// note and a note on an answer that lost nothing would be a lie.
func TestTheGetToolAnswerUnderTheCeilingIsUntouched(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "6b1f",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind:       &v1.GetResponse_Outputs{Outputs: smallTranscript()},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"answer": v1.NewValue("42"),
		}},
	}

	document, notes, result := getToolText(t, response)

	require.False(t, result.IsError, "a small answer must not be refused: %v", result.Content)
	assert.Empty(t, notes, "an answer that dropped nothing must not claim it dropped something")

	expected, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response)
	require.NoError(t, err)
	assert.JSONEq(t, string(expected), document,
		"an answer under the ceiling stopped being the protojson of its response message")

	// The transcript and the declared outputs both survive, named rather than
	// inferred from the byte comparison above, so a future change that reduces
	// an answer that fits fails here with a readable reason.
	var got v1.GetResponse
	require.NoError(t, protojson.Unmarshal([]byte(document), &got))
	assert.NotNil(t, got.GetOutputs(), "the step transcript was dropped from an answer that fits")
	assert.NotNil(t, got.GetRunOutputs(), "the declared outputs were dropped from an answer that fits")
}

// TestTheGetToolReducesAnOversizedTranscript is the first rung: a run whose
// step transcript alone is past the ceiling comes back without it, and *says*
// so, rather than not coming back at all.
//
// The bound is asserted reached rather than merely respected — the untouched
// document is measured and required to exceed the ceiling — because a fixture
// that quietly fit would make every assertion below pass while testing nothing,
// which is the "green by not running" failure this repo hunts.
func TestTheGetToolReducesAnOversizedTranscript(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "6b1f",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind:       &v1.GetResponse_Outputs{Outputs: hugeTranscript(t)},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"answer": v1.NewValue("42"),
		}},
	}

	untouched, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(response)
	require.NoError(t, err)
	require.Greater(t, len(untouched), flowmcp.MaxResultBytes,
		"the fixture fits, so this test would pass without the ladder ever running")

	document, notes, result := getToolText(t, response)

	// Not an error: a reduced answer is an answer. The refusal this replaced is
	// what an error result meant here, and a model that reads IsError as "ask
	// again with something different" would loop on a run it was just handed.
	assert.False(t, result.IsError, "a reduced answer must not be reported as a failure")

	assert.LessOrEqual(t, len(document), flowmcp.MaxResultBytes,
		"the reduced answer is still over the surface's ceiling")

	// Parses, and parses as the message the schema says this tool answers with
	// — not merely as some JSON object. Half a document would satisfy the
	// length assertion above and nothing else.
	var got v1.GetResponse
	require.NoError(t, protojson.Unmarshal([]byte(document), &got),
		"the reduced answer stopped being a parseable GetResponse")

	assert.Nil(t, got.GetOutputs(), "the oversized step transcript was kept")

	// What a reader most needs is what survives: the status, the ids, and the
	// run's declared outputs — the answer, as against the transcript.
	assert.Equal(t, v1.RunResponse_STATUS_COMPLETED, got.GetStatus())
	assert.Equal(t, "flowstate-workflow-3f7c", got.GetWorkflowId())
	assert.Equal(t, "6b1f", got.GetRunId())
	require.NotNil(t, got.GetRunOutputs(), "the declared outputs were dropped before they had to be")
	assert.Equal(t, "42", got.GetRunOutputs().GetValues()["answer"].GetLiteral().GetStringValue())

	// And it says it degraded, in a block a model reads and can act on.
	require.Len(t, notes, 1, "a reduced answer has to say what left")
	assert.Contains(t, notes[0], "step transcript", "the note should name what was dropped")
	assert.Contains(t, notes[0], fmt.Sprint(flowmcp.MaxResultBytes), "the note should name the limit")
	assert.Contains(t, notes[0], "flow get", "the note should say how to read what left")
}

// TestTheGetToolFloorIsReturnedWhenEvenTheOutputsAreOversized walks to the
// bottom of the ladder: every workflow-chosen field is past the ceiling on its
// own, so all three rungs fire and the floor is returned whether or not it fits.
//
// The floor is the point of the whole design. What remains is bounded by the
// schema rather than by the workload — a status, two ids, timing — so this is
// the document that cannot be reduced further, and it must still parse.
func TestTheGetToolFloorIsReturnedWhenEvenTheOutputsAreOversized(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "6b1f",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind:       &v1.GetResponse_Outputs{Outputs: hugeTranscript(t)},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"answer": hugeValue(t),
		}},
	}

	document, notes, result := getToolText(t, response)

	assert.False(t, result.IsError)
	assert.LessOrEqual(t, len(document), flowmcp.MaxResultBytes,
		"the floor should fit: what remains is bounded by the schema")

	var got v1.GetResponse
	require.NoError(t, protojson.Unmarshal([]byte(document), &got),
		"the floor stopped being a parseable GetResponse")

	assert.Nil(t, got.GetOutputs(), "the transcript survived the floor")
	assert.Nil(t, got.GetRunOutputs(), "the declared outputs survived the floor")

	// The identity of the run is exactly what a caller needs in order to go and
	// read it another way, so the floor keeping it is the difference between a
	// reduced answer and no answer.
	assert.Equal(t, "flowstate-workflow-3f7c", got.GetWorkflowId())
	assert.Equal(t, "6b1f", got.GetRunId())
	assert.Equal(t, v1.RunResponse_STATUS_COMPLETED, got.GetStatus())

	require.Len(t, notes, 1)
	assert.Contains(t, notes[0], "declared outputs",
		"the floor's note should name the declared outputs, the last thing it dropped")
}

// TestTheGetToolKeepsTheErrorOfAFailedRun is the rung that must not fire.
//
// GetResponse's oneof carries either the step transcript or the run's error, and
// clearing it to shed bytes would drop the reason a run failed — the single most
// valuable thing in a failed run's document, and the reason an agent called this
// tool at all. The transcript rung is therefore conditional on the oneof
// actually holding a transcript, and this is what says so.
func TestTheGetToolKeepsTheErrorOfAFailedRun(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "6b1f",
		Status:     v1.RunResponse_STATUS_FAILED,
		Kind: &v1.GetResponse_Error{Error: &v1.RunResponse_Error{
			Message: "step charge failed: upstream returned 503",
		}},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"answer": hugeValue(t),
		}},
	}

	document, _, result := getToolText(t, response)

	assert.False(t, result.IsError)
	assert.LessOrEqual(t, len(document), flowmcp.MaxResultBytes)

	var got v1.GetResponse
	require.NoError(t, protojson.Unmarshal([]byte(document), &got))

	require.NotNil(t, got.GetError(),
		"the ladder dropped the reason a run failed, which is what the caller asked for")
	assert.Contains(t, got.GetError().GetMessage(), "upstream returned 503")
}

// TestAnOversizedListingIsStillRefused pins a decision, not an omission.
//
// A listing looks like the easiest thing on this surface to shorten and is the
// one thing here that must not be shortened. ListResponse.next_page_token
// addresses where the *server's* scan stopped, and cmd/flow cannot mint one — so
// returning fewer runs beside the server's token would leave the dropped runs
// behind a cursor already past them, absent from every later page rather than
// delayed. That is the defect server/list.go bounds its own batch size to make
// unrepresentable (see the comment on `batch :=` there), and this test is what
// stops it being reintroduced one layer up by someone extending the Get ladder
// "for consistency".
//
// Returning fewer runs with an empty token would be worse still: a truncated
// listing claiming to be the whole of it.
func TestAnOversizedListingIsStillRefused(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	// A page big enough to pass the ceiling on run count alone, which is the
	// only way a listing gets large: every field of a RunSummary is bounded by
	// the schema.
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
			"the refusal came back as a listing carrying some of the runs, which is the skipping page this test exists to forbid")
	}
}

// TestAnOversizedValidationReportIsStillRefused is the same decision for the
// other message a caller can make large, and it is here because the reason
// differs: diagnostics are a feature, and a report carrying some of a file's
// problems tells an author to fix what they were shown and ship the rest.
//
// TestAnRPCToolAnswerIsBounded already drives Validate over a real session and
// asserts the refusal. This one states *why* it is still a refusal after the Get
// ladder landed, so the two are not read as one having been forgotten.
func TestAnOversizedValidationReportIsStillRefused(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	var source strings.Builder
	source.WriteString("edition: v2026.3\nname: x\nsteps:\n")
	for i := range 400 {
		fmt.Fprintf(&source, "  - id: step%04d\n    nope:\n      x: y\n", i)
	}

	files := make([]map[string]any, 0, 64)
	for i := range 64 {
		files = append(files, map[string]any{
			"name":   fmt.Sprintf("%s-%02d.yaml", strings.Repeat("d", 48), i),
			"source": []byte(source.String()),
		})
	}

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("Validate"),
		Arguments: map[string]any{"files": files},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	require.True(t, result.IsError,
		"a report that cannot be given whole must be refused, not reduced to some of a file's problems")

	text := result.Content[0].(*mcp.TextContent).Text
	assert.LessOrEqual(t, len(text), flowmcp.MaxResultBytes)

	// Not a partial report: a ValidationReport that parses and carries a subset
	// of the diagnostics is exactly the answer this refusal exists to avoid.
	var partial v1.ValidationReport
	if protojson.Unmarshal([]byte(text), &partial) == nil {
		require.Empty(t, partial.GetFiles(),
			"the refusal came back as a report carrying some of the files' diagnostics")
	}
}

// TestTheGetLadderNoteIsNotSmuggledIntoTheDocument guards the shape of the
// answer rather than its size.
//
// The note lives in a second content block precisely so the first stays exactly
// the protojson of a GetResponse — the same bytes `--output json` prints, which
// is what keeps this surface from being a second dialect. A future change that
// moved the note into the document would make flowstate_get answer a shape the
// schema does not describe, and every caller that unmarshals strictly would
// break on the day a run got large.
func TestTheGetLadderNoteIsNotSmuggledIntoTheDocument(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind:       &v1.GetResponse_Outputs{Outputs: hugeTranscript(t)},
	}

	document, notes, _ := getToolText(t, response)
	require.Len(t, notes, 1)

	// Strict: an unknown field is an error, which is what catches a note added
	// as `"note": "..."` beside the response's own fields.
	var got v1.GetResponse
	require.NoError(t, protojson.UnmarshalOptions{DiscardUnknown: false}.Unmarshal([]byte(document), &got),
		"the answer carries a field the schema does not describe")

	// And the note's own prose is not in the document under any spelling.
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(document), &fields))
	for name := range fields {
		assert.NotContains(t, strings.ToLower(name), "note",
			"the degradation note was smuggled into the document as %q", name)
	}
}
