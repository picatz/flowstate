package mcp

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// bytesOfSize is a rung that answers with a document of a chosen size.
func bytesOfSize(n int) func() ([]byte, error) {
	return func() ([]byte, error) { return []byte(strings.Repeat("x", n)), nil }
}

// TestFitResultStopsAtTheFirstRungThatFits is the property that makes a ladder
// a ladder rather than a switch: an answer loses the least it can.
//
// A runner that ran every rung and returned the last would satisfy "under the
// bound" while throwing away a transcript that would have fit, which is the
// quiet version of the bug this whole design exists to avoid.
func TestFitResultStopsAtTheFirstRungThatFits(t *testing.T) {
	t.Parallel()

	var ran []int

	record := func(i, size int) func() ([]byte, error) {
		return func() ([]byte, error) {
			ran = append(ran, i)

			return bytesOfSize(size)()
		}
	}

	encoded, rung, err := FitResult(
		record(0, MaxResultBytes+1),
		record(1, MaxResultBytes), // Exactly the bound, which must count as fitting.
		record(2, 1),
	)
	require.NoError(t, err)

	assert.Equal(t, 1, rung, "the ladder did not settle on the first rung that fits")
	assert.Len(t, encoded, MaxResultBytes)
	assert.Equal(t, []int{0, 1}, ran,
		"a rung past the first that fits was evaluated, so the answer lost more than it had to")
}

// TestFitResultLeavesAnAnswerThatFitsUntouched pins rung 0.
//
// Reported as rung 0 rather than merely "some rung", because that is how a
// caller tells "nothing was dropped" from "the first reduction was enough"
// without comparing bytes — and it is what decides whether a degradation note
// is attached to the answer at all.
func TestFitResultLeavesAnAnswerThatFitsUntouched(t *testing.T) {
	t.Parallel()

	encoded, rung, err := FitResult(bytesOfSize(16), bytesOfSize(1))
	require.NoError(t, err)

	assert.Equal(t, 0, rung)
	assert.Len(t, encoded, 16, "an answer that fits was reduced anyway")
}

// TestFitResultReturnsTheFloorEvenWhenItDoesNotFit is the contract that keeps a
// large answer from becoming no answer.
//
// Every rung's document parses; the floor is simply the smallest one this
// surface knows how to build. Returning nothing when even that is too large
// would hand a caller an empty result for a run it can still be told the
// status and identity of, and "too big to look at" is not an answer to "what
// happened".
func TestFitResultReturnsTheFloorEvenWhenItDoesNotFit(t *testing.T) {
	t.Parallel()

	encoded, rung, err := FitResult(
		bytesOfSize(MaxResultBytes+3),
		bytesOfSize(MaxResultBytes+2),
		bytesOfSize(MaxResultBytes+1),
	)
	require.NoError(t, err)

	assert.Equal(t, 2, rung, "the floor is the last rung, whether or not it fits")
	assert.Len(t, encoded, MaxResultBytes+1, "the floor was not returned")
	assert.Greater(t, len(encoded), MaxResultBytes,
		"this test is only meaningful if the floor is genuinely over the bound")
}

// TestFitResultStopsOnAnEncodingError keeps a defect visible.
//
// A rung that cannot be encoded is a bug in this surface, not a large answer.
// Falling through to the next rung would answer with a smaller document and
// hide it — the caller would see a reduced run and never learn that the full
// one was never rendered.
func TestFitResultStopsOnAnEncodingError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("rendering the answer: boom")

	var reachedFloor bool

	_, rung, err := FitResult(
		bytesOfSize(MaxResultBytes+1),
		func() ([]byte, error) { return nil, sentinel },
		func() ([]byte, error) { reachedFloor = true; return bytesOfSize(1)() },
	)

	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, rung, "the failing rung should be the one reported")
	assert.False(t, reachedFloor,
		"the ladder carried on past a rung that could not be encoded, hiding the defect behind a smaller answer")
}

// TestTheGetResponseLadderDropsInOrder walks the rungs directly, which the
// end-to-end tests in cmd/flow cannot do: they can see where the ladder
// *stopped*, not that rung 2 leaves the declared outputs alone.
//
// The order is the claim being pinned. Dropping the declared outputs before the
// step transcript would shed the answer to keep the commentary.
func TestTheGetResponseLadderDropsInOrder(t *testing.T) {
	t.Parallel()

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind: &v1.GetResponse_Outputs{Outputs: &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{
				"fetch": {NamedValues: map[string]*v1.Value{"body": v1.NewValue("transcript")}},
			},
		}},
		EntityState: &v1.EntityState{},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"answer": v1.NewValue("42"),
		}},
	}

	encode := func(message proto.Message) ([]byte, error) {
		return protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(message)
	}

	rungs, notes := getResponseLadder(response, encode)
	require.Len(t, rungs, 4, "the ladder is the untouched answer plus three reductions")
	require.Len(t, notes, len(rungs), "every rung needs a note, or a reduction cannot say what it dropped")

	// Rung 0 says nothing, because it dropped nothing.
	assert.Empty(t, notes[0])

	decode := func(t *testing.T, i int) *v1.GetResponse {
		t.Helper()

		encoded, err := rungs[i]()
		require.NoError(t, err)

		var got v1.GetResponse
		require.NoError(t, protojson.Unmarshal(encoded, &got),
			"rung %d produced a document that does not parse", i)

		return &got
	}

	untouched := decode(t, 0)
	require.NotNil(t, untouched.GetOutputs(), "rung 0 must be the answer as it arrived")
	require.NotNil(t, untouched.GetRunOutputs())

	// Rung 1: the transcript goes, and nothing else.
	first := decode(t, 1)
	assert.Nil(t, first.GetOutputs(), "rung 1 did not drop the step transcript")
	assert.NotNil(t, first.GetEntityState(), "rung 1 dropped the carried state early")
	assert.NotNil(t, first.GetRunOutputs(), "rung 1 dropped the declared outputs early")
	assert.NotEmpty(t, notes[1])

	// Rung 2: the carried state as well, declared outputs still intact.
	second := decode(t, 2)
	assert.Nil(t, second.GetEntityState(), "rung 2 did not drop the carried state")
	assert.NotNil(t, second.GetRunOutputs(), "rung 2 dropped the declared outputs early")

	// Rung 3: the floor. The identity and status survive, which is what makes it
	// an answer rather than a shrug.
	floor := decode(t, 3)
	assert.Nil(t, floor.GetRunOutputs(), "the floor kept the declared outputs")
	assert.Equal(t, "flowstate-workflow-3f7c", floor.GetWorkflowId())
	assert.Equal(t, v1.RunResponse_STATUS_COMPLETED, floor.GetStatus())

	// Every note names the bound and what to do, since a note a model cannot act
	// on is a note that produces the same call again.
	for i := 1; i < len(notes); i++ {
		assert.Contains(t, notes[i], "flow get", "rung %d's note does not say how to read what left", i)
	}
}

// TestTheGetResponseLadderNeverDropsTheRunsError is the rung that must not
// fire, checked at the ladder rather than end to end.
//
// GetResponse's oneof carries either the transcript or the error. Clearing it
// unconditionally would shed the reason a run failed, which is the single thing
// a caller most needs from a failed run.
func TestTheGetResponseLadderNeverDropsTheRunsError(t *testing.T) {
	t.Parallel()

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		Status:     v1.RunResponse_STATUS_FAILED,
		Kind: &v1.GetResponse_Error{Error: &v1.RunResponse_Error{
			Message: "step charge failed: upstream returned 503",
		}},
	}

	encode := func(message proto.Message) ([]byte, error) {
		return protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(message)
	}

	rungs, _ := getResponseLadder(response, encode)

	// Every rung, including the floor: no reduction may take the error with it.
	for i := range rungs {
		encoded, err := rungs[i]()
		require.NoError(t, err)

		var got v1.GetResponse
		require.NoError(t, protojson.Unmarshal(encoded, &got))

		require.NotNil(t, got.GetError(), "rung %d dropped the reason the run failed", i)
		assert.Contains(t, got.GetError().GetMessage(), "upstream returned 503")
	}
}

// TestTheGetResponseLadderDoesNotMutateItsInput matters because the local
// server may hand back a message it still holds, and a reduction is supposed to
// be a view of an answer rather than an edit to it.
func TestTheGetResponseLadderDoesNotMutateItsInput(t *testing.T) {
	t.Parallel()

	response := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind: &v1.GetResponse_Outputs{Outputs: &v1.Workflow_StepOutputs{
			StepValues: map[string]*v1.Node_Outputs{
				"fetch": {NamedValues: map[string]*v1.Value{"body": v1.NewValue("transcript")}},
			},
		}},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{"answer": v1.NewValue("42")}},
	}

	before := proto.Clone(response)

	encode := func(message proto.Message) ([]byte, error) {
		return protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(message)
	}

	rungs, _ := getResponseLadder(response, encode)
	for i := range rungs {
		_, err := rungs[i]()
		require.NoError(t, err)
	}

	assert.True(t, proto.Equal(before, response),
		"running the ladder edited the caller's own response message")
}
