package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// takesABlob is a workflow declaring one string input, for the size cases below.
func takesABlob() *v1.Workflow {
	return &v1.Workflow{
		Name:           "takes-a-blob",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{{Name: "blob", Type: v1.InputDeclaration_TYPE_STRING}},
		Steps: []*v1.Node{{
			Id:   "a",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")}}},
		}},
	}
}

// TestArgumentsAloneCannotPushARunPastWhatItCanCarry is the size half of the submit
// check, in the direction the specification's own check cannot see.
//
// `CheckSpecSize` weighs what an author wrote, and passes here: the workflow is a
// few hundred bytes. What a caller chose is the rest of what the run carries, and a
// caller who could push the pair past the blob limit with arguments alone would
// have found the hang invariant 9 exists to convert into an answer — the submission
// accepted, the run started, and the first Continue-As-New wedging a workflow task
// that is retried forever.
func TestArgumentsAloneCannotPushARunPastWhatItCanCarry(t *testing.T) {
	t.Parallel()

	spec := takesABlob()
	require.NoError(t, v1.CheckSpecSize(spec),
		"the fixture's own specification is already too large, so this proves nothing")

	inputs := map[string]*v1.Value{"blob": v1.NewLiteral(strings.Repeat("x", v1.MaxSpecBytes))}

	err := v1.CheckSubmissionSize(spec, inputs)
	require.Error(t, err, "arguments larger than a run can carry were accepted")
	require.Contains(t, err.Error(), "inputs it is being run with",
		"the refusal does not say which half is the problem: %v", err)

	// And the local driver refuses it too, through the same function, because the
	// two drivers share a submit boundary rather than each having their own.
	_, runErr := v1.RunWithInputs(t.Context(), spec, inputs)
	require.Error(t, runErr, "the local driver started a run the server would refuse")
}

// TestADefaultIsAppliedOnceAtSubmit pins where the defaulting happens.
//
// The bound map is what a run carries, so a value the caller did not send is
// decided here and never again. Re-deriving it per segment would let a declaration
// edited between deploys change an argument underneath a run in flight, which is
// the class of thing invariant 10 exists to stop — and the reason `RunState` holds
// the arguments rather than the engine reading the declarations.
func TestADefaultIsAppliedOnceAtSubmit(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "defaults",
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "retries", Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral(int64(3))},
			{Name: "tag", Type: v1.InputDeclaration_TYPE_STRING},
		},
	}

	bound, err := v1.BindRunInputs(spec, nil)
	require.NoError(t, err)

	require.Equal(t, int64(3), bound["retries"].GetLiteral().GetInt64Value(),
		"the default was not applied")

	// An optional input with no default and no value is *absent* rather than null.
	// "Not given" and "given as null" are different things, and only the first one
	// lets a file ask whether it was given one at all.
	require.NotContains(t, bound, "tag",
		"an input nobody supplied and nobody defaulted was invented")
}

// TestADefaultThatIsMistypedIsRefusedAtSubmitToo covers the specification that
// never was a Flowfile.
//
// `flow validate` reports a mistyped default where there is a line to point at.
// This is the same rule at the boundary a hand-built specification arrives
// through — an RPC caller, an agent, a generator — where fail-closed means the
// value is checked whoever built it.
func TestADefaultThatIsMistypedIsRefusedAtSubmitToo(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "bad-default",
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "retries", Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral("three")},
		},
	}

	_, err := v1.BindRunInputs(spec, nil)
	require.Error(t, err, "a default of the wrong type was accepted because nobody sent a value")
	require.Contains(t, err.Error(), "is declared int but was given string")
}
