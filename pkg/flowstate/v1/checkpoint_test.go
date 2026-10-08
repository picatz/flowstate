package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func step(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
}

func checkpointFixture(t *testing.T) (*v1.RunState, *v1.CheckpointOrigin) {
	t.Helper()

	state := &v1.RunState{
		Workflow: &v1.Workflow{Name: "wf", Steps: []*v1.Node{step("a"), step("b"), step("c")}},
		Frames:   []*v1.Frame{{NextNode: 2}},
		Inputs:   map[string]*v1.Value{"who": v1.NewLiteral("world")},
		Vars:     map[string]*v1.Value{"v": v1.NewLiteral("carried")},
	}

	return state, &v1.CheckpointOrigin{Run: &v1.RunAddress{WorkflowId: "wf", RunId: "r1"}, Step: "c"}
}

func TestCheckpointResumeCarriesStateExactly(t *testing.T) {
	t.Parallel()

	state, origin := checkpointFixture(t)
	cp, err := v1.NewCheckpoint(state, "build-1", origin)
	require.NoError(t, err)

	got, err := cp.Resume("build-1", nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(state, got), "an unpatched resume must carry the state exactly")

	// The checkpoint is a copy: mutating the resumed state must not change it.
	got.Inputs["who"] = v1.NewLiteral("tampered")
	again, err := cp.Resume("build-1", nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(state, again))
}

func TestCheckpointResumeAcceptsPatchAfterPosition(t *testing.T) {
	t.Parallel()

	state, origin := checkpointFixture(t)
	cp, err := v1.NewCheckpoint(state, "build-1", origin)
	require.NoError(t, err)

	patch := proto.Clone(state.GetWorkflow()).(*v1.Workflow)
	patch.Steps[2] = step("c_fixed")
	patch.Steps = append(patch.Steps, step("d"))

	got, err := cp.Resume("build-1", patch)
	require.NoError(t, err)
	require.Equal(t, "c_fixed", got.GetWorkflow().GetSteps()[2].GetId())
	require.True(t, proto.Equal(state.GetInputs()["who"], got.GetInputs()["who"]))
}

func TestCheckpointRefusals(t *testing.T) {
	t.Parallel()

	t.Run("patch rewrites an executed step", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		patch := proto.Clone(state.GetWorkflow()).(*v1.Workflow)
		patch.Steps[1] = step("b_rewritten")
		_, err = cp.Resume("build-1", patch)
		require.ErrorIs(t, err, v1.ErrCheckpointPatch)
	})

	t.Run("patch drops steps before the position", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		_, err = cp.Resume("build-1", &v1.Workflow{Name: "wf", Steps: state.GetWorkflow().GetSteps()[:1]})
		require.ErrorIs(t, err, v1.ErrCheckpointPatch)
	})

	t.Run("other build", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		_, err = cp.Resume("build-2", nil)
		require.ErrorIs(t, err, v1.ErrCheckpointBuild)
	})

	t.Run("state edited after emission", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		cp.State.Workflow.Steps[0] = step("other_spec")
		_, err = cp.Resume("build-1", nil)
		require.ErrorIs(t, err, v1.ErrCheckpointTampered)
	})

	t.Run("position past the workflow", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		cp.State.Frames[0].NextNode = 99
		_, err = cp.Resume("build-1", nil)
		require.ErrorIs(t, err, v1.ErrCheckpointTampered)
	})

	t.Run("negative position", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		cp.State.Frames = nil
		cp.State.NextStep = -1
		require.Error(t, cp.Verify())
		_, err = cp.Resume("build-1", &v1.Workflow{Name: "wf", Steps: state.GetWorkflow().GetSteps()})
		require.Error(t, err)
	})

	t.Run("position rewound past an executed step", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		cp.State.Frames[0].NextNode = 1
		require.ErrorIs(t, cp.Verify(), v1.ErrCheckpointTampered)
	})

	t.Run("position inside a nested frame", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		state.Frames = []*v1.Frame{{NextNode: 2}, {NextNode: 99}}
		_, err := v1.NewCheckpoint(state, "build-1", origin)
		require.ErrorIs(t, err, v1.ErrCheckpointUnsupported)

		state.Frames = []*v1.Frame{{NextNode: 2, NextIteration: 3}}
		_, err = v1.NewCheckpoint(state, "build-1", origin)
		require.ErrorIs(t, err, v1.ErrCheckpointUnsupported)
	})

	t.Run("patched workflow violates the schema", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		patch := proto.Clone(state.GetWorkflow()).(*v1.Workflow)
		patch.Name = ""
		patch.Steps[2] = &v1.Node{Id: "c"}
		_, err = cp.Resume("build-1", patch)
		require.Error(t, err)
		require.Contains(t, err.Error(), "patched workflow")
	})

	t.Run("unknown version", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		cp, err := v1.NewCheckpoint(state, "build-1", origin)
		require.NoError(t, err)

		cp.Version = 2
		_, err = cp.Resume("build-1", nil)
		require.Error(t, err)
	})

	t.Run("oversize state", func(t *testing.T) {
		t.Parallel()
		state, origin := checkpointFixture(t)
		state.Vars["big"] = v1.NewLiteral(strings.Repeat("x", v1.MaxRunStateBytes+1))
		_, err := v1.NewCheckpoint(state, "build-1", origin)
		require.Error(t, err)
		require.Contains(t, err.Error(), "byte limit")
	})

	t.Run("missing origin", func(t *testing.T) {
		t.Parallel()
		state, _ := checkpointFixture(t)
		_, err := v1.NewCheckpoint(state, "build-1", nil)
		require.Error(t, err)
	})
}
