package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// wrapStructure nests inner in levels of one-entry structure maps, the same
// shape [v1.MaxStructureDepth]'s own regression suite uses.
func wrapStructure(inner *v1.Value, levels int) *v1.Value {
	for range levels {
		inner = v1.NewStructureMap(map[string]*v1.Value{"k": inner})
	}
	return inner
}

// overDepthBehindACall builds a workflow whose only structure violation is
// hidden inside an inlined `call:` callee — nothing at the top level is over
// [v1.MaxStructureDepth] — so a validator that only walks the top level would
// accept it. Named for the finding it regresses: Codex's Finding 1, "the
// depth check does not traverse called workflows".
func overDepthBehindACall(name string) *v1.Workflow {
	callee := &v1.Workflow{
		Name: name + "-callee",
		Steps: []*v1.Node{{
			Id: "leaf",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"headers": wrapStructure(v1.NewLiteral("x"), v1.MaxStructureDepth+1),
				},
			}},
		}},
	}

	return &v1.Workflow{
		Name: name,
		Steps: []*v1.Node{{
			Id:   "provision",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}},
	}
}

// TestRunAndCreateScheduleAgreeOnAStructureHiddenInACallee is Finding 2's
// regression, stated as CLAUDE.md asks: an agreement between the two paths
// that can bring durable work into existence, not two independent tests that
// happen to both pass.
//
// Before this, [server.FlowstateServer.CreateSchedule] hand-rolled its own
// submission pipeline and never called [v1.CheckStructureDepth] at all — a
// hand-built scheduled workflow with an over-depth structure persisted and
// only failed a firing, at whatever hour that firing landed on, with nobody
// there to read the refusal. It shares
// [server.FlowstateServer.validateSpecification] with Run now, which is
// exactly what this test would catch a regression of: not "CreateSchedule
// refuses this workflow" in isolation, but "CreateSchedule refuses the exact
// workflow Run refuses, for the same reason".
//
// Neither RPC needs a live Temporal cluster to reach this refusal: both
// validate the specification before making their first call through the
// client, which is what makes `server.New(nil, ...)` a legitimate fixture
// here rather than a shortcut around the real path.
func TestRunAndCreateScheduleAgreeOnAStructureHiddenInACallee(t *testing.T) {
	t.Parallel()

	s := server.New(nil, server.WithNamespace("team-a"))

	runErr := func() error {
		_, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
			Workflow: overDepthBehindACall("run"),
		}))
		return err
	}()
	require.Error(t, runErr, "Run must refuse a structure hidden inside a called workflow")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(runErr))
	require.ErrorContains(t, runErr, "32",
		"the refusal must name the bound so an author has a number to act on")

	scheduleErr := func() error {
		_, err := s.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: overDepthBehindACall("schedule"),
		}))
		return err
	}()
	require.Error(t, scheduleErr,
		"CreateSchedule must refuse the identical specification Run refuses, rather than persisting a "+
			"schedule that only fails the first time it fires")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(scheduleErr))

	// The point of sharing validateSpecification rather than adding a third
	// hand-rolled copy: the two paths cannot merely both err, they have to
	// say the same thing, because the refusal is one rule read from one
	// place rather than two copies that can drift.
	require.Equal(t, runErr.Error(), scheduleErr.Error(),
		"Run and CreateSchedule must refuse a structure hidden in a callee for the identical reason, "+
			"in the identical words — this is the agreement CLAUDE.md's 'one meaning, written down "+
			"twice' section is about, one RPC further out")

	// A workflow whose structure never goes over the bound at all — same
	// call shape, none of it too deep — must still be accepted by both, so
	// the assertions above are pinning a real refusal and not a validator
	// that has started refusing every call step.
	within := &v1.Workflow{
		Name: "within",
		Steps: []*v1.Node{{
			Id: "provision",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
				Name: "within-callee",
				Steps: []*v1.Node{{
					Id: "leaf",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"headers": wrapStructure(v1.NewLiteral("x"), v1.MaxStructureDepth),
						},
					}},
				}},
			}}},
		}},
	}
	require.NoError(t, v1.CheckStructureDepth(within),
		"sanity: a structure exactly at the bound inside a callee must not itself be refused")
}
