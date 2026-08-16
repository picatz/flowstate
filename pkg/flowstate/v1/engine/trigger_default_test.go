package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// What a durable run reports when its state records no trigger — the other half
// of the one asymmetry [tests.TriggerContextCases] deliberately leaves out.
//
// Every entry path in this repository records one now, so a state with none is a
// run that started before the field existed. Empty is the honest answer for it,
// and answering "manual" instead would be inventing provenance: that run may well
// have been a delivery, and a workflow branching on the invention would take a
// branch nobody chose for it (invariant 10 — a run that predates a field keeps
// meaning what it meant).
//
// What must not happen either way is a failure. Every field resolves, to an empty
// string, so a file reading `${trigger.name}` on such a run computes with a blank
// rather than dying three steps in on an unresolved reference.
func TestARunRecordingNoTriggerReadsEmptyRatherThanFailing(t *testing.T) {
	workflow := &v1.Workflow{
		Name:    "durable-absent-trigger",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   "how",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`"[" + trigger.kind + trigger.name + "]"`)},
		}},
	}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

	// No Trigger field: exactly the state a run started before this field existed
	// replays with.
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: workflow})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a run recording no trigger must still resolve `trigger`, or every workflow that predates the "+
			"field fails the moment one reads it")

	var out v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Equal(t, "[]",
		out.GetStepValues()["how"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetStringValue())
}

// A kind this build cannot produce is refused before any expression compares
// against it.
//
// The failure it prevents is quiet and authorization-shaped. A specification
// submitted by hand could record `kind: "admin"`, and every
// `${trigger.kind == "..."}` in the file would then compare against a word
// nothing can ever answer with — a branch that is never taken, forever, with no
// diagnostic anywhere. Fail closed: the run is refused rather than started under a
// provenance nobody checked.
func TestARunRecordingAnUnknownTriggerKindIsRefused(t *testing.T) {
	workflow := &v1.Workflow{
		Name:    "durable-bogus-trigger",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   "how",
			Kind: &v1.Node_Value{Value: v1.NewExpr("trigger.kind")},
		}},
	}

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: workflow,
		Trigger:  &v1.TriggerContext{Kind: "admin"},
	})
	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "a trigger kind this build cannot produce must refuse the run")
	require.Contains(t, err.Error(), "admin")
}
