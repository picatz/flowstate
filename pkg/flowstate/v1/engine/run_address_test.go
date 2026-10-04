package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunAddressShapeDurable checks the durable half of the shared assertion in
// [conformance.AssertRunAddressShape]: a durable run reports the workflow id it is
// addressed by — the id `flow signal` and `flow get` take — and a run id that
// identifies which execution of it this is. The local half is
// [flowstatev1_test.TestRunAddressShapeLocal].
//
// The expected values are Temporal's own test-environment defaults rather than
// anything this repository chooses, which is the point: the durable driver reads
// the address from the substrate instead of inventing one.
func TestRunAddressShapeDurable(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: conformance.RunAddressWorkflow(),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	// "default-test-workflow-id" and "default-test-run-id" are the test
	// environment's own constants. The run id is the *current* execution's here
	// because the test environment leaves FirstRunID unset and engine.runAddress
	// falls back — see its doc, and TestRunAddressPrefersFirstRunID below for the
	// half of that rule the test environment cannot exercise.
	conformance.AssertRunAddressShape(t, &outputs, "default-test-workflow-id", "default-test-run-id")
}

// TestRunAddressPrefersFirstRunID pins the choice engine.runAddress exists to
// make: given both, the address reports the *first* run id of the
// continued-execution chain and not the current execution's.
//
// This is the rule a workload that suspends depends on and the test environment
// cannot show, because it never populates FirstRunID at all. Without it, a run
// that continued as new — which this engine does on its own step budget, with
// nothing in the file to say when — would hand out one callback address before
// it suspended and a different one after.
func TestRunAddressPrefersFirstRunID(t *testing.T) {
	require.Equal(t, "first", engine.RunAddressFrom("wf", "first", "current").GetRunId())
	require.Equal(t, "wf", engine.RunAddressFrom("wf", "first", "current").GetWorkflowId())

	// And the fallback, for the one case where the substrate does not offer a
	// first run id: the current execution's, which is the correct answer for a
	// run that has not continued as new — the two are the same value then.
	require.Equal(t, "current", engine.RunAddressFrom("wf", "", "current").GetRunId())
}

// durableInstants runs the shared address workflow on the durable driver with the
// given state, an environment that started at start, and the search attributes a
// schedule would have attached, and returns what it reported.
func durableInstants(t *testing.T, state *v1.RunState, start time.Time, attributes temporal.SearchAttributes, continuedFrom string) *v1.Workflow_StepOutputs {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(start)
	if continuedFrom != "" {
		env.SetContinuedExecutionRunID(continuedFrom)
	}
	require.NoError(t, env.SetTypedSearchAttributesOnStart(attributes))

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)

	state.Workflow = conformance.RunAddressWorkflow()
	env.ExecuteWorkflow(engine.Run, state)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	return &outputs
}

// The durable half of [conformance.AssertRunInstants]: a first segment reports the
// start its own history records, and a run with no schedule has no slot.
func TestRunInstantsDurable(t *testing.T) {
	start := time.Date(2026, 8, 2, 7, 30, 0, 0, time.UTC)

	outputs := durableInstants(t, &v1.RunState{}, start, temporal.SearchAttributes{}, "")

	conformance.AssertRunInstants(t, outputs, start, time.Unix(0, 0))
}

// A continued segment reports the workload's start, which the chain carried, and
// not its own: a run that suspended reads the same instant before and after.
func TestRunStartedAtSurvivesContinueAsNew(t *testing.T) {
	began := time.Date(2026, 8, 2, 7, 30, 0, 0, time.UTC)
	later := began.Add(3 * time.Hour)

	outputs := durableInstants(t, &v1.RunState{
		WorkloadStartedAt: timestamppb.New(began),
		Segment:           2,
	}, later, temporal.SearchAttributes{}, "previous-run")

	conformance.AssertRunInstants(t, outputs, began, time.Unix(0, 0))
}

// A segment continued into by an interpreter that predates the carried start has
// none to report, and says so as the epoch rather than naming its own start as
// the workload's.
func TestRunStartedAtIsNotInventedForAChainThatNeverRecordedIt(t *testing.T) {
	outputs := durableInstants(t, &v1.RunState{Segment: 1}, time.Date(2026, 8, 2, 10, 30, 0, 0, time.UTC),
		temporal.SearchAttributes{}, "previous-run")

	conformance.AssertRunInstants(t, outputs, time.Unix(0, 0), time.Unix(0, 0))
}

// A scheduled firing reads the slot Temporal attached to its execution, once, and
// the slot is not the start: a backfilled firing runs now for a slot that was
// yesterday's. A manual run, and a firing with no attribute, have none.
func TestScheduledSlotIsReadFromTheExecution(t *testing.T) {
	start := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	slot := time.Date(2026, 8, 2, 7, 0, 0, 0, time.UTC)
	attributes := temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyTime("TemporalScheduledStartTime").ValueSet(slot))

	t.Run("a scheduled firing", func(t *testing.T) {
		outputs := durableInstants(t, &v1.RunState{Trigger: v1.NewScheduleTriggerContext("nightly", "ops")}, start, attributes, "")
		conformance.AssertRunInstants(t, outputs, start, slot)
	})

	t.Run("a manual run ignores it", func(t *testing.T) {
		outputs := durableInstants(t, &v1.RunState{Trigger: v1.NewManualTriggerContext("ops")}, start, attributes, "")
		conformance.AssertRunInstants(t, outputs, start, time.Unix(0, 0))
	})

	t.Run("a firing the execution does not date", func(t *testing.T) {
		outputs := durableInstants(t, &v1.RunState{Trigger: v1.NewScheduleTriggerContext("nightly", "ops")}, start, temporal.SearchAttributes{}, "")
		conformance.AssertRunInstants(t, outputs, start, time.Unix(0, 0))
	})

	t.Run("a later segment carries the one it was handed", func(t *testing.T) {
		trigger := v1.NewScheduleTriggerContext("nightly", "ops")
		trigger.ScheduledAt = timestamppb.New(slot)
		outputs := durableInstants(t, &v1.RunState{Trigger: trigger, WorkloadStartedAt: timestamppb.New(start), Segment: 1},
			start.Add(time.Hour), temporal.SearchAttributes{}, "previous-run")
		conformance.AssertRunInstants(t, outputs, start, slot)
	})
}
