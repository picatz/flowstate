package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// scheduledWorkflow is a workload that finishes on its own, which is what makes it
// safe to actually fire in a test.
//
// The gated workflow the other tenancy tests use would sit at its approval gate for
// two minutes per firing; here the run is not the subject, the schedule is.
func scheduledWorkflow(name string) *v1.Workflow {
	return &v1.Workflow{
		Name: name,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "region", Type: v1.InputDeclaration_TYPE_STRING, Default: v1.NewLiteral("eu-west-1")},
			{Name: "attempts", Type: v1.InputDeclaration_TYPE_INT, Required: true},
		},
		Triggers: &v1.Triggers{
			Schedule: &v1.ScheduleTrigger{
				// Hourly rather than by the minute: this test triggers the schedule
				// itself, so the cadence only has to be a legal one that will not fire
				// on its own while the test is looking at it.
				Cron:    []string{"0 * * * *"},
				Overlap: v1.ScheduleTrigger_OVERLAP_SKIP,
			},
		},
		Steps: []*v1.Node{
			{
				Id: "report",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("reporting")},
				}},
			},
		},
	}
}

// TestAScheduleIsCreatedDescribedFiredAndDeleted walks the whole life of a
// schedule against a real Temporal.
//
// One test rather than five, because the verbs are only meaningful in sequence: a
// describe of a schedule nothing created proves nothing, and a delete is only
// interesting if something was there. This is also the only place the arguments
// bound at creation are shown surviving to the far side — read back out of what the
// cluster stored rather than out of what this process still holds.
func TestAScheduleIsCreatedDescribedFiredAndDeleted(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	created, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: scheduledWorkflow("nightly-report"),
		Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(3))},

		// Created paused, which is what makes this test deterministic: nothing fires
		// except what the test asks to fire. It is also the posture the CLI's help
		// recommends, so the path exercised here is the one people are told to use.
		Paused: true,
	}))
	require.NoError(t, err)

	schedule := created.Msg.GetSchedule()
	require.Equal(t, "nightly-report", schedule.GetName())
	require.True(t, schedule.GetPaused())

	t.Run("it describes what was created", func(t *testing.T) {
		described, err := fixture.teamA.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: "nightly-report"}))
		require.NoError(t, err)

		got := described.Msg.GetSchedule()
		assert.Equal(t, "nightly-report", got.GetWorkflowName())

		// The cadence as the file declared it. Asking Temporal what the cadence is
		// answers a cron expression as a set of calendar ranges, so this is read back
		// out of the stored specification — and this assertion is what holds that.
		assert.Equal(t, []string{"0 * * * *"}, got.GetTrigger().GetCron())
		assert.Equal(t, v1.ScheduleTrigger_OVERLAP_SKIP, got.GetTrigger().GetOverlap())

		// The arguments, bound once at creation and carried. The default was filled
		// in there and not here, which is the point: every firing gets what this
		// submission established, not what a later reading of the declarations would
		// produce.
		assert.Equal(t, int64(3), got.GetInputs()["attempts"].GetLiteral().GetInt64Value())
		assert.Equal(t, "eu-west-1", got.GetInputs()["region"].GetLiteral().GetStringValue())
	})

	t.Run("it appears in the listing", func(t *testing.T) {
		listed, err := fixture.teamA.ListSchedules(t.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
		require.NoError(t, err)
		require.False(t, listed.Msg.GetTruncated())

		names := make([]string, 0, len(listed.Msg.GetSchedules()))
		for _, summary := range listed.Msg.GetSchedules() {
			names = append(names, summary.GetName())
		}
		assert.Contains(t, names, "nightly-report")
	})

	t.Run("triggering it starts a run", func(t *testing.T) {
		_, err := fixture.teamA.TriggerSchedule(t.Context(),
			connect.NewRequest(&v1.TriggerScheduleRequest{Name: "nightly-report"}))
		require.NoError(t, err)

		// A trigger returns before the cluster takes the action, so the run appears
		// afterwards — which is why the RPC answers with nothing and why this waits
		// rather than asserting immediately.
		var workflowID string
		require.Eventually(t, func() bool {
			described, err := fixture.teamA.DescribeSchedule(t.Context(),
				connect.NewRequest(&v1.DescribeScheduleRequest{Name: "nightly-report"}))
			if err != nil || len(described.Msg.GetSchedule().GetRecentRuns()) == 0 {
				return false
			}
			workflowID = described.Msg.GetSchedule().GetRecentRuns()[0].GetWorkflowId()

			return workflowID != ""
		}, 60*time.Second, 200*time.Millisecond, "the schedule never took an action")

		// The run it started is an ordinary run of this tenant's — addressable by
		// `flow get`, listed by `flow list`, authorized by the same memo. A firing
		// that produced a run nobody could reach would be a schedule that works and a
		// feature that does not.
		require.Eventually(t, func() bool {
			got, err := fixture.teamA.Get(t.Context(),
				connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))

			return err == nil && got.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
		}, 60*time.Second, 200*time.Millisecond, "the run the schedule started never completed")
	})

	t.Run("it can be resumed and paused again", func(t *testing.T) {
		_, err := fixture.teamA.ResumeSchedule(t.Context(),
			connect.NewRequest(&v1.ResumeScheduleRequest{Name: "nightly-report", Note: "back on"}))
		require.NoError(t, err)

		described, err := fixture.teamA.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: "nightly-report"}))
		require.NoError(t, err)
		assert.False(t, described.Msg.GetSchedule().GetPaused())
		assert.NotEmpty(t, described.Msg.GetSchedule().GetNextRunTimes(),
			"a live schedule reports when it next fires")

		_, err = fixture.teamA.PauseSchedule(t.Context(),
			connect.NewRequest(&v1.PauseScheduleRequest{Name: "nightly-report", Note: "enough"}))
		require.NoError(t, err)

		described, err = fixture.teamA.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: "nightly-report"}))
		require.NoError(t, err)
		assert.True(t, described.Msg.GetSchedule().GetPaused())
		assert.Equal(t, "enough", described.Msg.GetSchedule().GetNote())
	})

	t.Run("and deleting it takes it away", func(t *testing.T) {
		_, err := fixture.teamA.DeleteSchedule(t.Context(),
			connect.NewRequest(&v1.DeleteScheduleRequest{Name: "nightly-report"}))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, err := fixture.teamA.DescribeSchedule(t.Context(),
				connect.NewRequest(&v1.DescribeScheduleRequest{Name: "nightly-report"}))

			return err != nil && connect.CodeOf(err) == connect.CodeNotFound
		}, 30*time.Second, 100*time.Millisecond, "the deleted schedule is still describable")
	})
}

// TestAnotherTenantCannotReachASchedule is the negative direction, and the one
// worth having.
//
// CLAUDE.md's rule: an isolation test asserting that each party reaches its own
// resource is a functionality test wearing a security test's clothes. So team B is
// given the exact name team A used — which is the whole attack, since a schedule
// name is chosen by a person and `nightly-report` is what everybody calls theirs —
// and every verb is tried.
func TestAnotherTenantCannotReachASchedule(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: scheduledWorkflow("nightly-report"),
		Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
		Paused:   true,
	}))
	require.NoError(t, err)

	const name = "nightly-report"

	t.Run("cannot describe it", func(t *testing.T) {
		_, err := fixture.teamB.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: name}))
		require.Error(t, err, "another tenant read a schedule by name")

		// Not found rather than denied: denied would confirm a schedule of that name
		// exists somewhere, which is the fact a caller in the wrong tenant must not
		// learn — and with a human-chosen name, guessing one is trivial.
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot see it in a listing", func(t *testing.T) {
		listed, err := fixture.teamB.ListSchedules(t.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
		require.NoError(t, err)

		for _, summary := range listed.Msg.GetSchedules() {
			assert.NotEqual(t, name, summary.GetName(), "another tenant's schedule appeared in a listing")
		}
	})

	t.Run("cannot pause it", func(t *testing.T) {
		_, err := fixture.teamB.PauseSchedule(t.Context(),
			connect.NewRequest(&v1.PauseScheduleRequest{Name: name}))
		require.Error(t, err, "another tenant paused a schedule it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot resume it", func(t *testing.T) {
		_, err := fixture.teamB.ResumeSchedule(t.Context(),
			connect.NewRequest(&v1.ResumeScheduleRequest{Name: name}))
		require.Error(t, err, "another tenant resumed a schedule it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot trigger it", func(t *testing.T) {
		// The one with a cost beyond disclosure: a trigger runs somebody else's
		// workload, on their behalf, whenever the caller likes.
		_, err := fixture.teamB.TriggerSchedule(t.Context(),
			connect.NewRequest(&v1.TriggerScheduleRequest{Name: name}))
		require.Error(t, err, "another tenant fired a schedule it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot delete it", func(t *testing.T) {
		_, err := fixture.teamB.DeleteSchedule(t.Context(),
			connect.NewRequest(&v1.DeleteScheduleRequest{Name: name}))
		require.Error(t, err, "another tenant deleted a schedule it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("and may use the same name for its own", func(t *testing.T) {
		// The other half of the boundary, and not merely a positive control: a name
		// is chosen by a person, so if two tenants shared a namespace of names then
		// team B would be told `nightly-report` was taken — which denies them a name
		// and discloses team A in one answer.
		_, err := fixture.teamB.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow("nightly-report"),
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
			Paused:   true,
		}))
		require.NoError(t, err, "a tenant could not use a name another tenant had taken")

		// And it is their own, not a handle on team A's.
		described, err := fixture.teamB.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: name}))
		require.NoError(t, err)
		require.Equal(t, int64(1), described.Msg.GetSchedule().GetInputs()["attempts"].GetLiteral().GetInt64Value(),
			"a tenant's schedule resolved to another tenant's")
	})

	t.Run("and team A's is untouched", func(t *testing.T) {
		// Every refusal above would also be satisfied by a verb that failed *after*
		// doing its work. What matters to the owner is that the schedule is still
		// there, still paused, and still carrying what they gave it.
		described, err := fixture.teamA.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: name}))
		require.NoError(t, err, "the owner lost their own schedule")
		assert.True(t, described.Msg.GetSchedule().GetPaused(), "a refused resume resumed it anyway")
		assert.Zero(t, described.Msg.GetSchedule().GetNumActions(), "a refused trigger fired it anyway")
	})
}

// TestCreateRefusesWhatMustNotFireAtThreeInTheMorning covers the fail-closed half.
//
// Each of these is a mistake that would otherwise be discovered by a worker at the
// cadence's first firing, with nobody there to read it. They are refused while the
// caller is present, which is invariant 6 applied to a surface where "later" can
// mean a week later.
func TestCreateRefusesWhatMustNotFireAtThreeInTheMorning(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	t.Run("a workflow that declares no schedule", func(t *testing.T) {
		workflow := scheduledWorkflow("no-cadence")
		workflow.Triggers = nil

		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: workflow,
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.ErrorContains(t, err, "a schedule needs a cadence")
	})

	t.Run("a cadence that cannot be read", func(t *testing.T) {
		workflow := scheduledWorkflow("bad-cron")
		workflow.Triggers.Schedule.Cron = []string{"0 9 * *"}

		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: workflow,
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.ErrorContains(t, err, "has 4 fields")
	})

	t.Run("a required argument left out", func(t *testing.T) {
		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow("missing-argument"),
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.ErrorContains(t, err, `input "attempts" is required`)
	})

	t.Run("an argument of the wrong type", func(t *testing.T) {
		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow("mistyped-argument"),
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral("three")},
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.ErrorContains(t, err, "is declared int but was given string")
	})

	t.Run("an argument that is an expression the server would evaluate", func(t *testing.T) {
		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow("expression-argument"),
			Inputs:   map[string]*v1.Value{"attempts": v1.NewExpr("1 + 2")},
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.ErrorContains(t, err, "is an expression, and an input is a value")
	})

	t.Run("a name already taken within this tenant", func(t *testing.T) {
		request := &v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow("taken"),
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
			Paused:   true,
		}

		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(request))
		require.NoError(t, err)

		_, err = fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(request))
		require.Error(t, err)
		require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
	})
}

// TestATriggerDeclarationDoesNotStartAnything is the design decision, asserted.
//
// Running a scheduled workflow must create no schedule: a file that starts running
// on its own when it merges is a surprise, and `flow run` never causing one is what
// keeps the declaration and the act separate. Without this, the property is a
// sentence in a doc comment.
func TestATriggerDeclarationDoesNotStartAnything(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	workflow := scheduledWorkflow("declared-not-created")
	workflow.DeclaredInputs = nil

	_, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: workflow}))
	require.NoError(t, err)

	listed, err := fixture.teamA.ListSchedules(t.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
	require.NoError(t, err)
	require.Empty(t, listed.Msg.GetSchedules(), "running a workflow created a schedule")
}

// TestAnIntervalScheduleIsAcceptedToo covers the other cadence spelling end to end,
// since `every:` and `cron:` reach Temporal through different fields of its spec
// and a projection is exactly the kind of code where one arm is written and the
// other is assumed.
func TestAnIntervalScheduleIsAcceptedToo(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	workflow := scheduledWorkflow("every-hour")
	workflow.DeclaredInputs = nil
	workflow.Triggers.Schedule = &v1.ScheduleTrigger{
		Every:  durationpb.New(time.Hour),
		Jitter: durationpb.New(time.Minute),
	}

	created, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: workflow,
	}))
	require.NoError(t, err)

	schedule := created.Msg.GetSchedule()
	require.Equal(t, "every-hour", schedule.GetName())
	assert.Equal(t, time.Hour, schedule.GetTrigger().GetEvery().AsDuration())
	assert.NotEmpty(t, schedule.GetNextRunTimes(), "an interval schedule reports when it next fires")
}
