package server_test

import (
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
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
		// Derived from the same two inputs the server derived it from, and checked
		// as a *prefix*: Temporal appends the scheduled time to keep firings
		// distinct, so the claim is that a firing's workflow id begins with this
		// tenant's schedule id — not merely that the string turns up somewhere in
		// it, which a suffix, an infix, or a wrapping in some other id would
		// satisfy just as well.
		scheduleID := server.ScheduleIDForTest(teamANamespace, "nightly-report")
		require.True(t, strings.HasPrefix(workflowID, scheduleID),
			"a scheduled run's workflow id %q is not derived from its tenant's schedule id %q", workflowID, scheduleID)

		// And the tenant half spelled out, because the assertion above is built
		// from the same function the server used: a derivation that stopped
		// scoping by tenant altogether would agree with itself and pass. This is
		// the one place the encoding is written down twice on purpose.
		require.True(t, strings.HasPrefix(workflowID, "flowstate-schedule-"+teamANamespace+"_"),
			"a scheduled run's workflow id %q is not scoped to its tenant", workflowID)

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

// TestTwoTenantsSchedulesOfOneNameCannotCollide is the property the tenant-scoped
// id exists for, asserted in the direction that can actually fail.
//
// TestAScheduleIsCreatedDescribedFiredAndDeleted asserts that team A's schedule
// derives an id carrying team A — the positive direction, which an encoding that
// had stopped separating tenants entirely would also satisfy, since every tenant's
// id would carry every tenant's name. What makes this a tenancy test is the other
// direction: two tenants who both call their schedule `nightly-report` — and they
// will, because a person chooses that name — must land on ids that cannot be one
// another's, and no name team A is free to choose may derive team B's id.
//
// That last clause is the env secret provider's bug asked about this encoding.
// There, `prefix + NAMESPACE + "_" + name` let three tenants resolve to one
// variable because every character legal in a namespace was legal in a name. Here
// the same concatenation is safe *only* because [auth.ValidateNamespace] forbids
// an underscore in a namespace, so the separator cannot be forged from the name
// side and cannot be reached across from the namespace side. The unit-level
// version of this is TestScheduleIDsAreUnambiguous; this is the same claim about
// the ids two real tenants' schedules actually occupy in a real cluster.
func TestTwoTenantsSchedulesOfOneNameCannotCollide(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	const name = "nightly-report"

	for tenant, tenantServer := range map[string]*server.FlowstateServer{
		teamANamespace: fixture.teamA,
		teamBNamespace: fixture.teamB,
	} {
		_, err := tenantServer.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow(name),
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
			Paused:   true,
		}))
		require.NoError(t, err, "%s could not create a schedule of a name another tenant had taken", tenant)
	}

	idA := server.ScheduleIDForTest(teamANamespace, name)
	idB := server.ScheduleIDForTest(teamBNamespace, name)

	require.NotEqual(t, idA, idB, "two tenants' schedules of one name derive one id")
	assert.NotContains(t, idA, teamBNamespace, "a tenant's schedule id names another tenant")
	assert.NotContains(t, idB, teamANamespace, "a tenant's schedule id names another tenant")

	// A cannot reach B, whatever it calls its own schedule. Each candidate is a
	// name team A is free to choose that spells some part of team B's half of the
	// id, which is exactly how the env provider's tenants collided.
	for _, candidate := range []string{
		name,
		teamBNamespace + "_" + name,
		"_" + teamBNamespace + "_" + name,
		"_" + name,
		idB,
	} {
		assert.NotEqual(t, idB, server.ScheduleIDForTest(teamANamespace, candidate),
			"%q derived %q's schedule id from %q's namespace", candidate, teamBNamespace, teamANamespace)
	}

	// And the cluster holds two schedules rather than one shared one — the claim
	// above is about the derivation, this is about what it actually created. A
	// collision would show up here as a single id.
	//
	// Listed rather than described, because a description is asked for by name
	// and would answer from the same derivation this is checking. Temporal's
	// schedule listing is served from visibility and lags a create, hence the
	// wait; the assertion that reports what was actually found runs afterwards,
	// on the test's own goroutine, since a `require` inside a polled condition
	// is a `FailNow` from somewhere it cannot stop the test.
	//
	// scheduleIDs reads only ids this deployment created — the Temporal
	// namespace belongs to this test, but the prefix is what the server's own
	// listing scopes by, so the count below cannot be thrown off by anything
	// else that appears there.
	scheduleIDs := func() []string {
		var ids []string

		list, err := fixture.temporal.ScheduleClient().List(t.Context(), client.ScheduleListOptions{})
		if err != nil {
			return nil
		}
		for list.HasNext() {
			entry, err := list.Next()
			if err != nil {
				return nil
			}
			if strings.HasPrefix(entry.ID, "flowstate-schedule-") {
				ids = append(ids, entry.ID)
			}
		}

		return ids
	}

	require.Eventually(t, func() bool {
		return len(scheduleIDs()) == 2
	}, 30*time.Second, 200*time.Millisecond,
		"the cluster does not hold two schedules, one per tenant")
	require.ElementsMatch(t, []string{idA, idB}, scheduleIDs(),
		"two tenants' schedules of one name did not land on one id each")

	// Each is its owner's and no one else's, asked through the RPC surface rather
	// than the id: a listing is what a tenant is actually shown.
	for tenant, tenantServer := range map[string]*server.FlowstateServer{
		teamANamespace: fixture.teamA,
		teamBNamespace: fixture.teamB,
	} {
		listed, err := tenantServer.ListSchedules(t.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
		require.NoError(t, err)
		require.Len(t, listed.Msg.GetSchedules(), 1,
			"%s was shown a schedule that is not its own", tenant)
		assert.Equal(t, name, listed.Msg.GetSchedules()[0].GetName())
	}
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

// TestBoundedRecoveryControlsReachTemporal is the honesty check on the knobs the
// bounded recovery controls added: each one is a projection onto a real Temporal
// feature, and a projection is exactly the kind of code that compiles while
// carrying nothing.
//
// It asks a real cluster, because that is the only thing that can answer. A unit
// test on [scheduleSpecOf] would prove the struct was filled in, and the defect
// this test exists to catch was a struct filled in *correctly by its own lights*:
// an unset `end_at` copied through `(*timestamppb.Timestamp)(nil).AsTime()`, which
// is 1970 rather than the zero time the SDK reads as "no end". The schedule was
// created, reported success, and had no future firing. So the assertion that
// matters is the one about the times the cluster says it will next fire.
func TestBoundedRecoveryControlsReachTemporal(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	// A window wide enough that the schedule is live now and firings remain ahead
	// of it, since a schedule whose window has closed reports no next firing for a
	// legitimate reason and would make this test say nothing.
	start := time.Now().UTC().Add(-time.Hour)
	end := start.Add(365 * 24 * time.Hour)

	workflow := scheduledWorkflow("bounded-recovery")
	workflow.DeclaredInputs = nil
	workflow.Triggers.Schedule = &v1.ScheduleTrigger{
		// A calendar and nothing else, so this also proves a calendar alone is a
		// cadence Temporal accepts: every second hour between 09:00 and 17:00.
		Calendars: []*v1.ScheduleTrigger_Calendar{{
			Hour:    []*v1.ScheduleTrigger_Calendar_Range{{Start: 9, End: 17, Step: 2}},
			Minute:  []*v1.ScheduleTrigger_Calendar_Range{{Start: 0}},
			Comment: "office hours",
		}},
		StartAt:        timestamppb.New(start),
		EndAt:          timestamppb.New(end),
		CatchupWindow:  durationpb.New(6 * time.Hour),
		PauseOnFailure: true,
		Overlap:        v1.ScheduleTrigger_OVERLAP_SKIP,
	}

	created, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: workflow,

		// A backfill of a window that was missed, at the count and span the bounds
		// allow, so the path that hands ranges to Temporal is exercised rather than
		// merely validated.
		Backfill: []*v1.ScheduleBackfill{{
			StartAt: timestamppb.New(start.Add(-48 * time.Hour)),
			EndAt:   timestamppb.New(start.Add(-24 * time.Hour)),
			Overlap: v1.ScheduleTrigger_OVERLAP_SKIP,
		}},

		// Paused, so the backfilled firings do not race the assertions below. A
		// paused schedule still reports the times it *would* fire, which is what is
		// being read.
		Paused: true,
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, created.Msg.GetSchedule().GetNextRunTimes(),
		"a schedule with a calendar and a window still has firings ahead of it")

	described, err := fixture.teamA.DescribeSchedule(t.Context(),
		connect.NewRequest(&v1.DescribeScheduleRequest{Name: "bounded-recovery"}))
	require.NoError(t, err)

	// Read back out of the stored specification, the way the cron case is: asking
	// Temporal what a cadence is answers in its own vocabulary, and what an operator
	// asked for is what a describe should say.
	trigger := described.Msg.GetSchedule().GetTrigger()
	require.Len(t, trigger.GetCalendars(), 1)
	assert.Equal(t, int32(17), trigger.GetCalendars()[0].GetHour()[0].GetEnd())
	assert.Equal(t, int32(2), trigger.GetCalendars()[0].GetHour()[0].GetStep())
	assert.Equal(t, 6*time.Hour, trigger.GetCatchupWindow().AsDuration())
	assert.True(t, trigger.GetPauseOnFailure())

	// Every next firing falls inside the declared window and on the calendar. The
	// hour check is what proves the calendar reached the cluster rather than being
	// dropped on the way: a schedule with no spec at all also reports firings.
	next := described.Msg.GetSchedule().GetNextRunTimes()
	require.NotEmpty(t, next, "a live schedule reports when it next fires")
	for _, at := range next {
		assert.True(t, at.AsTime().After(start), "a firing before the declared start: %s", at.AsTime())
		assert.True(t, at.AsTime().Before(end), "a firing after the declared end: %s", at.AsTime())
		assert.Contains(t, []int{9, 11, 13, 15, 17}, at.AsTime().UTC().Hour(),
			"a firing at an hour the calendar does not name: %s", at.AsTime())
	}
}

// TestABackfillBeyondItsBoundsIsRefusedByTheServer is the other half of
// [TestBoundedRecoveryControlsReachTemporal]: the bounds are the server's, not the
// CLI's.
//
// `flow schedule create` checks a backfill before sending it, which is the right
// place for the message an operator reads. It is not a bound: the RPC is public,
// and a caller that is not the CLI is the caller a bound exists for. This asserts
// the refusal comes from the server, over the wire, with the code that says the
// request was wrong rather than the cluster.
func TestABackfillBeyondItsBoundsIsRefusedByTheServer(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	now := time.Now().UTC()
	workflow := scheduledWorkflow("too-much-history")
	workflow.DeclaredInputs = nil

	t.Run("more history than the span allows", func(t *testing.T) {
		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: workflow,
			Backfill: []*v1.ScheduleBackfill{{
				StartAt: timestamppb.New(now.Add(-(v1.MaxScheduleBackfillSpan + time.Hour))),
				EndAt:   timestamppb.New(now),
			}},
		}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		assert.ErrorContains(t, err, "more than 744h0m0s of history")
	})

	t.Run("more ranges than the count allows", func(t *testing.T) {
		var backfill []*v1.ScheduleBackfill
		for i := range v1.MaxScheduleBackfills + 1 {
			at := now.Add(-time.Duration(i+1) * time.Hour)
			backfill = append(backfill, &v1.ScheduleBackfill{
				StartAt: timestamppb.New(at), EndAt: timestamppb.New(at.Add(time.Minute)),
			})
		}

		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: workflow,
			Backfill: backfill,
		}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("a calendar nobody could have meant", func(t *testing.T) {
		refused := scheduledWorkflow("impossible-calendar")
		refused.DeclaredInputs = nil
		refused.Triggers.Schedule = &v1.ScheduleTrigger{
			Calendars: []*v1.ScheduleTrigger_Calendar{{Month: []*v1.ScheduleTrigger_Calendar_Range{{Start: 13}}}},
		}

		_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: refused,
		}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		assert.ErrorContains(t, err, "outside 1-12")
	})

	// Nothing above may have created a schedule: a refusal that half-creates is
	// worse than either answer, and the listing is the only thing that can say.
	listed, err := fixture.teamA.ListSchedules(t.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
	require.NoError(t, err)
	assert.Empty(t, listed.Msg.GetSchedules(), "a refused creation left a schedule behind")
}

// trustedScheduledWorkflow and attackerScheduledWorkflow share a name and a
// cadence, and differ only in what their one step produces — which is exactly
// what lets [TestCreateScheduleExecutesTheTrustedCopyNotTheSubmittedOne] tell
// which specification actually ran.
func trustedScheduledWorkflow(name string) *v1.Workflow {
	wf := scheduledWorkflow(name)
	wf.Steps = []*v1.Node{{Id: "report", Kind: &v1.Node_Value{Value: v1.NewLiteral("trusted")}}}
	return wf
}

func attackerScheduledWorkflow(name string) *v1.Workflow {
	wf := scheduledWorkflow(name)
	wf.Steps = []*v1.Node{{Id: "report", Kind: &v1.Node_Value{Value: v1.NewLiteral("attacker")}}}
	return wf
}

// TestCreateScheduleExecutesTheTrustedCopyNotTheSubmittedOne is CreateSchedule's
// half of the trust boundary [server.WithTrustedWorkflows] establishes: a
// caller naming a registered workflow must have their submission replaced by the
// deployment's own copy, the same as [FlowstateServer.Run] and
// [FlowstateServer.SignalWithStart] already are. Without it, a caller could
// register a schedule trigger on their own copy of a trusted, `manual: denied`
// workflow and have this handler create — and eventually fire — the
// caller-controlled specification under the trusted name.
func TestCreateScheduleExecutesTheTrustedCopyNotTheSubmittedOne(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	trusted := trustedScheduledWorkflow("rotate-keys")
	flowstate := mustNew(t, temporal, server.WithTrustedWorkflows("", trusted))

	created, err := flowstate.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		// The caller's own copy: same name, same cadence, a step that would
		// prove the caller's specification ran rather than the deployment's.
		Workflow: attackerScheduledWorkflow("rotate-keys"),
		Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
		Paused:   true,
	}))
	require.NoError(t, err)
	require.Equal(t, "rotate-keys", created.Msg.GetSchedule().GetName())

	_, err = flowstate.TriggerSchedule(t.Context(),
		connect.NewRequest(&v1.TriggerScheduleRequest{Name: "rotate-keys"}))
	require.NoError(t, err)

	var workflowID string
	require.Eventually(t, func() bool {
		described, err := flowstate.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: "rotate-keys"}))
		if err != nil || len(described.Msg.GetSchedule().GetRecentRuns()) == 0 {
			return false
		}
		workflowID = described.Msg.GetSchedule().GetRecentRuns()[0].GetWorkflowId()
		return workflowID != ""
	}, 60*time.Second, 200*time.Millisecond, "the schedule never took an action")

	var out v1.Workflow_StepOutputs
	require.Eventually(t, func() bool {
		return temporal.GetWorkflow(t.Context(), workflowID, "").Get(t.Context(), &out) == nil
	}, 60*time.Second, 200*time.Millisecond, "the run the schedule started never completed")

	assert.Equal(t, "trusted",
		out.GetStepValues()["report"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetStringValue(),
		"CreateSchedule executed the caller's submitted copy instead of the deployment's trusted one")
}
