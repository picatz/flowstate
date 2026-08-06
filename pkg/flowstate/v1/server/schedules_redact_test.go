package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// sensitiveScheduledWorkflow declares two bound inputs, one marked
// `sensitive: true` and one not, on a workflow that also fires — mirroring
// scheduledWorkflow, but with a value worth checking is never rendered.
func sensitiveScheduledWorkflow(name string) *v1.Workflow {
	return &v1.Workflow{
		Name: name,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "api_token", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true, Required: true},
			{Name: "region", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
		},
		Triggers: &v1.Triggers{
			Schedule: &v1.ScheduleTrigger{
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

// theSecretValue is the value that must never survive to a rendered
// [v1.ScheduleDescription] — checked as the literal string, not just as the
// absence of the plaintext value under its own marker, because a fix that hides
// the marker but still ships the value elsewhere in the message would still pass
// a check that only looked for the marker.
const theSecretValue = "sk-live-should-never-be-printed"

// TestDescribeScheduleRedactsSensitiveInputs is issue #211: a schedule holds its
// bound inputs persistently, for as long as the schedule exists, which makes this
// the worst place in the system to leak one — a run's `Get` carries no inputs at
// all (see [v1.GetResponse]), so `DescribeSchedule` is the one call site in the
// server that renders a bound input value in the first place.
//
// Both `DescribeSchedule` and `ListSchedules` route through the same
// `describeSchedule`, so this covers both without a second test — see that
// function's own comment.
func TestDescribeScheduleRedactsSensitiveInputs(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: sensitiveScheduledWorkflow("token-rotation"),
		Inputs: map[string]*v1.Value{
			"api_token": v1.NewLiteral(theSecretValue),
			"region":    v1.NewLiteral("eu-west-1"),
		},
		Paused: true,
	}))
	require.NoError(t, err)

	described, err := fixture.teamA.DescribeSchedule(t.Context(),
		connect.NewRequest(&v1.DescribeScheduleRequest{Name: "token-rotation"}))
	require.NoError(t, err)

	schedule := described.Msg.GetSchedule()

	// The redacted entry reads as the marker PR #212 established
	// (`[redacted: <name>]`), not something this branch invented.
	assert.Equal(t, "[redacted: api_token]",
		schedule.GetInputs()["api_token"].GetLiteral().GetStringValue())

	// The over-redaction direction: an unmarked input must be unaffected, so a fix
	// that just hides every input does not pass this test.
	assert.Equal(t, "eu-west-1", schedule.GetInputs()["region"].GetLiteral().GetStringValue())

	// The real check: the secret string is absent from the wire bytes entirely —
	// not merely "hidden behind the marker at the name we happened to check" but
	// genuinely gone, in both the binary and the JSON encodings this message
	// travels in. A bug that redacted the string in one place and left a second
	// copy elsewhere in the message would pass an assertion on schedule.Inputs
	// alone and fail this one.
	wire, err := proto.Marshal(described.Msg)
	require.NoError(t, err)
	assert.NotContains(t, string(wire), theSecretValue,
		"the secret value must not appear anywhere in the wire-encoded response")

	// ListSchedules does not return per-schedule inputs today (only
	// DescribeSchedule/CreateSchedule do), but it shares describeSchedule, so a
	// regression there would show up here too if that ever changes; nothing more
	// to assert on List for this issue.
}

// TestDescribeScheduleFailsClosedWhenSpecIsUnavailable is CLAUDE.md's fail-closed
// rule applied to the one case describeSchedule already treats as "not known":
// `storedRunState` returns nil for anything it cannot decode — a schedule created
// by a build whose message shape has since moved, or (as simulated here) a
// firing whose stored argument this server cannot parse as a [*v1.RunState] at
// all. With no specification to consult, nothing says which bound inputs, if
// any, are sensitive — so today, and after this change, DescribeSchedule reports
// no inputs at all for that schedule, matching the existing "silent, best
// effort" comment in describeSchedule: everything else about the schedule
// (its name, pause state, next-run times) still describes normally, only the
// stored workflow name and inputs come back empty. That was already true before
// this change (`reported.Inputs` was simply never set), and this test locks it
// in as the fail-closed answer directly, along with the fact that a corrupted
// stored argument can never make redactInputs "guess" a value into the clear.
func TestDescribeScheduleFailsClosedWhenSpecIsUnavailable(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	created, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: sensitiveScheduledWorkflow("undecodable"),
		Inputs: map[string]*v1.Value{
			"api_token": v1.NewLiteral(theSecretValue),
			"region":    v1.NewLiteral("eu-west-1"),
		},
		Paused: true,
	}))
	require.NoError(t, err)
	require.True(t, created.Msg.GetSchedule().GetPaused())

	// Find the Temporal-side schedule id this server chose. The encoding is
	// deliberately unexported (scheduleIDFor), so this test does what any other
	// caller would: it lists what Temporal has and finds the one this test just
	// created, rather than reimplementing the id scheme.
	var scheduleID string
	list, err := fixture.temporal.ScheduleClient().List(t.Context(), client.ScheduleListOptions{})
	require.NoError(t, err)
	for list.HasNext() {
		entry, err := list.Next()
		require.NoError(t, err)
		if idHasSuffix(entry.ID, "_undecodable") {
			scheduleID = entry.ID
			break
		}
	}
	require.NotEmpty(t, scheduleID, "could not find the schedule this test just created")

	handle := fixture.temporal.ScheduleClient().GetHandle(t.Context(), scheduleID)

	// Corrupt the stored action's argument so storedRunState cannot decode it as
	// a *v1.RunState: a plain string is a legal Temporal payload and an illegal
	// RunState.
	err = handle.Update(t.Context(), client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			action, ok := input.Description.Schedule.Action.(*client.ScheduleWorkflowAction)
			require.True(t, ok)

			corrupted := *action
			corrupted.Args = []any{"not a run state"}

			return &client.ScheduleUpdate{
				Schedule: &client.Schedule{
					Action: &corrupted,
					Spec:   input.Description.Schedule.Spec,
					Policy: input.Description.Schedule.Policy,
					State:  input.Description.Schedule.State,
				},
			}, nil
		},
	})
	require.NoError(t, err)

	described, err := fixture.teamA.DescribeSchedule(t.Context(),
		connect.NewRequest(&v1.DescribeScheduleRequest{Name: "undecodable"}))
	require.NoError(t, err)

	schedule := described.Msg.GetSchedule()

	// Fail closed: no specification to consult means nothing is reported, not
	// "reported in the clear because nothing said not to."
	assert.Empty(t, schedule.GetInputs(), "an undecodable spec must not report any inputs")
	assert.Empty(t, schedule.GetWorkflowName(), "an undecodable spec must not report a workflow name either")

	wire, err := proto.Marshal(described.Msg)
	require.NoError(t, err)
	assert.NotContains(t, string(wire), theSecretValue)
}

func idHasSuffix(id, suffix string) bool {
	if len(id) < len(suffix) {
		return false
	}
	return id[len(id)-len(suffix):] == suffix
}
