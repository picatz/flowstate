package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

func logStep(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "log",
		Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
	}}}
}

// TestSubmitRefusesStepIDsThatBreakTheScopeRules is the server's half of #1430. The
// submit boundary ran protovalidate alone, so a hand-built specification could
// carry a step named for a root, reuse a step id, or name a step `1a`, and run.
// Run and CreateSchedule are the two doors that bring durable work into being,
// and both must refuse each of them before touching Temporal, with the offending
// id named, in the same words.
func TestSubmitRefusesStepIDsThatBreakTheScopeRules(t *testing.T) {
	t.Parallel()

	s := mustNew(t, nil, server.WithNamespace("team-a"))

	for name, tc := range map[string]struct {
		steps []*v1.Node
		want  string
	}{
		"a step named for a root": {steps: []*v1.Node{logStep("vars")}, want: "vars"},
		"a duplicate id":          {steps: []*v1.Node{logStep("x"), logStep("x")}, want: `duplicate id "x"`},
		"an id starting with a digit": {
			// The schema's own pattern refuses this one, naming the field.
			steps: []*v1.Node{logStep("1a")}, want: "steps[0].id",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			workflow := func(suffix string) *v1.Workflow {
				return &v1.Workflow{
					Name:     "step-ids-" + suffix,
					Steps:    tc.steps,
					Triggers: &v1.Triggers{Schedule: &v1.ScheduleTrigger{Cron: []string{"0 * * * *"}}},
				}
			}

			_, runErr := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: workflow("run")}))
			require.Error(t, runErr, "Run accepted the specification")
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(runErr))
			require.ErrorContains(t, runErr, tc.want, "the refusal must name the step")

			_, scheduleErr := s.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
				Workflow: workflow("schedule"),
			}))
			require.Error(t, scheduleErr, "CreateSchedule persisted a schedule that could never fire")
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(scheduleErr))
			require.ErrorContains(t, scheduleErr, tc.want)
		})
	}
}
