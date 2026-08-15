package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// `manual:` enforced by the handler, and the trigger it records on the run.
//
// The enforcement belongs here and nowhere else, which is the whole design: a
// `${trigger.principal == "admin"}` in a step's `if:` is an expression the workflow
// evaluates about itself, where this is the server deciding against an identity it
// attested, before Temporal sees anything. So the test that matters is that the
// *handler* asks — the same lesson `Run` learned once already about specification
// validation, which held only because the CLI happened to install an interceptor.

// narrowedWorkflow refuses a manual start unless the caller is one named subject
// and says why.
func narrowedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "break-glass",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			RequireReason:     true,
			AllowedPrincipals: []string{"oncall@example.com"},
		}},
		Steps: []*v1.Node{{
			Id:   "rotate",
			Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")},
		}},
	}
}

// TestRunRefusesAManualStartTheWorkflowNarrowedAway is the refusal, from outside.
//
// Nothing is started, which is the claim: the run that may not happen does not
// happen, while the caller is still present to be told why. Reported as
// PermissionDenied rather than InvalidArgument, because the request is
// well-formed — it is the caller who is not permitted, and a caller told
// "invalid argument" would go looking at their arguments.
func TestRunRefusesAManualStartTheWorkflowNarrowedAway(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := server.New(temporal)

	for _, test := range []struct {
		name     string
		workflow *v1.Workflow
		reason   string
		contains string
	}{
		{
			// The test server attests an anonymous caller (no identity provider),
			// so this is also the empty-subject case: a policy naming somebody
			// must not admit nobody in particular, or it admits everyone.
			name:     "a principal outside the allowed set",
			workflow: narrowedWorkflow(),
			reason:   "rotating the leaked key",
			contains: "oncall@example.com",
		},
		{
			name:     "no reason where the workflow requires one",
			workflow: reasonOnlyWorkflow(),
			contains: "requires a reason",
		},
		{
			name:     "a workflow that refuses manual starts outright",
			workflow: deniedWorkflow(),
			reason:   "because I said so",
			contains: "`manual: denied`",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
				Workflow: test.workflow,
				Reason:   test.reason,
			}))

			require.Error(t, err, "a manual start the workflow narrowed away was accepted")
			assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err),
				"reported as something other than a permission decision: %v", err)
			assert.Contains(t, err.Error(), test.contains,
				"the refusal does not say which rule refused: %v", err)
		})
	}
}

// reasonOnlyWorkflow narrows on the reason alone, so the reason can be tested
// without the principal check refusing first.
func reasonOnlyWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:     "reason-required",
		Profile:  v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{RequireReason: true}},
		Steps: []*v1.Node{{
			Id:   "rotate",
			Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")},
		}},
	}
}

// deniedWorkflow refuses manual starts and declares a schedule that does start it,
// so the refusal has an alternative to name — a refusal with none would be a
// separate mistake, which the compiler reports.
func deniedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "scheduled-only",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{
			Manual:   &v1.ManualTrigger{Denied: true},
			Schedule: &v1.ScheduleTrigger{Cron: []string{"0 2 * * *"}},
		},
		Steps: []*v1.Node{{
			Id:   "sweep",
			Kind: &v1.Node_Value{Value: v1.NewLiteral("swept")},
		}},
	}
}

// TestAManualStartWithAReasonRunsAndReportsItsTrigger is the accepted direction,
// end to end, and the half that proves the feature is reachable rather than merely
// enforced.
//
// A permitted caller starts the run, and the run reads how it started: the server
// records the context in `RunState`, the durable driver carries it, and the
// workflow's own declared output answers `manual`. That path — request to
// expression — is the one no unit test covers, and it is where a field silently
// failing to be recorded would look exactly like a workflow that computed
// something else.
func TestAManualStartWithAReasonRunsAndReportsItsTrigger(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	flowstate := server.New(temporal)

	spec := &v1.Workflow{
		Name:     "reason-required",
		Profile:  v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{RequireReason: true}},
		Steps: []*v1.Node{{
			Id:   "rotate",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`"rotated by a " + trigger.kind + " start"`)},
		}},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "started_by", Value: v1.NewExpr("trigger.kind")},
			{Name: "source", Value: v1.NewExpr("trigger.name")},
		},
	}

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: spec,
		Reason:   "rotating the leaked signing key",
	}))
	require.NoError(t, err, "a permitted manual start with a reason was refused")

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		resp, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: started.Msg.GetWorkflowId(),
		}))
		if err != nil || resp.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = resp

		return true
	}, 30*time.Second, 100*time.Millisecond, "the run did not finish")

	outputs := final.Msg.GetRunOutputs().GetValues()

	assert.Equal(t, v1.TriggerKindManual, outputs["started_by"].GetLiteral().GetStringValue(),
		"the server did not record how the run started, so `${trigger.kind}` answered with a blank")
	assert.Empty(t, outputs["source"].GetLiteral().GetStringValue(),
		"a manual start has no declared source to name, so this is empty rather than invented")
}
