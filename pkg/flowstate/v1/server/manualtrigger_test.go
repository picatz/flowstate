package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

const manualTestIssuer = "https://issuer.example.com"

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
			AllowedPrincipals: []string{manualTestIssuer + "#oncall@example.com"},
		}},
		Steps: []*v1.Node{{
			Id:   "rotate",
			Kind: &v1.Node_Value{Value: v1.NewLiteral("rotated")},
		}},
	}
}

// TestManualStartHandlersUseIssuerQualifiedPrincipals is the two-issuer
// collision at both production creation boundaries. The subjects are identical;
// only the authenticated issuer differs. Replacing either handler argument with
// identity.GetSubject(), or comparing only the suffix after '#', admits issuer B
// and fails this test.
func TestManualStartHandlersUseIssuerQualifiedPrincipals(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal)

	workflow := &v1.Workflow{
		Name:    "issuer-scoped-manual-start",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			AllowedPrincipals: []string{"https://issuer-a.example.com#runner"},
		}},
		Steps: []*v1.Node{{
			Id: "mutation",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{
				Name: "mutate",
			}}}},
		}},
	}
	issuerA := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer-a.example.com", Subject: "runner",
	})
	issuerB := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer-b.example.com", Subject: "runner",
		Claims: map[string]any{"access_token": "must-not-appear"},
	})

	for _, test := range []struct {
		name string
		call func(context.Context, string) error
	}{
		{
			name: "Run",
			call: func(ctx context.Context, _ string) error {
				_, err := flowstate.Run(ctx, connect.NewRequest(&v1.RunRequest{Workflow: workflow}))
				return err
			},
		},
		{
			name: "SignalWithStart",
			call: func(ctx context.Context, key string) error {
				_, err := flowstate.SignalWithStart(ctx, connect.NewRequest(&v1.SignalWithStartRequest{
					EntityKey: key,
					Workflow:  workflow,
					Name:      "mutate",
				}))
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, test.call(issuerA, "manual-qualified-allowed"),
				"issuer A's exact qualified principal was refused")
			err := test.call(issuerB, "manual-qualified-denied")
			require.Error(t, err, "issuer B reused issuer A's allowed subject")
			assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
			assert.Contains(t, err.Error(), "https://issuer-b.example.com#runner")
			assert.NotContains(t, err.Error(), "must-not-appear",
				"manual-start denial leaked an arbitrary verified claim")
		})
	}

	mtls := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "flowstate:mtls/mesh", Subject: "spiffe://example.test/ns/ops/sa/runner",
	})
	mtlsWorkflow := proto.Clone(workflow).(*v1.Workflow)
	mtlsWorkflow.GetTriggers().GetManual().AllowedPrincipals = []string{
		"flowstate:mtls/mesh#spiffe://example.test/ns/ops/sa/runner",
	}
	_, err := flowstate.Run(mtls, connect.NewRequest(&v1.RunRequest{Workflow: mtlsWorkflow}))
	require.NoError(t, err, "an mTLS principal's configured issuer and SAN-derived subject did not form its stable ID")

	anonymousWorkflow := proto.Clone(workflow).(*v1.Workflow)
	anonymousWorkflow.GetTriggers().GetManual().AllowedPrincipals = []string{
		auth.AnonymousIssuer + "#" + auth.AnonymousSubject,
	}
	_, err = flowstate.Run(auth.ContextWithPrincipal(t.Context(), auth.AnonymousPrincipal()),
		connect.NewRequest(&v1.RunRequest{Workflow: anonymousWorkflow}))
	require.Error(t, err, "the insecure anonymous development identity satisfied a manual-start allowlist")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
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
	flowstate := mustNew(t, temporal)

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

// TestRunCannotRemoveADeploymentOwnedManualPolicy covers the trust boundary:
// the request names the registered workload but removes its manual restriction.
// Authorization must still consult the server's copy, or the restriction is an
// assertion made by the attacker it is intended to restrict.
func TestRunCannotRemoveADeploymentOwnedManualPolicy(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	trusted := narrowedWorkflow()
	flowstate := mustNew(t, temporal, server.WithTrustedWorkflows("", trusted))

	modified := narrowedWorkflow()
	modified.Triggers.Manual = nil
	_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: modified,
		Reason:   "trying to bypass the deployment policy",
	}))

	require.Error(t, err, "removing manual policy from the submitted copy bypassed authorization")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "oncall@example.com")
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
	flowstate := mustNew(t, temporal)

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
