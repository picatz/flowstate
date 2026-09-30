package server_test

import (
	"io"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// gatedOn is a workflow whose signal gate reads its subject from its
// `approver` input, declared sensitive or not.
func gatedOn(sensitive bool) *v1.Workflow {
	return &v1.Workflow{
		Name:           "gated-subject",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "approver", Type: v1.InputDeclaration_TYPE_STRING, Required: true, Sensitive: sensitive}},
		Steps: []*v1.Node{{Id: "wait", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "approved"}},
			Timeout: durationpb.New(time.Hour),
		}}}},
		Triggers: &v1.Triggers{Schedule: &v1.ScheduleTrigger{Every: durationpb.New(time.Hour)}},
		Signals: map[string]*v1.SignalPolicy{"approved": {
			DistinctFromStarter: true,
			Allow:               []*v1.SignalPolicyRule{{SubjectFrom: v1.NewExpr("inputs.approver")}},
		}},
	}
}

// TestADeploymentCopyWithholdsWhatItsSubjectRefusalQuotes is #2100 at the
// server, both ways. A deployment-owned copy that declares `approver`
// sensitive runs in place of a submitted file that does not, so the client
// holds no declaration to redact the refusal against, and the server withholds
// the value itself. A run of the submitted workflow leaves it to the client,
// which holds the same declarations and honours `--reveal-sensitive`, as the
// local driver does.
func TestADeploymentCopyWithholdsWhatItsSubjectRefusalQuotes(t *testing.T) {
	t.Parallel()

	const approver = `approver-"lead"@corp.example`
	for name, test := range map[string]struct {
		trusted, submittedSensitive, withheld bool
		// defaulted, when set, has both copies default `approver` to it and
		// the caller send none: the deployment's to the value, the caller's
		// to another.
		defaulted bool
	}{
		"a deployment-owned default the caller never held":  {trusted: true, submittedSensitive: true, defaulted: true, withheld: true},
		"a deployment-owned copy":                           {trusted: true, withheld: true},
		"a deployment-owned copy the submitter agrees with": {trusted: true, submittedSensitive: true},
		"the submitted workflow":                            {submittedSensitive: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			temporal, _ := newTemporalNamespace(t)
			deployed, submitted := gatedOn(true), gatedOn(test.submittedSensitive)
			inputs := map[string]*v1.Value{"approver": v1.NewLiteral(approver)}
			if test.defaulted {
				deployed.DeclaredInputs[0].Required, deployed.DeclaredInputs[0].Default = false, v1.NewLiteral(approver)
				submitted.DeclaredInputs[0].Required, submitted.DeclaredInputs[0].Default = false, v1.NewLiteral("someone#else")
				inputs = nil
			}
			var opts []server.Option
			if test.trusted {
				opts = append(opts, server.WithTrustedWorkflows("", deployed))
			}
			flowstate := mustNew(t, temporal, opts...)

			for rpc, submit := range map[string]func() error{
				"Run": func() error {
					_, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: submitted, Inputs: inputs}))
					return err
				},
				"SignalWithStart": func() error {
					_, err := flowstate.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
						EntityKey: "order-1", Name: "approved", Workflow: submitted, Inputs: inputs,
					}))
					return err
				},
				"CreateSchedule": func() error {
					_, err := flowstate.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{Workflow: submitted, Inputs: inputs}))
					return err
				},
			} {
				err := submit()
				require.Error(t, err, rpc)
				require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "%s: %v", rpc, err)
				require.Contains(t, err.Error(), "<issuer>#<subject>", "%s: the refusal no longer says what was wrong", rpc)
				if test.withheld {
					assert.NotContains(t, err.Error(), "lead", "%s: the server quoted what its own copy declares sensitive", rpc)

					continue
				}
				assert.Contains(t, err.Error(), "lead", "%s: the server withheld what the client can redact, or reveal, itself", rpc)
			}
		})
	}
}

// TestAWebhookDeliveryWithholdsWhatItsSubjectRefusalQuotes: a webhook's
// sender holds no Flowfile to redact the refusal against, so the server
// withholds a sensitive input its deployment's workflow resolved into a bare
// subject before the delivery is answered.
func TestAWebhookDeliveryWithholdsWhatItsSubjectRefusalQuotes(t *testing.T) {
	t.Parallel()

	gated := gatedOn(true)
	gated.Profile = v1.CurrentProfile
	gated.Triggers = &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
		Name: "approvals",
		Verify: map[string]*v1.Value{
			v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
				SecretRef: &v1.SecretRef{Scheme: "env", Name: "APPROVALS_WEBHOOK_SECRET"},
			}},
		},
		IdempotencyKey: v1.NewExpr(`event.body.id`),
		Arguments:      map[string]*v1.Value{"approver": v1.NewExpr(`event.body.approver`)},
	}}}

	receiver, err := mustNew(t, nil).NewWebhookReceiver(t.Context(), "", []*v1.Workflow{gated}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	body := `{"id":"evt_1","approver":"approver-\"lead\"@corp.example"}`
	resp := deliver(t, receiver, "/webhooks/gated-subject/approvals", body, signed)
	answer, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(answer), "<issuer>#<subject>", "the delivery was not refused at its signal policy, so this proves nothing: %d %s", resp.StatusCode, answer)
	assert.NotContains(t, string(answer), "lead", "the delivery's answer quoted a sensitive input")
}
