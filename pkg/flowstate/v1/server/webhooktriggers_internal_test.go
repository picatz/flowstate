package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A webhook's rules were reachable from a Flowfile and from nowhere else.
//
// `flow validate` refused a literal signing key against a line and a column;
// `Run` and `SignalWithStart` accepted the identical specification, because
// neither `v1.Validate` nor the shared submission pipeline asked
// `v1.CheckWebhookTriggers` anything. protovalidate has no opinion about which
// `Value.kind` a `verify:` entry holds, so `verify: {hmac-sha256: "whsec_live_…"}`
// satisfied the schema, passed submission, and was written into Temporal history
// with the specification — durable and broadly readable, which is the one place
// invariant 8 says a secret must never reach.
//
// These are internal because the interesting assertion is about the function every
// submit path shares, which is reachable without a Temporal server.

// triggeringWorkflow is a runnable workflow with one declared webhook, whose
// `verify:` entry the caller chooses.
func triggeringWorkflow(key *v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name: "paid",
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "order", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
		}},
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name:           "stripe",
			Verify:         map[string]*v1.Value{v1.WebhookSchemeHMACSHA256: key},
			IdempotencyKey: v1.NewExpr(`event.headers["stripe-signature"]`),
			Arguments:      map[string]*v1.Value{"order": v1.NewExpr(`event.body.id`)},
		}}},
		Steps: []*v1.Node{{
			Id: "say",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
			}},
		}},
	}
}

// TestSubmissionRefusesALiteralWebhookSigningKey is the mutation proof for wiring
// the checker into the shared path: remove the `CheckWebhookTriggers` call from
// `v1.BindRunInputs` and this fails, because nothing else on the RPC path looks at
// a `verify:` entry's kind.
//
// The negative direction of the boundary CLAUDE.md's isolation section asks for:
// not "a secret reference is accepted" but "a value that is not one cannot get in".
func TestSubmissionRefusesALiteralWebhookSigningKey(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{}

	literal := triggeringWorkflow(v1.NewLiteral("whsec_live_do_not_commit_me"))

	// The schema itself is satisfied, which is the whole reason this rule has to
	// be asked at submit rather than left to protovalidate.
	require.NoError(t, v1.Validate(&v1.RunRequest{
		Workflow: literal,
		Inputs:   map[string]*v1.Value{"order": v1.NewLiteral("ord_1")},
	}))

	_, err := s.validateSubmission(literal, map[string]*v1.Value{"order": v1.NewLiteral("ord_1")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "using a value written in the file")

	// And the refusal does not name the key, because a diagnostic that quotes the
	// secret it is refusing has published it.
	require.NotContains(t, err.Error(), "whsec_live_do_not_commit_me")
}

// TestSubmissionAcceptsAReferencedWebhookSigningKey keeps the fix from being a
// blanket refusal: the shape the rule asks for still submits.
func TestSubmissionAcceptsAReferencedWebhookSigningKey(t *testing.T) {
	t.Parallel()

	s := &FlowstateServer{}

	referenced := triggeringWorkflow(&v1.Value{
		Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
			Scheme: "env", Name: "WEBHOOK_SECRET",
		}},
	})

	_, err := s.validateSubmission(referenced, map[string]*v1.Value{"order": v1.NewLiteral("ord_1")})
	require.NoError(t, err)
}

// TestSubmissionRefusesTheRestOfTheWebhookRules covers what else lived only in the
// Flowfile path, one specification per rule, so a regression names which rule came
// loose rather than only that some did.
func TestSubmissionRefusesTheRestOfTheWebhookRules(t *testing.T) {
	t.Parallel()

	reference := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
		Scheme: "env", Name: "WEBHOOK_SECRET",
	}}}

	for _, tt := range []struct {
		name   string
		mutate func(*v1.Workflow)
		want   string
	}{
		{
			name:   "no scheme at all",
			mutate: func(wf *v1.Workflow) { wf.GetTriggers().GetWebhooks()[0].Verify = nil },
			want:   "declares no `verify:`",
		},
		{
			name: "a scheme nothing implements",
			mutate: func(wf *v1.Workflow) {
				wf.GetTriggers().GetWebhooks()[0].Verify = map[string]*v1.Value{"hmac-md4": reference}
			},
			want: "which is not a scheme Flowstate can check",
		},
		{
			name:   "no idempotency key",
			mutate: func(wf *v1.Workflow) { wf.GetTriggers().GetWebhooks()[0].IdempotencyKey = nil },
			want:   "declares no `idempotency_key:`",
		},
		{
			name: "a constant idempotency key",
			mutate: func(wf *v1.Workflow) {
				wf.GetTriggers().GetWebhooks()[0].IdempotencyKey = v1.NewLiteral("always-the-same")
			},
			want: "does not depend on the delivery",
		},
		{
			name: "two webhooks under one name",
			mutate: func(wf *v1.Workflow) {
				first := wf.GetTriggers().GetWebhooks()[0]
				wf.GetTriggers().Webhooks = append(wf.GetTriggers().GetWebhooks(), first)
			},
			want: "is declared twice",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &FlowstateServer{}
			wf := triggeringWorkflow(reference)
			tt.mutate(wf)

			_, err := s.validateSubmission(wf, map[string]*v1.Value{"order": v1.NewLiteral("ord_1")})
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}
