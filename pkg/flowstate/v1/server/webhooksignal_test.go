package server_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The bridge, end to end: HTTP in, a run's parked gate answered, with the
// authorization and the addressing decided by the server rather than by the
// payload.
//
// Everything a *file* decides about a bridge is pinned in
// `pkg/flowstate/v1/webhooksignal_test.go`; everything both *drivers* must
// agree about is the shared dedupe corpus. What is here is the half only a
// receiver has: which run a delivery reaches, whether the gate's policy admits
// it, and what a sender is told when either answer is no.

// gateWorkflow is the served specification: an entity-addressed run parked at a
// gate its own webhook may answer.
func bridgedGateWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "gate-webhook",
		Profile: v1.CurrentProfile,
		Signals: map[string]*v1.SignalPolicy{
			"stage-approved": {Allow: []*v1.SignalPolicyRule{{
				Subject: v1.QualifiedSubject(v1.WebhookPrincipalIssuer,
					v1.WebhookTriggerSubject("gate-webhook", "slack-approval")),
			}}},
		},
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "slack-approval",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "SLACK_SIGNING_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.id`),
			Signal: &v1.WebhookTrigger_Signal{
				Name:      "stage-approved",
				Correlate: v1.NewExpr(`event.body.run`),
				Arguments: map[string]*v1.Value{
					"approved": v1.NewExpr(`event.body.action == "approve"`),
				},
			},
		}}},
		// Two gates on one name, which is the shape a redelivery is dangerous
		// in: the second is what a replay would answer if the run did not
		// remember what it had already consumed. Its bound is short because a
		// case that reaches it is a case where nobody genuine answered.
		Steps: []*v1.Node{
			{
				Id: "gate",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Timeout: durationpb.New(time.Minute),
					Kind: &v1.Wait_Signal{Signal: &v1.Signal{
						Name: "stage-approved",
						Outputs: map[string]*v1.Value{
							"decision": v1.NewExpr(`payload.approved ? "approved" : "held"`),
						},
					}},
				}},
			},
			{
				Id: "next",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Timeout: durationpb.New(2 * time.Second),
					Kind: &v1.Wait_Signal{Signal: &v1.Signal{
						Name: "stage-approved",
						Outputs: map[string]*v1.Value{
							"answered": v1.NewExpr(`!timed_out`),
						},
					}},
				}},
			},
		},
	}
}

// gateDelivery is one click: which run it answers, and what it decides.
func gateDelivery(event, entity, action string) string {
	return fmt.Sprintf(`{"id":%q,"run":%q,"action":%q}`, event, entity, action)
}

// gateReceiver serves that workflow, over a real cluster.
func gateReceiver(t *testing.T, temporal client.Client) *server.WebhookReceiver {
	t.Helper()

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{bridgedGateWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	return receiver
}

// startParkedRun starts the served workflow under an entity key and waits for
// it to reach its gate, so a delivery has something to answer.
func startParkedRun(t *testing.T, temporal client.Client, entity string) client.WorkflowRun {
	t.Helper()

	id, err := v1.EntityWorkflowID("", entity)
	require.NoError(t, err)

	run, err := temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, &v1.RunState{Workflow: bridgedGateWorkflow()})
	require.NoError(t, err)

	return run
}

// TestABridgedDeliveryAnswersAParkedGate is the slice, end to end: a signed
// click reaches the run its payload names and resolves the gate, as the trigger.
func TestABridgedDeliveryAnswersAParkedGate(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver := gateReceiver(t, temporal)
	run := startParkedRun(t, temporal, "order-4471")

	body := gateDelivery("evt_click", "order-4471", "approve")
	resp := deliver(t, receiver, "/webhooks/gate-webhook/slack-approval", body, signed)

	// 200 and `joined`, not 202: a bridged delivery starts nothing, so there is
	// no acceptance-for-processing to report. See WebhookReceiver.answer.
	require.Equal(t, http.StatusOK, resp.StatusCode, "a genuine delivery was not accepted")

	accepted := readAccepted(t, resp)
	assert.True(t, accepted.Joined, "a delivery that answered an existing run reported starting one")
	assert.NotContains(t, accepted.DeliveryID, "evt_click",
		"the raw idempotency key was handed back to the sender")

	var out v1.Workflow_StepOutputs
	require.NoError(t, run.Get(t.Context(), &out))
	assert.Equal(t, "approved",
		out.GetStepValues()["gate"].GetNamedValues()["decision"].GetLiteral().GetStringValue(),
		"the gate did not resolve with what the delivery carried")
}

// TestABridgedRedeliveryAnswersOneGate is the at-least-once half, at the
// receiver: a provider retrying a click it already delivered gets the same
// answer and the run takes it once.
func TestABridgedRedeliveryAnswersOneGate(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver := gateReceiver(t, temporal)
	run := startParkedRun(t, temporal, "order-4472")

	body := gateDelivery("evt_retried", "order-4472", "approve")

	first := deliver(t, receiver, "/webhooks/gate-webhook/slack-approval", body, signed)
	require.Equal(t, http.StatusOK, first.StatusCode)

	// The same 200 and the same shape: a sender retrying converges rather than
	// alternating between a success and an error, which is the whole point of a
	// dedupe key. The receiver cannot tell the two apart and does not try —
	// the run is what knows, and it drops the second at intake.
	second := deliver(t, receiver, "/webhooks/gate-webhook/slack-approval", body, signed)
	require.Equal(t, http.StatusOK, second.StatusCode, "a redelivery was answered differently")
	assert.Equal(t, readAccepted(t, first).DeliveryID, readAccepted(t, second).DeliveryID,
		"one event produced two delivery ids")

	var out v1.Workflow_StepOutputs
	require.NoError(t, run.Get(t.Context(), &out))
	assert.Equal(t, "approved",
		out.GetStepValues()["gate"].GetNamedValues()["decision"].GetLiteral().GetStringValue())

	// The claim the whole design turns on, through the receiver rather than
	// through a driver's own delivery: the second gate lapsed, so the replay
	// the receiver happily accepted answered nothing.
	assert.False(t, out.GetStepValues()["next"].GetNamedValues()["answered"].GetLiteral().GetBoolValue(),
		"a redelivery answered the next gate on the same name")
}

// TestABridgedDeliveryToNoRunIsRefused is the first of the two refusals only a
// receiver can make: a correlation naming nothing here.
//
// Refused rather than started, which is the line the bridge does not cross: a
// `signal:` trigger answers runs and never creates them, so an entity key
// nobody is waiting under is a 404 and not a new run.
func TestABridgedDeliveryToNoRunIsRefused(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver := gateReceiver(t, temporal)

	body := gateDelivery("evt_lost", "order-nobody-started", "approve")
	resp := deliver(t, receiver, "/webhooks/gate-webhook/slack-approval", body, signed)

	assert.Equal(t, http.StatusNotFound, resp.StatusCode,
		"a delivery naming no run was not refused")

	// And nothing was created under that key.
	id, err := v1.EntityWorkflowID("", "order-nobody-started")
	require.NoError(t, err)
	_, err = temporal.DescribeWorkflowExecution(t.Context(), id, "")
	assert.Error(t, err, "a delivery that answered no gate started a run instead")
}

// TestAnUnverifiableBridgedDeliveryIsRefusedLikeAnyOther holds the bridge to
// the receiver's standing rule: every refusal decided before a delivery is
// known genuine is one status and one sentence.
func TestAnUnverifiableBridgedDeliveryIsRefusedLikeAnyOther(t *testing.T) {
	t.Parallel()

	receiver, err := mustNew(t, nil).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{bridgedGateWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	body := gateDelivery("evt_forged", "order-4471", "approve")

	forgedResp := deliver(t, receiver, "/webhooks/gate-webhook/slack-approval", body, forged)
	unrouted := deliver(t, receiver, "/webhooks/gate-webhook/nope", body, signed)

	assert.Equal(t, http.StatusNotFound, forgedResp.StatusCode)
	assert.Equal(t, http.StatusNotFound, unrouted.StatusCode,
		"a bad signature and an unknown trigger are distinguishable from outside")
}

// TestABridgeIsRefusedAtLoadWhenItsGateCannotAdmitIt is the fail-closed half a
// deployment answers: the receiver refuses to serve a bridge whose policy could
// never admit its trigger, at startup, where an operator can read it.
func TestABridgeIsRefusedAtLoadWhenItsGateCannotAdmitIt(t *testing.T) {
	t.Parallel()

	wf := bridgedGateWorkflow()
	wf.Signals = nil // the zero case: any sender, on a public route

	_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{wf}, keyStore(t, webhookSecret))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declares no `signals:` policy")
}

var _ = secrets.NewRef
