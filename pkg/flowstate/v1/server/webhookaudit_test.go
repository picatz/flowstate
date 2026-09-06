package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// What the webhook receiver writes to the audit trail (#1774), and the bridge
// answering a gate on a deployment that has a recorder at all (#1797).

// trail is an emitter that keeps every record, for a test to read back.
type trail struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (t *trail) Emit(_ context.Context, record *v1.AuditRecord) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.records = append(t.records, record)

	return nil
}

func (t *trail) all() []*v1.AuditRecord {
	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]*v1.AuditRecord(nil), t.records...)
}

// auditedReceiver is a receiver over one workflow whose server records to sink.
func auditedReceiver(t *testing.T, temporal client.Client, sink audit.Emitter, served []*v1.Workflow, opts ...server.WebhookOption) *server.WebhookReceiver {
	t.Helper()

	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)

	receiver, err := mustNew(t, temporal, server.WithAudit(recorder)).NewWebhookReceiver(t.Context(),
		"", served, keyStore(t, webhookSecret), opts...)
	require.NoError(t, err)

	return receiver
}

// TestARefusedDeliveryIsRecordedByClass: every refusal decided before a run is
// one deny record under the WEBHOOK_DELIVERY point, coded by class, naming the
// route and nothing the sender wrote. Named from
// TestEveryEnforcementPointIsRecordedBySomeSeam in the root package, which
// cannot reach this seam itself.
func TestARefusedDeliveryIsRecordedByClass(t *testing.T) {
	t.Parallel()

	sink := &trail{}
	receiver := auditedReceiver(t, nil, sink, []*v1.Workflow{orderWebhookWorkflow()},
		server.WithWebhookRefusalInterval(0))

	const route = "order-webhook/storefront"
	body := deliveryBody("evt_refused")

	cases := []struct {
		name     string
		deliver  func() *http.Response
		status   int
		code     v1.AuditDenyCode
		route    string
		identity string
	}{
		{
			name:    "a wrong key",
			deliver: func() *http.Response { return deliver(t, receiver, "/webhooks/"+route, body, forged) },
			status:  http.StatusNotFound,
			code:    v1.AuditDenyCode_AUDIT_DENY_CODE_SIGNATURE_INVALID,
			route:   route,
		},
		{
			name: "no signature at all",
			deliver: func() *http.Response {
				req := httptest.NewRequest(http.MethodPost, "/webhooks/"+route, strings.NewReader(body))
				rec := httptest.NewRecorder()
				receiver.ServeHTTP(rec, req)
				return rec.Result()
			},
			status: http.StatusNotFound,
			code:   v1.AuditDenyCode_AUDIT_DENY_CODE_SIGNATURE_MISSING,
			route:  route,
		},
		{
			name:    "no such route",
			deliver: func() *http.Response { return deliver(t, receiver, "/webhooks/order-webhook/nope", body, signed) },
			status:  http.StatusNotFound,
			code:    v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND,
			route:   "",
		},
		{
			name: "a body past the bound",
			deliver: func() *http.Response {
				huge := strings.Repeat("x", v1.MaxWebhookPayloadBytes+1)
				return deliver(t, receiver, "/webhooks/"+route, huge, signed)
			},
			status: http.StatusRequestEntityTooLarge,
			code:   v1.AuditDenyCode_AUDIT_DENY_CODE_PAYLOAD_TOO_LARGE,
			route:  route,
		},
		{
			name: "verified, and the payload does not map",
			deliver: func() *http.Response {
				return deliver(t, receiver, "/webhooks/"+route,
					`{"id":"evt_refused","order":{"id":"ord_1","total_cents":"forty-two"}}`, signed)
			},
			status:   http.StatusUnprocessableEntity,
			code:     v1.AuditDenyCode_AUDIT_DENY_CODE_BINDING_FAILED,
			route:    route,
			identity: route,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(sink.all())
			resp := tc.deliver()
			require.Equal(t, tc.status, resp.StatusCode)

			records := sink.all()[before:]
			require.Len(t, records, 1, "one refusal, one record")
			record := records[0]

			assert.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
			assert.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_WEBHOOK_DELIVERY, record.GetEnforcementPoint())
			assert.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED, record.GetAction())
			assert.Equal(t, tc.code, record.GetDenyCode())
			assert.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_WEBHOOK_ROUTE, record.GetResourceKind())
			assert.Equal(t, tc.route, record.GetResourceKey())
			assert.Equal(t, uint32(1), record.GetCount())
			assert.Empty(t, record.GetDeliveryId(), "a refused delivery has no key to digest")

			if tc.identity == "" {
				assert.Nil(t, record.GetIdentity(), "a delivery that proved nothing was recorded as somebody")
			} else {
				assert.Equal(t, tc.identity, record.GetIdentity().GetSubject())
			}

			// Nothing the sender wrote. The body's event id, the order id, the
			// signature and the path are all peer text, and none of it has a
			// field to land in.
			text := prototext.Format(record)
			for _, peer := range []string{"evt_refused", "ord_", signed(body), forged(body), "/webhooks/", "nope", "forty-two"} {
				assert.NotContains(t, text, peer, "the record carries the sender's text")
			}
		})
	}
}

// TestRefusalRecordsAreBoundedPerClassPerRoute: a flood of one class on one
// route is one record per interval, and the record after the interval says
// how many it stood for. A different class on the same route is its own
// record, so the count is per class rather than per route.
func TestRefusalRecordsAreBoundedPerClassPerRoute(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	var clock sync.Mutex
	sink := &trail{}
	receiver := auditedReceiver(t, nil, sink, []*v1.Workflow{orderWebhookWorkflow()},
		server.WithWebhookClock(func() time.Time {
			clock.Lock()
			defer clock.Unlock()
			return now
		}))

	body := deliveryBody("evt_flood")
	for range 50 {
		resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, forged)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	}

	records := sink.all()
	require.Len(t, records, 1, "fifty refusals of one class inside one interval should be one record")
	assert.Equal(t, uint32(1), records[0].GetCount(), "the first refusal is written as it happens")

	// Another class, the same route, the same interval: its own record.
	req := httptest.NewRequest(http.MethodPost, "/webhooks/order-webhook/storefront", strings.NewReader(body))
	rec := httptest.NewRecorder()
	receiver.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Result().StatusCode)

	records = sink.all()
	require.Len(t, records, 2)
	assert.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_SIGNATURE_MISSING, records[1].GetDenyCode())

	// The interval passes, and the next refusal of the flooded class carries
	// what the window swallowed.
	clock.Lock()
	now = now.Add(server.DefaultWebhookRefusalInterval + time.Second)
	clock.Unlock()

	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, forged)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	records = sink.all()
	require.Len(t, records, 3)
	assert.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_SIGNATURE_INVALID, records[2].GetDenyCode())
	assert.Equal(t, uint32(50), records[2].GetCount(),
		"the record after the interval should stand for the 49 it swallowed and itself")
}

// TestAnAcceptedDeliveryIsRecordedWithItsRun: an accepted delivery is one
// allow record naming the run it started, the delivery id provenance records,
// and the trigger as the principal — written before the start. A redelivery
// adds its admission and the join it was answered with.
func TestAnAcceptedDeliveryIsRecordedWithItsRun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	sink := &trail{}
	receiver := auditedReceiver(t, temporal, sink, []*v1.Workflow{orderWebhookWorkflow()})

	body := deliveryBody("evt_recorded")
	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	accepted := readAccepted(t, resp)

	records := sink.all()
	require.Len(t, records, 1, "one accepted delivery, one record")
	first := records[0]
	assert.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, first.GetDecision())
	assert.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_WEBHOOK_DELIVERY, first.GetEnforcementPoint())
	assert.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, first.GetResourceKind())
	assert.Equal(t, accepted.WorkflowID, first.GetResourceKey(), "the record does not name the run the delivery started")
	assert.Equal(t, accepted.DeliveryID, first.GetDeliveryId(), "the record does not carry the delivery id provenance records")
	assert.False(t, first.GetJoined())
	assert.Equal(t, "order-webhook/storefront", first.GetIdentity().GetSubject())
	assert.Equal(t, v1.WebhookPrincipalIssuer, first.GetIdentity().GetIssuer())
	assert.NotContains(t, prototext.Format(first), "evt_recorded", "the raw idempotency key reached the trail")

	// The redelivery: admitted, then recognized as a join.
	resp = deliver(t, receiver, "/webhooks/order-webhook/storefront", body, signed)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, readAccepted(t, resp).Joined)

	records = sink.all()
	require.Len(t, records, 3, "a redelivery is its admission and the join it was answered with")
	assert.False(t, records[1].GetJoined())
	assert.True(t, records[2].GetJoined())
	for _, record := range records[1:] {
		assert.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
		assert.Equal(t, accepted.WorkflowID, record.GetResourceKey())
		assert.Equal(t, accepted.DeliveryID, record.GetDeliveryId())
	}
}

// TestARequiredRecorderThatCannotRecordRefusesTheStart is the fail-closed
// direction: under --audit-required a sink that is down means no run starts,
// and the sender is told to retry rather than told their payload is wrong.
func TestARequiredRecorderThatCannotRecordRefusesTheStart(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	broken, err := audit.NewRecorder(audit.WithoutStderr(), audit.Required(), audit.WithEmitter(downSink{}))
	require.NoError(t, err)
	receiver, err := mustNew(t, temporal, server.WithAudit(broken)).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	resp := deliver(t, receiver, "/webhooks/order-webhook/storefront", deliveryBody("evt_unrecorded"), signed)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"a delivery the deployment could not record was answered as though it were accepted or at fault")
	assert.NotEmpty(t, resp.Header.Get("Retry-After"))

	// And nothing started: the record is written before the mutation it permits.
	listed, err := temporal.ListWorkflow(t.Context(), &workflowservice.ListWorkflowExecutionsRequest{})
	require.NoError(t, err)
	assert.Empty(t, listed.GetExecutions(), "a run started under a required recorder that could not record")
}

type downSink struct{}

func (downSink) Emit(context.Context, *v1.AuditRecord) error {
	return context.DeadlineExceeded
}

// TestABridgedDeliveryIsRecordedRatherThanRefusedByTheRecorder pins #1797: the
// bridge's records used to be written under an RPC verb no action binds, so a
// deployment with a recorder answered every bridged delivery 503. With a
// recorder present, a genuine delivery answers the gate and is one allow
// record against the run; a delivery naming no run is one deny record.
func TestABridgedDeliveryIsRecordedRatherThanRefusedByTheRecorder(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	sink := &trail{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	deployment := mustNew(t, temporal, server.WithAudit(recorder))
	receiver, err := deployment.NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{bridgedGateWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	workflowID, runID := startParkedRun(t, deployment, bridgedGateWorkflow(), "order-1797")
	before := len(sink.all())

	resp := deliver(t, receiver, "/webhooks/gate-webhook/slack-approval",
		gateDelivery("evt_1797", "order-1797", "approve"), signed)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a genuine bridged delivery was refused on a deployment with a recorder")

	var out v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), workflowID, runID).Get(t.Context(), &out))
	assert.Equal(t, "approved",
		out.GetStepValues()["gate"].GetNamedValues()["decision"].GetLiteral().GetStringValue())

	records := sink.all()[before:]
	require.Len(t, records, 1, "one answered gate, one record")
	assert.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, records[0].GetDecision())
	assert.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_WEBHOOK_DELIVERY, records[0].GetEnforcementPoint())
	assert.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, records[0].GetResourceKind())
	assert.Equal(t, workflowID, records[0].GetResourceKey())
	assert.True(t, records[0].GetJoined(), "a bridged delivery starts nothing, so it always joins")
	assert.NotEmpty(t, records[0].GetDeliveryId())
	assert.Equal(t, "gate-webhook/slack-approval", records[0].GetIdentity().GetSubject())

	// A delivery naming no run: refused, and the refusal recorded.
	before = len(sink.all())
	resp = deliver(t, receiver, "/webhooks/gate-webhook/slack-approval",
		gateDelivery("evt_lost", "order-nobody", "approve"), signed)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	records = sink.all()[before:]
	require.Len(t, records, 1)
	assert.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, records[0].GetDecision())
	assert.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND, records[0].GetDenyCode())
	assert.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, records[0].GetResourceKind())
}
