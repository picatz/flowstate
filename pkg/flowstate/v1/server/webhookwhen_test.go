package server_test

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The webhook admission predicate at the receiver: a delivery that verifies and
// is declined by `when:` is answered 204, starts nothing and is recorded; one
// whose `when:` cannot be answered is refused with a fixed sentence and starts
// nothing.

// filteredWorkflow is the served specification with a `when:` on its webhook.
func filteredWorkflow(when string) *v1.Workflow {
	wf := orderWebhookWorkflow()
	wf.GetTriggers().GetWebhooks()[0].When = v1.NewExpr(when)

	return wf
}

func filteredBody(event, action string) string {
	return `{"id":"` + event + `","action":"` + action + `","order":{"id":"ord_H1x9","total_cents":4200}}`
}

const filteredRoute = "/webhooks/order-webhook/storefront"

// TestAnAdmittedDeliveryStartsARunAndADeclinedOneDoesNot: the acceptance
// criterion end to end. Both go to a real cluster; only the admitted one may
// leave anything on it.
func TestAnAdmittedDeliveryStartsARunAndADeclinedOneDoesNot(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	sink := &trail{}
	receiver := auditedReceiver(t, temporal, sink,
		[]*v1.Workflow{filteredWorkflow(`event.body.action == "opened"`)})

	declined := deliver(t, receiver, filteredRoute, filteredBody("evt_sync", "synchronize"), signed)
	require.Equal(t, http.StatusNoContent, declined.StatusCode, "a declined delivery must answer 204 so the provider does not retry")
	said, err := io.ReadAll(declined.Body)
	require.NoError(t, err)
	assert.Empty(t, said, "a 204 has no body")

	admitted := deliver(t, receiver, filteredRoute, filteredBody("evt_open", "opened"), signed)
	require.Equal(t, http.StatusAccepted, admitted.StatusCode)
	accepted := readAccepted(t, admitted)
	require.False(t, accepted.Joined)

	// The declined event's key names no run: delivering the same event again
	// after the file would admit it must start one rather than join anything,
	// which is the "a declined delivery is not recorded as a key" constraint.
	receiver = auditedReceiver(t, temporal, sink, []*v1.Workflow{orderWebhookWorkflow()})
	again := deliver(t, receiver, filteredRoute, filteredBody("evt_sync", "synchronize"), signed)
	require.Equal(t, http.StatusAccepted, again.StatusCode)
	assert.False(t, readAccepted(t, again).Joined, "a declined delivery left a key behind")
}

// TestADeclinedDeliveryIsRecordedAndStartsNothing: no Temporal client at all,
// so a start or a signal attempted for a declined delivery would fail the
// test; and the record names the route and the new deny code under the trigger's
// own identity, with nothing the sender wrote.
func TestADeclinedDeliveryIsRecordedAndStartsNothing(t *testing.T) {
	t.Parallel()

	sink := &trail{}
	receiver := auditedReceiver(t, nil, sink,
		[]*v1.Workflow{filteredWorkflow(`event.body.action == "opened"`)})

	resp := deliver(t, receiver, filteredRoute, filteredBody("evt_sync_secretish", "synchronize"), signed)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	records := sink.all()
	require.Len(t, records, 1, "a declined delivery is one decision, recorded")
	record := records[0]

	assert.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	assert.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_WEBHOOK_DELIVERY, record.GetEnforcementPoint())
	assert.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_WEBHOOK_DECLINED, record.GetDenyCode())
	assert.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_WEBHOOK_ROUTE, record.GetResourceKind())
	assert.Equal(t, "order-webhook/storefront", record.GetResourceKey())
	assert.Equal(t, "order-webhook/storefront", record.GetIdentity().GetSubject())
	assert.Empty(t, record.GetDeliveryId())

	text := prototext.Format(record)
	for _, peer := range []string{"evt_sync_secretish", "synchronize", "ord_H1x9", "/webhooks/"} {
		assert.NotContains(t, text, peer, "the record carries the delivery's text")
	}
}

// TestAFloodOfDeclinesIsOneBoundedRecord: a provider's firehose is the case this
// feature exists for, and it must not become a durable write per delivery.
func TestAFloodOfDeclinesIsOneBoundedRecord(t *testing.T) {
	t.Parallel()

	sink := &trail{}
	receiver := auditedReceiver(t, nil, sink,
		[]*v1.Workflow{filteredWorkflow(`event.body.action == "opened"`)})

	for _, event := range []string{"evt_1", "evt_2", "evt_3", "evt_4"} {
		resp := deliver(t, receiver, filteredRoute, filteredBody(event, "labeled"), signed)
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	}

	assert.Len(t, sink.all(), 1, "every declined delivery wrote its own record")
}

// TestADeclineARequiredRecorderCannotWriteIsNeverAnsweredAsRecorded: the first
// decline's write fails and answers 503; the window it opened must not turn the
// retries into 204s that nothing recorded. Each is told to retry until a write
// lands, and that write stands for every decline since.
func TestADeclineARequiredRecorderCannotWriteIsNeverAnsweredAsRecorded(t *testing.T) {
	t.Parallel()

	sink := &flakySink{failures: 3}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.Required(), audit.WithEmitter(sink))
	require.NoError(t, err)

	receiver, err := mustNew(t, nil, server.WithAudit(recorder)).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{filteredWorkflow(`event.body.action == "opened"`)}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	for _, event := range []string{"evt_1", "evt_2", "evt_3"} {
		resp := deliver(t, receiver, filteredRoute, filteredBody(event, "labeled"), signed)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
			"a decline the recorder could not write was answered as though it were recorded")
		assert.NotEmpty(t, resp.Header.Get("Retry-After"))
	}

	resp := deliver(t, receiver, filteredRoute, filteredBody("evt_4", "labeled"), signed)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	records := sink.written()
	require.Len(t, records, 1)
	assert.GreaterOrEqual(t, records[0].GetCount(), uint32(4), "the retried write lost the declines before it")
}

// flakySink fails its first `failures` emits and records the rest.
type flakySink struct {
	mu       sync.Mutex
	failures int
	records  []*v1.AuditRecord
}

func (s *flakySink) Emit(_ context.Context, record *v1.AuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failures > 0 {
		s.failures--

		return context.DeadlineExceeded
	}
	s.records = append(s.records, record)

	return nil
}

func (s *flakySink) written() []*v1.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*v1.AuditRecord(nil), s.records...)
}

// TestAWhenThatCannotBeAnsweredRefusesTheDelivery: fail closed, as a refusal and
// not as a decline, with the fixed sentence and the broken-rule code.
func TestAWhenThatCannotBeAnsweredRefusesTheDelivery(t *testing.T) {
	t.Parallel()

	for name, when := range map[string]string{
		"an evaluation error": `event.body.nosuchfield == "wanted-value"`,
		"a non-bool":          `event.body.action`,
		"the cost bound": `size([1,2,3,4,5,6,7,8,9,10].map(a, [1,2,3,4,5,6,7,8,9,10].map(b, ` +
			`[1,2,3,4,5,6,7,8,9,10].map(c, [1,2,3,4,5,6,7,8,9,10].map(d, ` +
			`[1,2,3,4,5,6,7,8,9,10].map(e, [1,2,3,4,5,6,7,8,9,10].map(f, a+b+c+d+e+f)))))))` +
			` > 0 && event.body.id != ""`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sink := &trail{}
			receiver := auditedReceiver(t, nil, sink, []*v1.Workflow{filteredWorkflow(when)})

			resp := deliver(t, receiver, filteredRoute, filteredBody("evt_x", "opened"), signed)
			require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)

			said, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Contains(t, string(said), `webhook "storefront"`)
			assert.Contains(t, string(said), "`when:` could not be answered")
			for _, echoed := range []string{"wanted-value", "evt_x", "opened", "no such key", "cost limit"} {
				assert.NotContains(t, string(said), echoed, "the refusal echoes the delivery or the evaluator")
			}

			records := sink.all()
			require.Len(t, records, 1)
			assert.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_RULE_ERROR, records[0].GetDenyCode())
		})
	}
}
