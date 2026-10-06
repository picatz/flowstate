package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// A webhook that holds the delivery open for the run's answer: `respond_within:`.
//
// The status code keeps its meaning, a delivery disposition, and `status` says
// what the run did. Every claim here fails if the receiver regresses in the
// direction it names: a leak of a sensitive output, a timeout answered as a
// success, a second run for a redelivery.

const respondRoute = "/webhooks/order-webhook/storefront"

// respondWorkflow is the served specification with a bounded wait and outputs.
//
// What the run does is chosen by the order id the delivery carries, so one
// served specification answers every case: `ord_fail` fails, `ord_wait` parks
// at a gate nothing answers until the test does, and anything else completes.
// `pad` is echoed twice into an output, which is how a response is made large
// without a delivery body that could not be.
func respondWorkflow(within time.Duration) *v1.Workflow {
	wf := orderWebhookWorkflow()
	wf.DeclaredErrors = []*v1.ErrorDeclaration{{Name: "Refused"}}
	wf.DeclaredInputs = append(wf.DeclaredInputs,
		&v1.InputDeclaration{Name: "pad", Type: v1.InputDeclaration_TYPE_STRING, Default: v1.NewLiteral("")})

	trigger := wf.GetTriggers().GetWebhooks()[0]
	trigger.Arguments["pad"] = v1.NewExpr(`has(event.body.pad) ? event.body.pad : ""`)
	trigger.RespondWithin = durationpb.New(within)

	wf.DeclaredOutputs = []*v1.OutputDeclaration{
		{Name: "order", Value: v1.NewExpr(`inputs.order_id`)},
		{Name: "total", Value: v1.NewExpr(`inputs.amount`)},
		{Name: "padded", Value: v1.NewExpr(`inputs.pad + inputs.pad`)},
	}
	wf.Steps = []*v1.Node{
		{
			Id:        "refuse",
			Condition: v1.NewExpr(`inputs.order_id == "ord_fail"`),
			Kind: &v1.Node_Fail{Fail: &v1.Fail{
				Error:   "Refused",
				Message: v1.NewExpr(`"the ledger refused " + inputs.order_id`),
			}},
		},
		{
			Id:        "hold",
			Condition: v1.NewExpr(`inputs.order_id == "ord_wait"`),
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "release"}},
			}},
		},
		{
			// A step's own output must never be part of the answer.
			Id:   "record",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`"step-secret-" + inputs.order_id`)},
		},
	}

	return wf
}

// withToken declares a sensitive output, which also withholds a failure
// sentence whole: an output is computed from values a failure can quote.
func withToken(wf *v1.Workflow) *v1.Workflow {
	wf.DeclaredOutputs = append(wf.DeclaredOutputs,
		&v1.OutputDeclaration{Name: "token", Value: v1.NewExpr(`"tok-" + inputs.order_id`), Sensitive: true})

	return wf
}

func respondBody(event, order string, pad string) string {
	body := map[string]any{"id": event, "order": map[string]any{"id": order, "total_cents": 4200}}
	if pad != "" {
		body["pad"] = pad
	}
	encoded, _ := json.Marshal(body)

	return string(encoded)
}

// respondDocument is what a waiting receiver answers with.
type respondDocument struct {
	WorkflowID string                    `json:"workflow_id"`
	RunID      string                    `json:"run_id"`
	DeliveryID string                    `json:"delivery_id"`
	Joined     bool                      `json:"joined"`
	Status     string                    `json:"status"`
	Outputs    map[string]any            `json:"outputs"`
	Error      *struct{ Message string } `json:"error"`
}

func readRespond(t *testing.T, resp *http.Response) (respondDocument, string) {
	t.Helper()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var document respondDocument
	require.NoError(t, json.Unmarshal(raw, &document), "the answer is not a JSON document")

	return document, string(raw)
}

func respondReceiver(t *testing.T, temporal client.Client, wf *v1.Workflow, opts ...server.WebhookOption) *server.WebhookReceiver {
	t.Helper()

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{wf}, keyStore(t, webhookSecret), opts...)
	require.NoError(t, err)

	return receiver
}

func releaseRun(t *testing.T, temporal client.Client, workflowID string) {
	t.Helper()

	_, err := mustNew(t, temporal).Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "release",
	}))
	require.NoError(t, err)
}

// TestACompletedRunIsAnsweredWithItsDeclaredOutputs: the feature, end to end.
// The run's declared outputs leave, a sensitive one is withheld with no way to
// ask for it, and nothing a step produced is in the document.
func TestACompletedRunIsAnsweredWithItsDeclaredOutputs(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, withToken(respondWorkflow(20*time.Second)))

	resp := deliver(t, receiver, respondRoute, respondBody("evt_done", "ord_H1x9", ""), signed)
	require.Equal(t, http.StatusOK, resp.StatusCode, "a delivery whose run finished within the bound is answered 200")

	document, raw := readRespond(t, resp)
	assert.Equal(t, "completed", document.Status)
	assert.False(t, document.Joined)
	assert.NotEmpty(t, document.RunID)
	assert.Equal(t, "ord_H1x9", document.Outputs["order"])
	assert.EqualValues(t, 4200, document.Outputs["total"])
	assert.Nil(t, document.Error)

	// The leak this surface must never have: the sensitive output's value, in
	// any spelling, and anything a step computed.
	assert.NotContains(t, raw, "tok-ord_H1x9", "a sensitive output left in the clear")
	assert.NotContains(t, raw, "step-secret", "a step's output is part of the answer")
	assert.Contains(t, document.Outputs, "token", "the withheld output vanished instead of being withheld")
}

// TestAFailedRunIsStillADeliveredDelivery: a failure is told in the document
// and is not an HTTP error, because a 4xx or 5xx tells a provider to retry a
// delivery that landed and started a run.
func TestAFailedRunIsStillADeliveredDelivery(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, respondWorkflow(20*time.Second))

	resp := deliver(t, receiver, respondRoute, respondBody("evt_fail", "ord_fail", ""), signed)
	require.Equal(t, http.StatusOK, resp.StatusCode, "a failed run turned a delivered delivery into an error status")

	document, raw := readRespond(t, resp)
	assert.Equal(t, "failed", document.Status)
	require.NotNil(t, document.Error)
	assert.Contains(t, document.Error.Message, "the ledger refused ord_fail")
	assert.Nil(t, document.Outputs, "a failed run answered with outputs")
	assert.NotContains(t, raw, "goroutine", "a stack reached the sender")
	assert.NotContains(t, raw, ".go:", "a source position reached the sender")
}

// TestAFailureSentenceIsWithheldWhenTheRunDeclaresASensitiveOutput: the same
// fail-closed answer `Get` gives. A sentence that could quote a value the run
// withholds is not sent, and the document still says the run failed.
func TestAFailureSentenceIsWithheldWhenTheRunDeclaresASensitiveOutput(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, withToken(respondWorkflow(20*time.Second)))

	resp := deliver(t, receiver, respondRoute, respondBody("evt_fail_sensitive", "ord_fail", ""), signed)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	document, raw := readRespond(t, resp)
	assert.Equal(t, "failed", document.Status)
	require.NotNil(t, document.Error)
	assert.Equal(t, v1.FailureWithheldMarker, document.Error.Message)
	assert.NotContains(t, raw, "the ledger refused")
}

// TestARunStillGoingAtTheBoundIsAnsweredRunning: the bound passes, the run is
// not failed or cancelled, and the delivery is accepted rather than succeeded:
// 202 with the address, which the caller reads with Get.
func TestARunStillGoingAtTheBoundIsAnsweredRunning(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, respondWorkflow(300*time.Millisecond))

	started := time.Now()
	resp := deliver(t, receiver, respondRoute, respondBody("evt_wait", "ord_wait", ""), signed)
	elapsed := time.Since(started)

	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"a timeout was answered as though the run had finished")
	assert.GreaterOrEqual(t, elapsed, 250*time.Millisecond, "the receiver did not wait for the run")
	assert.Less(t, elapsed, 15*time.Second, "the receiver waited past its bound")

	document, _ := readRespond(t, resp)
	assert.Equal(t, "running", document.Status)
	assert.Nil(t, document.Outputs)
	assert.NotEmpty(t, document.RunID)

	// The run is unharmed by the bound passing: still running, and finishing
	// when it is released.
	described, err := temporal.DescribeWorkflowExecution(t.Context(), document.WorkflowID, document.RunID)
	require.NoError(t, err)
	assert.Equal(t, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, described.GetWorkflowExecutionInfo().GetStatus())

	releaseRun(t, temporal, document.WorkflowID)
	var out v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), document.WorkflowID, document.RunID).Get(t.Context(), &out))
	assert.Equal(t, "ord_wait", out.GetRunOutputs().GetValues()["order"].GetLiteral().GetStringValue())
}

// TestARedeliveryJoinsTheRunAndWaitsTheSameBound: a redelivery of an event
// whose run is still going waits too, answers joined with the same status, and
// once the run finishes answers the same document a first delivery would have.
func TestARedeliveryJoinsTheRunAndWaitsTheSameBound(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, withToken(respondWorkflow(300*time.Millisecond)))

	body := respondBody("evt_redelivered", "ord_wait", "")

	first, _ := readRespond(t, deliver(t, receiver, respondRoute, body, signed))
	require.Equal(t, "running", first.Status)
	require.False(t, first.Joined)

	started := time.Now()
	again := deliver(t, receiver, respondRoute, body, signed)
	assert.GreaterOrEqual(t, time.Since(started), 250*time.Millisecond, "a redelivery did not wait the bound")
	require.Equal(t, http.StatusOK, again.StatusCode, "a redelivery that joined a running run keeps the joined disposition")

	joined, _ := readRespond(t, again)
	assert.True(t, joined.Joined)
	assert.Equal(t, "running", joined.Status)
	assert.Equal(t, first.RunID, joined.RunID, "a redelivery started a second run")

	releaseRun(t, temporal, first.WorkflowID)
	var out v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), first.WorkflowID, first.RunID).Get(t.Context(), &out))

	done := deliver(t, receiver, respondRoute, body, signed)
	require.Equal(t, http.StatusOK, done.StatusCode)
	document, raw := readRespond(t, done)
	assert.True(t, document.Joined)
	assert.Equal(t, "completed", document.Status)
	assert.Equal(t, "ord_wait", document.Outputs["order"])
	assert.NotContains(t, raw, "tok-ord_wait", "a redelivery's answer carried a sensitive output")
}

// TestAnAnswerTooLargeToSendIsAnsweredRunning: past the response bound the
// document is not truncated and does not error: the caller is told the run is
// going and reads the whole answer with Get.
func TestAnAnswerTooLargeToSendIsAnsweredRunning(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, respondWorkflow(20*time.Second))

	// Two copies of a 600 KB value: past the 1 MiB bound, under what the
	// delivery body bound lets a sender submit.
	pad := strings.Repeat("x", 600_000)
	resp := deliver(t, receiver, respondRoute, respondBody("evt_big", "ord_big", pad), signed)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "an oversize answer was sent, or turned into an error")

	document, raw := readRespond(t, resp)
	assert.Equal(t, "running", document.Status)
	assert.Nil(t, document.Outputs)
	assert.Less(t, len(raw), v1.MaxWebhookResponseBytes)

	var out v1.Workflow_StepOutputs
	require.NoError(t, temporal.GetWorkflow(t.Context(), document.WorkflowID, document.RunID).Get(t.Context(), &out))
	assert.Len(t, out.GetRunOutputs().GetValues()["padded"].GetLiteral().GetStringValue(), 1_200_000,
		"the run itself was affected by the oversize answer")
}

// stallingHistory is a client whose history reads never answer on their own: the
// call returns when its context ends, as a stalled frontend would.
type stallingHistory struct{ client.Client }

func (stallingHistory) GetWorkflowHistory(ctx context.Context, _, _ string, _ bool, _ enums.HistoryEventFilterType) client.HistoryEventIterator {
	return stalledIterator{ctx: ctx}
}

type stalledIterator struct{ ctx context.Context }

func (i stalledIterator) HasNext() bool { <-i.ctx.Done(); return false }

func (stalledIterator) Next() (*historypb.HistoryEvent, error) { return nil, errors.New("stalled") }

// TestAReadThatStallsAfterTheRunFinishedIsHeldToTheBound: the bound covers the
// reads that follow the result as well as the wait for it. The run completes
// at once and the read of its specification never answers; the delivery is
// answered running at the bound rather than holding its slot until the sender
// gives up.
func TestAReadThatStallsAfterTheRunFinishedIsHeldToTheBound(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, stallingHistory{Client: temporal}, respondWorkflow(500*time.Millisecond))

	answered := make(chan *http.Response, 1)
	go func() {
		answered <- deliver(t, receiver, respondRoute, respondBody("evt_stall", "ord_H1x9", ""), signed)
	}()

	select {
	case resp := <-answered:
		document, _ := readRespond(t, resp)
		assert.Equal(t, "running", document.Status)
		assert.Nil(t, document.Outputs, "an answer left while the run's specification could not be read")
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery was held past respond_within by a read that stalled after the run finished")
	}
}

// TestAHungUpSenderEndsTheWaitWithoutEndingTheRun: the request context is the
// other bound. A sender that goes away does not hold the slot for the author's
// whole `respond_within:`, and the run goes on.
func TestAHungUpSenderEndsTheWaitWithoutEndingTheRun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	receiver := respondReceiver(t, temporal, respondWorkflow(30*time.Second))

	body := respondBody("evt_hangup", "ord_wait", "")
	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodPost, respondRoute, strings.NewReader(body)).WithContext(ctx)
	req.Header.Set(v1.WebhookSignatureHeader, signed(body))
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		receiver.ServeHTTP(recorder, req)
	}()

	time.AfterFunc(500*time.Millisecond, cancel)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a hung-up sender held the receiver for its whole bound")
	}

	document, _ := readRespond(t, recorder.Result())
	assert.Equal(t, "running", document.Status)

	described, err := temporal.DescribeWorkflowExecution(t.Context(), document.WorkflowID, document.RunID)
	require.NoError(t, err)
	assert.Equal(t, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, described.GetWorkflowExecutionInfo().GetStatus(),
		"the sender hanging up ended the run")
}

// acceptedLog is a logger that signals the first time the receiver says it
// accepted a delivery.
type acceptedLog struct {
	once sync.Once
	done chan struct{}
}

func (*acceptedLog) Enabled(context.Context, slog.Level) bool { return true }

func (l *acceptedLog) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "accepted a delivery" {
		l.once.Do(func() { close(l.done) })
	}

	return nil
}

func (l *acceptedLog) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *acceptedLog) WithGroup(string) slog.Handler      { return l }

// TestAWaitingDeliveryHoldsItsConcurrencySlot: the wait spends the receiver's
// one concurrency bound, so a second delivery is shed while the first waits and
// admitted after it is answered. There is no second pool.
func TestAWaitingDeliveryHoldsItsConcurrencySlot(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	accepted := &acceptedLog{done: make(chan struct{})}
	receiver := respondReceiver(t, temporal, respondWorkflow(5*time.Second),
		server.WithWebhookConcurrency(1), server.WithWebhookLogger(slog.New(accepted)))

	first := make(chan *http.Response, 1)
	go func() {
		first <- deliver(t, receiver, respondRoute, respondBody("evt_slot", "ord_wait", ""), signed)
	}()

	// The receiver logs the acceptance after the run is started and before it
	// waits on it, so from here the first delivery is in its wait.
	select {
	case <-accepted.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the first delivery was never accepted")
	}

	shed := deliver(t, receiver, respondRoute, respondBody("evt_other", "ord_H1x9", ""), signed)
	require.Equal(t, http.StatusServiceUnavailable, shed.StatusCode, "a waiting delivery did not hold its slot")

	resp := <-first
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	after := deliver(t, receiver, respondRoute, respondBody("evt_after", "ord_H1x9", ""), signed)
	assert.Equal(t, http.StatusOK, after.StatusCode, "the slot was never released")
}

// TestAddressOnlyIsStillTheDefault: a trigger that declares no
// `respond_within:` answers the document it always did, with no `status`.
func TestAddressOnlyIsStillTheDefault(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	receiver, err := mustNew(t, temporal).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{orderWebhookWorkflow()}, keyStore(t, webhookSecret))
	require.NoError(t, err)

	resp := deliver(t, receiver, respondRoute, deliveryBody("evt_plain"), signed)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	_, raw := readRespond(t, resp)
	assert.NotContains(t, raw, "status")
	assert.NotContains(t, raw, "outputs")
}

// TestAWebhookThatCannotAnswerIsNotServed: the contradictions the compiler
// refuses are refused at registration too, for a specification that never was
// a Flowfile.
func TestAWebhookThatCannotAnswerIsNotServed(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*v1.Workflow){
		"no outputs": func(wf *v1.Workflow) { wf.DeclaredOutputs = nil },
		"a signal bridge": func(wf *v1.Workflow) {
			wf.GetTriggers().GetWebhooks()[0].Signal = &v1.WebhookTrigger_Signal{Name: "release", Correlate: v1.NewExpr(`event.body.id`)}
		},
		"past the ceiling": func(wf *v1.Workflow) {
			wf.GetTriggers().GetWebhooks()[0].RespondWithin = durationpb.New(time.Minute)
		},
		"under the floor": func(wf *v1.Workflow) {
			wf.GetTriggers().GetWebhooks()[0].RespondWithin = durationpb.New(time.Millisecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := respondWorkflow(time.Second)
			mutate(wf)

			_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(), "", []*v1.Workflow{wf}, keyStore(t, webhookSecret))
			require.Error(t, err, "a webhook that cannot answer was served")
		})
	}
}
