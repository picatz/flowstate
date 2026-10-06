package flowstatev1_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The document a waiting receiver answers with, and the checks on the field
// that asks for it. The receiver and `flow test` both build the document through
// WebhookResponse, so what is asserted here is asserted for both.

// respondSpec is a specification declaring two outputs, one of them sensitive,
// and a sensitive input a failure could quote.
func respondSpec() *v1.Workflow {
	return &v1.Workflow{
		Name: "respond",
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Required: true, Sensitive: true},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "order", Value: v1.NewExpr(`"x"`)},
			{Name: "secret", Value: v1.NewExpr(`"y"`), Sensitive: true},
		},
	}
}

func respondOutputs(order, secret string) *v1.RunOutputs {
	return &v1.RunOutputs{Values: map[string]*v1.Value{
		"order":  v1.NewLiteral(order),
		"secret": v1.NewLiteral(secret),
	}}
}

func marshalDocument(t *testing.T, document v1.WebhookResponseDocument) (map[string]any, string) {
	t.Helper()

	raw, err := json.Marshal(document)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))

	return decoded, string(raw)
}

// TestACompletedRunAnswersItsDeclaredOutputsAndWithholdsASensitiveOne: the
// document carries the plain-JSON projection of the declared outputs, and a
// sensitive one is withheld however the value is spelled.
func TestACompletedRunAnswersItsDeclaredOutputsAndWithholdsASensitiveOne(t *testing.T) {
	t.Parallel()

	accepted := v1.AcceptedDelivery{WorkflowID: "wf", RunID: "run", DeliveryID: "d"}
	document := v1.WebhookResponse(accepted, v1.WebhookRun{
		Status:  v1.WebhookRunCompleted,
		Outputs: respondOutputs("ord_1", "hunter2"),
	}, respondSpec())

	decoded, raw := marshalDocument(t, document)
	assert.Equal(t, "completed", decoded["status"])
	assert.Equal(t, "wf", decoded["workflow_id"])
	assert.Equal(t, "d", decoded["delivery_id"])
	assert.Equal(t, false, decoded["joined"])
	assert.NotContains(t, decoded, "error")

	outputs, ok := decoded["outputs"].(map[string]any)
	require.True(t, ok, "outputs is not the declared outputs as a plain object: %s", raw)
	assert.Equal(t, "ord_1", outputs["order"])
	assert.Contains(t, outputs, "secret", "a withheld output vanished rather than being withheld")
	assert.NotContains(t, raw, "hunter2", "a sensitive output left in the clear")
	assert.Equal(t, http.StatusOK, document.HTTPStatus())
}

// TestAnUnreadableSpecificationWithholdsEverything: no specification, no way to
// say which outputs are safe, so none is sent and the failure sentence goes
// whole. The same fail-closed answer Get gives.
func TestAnUnreadableSpecificationWithholdsEverything(t *testing.T) {
	t.Parallel()

	completed := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunCompleted,
		Outputs: respondOutputs("ord_1", "hunter2"),
	}, nil)
	_, raw := marshalDocument(t, completed)
	assert.NotContains(t, raw, "ord_1")
	assert.NotContains(t, raw, "hunter2")

	failed := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunFailed,
		Failure: "the ledger refused ord_1",
	}, nil)
	require.NotNil(t, failed.Error)
	assert.Equal(t, v1.FailureWithheldMarker, failed.Error.Message)
}

// TestAFailedRunAnswersItsSentenceWithSensitiveValuesRemoved: a failure that
// quotes a sensitive input does not carry it out, and a run declaring a
// sensitive output withholds the sentence whole, because an output can be
// computed from a value the sentence quotes.
func TestAFailedRunAnswersItsSentenceWithSensitiveValuesRemoved(t *testing.T) {
	t.Parallel()

	inputs := map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}

	// No sensitive output: only the input's value is removed.
	spec := respondSpec()
	spec.DeclaredOutputs = spec.GetDeclaredOutputs()[:1]
	document := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunFailed,
		Failure: "step charge: bad token hunter2",
		Inputs:  inputs,
	}, spec)

	require.NotNil(t, document.Error)
	assert.NotContains(t, document.Error.Message, "hunter2")
	assert.Contains(t, document.Error.Message, "step charge: bad token")
	assert.Equal(t, v1.WebhookRunFailed, document.Status)
	assert.Empty(t, document.Outputs)
	assert.Equal(t, http.StatusOK, document.HTTPStatus(), "a failed run is still a delivered delivery")

	// A sensitive output declared: the whole sentence goes.
	withheld := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunFailed,
		Failure: "step charge: bad token hunter2",
		Inputs:  inputs,
	}, respondSpec())
	require.NotNil(t, withheld.Error)
	assert.Equal(t, v1.FailureWithheldMarker, withheld.Error.Message)
}

// TestARunStillGoingAnswersNoOutputs: running carries the address and the
// status, and nothing a run has not finished producing.
func TestARunStillGoingAnswersNoOutputs(t *testing.T) {
	t.Parallel()

	for _, run := range []v1.WebhookRun{
		{Status: v1.WebhookRunRunning},
		{}, // the zero value is running
		// Outputs handed to a running document are not sent.
		{Status: v1.WebhookRunRunning, Outputs: respondOutputs("ord_1", "hunter2")},
	} {
		document := v1.WebhookResponse(v1.AcceptedDelivery{WorkflowID: "wf"}, run, respondSpec())

		decoded, raw := marshalDocument(t, document)
		assert.Equal(t, "running", decoded["status"])
		assert.NotContains(t, decoded, "outputs")
		assert.NotContains(t, decoded, "error")
		assert.NotContains(t, raw, "ord_1")
	}
}

// TestHTTPStatusIsTheDeliverysDispositionAndNeverTheRuns: 2xx always. A run
// that finished is 200; one still going keeps the address-only answer's own
// disposition, 202 for the delivery that started it and 200 for one that joined.
func TestHTTPStatusIsTheDeliverysDispositionAndNeverTheRuns(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		status v1.WebhookRunStatus
		joined bool
		want   int
	}{
		{"completed", v1.WebhookRunCompleted, false, http.StatusOK},
		{"failed", v1.WebhookRunFailed, false, http.StatusOK},
		{"running, a start", v1.WebhookRunRunning, false, http.StatusAccepted},
		{"running, a redelivery", v1.WebhookRunRunning, true, http.StatusOK},
		{"completed, a redelivery", v1.WebhookRunCompleted, true, http.StatusOK},
	} {
		document := v1.WebhookResponseDocument{
			AcceptedDelivery: v1.AcceptedDelivery{Joined: test.joined}, Status: test.status,
		}
		assert.Equal(t, test.want, document.HTTPStatus(), test.name)
	}
}

// TestAnAnswerOverTheResponseBoundIsRunningWithNoOutputs: not truncated, not an
// error. Just under the bound is sent.
func TestAnAnswerOverTheResponseBoundIsRunningWithNoOutputs(t *testing.T) {
	t.Parallel()

	spec := respondSpec()
	spec.DeclaredOutputs = spec.GetDeclaredOutputs()[:1]

	over := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunCompleted,
		Outputs: &v1.RunOutputs{Values: map[string]*v1.Value{"order": v1.NewLiteral(strings.Repeat("x", v1.MaxWebhookResponseBytes))}},
	}, spec)
	assert.Equal(t, v1.WebhookRunRunning, over.Status, "an answer past the bound was sent")
	assert.Empty(t, over.Outputs)

	within := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunCompleted,
		Outputs: &v1.RunOutputs{Values: map[string]*v1.Value{"order": v1.NewLiteral(strings.Repeat("x", v1.MaxWebhookResponseBytes-1024))}},
	}, spec)
	assert.Equal(t, v1.WebhookRunCompleted, within.Status, "an answer within the bound was refused")
	assert.LessOrEqual(t, len(within.Outputs), v1.MaxWebhookResponseBytes)
}

// TestTheResponseBoundIsMeasuredOnTheWire: the JSON encoder the receiver writes
// with escapes `<`, `>` and `&` to six bytes each, so an output the projection
// holds under the bound can be several times larger on the wire. The bound is on
// what is sent.
func TestTheResponseBoundIsMeasuredOnTheWire(t *testing.T) {
	t.Parallel()

	spec := respondSpec()
	spec.DeclaredOutputs = spec.GetDeclaredOutputs()[:1]

	// 200,000 bytes in the projection, 1.2 MB once escaped.
	document := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{
		Status:  v1.WebhookRunCompleted,
		Outputs: &v1.RunOutputs{Values: map[string]*v1.Value{"order": v1.NewLiteral(strings.Repeat("<", 200_000))}},
	}, spec)
	assert.Equal(t, v1.WebhookRunRunning, document.Status, "an answer past the bound on the wire was sent")
	assert.Empty(t, document.Outputs)
}

// TestACompletedRunWithNoOutputsAnswersAnEmptyObject: `{}` and not `null`, the
// stable answer a run document gives.
func TestACompletedRunWithNoOutputsAnswersAnEmptyObject(t *testing.T) {
	t.Parallel()

	document := v1.WebhookResponse(v1.AcceptedDelivery{}, v1.WebhookRun{Status: v1.WebhookRunCompleted}, respondSpec())
	assert.JSONEq(t, `{}`, string(document.Outputs))
}

// respondTrigger is a webhook declaring a bounded wait, in a workflow that
// declares an output.
func respondTrigger(within time.Duration) (*v1.Workflow, *v1.WebhookTrigger) {
	wf := &v1.Workflow{
		Name:            "respond",
		DeclaredOutputs: []*v1.OutputDeclaration{{Name: "order", Value: v1.NewExpr(`"x"`)}},
		Steps:           []*v1.Node{{Id: "s", Kind: &v1.Node_Value{Value: v1.NewExpr(`"x"`)}}},
	}
	trigger := stripeTrigger()
	trigger.RespondWithin = durationpb.New(within)
	wf.Triggers = &v1.Triggers{Webhooks: []*v1.WebhookTrigger{trigger}}

	return wf, trigger
}

// TestRespondWithinIsRefusedWhereItCannotWork: the bound, the bridge and the
// workflow with nothing to answer with. Both directions: the legal edges pass.
func TestRespondWithinIsRefusedWhereItCannotWork(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		mutate  func(*v1.Workflow, *v1.WebhookTrigger)
		within  time.Duration
		refused string
	}{
		{name: "absent is the address-only receiver", within: 0},
		{name: "the floor", within: v1.MinWebhookRespondWithin},
		{name: "the ceiling", within: v1.MaxWebhookRespondWithin},
		{name: "under the floor", within: v1.MinWebhookRespondWithin - time.Nanosecond, refused: "outside"},
		{name: "over the ceiling", within: v1.MaxWebhookRespondWithin + time.Nanosecond, refused: "outside"},
		{
			name: "with a signal bridge", within: time.Second, refused: "`signal:`",
			mutate: func(_ *v1.Workflow, trigger *v1.WebhookTrigger) {
				trigger.Signal = &v1.WebhookTrigger_Signal{Name: "go", Correlate: v1.NewExpr(`event.body.id`)}
			},
		},
		{
			name: "without outputs", within: time.Second, refused: "`outputs:`",
			mutate: func(wf *v1.Workflow, _ *v1.WebhookTrigger) { wf.DeclaredOutputs = nil },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf, trigger := respondTrigger(test.within)
			if test.within == 0 {
				trigger.RespondWithin = nil
			}
			if test.mutate != nil {
				test.mutate(wf, trigger)
			}

			err := v1.CheckWebhookRespondWithin(wf, trigger)
			if test.refused == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.refused)
			assert.Contains(t, err.Error(), `"`+trigger.GetName()+`"`)

			// Every submit path is held to it, not only the compiler.
			_, bindErr := v1.BindRunInputs(wf, nil)
			require.Error(t, bindErr, "a hand-built specification carried the contradiction past submit")
		})
	}
}
