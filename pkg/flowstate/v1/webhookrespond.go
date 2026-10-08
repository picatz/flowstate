package flowstatev1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
)

// A webhook's `respond_within:`: hold the delivery open for the run's answer.
//
// A receiver answers a delivery with the run's address and nothing else, which
// is the right default and the wrong shape for a provider that wants the
// answer in the same exchange: a payment form that needs the order id, a slash
// command that needs the reply it will render. `respond_within:` is the bound
// on a wait for exactly that, and it is the only form of the feature. A
// callback would be a second spelling of "tell somebody when a run finishes",
// which is a last step using `webhook.send`.
//
// The document the receiver answers with is built here, by one pure function,
// so that the receiver and the `flow test` rehearsal of a `trigger:` case
// render the same shape and a case asserting `response:` is asserting the
// receiver's own answer rather than a re-description of it (invariant 3). What
// is receiver-only is the wait itself: `flow test` has no HTTP listener and no
// run that outlives its case.

// The bounds on `respond_within:`.
//
// The floor is below a useful round trip to a run; the ceiling is under every
// provider timeout this has been checked against, because a wait that outlives
// the sender's own timeout is a delivery the sender retries while the receiver
// still holds the first. Both restate [WebhookTrigger]'s schema rule as the
// constants the explicit check reads.
const (
	MinWebhookRespondWithin = 100 * time.Millisecond
	MaxWebhookRespondWithin = 30 * time.Second
)

// MaxWebhookResponseBytes bounds the document a waiting receiver writes back,
// and is the delivery body's own bound: both are a document the far side of a
// trust boundary has to read whole, and one number for that class is one number
// to reason about. A document over it is answered as a run still in progress,
// with no outputs, rather than truncated: a clipped document is a wrong
// answer, and the caller can read the full one with `Get`.
const MaxWebhookResponseBytes = MaxWebhookPayloadBytes

// AcceptedDelivery is what a receiver answers a sender with.
//
// Deliberately small. It names the run so a sender can correlate, and says whether
// this delivery started it, and nothing else: a response body is the one part of
// this exchange an attacker who *does* hold the signing key can read, so it
// carries no specification, no inputs and no deployment detail.
type AcceptedDelivery struct {
	// WorkflowID is the run's addressable id, derived from the idempotency key.
	WorkflowID string `json:"workflow_id"`

	// RunID is the Temporal run this delivery started or joined.
	RunID string `json:"run_id"`

	// DeliveryID names the delivery, and is what provenance records — a digest of
	// the idempotency key rather than the key itself. See [WebhookDeliveryID].
	DeliveryID string `json:"delivery_id"`

	// Joined is true when this delivery was a redelivery: the run already
	// existed, and no second one was started.
	Joined bool `json:"joined"`
}

// WebhookRunStatus is what a run was doing when a waiting receiver answered.
type WebhookRunStatus string

const (
	// WebhookRunCompleted: the run finished; the document carries its declared
	// outputs.
	WebhookRunCompleted WebhookRunStatus = "completed"

	// WebhookRunFailed: the run ended in failure; the document carries the
	// failure's sentence. A failed run is still a delivered delivery, so this is
	// not an HTTP error: the status code answers what happened to the delivery,
	// this answers what happened to the run.
	WebhookRunFailed WebhookRunStatus = "failed"

	// WebhookRunRunning: the run has not finished within the bound, or its
	// answer does not fit the response bound. It continues; `Get` reads it.
	WebhookRunRunning WebhookRunStatus = "running"
)

// WebhookRun is what a run did, as far as the response document is concerned.
type WebhookRun struct {
	// Status is the run's state; the zero value is [WebhookRunRunning].
	Status WebhookRunStatus

	// Outputs are the run's declared outputs, as the run produced them: nothing
	// has been withheld yet. Only meaningful for [WebhookRunCompleted].
	Outputs *RunOutputs

	// Failure is the run's failure sentence as the engine wrote it, and Inputs
	// the run's bound inputs, which are what a sensitive input's value is
	// removed from it by. Only meaningful for [WebhookRunFailed].
	Failure string
	Inputs  map[string]*Value
}

// WebhookResponseError is the failure a `failed` document carries: the sentence
// and nothing else. Never a stack, a step transcript or a value.
type WebhookResponseError struct {
	Message string `json:"message"`
}

// WebhookResponseDocument is the JSON a waiting receiver writes.
//
// The delivery's own address leads, unchanged from the address-only answer, and
// `status` says what the run did: a caller that ignores everything after
// `delivery_id` is reading the document it always read.
type WebhookResponseDocument struct {
	AcceptedDelivery

	// Status is set whenever the trigger declared `respond_within:`.
	Status WebhookRunStatus `json:"status"`

	// Outputs is the declared outputs as the plain-JSON projection every run
	// document uses (#1553), present for a completed run only. Sensitive
	// outputs are withheld; there is no way to reveal one on this surface.
	Outputs json.RawMessage `json:"outputs,omitempty"`

	// Error is present for a failed run only.
	Error *WebhookResponseError `json:"error,omitempty"`
}

// HTTPStatus is the response's status code, which is the delivery's
// disposition and never the run's: 2xx always, because a provider reads any
// other as "retry", and the delivery landed whatever the run then did.
//
// A run that finished is answered 200. One still running keeps the address-only
// answer's own disposition: 202 for a delivery that started it, 200 for a
// redelivery that joined it.
func (d WebhookResponseDocument) HTTPStatus() int {
	if d.Status == WebhookRunRunning && !d.Joined {
		return http.StatusAccepted
	}

	return http.StatusOK
}

// WebhookResponse builds the document a waiting receiver answers with, and the
// one `flow test` asserts a `trigger:` case's `response:` against.
//
// Pure: it reads no clock, no network and no run, only what it is handed, so
// the two drivers cannot disagree about it. spec is the specification the run
// executed under, which is what decides what is withheld; nil, or one whose
// sensitivity cannot be decided, withholds every output and the whole failure
// sentence, the same fail-closed answer `Get` gives (invariant 6).
//
// What leaves is the run's declared outputs and a failure sentence, never a
// step output or a transcript, and never a value a `sensitive:` declaration
// withholds, redacted by the functions `Get` uses and with no reveal arm
// (invariant 7). A document over [MaxWebhookResponseBytes] is answered as
// `running` with no outputs (invariant 5).
func WebhookResponse(accepted AcceptedDelivery, run WebhookRun, spec *Workflow) WebhookResponseDocument {
	document := WebhookResponseDocument{AcceptedDelivery: accepted, Status: WebhookRunRunning}

	// Decided once, here, because both arms read it: a run whose sensitive
	// declarations cannot be enumerated is withheld whole, as `Get` does.
	var (
		sensitive map[string]bool
		values    SensitiveValues
	)
	if spec == nil || sensitivityUndecidable(spec) {
		values = WithheldSensitiveValues()
	} else {
		sensitive = SensitiveOutputNames(spec)
		values = RunFailureSensitiveValues(spec, run.Inputs)
	}

	switch run.Status {
	case WebhookRunCompleted:
		outputs := run.Outputs
		if outputs == nil {
			// A completed run that declared nothing answers `{}` rather than
			// `null`, the stable answer to a stable question the run document
			// gives.
			outputs = &RunOutputs{}
		}

		encoded, err := MarshalRunDocument(redactRunOutputs(outputs, sensitive, false), false, false)
		if err != nil {
			return document
		}

		// The bound is on what is sent, so it is measured over the whole
		// document as the receiver writes it: its envelope, its newline, and the
		// escaping the JSON encoder applies to `<`, `>` and `&` inside the
		// outputs, which the projection above does not.
		completed := document
		completed.Status = WebhookRunCompleted
		completed.Outputs = encoded
		if wireSize(completed) > MaxWebhookResponseBytes {
			// Not truncated: the caller reads the whole answer with `Get`.
			return document
		}

		return completed

	case WebhookRunFailed:
		document.Status = WebhookRunFailed
		document.Error = &WebhookResponseError{
			Message: values.RedactTextWithin(run.Failure, FailureWithheldMarker, RedactedEntityStateAllowance),
		}

	case WebhookRunRunning:
	}

	return document
}

// wireSize is the length of the document as the receiver writes it: the
// default JSON encoder's output, newline included.
func wireSize(document WebhookResponseDocument) int {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(document); err != nil {
		return math.MaxInt
	}

	return buf.Len()
}

// sensitivityUndecidable reports a specification embedding a workflow that
// declares a sensitive value, or one that cannot be walked: a callee's
// declarations reach the caller's values through expressions nothing can trace,
// so the run is withheld whole, exactly as `Get` withholds it.
func sensitivityUndecidable(spec *Workflow) bool {
	callee, err := CalleeDeclaresSensitiveValues(spec)

	return err != nil || callee
}

// CheckWebhookRespondTriggers applies [CheckWebhookRespondWithin] to every
// webhook a workflow declares.
//
// Called from [BindRunInputs] beside [CheckWebhookSignalBridges], for that
// function's reason: every submit path passes through it, so a specification
// that never was a Flowfile is held to the rule the compiler enforces.
func CheckWebhookRespondTriggers(wf *Workflow) error {
	for _, trigger := range wf.GetTriggers().GetWebhooks() {
		if err := CheckWebhookRespondWithin(wf, trigger); err != nil {
			return err
		}
	}

	return nil
}

// CheckWebhookRespondWithin reports a `respond_within:` that is written and
// cannot work. Absent is valid and is the address-only receiver.
//
// Everything refused here is a property of the file. The bound is the field's
// own: there is no default, so a wait is exactly as long as an author wrote
// down. `signal:` is refused because a bridge starts nothing and has no run of
// its own to wait for; a workflow with no `outputs:` is refused because the
// declared outputs are the only thing a run can answer with, and a wait that
// can only ever say "done" is a latency cost for no information.
func CheckWebhookRespondWithin(wf *Workflow, trigger *WebhookTrigger) error {
	within := trigger.GetRespondWithin()
	if within == nil {
		return nil
	}

	name := trigger.GetName()

	if err := within.CheckValid(); err != nil {
		return fmt.Errorf("webhook %q writes a `respond_within:` that is not a duration: %w", name, err)
	}
	if d := within.AsDuration(); d < MinWebhookRespondWithin || d > MaxWebhookRespondWithin {
		return fmt.Errorf("webhook %q writes `respond_within: %s`, outside the %s to %s a receiver will hold "+
			"a delivery open for; a longer answer is one the caller reads with `flow get` or `Get`",
			name, d, MinWebhookRespondWithin, MaxWebhookRespondWithin)
	}

	if trigger.GetSignal() != nil {
		return fmt.Errorf("webhook %q declares both `respond_within:` and `signal:`; a `signal:` delivery "+
			"answers a run that is already waiting and starts none, so there is no run of its own to wait "+
			"for. Keep `signal:` to answer a gate, or `respond_within:` to answer with a run's outputs", name)
	}

	if WebhookAnswersEmpty(trigger) {
		return fmt.Errorf("webhook %q declares `respond_within:` and verifies with a scheme whose sender takes only "+
			"a bodyless 200; Slack shows any other answer to the person who clicked as a failure, so a run's "+
			"outputs have nowhere to go. Drop `respond_within:` and read the outcome with `flow get`, or post it "+
			"back to the channel from a step", name)
	}

	if len(wf.GetDeclaredOutputs()) == 0 {
		return fmt.Errorf("webhook %q declares `respond_within:`, but this workflow declares no `outputs:`; "+
			"the declared outputs are the only thing a run can answer with, so declare the ones the caller "+
			"needs, or drop `respond_within:` to answer with the run's address alone", name)
	}

	return nil
}
