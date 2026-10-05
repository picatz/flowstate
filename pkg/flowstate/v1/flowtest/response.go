package flowtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ResponseExpectation asserts the document a waiting receiver would answer a
// replayed delivery with, for a webhook that declares `respond_within:`.
//
// The document is built by [v1.WebhookResponse], the function the receiver
// calls, from the run this case rehearsed: a run that completed answers
// `completed` with its declared outputs, one that failed answers `failed`, and
// one parked at a `wait_for_signal:` that no `signals:` entry answers is the
// run a caller would still be waiting on at the bound, `running`. What is not
// rehearsed is the wait itself, which is the receiver's alone: `flow test` has
// no listener and no run that outlives its case, so how *long* a run takes
// against the bound is not something a case can assert.
type ResponseExpectation struct {
	// Status is `completed`, `failed` or `running`: what the run was doing when
	// the receiver answered.
	Status string `yaml:"status"`

	// Outputs, for a `completed` run, must equal the document's `outputs`
	// exactly, in both directions as [Expectation.Outputs] is. A sensitive
	// output is compared as the document carries it: withheld.
	Outputs map[string]any `yaml:"outputs"`
}

// checkResponseClaim refuses a `response:` that cannot mean what it says, when
// the file loads.
func checkResponseClaim(p *problems, r site, test *Test) {
	want := test.Expect.Response
	if want == nil {
		return
	}

	at := r.in(r.at.field("expect").field("response"))
	known := []string{string(v1.WebhookRunCompleted), string(v1.WebhookRunFailed), string(v1.WebhookRunRunning)}

	switch {
	case want.Status == "":
		p.report(at, "test %q expect.response: names no `status:`; write one of %s", test.Name, strings.Join(known, ", "))
	case !slices.Contains(known, want.Status):
		p.report(at.in(at.at.field("status")), "test %q expect.response: status: %q is not a status a receiver "+
			"answers with; the statuses are %s", test.Name, want.Status, strings.Join(known, ", "))
	case want.Outputs != nil && want.Status != string(v1.WebhookRunCompleted):
		p.report(at.in(at.at.field("outputs")), "test %q expect.response: expects outputs from a run that is "+
			"%q; only a completed run's answer carries them", test.Name, want.Status)
	}
}

// assertResponse builds the response document the rehearsed run would have
// been answered with and compares it with the case's claim.
//
// accepted is the address the receiver would have written: the delivery id is
// the one [v1.WebhookDeliveryID] computed, and the run has no id of its own
// here. A webhook that declares no `respond_within:` has no document to
// assert, which is a failure of the case rather than a vacuous pass.
func assertResponse(want *ResponseExpectation, workflow *v1.Workflow, webhook, deliveryID string,
	inputs map[string]*v1.Value, outputs *v1.Workflow_StepOutputs, runErr error, parked bool,
) []*v1.Diagnostic {
	trigger, _ := v1.FindWebhookTrigger(workflow, webhook)
	if trigger.GetRespondWithin() == nil {
		return []*v1.Diagnostic{{
			Field: "expect.response",
			Value: webhook,
			Message: fmt.Sprintf("webhook %q declares no `respond_within:`, so its receiver answers with the "+
				"run's address alone and there is no document to assert; add `respond_within:` to the "+
				"trigger, or drop `response:` from the case", webhook),
		}}
	}

	run := v1.WebhookRun{Status: v1.WebhookRunCompleted, Outputs: outputs.GetRunOutputs()}
	switch {
	case parked:
		run = v1.WebhookRun{Status: v1.WebhookRunRunning}
	case runErr != nil:
		run = v1.WebhookRun{Status: v1.WebhookRunFailed, Failure: runErr.Error(), Inputs: inputs}
	}

	document := v1.WebhookResponse(v1.AcceptedDelivery{DeliveryID: deliveryID}, run, workflow)

	var failures []*v1.Diagnostic
	if string(document.Status) != want.Status {
		failures = append(failures, &v1.Diagnostic{
			Field: "expect.response.status",
			Message: fmt.Sprintf("expected the receiver to answer %q, but this run answers %q",
				want.Status, document.Status),
		})
	}

	if want.Outputs != nil {
		failures = append(failures, compareResponseOutputs(want.Outputs, document)...)
	}

	return failures
}

// compareResponseOutputs checks a document's `outputs` against a claim, in both
// directions, reading the JSON the receiver would write rather than the values
// the run held: what a caller is told is what is asserted.
func compareResponseOutputs(want map[string]any, document v1.WebhookResponseDocument) []*v1.Diagnostic {
	if document.Status != v1.WebhookRunCompleted {
		return []*v1.Diagnostic{{
			Field:   "expect.response.outputs",
			Message: fmt.Sprintf("expected outputs, but a %q answer carries none", document.Status),
		}}
	}

	decoder := json.NewDecoder(bytes.NewReader(document.Outputs))
	decoder.UseNumber()
	var got map[string]any
	if err := decoder.Decode(&got); err != nil {
		return []*v1.Diagnostic{{
			Field:   "expect.response.outputs",
			Message: fmt.Sprintf("the answer's outputs are not a JSON object: %v", err),
		}}
	}

	var failures []*v1.Diagnostic
	for name, wantValue := range want {
		gotValue, ok := got[name]
		if !ok {
			failures = append(failures, &v1.Diagnostic{
				Field: "expect.response.outputs", Value: name,
				Message: fmt.Sprintf("expected output %q, but the answer carries none", name),
			})
			continue
		}
		if !looseEqual(wantValue, jsonNative(gotValue)) {
			failures = append(failures, &v1.Diagnostic{
				Field: "expect.response.outputs", Value: name,
				Message: fmt.Sprintf("output %q: expected %s, the answer carries %s", name,
					typedText(wantValue, sensitiveInputs{}), typedText(jsonNative(gotValue), sensitiveInputs{})),
			})
		}
	}
	for _, name := range slices.Sorted(maps.Keys(got)) {
		if _, expected := want[name]; !expected {
			failures = append(failures, &v1.Diagnostic{
				Field: "expect.response.outputs", Value: name,
				Message: fmt.Sprintf("unexpected output %q, which expect.response.outputs does not name", name),
			})
		}
	}

	return failures
}

// jsonNative turns a decoded JSON value into the natives [looseEqual] compares:
// a number becomes an int64 where it is one and a float64 where it is not, so
// an integer is compared exactly rather than through a float.
func jsonNative(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}

		return v.String()
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = jsonNative(item)
		}

		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = jsonNative(item)
		}

		return out
	default:
		return value
	}
}
