package flowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Replaying a stored delivery is what makes a trigger's argument mapping a unit
// test instead of the one part of a workflow debuggable only in production.
//
// A delivery on disk is read here and nowhere else, under the byte bound a live
// receiver will apply to a request body ([v1.MaxWebhookPayloadBytes]). It is the
// ordinary treatment of untrusted input in this repository, and a fixture is
// untrusted like anything else: a `*.test.yaml` and its testdata can arrive with a
// called workflow's repository or out of a fork.
//
// The bound is on the *stream* — see [readBounded], which is also where the two
// ways a size-then-read bound is not a bound are written down. Briefly: a symlink
// to `/dev/zero` stats as nothing and reads forever, and a file replaced between
// the two calls is bounded by the size of the file that is gone.

// loadDelivery reads a stored delivery: one JSON document with `headers` and
// `body`.
//
// Both halves, because a delivery is both. An idempotency key is usually a
// signature header — `${event.headers["stripe-signature"]}` — so a fixture holding
// only a body could not exercise the required field at all, and a case would be
// asserting the easy half of the mapping.
//
// Verified is left false here and set by the case, deliberately: this function
// knows what arrived, not whether it was genuine. See [TriggerDelivery].
func loadDelivery(path string) (v1.WebhookDelivery, error) {
	data, err := readBounded(path, v1.MaxWebhookPayloadBytes, "delivery")
	if err != nil {
		return v1.WebhookDelivery{}, fmt.Errorf("reading the delivery: %w", err)
	}

	// Decoded strictly, so a fixture that spells `header:` or `payload:` is a
	// refusal naming the mistake rather than a delivery with no headers at all —
	// which would surface a whole mapping later as an idempotency key that does
	// not resolve, sending the author to the workflow rather than to the fixture.
	var stored struct {
		Headers map[string]string `json:"headers"`
		Body    any               `json:"body"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	// Numbers read the way a payload reads them rather than as float64s: see
	// [v1.NormalizeDeliveryNumbers], which the live receiver will apply to the
	// identical decode so that a replayed delivery and a real one produce the same
	// value for `"amount": 4200`.
	decoder.UseNumber()
	if err := decoder.Decode(&stored); err != nil {
		return v1.WebhookDelivery{}, fmt.Errorf(
			"the delivery %s is not one JSON document with `headers` and `body`: %w", path, err)
	}

	return v1.WebhookDelivery{Headers: stored.Headers, Body: v1.NormalizeDeliveryNumbers(stored.Body)}, nil
}

// replayDelivery runs one trigger case up to the point a run would start.
//
// The three returns say three different things, and keeping them apart is what
// makes a refusal a *verdict* rather than an error:
//
//   - err is a mistake in the case or its fixture — a webhook the workflow does
//     not declare, a delivery that will not read — which is reported the way a
//     failure to compile a workflow is, because there is no assertion to reach.
//   - failures are assertions that did not hold.
//   - inputs are what the run starts with, non-nil only when the delivery was
//     accepted and every assertion about it held. Nil with no failures is the
//     `refused: true` case passing: no run happens, and none should.
func replayDelivery(test *Test, deliveryPath string, workflow *v1.Workflow) (map[string]*v1.Value, []*v1.Diagnostic, error) {
	trigger, declared := v1.FindWebhookTrigger(workflow, test.Trigger.Webhook)
	if !declared {
		names := v1.WebhookTriggerNames(workflow)
		declares := "declares no webhook triggers at all"
		if len(names) > 0 {
			declares = "declares " + strings.Join(names, ", ")
		}

		return nil, nil, fmt.Errorf("trigger %q: workflow %q %s",
			test.Trigger.Webhook, workflow.GetName(), declares)
	}

	delivery, err := loadDelivery(deliveryPath)
	if err != nil {
		return nil, nil, fmt.Errorf("trigger %q: %w", test.Trigger.Webhook, err)
	}
	delivery.Verified = test.Trigger.Verified()

	wantRefused := test.Expect.Refused != nil && *test.Expect.Refused

	// The same function a live receiver will call, with the same fail-closed
	// order: the trigger's own declaration, then verification, then — and only
	// then — anything the delivery chose.
	bound, key, bindErr := v1.BindWebhookTriggerInputs(context.Background(), workflow, trigger, delivery)
	if bindErr != nil {
		if wantRefused {
			return nil, nil, nil
		}

		return nil, []*v1.Diagnostic{{
			Field:   "trigger",
			Value:   test.Trigger.Webhook,
			Message: fmt.Sprintf("the delivery did not start a run: %v", bindErr),
		}}, nil
	}

	if wantRefused {
		return nil, []*v1.Diagnostic{{
			Field: "expect.refused",
			Value: test.Trigger.Webhook,
			Message: "expected the delivery to be refused, but it was accepted and mapped to inputs; " +
				"a case asserting a refusal that does not happen is the one that must fail loudly",
		}}, nil
	}

	var failures []*v1.Diagnostic
	if test.Expect.IdempotencyKey != "" && test.Expect.IdempotencyKey != key {
		failures = append(failures, &v1.Diagnostic{
			Field:   "expect.idempotency_key",
			Message: fmt.Sprintf("expected the delivery to be named %q, got %q", test.Expect.IdempotencyKey, key),
		})
	}
	if test.Expect.Inputs != nil {
		failures = append(failures, compareInputs(test.Expect.Inputs, bound)...)
	}
	if len(failures) > 0 {
		// The mapping is what a trigger case is about, so a case whose mapping is
		// wrong stops here rather than running the workflow on values it has
		// already reported as the wrong ones.
		return nil, failures, nil
	}

	return bound, nil, nil
}

// compareInputs checks the inputs a delivery produced against what a case
// expected, in both directions.
//
// Both, for the reason [compareOutputs] checks both: a case naming two inputs and
// getting a third is a case whose workflow no longer matches the mapping the case
// was written against — most often because an input was added and this call site
// was never updated, which is exactly the drift `with:` exists to make visible.
func compareInputs(want map[string]any, got map[string]*v1.Value) []*v1.Diagnostic {
	var failures []*v1.Diagnostic

	for _, name := range slices.Sorted(maps.Keys(want)) {
		value, ok := got[name]
		if !ok {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.inputs",
				Value:   name,
				Message: fmt.Sprintf("expected input %q, but the delivery bound none", name),
			})
			continue
		}
		native, err := literalToGo(value.GetLiteral())
		if err != nil {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.inputs",
				Value:   name,
				Message: fmt.Sprintf("input %q: could not compare: %v", name, err),
			})
			continue
		}
		if !looseEqual(want[name], native) {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.inputs",
				Value:   name,
				Message: fmt.Sprintf("input %q: expected %v, got %v", name, want[name], native),
			})
		}
	}

	for _, name := range slices.Sorted(maps.Keys(got)) {
		if _, expected := want[name]; !expected {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.inputs",
				Value:   name,
				Message: fmt.Sprintf("unexpected input %q, which expect.inputs does not name", name),
			})
		}
	}

	return failures
}
