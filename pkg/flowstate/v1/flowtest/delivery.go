package flowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
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
// The raw bytes of the body come back beside the decoded delivery, because
// they are what a computed verification signs over ([replayDelivery]): a
// signature is over bytes, not over a document, and a re-encoded body differs
// from the stored one for reasons nobody can see — the identical argument
// [v1.VerifyWebhookDelivery] makes for the receiver reading its body once.
//
// Two spellings carry them, strictly one per fixture. `body` embeds the
// payload as a JSON value, and its bytes are exactly the fixture's own
// spelling of that value — the common case, readable in the fixture. What an
// embedded value can never carry is the whitespace *around* a captured HTTP
// body: a sender signs `  {"id":1}` followed by a newline as those exact
// bytes, and a JSON value's decoder owns the surrounding whitespace, so a
// genuine captured signature would fail offline with nothing visibly wrong
// (Codex, #1109). `raw_body` is the exact-bytes door for that capture: a JSON
// string holding the body verbatim, whitespace and all, whose contents must
// still decode as JSON for the `${event.body...}` mappings. Declaring both is
// refused naming both, since two spellings of one body is the
// two-sources-of-truth bug as a fixture.
//
// Verified is left false here and set by the case or computed from its bound
// keys, deliberately: this function knows what arrived, not whether it was
// genuine. See [TriggerDelivery].
func loadDelivery(path string) (v1.WebhookDelivery, []byte, error) {
	data, err := readBounded(path, v1.MaxWebhookPayloadBytes, "delivery")
	if err != nil {
		return v1.WebhookDelivery{}, nil, fmt.Errorf("reading the delivery: %w", err)
	}

	// Decoded strictly, so a fixture that spells `header:` or `payload:` is a
	// refusal naming the mistake rather than a delivery with no headers at all —
	// which would surface a whole mapping later as an idempotency key that does
	// not resolve, sending the author to the workflow rather than to the fixture.
	var stored struct {
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
		RawBody *string           `json:"raw_body"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return v1.WebhookDelivery{}, nil, fmt.Errorf(
			"the delivery %s is not one JSON document with `headers` and `body` (or `raw_body`): %w", path, err)
	}

	raw := []byte(stored.Body)
	if stored.RawBody != nil {
		if len(stored.Body) > 0 {
			return v1.WebhookDelivery{}, nil, fmt.Errorf(
				"the delivery %s declares both `body` and `raw_body`, and a signature can only be over one "+
					"byte sequence; keep `raw_body` for a captured body's exact bytes, or `body` for a "+
					"payload this fixture spells itself", path)
		}
		raw = []byte(*stored.RawBody)
	}

	// An empty body takes the receiver's answer too: decodeDeliveryBody always
	// decodes, and zero bytes fail it with EOF, so a fixture with no body — an
	// absent `body`, or `raw_body: ""` — would rehearse a delivery production
	// refuses, and could even verify an HMAC over the empty sequence on the
	// way (Codex, #1109). Refused with the guidance the decoder's bare EOF
	// would not give.
	if len(raw) == 0 {
		return v1.WebhookDelivery{}, nil, fmt.Errorf(
			"the delivery %s carries no body, and the receiver refuses an empty delivery body, so a "+
				"rehearsal must too; store the payload under `body`, or a captured one under `raw_body`", path)
	}

	// Numbers read the way a payload reads them rather than as float64s: see
	// [v1.NormalizeDeliveryNumbers], which the live receiver applies to the
	// identical decode so that a replayed delivery and a real one produce the same
	// value for `"amount": 4200`.
	var body any
	{
		bodyDecoder := json.NewDecoder(bytes.NewReader(raw))
		bodyDecoder.UseNumber()
		if err := bodyDecoder.Decode(&body); err != nil {
			return v1.WebhookDelivery{}, nil, fmt.Errorf(
				"the delivery %s carries a body that will not decode: %w", path, err)
		}

		// And nothing after it — the receiver's own rule (decodeDeliveryBody,
		// server/webhook.go), which refuses `{"id":"a"} {"id":"b"}` as more
		// than one document rather than starting a run from the prefix. A
		// `body` value cannot carry a trailing document (a RawMessage is one
		// token by construction), but `raw_body` is arbitrary bytes, and a
		// rehearsal accepting what production refuses is the rehearsal lying
		// about production (Codex, #1109).
		var trailing json.RawMessage
		if err := bodyDecoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return v1.WebhookDelivery{}, nil, fmt.Errorf(
				"the delivery %s carries more than one JSON document in its body; the receiver refuses "+
					"such a delivery, so a rehearsal must too — store one document per delivery", path)
		}
	}

	return v1.WebhookDelivery{Headers: stored.Headers, Body: v1.NormalizeDeliveryNumbers(body)}, raw, nil
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
//
// The delivery id returned beside the inputs is what the run then reports as
// `${trigger.delivery_id}`: [v1.WebhookDeliveryID] over the same evaluated key,
// which is the function the live receiver calls, so a case asserting on it is
// asserting against production's answer and not against a value this harness
// invented.
func replayDelivery(test *Test, deliveryPath string, workflow *v1.Workflow) (map[string]*v1.Value, string, []*v1.Diagnostic, error) {
	trigger, declared := v1.FindWebhookTrigger(workflow, test.Trigger.Webhook)
	if !declared {
		names := v1.WebhookTriggerNames(workflow)
		declares := "declares no webhook triggers at all"
		if len(names) > 0 {
			declares = "declares " + strings.Join(names, ", ")
		}

		return nil, "", nil, fmt.Errorf("trigger %q: workflow %q %s",
			test.Trigger.Webhook, workflow.GetName(), declares)
	}

	delivery, rawBody, err := loadDelivery(deliveryPath)
	if err != nil {
		return nil, "", nil, fmt.Errorf("trigger %q: %w", test.Trigger.Webhook, err)
	}

	// The verification outcome: computed when the case binds every key the
	// trigger's `verify:` names, declared otherwise (#935). Computed through
	// [v1.VerifyWebhookDelivery] — the function the served receiver calls —
	// over the fixture's exact `body` bytes, at the virtual clock's epoch, so
	// a timestamped scheme's fixture pins its timestamp near the epoch and
	// stays deterministic forever. A case that binds the keys and also
	// declares `signature:` is refused, naming both: a declaration that could
	// contradict the arithmetic is the two-spellings bug as a test fixture.
	// Binding some keys and not others is refused too, rather than quietly
	// falling back to the declared rehearsal a partially-bound author did not
	// mean to write.
	var verifyErr error
	keys, missing := caseVerifyKeys(test, trigger)
	switch {
	case len(trigger.GetVerify()) > 0 && len(missing) == 0:
		if test.Trigger.Signature != "" {
			return nil, "", nil, fmt.Errorf("trigger %q: this case binds every key `verify:` names, so "+
				"the verification outcome is computed by the same function the receiver runs — and it also "+
				"declares `signature: %s`, which could contradict that arithmetic. Drop `signature:` to "+
				"keep the computed outcome, or drop the bound keys to keep the declared rehearsal",
				test.Trigger.Webhook, test.Trigger.Signature)
		}
		verifyErr = v1.VerifyWebhookDelivery(trigger, keys, delivery.Headers, rawBody, epoch)
		delivery.Verified = verifyErr == nil

	case len(keys) > 0:
		return nil, "", nil, fmt.Errorf("trigger %q: this case binds %d of the %d keys `verify:` names, "+
			"and a partly-checkable delivery is not a rehearsal of anything; bind %s too, or bind none "+
			"and declare the outcome with `signature:`",
			test.Trigger.Webhook, len(keys), len(trigger.GetVerify()), strings.Join(missing, ", "))

	default:
		delivery.Verified = test.Trigger.Verified()
	}

	wantRefused := test.Expect.Refused != nil && *test.Expect.Refused

	// The same function a live receiver will call, with the same fail-closed
	// order: the trigger's own declaration, then verification, then — and only
	// then — anything the delivery chose.
	bound, key, bindErr := v1.BindWebhookTriggerInputs(context.Background(), workflow, trigger, delivery)
	if bindErr != nil {
		if wantRefused {
			return nil, "", nil, nil
		}

		message := fmt.Sprintf("the delivery did not start a run: %v", bindErr)
		if verifyErr != nil {
			// The computed outcome is what refused it, so the diagnostic names
			// the arithmetic's own reason and the cost the fixture carries: the
			// signed bytes are the fixture's exact `body` spelling, so an
			// edited payload needs re-signing under the bound key.
			message = fmt.Sprintf("the delivery did not verify against the case's bound keys: %v. "+
				"The signature covers the fixture's exact `body` bytes, so an edited payload must be "+
				"re-signed with the key this case binds", verifyErr)
		}

		return nil, "", []*v1.Diagnostic{{
			Field:   "trigger",
			Value:   test.Trigger.Webhook,
			Message: message,
		}}, nil
	}

	if wantRefused {
		return nil, "", []*v1.Diagnostic{{
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
		return nil, "", failures, nil
	}

	return bound, v1.WebhookDeliveryID(key), nil, nil
}

// caseVerifyKeys resolves the trigger's `verify:` keys against the case's own
// `secrets:` — the same reference boundary a stubbed task's `${secret(...)}`
// resolves at, reused rather than invented (#321): the key is bound by the
// reference's text form, exactly as the workflow names it.
//
// missing names what `verify:` needs and the case does not bind, so the caller
// can tell "computed" (everything bound) from "declared" (nothing bound) from
// the partial binding it refuses.
func caseVerifyKeys(test *Test, trigger *v1.WebhookTrigger) (map[string]secrets.Secret, []string) {
	keys := make(map[string]secrets.Secret, len(trigger.GetVerify()))
	var missing []string
	for scheme, value := range trigger.GetVerify() {
		ref := value.GetSecretRef()
		if ref == nil {
			// CheckWebhookTrigger refuses this shape; unresolvable here means
			// unbindable, which the caller reports as missing.
			missing = append(missing, scheme)
			continue
		}
		text := secrets.RefString(ref)
		plaintext, bound := test.Secrets[text]
		if !bound {
			missing = append(missing, text)
			continue
		}
		keys[scheme] = secrets.NewSecret(ref, plaintext)
	}
	slices.Sort(missing)

	return keys, missing
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
