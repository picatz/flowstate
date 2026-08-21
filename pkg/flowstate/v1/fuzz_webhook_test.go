package flowstatev1_test

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FuzzWebhookEventBinding fuzzes the path an unauthenticated HTTP delivery takes
// from raw bytes to a CEL activation: [v1.NewWebhookEvent] turning a decoded JSON
// body into a nested [v1.Value] tree, and [v1.BindWebhookTriggerInputs] binding
// that tree as `event` and evaluating a trigger's `idempotency_key:` and
// `arguments:` against it.
//
// Every other fuzz target in this repository covers a CLI-only or plugin-local
// input — a Flowfile an author wrote, an MCP client's arguments, a plugin's own
// descriptor. This is the one surface where the bytes belong to whoever can reach
// the (not yet built) receiver, no authentication required until the signature
// check that happens *before* this path runs — [v1.BindWebhookTriggerInputs]
// refuses an unverified delivery first — so what is fuzzed here is deliberately
// the half that follows verification, the same half `flow test` already replays a
// stored delivery through in flowtest/delivery.go.
//
// The trigger is fixed, matching the issue this target was written for
// (picatz/flowstate#799): only the delivery — headers and body — is fuzzed, so a
// finding here is about the JSON→Value→CEL conversion and not about the checker
// that reads a trigger's own declaration (that is [FuzzCELCompile]'s and
// webhook_test.go's territory, covered elsewhere).
//
// What this is not: #403's item 2 ("CEL evaluation... fuzz eval of compiled
// expressions over fuzzed activation values") fuzzes an arbitrary activation
// against an arbitrary compiled expression in general; this target fixes both
// the expression *and* the activation's shape (an `event` built the one way a
// webhook ever builds one) and fuzzes only the untrusted half — the delivery.
// Nor is it #403's item 4 (signal payload decoding and Value proto round-trips),
// which is the signal API surface, a different transport with a different
// decode path entirely. Neither of #403's five listed items covers this.
//
// The bound named in the issue as gating this path is maxActivationDepth
// (unexported, eval.go) — but reading the path this target actually drives
// shows the walk that bound guards (resolving a *stored expression* referenced
// from another stored expression) is not the one a webhook body reaches at all:
// a delivery's body is data, not a stored expression, so it is never handed to
// StepsOutputActivation.resolveValue. The bound that *is* on this path, and
// that [v1.CheckWebhookTrigger] enforces on the trigger's own `idempotency_key:`
// syntax before any delivery is ever accepted, is maxIdentifierWalkDepth
// (webhook.go) — 32, "the same bound and the same argument as
// maxActivationDepth" per its own doc comment, but a distinct constant, and one
// this target's fixed trigger ([fuzzWebhookTrigger]) never approaches because it
// writes a four-token expression.
//
// What actually stands between an attacker-shaped body and a crash, measured
// before this target was written: encoding/json's own decoder refuses to decode
// past 10,000 levels of nesting (verified empirically: depth 10000 decodes,
// 10001 fails with "exceeded max depth"), and past roughly half that,
// eventRefValue's conversion of the built [v1.Value] into a CEL ref.Val — which
// goes through a protobuf marshal path — hits protobuf-go's own recursion guard
// and returns "proto: exceeded maximum recursion depth" as an ordinary error
// value, which [BindWebhookTriggerInputs] then reports as a failed evaluation
// rather than crashing on. Neither [v1.NewValue], [v1.NewLiteralMap] nor
// [v1.NewLiteralList] carries an explicit depth bound of its own — confirmed by
// reading them — so what protects this path today is two bounds this package
// does not own (the standard library's JSON nesting cap, and protobuf-go's own
// recursion guard) rather than one this package wrote for the purpose. That is
// the finding this target exists to keep tested: nothing here crashes today, but
// nothing here would notice if either of those borrowed bounds moved.
//
// The invariant under fuzz: no panic, no hang, and no allocation past what
// GOMEMLIMIT allows for any byte sequence handed to it as a webhook body. A
// refusal — bad JSON, a type CEL cannot convert, an idempotency key that will
// not evaluate — is an ordinary, expected answer and is not asserted against;
// there is no oracle here for what a malformed delivery should produce beyond
// "must not cost more than decoding a bounded JSON document should."
func FuzzWebhookEventBinding(f *testing.F) {
	workflow := &v1.Workflow{
		Name:    "webhook-fuzz-target",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "order_id", Type: v1.InputDeclaration_TYPE_STRING},
			{Name: "amount", Type: v1.InputDeclaration_TYPE_STRING},
		},
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{fuzzWebhookTrigger()}},
	}
	trigger := workflow.GetTriggers().GetWebhooks()[0]

	for _, seed := range []string{
		// The well-formed shape: headers carrying the signature the idempotency
		// key reads, a body carrying the two arguments.
		`{"headers":{"Stripe-Signature":"t=1,v1=abc"},"body":{"data":{"object":{"amount":4200,"metadata":{"order_id":"ord_H1x9"}}}}}`,
		// Both halves empty, and both halves absent.
		`{"headers":{},"body":{}}`,
		`{}`,
		// A body that is not an object at all — a bare scalar, and a bare array
		// — since `body` is `any` and NewWebhookEvent must handle whatever
		// encoding/json decoded it to.
		`{"headers":{"Stripe-Signature":"x"},"body":42}`,
		`{"headers":{"Stripe-Signature":"x"},"body":"a string body"}`,
		`{"headers":{"Stripe-Signature":"x"},"body":[1,2,3]}`,
		`{"headers":{"Stripe-Signature":"x"},"body":null}`,
		// A header whose value looks numeric or boolean, which JSON would decode
		// as one of those if headers were `any` — pinned as strings by the
		// struct tag, so this exercises the decoder's own type refusal rather
		// than the conversion, and is here so the fuzzer starts from a case that
		// is rejected one step earlier than the others.
		`{"headers":{"Stripe-Signature":123},"body":{}}`,
		// Deeply nested, both shapes CLAUDE.md calls out — arrays and objects —
		// at three points relative to where this path is measured to actually
		// fail closed: comfortably under it, one step past where the CEL
		// conversion starts erroring instead of succeeding, and at
		// encoding/json's own ceiling (10000; one past that is refused before
		// any of this package ever sees it, so seeding past it would only
		// exercise the standard library's decoder, not this path).
		fuzzWebhookDeepBody(1000, '[', ']'),
		fuzzWebhookDeepBody(5000, '[', ']'),
		fuzzWebhookDeepBody(9999, '[', ']'),
		fuzzWebhookDeepBody(1000, '{', '}'),
		fuzzWebhookDeepBody(5000, '{', '}'),
		fuzzWebhookDeepBody(9999, '{', '}'),
		// Wide rather than deep: breadth is the resource a depth bound does
		// nothing about, so a seed that is flat and enormous covers the
		// direction the deep ones cannot.
		fuzzWebhookWideBody(20000),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		data := []byte(raw)

		// The bound a live receiver will apply to a request body before
		// reading it into memory at all — see [v1.MaxWebhookPayloadBytes]'s own
		// doc comment. Applied here for the same reason: bytes are the
		// resource an attacker controls before any of the rest of this
		// function runs, so this target bounds them exactly where the eventual
		// receiver must.
		if len(data) > v1.MaxWebhookPayloadBytes {
			return
		}

		// Decoded the same way flowtest/delivery.go's loadDelivery decodes a
		// stored one — the reference implementation [v1.MaxWebhookPayloadBytes]'s
		// doc comment names — so a finding here is a finding about the shared
		// path and not an artifact of a fuzz-only decode.
		var stored struct {
			Headers map[string]string `json:"headers"`
			Body    any               `json:"body"`
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		decoder.UseNumber()
		if err := decoder.Decode(&stored); err != nil {
			return
		}

		delivery := v1.WebhookDelivery{
			Headers: stored.Headers,
			Body:    v1.NormalizeDeliveryNumbers(stored.Body),
			// Forced true: verification is a different check
			// (webhookverify_test.go's territory), and this target exists to
			// fuzz what runs *after* it, per the issue's own framing — a fixed
			// trigger, a fuzzed delivery.
			Verified: true,
		}

		// The whole path under fuzz: NewWebhookEvent builds the Value tree,
		// then the idempotency key and both arguments are evaluated against it.
		// A refusal is an ordinary answer for fuzzed bytes; a panic, a hang, or
		// memory past GOMEMLIMIT is not.
		inputs, key, err := v1.BindWebhookTriggerInputs(t.Context(), workflow, trigger, delivery)
		_ = inputs
		_ = key
		_ = err
	})
}

// fuzzWebhookTrigger is this target's fixed trigger: a real idempotency-key
// expression reading a header, and two arguments reading into the body at
// different depths, so both the key path and the argument path in
// [v1.BindWebhookTriggerInputs] are exercised by every fuzzed delivery.
func fuzzWebhookTrigger() *v1.WebhookTrigger {
	return &v1.WebhookTrigger{
		Name: "fuzzed",
		Verify: map[string]*v1.Value{
			v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
				SecretRef: &v1.SecretRef{Scheme: "env", Name: "FUZZ_WEBHOOK_SECRET"},
			}},
		},
		IdempotencyKey: v1.NewExpr(`event.headers["stripe-signature"]`),
		Arguments: map[string]*v1.Value{
			"order_id": v1.NewExpr(`string(event.body.data.object.metadata.order_id)`),
			"amount":   v1.NewExpr(`string(event.body.data.object.amount)`),
		},
	}
}

// fuzzWebhookDeepBody renders one JSON document with a signature header and a
// body nested n levels deep using the given open/close bracket pair — '[' ']'
// for an array, '{' '}' for an object (each object level repeats one key, "k",
// so the shape is a single deep chain rather than a wide tree at every level).
func fuzzWebhookDeepBody(n int, openByte, closeByte byte) string {
	var b strings.Builder
	b.WriteString(`{"headers":{"stripe-signature":"deep` + strconv.Itoa(n) + `"},"body":`)
	for range n {
		b.WriteByte(openByte)
		if openByte == '{' {
			b.WriteString(`"k":`)
		}
	}
	b.WriteByte('0')
	for range n {
		b.WriteByte(closeByte)
	}
	b.WriteString(`}`)
	return b.String()
}

// fuzzWebhookWideBody renders one JSON document whose body is a single object
// with n distinct keys — flat breadth rather than nesting, so the fuzzer starts
// from a shape a depth bound would do nothing about.
func fuzzWebhookWideBody(n int) string {
	var b strings.Builder
	b.WriteString(`{"headers":{"stripe-signature":"wide` + strconv.Itoa(n) + `"},"body":{`)
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"k` + strconv.Itoa(i) + `":0`)
	}
	b.WriteString(`}}`)
	return b.String()
}
