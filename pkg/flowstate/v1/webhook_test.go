package flowstatev1_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// The mapping from a delivery to a run's inputs is the whole of what a webhook
// trigger does, and it is a pure function — which is what makes it testable with
// no network, no server and no receiver. These are the claims a live receiver will
// inherit rather than restate.

// stripeTrigger is a well-formed declaration: the shape the corpus example ships.
func stripeTrigger() *v1.WebhookTrigger {
	return &v1.WebhookTrigger{
		Name: "stripe",
		Verify: map[string]*v1.Value{
			v1.WebhookSchemeStripe: {Kind: &v1.Value_SecretRef{
				SecretRef: &v1.SecretRef{Scheme: "env", Name: "STRIPE_WEBHOOK_SECRET"},
			}},
		},
		IdempotencyKey: v1.NewExpr(`event.headers["stripe-signature"]`),
		Arguments: map[string]*v1.Value{
			"order_id": v1.NewExpr(`event.body.data.object.metadata.order_id`),
			"amount":   v1.NewExpr(`event.body.data.object.amount`),
		},
	}
}

func TestCheckWebhookIdempotencyKeyRejectsConstantExpression(t *testing.T) {
	t.Parallel()

	err := v1.CheckWebhookIdempotencyKey("constant", v1.NewExpr(`"all-events"`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not depend on the delivery")
	require.NoError(t, v1.CheckWebhookIdempotencyKey("derived", v1.NewExpr(`event.body.id`)))
}

// TestCheckWebhookIdempotencyKeyRejectsAConstantShadowedByAComprehension is the
// scope-aware half of the check above: a comprehension can bind an iteration
// variable named `event`, and every occurrence inside that comprehension's own
// body then refers to *that* binding, not to the delivery root. Left
// scope-naive, `[1].map(event, string(event))[0]` type-checks to the constant
// `"1"` and would pass as though it depended on the delivery — so every
// delivery would still be named alike, and no redelivery would ever be
// recognized as one.
func TestCheckWebhookIdempotencyKeyRejectsAConstantShadowedByAComprehension(t *testing.T) {
	t.Parallel()

	err := v1.CheckWebhookIdempotencyKey("shadowed", v1.NewExpr(`[1].map(event, string(event))[0]`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not depend on the delivery")

	// The same shape, genuinely depending on the delivery outside the
	// comprehension's own scope, must still be accepted.
	require.NoError(t, v1.CheckWebhookIdempotencyKey("derived",
		v1.NewExpr(`event.body.id + string([1].map(event, string(event))[0])`)))
}

// orderWorkflow declares the signature the trigger above is a call site of.
func orderWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "order-webhook",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "order_id", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "amount", Type: v1.InputDeclaration_TYPE_INT, Required: true},
			{Name: "currency", Type: v1.InputDeclaration_TYPE_STRING, Default: v1.NewLiteral("usd")},
		},
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{stripeTrigger()}},
		Steps: []*v1.Node{{
			Id:   "record",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`inputs.order_id`)},
		}},
	}
}

// stripeDelivery is one arrival, shaped the way a decoded JSON body is.
func stripeDelivery(verified bool) v1.WebhookDelivery {
	return v1.WebhookDelivery{
		Headers: map[string]string{"Stripe-Signature": "t=1,v1=abc"},
		Body: map[string]any{
			"data": map[string]any{
				"object": map[string]any{
					"amount":   int64(4200),
					"metadata": map[string]any{"order_id": "ord_H1x9"},
				},
			},
		},
		Verified: verified,
	}
}

// TestADeliveryBecomesTheRunsInputs is the happy path: the mapping evaluates, the
// key comes back, and the inputs are the *bound* ones — a declaration's default
// filled in for what the delivery does not carry, because that is what the run
// will actually see.
func TestADeliveryBecomesTheRunsInputs(t *testing.T) {
	t.Parallel()

	inputs, key, err := v1.BindWebhookTriggerInputs(
		t.Context(), orderWorkflow(), stripeTrigger(), stripeDelivery(true))
	require.NoError(t, err)

	assert.Equal(t, "t=1,v1=abc", key)
	assert.Equal(t, "ord_H1x9", inputs["order_id"].GetLiteral().GetStringValue())
	assert.Equal(t, int64(4200), inputs["amount"].GetLiteral().GetInt64Value())
	assert.Equal(t, "usd", inputs["currency"].GetLiteral().GetStringValue(),
		"the run starts with the declaration's default, so that is what a case sees")
}

// TestAnUnverifiedDeliveryIsRefused is the fail-closed rule, and the direction a
// functionality test would miss: not "a genuine delivery works" but "one that did
// not verify produces nothing".
//
// Refused *before* anything the delivery chose is evaluated, which is why the
// assertion is on the error rather than on partial inputs: there are none.
func TestAnUnverifiedDeliveryIsRefused(t *testing.T) {
	t.Parallel()

	inputs, key, err := v1.BindWebhookTriggerInputs(
		t.Context(), orderWorkflow(), stripeTrigger(), stripeDelivery(false))
	require.Error(t, err)

	assert.Nil(t, inputs)
	assert.Empty(t, key)
	assert.Contains(t, err.Error(), `delivery to webhook "stripe" did not verify`)
	assert.Contains(t, err.Error(), "never accepted on the grounds that it could not be checked")
}

// TestAMalformedTriggerRefusesEveryDelivery: the declaration is checked before the
// delivery is, so a webhook that could never verify anything cannot accept
// anything either — including one whose `verify:` block was emptied after the file
// was written, which is the shape a hand-built specification takes.
func TestAMalformedTriggerRefusesEveryDelivery(t *testing.T) {
	t.Parallel()

	trigger := stripeTrigger()
	trigger.Verify = nil

	_, _, err := v1.BindWebhookTriggerInputs(
		t.Context(), orderWorkflow(), trigger, stripeDelivery(true))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declares no `verify:`")
}

// TestADeliveryIsBoundThroughTheOneInputContract: a webhook lands in the same
// `inputs:` contract as `flow run --input` and an MCP invocation, with the same
// refusal. A trigger that could bypass it would make the contract decorative.
func TestADeliveryIsBoundThroughTheOneInputContract(t *testing.T) {
	t.Parallel()

	trigger := stripeTrigger()
	trigger.Arguments["amount"] = v1.NewExpr(`string(event.body.data.object.amount)`)

	_, _, err := v1.BindWebhookTriggerInputs(
		t.Context(), orderWorkflow(), trigger, stripeDelivery(true))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `input "amount" is declared int`)
}

// TestAnIdempotencyKeyMustNameSomething: an empty key names every delivery alike,
// so either all of them dedupe against each other or none does. Refused rather
// than accepted as "no key", because the file declared one.
func TestAnIdempotencyKeyMustNameSomething(t *testing.T) {
	t.Parallel()

	delivery := stripeDelivery(true)
	delivery.Headers = map[string]string{"Stripe-Signature": "   "}

	_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), stripeTrigger(), delivery)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "would name every delivery alike")
}

// TestHeadersAreMatchedWithoutRegardToCase, which is the one normalization the
// mapping performs: HTTP header names are case-insensitive, so a stored delivery
// and a live one must agree about what `event.headers["stripe-signature"]` finds.
func TestHeadersAreMatchedWithoutRegardToCase(t *testing.T) {
	t.Parallel()

	delivery := stripeDelivery(true)
	delivery.Headers = map[string]string{"STRIPE-SIGNATURE": "t=2,v1=def"}

	_, key, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), stripeTrigger(), delivery)
	require.NoError(t, err)
	assert.Equal(t, "t=2,v1=def", key)
}

// TestATriggerExpressionSeesOnlyTheEvent: `event` is the whole scope. A trigger is
// evaluated before the run exists, so a reference to a step or an input is an
// error at the moment it is evaluated, not a silent empty value that a workflow
// would then act on.
func TestATriggerExpressionSeesOnlyTheEvent(t *testing.T) {
	t.Parallel()

	trigger := stripeTrigger()
	trigger.Arguments["order_id"] = v1.NewExpr(`steps.record.value`)

	_, _, err := v1.BindWebhookTriggerInputs(
		t.Context(), orderWorkflow(), trigger, stripeDelivery(true))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `webhook "stripe": input "order_id"`)
}

// TestAWebhookIsCheckedForWhatAFileControls covers the checks `flow validate`
// reports against a position, at the other of their two moments: a specification
// that never was a Flowfile.
func TestAWebhookIsCheckedForWhatAFileControls(t *testing.T) {
	t.Parallel()

	t.Run("a scheme nothing implements", func(t *testing.T) {
		t.Parallel()

		trigger := stripeTrigger()
		trigger.Verify = map[string]*v1.Value{"rot13": trigger.GetVerify()[v1.WebhookSchemeStripe]}

		err := v1.CheckWebhookTrigger(trigger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), strings.Join(v1.WebhookVerificationSchemes(), ", "))
	})

	t.Run("a signing key written in the file", func(t *testing.T) {
		t.Parallel()

		trigger := stripeTrigger()
		trigger.Verify = map[string]*v1.Value{v1.WebhookSchemeStripe: v1.NewLiteral("whsec_0123")}

		err := v1.CheckWebhookTrigger(trigger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "using a value written in the file")
	})

	t.Run("no idempotency key", func(t *testing.T) {
		t.Parallel()

		trigger := stripeTrigger()
		trigger.IdempotencyKey = nil

		err := v1.CheckWebhookTrigger(trigger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at-least-once")
	})

	t.Run("two sources sharing a name", func(t *testing.T) {
		t.Parallel()

		err := v1.CheckWebhookTriggers(&v1.Triggers{
			Webhooks: []*v1.WebhookTrigger{stripeTrigger(), stripeTrigger()},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is declared twice")
	})
}

// TestADeliverysNumbersReadAsNumbers pins the conversion that makes a mapping
// correct in the file and correct on arrival: JSON's only number type is a float,
// and an input declared `int` would refuse every whole number a sender writes.
func TestADeliverysNumbersReadAsNumbers(t *testing.T) {
	t.Parallel()

	decoded := v1.NormalizeDeliveryNumbers(map[string]any{
		"whole":    jsonNumber("4200"),
		"fraction": jsonNumber("42.5"),
		"nested":   []any{jsonNumber("1"), map[string]any{"deep": jsonNumber("2")}},
		"text":     "4200",
	})

	object, ok := decoded.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(4200), object["whole"])
	assert.Equal(t, 42.5, object["fraction"])
	assert.Equal(t, "4200", object["text"], "a string that looks like a number is still a string")

	list, ok := object["nested"].([]any)
	require.True(t, ok)
	assert.Equal(t, int64(1), list[0])
	assert.Equal(t, int64(2), list[1].(map[string]any)["deep"])
}

// jsonNumber is what `encoding/json` hands back for a number when a decoder is
// told to keep the text ([json.Decoder.UseNumber]), which is how every reader of a
// delivery in this repository decodes one.
func jsonNumber(text string) json.Number { return json.Number(text) }

// TestAWebhookIsFoundByName covers the one lookup, which exists so that `flow
// test` and whatever addresses a delivery later cannot disagree about which
// trigger a name means.
func TestAWebhookIsFoundByName(t *testing.T) {
	t.Parallel()

	workflow := orderWorkflow()

	found, ok := v1.FindWebhookTrigger(workflow, "stripe")
	require.True(t, ok)
	assert.Equal(t, "stripe", found.GetName())

	_, ok = v1.FindWebhookTrigger(workflow, "shopify")
	assert.False(t, ok)
	assert.Equal(t, []string{"stripe"}, v1.WebhookTriggerNames(workflow))
}

// TestAnIdempotencyKeyWalkIsBoundedInDepth covers the resource an attacker
// controls here, which is neither bytes nor time. The submit path accepts a
// hand-built `RunRequest` carrying a `ParsedExpr` that never went through the
// CEL parser, so the specification's 1 MiB cap is the only upstream limit and
// it bounds size rather than nesting: compact nodes buy thousands of levels
// inside it. The walk is recursive, so what runs out is the stack.
//
// Built as protobuf rather than parsed, deliberately. Going through NewExpr
// would test cel-go's parser nesting limit instead, and the whole premise of
// this path is a specification that never met the parser.
//
// Nested past the bound, the answer must be the refusing one. Reading the key
// as delivery-dependent on an expression the walk gave up on is the open
// direction, and CLAUDE.md's rule is that a component which allows when it
// cannot decide eventually allows everything.
func TestAnIdempotencyKeyWalkIsBoundedInDepth(t *testing.T) {
	t.Parallel()

	// Deeper than the bound by a wide margin, and deep enough that a walk
	// copying its binding set at every level would be doing millions of map
	// insertions rather than thousands of comparisons.
	const depth = 5000

	// The innermost expression genuinely names the delivery root, so nothing
	// but the depth bound can be what refuses this.
	inner := &expr.Expr{ExprKind: &expr.Expr_IdentExpr{
		IdentExpr: &expr.Expr_Ident{Name: v1.EventRoot},
	}}

	// Each level binds a *distinct* iterator name, which is the shape that
	// made the old set-copying walk quadratic: nothing is ever shadowed, so
	// the set only grew.
	for i := range depth {
		inner = &expr.Expr{ExprKind: &expr.Expr_ComprehensionExpr{
			ComprehensionExpr: &expr.Expr_Comprehension{
				IterVar: "v" + strconv.Itoa(i),
				AccuVar: "a" + strconv.Itoa(i),
				IterRange: &expr.Expr{ExprKind: &expr.Expr_ListExpr{
					ListExpr: &expr.Expr_CreateList{},
				}},
				AccuInit: &expr.Expr{ExprKind: &expr.Expr_ListExpr{
					ListExpr: &expr.Expr_CreateList{},
				}},
				LoopCondition: &expr.Expr{ExprKind: &expr.Expr_ConstExpr{
					ConstExpr: &expr.Constant{ConstantKind: &expr.Constant_BoolValue{BoolValue: true}},
				}},
				LoopStep: inner,
				Result: &expr.Expr{ExprKind: &expr.Expr_ConstExpr{
					ConstExpr: &expr.Constant{ConstantKind: &expr.Constant_BoolValue{BoolValue: true}},
				}},
			},
		}}
	}

	key := &v1.Value{Kind: &v1.Value_Expr{
		Expr: &expr.ParsedExpr{Expr: inner},
	}}

	assert.Error(t, v1.CheckWebhookIdempotencyKey("deep", key),
		"a key nested past the walk's depth bound was accepted as delivery-dependent")
}

// TestAnIdempotencyKeyMayNameTheDeliveryInAComprehensionResult is the false
// diagnostic the shadowing rule can produce if it is applied too widely.
//
// A comprehension's result is not evaluated in the loop's scope. The iteration
// has ended by then and only the accumulator is still bound, so an `event` in
// the result names the delivery root even when the comprehension iterates over
// a variable of that same name. Treating the iterator as still shadowing it
// refuses a key that does depend on the delivery — and CLAUDE.md rates a false
// diagnostic worse than a missing one, because the author is told their correct
// file is wrong and has nothing to fix.
func TestAnIdempotencyKeyMayNameTheDeliveryInAComprehensionResult(t *testing.T) {
	t.Parallel()

	// Iterating over `event` — so inside loop_step, `event` is the iterator —
	// while the result names the outer `event` that the iteration left in
	// scope again.
	key := &v1.Value{Kind: &v1.Value_Expr{Expr: &expr.ParsedExpr{
		Expr: &expr.Expr{ExprKind: &expr.Expr_ComprehensionExpr{
			ComprehensionExpr: &expr.Expr_Comprehension{
				IterVar:   v1.EventRoot,
				AccuVar:   "__result__",
				IterRange: &expr.Expr{ExprKind: &expr.Expr_ListExpr{ListExpr: &expr.Expr_CreateList{}}},
				AccuInit: &expr.Expr{ExprKind: &expr.Expr_ConstExpr{
					ConstExpr: &expr.Constant{ConstantKind: &expr.Constant_BoolValue{BoolValue: true}},
				}},
				LoopCondition: &expr.Expr{ExprKind: &expr.Expr_ConstExpr{
					ConstExpr: &expr.Constant{ConstantKind: &expr.Constant_BoolValue{BoolValue: true}},
				}},
				// The iterator, which is shadowed here and must not count.
				LoopStep: &expr.Expr{ExprKind: &expr.Expr_IdentExpr{
					IdentExpr: &expr.Expr_Ident{Name: v1.EventRoot},
				}},
				// The delivery root, which is not shadowed here and must.
				Result: &expr.Expr{ExprKind: &expr.Expr_SelectExpr{
					SelectExpr: &expr.Expr_Select{
						Operand: &expr.Expr{ExprKind: &expr.Expr_IdentExpr{
							IdentExpr: &expr.Expr_Ident{Name: v1.EventRoot},
						}},
						Field: "id",
					},
				}},
			},
		}},
	}}}

	assert.NoError(t, v1.CheckWebhookIdempotencyKey("result-scope", key),
		"a key naming the delivery in a comprehension result was refused as constant")
}
