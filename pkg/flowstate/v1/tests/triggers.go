package tests

import (
	"context"
	"encoding/json"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for a declared `triggers:` block, run by both execution drivers.
//
// What is observable about a trigger, from inside a run, is deliberately almost
// nothing — and "almost nothing, identically on both drivers" is precisely the
// claim worth pinning, for the reason `flow run local` exists at all: a local run
// tells an author what production will do, so a declaration that changed a run on
// one driver and not the other would make the rehearsal lie about the file in front
// of them.
//
// Two things could drift, and each has a case:
//
//   - A webhook declaration is inert. It travels in the specification (so it is
//     carried across the wire, into history and through Continue-As-New on the
//     durable driver, and sits in a struct on the local one), and neither driver
//     may read it: the same steps run, with the same outputs, as they would with no
//     `triggers:` block at all. A driver that gated on a declared trigger — refusing
//     a run nobody delivered to, say — would be one an author could not test.
//   - `event` is not in scope in a step. It is bound where a trigger's arguments
//     are evaluated ([v1.BindWebhookTriggerInputs]) and nowhere else, which is the
//     design's rule rather than an implementation limit: everything a workflow
//     operates on arrives through `with:` into `inputs:`. `flow validate` reports a
//     step naming it, so this pins the other half — that a *specification* built
//     by hand, which never passed through the compiler, does not quietly resolve
//     it either, on either driver.
//
// Turning a delivery into inputs happens before a run exists, so the mapping
// itself belongs to neither driver — it is one function, covered where it lives
// (webhook_test.go). What *is* a driver's business, and is here, is what happens
// to the values it produces: a run started from a delivery has to compute the same
// thing under both drivers, and the values a payload produces are not the values a
// `flow run --input` produces. A JSON number carries no type, so the mapping
// decides whether `"amount": 4200` becomes an integer, and a driver that took it
// as a float would fail an `int` input on one side and not the other. See
// [WebhookDeliveryCases], whose inputs are built by the mapping rather than
// written out, so that a change to what a delivery yields fails here rather than
// silently changing what production runs.

// stripeWebhookDeclaration is the trigger both cases below carry: a well-formed
// one, so that nothing either driver might do with it is excused by it being
// malformed.
func stripeWebhookDeclaration() *v1.Triggers {
	return &v1.Triggers{
		Webhooks: []*v1.WebhookTrigger{{
			Name: "stripe",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeStripe: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "STRIPE_WEBHOOK_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.headers["stripe-signature"]`),
			Arguments: map[string]*v1.Value{
				"order_id": v1.NewExpr(`event.body.data.object.metadata.order_id`),
			},
		}},
	}
}

// deliveredWorkflow is the workflow a delivery starts in [WebhookDeliveryCases]:
// a signature with an `int` in it, which is the declaration a payload's numbers
// have to satisfy.
func deliveredWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "webhook-delivered",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "storefront",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "STOREFRONT_WEBHOOK_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.id`),
			Arguments: map[string]*v1.Value{
				"order_id": v1.NewExpr(`event.body.order.id`),
				"amount":   v1.NewExpr(`event.body.order.total_cents`),
			},
		}}},
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "order_id", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
			{Name: "amount", Type: v1.InputDeclaration_TYPE_INT, Required: true},
		},
		Steps: []*v1.Node{{
			Id:   "record",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`inputs.order_id + " for " + string(inputs.amount)`)},
		}},
	}
}

// deliveredInputs are what one delivery to that workflow binds to.
//
// Computed by [v1.BindWebhookTriggerInputs] rather than written out, which is the
// point: the case then asserts what *the mapping produces* runs identically on
// both drivers, instead of asserting that two hand-written literals do. The body
// is decoded the way both a stored delivery and a live one are decoded — with
// [json.Decoder.UseNumber] and [v1.NormalizeDeliveryNumbers] — so the `4200` below
// arrives as whatever a real payload's `4200` arrives as, which is the fact under
// test.
//
// A failure to bind is returned as a nil map rather than panicking, so the case
// fails as a case (mismatched outputs) rather than taking the package down at
// construction, where neither driver's name would be attached to it.
func deliveredInputs() map[string]*v1.Value {
	const payload = `{"id":"evt_88","order":{"id":"ord_H1x9","total_cents":4200}}`

	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()

	var body any
	if err := decoder.Decode(&body); err != nil {
		return nil
	}

	inputs, _, err := v1.BindWebhookTriggerInputs(context.Background(), deliveredWorkflow(),
		deliveredWorkflow().GetTriggers().GetWebhooks()[0], v1.WebhookDelivery{
			Headers: map[string]string{"x-flowstate-signature": "0f0f"},
			Body:    v1.NormalizeDeliveryNumbers(body),

			// Verified, because this case is about what a *started* run does with
			// the values, and a delivery that did not verify never starts one.
			Verified: true,
		})
	if err != nil {
		return nil
	}

	return inputs
}

// WebhookDeliveryCases are the shared cases for a run started by a delivery.
//
// Both drivers run every one of them, and both are handed exactly the inputs the
// mapping produced — which is the only part of a delivery that reaches a run at
// all. Everything else about the arrival (the endpoint, the signature, the dedupe)
// is the receiver's, happens before there is a run, and is asserted there.
func WebhookDeliveryCases() []Case {
	return []Case{
		{
			// The whole claim in one line: a payload's `4200` is an integer on
			// both drivers, so a workflow declaring `amount: {type: int}` runs
			// rather than refusing its own delivery on one side only.
			Name:     "a delivery's mapped inputs run the same on both drivers",
			Workflow: deliveredWorkflow(),
			Inputs:   deliveredInputs(),
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"record": {NamedValues: map[string]*v1.Value{
					v1.ValueOutput: v1.NewLiteral("ord_H1x9 for 4200"),
				}},
			}},
		},
	}
}

// WebhookTriggerCases returns the shared cases for a declared webhook trigger.
func WebhookTriggerCases() []Case {
	return []Case{
		{
			// The run is exactly the run the same file without a `triggers:` block
			// would be: the declaration is carried and read by nobody.
			Name: "a declared webhook leaves the run unchanged",
			Workflow: &v1.Workflow{
				Name:     "webhook-inert",
				Profile:  v1.CurrentProfile,
				Triggers: stripeWebhookDeclaration(),
				DeclaredInputs: []*v1.InputDeclaration{{
					Name:    "order_id",
					Type:    v1.InputDeclaration_TYPE_STRING,
					Default: v1.NewLiteral("ord_local"),
				}},
				Steps: []*v1.Node{{
					Id:   "record",
					Kind: &v1.Node_Value{Value: v1.NewExpr(`"order " + inputs.order_id`)},
				}},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"record": {NamedValues: map[string]*v1.Value{v1.ValueOutput: v1.NewLiteral("order ord_local")}},
			}},
		},
		{
			// The negative direction, which is the one a functionality test would
			// miss: not "a trigger's own expressions can read the delivery" but
			// "a step cannot". Written as a hand-built specification because the
			// compiler refuses this file — which is the point, since a spec reaching
			// a worker did not necessarily come from the compiler.
			Name: "a step cannot read the delivery a trigger was started by",
			Workflow: &v1.Workflow{
				Name:     "webhook-event-out-of-scope",
				Profile:  v1.CurrentProfile,
				Triggers: stripeWebhookDeclaration(),
				Steps: []*v1.Node{{
					Id:   "leak",
					Kind: &v1.Node_Value{Value: v1.NewExpr(`event.body.data.object.metadata.order_id`)},
				}},
			},
			ExpectFailure: true,
		},
	}
}
