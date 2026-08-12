package tests

import (
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
// The mapping itself is not here, and that is not an omission. Turning a delivery
// into inputs happens before a run exists, so it belongs to neither driver: it is
// one function, covered where it lives (webhook_test.go) and exercised end to end
// by `flow test` over examples/webhook-trigger.

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
