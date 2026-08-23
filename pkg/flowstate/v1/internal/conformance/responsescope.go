package conformance

import (
	"net/http"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for what an http step's `expect:` and `outputs:` can see, run by both
// execution drivers.
//
// These two positions are the only place in the language where an author's expression
// is evaluated by a *task* rather than by the engine, and that is exactly why they
// drifted: the engine builds one activation for every other position, and these two
// built their own. The hand-built one carried the step outputs and nothing else, so
// `vars.region` — legal in `if:`, in `items:`, in `wait_until:`, in a `body:` on the
// same step — was unresolvable one key below it.
//
// Worse than unresolvable, in the rooted case. An empty `vars` root still answers, by
// design, so that `vars.missing` reads as a missing key rather than an unbound name.
// With no vars in the activation at all, *every* var was a missing key: the author was
// told `no such key: region` about a variable declared eleven lines above, and sent
// looking for a typo that was not there.
//
// Shared because the divergence they guard is not between the drivers but between two
// positions inside one file — and a case that only ran locally would let the durable
// driver reacquire it the next time this activation is built by hand.

// ResponseScopeCases returns the shared cases for names visible inside an http step's
// `expect:` and `outputs:`.
//
// The base URL should come from [NewHTTPServer]. All of them need it: what these
// observe is an expression evaluated *against a response*, so there has to be one.
func ResponseScopeCases(httpBaseURL string) []Case {
	return []Case{
		{
			// The rooted namespace, read from `outputs:`. This is the comment in
			// httpResponseEnv made executable — it names `${vars.greeting.upperAscii()}`
			// as the thing that must work here, and half of that example (the function)
			// was fixed while the other half (the variable) stayed broken.
			Name: "outputs reads the workflow's vars",
			Workflow: &v1.Workflow{
				Name:    "outputs-reads-vars",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
				Steps: []*v1.Node{shaped("call", httpBaseURL,
					`{"said": vars.region}`)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": said("eu-west-1"),
			}},
		},
		{
			// The same namespace from the other position. `expect:` runs *before*
			// `outputs:` and on a different code path, so one being fixed says nothing
			// about the other — which is how they came to be two halves of one feature
			// with only one of them classified, one release earlier.
			//
			// The claim names the response as well as the var, so a run that somehow
			// lost the response root fails here rather than passing on the var alone.
			Name: "expect reads the workflow's vars",
			Workflow: &v1.Workflow{
				Name:    "expect-reads-vars",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"want": v1.NewLiteral(int64(200))},
				Steps: []*v1.Node{expects("call", httpBaseURL,
					`response.status_code == vars.want`)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": said("ok"),
			}},
		},
		{
			// How the run started, from the same position, and it is here rather
			// than in [TriggerContextCases] for the reason the whole set is here:
			// this is the one activation in the language that a *task* builds, in
			// an activity, on a worker holding a compacted scope and nothing else.
			// Every other root was dropped there once. A `${trigger.kind}` that
			// resolved in a step's `if:` and was unbound in the http task's
			// `outputs:` two lines below it would be one spelling with two
			// behaviours, decided by a property of the task an author has no
			// reason to know.
			//
			// The context is stated, because a stated one is the only thing both
			// drivers can be held to — see [TriggerContextCases].
			Name: "outputs reads how the run started",
			Workflow: &v1.Workflow{
				Name:    "outputs-reads-trigger",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{shaped("call", httpBaseURL,
					`{"said": trigger.kind + "/" + trigger.name}`)},
			},
			Trigger: v1.NewScheduleTriggerContext("nightly-sweep", "ops"),
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": said("schedule/nightly-sweep"),
			}},
		},
		{
			// And the other position, which runs before `outputs:` and on a
			// different code path — so one being right says nothing about the
			// other, which is how these two came to be two halves of one feature
			// with only one of them working.
			Name: "expect reads how the run started",
			Workflow: &v1.Workflow{
				Name:    "expect-reads-trigger",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{expects("call", httpBaseURL,
					`response.status_code == 200 && trigger.kind == "webhook"`)},
			},
			Trigger: v1.NewWebhookTriggerContext("storefront", "webhook", "6b1f0c"),
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": said("ok"),
			}},
		},
		{
			// A loop's iterator, which is a bare name rather than a rooted one and so
			// travels by a different field — [v1.Scope.Vars] rather than
			// [v1.Scope.AmbientVars]. Both were dropped, but only one of them said so:
			// an absent iterator reports `no such attribute(s): item`, which is at
			// least honest, while an absent rooted namespace reports a missing key.
			//
			// The default spelling deliberately, since that is the one an author gets
			// without asking for it.
			Name: "outputs reads a loop's iterator",
			Workflow: &v1.Workflow{
				Name:    "outputs-reads-iterator",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{{
					Id: "each",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items: v1.NewLiteralList("a"),
						Body: []*v1.Node{shaped("call", httpBaseURL,
							`{"said": item}`)},
					}},
				}},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"call": map[string]any{"said": "a"}},
					),
				}},
			}},
		},
		{
			// A step's own `vars:`, read from that same step's `outputs:`. The nearest
			// binding there is, and the last one to check: it reaches the activation by
			// a third route again — the executor's scope is swapped for the step, so a
			// position that rebuilds the activation from anything other than that scope
			// sees the workflow's bindings and not the step's.
			Name: "outputs reads the step's own vars",
			Workflow: &v1.Workflow{
				Name:    "outputs-reads-step-vars",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{withVars(
					shaped("call", httpBaseURL, `{"said": tag}`),
					map[string]*v1.Value{"tag": v1.NewExpr(`"v" + "2"`)},
				)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": said("v2"),
			}},
		},
		{
			// The precedence, pinned from the side that matters. `response` is the
			// task's own root and has to win inside these two positions whatever else
			// is in scope — it is what the documentation says they are evaluated
			// against and what `flow fix` rewrites bare names *to*.
			//
			// A workflow var of the same name cannot collide, because vars are rooted;
			// what can is a name in scope bare, so the claim is made against one. The
			// step's var is unreachable here on purpose, and that is the trade: one
			// name is spoken for in two positions out of a file's many.
			Name: "the response wins over a bare name of its own spelling",
			Workflow: &v1.Workflow{
				Name:    "response-wins",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{withVars(
					shaped("call", httpBaseURL, `{"said": string(response.status_code)}`),
					map[string]*v1.Value{"response": v1.NewLiteral("not the response")},
				)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"call": said("200"),
			}},
		},
	}
}

// shaped returns an http step whose `outputs:` is the given expression.
//
// [echoes] shapes a fixed expression over a body the caller chooses; this is the
// mirror of it, because what is under test here is the outputs expression itself. The
// request is a plain GET for the same reason — nothing about it should matter.
func shaped(id, httpBaseURL, outputs string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":     v1.NewLiteral(httpBaseURL + "/status/200"),
				"outputs": v1.NewExpr(outputs),
			},
		}},
	}
}

// expects returns an http step with the given `expect:` expression, whose outputs say
// only that it got past it.
//
// A satisfied expectation produces nothing of its own, so the case needs something to
// assert on; a fixed literal keeps the assertion about the expectation rather than
// about the response's headers.
func expects(id, httpBaseURL, expect string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method":  v1.NewLiteral(http.MethodGet),
				"url":     v1.NewLiteral(httpBaseURL + "/status/200"),
				"expect":  v1.NewExpr(expect),
				"outputs": v1.NewExpr(`{"said": "ok"}`),
			},
		}},
	}
}
