package flowstatev1_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// Rooting the ambient half of the namespace — `steps.<id>.<output>` rather than a
// bare `<id>.<output>` — is a change to what an author writes, and this is the
// step before that: the runtime answering both, so that nothing an author writes
// has to change yet.
//
// The staging is not caution, it is invariant 10. RunState carries the compiled
// workflow, ParsedExprs and all, and a worker evaluates the *stored* AST rather
// than re-parsing the source. A run started before a deploy therefore holds
// `Ident("a").Select("result")` and keeps evaluating it on a worker that has
// moved on. Retiring surface syntax is free; retiring something a running
// workflow already carries is not, which is exactly the line docs/DSL.md draws
// when it exempts "the wire format, compiled specs, running histories" from the
// no-deprecation rule.
//
// What these need that nothing else here does is a step with an *output* to point
// a name at. `echo` retired at v2026.2 and `log:` produces no values on purpose, so
// the steps below reach the loopback server instead: a request whose body comes
// back unchanged is the shortest way to a real step output, and shaping it to a
// single `result` keeps the reference spellings under test exactly as an older spec
// holds them.

// TestRootedAndBareReferencesBothResolve is the compatibility arm.
//
// Each claim is pinned from both directions — a step that runs when it holds beside
// one that runs when it does not — so a failure says whether the reference resolved
// to the wrong value or stopped resolving at all.
func TestRootedAndBareReferencesBothResolve(t *testing.T) {
	// Not parallel: the loopback exemption below swaps a process-global registry
	// entry and restores it when the test ends, so two top-level tests holding one
	// at once would have the first one's restore land while the second still runs.
	baseURL := tests.NewHTTPServer(t)

	cases := map[string]string{
		"rooted":                          `steps.a.result == "hello"`,
		"bare, as an older spec holds it": `a.result == "hello"`,
		"rooted, selecting deeper":        `steps.a.result.size() == 5`,
		"bare and rooted in one":          `a.result + steps.a.result == "hellohello"`,
	}

	for name, claim := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := &v1.Workflow{
				Name: "rooted",
				Steps: append(
					[]*v1.Node{echoStep("a", baseURL, v1.NewLiteral("hello"))},
					pins("b", claim)...,
				),
			}

			out, err := v1.Run(context.Background(), wf)
			require.NoError(t, err, "%s must resolve", claim)
			assert.Contains(t, out.GetStepValues(), "b", "%s did not hold", claim)
			assert.NotContains(t, out.GetStepValues(), "b_else",
				"%s resolved to something other than the value the step produced", claim)
		})
	}
}

// TestStepsRootReachesEveryStep covers the shape a prefix match would miss.
func TestStepsRootReachesEveryStep(t *testing.T) {
	// Not parallel, for the reason given above.
	baseURL := tests.NewHTTPServer(t)

	wf := &v1.Workflow{
		Name: "rooted",
		Steps: []*v1.Node{
			echoStep("first", baseURL, v1.NewLiteral("one")),
			echoStep("second", baseURL, v1.NewLiteral("two")),
			echoStep("joined", baseURL,
				v1.NewExpr("steps.first.result + ' ' + steps.second.result")),
		},
	}

	out, err := v1.Run(context.Background(), wf)
	require.NoError(t, err)
	assert.Equal(t, "one two", resultOf(t, out, "joined"))
}

// TestAStepNamedStepsStillWins is the case that makes the compatibility arm
// honest rather than merely additive.
//
// `steps` is not reserved yet, so a spec compiled before the root existed may
// contain a step by that name — and a worker replaying it must still resolve
// `steps.result` to that step's output rather than to a map of every step. The
// root is answered only when no step claims the name.
func TestAStepNamedStepsStillWins(t *testing.T) {
	// Not parallel, for the reason given above.
	baseURL := tests.NewHTTPServer(t)

	wf := &v1.Workflow{
		Name: "shadowed",
		Steps: []*v1.Node{
			echoStep("steps", baseURL, v1.NewLiteral("i am a step")),
			echoStep("reader", baseURL, v1.NewExpr("steps.result")),
		},
	}

	out, err := v1.Run(context.Background(), wf)
	require.NoError(t, err)
	assert.Equal(t, "i am a step", resultOf(t, out, "reader"),
		"an older spec's own step must not be shadowed by the root")
}

// TestUnknownRootedReferenceStaysUnresolved keeps the root from becoming a way to
// reach something that is not there.
//
// A reference to a step that has not run resolves to nothing, the same as the
// bare form, rather than to an empty map that would let `steps.nope.result`
// evaluate to a zero value and carry on.
func TestUnknownRootedReferenceStaysUnresolved(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "missing",
		Steps: []*v1.Node{
			logNode("a", v1.NewLiteral("hello")),
			logNode("b", v1.NewExpr("steps.nope.result")),
		},
	}

	_, err := v1.Run(context.Background(), wf)
	require.Error(t, err, "a reference to a step that does not exist must not resolve")
}

// TestALoopBindingIsNotReachableThroughTheRoot is the negative direction, and the
// reason rooting is worth doing at all.
//
// An iterator is a local binding and stays bare. It is not a step, so it must not
// appear under `steps.` — if it did, the root would be a second name for
// something that already has one, and the collision rooting exists to remove
// would simply have moved.
func TestALoopBindingIsNotReachableThroughTheRoot(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name: "loop",
		Steps: []*v1.Node{
			{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewExpr("['x']"),
				Iterator: "item",
				Body:     []*v1.Node{logNode("inner", v1.NewExpr("steps.item"))},
			}}},
		},
	}

	_, err := v1.Run(context.Background(), wf)
	require.Error(t, err, "a loop binding must not be reachable under the steps root")
}

// echoStep returns a step that hands body to the loopback server and records what
// came back under `result`.
//
// The output keeps the name an older spec would have written, because that name is
// the second half of every reference these tests exercise.
func echoStep(id, baseURL string, body *v1.Value) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name: "http",
		Inputs: map[string]*v1.Value{
			"method":  v1.NewLiteral(http.MethodPost),
			"url":     v1.NewLiteral(baseURL + "/echo"),
			"body":    body,
			"outputs": v1.NewExpr(`{"result": response.body}`),
		},
	}}}
}

// logNode returns a step that evaluates message and produces nothing.
//
// Used where what is under test is whether a reference resolves at all, so no
// output is needed and no server has to be reached to get one.
func logNode(id string, message *v1.Value) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "log",
		Inputs: map[string]*v1.Value{"message": message},
	}}}
}

// pins returns a pair of steps that together observe what claim evaluates to.
//
// The negative arm is the point: absence alone has two causes that matter
// differently — the claim was false, or conditions stopped being evaluated — and
// only the pair tells them apart. Same shape as the shared cases use, spelled here
// because that package's version is unexported.
func pins(id, claim string) []*v1.Node {
	return []*v1.Node{
		{
			Id:        id,
			Condition: v1.NewExpr(claim),
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("held: " + claim)},
			}},
		},
		{
			Id:        id + "_else",
			Condition: v1.NewExpr("!(" + claim + ")"),
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "log",
				Inputs: map[string]*v1.Value{"message": v1.NewLiteral("failed: " + claim)},
			}},
		},
	}
}

func resultOf(t *testing.T, out *v1.Workflow_StepOutputs, id string) string {
	t.Helper()
	require.Contains(t, out.GetStepValues(), id)
	return out.GetStepValues()[id].GetNamedValues()["result"].GetLiteral().GetStringValue()
}
