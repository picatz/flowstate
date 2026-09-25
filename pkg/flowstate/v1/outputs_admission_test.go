package flowstatev1_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestALiteralOutputViolationRunsNoStep is the side-effect half of the
// submission-boundary check, and the half a refusal message cannot prove.
//
// The conformance corpus pins that both drivers refuse a specification whose
// literal output contradicts its own declaration, in the same words. What it
// does not show is the thing the refusal exists for: that the run never
// happened. A check that produced the identical sentence *after* the steps ran
// would satisfy every assertion over that text and still have sent the request,
// charged the card, or written the file — which is exactly the shape this gap
// had before, since [v1.EvalRunOutputs] is where the contradiction used to be
// found.
//
// So this counts the side effect rather than reading the message: an http step
// against a server that records every request it receives, and a declared
// output that is wrong before the run starts.
func TestALiteralOutputViolationRunsNoStep(t *testing.T) {
	// The default egress policy denies loopback, so the http step could not
	// reach a test server at all; see [conformance.AllowLoopback].
	conformance.AllowLoopback(t)

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	reaches := func(outputs []*v1.OutputDeclaration) *v1.Workflow {
		return &v1.Workflow{
			Name:            "admission",
			Profile:         v1.CurrentProfile,
			DeclaredOutputs: outputs,
			Steps: []*v1.Node{
				{
					Id: "call",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"method": v1.NewLiteral(http.MethodGet),
							"url":    v1.NewLiteral(server.URL + "/effect"),
						},
					}},
				},
			},
		}
	}

	// The control first, so the assertion below is known to be capable of
	// failing: the identical workflow with a legal output does reach the server.
	_, err := v1.RunWithInputs(t.Context(), reaches([]*v1.OutputDeclaration{
		{
			Name:   "channel",
			Value:  v1.NewLiteral("stable"),
			Type:   v1.InputDeclaration_TYPE_ENUM,
			Values: []string{"stable", "beta"},
		},
	}), nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), requests.Load(),
		"the control never reached the server, so the count below would prove nothing")

	// And the case: the same step, behind an output that is already wrong.
	_, err = v1.RunWithInputs(t.Context(), reaches([]*v1.OutputDeclaration{
		{
			Name:   "channel",
			Value:  v1.NewLiteral("canary"),
			Type:   v1.InputDeclaration_TYPE_ENUM,
			Values: []string{"stable", "beta"},
		},
	}), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `output "channel" is "canary"`)
	assert.Equal(t, int64(1), requests.Load(),
		"the step ran before the declared output was refused, so the refusal came too late to prevent its side effect")
}

// TestAComputedOutputViolationStillFailsAtCompletion is the boundary of the
// change above, and the reason it is not simply "check outputs at submit".
//
// A computed output's value does not exist until the run has produced it, so
// there is nothing to judge at admission and the step must run. The same
// workflow shape as above, with the output reading the step rather than stating
// a constant: the request is made, and the refusal arrives afterwards.
func TestAComputedOutputViolationStillFailsAtCompletion(t *testing.T) {
	conformance.AllowLoopback(t)

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"channel":"canary"}`))
	}))
	t.Cleanup(server.Close)

	wf := &v1.Workflow{
		Name:    "admission-computed",
		Profile: v1.CurrentProfile,
		DeclaredOutputs: []*v1.OutputDeclaration{
			{
				Name:   "channel",
				Value:  v1.NewExpr(`steps.call.json.channel`),
				Type:   v1.InputDeclaration_TYPE_ENUM,
				Values: []string{"stable", "beta"},
			},
		},
		Steps: []*v1.Node{
			{
				Id: "call",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"method":     v1.NewLiteral(http.MethodGet),
						"url":        v1.NewLiteral(server.URL + "/effect"),
						"parse_json": v1.NewLiteral(true),
					},
				}},
			},
		},
	}

	_, err := v1.RunWithInputs(t.Context(), wf, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `output "channel" is "canary"`)
	assert.Equal(t, int64(1), requests.Load(),
		"a computed output cannot be judged before the step that produces it, so the step must have run")
}

// TestAContainerOutputIsBoundedByWalkDepth is the boundary the depth guard on
// [v1.LiteralToGo] draws, at both seams that reach it.
//
// [v1.CheckOutputValue] converts a declared `struct` or `list` to prove it will
// project as the plain value its type promises, and that conversion recurses.
// Both seams reach it with a literal nobody has weighed: [v1.BindRunInputs] runs
// ahead of [v1.CheckSubmissionSize] in [v1.RunWithInputs], and
// [v1.EvalRunOutputs] checks a literal output again at completion. An unbounded
// walk there exhausts the goroutine stack on a deep enough value — a crash of
// the embedding process, which no caller can recover from and no server can
// report. Reproduced before the guard existed: a 2,000,000-level literal through
// [v1.BindRunInputs] gave `fatal error: stack overflow`.
//
// Both directions, because a bound that refused everything would satisfy the
// refusal half alone: exactly at [v1.MaxStructureDepth] the value is accepted at
// both seams, one level past it is refused at both.
//
// Asserted here rather than as a shared conformance case for the accepted half,
// for a reason about the harness rather than the drivers: an accepted value has
// to be compared, and `protocmp.Transform` under `cmp.Diff` costs roughly 15x
// per four levels of message nesting — 4s at depth 16, 80s at depth 20 — so a
// case carrying a value at this bound would never finish in either driver's
// runner. The refusal is a shared case ([conformance.OutputValueRefusalCases]),
// which is where the observable both-drivers behavior is.
func TestAContainerOutputIsBoundedByWalkDepth(t *testing.T) {
	t.Parallel()

	// A cyclic literal would be terminated by the same bound, and is not built
	// here: an [expr.Value] decoded from the wire is a tree, so a cycle is only
	// constructible by an in-process caller that has already broken the proto
	// contract. Depth is what bounds both, which is why depth is what is pinned.
	for _, test := range []struct {
		name     string
		declared v1.InputDeclaration_Type
		depth    int
		refused  bool
	}{
		{name: "a struct at the bound", declared: v1.InputDeclaration_TYPE_STRUCT, depth: v1.MaxStructureDepth},
		{name: "a struct past the bound", declared: v1.InputDeclaration_TYPE_STRUCT, depth: v1.MaxStructureDepth + 1, refused: true},
		{name: "a list at the bound", declared: v1.InputDeclaration_TYPE_LIST, depth: v1.MaxStructureDepth},
		{name: "a list past the bound", declared: v1.InputDeclaration_TYPE_LIST, depth: v1.MaxStructureDepth + 1, refused: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			value := conformance.NestedMapLiteral(test.depth)
			if test.declared == v1.InputDeclaration_TYPE_LIST {
				// One list holding the nested map, so the declared type matches
				// and the whole value still nests exactly test.depth levels —
				// the list itself is the outermost of them, which is why the map
				// inside it is built one shallower.
				value = v1.NewLiteralList(conformance.NestedMapLiteral(test.depth - 1).GetLiteral())
			}

			declaration := &v1.OutputDeclaration{Name: "detail", Type: test.declared, Value: value}
			wf := &v1.Workflow{
				Name:            "depth",
				Profile:         v1.CurrentProfile,
				DeclaredOutputs: []*v1.OutputDeclaration{declaration},
				Steps: []*v1.Node{{Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")},
				}}}},
			}

			// The submit seam. [v1.BindRunInputs] is the one both drivers reach,
			// and the one an embedder can call directly with anything at all.
			_, submitErr := v1.BindRunInputs(wf, nil)

			// The completion seam, over the same declaration: a literal output
			// is checked again by [v1.EvalRunOutputs] against the value it
			// reports.
			outputs, completionErr := v1.EvalRunOutputs(t.Context(), wf, &v1.Scope{})

			if !test.refused {
				require.NoError(t, submitErr, "a value at the bound must be admitted")
				require.NoError(t, completionErr, "a value at the bound must still be reported")
				require.NotNil(t, outputs.GetValues()["detail"])

				return
			}

			require.Error(t, submitErr, "a value past the bound must be refused before anything runs")
			assert.Contains(t, submitErr.Error(),
				"nests deeper than the 32 levels this server can walk")
			require.Error(t, completionErr, "and refused again at the moment it would be reported")
			assert.Contains(t, completionErr.Error(),
				"nests deeper than the 32 levels this server can walk")

			// The bound is the schema's, not a number this test wrote down: a
			// message naming a different one would mean two bounds for one
			// resource, which is the shape the constant exists to prevent.
			assert.Contains(t, submitErr.Error(),
				fmt.Sprintf("the %d levels", v1.MaxStructureDepth))
		})
	}
}
