package flowstatev1_test

import (
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
