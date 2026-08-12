package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The walker-exhaustiveness guard, in the package that holds two of the walkers.
//
// A recursively-nested node kind needs an arm in every walk over the node tree, and
// nothing structural forces one: `loop` was added and six separate walkers silently
// kept skipping it, each a real defect (a lost signal, a pruned output, a bypassed
// bound). These tests make that failure loud. Each iterates the container kinds the
// schema actually has ([NodeContainerKinds]) and looks up a builder for each — so a
// fourth container kind added tomorrow has no builder, `require.True` below fails
// naming it, and once a builder is added the descent assertion runs the walker on it
// and fails again if the walker has no arm. The list is derived from the schema, so
// there is no third place to forget to update.

// sameScopeContainers builds one same-scope container of each kind around a body of
// steps. Keyed by the `kind` oneof arm name so a test can pair it with
// [NodeContainerKinds].
func sameScopeContainers() map[string]func(body []*Node) *Node {
	return map[string]func([]*Node) *Node{
		"for_each": func(body []*Node) *Node {
			return &Node{Id: "c_for_each", Kind: &Node_ForEach{ForEach: &ForEach{
				Items: NewLiteralList("x"),
				Body:  body,
			}}}
		},
		"loop": func(body []*Node) *Node {
			return &Node{Id: "c_loop", Kind: &Node_Loop{Loop: &Loop{
				Until:         NewExpr("true"),
				MaxIterations: 2,
				Body:          body,
			}}}
		},
		"parallel": func(body []*Node) *Node {
			return &Node{Id: "c_parallel", Kind: &Node_Parallel{Parallel: &Parallel{
				Branches: []*Parallel_Branch{{Steps: body}},
			}}}
		},
		"switch": func(body []*Node) *Node {
			// The probe body sits in the *default*, deliberately: a walker that
			// descends the cases and misses the default slot passes a
			// cases-only probe and still drops whatever the default holds.
			return &Node{Id: "c_switch", Kind: &Node_Switch{Switch: &Switch{
				Value:   NewExpr("'x'"),
				Cases:   []*Switch_Case{{Values: []*Value{NewLiteral("y")}, Steps: nil}},
				Default: &Switch_Default{Steps: body},
			}}}
		},
	}
}

// eachContainer runs fn against a builder for every container kind the schema has,
// failing when one has no builder — which is what makes a newly added container kind
// turn every guard that uses this red until it is covered.
func eachContainer(t *testing.T, fn func(t *testing.T, kind string, wrap func(body []*Node) *Node)) {
	t.Helper()
	builders := sameScopeContainers()
	for _, kind := range NodeContainerKinds() {
		wrap, ok := builders[kind]
		require.Truef(t, ok, "no test builder for container kind %q; add one to sameScopeContainers and confirm every node-tree walker descends it", kind)
		t.Run(kind, func(t *testing.T) { fn(t, kind, wrap) })
	}
}

// TestSignalNamesDescendsEveryContainer guards the signal enumeration walker.
func TestSignalNamesDescendsEveryContainer(t *testing.T) {
	eachContainer(t, func(t *testing.T, kind string, wrap func(body []*Node) *Node) {
		name := "sig-in-" + kind
		body := []*Node{{
			Id:   "gate",
			Kind: &Node_Wait{Wait: &Wait{Kind: &Wait_Signal{Signal: &Signal{Name: name}}}},
		}}
		wf := &Workflow{Name: "w", Steps: []*Node{wrap(body)}}

		require.Containsf(t, SignalNames(wf), name,
			"SignalNames does not descend a %s body: a wait_for_signal there is invisible to signal policy and dropped from the pre-suspend drain", kind)
	})
}

// TestValidateCredentialNodesDescendsEveryContainer guards the credential preflight
// walker: a literal credential target inside a container body must be caught before
// the run starts, not after an earlier iteration's side effects.
func TestValidateCredentialNodesDescendsEveryContainer(t *testing.T) {
	eachContainer(t, func(t *testing.T, kind string, wrap func(body []*Node) *Node) {
		body := []*Node{{
			Id: "reach",
			Kind: &Node_Task{Task: &Task{Name: "http", Inputs: map[string]*Value{
				"url":        NewLiteral("https://example.com"),
				"credential": NewLiteral("unconfigured-target"),
			}}},
		}}

		err := validateCredentialNodes([]*Node{wrap(body)}, map[string]struct{}{}, nil)
		require.Errorf(t, err,
			"validateCredentialNodes does not descend a %s body: a literal credential target there escapes preflight and is only denied after the run starts", kind)
		require.Contains(t, err.Error(), "unconfigured-target")
	})
}
