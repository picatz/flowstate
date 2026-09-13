package flowstatev1

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// The fixtures below name every workflow and every step, because what these
// tests assert is an order and a stopping point, and both are only legible if
// the failure message can say which position was reached.

// specWalkStep is a task step whose id identifies it in an order assertion.
func specWalkStep(id string) *Node {
	return &Node{Id: id, Kind: &Node_Task{Task: &Task{Name: "noop"}}}
}

// specWalkCall is a `call:` step embedding callee, the edge walkEmbeddedWorkflows
// follows and [WalkWorkflow] does not.
func specWalkCall(id string, callee *Workflow) *Node {
	return &Node{Id: id, Kind: &Node_Call{Call: &Call{Workflow: callee}}}
}

// specWalkTree is a root with two callees, one of them reached through a
// `for_each:` body and holding a callee of its own, so a walk that stops at the
// first structural level or at the first call is visibly short.
//
//	root         steps: a, to-first (call), b, to-second (call, inside for_each)
//	  first      steps: first-a
//	  second     steps: second-a, to-third (call)
//	    third    steps: third-a
func specWalkTree() *Workflow {
	third := &Workflow{Name: "third", Steps: []*Node{specWalkStep("third-a")}}
	second := &Workflow{Name: "second", Steps: []*Node{
		specWalkStep("second-a"),
		specWalkCall("to-third", third),
	}}
	first := &Workflow{Name: "first", Steps: []*Node{specWalkStep("first-a")}}

	return &Workflow{Name: "root", Steps: []*Node{
		specWalkStep("a"),
		specWalkCall("to-first", first),
		specWalkStep("b"),
		{Id: "fan", Kind: &Node_ForEach{ForEach: &ForEach{Body: []*Node{
			specWalkCall("to-second", second),
		}}}},
	}}
}

// deepCallChain returns a workflow whose `call:` edges nest depth levels, which
// is how a specification passes maxWorkflowScanDepth without nesting any one
// workflow's own steps.
func deepCallChain(depth int) *Workflow {
	wf := &Workflow{Name: "leaf", Steps: []*Node{specWalkStep("leaf-a")}}
	for i := depth; i > 0; i-- {
		wf = &Workflow{
			Name:  "level-" + strconv.Itoa(i),
			Steps: []*Node{specWalkCall("down", wf)},
		}
	}

	return wf
}

// TestSpecWorkflowsYieldsTheRootAndEveryCallee pins the unit a
// whole-specification check operates on: a callee is compiled into its caller,
// so one that is not yielded is half a specification checked as if it were
// whole.
func TestSpecWorkflowsYieldsTheRootAndEveryCallee(t *testing.T) {
	t.Parallel()

	var names []string
	for wf, err := range specWorkflows(specWalkTree()) {
		require.NoError(t, err)
		names = append(names, wf.GetName())
	}

	require.Equal(t, []string{"root", "first", "second", "third"}, names,
		"every workflow a `call:` reaches must be yielded, in document order, including one "+
			"reached through a structural body and one reached through another callee")
}

// TestSpecNodesYieldsEveryStepExactlyOnce is the other half of the same
// guarantee, and the direction that would silently under-report: a step yielded
// twice makes a requirement walk merely redundant, and a step never yielded
// makes it wrong.
func TestSpecNodesYieldsEveryStepExactlyOnce(t *testing.T) {
	t.Parallel()

	seen := map[string]int{}
	var order []string
	for node, err := range specNodes(specWalkTree()) {
		require.NoError(t, err)
		seen[node.GetId()]++
		order = append(order, node.GetId())
	}

	require.Equal(t,
		[]string{"a", "to-first", "b", "fan", "to-second", "first-a", "second-a", "to-third", "third-a"},
		order,
		"each workflow's own steps are delivered by WalkWorkflow before the next callee is entered")
	for id, count := range seen {
		require.Equal(t, 1, count, "step %q was yielded %d times; a `call:` step's body is "+
			"descended for its callee edge, never for its steps", id, count)
	}
}

// TestSpecWorkflowsStopsWhereTheConsumerStops is the behavior a callback could
// not express without cost. Before this iterator, the caller that had its
// answer either kept walking — CheckRequiredSecretInputs' latched variable,
// which entered every remaining callee to do nothing in — or bought its stop
// with a sentinel error it then had to filter back out.
func TestSpecWorkflowsStopsWhereTheConsumerStops(t *testing.T) {
	t.Parallel()

	var names []string
	for wf, err := range specWorkflows(specWalkTree()) {
		require.NoError(t, err)
		names = append(names, wf.GetName())
		if wf.GetName() == "first" {
			break
		}
	}

	require.Equal(t, []string{"root", "first"}, names,
		"breaking must stop the callee walk where it is, not merely skip the work under the "+
			"callees it goes on to enter")
}

// TestSpecNodesStopsEnteringCalleesWhereTheConsumerStops states the stopping
// grain exactly, both halves of it, because the honest answer is not "nothing
// more happens": [WalkWorkflow] has no early exit, so the workflow already in
// hand is finished and its remaining steps are discarded, while no *further*
// workflow is entered at all.
func TestSpecNodesStopsEnteringCalleesWhereTheConsumerStops(t *testing.T) {
	t.Parallel()

	var order []string
	for node, err := range specNodes(specWalkTree()) {
		require.NoError(t, err)
		order = append(order, node.GetId())
		if node.GetId() == "a" {
			break
		}
	}

	require.Equal(t, []string{"a"}, order,
		"no step may be delivered after the consumer stopped: yielding again would panic, and "+
			"the latch that prevents it must also stop the value reaching the loop body")
}

// TestSpecWalkReportsADepthRefusalAsItsLastPair fails closed: past
// maxWorkflowScanDepth the walk cannot inspect a whole specification, so the
// refusal has to reach the caller as an error rather than as a short answer
// that looks like a complete one.
func TestSpecWalkReportsADepthRefusalAsItsLastPair(t *testing.T) {
	t.Parallel()

	deep := deepCallChain(maxWorkflowScanDepth + 1)

	t.Run("workflows", func(t *testing.T) {
		t.Parallel()

		var refusal error
		yielded := 0
		for wf, err := range specWorkflows(deep) {
			if err != nil {
				require.Nil(t, wf, "a terminal error pair carries no workflow")
				require.Nil(t, refusal, "the error pair must be the last one yielded")
				refusal = err
				continue
			}
			yielded++
		}

		require.Error(t, refusal, "a call chain nested past the scan depth must be refused")
		require.ErrorContains(t, refusal, "steps nest more than",
			"the refusal must be the callee walk's own depth guard, so the sentence names the shape "+
				"of the problem")
		require.Equal(t, maxWorkflowScanDepth+1, yielded,
			"every workflow the guard could reach is still yielded before it refuses")
	})

	t.Run("nodes", func(t *testing.T) {
		t.Parallel()

		var refusal error
		for node, err := range specNodes(deep) {
			if err != nil {
				require.Nil(t, node, "a terminal error pair carries no step")
				refusal = err
				continue
			}
		}

		require.Error(t, refusal, "the step iterator must surface the same refusal, not walk the "+
			"prefix and report success")
	})
}

// TestStoppingASpecWalkIsNeverReportedAsAFailure guards the seam between the
// two: errStopWalk travels out through walkEmbeddedWorkflows, which wraps what
// a callee edge returns, and a caller that saw it would read its own `break` as
// a broken specification.
//
// Two mechanisms catch an escape, which is why there is no assertion here
// naming the sentinel. A sentinel yielded *before* the break fails the
// require.NoError below. One yielded *after* it cannot reach an assertion at
// all: the runtime panics with "range function continued iteration after
// function for loop body returned false". An `errors.Is(err, errStopWalk)`
// check would sit after require.NoError has already established err is nil, so
// it could only ever pass — the exact shape AGENTS.md calls worse than no
// claim of coverage.
func TestStoppingASpecWalkIsNeverReportedAsAFailure(t *testing.T) {
	t.Parallel()

	// Stopping inside the deepest callee, so the sentinel crosses the wrapping
	// edge on its way out rather than being returned by the top-level visit.
	workflows := 0
	for wf, err := range specWorkflows(specWalkTree()) {
		require.NoError(t, err, "the stop sentinel escaped to the consumer as a failure")
		workflows++
		if wf.GetName() == "third" {
			break
		}
	}
	require.Equal(t, 4, workflows, "the walk must have reached the deepest callee before stopping, "+
		"or this proves nothing about the sentinel crossing the wrapping edge")

	nodes := 0
	for node, err := range specNodes(specWalkTree()) {
		require.NoError(t, err, "the stop sentinel escaped to the consumer as a failure")
		nodes++
		if node.GetId() == "third-a" {
			break
		}
	}
	require.Equal(t, 9, nodes, "the step walk must likewise have reached the deepest callee's "+
		"last step before stopping")
}
