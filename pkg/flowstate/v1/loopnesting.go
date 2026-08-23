package flowstatev1

import "fmt"

// CheckLoopNesting refuses a workflow that runs a `loop:` inside another
// `loop:`, including one reached through a `call:`'s inlined callee, any
// number of calls deep up to [MaxCallDepth].
//
// This is the RPC-path half of the refusal `pkg/flowstate/v1/flowfile` reports
// while compiling a Flowfile (see that package's `bodyHasNestedLoop`). The
// engine does not suspend below the top of a loop body, so an inner loop runs
// atomically inside each outer iteration: the two iteration ceilings multiply,
// and no Continue-As-New can happen between inner iterations to compact what
// the run is carrying. That is not a shape with worse ergonomics, it is a shape
// whose Continue-As-New interaction across two carried-state frames nothing
// exercises, and a run that reaches it grows its history until Temporal refuses
// the Continue-As-New — at which point the run does not fail, it wedges.
//
// A specification built by hand and submitted straight to the RPC boundary
// arrives without the compiler in front of it, so the refusal has to run here
// too. A caller cannot be left able to submit through the public API what the
// language will not compile, least of all when what it buys them is a wedged
// run rather than a fast failure.
//
// Bounded the same way [CheckPolicyPlacement] is, and for the same reason —
// this walk runs over a [Workflow] an untrusted caller composed: by
// [maxStructureWalkNodes] total steps visited across every callee entered, and
// by [MaxCallDepth] for how many calls deep it follows, so neither a
// diamond-shaped call graph nor a maximally nested one can make it run long.
//
// Called from `FlowstateServer.validateSpecification`, alongside
// [CheckPolicyPlacement], [CheckSignalPolicies] and [CheckStructureDepth].
func CheckLoopNesting(wf *Workflow) error {
	nodesLeft := maxStructureWalkNodes
	exhausted := false

	// The pair the error names: the loop that encloses, and the loop found
	// under it. Both, because either alone leaves the reader looking for the
	// other, and through a call they can be in two different workflows.
	var outerID, innerID string

	var walk func(nodes []*Node, enclosingLoop string, callDepth int)
	walk = func(nodes []*Node, enclosingLoop string, callDepth int) {
		if exhausted || innerID != "" {
			return
		}
		for _, node := range nodes {
			if exhausted || innerID != "" {
				return
			}
			if nodesLeft <= 0 {
				exhausted = true
				return
			}
			nodesLeft--

			switch kind := node.GetKind().(type) {
			case *Node_Loop:
				if enclosingLoop != "" {
					outerID, innerID = enclosingLoop, node.GetId()
					return
				}
				walk(kind.Loop.GetBody(), node.GetId(), callDepth)
			case *Node_ForEach:
				walk(kind.ForEach.GetBody(), enclosingLoop, callDepth)
			case *Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					walk(branch.GetSteps(), enclosingLoop, callDepth)
				}
			case *Node_Switch:
				// A switch body shares its enclosing suspend scope, so a loop
				// in a case body inside a loop body is the same nesting.
				for _, body := range SwitchBodies(kind.Switch) {
					walk(body, enclosingLoop, callDepth)
				}
			case *Node_Call:
				callee := kind.Call.GetWorkflow()
				if callee == nil {
					continue
				}
				nextDepth := callDepth + 1
				if CheckCallDepth(nextDepth) != nil {
					// Refused at execution by CheckCallDepth itself whatever
					// this walk would find beneath it, so descending further
					// only inspects a callee nothing will ever run.
					continue
				}
				// A callee has its own frame and still runs atomically at the
				// caller's suspend level, which is why the enclosing loop is
				// carried across the boundary rather than reset.
				walk(callee.GetSteps(), enclosingLoop, nextDepth)
			}
		}
	}
	walk(wf.GetSteps(), "", 0)

	if innerID != "" {
		return fmt.Errorf(
			"step %q is a `loop:` running inside the `loop:` at step %q: the engine does not suspend "+
				"below the top of a loop body, so the inner loop's iterations multiply the outer's with no "+
				"Continue-As-New between them; flatten the two into one loop",
			innerID, outerID)
	}

	if exhausted {
		return fmt.Errorf(
			"the workflow's call graph holds more than %d steps once every `call:`'s callee is walked, "+
				"which is more than this server can inspect for nested loops; flatten the call graph or "+
				"reduce how many workflows it calls",
			maxStructureWalkNodes)
	}

	return nil
}
