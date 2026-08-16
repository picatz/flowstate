package flowstatev1

import "fmt"

// CheckPolicyPlacement refuses a workflow that sets `timeout:` or `retry:` on
// a step kind that schedules no single activity for either key to act on —
// including one nested inside a `call:`'s inlined callee, any number of calls
// deep up to [MaxCallDepth].
//
// This is the RPC-path half of the bound `pkg/flowstate/v1/flowfile` enforces
// while compiling a Flowfile (see that package's `checkPolicyPlacement`): a
// wait, a value, and the five composites — `for_each:`, `parallel:`, `call:`,
// `loop:`, and `switch:` — parse a `StepPolicy` because it compiles onto
// every step kind, but only a task's arm ever reads one (`activityOptionsFor`
// in the engine, and its counterpart in `eval.go`, are the one place per
// driver that consumes `timeout`/`retry`). A caller that builds a
// [Workflow] by hand and submits it directly to the RPC boundary never
// passes through that compiler, so without a second check here the same
// unsafe behavior the compile-time diagnostic exists to prevent — an author
// believing they bounded or retried a step that silently ignores both keys —
// reaches production for any specification that did not begin life as a
// Flowfile.
//
// [WalkWorkflow] does not descend into a call's callee — every other walk
// that shares it must not — so this function does that part itself, the same
// way [CheckStructureDepth] does: bounded by [maxStructureWalkNodes] total
// steps visited across every callee entered, and by [MaxCallDepth] for how
// many calls deep it follows, so a diamond-shaped or maximally-nested call
// graph cannot make the walk run long or loop forever. A `call:` step whose
// own policy is misplaced is still caught at the top level of this walk; the
// recursion is what catches one misplaced *inside* a callee a hand-built
// Workflow embedded directly, which [WalkWorkflow]'s contract does not reach.
//
// Called from `FlowstateServer.validateSpecification`, alongside
// [CheckSignalPolicies] and [CheckStructureDepth]: those ask other questions
// a compiled specification's shape can raise that protovalidate's per-field
// rules cannot see, and this is the policy-placement instance of the same
// question.
func CheckPolicyPlacement(wf *Workflow) error {
	var err error
	nodesLeft := maxStructureWalkNodes
	exhausted := false

	var walk func(w *Workflow, callDepth int)
	walk = func(w *Workflow, callDepth int) {
		if err != nil || exhausted {
			return
		}

		WalkWorkflow(w, Walk{
			Node: func(node *Node) {
				if err != nil || exhausted {
					return
				}
				if nodesLeft <= 0 {
					exhausted = true
					return
				}
				nodesLeft--

				var subject string
				switch node.GetKind().(type) {
				case *Node_Wait:
					subject = "a `wait:` step"
				case *Node_Value:
					subject = "a `value:` step"
				case *Node_ForEach:
					subject = "a `for_each:` step"
				case *Node_Parallel:
					subject = "a `parallel:` step"
				case *Node_Call:
					subject = "a `call:` step"
				case *Node_Loop:
					subject = "a `loop:` step"
				case *Node_Switch:
					subject = "a `switch:` step"
				}

				if subject != "" {
					if policy := node.GetPolicy(); policy != nil {
						switch {
						case policy.GetTimeout() != nil:
							err = fmt.Errorf(
								"step %q is %s: it schedules no single activity for `timeout:` to bound",
								node.GetId(), subject)
						case policy.GetRetry() != nil:
							err = fmt.Errorf(
								"step %q is %s: it schedules no single activity for `retry:` to re-run",
								node.GetId(), subject)
						}
						if err != nil {
							return
						}
					}
				}

				call, ok := node.GetKind().(*Node_Call)
				if !ok {
					return
				}
				callee := call.Call.GetWorkflow()
				if callee == nil {
					return
				}

				nextDepth := callDepth + 1
				if e := CheckCallDepth(nextDepth); e != nil {
					// A call chain this deep is refused at execution by
					// CheckCallDepth itself, regardless of what this walk
					// would find beneath it — descending further would only
					// be inspecting a callee nothing will ever run.
					return
				}

				walk(callee, nextDepth)
			},
		})
	}
	walk(wf, 0)

	if exhausted && err == nil {
		return fmt.Errorf(
			"the workflow's call graph holds more than %d steps once every `call:`'s callee is walked, "+
				"which is more than this server can inspect for misplaced retry/timeout policy; "+
				"flatten the call graph or reduce how many workflows it calls",
			maxStructureWalkNodes)
	}

	return err
}
