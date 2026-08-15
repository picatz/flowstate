package flowstatev1

import "fmt"

// CheckPolicyPlacement refuses a workflow that sets `timeout:` or `retry:` on
// a step kind that schedules no single activity for either key to act on.
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
// Called from `FlowstateServer.validateSpecification`, alongside
// [CheckSignalPolicies] and [CheckStructureDepth]: those ask other questions
// a compiled specification's shape can raise that protovalidate's per-field
// rules cannot see, and this is the policy-placement instance of the same
// question.
func CheckPolicyPlacement(wf *Workflow) error {
	var err error

	WalkWorkflow(wf, Walk{
		Node: func(node *Node) {
			if err != nil {
				return
			}

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
			default:
				return
			}

			policy := node.GetPolicy()
			if policy == nil {
				return
			}

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
		},
	})

	return err
}
