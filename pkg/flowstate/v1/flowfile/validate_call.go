package flowfile

import (
	"fmt"
	"maps"
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// validateCall checks a `call:` step: that its arguments resolve in the
// caller's own scope, that they cover what the callee declares and nothing
// else, and that the callee itself is well formed.
//
// The callee is validated here too, recursively, rather than only when someone
// opens that file directly — an author calling a workflow with a mistake in it
// meets that mistake at the call site, where they are looking, instead of only
// when the callee is validated on its own. [v1.CheckCallDepth] bounds how deep
// that recursion goes for the same reason it bounds the two execution drivers:
// a specification reaching this validator did not necessarily pass through
// [Parse], which is the one place depth is otherwise guaranteed to have been
// checked as the file was read.
func validateCallAtDepth(id string, call *v1.Call, scope refScope, index int, wf *v1.Workflow, depth int) Diagnostics {
	var ds Diagnostics

	if err := v1.CheckCallDepth(depth); err != nil {
		return append(ds, Diagnostic{Step: id, Field: "call", Message: err.Error()})
	}

	callee := call.GetWorkflow()
	if callee == nil {
		return append(ds, Diagnostic{Step: id, Field: "call", Message: "call has no workflow"})
	}

	// Arguments are resolved in the *caller's* scope — that is the whole of
	// [v1.CallScope]'s isolation guarantee — so they are checked against it
	// exactly as a task's inputs are.
	for _, name := range slices.Sorted(maps.Keys(call.GetArguments())) {
		ds = append(ds, validateInputRefs(id, "with."+name, call.GetArguments()[name], scope, index, wf)...)
	}

	declared := make(map[string]*v1.InputDeclaration, len(callee.GetDeclaredInputs()))
	for _, d := range callee.GetDeclaredInputs() {
		declared[d.GetName()] = d
	}
	for name := range call.GetArguments() {
		if _, ok := declared[name]; !ok {
			ds = append(ds, Diagnostic{
				Step: id, Field: "with." + name,
				Message: fmt.Sprintf("workflow %q declares no input named %q", callee.GetName(), name),
			})
		}
	}
	for _, d := range callee.GetDeclaredInputs() {
		if !d.GetRequired() || d.GetDefault() != nil {
			continue
		}
		if _, ok := call.GetArguments()[d.GetName()]; !ok {
			ds = append(ds, Diagnostic{
				Step: id, Field: "call",
				Message: fmt.Sprintf(
					"workflow %q requires input %q, which `with:` does not bind", callee.GetName(), d.GetName()),
			})
		}
	}

	// The callee's own steps, vars, and outputs, validated on its own terms and
	// in its own — isolated — scope: not the scope above, which is the caller's
	// and exactly what [v1.CallScope] refuses a callee.
	for _, d := range validateAtDepth(callee, depth) {
		d.Step = id
		d.Message = fmt.Sprintf("workflow %q: %s", callee.GetName(), d.Message)
		ds = append(ds, d)
	}

	return ds
}

// boundedCallExpansion reports whether wf's total compiled node count, once
// every call is counted, stays within [maxCallExpansionNodes].
//
// Called before anything else walks the tree, because a specification reaching
// this validator may not have passed through [Parse] — which enforces the same
// bound incrementally as a call tree is built — so this validator cannot assume
// a diamond-shaped tree was already refused. budget decrements as nodes are
// counted and the walk stops the moment it is exhausted, which is what keeps a
// hand-built diamond from costing this function what it would cost to fully
// walk.
func boundedCallExpansion(wf *v1.Workflow, budget *int) bool {
	return countBounded(wf.GetSteps(), budget)
}

func countBounded(nodes []*v1.Node, budget *int) bool {
	*budget += len(nodes)
	if *budget > maxCallExpansionNodes {
		return false
	}
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			if !countBounded(kind.ForEach.GetBody(), budget) {
				return false
			}
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				if !countBounded(branch.GetSteps(), budget) {
					return false
				}
			}
		case *v1.Node_Call:
			if !countBounded(kind.Call.GetWorkflow().GetSteps(), budget) {
				return false
			}
		}
	}
	return true
}
