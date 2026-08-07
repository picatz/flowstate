package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
)

// A `vars:` block may not hold a secret reference, at either level.
//
// The rule and the reason are the compiler's — see `flowfile.notInVarHelp`, which
// says it to an author against a line and a column. This is the same rule where
// there is no file: a specification can be built by something that never was a
// Flowfile, and invariant 7 is not a property of the parser.
//
// A var is evaluated by the workflow: the top-level block once before the first
// step, a step's block just before that step, and both results are bound into scope
// and written to durable state — `RunState.vars` rides Continue-As-New. There is no
// activity in any of that, so a reference reaching here has no contained place to be
// resolved and no reader that could keep it out of history. Refusing it at submit is
// the fail-closed half of the compile-time diagnostic: a caller is still there to be
// told, and nothing has run.
//
// Checked from [BindRunInputs], the one function every submit path already calls —
// the server at `Run` and `CreateSchedule`, `flow run local` before its first step.
// One function, two callers, for the reason that file's package doc gives.

// CheckVarsHoldNoSecretRef refuses a workflow whose `vars:` hold a secret
// reference, at the workflow level or on any step, however deeply the reference is
// nested inside a structure.
//
// Exported so that a caller building a specification by hand can ask the same
// question the submit boundary asks, rather than discovering the refusal at submit.
func CheckVarsHoldNoSecretRef(wf *Workflow) error {
	if err := checkVarsMap(wf.GetVars(), "workflow"); err != nil {
		return err
	}
	return checkNodeVars(wf.GetSteps(), 0)
}

// maxVarScanDepth bounds how deeply this walk descends, through loop bodies,
// parallel branches, inlined callees, and the structures a var's value may nest.
//
// A bound rather than trust in the shape, because the specification is chosen by
// whoever can submit a run and depth is the resource this walk spends: a message
// decoded from the wire is depth-limited by the protobuf runtime, but one built in
// process is not, and a walk with no bound of its own is one whose safety is a
// property of its callers. Set to [maxConstraintValueDepth]'s value for
// [maxConstraintValueDepth]'s reason — deeper than anything a person writes, shallow
// enough that recursion bounded by it cannot exhaust a goroutine's stack.
const maxVarScanDepth = maxConstraintValueDepth

// checkNodeVars walks every node that can carry a `vars:` block, including the
// bodies of loops and the branches of a parallel — a var inside a loop body is
// evaluated once per iteration and is no more contained for it.
func checkNodeVars(nodes []*Node, depth int) error {
	if depth > maxVarScanDepth {
		// Refused rather than returned clean, per fail closed: past this the walk
		// stops being able to say a `vars:` block down there holds nothing, and a
		// check that cannot decide must not allow.
		return fmt.Errorf("steps nest more than %d deep, past what a specification is checked to; "+
			"nothing this deep can be confirmed to keep secret references out of `vars:`", maxVarScanDepth)
	}

	for _, node := range nodes {
		if err := checkVarsMap(node.GetVars(), fmt.Sprintf("step %q", node.GetId())); err != nil {
			return err
		}

		if loop := node.GetForEach(); loop != nil {
			if err := checkNodeVars(loop.GetBody(), depth+1); err != nil {
				return err
			}
		}
		if loop := node.GetLoop(); loop != nil {
			if err := checkNodeVars(loop.GetBody(), depth+1); err != nil {
				return err
			}
		}
		// A callee is inlined in the caller's own specification — `runCall` reads
		// it off the node and evaluates its `vars:` through [EvalVars] like any
		// other block — so a submission carrying one carries its vars too, and a
		// walk that stopped at the call would check the half of the specification
		// that happens to be spelled at the top level.
		if callee := node.GetCall().GetWorkflow(); callee != nil {
			if err := checkVarsMap(callee.GetVars(), fmt.Sprintf("workflow %q", callee.GetName())); err != nil {
				return err
			}
			if err := checkNodeVars(callee.GetSteps(), depth+1); err != nil {
				return err
			}
		}
		if parallel := node.GetParallel(); parallel != nil {
			for _, branch := range parallel.GetBranches() {
				if err := checkNodeVars(branch.GetSteps(), depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// checkVarsMap reports the first var holding a reference, by name.
//
// Sorted, so a specification with two of them names the same one every time: a
// refusal that moves between runs of the same submission reads as a flake rather
// than as a rule.
func checkVarsMap(vars map[string]*Value, where string) error {
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		if !holdsSecretRef(vars[name], 0) {
			continue
		}
		return fmt.Errorf("%s var %q is a secret reference, which a var may not hold: "+
			"a var is evaluated by the workflow and its value is written to durable history, "+
			"so reference the secret on the task input that consumes it instead",
			where, name)
	}
	return nil
}

// holdsSecretRef reports whether a value is a secret reference or has one nested
// anywhere inside a structure.
//
// A structure's entries are values in their own right — that is the whole of why
// [Value_Structure] exists — so a reference can sit arbitrarily deep in one, and a
// check that only looked at the top would pass exactly the value that motivated the
// type.
func holdsSecretRef(value *Value, depth int) bool {
	if depth > maxVarScanDepth {
		// The same fail-closed answer one level down: a structure too deep to walk
		// is treated as holding one, because "could not tell" and "does not" are
		// not the same answer and only one of them is safe.
		return true
	}

	switch kind := value.GetKind().(type) {
	case *Value_SecretRef:
		return true
	case *Value_Structure_:
		switch structure := kind.Structure.GetKind().(type) {
		case *Value_Structure_List_:
			for _, element := range structure.List.GetValues() {
				if holdsSecretRef(element, depth+1) {
					return true
				}
			}
		case *Value_Structure_Map_:
			for _, entry := range structure.Map.GetEntries() {
				if holdsSecretRef(entry, depth+1) {
					return true
				}
			}
		}
	}
	return false
}
