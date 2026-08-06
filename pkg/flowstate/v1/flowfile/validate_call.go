package flowfile

import (
	"fmt"
	"maps"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"

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
//
// placement is the undo scope the `call:` step itself sits in, and the callee
// is validated at placement.IntoCall() rather than always at
// [v1.UndoScopeCall] — a call reached from inside a `for_each` body, a
// `parallel` branch, or a `loop:` body must carry that restriction into the
// callee, or the call would be a way to launder a compensation out of a scope
// that refuses one. Both execution drivers compose the identical way (see
// [v1.UndoScope.IntoCall]), which is what keeps `flow validate` from accepting
// a shape either engine refuses, or refusing one either engine would allow.
func validateCallAtDepth(id string, call *v1.Call, scope refScope, index int, wf *v1.Workflow, depth int, placement v1.UndoScope) Diagnostics {
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
	for _, name := range slices.Sorted(maps.Keys(call.GetArguments())) {
		declaration, ok := declared[name]
		if !ok {
			ds = append(ds, Diagnostic{
				Step: id, Field: "with." + name,
				Message: fmt.Sprintf("workflow %q declares no input named %q", callee.GetName(), name),
			})
			continue
		}

		// Calling a workflow should feel like calling a typed function: a
		// literal bound to a declared input is checked against that
		// declaration's type here, at compile time, through the identical
		// function [v1.BindRunInputs] runs at submit — rather than only
		// failing there, after whatever steps before this one in the run
		// already had their effect. An expression gets the same treatment
		// where the profile's own checker can determine its type without
		// running it; where it cannot (most of them, since `steps.x` and
		// `vars.x` type as `dyn`), it is exactly as unchecked here as it
		// always was, and [v1.BindRunInputs] still refuses a wrong type once
		// the expression has a value to check at run time.
		if diag := checkCallArgumentType(id, name, call.GetArguments()[name], declaration, callee.GetName()); diag != nil {
			ds = append(ds, *diag)
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
	for _, d := range validateAtDepth(callee, depth, placement.IntoCall()) {
		d.Step = id
		d.Message = fmt.Sprintf("workflow %q: %s", callee.GetName(), d.Message)
		ds = append(ds, d)
	}

	return ds
}

// checkCallArgumentType reports whether one `with:` argument's type can be
// known to disagree with what the callee declares, or nil when it cannot be
// known to (which includes agreeing, and includes "not known at all").
//
// A literal's type is exact, so it is checked directly through
// [v1.CheckInputValue] — the same function [v1.BindRunInputs] runs at submit,
// reached once rather than restated here. An expression's type is not always
// knowable without running it — `${steps.build.digest}` types as `dyn` the
// moment it names anything this validator did not declare a concrete type
// for — but where the profile's own checker *can* pin one down (a closed
// expression over literals: `${1 + 2}`, `${'a' + 'b'}`), disagreeing with the
// declaration is exactly as much a mistake as a literal's would be, and an
// author benefits from being told now rather than only when that expression
// is finally evaluated.
func checkCallArgumentType(stepID, name string, value *v1.Value, declaration *v1.InputDeclaration, calleeName string) *Diagnostic {
	switch value.GetKind().(type) {
	case *v1.Value_Literal:
		if err := v1.CheckInputValue(name, declaration, value); err != nil {
			return &Diagnostic{Step: stepID, Field: "with." + name, Message: err.Error()}
		}
		// A callee's constraints, not only its type, bind a call's own
		// arguments — the typed-function feel extends to preconditions: a
		// literal that violates the callee's `pattern:` or `must:` is a
		// mistake at the call site, caught here rather than only at the
		// run's own submit-equivalent inside BindRunInputs.
		if err := v1.CheckInputConstraints(name, declaration, value); err != nil {
			return &Diagnostic{Step: stepID, Field: "with." + name, Message: err.Error()}
		}
		return nil

	case *v1.Value_Expr:
		parsed := value.GetExpr()
		if parsed == nil {
			return nil
		}

		env, err := envDeclaring(referencedNames(parsed.GetExpr()))
		if err != nil {
			// A defect in this build rather than in the file; see typeErrors,
			// which makes the identical call for the identical reason.
			return nil
		}

		checked, issues := env.Check(cel.ParsedExprToAst(parsed))
		if issues != nil && issues.Err() != nil {
			// Does not even type-check on its own terms, which
			// checkExpressionTypes already reports; restating it here under a
			// type-mismatch banner would say the same thing twice in two
			// voices.
			return nil
		}

		declaredType, ok := declaredTypeOfCEL(checked.OutputType())
		if !ok {
			// Usually `dyn`, because the expression names a step or a var this
			// checker has no concrete type for. Left to run time, which is
			// exactly as far as this could ever be pinned down before the
			// expression actually has a value.
			return nil
		}
		if declaredType == declaration.GetType() {
			return nil
		}

		return &Diagnostic{
			Step: stepID, Field: "with." + name,
			Message: fmt.Sprintf(
				"with.%s is declared %s by workflow %q, but this expression always produces %s",
				name, v1.DeclaredTypeName(declaration.GetType()), calleeName, v1.DeclaredTypeName(declaredType)),
		}

	default:
		// Nil, or a kind `with:` cannot hold in the first place — a secret
		// reference is already refused before a Call.Arguments entry can be
		// one at all (see callArgumentValue in call.go), so nothing reaches
		// here for it.
		return nil
	}
}

// declaredTypeOfCEL maps a CEL type this validator's checker inferred back to
// the declared-type vocabulary an input is written in, when the two have an
// analogue at all.
//
// Unspecified for anything without one — `dyn`, a duration, a timestamp, an
// error type — which is read as "not statically knowable" by the caller
// rather than as a type this schema happens not to have a word for; either
// way the honest answer is silence rather than a guess.
func declaredTypeOfCEL(t *cel.Type) (v1.InputDeclaration_Type, bool) {
	if t == nil {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}

	switch t.Kind() {
	case types.StringKind:
		return v1.InputDeclaration_TYPE_STRING, true
	case types.IntKind, types.UintKind:
		return v1.InputDeclaration_TYPE_INT, true
	case types.DoubleKind:
		return v1.InputDeclaration_TYPE_FLOAT, true
	case types.BoolKind:
		return v1.InputDeclaration_TYPE_BOOL, true
	case types.MapKind:
		return v1.InputDeclaration_TYPE_STRUCT, true
	case types.ListKind:
		return v1.InputDeclaration_TYPE_LIST, true
	default:
		return v1.InputDeclaration_TYPE_UNSPECIFIED, false
	}
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
		case *v1.Node_Loop:
			// A loop body counts against the call-expansion bound exactly as a
			// `for_each` body does; without this arm a hand-built `call:` tree hides
			// unbounded nodes inside loop bodies and the bound this walk enforces is
			// bypassed.
			if !countBounded(kind.Loop.GetBody(), budget) {
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
