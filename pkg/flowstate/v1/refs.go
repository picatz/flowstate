package flowstatev1

import (
	"slices"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// This file holds the one CEL-reference walker both drivers need.
//
// It moved here from the durable driver's Continue-As-New compaction code
// (pkg/flowstate/v1/engine/workflow.go), which is still its primary caller —
// but it answers a question the local driver and #229's static suppression
// analysis need too: which steps, and which of their outputs, does a value or
// a node's expressions still reference? A second implementation of "walk a
// parsed CEL expression looking for `steps.<id>.<field>`" would drift from
// this one exactly the way the house rule on shared logic warns about — the
// edge cases here (a map-literal key position, `has()` on a field a
// continue_on_error step never set, a whole-step reference sharing a step
// with a field one) were each found the hard way once and are not obvious to
// rediscover.

// WholeStep marks a step whose every output is needed, in the field set a
// [CollectNodeRefs]/[CollectValueRefs] walk otherwise fills with named
// outputs.
//
// An empty field set used to mean the same thing, and could not survive
// company: a step referenced whole by one expression and by field from
// another (`${steps.a}` in a step's `vars:` beside `${steps.a.foo}` in its
// input) recorded the empty set first and then had `foo` put into it, after
// which "everything" and "just foo" were the same value. The empty string is
// the marker because it cannot collide — an output name is `min_len: 1` in
// the schema, so no field can ever be spelled this way.
const WholeStep = ""

// MarkWholeStep records that every output of a step is needed.
func MarkWholeStep(refs map[string]map[string]struct{}, step string) {
	fields, seen := refs[step]
	if !seen {
		fields = map[string]struct{}{}
		refs[step] = fields
	}
	fields[WholeStep] = struct{}{}
}

// MarkStepField records that one named output of a step is needed.
func MarkStepField(refs map[string]map[string]struct{}, step, field string) {
	fields, seen := refs[step]
	if !seen {
		fields = map[string]struct{}{}
		refs[step] = fields
	}
	fields[field] = struct{}{}
}

// NeededOutputs returns the outputs to carry for one step, or the whole set
// when anything asked for it whole.
//
// A field reference that matches nothing on a step's actual outputs is not
// the same as no reference at all. A `continue_on_error` step that succeeded
// has no `error` field, so `has(steps.checkout.error)` finds nothing to keep
// here — but the step still ran, and on resume its key must resolve as
// "present, field absent" (has() false) rather than "no such key" (a hard CEL
// error). So this never reports "nothing needed" for a step a caller already
// knows was referenced: it always returns a non-nil value, even when every
// requested field comes up empty, and the step's key survives compaction.
// See #176.
func NeededOutputs(full *Node_Outputs, fields map[string]struct{}) *Node_Outputs {
	if _, whole := fields[WholeStep]; whole || len(fields) == 0 {
		return full
	}

	nv := map[string]*Value{}
	for field := range fields {
		if value, has := full.GetNamedValues()[field]; has {
			nv[field] = value
		}
	}

	return &Node_Outputs{NamedValues: nv}
}

// RootedStepRef reads a reference written under the steps root, returning the
// step it names and the output selected on it.
//
// Two shapes resolve, and a third deliberately does not:
//
//	steps.a.result         -> step "a", output "result"
//	steps.a.result.field   -> step "a", output "result"  (the outer select is CEL's)
//	steps.a                -> step "a", every output
//	steps                  -> not a reference to any one step
//
// The last is why this returns false rather than the whole root: an
// expression naming `steps` bare needs all of them, and saying so is the
// caller's job.
func RootedStepRef(sel *expr.Expr_Select) (step, field string, ok bool) {
	// Walked to the base rather than matched at a fixed depth, because the depth
	// is not fixed: `steps.a.result.code` is three selects over the root and
	// reaching only two of them leaves the reference unrecognised — which
	// prunes every output rather than one, silently.
	var fields []string
	node := sel
	for node != nil {
		fields = append(fields, node.GetField())
		operand := node.GetOperand()
		if ident := operand.GetIdentExpr(); ident != nil {
			if ident.GetName() != StepsRoot {
				return "", "", false
			}
			break
		}
		node = operand.GetSelectExpr()
	}
	if node == nil {
		return "", "", false
	}

	// Collected outermost first, so the step is last and its output second to
	// last.
	slices.Reverse(fields)
	switch len(fields) {
	case 0:
		return "", "", false
	case 1:
		// `steps.a` — the whole step.
		return fields[0], "", true
	default:
		// Anything deeper selects into the output, which is CEL's business. Only
		// the output itself has to be kept.
		return fields[0], fields[1], true
	}
}

// CollectRefsFromExpr recursively walks a CEL expression and records, into
// refs, the step IDs and fields it references. If a step is referenced
// without a field (e.g. just `a`), [WholeStep] is added to its set to mark
// that every output is needed.
func CollectRefsFromExpr(e *expr.Expr, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if e == nil {
		return
	}
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if prev == nil || prev.StepValues == nil {
			return
		}
		if _, ok := prev.StepValues[name]; ok {
			MarkWholeStep(refs, name)

			return
		}
		// The root named on its own — `has(steps.a)`, or handed to a macro —
		// asks for all of them, and there is no way to narrow that from here.
		// Kept whole rather than pruned to nothing, since being wrong in the
		// other direction costs history size and being wrong this way costs the
		// run.
		if name == StepsRoot {
			for id := range prev.StepValues {
				MarkWholeStep(refs, id)
			}
		}
	case *expr.Expr_SelectExpr:
		// A reference rooted at `steps` names its step one level further in:
		// `steps.a.result` is Select(Select(Ident("steps"), "a"), "result"), so
		// the step is the *field* of the inner select rather than an ident at
		// all.
		//
		// Handled before the bare form, and handled here rather than left to the
		// walk below, because the walk cannot see it: it looks for an ident
		// naming a step, finds `steps`, matches nothing, and records nothing —
		// after which the caller prunes every output and the activity is handed
		// an empty map. No error anywhere, and the run only fails after a
		// Continue-As-New.
		if step, field, ok := RootedStepRef(kind.SelectExpr); ok {
			if prev != nil && prev.StepValues != nil {
				if _, known := prev.StepValues[step]; known {
					if field == "" {
						MarkWholeStep(refs, step)
					} else {
						MarkStepField(refs, step, field)
					}
				}
			}
			return
		}

		// Handle a.b[.c...] by walking the operand chain down to the root ident.
		// We record the first field selected after the step ident.
		// For nested selects, this still captures the top-level output field.
		// E.g., a.result.subfield -> keep `result` from step `a`.
		// Traverse to find the base ident name.
		// Recursed into only when the operand is something other than the step
		// ident itself, which the loop below reads directly.
		//
		// The distinction is what `a` *means* here. Visited on its own it is a
		// whole-step reference and marks the step whole; visited as the operand
		// of `a.result` it is the root of a field reference, and marking it
		// whole there would carry every output of every step any expression
		// touched — the opposite mistake to the one [WholeStep] exists to
		// prevent, and one that shows up only as history nobody can explain.
		//
		// This used to work by accident: the ident case created an *empty* set,
		// the loop below then put a field into it, and empty-means-whole was
		// quietly overwritten. Making the marker explicit made the accident
		// visible as two failing tests.
		if _, bare := kind.SelectExpr.GetOperand().GetExprKind().(*expr.Expr_IdentExpr); !bare {
			CollectRefsFromExpr(kind.SelectExpr.GetOperand(), prev, refs)
		}
		// Try to find the base step ident name.
		base := kind.SelectExpr.GetOperand()
		for base != nil {
			switch b := base.GetExprKind().(type) {
			case *expr.Expr_IdentExpr:
				name := b.IdentExpr.GetName()
				if prev != nil && prev.StepValues != nil {
					if _, ok := prev.StepValues[name]; ok {
						if field := kind.SelectExpr.GetField(); field != "" {
							MarkStepField(refs, name, field)
						} else {
							MarkWholeStep(refs, name)
						}
					}
				}
				base = nil
			case *expr.Expr_SelectExpr:
				base = b.SelectExpr.GetOperand()
			case *expr.Expr_CallExpr:
				// could be obj.method() => method select on call target
				base = b.CallExpr.GetTarget()
			default:
				base = nil
			}
		}
	case *expr.Expr_CallExpr:
		if kind.CallExpr.GetTarget() != nil {
			CollectRefsFromExpr(kind.CallExpr.GetTarget(), prev, refs)
		}
		for _, a := range kind.CallExpr.GetArgs() {
			CollectRefsFromExpr(a, prev, refs)
		}
	case *expr.Expr_ListExpr:
		for _, e := range kind.ListExpr.GetElements() {
			CollectRefsFromExpr(e, prev, refs)
		}
	case *expr.Expr_StructExpr:
		for _, e := range kind.StructExpr.GetEntries() {
			// An entry's key is an expression too. `Expr_CreateStruct_Entry` has a
			// key_kind oneof: `field_key` is a bare string naming a message
			// field, but `map_key` is a full expression, and a map literal
			// written in a Flowfile — `${ {steps.name.result: steps.data.body} }`
			// — puts one there. Walking only the value made a reference in key
			// position invisible, so compaction pruned an output the resumed
			// segment then failed on. Every other CEL walker in the repo
			// (flowfile's validate, celcheck, secret and fixexpr passes) already
			// walks both halves; this one was the outlier.
			CollectRefsFromExpr(e.GetMapKey(), prev, refs)
			CollectRefsFromExpr(e.GetValue(), prev, refs)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		CollectRefsFromExpr(c.GetIterRange(), prev, refs)
		CollectRefsFromExpr(c.GetAccuInit(), prev, refs)
		CollectRefsFromExpr(c.GetLoopCondition(), prev, refs)
		CollectRefsFromExpr(c.GetLoopStep(), prev, refs)
		CollectRefsFromExpr(c.GetResult(), prev, refs)
	default:
		// literals and unknown kinds carry no references
	}
}

// CollectRefsFromParsedExpr extracts references from a ParsedExpr.
func CollectRefsFromParsedExpr(pe *expr.ParsedExpr, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if pe == nil {
		return
	}
	CollectRefsFromExpr(pe.GetExpr(), prev, refs)
}

// CollectValueRefs records the step outputs one value references.
//
// Only an expression can reference anything; a literal carries no reference
// regardless of what it happens to contain.
func CollectValueRefs(value *Value, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if kind, isExpr := value.GetKind().(*Value_Expr); isExpr {
		CollectRefsFromParsedExpr(kind.Expr, prev, refs)
	}
}

// CollectNodeRefs records every step output a node could still need.
//
// Every expression site in the node counts, not only a task's inputs: a
// step's condition, a step's own `vars:`, a loop's item list and everything
// in its body, every branch of a parallel block, and a wait's own
// expression. Dropping an output one of those needs is a correctness
// failure; keeping one it turns out not to need only costs payload. So when
// in doubt this keeps more.
//
// A call's own body is deliberately excluded — only its arguments are
// walked. An argument is resolved in the *caller's* scope, so a reference
// there is exactly as live as one in a task's inputs; the callee's steps run
// in the isolated scope [CallScope] builds, a different namespace than prev
// entirely, and walking into it here would either find nothing or, worse,
// collide with a caller step that happens to share an id. This is also what
// makes a callee unable to keep a caller's loop's results alive by
// referencing it: nothing inside a callee's own expressions is walked
// against the caller's steps at all.
func CollectNodeRefs(node *Node, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if node == nil {
		return
	}

	// A condition decides whether the step runs at all, so it is evaluated
	// before anything else and its references are needed just as much.
	CollectValueRefs(node.GetCondition(), prev, refs)

	// Outside the switch, with the condition, because both are step
	// *properties* rather than parts of one kind of work: a `for_each` and a
	// `wait` carry `vars:` exactly as a task step does.
	for _, value := range node.GetVars() {
		CollectValueRefs(value, prev, refs)
	}

	switch kind := node.GetKind().(type) {
	case *Node_Task:
		task := kind.Task
		for _, value := range task.GetInputs() {
			CollectValueRefs(value, prev, refs)
		}

	case *Node_ForEach:
		CollectValueRefs(kind.ForEach.GetItems(), prev, refs)
		for _, inner := range kind.ForEach.GetBody() {
			CollectNodeRefs(inner, prev, refs)
		}

	case *Node_Loop:
		// Every one of a loop's own expressions can name an outer step's
		// output, and each has to survive the Continue-As-New a long loop
		// suspends across. `init:` is evaluated once before the loop; `until:`
		// and `update:` after every iteration's body; and the body's own nodes
		// recurse the same way a `for_each`'s do.
		CollectValueRefs(kind.Loop.GetInitial(), prev, refs)
		CollectValueRefs(kind.Loop.GetUntil(), prev, refs)
		CollectValueRefs(kind.Loop.GetUpdate(), prev, refs)
		for _, inner := range kind.Loop.GetBody() {
			CollectNodeRefs(inner, prev, refs)
		}

	case *Node_Parallel:
		for _, branch := range kind.Parallel.GetBranches() {
			for _, inner := range branch.GetSteps() {
				CollectNodeRefs(inner, prev, refs)
			}
		}

	case *Node_Wait:
		// A `wait_until` expression can name a step's output — "wait until the
		// deadline the previous step computed" — and a run that suspended
		// before the wait needs that output to still be there when it resumes.
		//
		// The two computed durations are the same fact about the same node, and
		// missing one would be worse than missing a diagnostic: compaction drops
		// an output nothing appears to need, and the run fails on resume naming a
		// step it can no longer see. Every expression a [Wait] can hold is read
		// here, so growing the message means growing this.
		CollectValueRefs(kind.Wait.GetUntil(), prev, refs)
		CollectValueRefs(kind.Wait.GetDurationExpr(), prev, refs)
		CollectValueRefs(kind.Wait.GetTimeoutExpr(), prev, refs)

	case *Node_Call:
		for _, value := range kind.Call.GetArguments() {
			CollectValueRefs(value, prev, refs)
		}
	}
}
