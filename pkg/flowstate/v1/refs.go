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
// regardless of what it happens to contain — but an expression can be *inside* a
// structure, so this descends into one.
//
// That arm is not defensive. A [Value_Structure] used to hold literals and secret
// references and nothing else, so reading only the top level answered correctly
// by accident; output shaping written as a mapping compiles per entry, and each
// entry is an ordinary expression that may read `${steps.x.y}` or `${vars.n}`.
// Without the descent, compaction at a Continue-As-New boundary would drop the
// very output a shaped expression is about to read — a correctness failure, and
// exactly the walk-misses-a-branch shape this repository keeps paying for.
// The descent is depth-bounded for [MaxStructureDepth]'s reason and not a
// different one: a specification does not have to have come from a Flowfile, so
// the depth of a structure is a number an outside party chooses, and this walk
// is recursive.
//
// Past the bound, this fails closed rather than silently stopping: every step
// [prev] carries is marked as needed whole (see [MarkWholeStep]), because an
// expression this walk cannot see the bottom of might reference any of them.
// The alternative — CLAUDE.md's `walkSecretRefs` shape, answering "may hold a
// reference" rather than "holds none" — is exactly this question asked by
// Continue-As-New compaction instead of by secret authority: dropping an
// output a live expression still needs fails a run after compaction, so the
// walk that decides retention must answer conservatively at its own limit
// the same way the authority walk already does. No Flowfile can express a
// structure this deep — a shaped mapping's entries are values, not
// structures, and the compiler bounds nesting far below this — so the shape
// that reaches the bound is a specification submitted directly, where the
// alternative is a stack depth the peer decides.
func CollectValueRefs(value *Value, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	collectValueRefs(value, 0, prev, refs)
}

func collectValueRefs(value *Value, depth int, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if depth > MaxStructureDepth {
		retainAllSteps(prev, refs)
		return
	}

	switch kind := value.GetKind().(type) {
	case *Value_Expr:
		CollectRefsFromParsedExpr(kind.Expr, prev, refs)
	case *Value_Structure_:
		switch structure := kind.Structure.GetKind().(type) {
		case *Value_Structure_List_:
			for _, element := range structure.List.GetValues() {
				collectValueRefs(element, depth+1, prev, refs)
			}
		case *Value_Structure_Map_:
			for _, entry := range structure.Map.GetEntries() {
				collectValueRefs(entry, depth+1, prev, refs)
			}
		}
	}
}

// retainAllSteps marks every step prev carries as needed whole.
//
// The fail-closed answer for a walk that has lost the ability to say which
// step an expression references: over-retaining costs payload bytes on one
// Continue-As-New, and under-retaining fails the run the first time the
// pruned output is read.
func retainAllSteps(prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	for step := range prev.GetStepValues() {
		MarkWholeStep(refs, step)
	}
}

// CollectNodeRefs records every step output a node could still need.
//
// Every value position in the node counts, not only a task's inputs: a step's
// condition, a step's own `vars:`, a step's `undo:` inputs, a loop's item list
// and everything in its body, every branch of a parallel block, and a wait's
// own expressions. Dropping an output one of those needs is a correctness
// failure; keeping one it turns out not to need only costs payload. So when in
// doubt this keeps more.
//
// The positions come from [WalkNode] rather than from a list kept here, which is
// what closes the failure this walk had: `Node.undo` landed in the schema after
// this was written, nothing added it here, and a step whose compensation named an
// outstanding async step joined nothing, ran, succeeded and then could not
// resolve its own compensation (#492). All three callers had that blind spot
// behind one omission — [AsyncJoinTargets], the durable driver's Continue-As-New
// compaction, and [LoopResultsReferenced] — and each ended with an effect
// performed and no compensation registered for it. Growing the schema now means
// growing walk.go, and this gets the new position without being edited.
//
// A call's own body is deliberately excluded — only its arguments are walked, by
// the traversal, for the reason [NodeRecursionEdges] records: an argument is
// resolved in the *caller's* scope, so a reference there is exactly as live as
// one in a task's inputs, while the callee's steps run in the isolated scope
// [CallScope] builds, a different namespace than prev entirely. Walking into it
// here would either find nothing or, worse, collide with a caller step that
// happens to share an id. It is also what makes a callee unable to keep a
// caller's loop's results alive by referencing it.
func CollectNodeRefs(node *Node, prev *Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	WalkNode(node, Walk{
		Value: func(site ValueSite) {
			// Every position, with nothing filtered out. This walk has no reason
			// to be selective: a reference is a reference wherever it is written,
			// and the cost of reading one position too many is a payload byte
			// while the cost of reading one too few is a failed run.
			CollectValueRefs(site.Value, prev, refs)
		},
		// The shared traversal's own bound and [CollectValueRefs]'s are the
		// identical resource, so both fail closed the identical way: a
		// position this walk could not finish looking inside of might still
		// reference any step, so every step is retained rather than none.
		Truncated: func(ValueSite) {
			retainAllSteps(prev, refs)
		},
	})
}
