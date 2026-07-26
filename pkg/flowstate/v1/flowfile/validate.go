package flowfile

import (
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Validation exists so that authoring mistakes are reported before a workflow
// runs. Without it, a typo surfaces only during execution — after earlier steps
// have already sent requests and changed state — and some mistakes never surface
// as errors at all. Two duplicate step IDs silently overwrite each other's
// outputs, and a step named after a CEL keyword makes every reference to it fail
// to parse for reasons that point nowhere near the real cause.

// celReservedIdentifiers are the words CEL will not accept as an identifier.
//
// A step whose ID is one of these compiles, and then every ${...} referencing it
// fails to parse. The list mirrors the parser's own; keeping a copy is necessary
// because it is not exported.
var celReservedIdentifiers = []string{
	"as", "break", "const", "continue", "else", "false", "for", "function",
	"if", "import", "in", "let", "loop", "namespace", "null", "package",
	"return", "true", "var", "void", "while",
}

// stepProperties are the fields belonging to a step itself rather than to the task
// it runs, so a diagnostic can name them the way the file writes them.
var stepProperties = map[string]bool{
	"if":                true,
	"timeout":           true,
	"retry":             true,
	"continue_on_error": true,
	"for_each":          true,
	"parallel":          true,
	"iterator":          true,
	"items":             true,
}

// A Diagnostic describes one problem found in a Flowfile.
type Diagnostic struct {
	// Line is the 1-based source line, or zero when the position is unknown.
	Line int

	// Column is the 1-based column within Line, or zero when only the line is
	// known. It counts characters rather than bytes.
	Column int

	// Step is the ID of the step the problem was found in, when applicable.
	Step string

	// Field names the input or property at fault, when applicable.
	Field string

	// Message states the problem and, where possible, how to fix it.
	Message string
}

// Error renders the diagnostic in the conventional line:column: message form so it
// is readable in a terminal and parseable by editors.
func (d Diagnostic) Error() string {
	var b strings.Builder
	switch {
	case d.Line > 0 && d.Column > 0:
		fmt.Fprintf(&b, "%d:%d: ", d.Line, d.Column)
	case d.Line > 0:
		fmt.Fprintf(&b, "%d: ", d.Line)
	}
	switch {
	case d.Step != "" && d.Field != "":
		// A step's own properties are named as written in the file; anything
		// else is a task input, which is worth distinguishing because the fix
		// differs — one edits the step, the other the task.
		if stepProperties[d.Field] {
			fmt.Fprintf(&b, "step %q %s: ", d.Step, d.Field)
		} else {
			fmt.Fprintf(&b, "step %q input %q: ", d.Step, d.Field)
		}
	case d.Step != "":
		fmt.Fprintf(&b, "step %q: ", d.Step)
	case d.Field != "":
		fmt.Fprintf(&b, "%s: ", d.Field)
	}
	b.WriteString(d.Message)
	return b.String()
}

// Diagnostics is a collection of problems found in a Flowfile.
type Diagnostics []Diagnostic

// Error summarizes the diagnostics, one per line.
func (ds Diagnostics) Error() string {
	parts := make([]string, 0, len(ds))
	for _, d := range ds {
		parts = append(parts, d.Error())
	}
	return strings.Join(parts, "\n")
}

// Err returns ds as an error, or nil when there are no diagnostics, so callers
// can use it directly in an error return.
func (ds Diagnostics) Err() error {
	if len(ds) == 0 {
		return nil
	}
	return ds
}

// Validate reports problems in a compiled workflow that would otherwise surface
// only at run time.
//
// It checks that steps are addressable (present, uniquely and legally named),
// that every task exists, and that every expression references a step whose
// outputs will actually be available.
func Validate(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if wf == nil {
		return append(ds, Diagnostic{Message: "workflow is empty"})
	}
	if wf.GetName() == "" {
		ds = append(ds, Diagnostic{Field: "name", Message: "workflow has no name"})
	}
	if len(wf.GetSteps()) == 0 {
		return append(ds, Diagnostic{Field: "steps", Message: "workflow has no steps"})
	}

	// Step IDs are the names expressions use, so they are validated before
	// anything that depends on resolving a reference.
	seen := make(map[string]int, len(wf.GetSteps()))
	for i, node := range wf.GetSteps() {
		id := node.GetId()

		switch {
		case id == "":
			ds = append(ds, Diagnostic{
				Field:   fmt.Sprintf("steps[%d]", i),
				Message: "step has no id; every step needs an id so later steps can reference its outputs",
			})
		case slices.Contains(celReservedIdentifiers, id):
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is a CEL reserved word, so ${%s.…} cannot be parsed; choose another id", id, id),
			})
		case !isCELIdentifier(id):
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is not a valid identifier, so ${%s.…} cannot be parsed; use letters, digits, and underscores, starting with a letter or underscore",
					id, id),
			})
		}

		if first, dup := seen[id]; dup && id != "" {
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"duplicate id, already used by step %d; ids must be unique or one step's outputs silently replace the other's",
					first+1),
			})
		} else if id != "" {
			seen[id] = i
		}
	}

	// Tasks and expression references.
	available := make(map[string]bool, len(wf.GetSteps()))
	for i, node := range wf.GetSteps() {
		id := node.GetId()
		task := node.GetTask()

		if task == nil {
			// A step may be a loop or a parallel block rather than a task. Its
			// nested steps are validated with the enclosing scope visible, since
			// a body step may legitimately reference a step defined before the
			// block it sits in.
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, available, i, wf)...)
				// A loop's body outputs do not escape it — only its own `results`
				// output does — so body step ids must not become referenceable.

			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, available, i, wf)...)
				// Branch outputs are merged into the enclosing scope once the
				// block completes, so a later step may reference them by id.
				for _, branchID := range branchStepIDs(kind.Parallel) {
					available[branchID] = true
				}

			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, available, i, wf)...)

			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			available[id] = true
			continue
		}

		if task.GetName() == "" {
			ds = append(ds, Diagnostic{Step: id, Message: "task has no name"})
		} else if _, known := v1.LookupTask(task.GetName()); !known {
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf("unknown task %q; available tasks are %s",
					task.GetName(), strings.Join(v1.TaskNames(), ", ")),
			})
		}

		// Some inputs are evaluated by the task itself, in a scope this
		// validator does not model — the http task's `outputs` expression
		// references the response, not earlier steps. Checking references in
		// those would report every correct use as an unknown step, and a false
		// diagnostic is worse than a missing one: it trains authors to ignore
		// the tool. The registry declares which inputs those are.
		// A condition is an expression like any other, and resolves against the
		// same names, so it is checked the same way.
		ds = append(ds, validateInputRefs(id, "if", node.GetCondition(), available, i, wf)...)

		// What the task declares its inputs to be is checked separately from what
		// they reference, because the two fail differently: a reference that
		// cannot resolve is a mistake about the workflow, and an input the task
		// does not have is a mistake about the task.
		ds = append(ds, validateTaskInputs(id, task)...)

		checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
		for _, name := range sortedInputNames(checkable) {
			ds = append(ds, validateInputRefs(id, name, checkable[name], available, i, wf)...)
		}

		// Only after a step's inputs are checked do its outputs become
		// available, which is what makes a self- or forward-reference detectable.
		available[id] = true
	}

	return ds
}

// branchStepIDs returns the ids of every step across a parallel block's branches,
// including those nested inside branch control flow whose outputs also merge out.
func branchStepIDs(parallel *v1.Parallel) []string {
	var ids []string
	for _, branch := range parallel.GetBranches() {
		ids = append(ids, mergedStepIDs(branch.GetSteps())...)
	}
	return ids
}

// mergedStepIDs returns the ids whose outputs become visible to steps following a
// list of nodes.
//
// A loop contributes only its own id, because its body's outputs are reported
// through its `results` output rather than merged. A nested parallel block
// contributes its branches' ids, because those are merged.
func mergedStepIDs(nodes []*v1.Node) []string {
	var ids []string
	for _, node := range nodes {
		ids = append(ids, node.GetId())
		if p, ok := node.GetKind().(*v1.Node_Parallel); ok {
			ids = append(ids, branchStepIDs(p.Parallel)...)
		}
	}
	return ids
}

// validateLoop checks a for_each node and its body.
//
// The body is checked with the enclosing steps visible, because a body step may
// legitimately reference a step defined before the loop, plus the iterator, which
// exists only inside the body.
func validateLoop(stepID string, loop *v1.ForEach, enclosing map[string]bool, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if loop.GetItems() == nil {
		ds = append(ds, Diagnostic{Step: stepID, Field: "for_each", Message: "items is required"})
	}

	iterator := v1.IteratorName(loop)
	if !isCELIdentifier(iterator) {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "iterator",
			Message: fmt.Sprintf("%q is not a valid identifier", iterator),
		})
	}
	if slices.Contains(celReservedIdentifiers, iterator) {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "iterator",
			Message: fmt.Sprintf("%q is a CEL reserved word, so ${%s} cannot be parsed", iterator, iterator),
		})
	}
	if enclosing[iterator] {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "iterator",
			Message: fmt.Sprintf(
				"%q is also a step id; expressions resolve both from one namespace, so the loop variable would hide the step",
				iterator),
		})
	}

	inner := make(map[string]bool, len(enclosing)+1)
	for k := range enclosing {
		inner[k] = true
	}
	inner[iterator] = true

	return append(ds, validateNested(loop.GetBody(), inner, index, wf)...)
}

// validateParallel checks a parallel node and its branches.
func validateParallel(stepID string, parallel *v1.Parallel, enclosing map[string]bool, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if len(parallel.GetBranches()) == 0 {
		ds = append(ds, Diagnostic{Step: stepID, Field: "parallel", Message: "at least one branch is required"})
	}

	// Branch outputs merge into one namespace after the block, so ids must not
	// collide across branches — and a branch must not reference a sibling, since
	// branches are unordered.
	seen := make(map[string]bool, len(enclosing))
	for k := range enclosing {
		seen[k] = true
	}

	for i, branch := range parallel.GetBranches() {
		for _, node := range branch.GetSteps() {
			if seen[node.GetId()] {
				ds = append(ds, Diagnostic{
					Step: node.GetId(),
					Message: fmt.Sprintf(
						"id is already used outside branch %d; parallel branches share one output namespace, so ids must be unique across them",
						i),
				})
			}
		}
		// Each branch sees only what existed before the block, never a sibling's
		// steps, which is what validation must model to catch a cross-branch
		// reference.
		ds = append(ds, validateNested(branch.GetSteps(), enclosing, index, wf)...)
		for _, node := range branch.GetSteps() {
			seen[node.GetId()] = true
		}
	}
	return ds
}

// validateNested checks a nested list of steps against the names visible to it.
func validateNested(nodes []*v1.Node, enclosing map[string]bool, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	available := make(map[string]bool, len(enclosing)+len(nodes))
	for k := range enclosing {
		available[k] = true
	}

	for _, node := range nodes {
		id := node.GetId()
		if id == "" {
			ds = append(ds, Diagnostic{Message: "nested step has no id"})
		}

		task := node.GetTask()
		if task == nil {
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, available, index, wf)...)
			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, available, index, wf)...)
			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, available, index, wf)...)
			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			available[id] = true
			continue
		}

		if _, known := v1.LookupTask(task.GetName()); !known && task.GetName() != "" {
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf("unknown task %q; available tasks are %s",
					task.GetName(), strings.Join(v1.TaskNames(), ", ")),
			})
		}

		ds = append(ds, validateInputRefs(id, "if", node.GetCondition(), available, index, wf)...)
		ds = append(ds, validateTaskInputs(id, task)...)

		checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
		for _, name := range sortedInputNames(checkable) {
			ds = append(ds, validateInputRefs(id, name, checkable[name], available, index, wf)...)
		}

		available[id] = true
	}
	return ds
}

// validateInputRefs reports references in one input that cannot resolve.
func validateInputRefs(stepID, inputName string, val *v1.Value, available map[string]bool, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	parsed := val.GetExpr()
	if parsed == nil {
		// A literal input carries no references, with one exception: the `cel`
		// task's expression arrives as a literal string containing CEL.
		return ds
	}

	for _, ref := range referencedIdentifiers(parsed) {
		if available[ref] {
			continue
		}

		// Distinguish the two ways a reference fails, because the fixes differ.
		declaredLater := false
		for _, other := range wf.GetSteps()[index:] {
			if other.GetId() == ref {
				declaredLater = true
				break
			}
		}

		switch {
		case ref == stepID:
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: fmt.Sprintf("references its own step %q, which has no outputs yet", ref),
			})
		case declaredLater:
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: fmt.Sprintf(
					"references step %q, which runs later; steps can only reference steps defined before them", ref),
			})
		default:
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: fmt.Sprintf("references unknown step %q", ref),
			})
		}
	}
	return ds
}

// referencedIdentifiers returns the free identifiers an expression references.
//
// Identifiers bound by a comprehension are excluded: in `items.map(x, x + 1)`
// the name `x` is introduced by the expression itself and is not a step
// reference. Reporting those would make every use of a comprehension look broken.
func referencedIdentifiers(parsed *expr.ParsedExpr) []string {
	found := map[string]struct{}{}
	collectFreeIdentifiers(parsed.GetExpr(), map[string]struct{}{}, found)

	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// collectFreeIdentifiers walks an expression, recording identifiers that are not
// bound by an enclosing comprehension.
func collectFreeIdentifiers(e *expr.Expr, bound, found map[string]struct{}) {
	if e == nil {
		return
	}
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if _, isBound := bound[name]; !isBound {
			found[name] = struct{}{}
		}
	case *expr.Expr_SelectExpr:
		collectFreeIdentifiers(kind.SelectExpr.GetOperand(), bound, found)
	case *expr.Expr_CallExpr:
		collectFreeIdentifiers(kind.CallExpr.GetTarget(), bound, found)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectFreeIdentifiers(arg, bound, found)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			collectFreeIdentifiers(el, bound, found)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectFreeIdentifiers(entry.GetMapKey(), bound, found)
			collectFreeIdentifiers(entry.GetValue(), bound, found)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr

		// The range and the accumulator's initial value are evaluated outside
		// the comprehension's scope.
		collectFreeIdentifiers(c.GetIterRange(), bound, found)
		collectFreeIdentifiers(c.GetAccuInit(), bound, found)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		collectFreeIdentifiers(c.GetLoopCondition(), inner, found)
		collectFreeIdentifiers(c.GetLoopStep(), inner, found)
		collectFreeIdentifiers(c.GetResult(), inner, found)
	}
}

// isCELIdentifier reports whether s is a legal CEL identifier.
func isCELIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// sortedInputNames returns input names in a stable order so diagnostics do not
// vary between runs over the same file.
func sortedInputNames(inputs map[string]*v1.Value) []string {
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// ValidateSource compiles a Flowfile and validates it, placing each diagnostic at
// the position in the source it is about.
//
// Positions come from the compiler, which recorded them as it read the document,
// rather than from searching the text afterwards. That is what lets a diagnostic
// about one input of a step with six of them point at that input.
//
// A failure to compile is returned as an error rather than as diagnostics: there
// is no workflow to validate, and the error already describes every problem the
// compiler found, with positions.
func ValidateSource(data []byte) (Diagnostics, error) {
	wf, positions, err := Parse(data)
	if err != nil {
		return nil, err
	}

	ds := Validate(wf)
	for i := range ds {
		if span, ok := positions.Locate(ds[i].Step, ds[i].Field); ok {
			ds[i].Line = span.Start.Line
			ds[i].Column = span.Start.Column
		}
	}
	return ds, nil
}

// validateWait checks a waiting step.
//
// A wait's outputs are referenceable like any other step's — `timed_out` always,
// plus whatever a signal's sender put in its payload — so the caller marks the step
// available afterwards. What can be checked here is the shape of the wait and the
// references in its own expression; what cannot is whether a payload will contain a
// given key, because that is up to whoever sends the signal and is not knowable from
// the file.
func validateWait(id string, wait *v1.Wait, available map[string]bool, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if err := v1.ValidateWait(wait); err != nil {
		ds = append(ds, Diagnostic{Step: id, Message: err.Error()})
		return ds
	}

	if until := wait.GetUntil(); until != nil {
		// The same reference checking a condition gets, since it is the same kind
		// of expression resolving against the same names.
		ds = append(ds, validateInputRefs(id, "wait_until", until, available, index, wf)...)
	}

	return ds
}
