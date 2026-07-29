package flowfile

import (
	"fmt"
	"maps"
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
// celLiteralWords are the reserved words that stay refused as step ids.
//
// The rest of celReservedIdentifiers became legal when references were rooted:
// cel-go checks reserved words in identifier position only, and `steps.<id>` is a
// field select. These three are lexer tokens, not identifiers, so `steps.true` is
// a syntax error in the grammar itself and no rooting can help it.
//
// The full list is still needed, because a `for_each` iterator is still written
// bare and is still an identifier.
var celLiteralWords = []string{"true", "false", "null"}

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

	// Kind names the step's kind key — the task's name, or `for_each`, `sleep`,
	// and so on — when the problem is with that key rather than with anything
	// under it.
	//
	// Separate from Field because the two read differently and are fixed
	// differently. `step "x" input "url"` sends an author to a value they wrote
	// under the task; a wrong kind key sends them to the word naming the work
	// itself, which Message already quotes. It positions the same way Field does,
	// which is what lets `unknown task "shell"` underline `shell` instead of
	// falling back to the whole step.
	Kind string

	// Value is the literal at fault *inside* Field, when the field holds a list
	// and one element of it is the problem.
	//
	// This validator runs against the compiled workflow, which carries no
	// positions, so on its own it can only name the field: `libs: [json, nope]`
	// is reported against `libs`. A surface that does have positions — the
	// language server — can then underline `nope` rather than the whole list.
	//
	// Naming the element in a field rather than leaving it to be read back out of
	// Message is the point. Deriving a range from message text is how rewording a
	// diagnostic silently moves a squiggle somewhere else.
	Value string

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
	default:
		// No position at all, which happens for a problem with the document as a
		// whole rather than with anything in it. Callers join this to a filename
		// with a colon, so without a separator here `file.yaml` and the first word
		// of the message run together.
		b.WriteString(" ")
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
// nameRune reports whether a rune is one a workflow name may contain.
//
// Kept in step with the pattern the schema declares on Workflow.name. Written out
// rather than compiled from the descriptor because a diagnostic wants to name the
// offending character, and a regular expression can only say that the whole string
// did not match.
func nameRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		return true
	}
	return false
}

// firstIllegalNameRune describes the first character a name may not contain, or
// empty when every character is legal.
func firstIllegalNameRune(name string) string {
	for _, r := range name {
		if nameRune(r) {
			continue
		}
		if r == ' ' {
			return "spaces"
		}
		return fmt.Sprintf("%q", string(r))
	}
	return ""
}

// suggestedName is the name with every illegal character replaced, so the
// diagnostic can offer something to paste rather than a rule to apply.
func suggestedName(name string) string {
	return strings.Map(func(r rune) rune {
		if nameRune(r) {
			return r
		}
		return '-'
	}, name)
}

func Validate(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if wf == nil {
		return append(ds, Diagnostic{Message: "workflow is empty"})
	}
	if wf.GetName() == "" {
		ds = append(ds, Diagnostic{Field: "name", Message: "workflow has no name"})
	} else if bad := firstIllegalNameRune(wf.GetName()); bad != "" {
		// The schema constrains a workflow's name and nothing checked it here, so
		// a file with a space in its name compiled cleanly, said "ok", and was
		// refused by the server the first time anyone ran it. Eight of the shipped
		// examples were in that state.
		//
		// It is checked here because this is where a position exists. The same rule
		// enforced only at submit names a field path in a protobuf message, which
		// is true and useless to somebody looking at a line of YAML.
		ds = append(ds, Diagnostic{
			Field: "name",
			Message: fmt.Sprintf("name may not contain %s; a workflow name is used as an "+
				"identifier, so it takes letters, digits, - and _ (try %q)",
				bad, suggestedName(wf.GetName())),
		})
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
		case slices.Contains(celLiteralWords, id):
			// Only three words, where there used to be twenty-one.
			//
			// cel-go refuses a reserved word in *identifier* position and nowhere
			// else — its own parser says so, and excludes them post-parse because
			// they are valid field names. A step is named `steps.<id>` now, which is
			// field-select position, so `loop`, `in`, `for` and the other eighteen
			// became legal step ids the moment references were rooted.
			//
			// These three did not, and for a different reason: `true`, `false` and
			// `null` are lexer tokens rather than identifiers, so `steps.true` fails
			// in the grammar before any of that applies.
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is a literal in CEL rather than a name, so ${%s.%s} cannot be parsed; choose another id",
					id, v1.StepsRoot, id),
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
	scope := newRefScope()
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
				ds = append(ds, validateLoop(id, kind.ForEach, scope, i, wf)...)
				// A loop's body outputs do not escape it — only its own `results`
				// output does — so body step ids must not become referenceable.

			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, scope, i, wf)...)
				// Branch outputs are merged into the enclosing scope once the
				// block completes, so a later step may reference them by id.
				for _, branchID := range branchStepIDs(kind.Parallel) {
					scope.steps[branchID] = true
				}

			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, scope, i, wf)...)

			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			scope.steps[id] = true
			continue
		}

		if task.GetName() == "" {
			ds = append(ds, Diagnostic{Step: id, Message: "task has no name"})
		} else if _, known := v1.LookupTask(task.GetName()); !known {
			ds = append(ds, Diagnostic{
				Step: id,
				// Under the flattening the task's name is a key an author wrote, so
				// there is a token to underline rather than a whole step.
				Kind: task.GetName(),
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
		ds = append(ds, validateInputRefs(id, "if", node.GetCondition(), scope, i, wf)...)

		// What the task declares its inputs to be is checked separately from what
		// they reference, because the two fail differently: a reference that
		// cannot resolve is a mistake about the workflow, and an input the task
		// does not have is a mistake about the task.
		ds = append(ds, validateTaskInputs(id, task)...)
		ds = append(ds, validateTaskLibraries(id, task)...)

		checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
		for _, name := range sortedInputNames(checkable) {
			ds = append(ds, validateInputRefs(id, name, checkable[name], scope, i, wf)...)
		}

		// Only after a step's inputs are checked do its outputs become
		// available, which is what makes a self- or forward-reference detectable.
		scope.steps[id] = true
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

// A refScope is what the expressions in one step may name.
//
// Two sets, not one. Before rooting there was a single map holding step ids and
// loop iterators together, which is precisely the flat namespace this grammar
// removed: with both in it, a bare `${name}` inside a loop cannot be told from a
// reference to a step called `name`, and three collision rules existed only to
// make sure the two could never both be present.
//
// Keeping them apart is what deletes those rules. A step is named through the
// root and a local is named bare, so neither can be mistaken for the other and
// there is nothing left for a rule to forbid.
type refScope struct {
	// steps are the ids whose outputs exist at this point, reachable as
	// `steps.<id>`.
	steps map[string]bool

	// locals are the names bound bare here: a loop's iterator, and `now` inside a
	// wait expression. They are not steps and never were.
	locals map[string]bool
}

// newRefScope returns an empty scope.
func newRefScope() refScope {
	return refScope{steps: map[string]bool{}, locals: map[string]bool{}}
}

// clone returns a copy that can be extended without disturbing the original,
// which is what lets a loop body see its enclosing scope plus its own iterator
// while the steps after the loop see neither.
func (s refScope) clone() refScope {
	out := refScope{
		steps:  make(map[string]bool, len(s.steps)+1),
		locals: make(map[string]bool, len(s.locals)+1),
	}
	maps.Copy(out.steps, s.steps)
	maps.Copy(out.locals, s.locals)
	return out
}

// withLocal returns a copy with one more bare name bound.
func (s refScope) withLocal(name string) refScope {
	out := s.clone()
	if name != "" {
		out.locals[name] = true
	}
	return out
}

// validateLoop checks a for_each node and its body.
//
// The body is checked with the enclosing steps visible, because a body step may
// legitimately reference a step defined before the loop, plus the iterator, which
// exists only inside the body.
func validateLoop(stepID string, loop *v1.ForEach, enclosing refScope, index int, wf *v1.Workflow) Diagnostics {
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
	// An iterator sharing a step's id used to be refused here, because both
	// resolved from one namespace and the loop variable would hide the step. It is
	// no longer a collision: a step is named `steps.<id>` and an iterator is named
	// bare, so the two cannot be confused and there is nothing left to forbid.
	// This is the rule rooting was worth doing for.
	if iterator == v1.NowIdentifier {
		// The same refusal a step id gets, for the same reason and by the other
		// route into a wait's scope. A loop variable is bound into the scope's
		// vars; `wait_until:` binds the clock on top of that and wins. So a body
		// that says `${now}` reads as the item everywhere except inside a wait,
		// where it silently becomes the clock — and a deadline computed from the
		// wrong value is not a failure anyone sees, just a wait that ends at the
		// wrong moment.
		//
		// Refusing the name is the whole fix: binding order cannot be reversed
		// without making a step or a loop able to hide the clock, which is the
		// same problem pointed the other way.
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "iterator",
			Message: fmt.Sprintf(
				"%q is the built-in naming the moment a wait is evaluated, which a loop variable of the same name would shadow inside `wait_until:`; choose another iterator",
				iterator),
		})
	}

	// The iterator is bound as a *local*, not merged in beside the step ids. That
	// one line is the whole of what rooting deletes: with the two apart, an
	// iterator sharing a step's name is no longer ambiguous, so the rule that used
	// to forbid it has nothing left to prevent.
	return append(ds, validateNested(loop.GetBody(), enclosing.withLocal(iterator), index, wf)...)
}

// validateParallel checks a parallel node and its branches.
func validateParallel(stepID string, parallel *v1.Parallel, enclosing refScope, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if len(parallel.GetBranches()) == 0 {
		ds = append(ds, Diagnostic{Step: stepID, Field: "parallel", Message: "at least one branch is required"})
	}

	// Branch outputs merge into one namespace after the block, so ids must not
	// collide across branches — and a branch must not reference a sibling, since
	// branches are unordered.
	seen := make(map[string]bool, len(enclosing.steps))
	maps.Copy(seen, enclosing.steps)

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
func validateNested(nodes []*v1.Node, enclosing refScope, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	scope := enclosing.clone()

	for _, node := range nodes {
		id := node.GetId()
		if id == "" {
			ds = append(ds, Diagnostic{Message: "nested step has no id"})
		}

		// A nested id may not shadow one already in scope, for the same reason two
		// top-level steps may not share one: expressions resolve both from one
		// namespace, so a reference inside the body means whichever the engine
		// happens to bind last.
		//
		// This was missing while the top-level rule was present, which made the
		// hole exactly the one that is hardest to see — a body step is written far
		// from the step it collides with, often in a different part of the file, and
		// nothing said so. It also left a diagnostic about the body step landing on
		// the top-level one, since a source position is looked up by id.
		if id != "" && enclosing.steps[id] {
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is already used by a step this one is nested inside; expressions resolve both from one namespace, so a reference here would be ambiguous",
					id),
			})
		}

		task := node.GetTask()
		if task == nil {
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, scope, index, wf)...)
			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, scope, index, wf)...)
			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, scope, index, wf)...)
			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			scope.steps[id] = true
			continue
		}

		if _, known := v1.LookupTask(task.GetName()); !known && task.GetName() != "" {
			ds = append(ds, Diagnostic{
				Step: id,
				Kind: task.GetName(),
				Message: fmt.Sprintf("unknown task %q; available tasks are %s",
					task.GetName(), strings.Join(v1.TaskNames(), ", ")),
			})
		}

		ds = append(ds, validateInputRefs(id, "if", node.GetCondition(), scope, index, wf)...)
		ds = append(ds, validateTaskInputs(id, task)...)

		checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
		for _, name := range sortedInputNames(checkable) {
			ds = append(ds, validateInputRefs(id, name, checkable[name], scope, index, wf)...)
		}

		scope.steps[id] = true
	}
	return ds
}

// validateInputRefs reports references in one input that cannot resolve.
func validateInputRefs(stepID, inputName string, val *v1.Value, scope refScope, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	parsed := val.GetExpr()
	if parsed == nil {
		// A literal input carries no references, with one exception: the `cel`
		// task's expression arrives as a literal string containing CEL.
		return ds
	}

	rooted, bare := referencedIdentifiers(parsed)

	// A rooted reference names a step and can only fail by naming one that is not
	// in scope. There is no second reading of it to rule out, which is the point of
	// the root.
	for _, ref := range rooted {
		if scope.steps[ref] {
			continue
		}
		ds = append(ds, unresolvedStep(stepID, inputName, ref, index, wf)...)
	}

	for _, ref := range bare {
		// A name bound here — a loop's iterator, `now` inside a wait — is exactly
		// what stays bare, and is not a step.
		if scope.locals[ref] {
			continue
		}
		if ref == v1.StepsRoot {
			// The root itself, from a macro or `size(steps)`. It names every step
			// and there is nothing to narrow.
			continue
		}

		// Everything else written bare is either the retired spelling of a
		// reference or a name that means nothing, and the two want different
		// answers: one is a migration someone can run, the other is a mistake.
		if declaredAnywhere(ref, wf) {
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: fmt.Sprintf(
					"`%s` is a step, and a step is named `%s.%s` now; run `flow fix` to rewrite this file",
					ref, v1.StepsRoot, ref),
			})
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
		case ref == v1.NowIdentifier:
			// Reported as what it is rather than as an unknown step. `now` does
			// exist — an author has read about it, or copied a `wait_until:` — so
			// "unknown step" sends them looking for a step they never wrote. The
			// answer they need is that it is bound where a clock exists and not
			// here, and what to do instead.
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: "`now` is only available in `wait_until:`, where the engine binds it to the " +
					"moment the wait is evaluated; a task input is resolved inside an activity, which " +
					"has no clock that survives a retry, so compute the moment in a `wait_until:` or " +
					"pass the time in as an input",
			})
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
func referencedIdentifiers(parsed *expr.ParsedExpr) (rooted, bare []string) {
	roots, free := map[string]struct{}{}, map[string]struct{}{}
	collectReferences(parsed.GetExpr(), map[string]struct{}{}, roots, free)
	return sortedNames(roots), sortedNames(free)
}

// sortedNames returns a set's members in order, so one file always reports the
// same diagnostics in the same sequence.
func sortedNames(set map[string]struct{}) []string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// collectReferences separates the two ways a name can appear.
//
// A step is written `steps.<id>.<output>`, so it is a *field* of a select over
// the root rather than a free identifier at all. Anything still bare is either
// something that is legitimately bare — a loop binding, `now` — or a reference in
// the spelling this grammar retired, and those want different diagnostics. They
// are collected apart rather than merged and sorted out afterwards, because the
// distinction is exactly what the caller needs and is lost by then.
func collectReferences(e *expr.Expr, bound, rooted, free map[string]struct{}) {
	if e == nil {
		return
	}
	if sel := e.GetSelectExpr(); sel != nil {
		if step, ok := rootedStepName(sel, bound); ok {
			rooted[step] = struct{}{}
			return
		}
	}
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if _, isBound := bound[name]; !isBound {
			free[name] = struct{}{}
		}
	case *expr.Expr_SelectExpr:
		collectReferences(kind.SelectExpr.GetOperand(), bound, rooted, free)
	case *expr.Expr_CallExpr:
		collectReferences(kind.CallExpr.GetTarget(), bound, rooted, free)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectReferences(arg, bound, rooted, free)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			collectReferences(el, bound, rooted, free)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectReferences(entry.GetMapKey(), bound, rooted, free)
			collectReferences(entry.GetValue(), bound, rooted, free)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr

		// The range and the accumulator's start are evaluated outside the
		// comprehension's own scope.
		collectReferences(c.GetIterRange(), bound, rooted, free)
		collectReferences(c.GetAccuInit(), bound, rooted, free)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		collectReferences(c.GetLoopCondition(), inner, rooted, free)
		collectReferences(c.GetLoopStep(), inner, rooted, free)
		collectReferences(c.GetResult(), inner, rooted, free)
	}
}

// rootedStepName reads the step a rooted reference names.
//
// The chain is walked to its base rather than matched at a fixed depth, because
// the depth is whatever the author selected: `steps.a` is one select over the
// root and `steps.a.result.code` is three.
func rootedStepName(sel *expr.Expr_Select, bound map[string]struct{}) (string, bool) {
	var fields []string
	node := sel
	for node != nil {
		fields = append(fields, node.GetField())
		operand := node.GetOperand()
		if ident := operand.GetIdentExpr(); ident != nil {
			if ident.GetName() != v1.StepsRoot {
				return "", false
			}
			if _, shadowed := bound[v1.StepsRoot]; shadowed {
				// A comprehension bound the root's name. Then it is a binding, not
				// the root, and whatever hangs off it is not a step.
				return "", false
			}
			break
		}
		node = operand.GetSelectExpr()
	}
	if node == nil || len(fields) == 0 {
		return "", false
	}
	// Collected outermost first, so the step is the last one reached.
	return fields[len(fields)-1], true
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
		span, ok := positions.Locate(ds[i].Step, ds[i].Field)
		if ds[i].Kind != "" {
			// A kind key is addressed exactly rather than by the candidate search
			// Locate does for a field, because a kind is a key of the step and there
			// is nowhere else it could be.
			span, ok = positions.LocateKind(ds[i].Step, ds[i].Kind)
		}
		if ok {
			ds[i].Line = span.Start.Line
			ds[i].Column = span.Start.Column
		}
	}

	// Whether it will *fit* is a different question from whether it is well
	// formed, and an author should meet it here rather than at submit.
	//
	// It has no position, because there is nothing to point at: no single line is
	// at fault, the document is. A diagnostic with no line is unusual enough in
	// this package to be worth saying out loud — everything else here names a
	// token, and the exception is deliberate rather than an omission.
	if err := v1.CheckSpecSize(wf); err != nil {
		ds = append(ds, Diagnostic{Message: err.Error()})
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
func validateWait(id string, wait *v1.Wait, scope refScope, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if err := v1.ValidateWait(wait); err != nil {
		ds = append(ds, Diagnostic{Step: id, Message: err.Error()})
		return ds
	}

	if until := wait.GetUntil(); until != nil {
		// The same reference checking a condition gets, since it is the same kind
		// of expression resolving against the same names — plus `now`, which the
		// engine binds when it evaluates a deadline and binds nowhere else. Added
		// for this one field rather than to the workflow's scope, so that using it
		// in a task input is still reported: there is no clock behind it there,
		// and a name that resolves in one place and not another has to say so.
		ds = append(ds, validateInputRefs(id, "wait_until", until, scope.withLocal(v1.NowIdentifier), index, wf)...)
	}

	return ds
}

// declaredAnywhere reports whether a workflow has a step with this id, at any
// depth and whatever the order.
//
// Used to tell a retired spelling from a name that means nothing. `${a.result}`
// where a step is called `a` is a file someone wrote before the root existed, and
// the answer is one command; `${a.result}` where nothing is called `a` is a
// mistake, and the answer is to look at what they meant. Reporting the second
// message for the first case sends an author hunting for a typo they did not
// make.
func declaredAnywhere(id string, wf *v1.Workflow) bool {
	var walk func([]*v1.Node) bool
	walk = func(nodes []*v1.Node) bool {
		for _, node := range nodes {
			if node.GetId() == id {
				return true
			}
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				if walk(kind.ForEach.GetBody()) {
					return true
				}
			case *v1.Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					if walk(branch.GetSteps()) {
						return true
					}
				}
			}
		}
		return false
	}
	return walk(wf.GetSteps())
}

// unresolvedStep reports a rooted reference naming a step that is not in scope.
func unresolvedStep(stepID, inputName, ref string, index int, wf *v1.Workflow) Diagnostics {
	if ref == stepID {
		return Diagnostics{{
			Step: stepID, Field: inputName,
			Message: fmt.Sprintf("references its own step %q, which has no outputs yet", ref),
		}}
	}
	for _, other := range wf.GetSteps()[index:] {
		if other.GetId() == ref {
			return Diagnostics{{
				Step: stepID, Field: inputName,
				Message: fmt.Sprintf(
					"references step %q, which runs later; steps can only reference steps defined before them", ref),
			}}
		}
	}
	return Diagnostics{{
		Step: stepID, Field: inputName,
		Message: fmt.Sprintf("references unknown step %q", ref),
	}}
}
