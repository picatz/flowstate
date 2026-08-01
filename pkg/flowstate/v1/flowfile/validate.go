package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
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
// celUnusableStepIDs are the words no step may be named even under the root.
//
// Seventeen of the twenty-one in celReservedIdentifiers became legal the moment
// references were rooted: cel-go refuses a reserved word in *identifier* position
// and nowhere else, and `steps.<id>` is a field select. These four are refused a
// level lower, by the lexer, which no amount of qualifying can reach — `true`,
// `false` and `null` are literals and `in` is an operator, so `steps.in` is a
// syntax error in the grammar itself.
//
// `in` is the one that is easy to miss, and missing it is not harmless: the step
// compiles, and then every reference to it fails to *parse*, so the author gets a
// syntax error pointing at an expression instead of a diagnostic pointing at the
// id — which is precisely the failure celReservedIdentifiers exists to prevent.
// TestCELWordsUnusableAsStepIDs derives this set from cel-go rather than trusting
// the reasoning above.
//
// The full list is still needed, because a `for_each` iterator is still written
// bare and so is still an identifier.
var celUnusableStepIDs = []string{"true", "false", "null", "in"}

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
	"as":                true,
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
		case id == v1.StepsRoot:
			// The root itself. Refused as an id, and this is the one collision
			// rooting *creates* rather than removes — worth stating, because the
			// rest of this change is about deleting rules like it.
			//
			// It has to be refused here rather than left to resolve, because the
			// runtime deliberately lets a step of this name win: a spec compiled
			// before the root existed may contain one, and a worker replaying it
			// must keep resolving the way it always did. That compatibility is only
			// safe while no *new* file can create the situation — otherwise a step
			// called `steps` shadows the root, and every rooted reference in the
			// file resolves against that step's outputs instead. Which validates
			// clean and fails at run time with `no such key`.
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is the root every step is named under, so a step called that would hide all the others; choose another id",
					id),
			})
		case slices.Contains(celUnusableStepIDs, id):
			// Four words, where there used to be twenty-one — see celUnusableStepIDs
			// for which seventeen rooting made legal and why these did not follow.
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is punctuation in CEL rather than a name, so ${%s.%s} cannot be parsed at all; choose another id",
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

	ds = append(ds, validateWorkflowVars(wf)...)

	// What is wrong with an expression regardless of what the file means — a
	// function nobody declared, an operator with no overload for the types written
	// beside it. Separate from the reference checks below and deliberately unaware
	// of scope; see celcheck.go for why that is what keeps it from reporting the
	// same mistake twice in two voices.
	ds = append(ds, checkExpressionTypes(wf)...)

	// Tasks and expression references.
	scope := newRefScope(wf)
	for i, node := range wf.GetSteps() {
		id := node.GetId()
		task := node.GetTask()

		// The step's own `vars:`, in scope for everything the step contains — its
		// inputs, and for a block its items expression and its whole body. Derived
		// before the kind is known because the rule does not depend on the kind: a
		// name a node declares is bound throughout that node and nowhere else.
		//
		// It was derived inside the task branch first, which made the validator and
		// the engine disagree about a loop: the engine binds a loop's vars for its
		// body, and the validator did not know they were bound, so a body step
		// redeclaring one was allowed to silently shadow it.
		inner, varDiagnostics := scopeWithStepVars(id, node, scope, i, wf)
		ds = append(ds, varDiagnostics...)

		if task == nil {
			// A step may be a loop or a parallel block rather than a task. Its
			// nested steps are validated with the enclosing scope visible, since
			// a body step may legitimately reference a step defined before the
			// block it sits in.
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, inner, i, wf)...)
				// A loop's body outputs do not escape it — only its own `results`
				// output does — so body step ids must not become referenceable.

			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, inner, i, wf)...)
				// Branch outputs are merged into the enclosing scope once the
				// block completes, so a later step may reference them by id.
				for _, branchID := range branchStepIDs(kind.Parallel) {
					scope.steps[branchID] = true
				}

			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, inner, i, wf)...)

			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			scope.steps[id] = true
			continue
		}

		ds = append(ds, validateTaskStep(id, node, task, scope, inner, i, wf)...)

		// Only after a step's inputs are checked do its outputs become
		// available, which is what makes a self- or forward-reference detectable.
		scope.steps[id] = true
	}

	return ds
}

// validateWorkflowVars reports references in the workflow's own `vars:` block, where
// the answer for every one of them is that it cannot resolve.
//
// The block is evaluated once, before the first step, against a scope holding nothing:
// no step has run, no loop is open, and a var may not read a sibling because a protobuf
// map has no order for "the one above" to mean. So the engine's rule is simply that a
// var is literals, operators and the profile's functions — and until this ran, nothing
// said so until run time, where the failure arrives as a workflow that starts and dies
// before its first step with a message about an unknown name.
//
// Which reference it is decides the sentence, because the three are different mistakes.
// `vars.other` is someone expecting a `let` block. `steps.x` is someone forgetting when
// this is evaluated. A bare name is usually neither — it is a name that means nothing
// anywhere, and the general diagnostic already says that well.
func validateWorkflowVars(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	for _, name := range slices.Sorted(maps.Keys(wf.GetVars())) {
		parsed := wf.GetVars()[name].GetExpr()
		if parsed == nil {
			continue
		}

		field := v1.VarsRoot + "." + name
		rooted, vars, bare := referencedIdentifiers(parsed)

		for _, ref := range vars {
			ds = append(ds, Diagnostic{
				Field: field, Value: ref,
				Message: fmt.Sprintf(
					"a var may not read another var: %q is evaluated at the same moment as %q, and "+
						"`%s:` is a mapping rather than a sequence, so there is no order that would "+
						"make one available to the other; inline the value, or compute it in a step",
					name, ref, v1.VarsRoot),
			})
		}

		for _, ref := range rooted {
			ds = append(ds, Diagnostic{
				Field: field, Value: ref.ID,
				Message: fmt.Sprintf(
					"a var may not read a step: `%s:` is evaluated once before the first step runs, "+
						"so %q has produced nothing yet; move this into the step that needs it, or "+
						"give the step an input",
					v1.VarsRoot, ref.ID),
			})
		}

		for _, ref := range bare {
			if ref == v1.StepsRoot || ref == v1.VarsRoot {
				// A root as an operand, which fails here for the same reason a
				// selection through it does — and is described by the two loops
				// above, not by the general "unknown name" sentence.
				ds = append(ds, Diagnostic{
					Field: field, Value: ref,
					Message: fmt.Sprintf(
						"a var may not read `%s`: `%s:` is evaluated before the first step runs, "+
							"against a scope holding literals, operators and the profile's functions "+
							"and nothing else",
						ref, v1.VarsRoot),
				})
				continue
			}

			if functionNamespaces[ref] {
				// The profile's own functions, which a var may absolutely use — the
				// sentence below says so and this used to refuse them anyway.
				continue
			}

			ds = append(ds, Diagnostic{
				Field: field, Value: ref,
				Message: fmt.Sprintf(
					"references unknown name %q; a var is evaluated before the first step runs, so "+
						"it may use literals, operators and the profile's functions and nothing else",
					ref),
			})
		}
	}

	return ds
}

// validateTaskStep checks everything about one task step that does not depend on
// whether it is written at the top level or nested inside a block.
//
// Shared rather than written twice, which it was: the top-level walk and
// [validateNested] each had their own copy of this sequence, and step-level `vars:`
// was added to one of them. A file whose only step was top level then reported the
// name it had just declared as unknown — the feature worked exactly where a test
// happened not to look. Anything a step means regardless of nesting belongs here, and
// what remains at each call site is what genuinely differs about the position.
func validateTaskStep(id string, node *v1.Node, task *v1.Task, scope, inner refScope, index int, wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	if task.GetName() == "" {
		ds = append(ds, Diagnostic{Step: id, Message: "task has no name"})
	} else if _, known := v1.LookupTask(task.GetName()); !known {
		message := fmt.Sprintf("unknown task %q; available tasks are %s",
			task.GetName(), strings.Join(v1.TaskNames(), ", "))

		// A dot names a plugin — `slack.post` is the `post` task of the plugin
		// discovered as `flowstate-plugin-slack` — so the honest diagnosis is
		// about installation, not spelling. "Unknown task" would send the author
		// to check a name that may be exactly right, in a file that will run
		// unchanged on a worker that has the plugin; and this validator, which
		// launches nothing, genuinely cannot tell that case from a typo. Saying
		// what it cannot know is the diagnostic here.
		if plugin, _, dotted := strings.Cut(task.GetName(), "."); dotted {
			message = fmt.Sprintf("no plugin task %q is registered here; if the %q plugin "+
				"is installed on the worker this will run on, the file is fine and this "+
				"process simply has not loaded it — `flow plugins` shows what a plugin "+
				"directory provides", task.GetName(), plugin)
		}

		ds = append(ds, Diagnostic{
			Step: id,
			// Under the flattening the task's name is a key an author wrote, so
			// there is a token to underline rather than a whole step.
			Kind:    task.GetName(),
			Message: message,
		})
	}

	// Some inputs are evaluated by the task itself, in a scope this validator does
	// not model — the http task's `outputs` expression references the response, not
	// earlier steps. Checking references in those would report every correct use as
	// an unknown step, and a false diagnostic is worse than a missing one: it trains
	// authors to ignore the tool. The registry declares which inputs those are.
	//
	// A condition is an expression like any other and resolves against the same
	// names, so it is checked the same way — but *before* the step's own vars are in
	// scope, because it decides whether the step runs at all, so a var this step
	// declares does not exist yet when the question is asked.
	ds = append(ds, validateInputRefs(id, "if", node.GetCondition(), scope, index, wf)...)

	// What the task declares its inputs to be is checked separately from what they
	// reference, because the two fail differently: a reference that cannot resolve is
	// a mistake about the workflow, and an input the task does not have is a mistake
	// about the task.
	ds = append(ds, validateTaskInputs(id, task)...)

	checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
	for _, name := range sortedInputNames(checkable) {
		ds = append(ds, validateInputRefs(id, name, checkable[name], inner, index, wf)...)
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

	// vars are the workflow's declared vars, reachable as `vars.<name>`.
	//
	// Unlike steps, this set does not grow as the walk proceeds: workflow vars are
	// evaluated before any step runs, so every step sees all of them and there is no
	// forward-reference case to report. That is the whole difference between an
	// ambient name and a step output, and it is why the two roots need different
	// diagnostics even though they share a resolution rule.
	vars map[string]bool
}

// newRefScope returns a scope holding the workflow's declared vars and nothing else.
//
// The vars are in from the start rather than accumulated, because that is when they
// exist: they are evaluated once before any step runs, so the first step sees the same
// set as the last.
func newRefScope(wf *v1.Workflow) refScope {
	vars := make(map[string]bool, len(wf.GetVars()))
	for name := range wf.GetVars() {
		vars[name] = true
	}

	return refScope{steps: map[string]bool{}, locals: map[string]bool{}, vars: vars}
}

// clone returns a copy that can be extended without disturbing the original,
// which is what lets a loop body see its enclosing scope plus its own iterator
// while the steps after the loop see neither.
func (s refScope) clone() refScope {
	out := refScope{
		steps:  make(map[string]bool, len(s.steps)+1),
		locals: make(map[string]bool, len(s.locals)+1),
		// Shared rather than copied: nothing extends the var set after the scope is
		// built, because workflow vars all exist before the first step runs.
		vars: s.vars,
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
			Step: stepID, Field: "as",
			Message: fmt.Sprintf("%q is not a valid identifier", iterator),
		})
	}
	if slices.Contains(celReservedIdentifiers, iterator) {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as",
			Message: fmt.Sprintf("%q is a CEL reserved word, so ${%s} cannot be parsed", iterator, iterator),
		})
	}
	if iterator == v1.StepsRoot {
		// The root, by the other route into a body's scope. A bound name wins over
		// the scope it is bound into, so an iterator spelled `steps` hides every
		// step from the body — and the body is exactly where rooted references are
		// written.
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as",
			Message: fmt.Sprintf(
				"%q is the root every step is named under, and a loop variable of that name would hide all of them inside the body; choose another iterator",
				iterator),
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
			Step: stepID, Field: "as",
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
		if id == v1.StepsRoot {
			// The same refusal a top-level id gets, for the same reason. A nested
			// step's outputs are named through the root too, so one called `steps`
			// hides them from everything after it in the body.
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is the root every step is named under, so a step called that would hide all the others; choose another id",
					id),
			})
		}

		if id != "" && enclosing.steps[id] {
			ds = append(ds, Diagnostic{
				Step: id,
				Message: fmt.Sprintf(
					"id %q is already used by a step this one is nested inside; expressions resolve both from one namespace, so a reference here would be ambiguous",
					id),
			})
		}

		// See the top-level walk: a node's own vars are bound throughout it, whatever
		// kind of work it turns out to do.
		inner, varDiagnostics := scopeWithStepVars(id, node, scope, index, wf)
		ds = append(ds, varDiagnostics...)

		task := node.GetTask()
		if task == nil {
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, inner, index, wf)...)
			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, inner, index, wf)...)
			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, inner, index, wf)...)
			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			scope.steps[id] = true
			continue
		}

		ds = append(ds, validateTaskStep(id, node, task, scope, inner, index, wf)...)

		scope.steps[id] = true
	}
	return ds
}

// scopeWithStepVars adds a step's own `vars:` to the scope its inputs are checked
// against, and reports what is wrong with them.
//
// A step's vars are bare and *private*: the returned scope is used for this step's
// inputs and thrown away, so a name one step binds is not in scope for the next. That
// is what makes them safe to name freely — `modified` in one step has nothing to do
// with `modified` in another.
//
// Two things are refused here.
//
// Each var's own expression is checked against the scope *without* the step's vars in
// it, so a var cannot read a sibling. A protobuf map has no order, so "the one above"
// is not something the file can mean; the same rule the workflow's vars follow, for
// the same reason.
//
// And a name that collides with a bare name already in scope is refused rather than
// resolved. Silent shadowing is how `${body}` comes to mean two things eleven lines
// apart, and a precedence rule is something every reader would have to know before
// they could read the second one correctly.
func scopeWithStepVars(id string, node *v1.Node, scope refScope, index int, wf *v1.Workflow) (refScope, Diagnostics) {
	vars := node.GetVars()
	if len(vars) == 0 {
		return scope, nil
	}

	var ds Diagnostics
	next := scope.clone()

	for _, name := range slices.Sorted(maps.Keys(vars)) {
		ds = append(ds, validateInputRefs(id, "vars."+name, vars[name], scope, index, wf)...)

		switch {
		case scope.locals[name]:
			ds = append(ds, Diagnostic{
				Step: id, Field: "vars." + name, Value: name,
				Message: fmt.Sprintf(
					"`%s` is already bound here by an enclosing loop or step, and a bare name may "+
						"mean one thing at a time; rename this one, or read the outer value under a "+
						"different name", name),
			})

		case name == v1.NowIdentifier:
			ds = append(ds, Diagnostic{
				Step: id, Field: "vars." + name, Value: name,
				Message: "`" + v1.NowIdentifier + "` is the moment a `wait_until:` is evaluated, so a " +
					"var of that name would shadow it wherever both are in scope; rename this one",
			})

		default:
			next.locals[name] = true
		}
	}

	return next, ds
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

	rooted, vars, bare := referencedIdentifiers(parsed)

	// A var can only fail one way — nothing declared it — because they all exist
	// before the first step runs. So there is no forward reference to distinguish and
	// no scope to explain, which is what lets this be the shortest of the three.
	for _, ref := range vars {
		if scope.vars[ref] {
			continue
		}
		ds = append(ds, unresolvedVar(stepID, inputName, ref, scope))
	}

	// A rooted reference names a step and can only fail by naming one that is not
	// in scope. There is no second reading of it to rule out, which is the point of
	// the root.
	for _, ref := range rooted {
		if !scope.steps[ref.ID] {
			ds = append(ds, unresolvedStep(stepID, inputName, ref.ID, index, wf)...)

			continue
		}
		if d, wrong := unknownStepOutput(stepID, inputName, ref, wf); wrong {
			ds = append(ds, d)
		}
	}

	for _, ref := range bare {
		// A name bound here — a loop's iterator, `now` inside a wait — is exactly
		// what stays bare, and is not a step.
		if scope.locals[ref] {
			continue
		}
		if ref == v1.StepsRoot || ref == v1.VarsRoot {
			// A root written as an operand rather than selected through:
			// `size(steps)`, or `vars["region"]` where the key is computed. Both
			// resolve — the activation answers a root whole — so reporting either as
			// an unknown name would be a false diagnostic about a working file.
			//
			// `steps` was exempted when rooting landed and `vars` was not, which is
			// the shape a second root always takes: the rule was written where the
			// first one needed it rather than where the *category* does.
			continue
		}
		if functionNamespaces[ref] {
			// A namespaced function from the profile — `regex.replace(...)`,
			// `math.greatest(...)`. cel-go parses the qualifier as an identifier, so
			// it arrives here looking exactly like a name nobody bound, and every one
			// of them was reported as an unknown step in every expression position in
			// the language. A false diagnostic about a documented function, which is
			// how a tool teaches people to stop reading it.
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

		// Only two answers are left here, and that is worth saying because it used
		// to be four. A self-reference and a forward reference are both references
		// to a step that *is* declared, so both are caught above and told to run
		// `flow fix` — after which they resolve as rooted references and get the
		// right message from unresolvedStep. Keeping the two branches here would
		// have been code that cannot run, which is the kind that rots unnoticed.
		if ref == v1.NowIdentifier {
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
			continue
		}
		// Named rather than guessed at. Bare now means a local binding, so this is
		// not necessarily a step someone misspelled — and saying which two things a
		// bare name can be is the difference between a diagnostic an author can act
		// on and one that only says no.
		ds = append(ds, Diagnostic{
			Step: stepID, Field: inputName,
			Message: fmt.Sprintf(
				"references unknown name %q; a step is written `%s.%s`, and a bare name is a loop's "+
					"iterator, a name this step declares in its own `vars:`, or `now`",
				ref, v1.StepsRoot, ref),
		})
	}
	return ds
}

// unresolvedVar reports a reference to a var the workflow does not declare.
//
// The declared names are offered because they are few, known in full, and all in scope
// at once — none of which is true of steps, where a listing would be long and would
// include names not yet available at this point in the walk. Where one is close enough
// to be a typo, it is named first: "did you mean" is the shortest path from the
// diagnostic to the edit.
func unresolvedVar(stepID, inputName, ref string, scope refScope) Diagnostic {
	declared := slices.Sorted(maps.Keys(scope.vars))

	message := fmt.Sprintf("references unknown var %q", ref)
	switch {
	case len(declared) == 0:
		// The likeliest mistake by far when nothing is declared: `vars:` belongs at
		// the top of the file, and an author who has not written one yet is reaching
		// for a feature rather than misspelling a name.
		message += "; this workflow declares no `vars:`, which is a top-level block of " +
			"names and values that every step can read as `" + v1.VarsRoot + ".<name>`"
	default:
		if suggestion, ok := nearest(ref, declared); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			message += fmt.Sprintf("; this workflow declares %s", strings.Join(declared, ", "))
		}
	}

	return Diagnostic{Step: stepID, Field: inputName, Value: ref, Message: message}
}

// referencedIdentifiers returns the names an expression references, in three groups:
// steps reached under the `steps.` root, vars reached under the `vars.` root, and
// whatever is written bare.
//
// Three groups rather than one because each fails differently and so wants a different
// diagnostic. An unknown step may be a forward reference, a typo, or a step that exists
// outside this scope; an unknown var can only be a typo, since every var exists before
// the first step runs; a bare name may be a local, `now`, or a reference in the spelling
// this grammar retired. Merging them and sorting it out afterwards loses exactly the
// distinction the caller needs.
//
// Identifiers bound by a comprehension are excluded: in `items.map(x, x + 1)`
// the name `x` is introduced by the expression itself and is not a step
// reference. Reporting those would make every use of a comprehension look broken.
func referencedIdentifiers(parsed *expr.ParsedExpr) (rooted []stepRef, vars, bare []string) {
	roots := map[stepRef]struct{}{}
	varNames, free := map[string]struct{}{}, map[string]struct{}{}
	collectReferences(parsed.GetExpr(), map[string]struct{}{}, roots, varNames, free)

	return sortedStepRefs(roots), sortedNames(varNames), sortedNames(free)
}

// A stepRef is one reference to a step: which step, and which of its outputs.
//
// The output name used to be discarded at the point it was parsed, which is why
// `${steps.web.nonsense}` validated cleanly and then resolved to nothing at run time.
// Carrying it costs one field and is the difference between "that step exists" — which
// is all the tool could say — and "that step has no such output, it has these".
type stepRef struct {
	// ID is the step named under the root.
	ID string

	// Output is the name selected from it, or empty when the reference is to the
	// step's whole outputs mapping. Empty is legal: `${steps.web}` is the mapping,
	// which is a thing an expression may pass around.
	Output string
}

// sortedStepRefs orders references so one file reports the same diagnostics in the same
// sequence.
func sortedStepRefs(set map[stepRef]struct{}) []stepRef {
	refs := make([]stepRef, 0, len(set))
	for ref := range set {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b stepRef) int {
		if a.ID != b.ID {
			return strings.Compare(a.ID, b.ID)
		}

		return strings.Compare(a.Output, b.Output)
	})

	return refs
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
func collectReferences(e *expr.Expr, bound map[string]struct{}, rooted map[stepRef]struct{}, vars, free map[string]struct{}) {
	if e == nil {
		return
	}
	if sel := e.GetSelectExpr(); sel != nil {
		// Both roots are recognised here, and an unrecognised root falls through to
		// the walk below so that `foo.bar` still reports `foo` as a bare name. That
		// fall-through is what keeps adding a root from silently swallowing the
		// diagnostic for a name that has none.
		if root, name, under, ok := rootedName(sel, bound); ok {
			switch root {
			case v1.StepsRoot:
				rooted[stepRef{ID: name, Output: under}] = struct{}{}

				return
			case v1.VarsRoot:
				vars[name] = struct{}{}

				return
			}
		}
	}
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if _, isBound := bound[name]; !isBound {
			free[name] = struct{}{}
		}
	case *expr.Expr_SelectExpr:
		collectReferences(kind.SelectExpr.GetOperand(), bound, rooted, vars, free)
	case *expr.Expr_CallExpr:
		collectReferences(kind.CallExpr.GetTarget(), bound, rooted, vars, free)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectReferences(arg, bound, rooted, vars, free)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			collectReferences(el, bound, rooted, vars, free)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectReferences(entry.GetMapKey(), bound, rooted, vars, free)
			collectReferences(entry.GetValue(), bound, rooted, vars, free)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr

		// The range and the accumulator's start are evaluated outside the
		// comprehension's own scope.
		collectReferences(c.GetIterRange(), bound, rooted, vars, free)
		collectReferences(c.GetAccuInit(), bound, rooted, vars, free)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		collectReferences(c.GetLoopCondition(), inner, rooted, vars, free)
		collectReferences(c.GetLoopStep(), inner, rooted, vars, free)
		collectReferences(c.GetResult(), inner, rooted, vars, free)
	}
}

// rootedStepName reads the step a rooted reference names.
//
// The chain is walked to its base rather than matched at a fixed depth, because
// the depth is whatever the author selected: `steps.a` is one select over the
// root and `steps.a.result.code` is three.
func rootedStepName(sel *expr.Expr_Select, bound map[string]struct{}) (string, bool) {
	root, name, _, ok := rootedName(sel, bound)

	return name, ok && root == v1.StepsRoot
}

// rootedName returns the root a select chain hangs from and the first field under it.
//
// One walk for both roots the language has. `steps.a.result` and `vars.region.zone`
// have identical shapes and differ only in the word at the bottom, so deciding which
// root it is belongs to the caller — a second copy of this walk per root is how the two
// come to disagree about a shadowed root or a chain three deep.
//
// ok is false when the chain does not bottom out in a plain identifier, or when a
// comprehension bound the root's name: then it is a binding rather than the root, and
// whatever hangs off it is neither a step nor a var.
func rootedName(sel *expr.Expr_Select, bound map[string]struct{}) (root, name, under string, ok bool) {
	var fields []string
	node := sel
	for node != nil {
		fields = append(fields, node.GetField())
		operand := node.GetOperand()
		if ident := operand.GetIdentExpr(); ident != nil {
			root = ident.GetName()
			if _, shadowed := bound[root]; shadowed {
				return "", "", "", false
			}
			break
		}
		node = operand.GetSelectExpr()
	}
	if node == nil || len(fields) == 0 {
		return "", "", "", false
	}

	// Collected outermost first, so the name under the root is the last one reached
	// and what is selected *from* it is the one before that.
	//
	// Only one level down is returned, because only one level is the language's:
	// `steps.web.body` names an output and `steps.web.body.items` selects into that
	// output's value, which is CEL's business and not something this can check.
	name = fields[len(fields)-1]
	if len(fields) >= 2 {
		under = fields[len(fields)-2]
	}

	return root, name, under, true
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

// toleratedErrorOutput is the output a step gains by being allowed to fail.
//
// Written by both drivers in place of a failed task's own outputs when
// `continue_on_error:` is set, which is what a later step branches on. Named here
// because the validator has to know about it and does not otherwise: it is the one
// output that comes from the *policy* rather than from the task.
const toleratedErrorOutput = v1.StepErrorOutput

// unknownStepOutput reports a reference to an output a step does not produce.
//
// Silent unless it is sure, which is most of the time and deliberately so. It answers
// only for a step running a *task* whose outputs are declared as a message, which is
// where the set is known in full — every other shape produces names this cannot
// enumerate:
//
//   - `http` with an `outputs:` input replaces its declared outputs with names the
//     author chose, and the whole point of that input is that they are not fixed;
//   - a `for_each` reports `results`, a `parallel` merges its branches, and a
//     `wait_for_signal` carries whatever a sender sent — none of them a task, and each
//     wanting knowledge this does not have yet;
//   - a plugin may decline to describe its outputs at all.
//
// A false diagnostic is worse than a missing one, so every one of those is silence. The
// case this does cover is the one that is never right: a step whose task declares a
// fixed set, referenced by a name outside it — which `log`, declaring none, makes total
// rather than occasional.
func unknownStepOutput(stepID, inputName string, ref stepRef, wf *v1.Workflow) (Diagnostic, bool) {
	if ref.Output == "" {
		// The whole outputs mapping. Legal for any step, including one with nothing
		// in it.
		return Diagnostic{}, false
	}

	node := nodeWithID(ref.ID, wf)
	task := node.GetTask()
	if task == nil {
		return Diagnostic{}, false
	}

	// A tolerated step gains an output no task declares.
	//
	// When `continue_on_error:` is set and the step fails, both drivers synthesise
	// `error` in place of the task's own outputs — which is the whole point of the
	// policy: a later step branches on it. So the set of outputs is the descriptor's
	// *plus* that one, and checking the descriptor alone reports a documented pattern
	// as a mistake. Precisely the failure this check's own comment warns about, and it
	// shipped that way for one review cycle.
	if ref.Output == toleratedErrorOutput && node.GetPolicy().GetContinueOnError() {
		return Diagnostic{}, false
	}

	def, known := v1.LookupTask(task.GetName())
	if !known || def.Outputs == nil {
		return Diagnostic{}, false
	}

	// A task that names its own outputs answers for them. Checked by presence of the
	// input rather than by task name, so a plugin adopting the same shape inherits the
	// exemption rather than being reported against a set it replaced.
	if _, replaced := task.GetInputs()["outputs"]; replaced {
		return Diagnostic{}, false
	}

	if def.Outputs.Fields().ByName(protoreflect.Name(ref.Output)) != nil {
		return Diagnostic{}, false
	}

	produced := fieldNames(def.Outputs)
	if node.GetPolicy().GetContinueOnError() {
		// Listed because it is available here, and a list that omits a name the very
		// next edit might need sends the author to the docs for something the tool
		// already knew.
		produced = append(produced, toleratedErrorOutput)
	}

	message := fmt.Sprintf("step %q has no output %q", ref.ID, ref.Output)
	switch {
	case len(produced) == 0:
		// The whole point of `log`, and worth saying rather than listing an empty set:
		// an author reading "it produces: " learns nothing about why.
		message += fmt.Sprintf("; the %s task produces no outputs, because a %s step is an effect rather than a value",
			task.GetName(), task.GetName())

		// Except that a tolerated step always has one, whatever its task produces —
		// so the sentence above would be false where the policy is set, and this is
		// the one case where "produces nothing" needs a qualifier.
		if node.GetPolicy().GetContinueOnError() {
			message += fmt.Sprintf(" (it does produce %q, since it may be tolerated)", toleratedErrorOutput)
		}
	default:
		if suggestion, ok := nearest(ref.Output, produced); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			message += fmt.Sprintf("; it produces %s", strings.Join(produced, ", "))
		}
	}

	return Diagnostic{Step: stepID, Field: inputName, Value: ref.Output, Message: message}, true
}

// nodeWithID returns the node an id names, anywhere in the workflow, or nil.
func nodeWithID(id string, wf *v1.Workflow) *v1.Node {
	var walk func([]*v1.Node) *v1.Node
	walk = func(nodes []*v1.Node) *v1.Node {
		for _, node := range nodes {
			if node.GetId() == id {
				return node
			}
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				if found := walk(kind.ForEach.GetBody()); found != nil {
					return found
				}
			case *v1.Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					if found := walk(branch.GetSteps()); found != nil {
						return found
					}
				}
			}
		}

		return nil
	}

	return walk(wf.GetSteps())
}

// functionNamespaces are the qualifiers a profile's namespaced functions hang from.
//
// cel-go parses `regex.replace(s, a, b)` as a select over the identifier `regex`, so the
// qualifier reaches the reference walk looking exactly like a name nobody bound — and
// every use of one was reported as an unknown step, in every expression position the
// language has. The functions are documented, `flow tasks` prints them, and the
// validator refused them.
//
// # Derived from the declarations, not from the library names
//
// The first version of this read [v1.ExtensionLibraries], and those are *registration*
// names rather than qualifiers. They coincide often enough to look right — `regex`,
// `math`, `sets` — and then do not: `encoders` declares `base64.encode`, `protos`
// declares `proto.getExt`, `bindings` declares `cel.bind`. So `${base64.encode(b)}` was
// still refused, and `${string(encoders)}` — a name that means nothing anywhere — was
// quietly accepted. A set that is wrong in both directions at once is the mark of
// having asked the wrong thing.
//
// Asking the environment removes the guess entirely. A qualifier is exactly the part of
// a declared function's name before its last dot, which is a fact about the profile
// rather than a naming convention this file hopes holds.
var functionNamespaces = func() map[string]bool {
	out := map[string]bool{}

	env, err := v1.DefaultEvaluator().ProfileEnv(v1.CurrentProfile)
	if err != nil {
		// A profile this build cannot build an environment for is a defect rather
		// than something a workflow can cause, and it will be reported far more
		// clearly the moment anything evaluates. Answering with an empty set here
		// costs a false diagnostic; panicking would cost every command.
		return out
	}

	for name := range env.Functions() {
		at := strings.LastIndex(name, ".")
		if at <= 0 {
			// A bare function — `size`, `has` — hangs from nothing and is never
			// written as a qualifier.
			continue
		}
		out[name[:at]] = true
	}

	return out
}()

// Proto renders a diagnostic as the schema message every surface reads.
//
// The Go struct stays the working type inside this package — it is what the checks
// build and what [Diagnostic.Error] renders — and this is the projection of it that
// leaves. Constitution point 6, applied to a diagnostic: the compiled proto is the
// contract, and anything this package can say, another surface says by reading the
// same message.
//
// Positions are widened to uint32 unchanged, zero and all. A diagnostic with no
// position is a real answer, so it must not be turned into line 1.
func (d Diagnostic) Proto() *v1.Diagnostic {
	return &v1.Diagnostic{
		Line:    uint32(max(d.Line, 0)),
		Column:  uint32(max(d.Column, 0)),
		Message: d.Message,
		Step:    d.Step,
		Field:   d.Field,
		Kind:    d.Kind,
		Value:   d.Value,
	}
}

// Report renders a file's diagnostics as the schema message.
//
// A file with no diagnostics still produces a report, with an empty list. "Checked and
// clean" and "not checked" are different facts, and a consumer that only ever saw
// failures could not tell them apart.
func (ds Diagnostics) Report(file string) *v1.DiagnosticReport {
	report := &v1.DiagnosticReport{
		File:        file,
		Diagnostics: make([]*v1.Diagnostic, 0, len(ds)),
	}
	for _, d := range ds {
		report.Diagnostics = append(report.Diagnostics, d.Proto())
	}

	return report
}
