package flowfile

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/common/operators"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
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
	"total_timeout":     true,
	"retry":             true,
	"continue_on_error": true,
	"undo":              true,
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

	// Code names the diagnostic's class in a form stable across a reworded
	// Message — see [v1.DiagnosticCode]'s own doc for why the set is small and
	// how it is kept from drifting. Left unset at most call sites, which is not
	// an omission: [Proto] falls it back to [v1.DiagnosticCodeGeneral], so a
	// site only needs to set this when its diagnostic belongs to one of the
	// classes worth branching on.
	Code v1.DiagnosticCode

	// Edits are repairs a program may apply to the source, each complete and
	// independently applyable, and each an alternative to its siblings rather
	// than a step in a sequence.
	//
	// Held as the schema type rather than as a Go shape projected into it,
	// because unlike everything above it there is no second thing this package
	// does with an edit: nothing here applies one, [Diagnostic.Error] does not
	// render one, and the only consumers are the surfaces that read the schema
	// message. A parallel Go definition would be a second description of a
	// contract with one description's worth of use.
	//
	// Nil at almost every site, which is the honest answer rather than an
	// omission: see the schema's own doc on [v1.Diagnostic.Edits] for why a
	// checker that cannot name the exact replacement leaves this empty.
	Edits []*v1.SuggestedEdit
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

// Validate checks a compiled workflow and reports every problem it can find.
//
// A specification reaching this function did not necessarily pass through
// [Parse] — the server accepts one built by hand over the API too — so nothing
// here may assume a check [Parse] already makes was made.
func Validate(wf *v1.Workflow) Diagnostics {
	if wf == nil {
		return Diagnostics{{Message: "workflow is empty"}}
	}

	// Checked before anything below walks the tree, including into any call a
	// step makes: a hand-built specification may be diamond-shaped in exactly
	// the way [Parse] refuses to compile, via [maxCallExpansionNodes] enforced
	// as the tree is built — and nothing below this point may assume that
	// protection ran first.
	if !boundedCallExpansion(wf, new(int)) {
		return Diagnostics{{
			Message: fmt.Sprintf(
				"compiles to more than %d steps once every call is counted, which is more than "+
					"a Flowfile is meant to expand to; nothing further was checked", maxCallExpansionNodes),
		}}
	}

	return validateAtDepth(wf, wf.GetProfile(), 0, v1.UndoScopeTopLevel)
}

// validateAtDepth is the whole of what [Validate] checks for one workflow, at
// the call depth ([v1.CheckCallDepth]) it was reached at — zero for the
// top-level workflow a run submits, and one deeper for every call standing
// between here and it.
//
// placement is this workflow's own undo scope, already composed by the caller
// through [v1.UndoScope.IntoCall] — [v1.UndoScopeTopLevel] for the run's own
// top level, [v1.UndoScopeCall] for a callee reached through a call sitting at
// top level, inside another call, or inside a `loop:` body, and
// [v1.UndoScopeConcurrent] for a callee reached through a call that itself sits
// inside a `for_each` body or a `parallel` branch — a call is transparent to
// whatever restriction already applies there, not an escape from it. See
// [validateCallAtDepth], which does the composing.
func validateAtDepth(wf *v1.Workflow, profile string, depth int, placement v1.UndoScope) Diagnostics {
	var ds Diagnostics

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
		case isDeclarationRoot(id):
			// A root itself. Refused as an id, and this is the one collision
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
			//
			// Every root rather than only `steps`: the argument is about what a name
			// hides, not about which root was written first.
			ds = append(ds, Diagnostic{
				Step:    id,
				Message: "id " + shadowsRoot("step", id),
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

	ds = append(ds, validateDeclaredInputs(wf, profile)...)
	ds = append(ds, validateTriggers(wf)...)
	ds = append(ds, validateSignals(wf)...)
	ds = append(ds, validateDebug(wf)...)
	ds = append(ds, validateReservedSignalNames(wf)...)
	ds = append(ds, validateConcurrency(wf)...)
	ds = append(ds, validateWorkflowVars(wf)...)

	// What is wrong with an expression regardless of what the file means — a
	// function nobody declared, an operator with no overload for the types written
	// beside it. Separate from the reference checks below and deliberately unaware
	// of scope; see celcheck.go for why that is what keeps it from reporting the
	// same mistake twice in two voices.
	ds = append(ds, checkExpressionTypes(wf)...)

	// Two sibling steps whose `if:` conditions look like they were meant to be
	// exact negations of each other, but have drifted apart — see negation.go.
	// Recurses into every for_each/loop body and parallel branch on its own, so
	// one call here covers the whole tree the way checkExpressionTypes does.
	ds = append(ds, checkNegationDrift(wf.GetSteps())...)

	// A `log:` message that writes an input declared `sensitive:` into the log
	// in the clear — see sensitive_log.go. Recurses into every for_each/loop
	// body and parallel branch on its own, the same way checkNegationDrift does,
	// so one call here covers the whole tree.
	ds = append(ds, checkSensitiveLog(wf)...)

	// A `wait_for_signal:`'s `prompt:` that reaches an input declared
	// `sensitive:`, or holds a secret reference - see sensitive_prompt.go for
	// why the rule there is wider than the one above it. Recurses on its own,
	// the same way the two checks above do.
	ds = append(ds, checkSensitivePrompt(wf)...)

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

		// This workflow's own placement, passed in by the caller — the true top
		// level, or whatever a callee reached through a call composes to; see
		// [validateAtDepth]'s doc.
		ds = append(ds, validateUndo(id, node, inner, i, wf, placement)...)
		ds = append(ds, validateAsync(id, node, placement)...)

		// Against `scope` and not `inner`, and before the kind is known — see
		// [validateCondition] for both.
		ds = append(ds, validateCondition(id, node, scope, i, wf)...)

		if task == nil {
			// A step may be a loop or a parallel block rather than a task. Its
			// nested steps are validated with the enclosing scope visible, since
			// a body step may legitimately reference a step defined before the
			// block it sits in.
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, inner, i, wf, profile, depth)...)
				// A loop's body outputs do not escape it — only its own `results`
				// output does — so body step ids must not become referenceable.

			case *v1.Node_Loop:
				ds = append(ds, validateNamedLoop(id, kind.Loop, inner, i, wf, profile, depth, placement)...)
				// A loop's body outputs do not escape it either — only its own
				// `results` (and `state`) outputs do.

			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, inner, i, wf, profile, depth)...)
				// Branch outputs are merged into the enclosing scope once the
				// block completes, so a later step may reference them by id —
				// recordStepInScope below adds them.

			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, inner, i, wf)...)

			case *v1.Node_Call:
				ds = append(ds, validateCallAtDepth(id, kind.Call, inner, i, wf, profile, depth+1, placement)...)

			case *v1.Node_Value:
				ds = append(ds, validateValue(id, kind.Value, inner, i, wf)...)

			case *v1.Node_Switch:
				ds = append(ds, validateSwitch(id, kind.Switch, inner, i, wf, profile, depth, placement)...)
				// Exactly one body runs and its outputs merge into the enclosing
				// scope, the way a parallel branch's do, so a later step may
				// reference a case-body step by id — and simply not resolve at
				// run time when a different case took the value, the same honest
				// outcome referencing an `if:`-skipped step already has.

			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			recordStepInScope(scope, node)
			continue
		}

		ds = append(ds, validateTaskStep(id, node, task, scope, inner, i, wf)...)

		// Only after a step's inputs are checked do its outputs become
		// available, which is what makes a self- or forward-reference detectable.
		scope.steps[id] = node
	}

	// Last, against the scope the walk ends with, because that is when a run
	// evaluates them: every top-level step has finished, so a reference to the final
	// step is correct here and would be a forward reference anywhere else.
	ds = append(ds, validateDeclaredOutputs(wf, profile, scope, len(wf.GetSteps()))...)
	if err := v1.CheckWorkflowAtomicBlockActivities(wf); err != nil {
		ds = append(ds, Diagnostic{Message: err.Error()})
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
		rooted, vars, inputs, run, trigger, bare := referencedIdentifiers(parsed)

		for _, ref := range inputs {
			// A run's arguments *are* known before the first step, so this refusal is
			// about where the block is evaluated rather than about what is knowable.
			// The workflow's `vars:` are evaluated in an activity under the durable
			// driver — that is what keeps a suspended run from recomputing them
			// against a different cel-go — and what reaches that activity is the
			// declared block and the profile, not the run's arguments. Rather than
			// widening that payload for a convenience, a var stays what it has been:
			// literals, operators, and the profile's functions.
			//
			// Named as its own sentence because "unknown name" would be false. The
			// input exists, the reference is spelled correctly, and the answer is
			// where to write it instead.
			ds = append(ds, Diagnostic{
				Field: field, Value: ref,
				Message: fmt.Sprintf(
					"a var may not read an input: `%s:` is evaluated before the run's arguments are in "+
						"scope, so write `%s.%s` where the value is used: in a step's `if:`, its own "+
						"`vars:`, or a task input",
					v1.VarsRoot, v1.InputsRoot, ref),
			})
		}

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

		// `trigger` fails for the same reason `run` does below, and it is worth its
		// own loop for the same reason: the field may be perfectly spelled, and the
		// mistake is reading it from `vars:` at all. A var is evaluated once before
		// the first step, in an activity under the durable driver, against a scope
		// deliberately holding nothing about the run — see the sentence below.
		for range trigger {
			ds = append(ds, Diagnostic{
				Field: field, Value: v1.TriggerRoot,
				Message: fmt.Sprintf(
					"a var may not read `%s`: `%s:` is evaluated before the first step runs, "+
						"against a scope holding literals, operators and the profile's functions "+
						"and nothing else. Read it in a step's `if:`, its own `vars:`, or a task input",
					v1.TriggerRoot, v1.VarsRoot),
			})
		}

		// `run` fails the same way `inputs` does, for the same reason: both are
		// known only once the run's arguments are — after this block has already
		// been evaluated (see the loop over inputs above). Reported here rather
		// than falling through to a per-field check, because the field itself may
		// be perfectly spelled; the mistake is reading it from `vars:` at all.
		for range run {
			ds = append(ds, Diagnostic{
				Field: field, Value: v1.RunRoot,
				Message: fmt.Sprintf(
					"a var may not read `%s`: `%s:` is evaluated before the first step runs, "+
						"against a scope holding literals, operators and the profile's functions "+
						"and nothing else",
					v1.RunRoot, v1.VarsRoot),
			})
		}

		for _, ref := range bare {
			if isDeclarationRoot(ref) {
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

// unknownTaskMessage is the diagnostic text for a task name the registry does not
// know. It is shared by the primary task-name check and the `undo:` compensation
// check so the two cannot drift — a step's task and its compensation task are the
// same kind of thing and must fail the same way.
//
// A dot names a plugin — `slack.post` is the `post` task of the plugin discovered
// as `flowstate-plugin-slack` — so the honest diagnosis is about installation, not
// spelling. "Unknown task" would send the author to check a name that may be exactly
// right, in a file that will run unchanged on a worker that has the plugin; and this
// validator, which launches nothing, genuinely cannot tell that case from a typo.
// Saying what it cannot know is the diagnostic here.
func unknownTaskMessage(name string) string {
	if plugin, _, dotted := strings.Cut(name, "."); dotted {
		return fmt.Sprintf("no plugin task %q is registered here; if the %q plugin "+
			"is installed on the worker this will run on, the file is fine and this "+
			"process simply has not loaded it; `flow plugins` shows what a plugin "+
			"directory provides", name, plugin)
	}
	// A near miss is named rather than the registry enumerated, which is the rule
	// every other misspelling diagnostic in this package already follows
	// (`unknown key "withh"; did you mean "with"?`) and the one this family was
	// missing. The enumeration is honest but does not scale: it is the whole task
	// registry, and a worker with plugins loaded multiplies that list, so the
	// answer to a one-keystroke typo grows without bound while the useful part of
	// it stays one word. `flow task run` reached the same conclusion for a name
	// typed at a shell (see [unknownTaskRunError] there), and this is that rule
	// arriving at the other surface that reads a task name.
	known := v1.TaskNames()
	if suggestion, ok := nearest.Name(name, known); ok {
		return fmt.Sprintf("unknown task %q; did you mean %q?", name, suggestion)
	}

	// Nothing close enough to name, so the list is the only help there is, with a
	// pointer at the command that describes each one, because for a long list
	// reading the names is the start of the question rather than the end.
	return fmt.Sprintf("unknown task %q; available tasks are %s (`flow tasks` describes each one)",
		name, strings.Join(known, ", "))
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
		ds = append(ds, Diagnostic{
			Step: id,
			// Under the flattening the task's name is a key an author wrote, so
			// there is a token to underline rather than a whole step.
			Kind:    task.GetName(),
			Message: unknownTaskMessage(task.GetName()),
			Code:    v1.DiagnosticCodeUnknownTask,
		})
	}

	// The step's `if:` is not checked here. It is a property of the *node* and not
	// of the task — every kind of step may carry one — so it is checked once, by
	// [validateCondition], from the walk that visits every kind (#869).
	//
	// What the task declares its inputs to be is checked separately from what they
	// reference, because the two fail differently: a reference that cannot resolve is
	// a mistake about the workflow, and an input the task does not have is a mistake
	// about the task.
	ds = append(ds, validateTaskInputs(id, task)...)

	// A literal input is type-checked against the field by validateTaskInputs; an
	// input written as a direct reference to a name this file types — `${inputs.x}`,
	// `${vars.x}` — is checked here, where the workflow is in hand to read the
	// declaration from (#158). A computed expression stays unchecked, deliberately.
	ds = append(ds, checkExpressionInputTypes(id, task, wf)...)

	// Some inputs are evaluated by the task itself, in a scope this validator does
	// not model — the http task's `outputs` expression references the response, not
	// earlier steps. Checking references in those would report every correct use as
	// an unknown step, and a false diagnostic is worse than a missing one: it trains
	// authors to ignore the tool. The registry declares which inputs those are.
	checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
	for _, name := range sortedInputNames(checkable) {
		ds = append(ds, validateInputRefs(id, name, checkable[name], inner, index, wf)...)
	}

	return ds
}

// validateCondition reports references in one step's `if:` that cannot resolve,
// whatever kind of step it is.
//
// It lives here, on the node, rather than on any one kind, because `if:` is a field
// of [v1.Node] and every kind may carry one — task, for_each, loop, parallel, wait,
// call, value and switch. Until #869 it was checked on the task path only, so the
// identical typo was a positioned diagnostic on a task step and silence on a
// `wait_for_signal:` or a `sleep:`; a condition is an expression like any other and
// resolves against the same names, so it is checked the same way.
//
// Two things about the scope, both taken from where the engine evaluates the
// expression ([v1.EvalConditionInScope], called from `runNodes` in eval.go and from
// the durable driver's execute.go) rather than from where it is written:
//
//   - The step's own `vars:` are *not* in scope. The condition decides whether the
//     step runs at all, and both drivers evaluate it before binding the node's vars,
//     so a var this step declares does not exist yet when the question is asked. This
//     is why the walk passes `scope` and not `inner`.
//   - `now` is *not* in scope, even on a wait. The engine binds it inside the wait's
//     own expressions ([v1.NowIdentifier], bound by evalWaitExpr) and the condition is
//     evaluated a level above that, in the same place a task step's condition is — so
//     `${now}` in a wait's `if:` is a run-time failure, and saying so here is the
//     whole point. [validateWait] adds it for the fields that do have a clock.
//
// A name an enclosing binding supplies — a loop's `as:` or the `item` it binds when
// it writes no `as:` — stays legal, because the scope handed in already carries it:
// the body's conditions are evaluated inside the body, where the engine has bound it.
func validateCondition(id string, node *v1.Node, scope refScope, index int, wf *v1.Workflow) Diagnostics {
	return validateInputRefs(id, "if", node.GetCondition(), scope, index, wf)
}

// branchStepNodes returns every step across a parallel block's branches,
// including those nested inside branch control flow whose outputs also merge out.
func branchStepNodes(parallel *v1.Parallel) []*v1.Node {
	var nodes []*v1.Node
	for _, branch := range parallel.GetBranches() {
		nodes = append(nodes, mergedStepNodes(branch.GetSteps())...)
	}
	return nodes
}

// mergedStepNodes returns the steps whose outputs become visible to steps
// following a list of nodes.
//
// A loop contributes only itself, because its body's outputs are reported
// through its `results` output rather than merged. A nested parallel block
// contributes its branches' steps, because those are merged.
//
// The nodes rather than their ids, because a scope that holds only names cannot
// answer what a name's outputs *are*: the check on `steps.<id>.<output>` used to
// find its node by searching the whole workflow for that id, which is the wrong
// step as soon as two blocks legally reuse one (#323). Recording what was put in
// scope, at the point it is put there, is what makes the later lookup exact.
func mergedStepNodes(nodes []*v1.Node) []*v1.Node {
	var out []*v1.Node
	for _, node := range nodes {
		out = append(out, node)
		if p, ok := node.GetKind().(*v1.Node_Parallel); ok {
			out = append(out, branchStepNodes(p.Parallel)...)
		}
		if s, ok := node.GetKind().(*v1.Node_Switch); ok {
			out = append(out, switchStepNodes(s.Switch)...)
		}
	}
	return out
}

// recordStepInScope marks a finished step in the scope the steps after it are
// checked against: its own id, plus — for a parallel block or a switch — the
// nested ids whose outputs execution merges out, which is exactly
// [mergedStepIDs] of this one node.
//
// One helper for both walks, because they briefly disagreed: the top-level walk
// merged a switch's case-body ids and [validateNested] recorded only the
// switch's own id, so the same later-sibling reference to a case-body step was
// legal at the top level and refused inside a `for_each` body — a scope rule
// with two spellings is two rules. Whatever a step contributes to the names
// after it is decided here, once.
func recordStepInScope(scope refScope, node *v1.Node) {
	for _, merged := range mergedStepNodes([]*v1.Node{node}) {
		scope.steps[merged.GetId()] = merged
	}
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
	// steps are the steps whose outputs exist at this point, reachable as
	// `steps.<id>`, keyed by the id a reference spells.
	//
	// The node and not merely the name, because an id is unique within a
	// *visibility domain* and not within a file — two sibling `loop:` blocks may
	// each declare a body step called `page`, legally, since body outputs do not
	// escape. A set of names forces the check on `steps.<id>.<output>` to go
	// looking for the node again, and the only lookup available to it was the
	// first match in the whole workflow: from the second loop that is the *first*
	// loop's step, whose outputs are a different set, so a legal file was reported
	// as referencing an output that does not exist (#323). What is in scope is
	// known exactly where the scope is built, so it is recorded there.
	steps map[string]*v1.Node

	// locals are the names bound bare here: a loop's iterator, and `now` inside a
	// wait expression. They are not steps and never were.
	locals map[string]bool

	// inputs are the run's declared inputs, reachable as `inputs.<name>`.
	//
	// Fixed for the whole file like vars and for a stronger reason: a declaration is
	// a promise about what a *caller* passes, so it neither grows as the walk
	// proceeds nor depends on where in the file the reference is written.
	inputs map[string]bool

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

	inputs := make(map[string]bool, len(wf.GetDeclaredInputs()))
	for _, declaration := range wf.GetDeclaredInputs() {
		inputs[declaration.GetName()] = true
	}

	return refScope{steps: map[string]*v1.Node{}, locals: map[string]bool{}, vars: vars, inputs: inputs}
}

// clone returns a copy that can be extended without disturbing the original,
// which is what lets a loop body see its enclosing scope plus its own iterator
// while the steps after the loop see neither.
func (s refScope) clone() refScope {
	out := refScope{
		steps:  make(map[string]*v1.Node, len(s.steps)+1),
		locals: make(map[string]bool, len(s.locals)+1),
		// Shared rather than copied: nothing extends the var set after the scope is
		// built, because workflow vars all exist before the first step runs. The same
		// is true of the declared inputs, more so — nothing in the file can add one.
		vars:   s.vars,
		inputs: s.inputs,
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
func validateLoop(stepID string, loop *v1.ForEach, enclosing refScope, index int, wf *v1.Workflow, profile string, depth int) Diagnostics {
	var ds Diagnostics

	if loop.GetItems() == nil {
		ds = append(ds, Diagnostic{Step: stepID, Field: "for_each", Message: "items is required"})
	}
	// A literal list longer than the trip-count ceiling, refused here where the
	// author can see it rather than at the first run. This is the static half of
	// [v1.MaxForEachItems] and deliberately the smaller half: `items:` is
	// usually an expression, whose length is a property of the run and not of
	// the file, so the load-bearing check is the runtime one both drivers apply.
	// What is checked here is only what the file itself decides, which is the
	// rule that separates a diagnostic this validator may report from one a
	// deployment answers.
	if list := loop.GetItems().GetLiteral().GetListValue(); list != nil && len(list.GetValues()) > v1.MaxForEachItems {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "items",
			Message: fmt.Sprintf(
				"items is a list of %d entries, over the ceiling of %d items a single for_each may iterate; iterate over fewer items, or page the work across several runs",
				len(list.GetValues()), v1.MaxForEachItems),
		})
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
	if isDeclarationRoot(iterator) {
		// A root, by the other route into a body's scope. A bound name wins over
		// the scope it is bound into, so an iterator spelled `steps` hides every
		// step from the body — and the body is exactly where rooted references are
		// written.
		//
		// Every root rather than the one this rule was written for: an iterator
		// called `vars` or `inputs` hides its namespace just as completely, and the
		// body is where those are read too.
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as",
			Message: shadowsRoot("loop variable", iterator),
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
				"%q is the built-in naming the moment a wait is evaluated, which a loop variable of the same name would shadow inside a wait's own expressions (`sleep:`, `wait_until:`, a signal's `timeout:`); choose another iterator",
				iterator),
		})
	}

	// The iterator is bound as a *local*, not merged in beside the step ids. That
	// one line is the whole of what rooting deletes: with the two apart, an
	// iterator sharing a step's name is no longer ambiguous, so the rule that used
	// to forbid it has nothing left to prevent.
	return append(ds, validateNested(loop.GetBody(), enclosing.withLocal(iterator), index, wf, profile, depth, v1.UndoScopeConcurrent)...)
}

// validateNamedLoop checks a `loop:` node: its required body and stop condition,
// the consistency of its carried-state triple, the state name against everything a
// bare binding may not shadow, and the references in its own expressions.
//
// The body is checked with the enclosing steps visible plus the carried state's
// name — a body step may reference a step defined before the loop, and reads the
// state under its bare name, exactly as a `for_each` body reads its iterator.
//
// placement is the scope this loop step itself sits in, composed for the body
// through [v1.UndoScope.IntoLoop] rather than assumed to be [v1.UndoScopeLoop].
// A loop body accepts an `undo:` since #253, and a `loop:` may be written inside
// a `for_each` body — so a body that claimed [v1.UndoScopeLoop] unconditionally
// would validate a compensation the engine refuses, which is invariant 3's exact
// shape pointed at the validator.
func validateNamedLoop(stepID string, loop *v1.Loop, enclosing refScope, index int, wf *v1.Workflow, profile string, depth int, placement v1.UndoScope) Diagnostics {
	var ds Diagnostics

	if len(loop.GetBody()) == 0 {
		ds = append(ds, Diagnostic{Step: stepID, Field: "loop", Message: "steps is required: a loop needs a body to run each iteration"})
	}
	if loop.GetUntil() == nil {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "until",
			Message: "until is required: a loop with no condition on when to stop never stops",
		})
	}

	// The carried-state triple stands or falls together: a name with no initial value
	// has nothing to bind, an initial value with no name is unreachable, and a state
	// that never updates is a constant dressed as a loop variable. Each is a mistake
	// worth naming rather than a shape to run — the same reason the schema refuses a
	// required input carrying a default.
	state := loop.GetState()
	hasState := state != ""
	hasInit := loop.GetInitial() != nil
	hasUpdate := loop.GetUpdate() != nil

	switch {
	case !hasState && (hasInit || hasUpdate):
		which := "init"
		if hasUpdate {
			which = "update"
		}
		ds = append(ds, Diagnostic{
			Step: stepID, Field: which,
			Message: "`" + which + ":` sets the loop's carried state, but the loop names none; add `as:` to name the value the body carries, or remove this",
		})
	case hasState && !hasInit:
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: fmt.Sprintf("loop carries state `%s` but has no `init:` to say what it holds on the first iteration", state),
		})
	case hasState && !hasUpdate:
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: fmt.Sprintf("loop carries state `%s` but has no `update:` to compute the next iteration's value; a state that never changes is a workflow var, not a loop variable", state),
		})
	}

	if hasState {
		ds = append(ds, validateLoopStateName(stepID, state, enclosing)...)
	}

	// Reference checks for the loop's own expressions, each against the scope the
	// engine evaluates it in. `init:` runs before the loop, so it sees the enclosing
	// scope and not the state it is defining. `until:` and `update:` run after the
	// body each iteration, so they see the carried state under its bare name *and*
	// the body's own top-level step outputs — which is what makes `${!steps.page.truncated}`
	// resolve.
	if loop.GetInitial() != nil {
		ds = append(ds, validateInputRefs(stepID, "init", loop.GetInitial(), enclosing, index, wf)...)
	}

	afterBody := enclosing.clone()
	if hasState {
		afterBody = afterBody.withLocal(state)
	}
	for _, node := range loop.GetBody() {
		if id := node.GetId(); id != "" {
			afterBody.steps[id] = node
		}
	}
	if loop.GetUntil() != nil {
		ds = append(ds, validateInputRefs(stepID, "until", loop.GetUntil(), afterBody, index, wf)...)
	}
	if loop.GetUpdate() != nil {
		ds = append(ds, validateInputRefs(stepID, "update", loop.GetUpdate(), afterBody, index, wf)...)
	}

	// A loop inside a loop is refused rather than accepted untested. The engine does
	// not suspend below the top of a loop body (a nested construct runs at a deeper
	// suspend level), so an inner loop would run atomically inside each outer
	// iteration — a shape whose Continue-As-New interaction across two carried-state
	// frames docs/DSL.md defers and nothing exercises. Accepting it would give an
	// author de-facto semantics the project will not stand behind; refusing is
	// additive to lift once that slice lands.
	//
	// The walk includes callees as well as same-scope containers. Although a callee
	// has its own frame, it runs atomically at the outer loop's deeper suspend level.
	// Allowing a loop there would multiply the two iteration ceilings without giving
	// the durable driver an opportunity to Continue-As-New between inner iterations.
	if nested, through := bodyHasNestedLoop(loop.GetBody()); nested {
		message := "a loop inside a loop is not supported in this edition: the Continue-As-New " +
			"interaction across two carried-state frames is not exercised yet; flatten the two into one"
		if through != "" {
			// The inner loop is in a file this author may not have written, and a
			// position there would name a line they cannot see from here. So the
			// diagnostic stays on the call site — a place in *this* file — and names
			// the callee it descended into, which is the pair an author needs to act:
			// which step of theirs reaches it, and which file to look in.
			message += fmt.Sprintf("; the inner loop is inside %s, which cannot hold one while it is "+
				"called from a loop body", through)
		}
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "loop",
			Message: message,
			Code:    v1.DiagnosticCodePlacementRefusal,
		})
	}

	// The state is bound as a *local*, the same standing a `for_each` iterator gets,
	// so the body reads it bare and a body step of the same id is unambiguous.
	bodyScope := enclosing
	if hasState {
		bodyScope = enclosing.withLocal(state)
	}
	return append(ds, validateNested(loop.GetBody(), bodyScope, index, wf, profile, depth, placement.IntoLoop())...)
}

// bodyHasNestedLoop reports whether a loop body directly or transitively contains
// another `loop:`, including through a call boundary. When the loop was reached
// through one or more `call:` steps, the second result describes the outermost
// call crossed — the call site in *this* file and the callee it names — for the
// diagnostic to quote; it is empty when the inner loop is in this file.
//
// A `call:` is transparent here for the reason docs/DSL.md gives: the callee's
// specification is resolved at compile time and carried whole, so what is walked
// is the specification that will actually run. Isolation is a runtime scoping
// property, not an analysis boundary.
//
// This recursion carries no depth bound of its own, unlike [v1.CheckPolicyPlacement],
// which walks a hand-built Workflow arriving over the RPC path and must therefore
// bound both its depth ([v1.CheckCallDepth]) and its node count. The difference is
// where the tree came from: this walk only ever runs over a Workflow that
// [Unmarshal] has already built, and the call expansion that produced these
// embedded callees is itself bounded — by depth at expansion and by
// `maxCallExpansionNodes` in total — so the tree is finite and shallow before this
// function sees it. The bound is real but inherited; if this walk ever gains a
// caller that did not come through [Unmarshal], it needs its own.
func bodyHasNestedLoop(nodes []*v1.Node) (bool, string) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Loop:
			return true, ""
		case *v1.Node_ForEach:
			if nested, through := bodyHasNestedLoop(kind.ForEach.GetBody()); nested {
				return true, through
			}
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				if nested, through := bodyHasNestedLoop(branch.GetSteps()); nested {
					return true, through
				}
			}
		case *v1.Node_Switch:
			// A switch body shares its enclosing suspend scope — a `loop:` in a
			// case body inside a loop body is the same unexercised nesting.
			for _, body := range v1.SwitchBodies(kind.Switch) {
				if nested, through := bodyHasNestedLoop(body); nested {
					return true, through
				}
			}
		case *v1.Node_Call:
			if nested, _ := bodyHasNestedLoop(kind.Call.GetWorkflow().GetSteps()); nested {
				// The outermost call crossed is the one an author can act on: it is
				// the step in the file they are editing. A deeper call's site is in
				// the callee, so it is dropped in favour of this one.
				return true, describeCallSite(node, kind.Call)
			}
		}
	}
	return false, ""
}

// describeCallSite names a `call:` step and the callee it names, for a diagnostic
// reporting something found on the other side of it.
func describeCallSite(node *v1.Node, call *v1.Call) string {
	if source := call.GetSource(); source != "" {
		return fmt.Sprintf("%q, called by step %q", source, node.GetId())
	}
	return fmt.Sprintf("the workflow called by step %q", node.GetId())
}

// validateLoopStateName refuses a loop's carried-state name that could not be read
// as a bare binding, or that would silently shadow something a bare name may not.
//
// The same refusals a `for_each` iterator and a step's own `vars:` key get, and for
// the same reasons — the state is bound bare, so it shares a namespace with every
// other bare binding and with the clock, and a bare name may mean one thing at a
// time.
func validateLoopStateName(stepID, state string, enclosing refScope) Diagnostics {
	var ds Diagnostics

	if !isCELIdentifier(state) {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: fmt.Sprintf("%q is not a valid identifier, so the body could not read it as `${%s}`", state, state),
		})
	}
	if slices.Contains(celReservedIdentifiers, state) {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: fmt.Sprintf("%q is a CEL reserved word, so ${%s} cannot be parsed", state, state),
		})
	}
	if isDeclarationRoot(state) {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: shadowsRoot("loop state", state),
		})
	}
	if state == v1.NowIdentifier {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: fmt.Sprintf(
				"%q is the built-in naming the moment a wait is evaluated, which a loop's carried state of the same name would shadow inside a wait's own expressions (`sleep:`, `wait_until:`, a signal's `timeout:`); choose another name",
				state),
		})
	}
	if enclosing.locals[state] {
		ds = append(ds, Diagnostic{
			Step: stepID, Field: "as", Value: state,
			Message: fmt.Sprintf(
				"`%s` is already bound here by an enclosing loop or step, and a bare name may mean one thing at a time; rename this one, or read the outer value under a different name",
				state),
		})
	}

	return ds
}

// validateParallel checks a parallel node and its branches.
func validateParallel(stepID string, parallel *v1.Parallel, enclosing refScope, index int, wf *v1.Workflow, profile string, depth int) Diagnostics {
	var ds Diagnostics

	if len(parallel.GetBranches()) == 0 {
		ds = append(ds, Diagnostic{Step: stepID, Field: "parallel", Message: "at least one branch is required"})
	}

	// Branch outputs merge into one namespace after the block, so ids must not
	// collide across branches — and a branch must not reference a sibling, since
	// branches are unordered.
	seen := make(map[string]bool, len(enclosing.steps))
	for id := range enclosing.steps {
		seen[id] = true
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
		ds = append(ds, validateNested(branch.GetSteps(), enclosing, index, wf, profile, depth, v1.UndoScopeConcurrent)...)
		for _, node := range branch.GetSteps() {
			seen[node.GetId()] = true
		}
	}
	return ds
}

// validateNested checks a nested list of steps against the names visible to it.
//
// placement is the [v1.UndoScope] every step in nodes is checked at — a
// `for_each` body and a `parallel` branch pass [v1.UndoScopeConcurrent], a
// `loop:` body passes whatever [v1.UndoScope.IntoLoop] composes from the scope
// the loop step itself sits in. A callee reached through a `call:` does not come
// through here at all; see [validateCallAtDepth].
func validateNested(nodes []*v1.Node, enclosing refScope, index int, wf *v1.Workflow, profile string, depth int, placement v1.UndoScope) Diagnostics {
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
		if isDeclarationRoot(id) {
			// The same refusal a top-level id gets, for the same reason. A nested
			// step's outputs are named through the root too, so one called `steps`
			// hides them from everything after it in the body.
			ds = append(ds, Diagnostic{
				Step:    id,
				Message: "id " + shadowsRoot("step", id),
			})
		}

		if _, shadowed := enclosing.steps[id]; id != "" && shadowed {
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

		// Reported here, where there is a position to put it on, rather than left to
		// the engine's own refusal at run time — see [v1.CheckUndoPlacement] for
		// which placements are allowed.
		ds = append(ds, validateUndo(id, node, inner, index, wf, placement)...)
		ds = append(ds, validateAsync(id, node, placement)...)

		// The same check the top-level walk makes, in the same place and against
		// the same scope: a nested step's `if:` is one expression evaluated by one
		// [v1.EvalConditionInScope], wherever the step is written.
		ds = append(ds, validateCondition(id, node, scope, index, wf)...)

		task := node.GetTask()
		if task == nil {
			switch kind := node.GetKind().(type) {
			case *v1.Node_ForEach:
				ds = append(ds, validateLoop(id, kind.ForEach, inner, index, wf, profile, depth)...)
			case *v1.Node_Loop:
				ds = append(ds, validateNamedLoop(id, kind.Loop, inner, index, wf, profile, depth, placement)...)
			case *v1.Node_Parallel:
				ds = append(ds, validateParallel(id, kind.Parallel, inner, index, wf, profile, depth)...)
			case *v1.Node_Wait:
				ds = append(ds, validateWait(id, kind.Wait, inner, index, wf)...)
			case *v1.Node_Call:
				ds = append(ds, validateCallAtDepth(id, kind.Call, inner, index, wf, profile, depth+1, placement)...)
			case *v1.Node_Value:
				ds = append(ds, validateValue(id, kind.Value, inner, index, wf)...)
			case *v1.Node_Switch:
				ds = append(ds, validateSwitch(id, kind.Switch, inner, index, wf, profile, depth, placement)...)
			default:
				ds = append(ds, Diagnostic{
					Step:    id,
					Message: "step must have one of " + stepKindList(),
				})
			}
			// The same merge the top-level walk performs: a parallel block's
			// branch steps and a switch's case-body steps are referenceable by
			// the siblings after it, at any nesting.
			recordStepInScope(scope, node)
			continue
		}

		ds = append(ds, validateTaskStep(id, node, task, scope, inner, index, wf)...)

		scope.steps[id] = node
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

		case isDeclarationRoot(name):
			// A bare binding wins over every root, so a step var of a root's name
			// hides that namespace for the whole step — its task's inputs, a loop's
			// items, and everything nested inside it. Refused rather than resolved,
			// which is the rule this block already applies to a name an enclosing
			// scope bound.
			ds = append(ds, Diagnostic{
				Step: id, Field: "vars." + name, Value: name,
				Message: shadowsRoot("var", name),
			})

		case name == v1.NowIdentifier:
			ds = append(ds, Diagnostic{
				Step: id, Field: "vars." + name, Value: name,
				Message: "`" + v1.NowIdentifier + "` is the moment a wait is evaluated, so a " +
					"var of that name would shadow it wherever both are in scope; rename this one",
			})

		default:
			next.locals[name] = true
		}
	}

	return next, ds
}

// validateUndo checks a step's compensation: where it may be written, what it may
// name, and whether its expressions resolve.
//
// # Where
//
// [v1.CheckUndoPlacement] owns the placement rules — allowed at the top level and
// inside a `call:`, refused inside a `for_each` body, a `parallel` branch, or a
// `loop:` body — because both drivers enforce them too and a rule spelled once
// cannot disagree with itself. What this adds is a position: an author meets the
// refusal in their editor, on the `undo:` key, rather than as a run that fails on
// its first step.
//
// # What its expressions may name
//
// Everything the step itself could name, plus the step's own outputs. That last
// part is the interesting one, because a step referencing itself is a forward
// reference everywhere else in the language and is refused as one. Inside its own
// `undo:` it is the ordinary case: `${steps.provision.id}` is how a compensation
// says which thing to delete, and by the time a compensation runs the step it
// belongs to has finished. The engine agrees — [v1.UndoRegistrationFor] resolves
// against the scope with the step's outputs added — so this is the validator
// modelling what the engine does rather than a special case invented here.
//
// Inputs the task evaluates for itself are skipped, exactly as they are for a
// step's own inputs and for the same reason: their scope is the response, which
// this validator cannot see, and reporting them would train authors to ignore the
// tool.
// validateAsync reports an `async:` the engine will not honour, on the key that
// carries it.
//
// # Where the rule lives
//
// [v1.CheckAsyncPlacement], which both drivers call before a step runs. One rule
// spelled once cannot disagree with itself, and this is the same arrangement
// `undo:` has for the same reason. What this adds is the position: an author
// meets the refusal in their editor, on their own `async:` key, rather than as a
// run that fails on its first step.
//
// # What is deliberately not reported here
//
// Nothing about whether the concurrency is *worth* it. A step marked async that
// the very next step joins is legal, costs nothing, and may well be a file in
// the middle of being written; a diagnostic there would be the validator having
// an opinion about the shape of a workload rather than about the file being
// wrong. The same rule keeps the width bound out: [v1.CheckAsyncWidth] counts
// what one scope has outstanding at one moment, which depends on which steps an
// `if:` skips, and a validator that guessed would report a file wrong on the
// strength of data it cannot see.
func validateAsync(id string, node *v1.Node, placement v1.UndoScope) Diagnostics {
	err := v1.CheckAsyncPlacement(node, placement)
	if err == nil {
		return nil
	}

	return Diagnostics{{
		Step: id, Field: "async", Message: err.Error(),
		Code: v1.DiagnosticCodePlacementRefusal,
	}}
}

func validateUndo(id string, node *v1.Node, scope refScope, index int, wf *v1.Workflow, placement v1.UndoScope) Diagnostics {
	undo := node.GetUndo()
	if undo == nil {
		return nil
	}

	var ds Diagnostics

	// Kind is set alongside Field on every diagnostic below, wherever it names
	// the `undo:` key itself, so [validateParsed] positions it through
	// [Positions.LocateKind] rather than the candidate search
	// [Positions.Locate] does. The step's own primary task may declare an
	// input literally named `undo` — a plugin task's input names come from
	// its own descriptor, so `undo` is not reserved — and Locate's candidate
	// search tries every registered task's `.undo` input before the step's
	// own `<step>.undo`, which would misplace these on that unrelated input.
	if err := v1.CheckUndoPlacement(node, placement); err != nil {
		return append(ds, Diagnostic{
			Step: id, Field: "undo", Kind: "undo", Message: err.Error(),
			Code: v1.DiagnosticCodePlacementRefusal,
		})
	}

	task := undo.GetTask()
	if task.GetName() == "" {
		return append(ds, Diagnostic{Step: id, Field: "undo", Kind: "undo", Message: "compensation has no task"})
	}
	if _, known := v1.LookupTask(task.GetName()); !known {
		return append(ds, Diagnostic{
			Step:    id,
			Field:   "undo",
			Kind:    "undo",
			Message: unknownTaskMessage(task.GetName()),
			Code:    v1.DiagnosticCodeUnknownTask,
		})
	}

	ds = append(ds, validateUndoInputs(id, task)...)

	// The step's own outputs, added to a copy: this scope is for the compensation
	// and is thrown away, so nothing after the step can reference itself by having
	// been undone.
	self := scope.clone()
	self.steps[id] = node

	checkable, _ := v1.ResolvableInputs(task.GetName(), task.GetInputs())
	for _, name := range sortedInputNames(checkable) {
		ds = append(ds, validateInputRefs(id, "undo", checkable[name], self, index, wf)...)
	}

	return ds
}

// validateUndoInputs reports what is wrong with a compensation's inputs, refiled
// against the step's `undo:` key.
//
// The check itself is [validateTaskInputs], unchanged: a compensation runs the
// same tasks a step runs, so an input a task does not have is the same mistake in
// both positions and deserves the same sentence. What differs is where the
// diagnostic lands. A `Field` naming the input would be looked up as an input of
// the *step's own* task and resolve to nothing or, worse, to a same-named input of
// it — so the field is the `undo:` key, and the input's name moves into the
// message where it is unambiguous.
func validateUndoInputs(id string, task *v1.Task) Diagnostics {
	inner := validateTaskInputs(id, task)

	ds := make(Diagnostics, 0, len(inner))
	for _, d := range inner {
		message := d.Message
		if d.Field != "" {
			message = fmt.Sprintf("input %q: %s", d.Field, message)
		}
		ds = append(ds, Diagnostic{Step: d.Step, Field: "undo", Kind: "undo", Value: d.Value, Message: message, Code: d.Code})
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

	rooted, vars, inputs, run, trigger, bare := referencedIdentifiers(parsed)

	// `trigger`'s shape is statically known and closed — four strings — for the
	// reason `run`'s is, and with one extra consequence: the closure is what keeps
	// a payload field from becoming reachable here. See [unknownTriggerField].
	for _, ref := range trigger {
		if d, wrong := unknownTriggerField(stepID, inputName, ref); wrong {
			ds = append(ds, d)
		}
	}

	// `trigger.kind` is a field this walk already resolves, above — the check
	// there is that the *name* is one of the four the engine renders. It says
	// nothing about a comparison's other side: `${trigger.kind == "schedual"}`
	// names a real field and is therefore invisible to that loop, compiles,
	// evaluates false on both drivers forever, and silently takes whichever
	// branch the author did not intend. The kinds are as closed as the fields
	// are ([v1.KnownTriggerKind]), so this is checked the same way and with the
	// same one-sided caution: only a string literal on the other side of `==`
	// or `!=` is judged, never a variable, an input, or another field, because
	// those are values this validator cannot know.
	for _, literal := range unknownTriggerKindLiterals(parsed.GetExpr(), map[string]struct{}{}) {
		ds = append(ds, unknownTriggerKindLiteral(stepID, inputName, literal))
	}

	// An input can only fail one way — nothing declared it — for the reason a var
	// can: every declaration exists before the run starts, so there is no forward
	// reference to distinguish and no scope to explain.
	for _, ref := range inputs {
		if scope.inputs[ref] {
			continue
		}
		ds = append(ds, unresolvedInput(stepID, inputName, ref, scope))
	}

	// `run`'s shape is statically known — `identity{subject,issuer,namespace,
	// claims}` and `local` — unlike `steps` and `inputs`, where the set of legal
	// names depends on the file. That is what makes an unknown field under it
	// diagnosable here rather than only at run time, where it surfaces as an
	// unresolved reference three steps into a run nobody can act on.
	for _, ref := range run {
		if d, wrong := unknownRunField(stepID, inputName, ref); wrong {
			ds = append(ds, d)
		}
	}

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
		target, inScope := scope.steps[ref.ID]
		if !inScope {
			ds = append(ds, unresolvedStep(stepID, inputName, ref.ID, index, wf)...)

			continue
		}
		// The step this scope put in scope under that id, rather than whichever
		// step in the file happens to be spelled that way — see [refScope.steps].
		if d, wrong := unknownStepOutput(stepID, inputName, ref, target); wrong {
			ds = append(ds, d)
		}
	}

	for _, ref := range bare {
		// A name bound here — a loop's iterator, `now` inside a wait — is exactly
		// what stays bare, and is not a step.
		if scope.locals[ref] {
			continue
		}
		if isDeclarationRoot(ref) {
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
				Code: v1.DiagnosticCodeRetiredKey,
			})
			continue
		}

		// Only two answers are left here, and that is worth saying because it used
		// to be four. A self-reference and a forward reference are both references
		// to a step that *is* declared, so both are caught above and told to run
		// `flow fix` — after which they resolve as rooted references and get the
		// right message from unresolvedStep. Keeping the two branches here would
		// have been code that cannot run, which is the kind that rots unnoticed.
		if ref == v1.EventRoot {
			// Reported as what it is rather than as an unknown step, for the reason
			// `now` is below: `event` does exist, and an author has read about it or
			// copied a trigger's `with:`, so "unknown step" sends them looking for a
			// step they never wrote. What they need is where the name is bound and
			// what to do instead — which is the design's own rule, not an
			// implementation limit: everything a workflow operates on arrives
			// through `with:` into `inputs:`, so a step reading the payload directly
			// would be a second input path `flow validate` could not check.
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: "`" + v1.EventRoot + "` is the delivery a trigger was started by, bound inside a " +
					"webhook's `with:` and `idempotency_key:` and nowhere else; bind what this step needs " +
					"under that `with:` and read it here as `" + v1.InputsRoot + ".<name>`",
			})
			continue
		}
		if ref == v1.NowIdentifier {
			// Reported as what it is rather than as an unknown step. `now` does
			// exist — an author has read about it, or copied a `wait_until:` — so
			// "unknown step" sends them looking for a step they never wrote. The
			// answer they need is that it is bound where a clock exists and not
			// here, and what to do instead.
			//
			// Why it is not bound here differs between an `if:` and everything
			// else, and the difference is the whole of what an author has to
			// understand. A condition on a *wait* is the case that reads like a
			// contradiction — the step is a wait, and `now` is still not in scope —
			// because both drivers evaluate the condition before entering the node
			// (`runNodes` in eval.go, the durable driver's execute.go), so at that
			// moment the wait has not started and there is no moment to bind. An
			// input is the older story: it is resolved inside an activity, which has
			// no clock that survives a retry.
			rest := "a task input is resolved inside an activity, which has no clock that " +
				"survives a retry, so compute the moment or the length in the wait itself, or " +
				"pass the time in as an input"
			if inputName == "if" {
				rest = "a step's `if:` is evaluated before the step is entered, so even on a wait " +
					"there is no moment to bind yet; move the comparison into the wait's own " +
					"expression, or gate the step on an input or an earlier step's output"
			}
			ds = append(ds, Diagnostic{
				Step: stepID, Field: inputName,
				Message: "`now` is only available inside a wait (`sleep:`, `wait_until:`, and a " +
					"signal's `timeout:`) where the engine binds it to the moment the wait is " +
					"evaluated; " + rest,
			})
			continue
		}
		// Named rather than guessed at. Bare now means a local binding, so this is
		// not necessarily a step someone misspelled — and saying which things a bare
		// name can be is the difference between a diagnostic an author can act on and
		// one that only says no.
		//
		// The step-output form is stated in the general — `steps.<id>.<output>` — and
		// never as `steps.<ref>`: this name matches no declared step, so splicing it in
		// after `steps.` fabricates a spelling that does not resolve and was never a
		// real suggestion (`${step.a.result}` would be told to write `steps.step`). A
		// concrete "did you mean" is offered only when a step id is genuinely a near
		// edit-distance match, taken from the steps in scope here so the suggestion
		// resolves.
		message := fmt.Sprintf(
			"references unknown name %q; a bare name is a loop's iterator, a name this step "+
				"declares in its own `vars:`, or `now`, and a step output is written `%s.<id>.<output>`",
			ref, v1.StepsRoot)
		if suggestion, ok := nearest.Name(ref, slices.Sorted(maps.Keys(scope.steps))); ok {
			message += fmt.Sprintf("; did you mean `%s.%s`?", v1.StepsRoot, suggestion)
		}
		ds = append(ds, Diagnostic{
			Step: stepID, Field: inputName,
			Message: message,
			Code:    v1.DiagnosticCodeUnresolvedReference,
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
		if suggestion, ok := nearest.Name(ref, declared); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			message += fmt.Sprintf("; this workflow declares %s", strings.Join(declared, ", "))
		}
	}

	return Diagnostic{Step: stepID, Field: inputName, Value: ref, Message: message, Code: v1.DiagnosticCodeUnresolvedReference}
}

// unresolvedInput reports a reference to an input the workflow does not declare.
//
// Shaped like [unresolvedVar] and for the same reasons: the declared names are few,
// known in full, and all in scope at once, so offering them is the shortest path
// from the diagnostic to the edit.
func unresolvedInput(stepID, inputName, ref string, scope refScope) Diagnostic {
	declared := slices.Sorted(maps.Keys(scope.inputs))

	message := fmt.Sprintf("references unknown input %q", ref)
	switch {
	case len(declared) == 0:
		message += "; this workflow declares no `inputs:`, which is a top-level block naming what a " +
			"run may be started with (each with a `type:`), read as `" + v1.InputsRoot + ".<name>`"
	default:
		if suggestion, ok := nearest.Name(ref, declared); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			message += fmt.Sprintf("; this workflow declares %s", strings.Join(declared, ", "))
		}
	}

	return Diagnostic{Step: stepID, Field: inputName, Value: ref, Message: message, Code: v1.DiagnosticCodeUnresolvedReference}
}

// referencedIdentifiers returns the names an expression references, in six groups:
// steps reached under the `steps.` root, vars reached under the `vars.` root, inputs
// reached under the `inputs.` root, fields reached under the `run.` root, fields
// reached under the `trigger.` root, and whatever is written bare.
//
// Six groups rather than one because each fails differently and so wants a different
// diagnostic. An unknown step may be a forward reference, a typo, or a step that exists
// outside this scope; an unknown var can only be a typo, since every var exists before
// the first step runs; a field under `run` can only be a typo too, because unlike
// `steps` and `inputs` its shape does not depend on the file — `identity{subject,
// issuer,namespace,claims}` and `local` are the whole of it; a field under `trigger`
// is the same case with a shorter list and one more rule, since a trigger is metadata
// and never data (see [v1.TriggerContextFields]); a bare name may be a
// local, `now`, or a reference in the spelling this grammar retired. Merging them and
// sorting it out afterwards loses exactly the distinction the caller needs.
//
// Identifiers bound by a comprehension are excluded: in `items.map(x, x + 1)`
// the name `x` is introduced by the expression itself and is not a step
// reference. Reporting those would make every use of a comprehension look broken.
func referencedIdentifiers(parsed *expr.ParsedExpr) (rooted []stepRef, vars, inputs []string, run []runRef, trigger, bare []string) {
	roots := map[stepRef]struct{}{}
	varNames, inputNames, free := map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
	runFields := map[runRef]struct{}{}
	triggerFields := map[string]struct{}{}
	collectReferences(parsed.GetExpr(), map[string]struct{}{}, roots, varNames, inputNames, runFields, triggerFields, free)

	return sortedStepRefs(roots), sortedNames(varNames), sortedNames(inputNames), sortedRunRefs(runFields),
		sortedNames(triggerFields), sortedNames(free)
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

// A runRef is one reference to a field under the `run` root: which field
// (`identity` or `local`), and — for `identity` — which of its own fields is
// selected from it.
//
// Shaped like [stepRef] for the same reason and named apart from it because the
// two answer different questions: a stepRef's ID may or may not be declared in
// this file, but every runRef's Field is checked against a fixed, known set —
// `run`'s shape does not depend on what an author wrote.
type runRef struct {
	// Field is the name selected directly under `run`: `identity` or `local`.
	Field string

	// Under is the name selected from Field, or empty when the reference is to
	// Field's whole value — `${run.identity}` or `${run.local}`, both legal.
	// Only meaningful when Field is `identity`; `local` is a bool with nothing
	// under it, and this walk carries at most one level down regardless (see
	// [rootedName]).
	Under string
}

// sortedRunRefs orders references so one file reports the same diagnostics in the
// same sequence.
func sortedRunRefs(set map[runRef]struct{}) []runRef {
	refs := make([]runRef, 0, len(set))
	for ref := range set {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b runRef) int {
		if a.Field != b.Field {
			return strings.Compare(a.Field, b.Field)
		}

		return strings.Compare(a.Under, b.Under)
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
func collectReferences(e *expr.Expr, bound map[string]struct{}, rooted map[stepRef]struct{}, vars, inputs map[string]struct{}, run map[runRef]struct{}, trigger, free map[string]struct{}) {
	if e == nil {
		return
	}
	if sel := e.GetSelectExpr(); sel != nil {
		// All four roots are recognised here, and an unrecognised root falls through
		// to the walk below so that `foo.bar` still reports `foo` as a bare name.
		// That fall-through is what keeps adding a root from silently swallowing the
		// diagnostic for a name that has none.
		if root, name, under, ok := rootedName(sel, bound); ok {
			switch root {
			case v1.StepsRoot:
				rooted[stepRef{ID: name, Output: under}] = struct{}{}

				return
			case v1.VarsRoot:
				vars[name] = struct{}{}

				return
			case v1.InputsRoot:
				inputs[name] = struct{}{}

				return
			case v1.RunRoot:
				run[runRef{Field: name, Under: under}] = struct{}{}

				return
			case v1.TriggerRoot:
				// No `under`: `trigger` is one level deep by construction, because
				// its fields are four strings. A selection *into* one of them —
				// `trigger.kind.foo` — is CEL's own error on a string, which is a
				// better sentence than anything this walk could invent, so it is
				// deliberately not reported here.
				trigger[name] = struct{}{}

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
		collectReferences(kind.SelectExpr.GetOperand(), bound, rooted, vars, inputs, run, trigger, free)
	case *expr.Expr_CallExpr:
		collectReferences(kind.CallExpr.GetTarget(), bound, rooted, vars, inputs, run, trigger, free)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectReferences(arg, bound, rooted, vars, inputs, run, trigger, free)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			collectReferences(el, bound, rooted, vars, inputs, run, trigger, free)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectReferences(entry.GetMapKey(), bound, rooted, vars, inputs, run, trigger, free)
			collectReferences(entry.GetValue(), bound, rooted, vars, inputs, run, trigger, free)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr

		// The range and the accumulator's start are evaluated outside the
		// comprehension's own scope.
		collectReferences(c.GetIterRange(), bound, rooted, vars, inputs, run, trigger, free)
		collectReferences(c.GetAccuInit(), bound, rooted, vars, inputs, run, trigger, free)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		collectReferences(c.GetLoopCondition(), inner, rooted, vars, inputs, run, trigger, free)
		collectReferences(c.GetLoopStep(), inner, rooted, vars, inputs, run, trigger, free)
		collectReferences(c.GetResult(), inner, rooted, vars, inputs, run, trigger, free)
	}
}

// rootedName returns the root a select chain hangs from and the first field under it.
//
// One walk for every root the language has. `steps.a.result`, `vars.region.zone`, and
// `run.identity.subject` have identical shapes and differ only in the word at the
// bottom, so deciding which root it is belongs to the caller — a second copy of this
// walk per root is how the two come to disagree about a shadowed root or a chain three
// deep.
//
// ok is false when the chain does not bottom out in a plain identifier, or when a
// comprehension bound the root's name: then it is a binding rather than the root, and
// whatever hangs off it is neither a step, a var, nor a field of `run`.
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
	return validateThroughEdition(data, "")
}

// ValidateSourceFile is [ValidateSource] for a file read from disk, additionally
// resolving any `call:` step relative to path's own directory — see [ParseFile].
func ValidateSourceFile(path string) (Diagnostics, error) {
	data, err := readBoundedSource(path)
	if err != nil {
		return nil, err
	}
	return validateThroughEdition(data, path)
}

// ValidateSourceAt is [ValidateSource] for data that is not necessarily what
// path holds on disk yet — an editor's unsaved buffer — resolving a `call:`
// step relative to path's directory all the same. See [ParseAt].
func ValidateSourceAt(data []byte, path string) (Diagnostics, error) {
	return validateThroughEdition(data, path)
}

// validateThroughEdition validates data, and when the only thing in the way is
// an edition `flow fix` can rewrite, goes on to validate the rewritten form.
//
// # Why the gate does not stay absolute here
//
// A declared edition this build does not compile stops the parse, and the reason
// is sound for a *newer* one: every other diagnostic would describe the wrong
// language, since `nonsense:` is an unknown key here and might be a perfectly
// good key there. False diagnostics are worse than missing ones.
//
// That reason does not hold for an edition this build knows and can rewrite
// from. There the grammar is not unknown; it is one `flow fix` mechanically
// converts. Stopping at the gate cost an author a round trip they did not need:
// run `flow fix`, and only then learn about the misspelled key and the broken
// expression that were knowable on the first pass. So the rewrite is done in
// memory and the rewritten document is validated, which is what makes the rest
// of the report describe this grammar rather than guessing at another (#385).
//
// # What decides whether there is a rewrite
//
// [Fix] does, rather than a test here of which editions are old. It is the thing
// that owns the rewriting, so asking it is the one answer that cannot disagree
// with what `flow fix` will actually do, and it refuses an edition it has no
// rewrite for. Any refusal at all leaves the gate exactly where it was: a
// document that will not convert entirely is one this build still cannot read,
// and reporting against a half-converted version of it is the false-diagnostic
// trap by another route.
//
// # Why only a rewrite that moves nothing
//
// Because a diagnostic's position has to be a position in the file the author is
// holding. The rewritten document is not written anywhere, so a line number
// taken from it is a line number in a file that does not exist yet, and a
// rewrite that collapses `task:`/`name:` into one key moves everything below it:
// a fixture with the mistake on line 11 reported it on line 8, naming a line
// whose content is something else entirely. That is the false diagnostic this
// package refuses to emit, arriving by a new route.
//
// So the extra diagnostics are carried only when the rewrite touched nothing but
// the `edition:` declaration itself, which is the case the round trip was costing
// an author anyway: an old file, a new binary, and a stamp to update. Any
// rewrite that moves a line leaves the gate exactly where it was, because the
// alternative is being helpful about the wrong line.
func validateThroughEdition(data []byte, path string) (Diagnostics, error) {
	ds, err := parseAndValidate(data, path)
	if err == nil {
		return ds, nil
	}

	var gate Diagnostics
	if !errors.As(err, &gate) || !isEditionGate(gate) {
		return nil, err
	}

	fixed, fixErr := Fix(data)
	if fixErr != nil || !fixed.Complete() || !fixed.Changed() {
		return nil, err
	}
	if !editionOnlyRewrite(data, fixed.Source) {
		return nil, err
	}

	rest, restErr := parseAndValidate(fixed.Source, path)
	if restErr != nil {
		var restDiagnostics Diagnostics
		if !errors.As(restErr, &restDiagnostics) {
			// The rewritten document does not compile for a reason that is not a
			// diagnostic at all. Nothing further can honestly be said about it, so
			// the edition line stands alone exactly as it did before.
			return nil, err
		}
		rest = restDiagnostics
	}

	// Returned as an error rather than as diagnostics, because that is what a
	// document which does not compile has always come back as, and the edition is
	// still the reason it does not. What changes is only how much of the rest of
	// the file the author is told about in the same breath.
	return nil, append(slices.Clone(gate), rest...)
}

// isEditionGate reports whether a compile failed at the edition gate and nowhere
// else.
//
// The gate returns the moment it reports, so it is always the whole of the
// failure: exactly one diagnostic, about the `edition` key. Any longer list came
// from somewhere the gate had already let through, and rewriting is not the
// answer to it.
func isEditionGate(ds Diagnostics) bool {
	return len(ds) == 1 && ds[0].Field == "edition" && ds[0].Step == ""
}

// editionOnlyRewrite reports whether the rewrite changed nothing but the
// `edition:` declaration, so that every other line still sits where the author
// left it and a position taken from the rewritten document is a position in the
// document on disk.
//
// Line for line rather than by asking [FixResult.Changes] what it did, because
// what matters here is not which rules fired but whether anything moved, and the
// bytes are the only answer to that which cannot be out of date.
func editionOnlyRewrite(before, after []byte) bool {
	oldLines := strings.Split(string(before), "\n")
	newLines := strings.Split(string(after), "\n")
	if len(oldLines) != len(newLines) {
		return false
	}
	for i := range oldLines {
		if oldLines[i] == newLines[i] {
			continue
		}
		// A differing line is allowed only when it is the declaration on both
		// sides: the stamp being updated in place. A line that became an edition
		// declaration, or stopped being one, is the key moving, which moves
		// everything a position could be measured from.
		if !isEditionDeclaration(oldLines[i]) || !isEditionDeclaration(newLines[i]) {
			return false
		}
	}
	return true
}

// isEditionDeclaration reports whether a source line declares the edition.
func isEditionDeclaration(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), "edition:")
}

// parseAndValidate compiles data and validates what it compiled to, resolving a
// `call:` relative to path when there is one.
func parseAndValidate(data []byte, path string) (Diagnostics, error) {
	if path == "" {
		return validateParsed(Parse(data))
	}
	return validateParsed(ParseAt(data, path))
}

func validateParsed(wf *v1.Workflow, positions *Positions, err error) (Diagnostics, error) {
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
	// These have no position because they describe a whole-document property.
	// The atomic-bound sentence still names the enclosing step, but no single
	// child line is the reason its aggregate crosses the ceiling.
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

	// A gate on a channel the engine owns would be answered by an ask to pause
	// the run for debugging (#928). Reported here rather than over the workflow,
	// because this is where the step id and the key the author wrote are both
	// known — see [reservedSignalWaitDiagnostic].
	if d, found := reservedSignalWaitDiagnostic(id, wait); found {
		ds = append(ds, d)
	}

	// The same reference checking a condition gets, since these are the same kind
	// of expression resolving against the same names — plus `now`, which the
	// engine binds when it evaluates a wait and binds nowhere else. Added for
	// these fields rather than to the workflow's scope, so that using it in a task
	// input is still reported: there is no clock behind it there, and a name that
	// resolves in one place and not another has to say so.
	//
	// All three of a wait's expressions, not just `wait_until:`. The clock is the
	// node kind's, not the field's — see [v1.NowIdentifier] — so a scope built per
	// field here would be the place the two disagreed.
	waiting := scope.withLocal(v1.NowIdentifier)

	if until := wait.GetUntil(); until != nil {
		ds = append(ds, validateInputRefs(id, "wait_until", until, waiting, index, wf)...)
	}
	if computed := wait.GetDurationExpr(); computed != nil {
		ds = append(ds, validateInputRefs(id, "sleep", computed, waiting, index, wf)...)
	}
	if computed := wait.GetTimeoutExpr(); computed != nil {
		// Named with its full path, not bare "timeout": a step-level `timeout:`
		// is legal beside a signal's, and a bare field made Locate find the
		// step-level span first, pointing the diagnostic at the valid outer
		// timeout while the faulty expression sat one level down (#318 review).
		field := "wait_for_signal.timeout"
		if wait.GetSignalBatch() != nil {
			// The key the author wrote, so [Locate] finds the span in their file
			// rather than falling back to the whole step — the same reason this
			// path is spelled out rather than bare.
			field = "wait_for_signals.timeout"
		}
		ds = append(ds, validateInputRefs(id, field, computed, waiting, index, wf)...)
	}
	if prompt := wait.GetSignalBatch().GetPrompt(); prompt != nil {
		ds = append(ds, validateInputRefs(id, "wait_for_signals.prompt", prompt, waiting, index, wf)...)
	}
	if prompt := wait.GetSignal().GetPrompt(); prompt != nil {
		// Named with its full path for the reason `timeout:` is: a step may carry
		// other keys called `prompt` in future, and a bare field name makes Locate
		// find whichever span it reaches first.
		//
		// Checked against `waiting` and not against the shaping scope below: a
		// prompt is evaluated when the wait *parks*, so the wait's own result does
		// not exist yet and `${payload.x}` here really is naming a step.
		ds = append(ds, validateInputRefs(id, "wait_for_signal.prompt", prompt, waiting, index, wf)...)
	}

	// A `wait_for_signal:`'s own `outputs:` sees three more names than the rest of
	// the wait does: its result. `payload` and `sender` are the signal's, and
	// `timed_out` is how it ended — bound bare, at the moment the wait resolves,
	// by [v1.ShapeSignalOutputs].
	//
	// Added to the scope for these expressions only, exactly as `now` is added for
	// the wait's own. A step in the same file may legitimately be called `payload`,
	// and the whole reason these are locals rather than a widening of the workflow
	// scope is that `${payload}` has to keep meaning that step everywhere else.
	if shaped := wait.GetSignal().GetOutputs(); len(shaped) > 0 {
		shaping := waiting.
			withLocal(v1.PayloadOutput).
			withLocal(v1.SenderOutput).
			withLocal(v1.TimedOutOutput)

		for _, name := range slices.Sorted(maps.Keys(shaped)) {
			ds = append(ds, validateInputRefs(id, "outputs."+name, shaped[name], shaping, index, wf)...)
		}
	}

	// A `wait_for_signals:`'s own `outputs:` sees its *own* result, which is a
	// different set of names: `deliveries` and `count` rather than `payload` and
	// `sender`, plus the `timed_out` both arms produce.
	//
	// Per arm rather than a union of the two, and this is the direction that
	// matters: a union would make `${payload.x}` resolve inside a batch's
	// shaping, where [v1.ShapeSignalBatchOutputs] binds no such name — so the
	// file would validate and then fail at run time on the one driver the author
	// was not looking at. Widening a scope is how a validator goes quiet about a
	// real mistake; the rewriter in `fix.go` takes the union for the opposite
	// reason, spelled out at [waitShapingNames].
	if shaped := wait.GetSignalBatch().GetOutputs(); len(shaped) > 0 {
		shaping := waiting.
			withLocal(v1.DeliveriesOutput).
			withLocal(v1.CountOutput).
			withLocal(v1.TimedOutOutput)

		for _, name := range slices.Sorted(maps.Keys(shaped)) {
			ds = append(ds, validateInputRefs(id, "outputs."+name, shaped[name], shaping, index, wf)...)
		}
	}

	return ds
}

// validateValue checks a `value:` step: that it holds something, and that what it
// holds resolves.
//
// The reference check is [validateInputRefs] unchanged, against the step's own
// scope, because a value is evaluated exactly where it is written and sees exactly
// what a task's inputs written there would see. There is no second scope to model
// and no name the kind binds, which is the whole of what makes this the shortest
// validator in the file, and is the point of the design rather than an omission.
//
// The three properties refused on this kind are refused elsewhere, each where it
// has a position to be refused at: `retry:` and `timeout:` by the parser, on their
// own keys, and `undo:` by [validateUndo] through [v1.CheckUndoPlacement], on the
// `undo:` key. Repeating any of them here would report one mistake twice.
func validateValue(id string, value *v1.Value, scope refScope, index int, wf *v1.Workflow) Diagnostics {
	if value == nil {
		return Diagnostics{{
			Step:    id,
			Field:   "value",
			Message: "has no expression; a `value:` step is the expression that produces the value it names",
		}}
	}

	return validateInputRefs(id, "value", value, scope, index, wf)
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
			case *v1.Node_Loop:
				if walk(kind.Loop.GetBody()) {
					return true
				}
			case *v1.Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					if walk(branch.GetSteps()) {
						return true
					}
				}
			case *v1.Node_Switch:
				for _, body := range v1.SwitchBodies(kind.Switch) {
					if walk(body) {
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
			Code:    v1.DiagnosticCodeUnresolvedReference,
		}}
	}
	for _, other := range wf.GetSteps()[index:] {
		if other.GetId() == ref {
			return Diagnostics{{
				Step: stepID, Field: inputName,
				Message: fmt.Sprintf(
					"references step %q, which runs later; steps can only reference steps defined before them", ref),
				Code: v1.DiagnosticCodeUnresolvedReference,
			}}
		}
	}
	return Diagnostics{{
		Step: stepID, Field: inputName,
		Message: fmt.Sprintf("references unknown step %q", ref),
		Code:    v1.DiagnosticCodeUnresolvedReference,
	}}
}

// toleratedErrorOutput is the output a step gains by being allowed to fail.
//
// Written by both drivers in place of a failed task's own outputs when
// `continue_on_error:` is set, which is what a later step branches on. Named here
// because the validator has to know about it and does not otherwise: it is the one
// output that comes from the *policy* rather than from the task.
const toleratedErrorOutput = v1.StepErrorOutput

// runIdentityFields are the fields [runRootValue] renders under `run.identity`.
//
// Kept here rather than derived from [WorkloadIdentity]'s own field list, because
// the two are deliberately not the same shape — `run.identity` is narrower, and
// [runRootValue]'s own doc says why (`deployment` is left off). This names what an
// expression actually reaches, which is the only set a diagnostic here can be
// honest about.
var runIdentityFields = []string{"subject", "issuer", "namespace", "claims"}

// runFields are the fields [runRootValue] renders directly under `run`.
//
// The set is closed on purpose and the two absences are the interesting part:
// there is no `start_time` and no `attempt`, so `${run.start_time}` is reported
// here rather than silently resolving to something. [v1.RunAddress] records why
// neither will be added — a start time is a clock read by another name, and
// `now` is bound only inside a wait precisely so a task cannot read a clock.
var runFields = []string{"identity", "local", "workflow_id", "run_id"}

// unknownRunField reports a reference to a field `run` does not have.
//
// Unlike [unknownStepOutput], this is never silent about a field it can name: a
// step's outputs depend on the task it runs and the file's own declarations, but
// `run`'s shape is fixed by the engine — [runRootValue] renders exactly these
// fields and no others, on every run, on both drivers — so an unknown one is
// always a mistake and never a shape this validator lacks the knowledge to judge.
//
// Silent below `claims`, deliberately: `run.identity.claims["team"]` and
// `run.identity.claims.team` are both legal, because claims are a map keyed by
// whatever the identity provider issued, and this only ever sees as far as
// `claims` itself — one level down, the same reach [rootedName] gives every
// other root. Reporting into a map's dynamic keys would be exactly the false
// diagnostic this package's own standard refuses to draw.
func unknownRunField(stepID, inputName string, ref runRef) (Diagnostic, bool) {
	if !slices.Contains(runFields, ref.Field) {
		message := fmt.Sprintf("references unknown field %q of `run`", ref.Field)
		if suggestion, ok := nearest.Name(ref.Field, runFields); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			// Comma-joined, as the `run.identity` branch below already is: the set
			// grew past the two names " and " read well between.
			message += fmt.Sprintf("; `run` has %s", strings.Join(runFields, ", "))
		}

		return Diagnostic{
			Step: stepID, Field: inputName, Value: v1.RunRoot + "." + ref.Field, Message: message,
			Code: v1.DiagnosticCodeUnresolvedReference,
		}, true
	}

	if ref.Field != "identity" || ref.Under == "" {
		// `run.local` (a bool, nothing under it) or the whole of either field —
		// `${run.identity}`, `${run.local}` — both legal.
		return Diagnostic{}, false
	}

	if slices.Contains(runIdentityFields, ref.Under) {
		return Diagnostic{}, false
	}

	message := fmt.Sprintf("references unknown field %q of `run.identity`", ref.Under)
	if suggestion, ok := nearest.Name(ref.Under, runIdentityFields); ok {
		message += fmt.Sprintf("; did you mean %q?", suggestion)
	} else {
		message += fmt.Sprintf("; `run.identity` has %s", strings.Join(runIdentityFields, ", "))
	}

	return Diagnostic{
		Step: stepID, Field: inputName,
		Value:   v1.RunRoot + ".identity." + ref.Under,
		Message: message,
		Code:    v1.DiagnosticCodeUnresolvedReference,
	}, true
}

// unknownTriggerField reports a reference to a field `trigger` does not have.
//
// Never silent, exactly as [unknownRunField] is never silent, and for the same
// reason: `trigger`'s shape is fixed by the engine ([v1.TriggerContextValue]
// renders these four fields and no others, on every run, on both drivers), so an
// unknown one is always a mistake rather than a shape this validator lacks the
// knowledge to judge.
//
// What is different here is *why* the set is closed, and it is worth the extra
// sentence in the diagnostic. `trigger` is metadata and never data: everything a
// workflow operates on arrives through a trigger's `with:` into `inputs:`, where
// declarations exist for this validator to check against. So `${trigger.body}` is
// not a field somebody forgot to add — it is the second input path this design
// exists to refuse, and an author reaching for it is told where the value they want
// actually comes from rather than only that the name is wrong.
func unknownTriggerField(stepID, inputName, field string) (Diagnostic, bool) {
	fields := v1.TriggerContextFields()
	if slices.Contains(fields, field) {
		return Diagnostic{}, false
	}

	message := fmt.Sprintf("references unknown field %q of `%s`", field, v1.TriggerRoot)
	if suggestion, ok := nearest.Name(field, fields); ok {
		message += fmt.Sprintf("; did you mean %q?", suggestion)
	} else {
		message += fmt.Sprintf("; `%s` has %s", v1.TriggerRoot, strings.Join(fields, ", "))
	}
	message += fmt.Sprintf(". It says how the run started and nothing about what it carries: a delivery's "+
		"payload reaches the run through the trigger's `with:` into `%s:`, and is read as `%s.<name>`",
		v1.InputsRoot, v1.InputsRoot)

	return Diagnostic{
		Step: stepID, Field: inputName, Value: v1.TriggerRoot + "." + field, Message: message,
		Code: v1.DiagnosticCodeUnresolvedReference,
	}, true
}

// unknownTriggerKindLiterals returns, in first-reached order and with
// duplicates removed, every string literal `e` compares `trigger.kind` to
// with `==` or `!=` that is not one of [v1.KnownTriggerKind]'s kinds.
//
// A comparison is judged only when the other side is a string constant.
// `${trigger.kind == vars.expected}` or `${trigger.kind == inputs.want}`
// compares against a value this validator cannot see at authoring time, and
// reporting one would be exactly the false diagnostic CLAUDE.md ranks worse
// than a missing one — the same restraint [unknownTriggerField] already
// applies to a field name it cannot resolve. A use of `trigger.kind` that is
// not a comparison at all — bare, interpolated into a message, passed to a
// function — is not visited by the walk below in any way that reports it,
// for the same reason.
//
// bound tracks names a comprehension has rebound, exactly as
// [collectReferences] tracks it, so that `["schedual"].exists(trigger,
// trigger.kind == "schedual")` — vanishingly unlikely, but the language
// allows it — does not misread the macro's own iteration variable as the
// root.
func unknownTriggerKindLiterals(e *expr.Expr, bound map[string]struct{}) []string {
	seen := map[string]struct{}{}
	var literals []string

	report := func(literal string) {
		if v1.KnownTriggerKind(literal) {
			return
		}
		if _, dup := seen[literal]; dup {
			return
		}
		seen[literal] = struct{}{}
		literals = append(literals, literal)
	}

	var walk func(e *expr.Expr, bound map[string]struct{})
	walk = func(e *expr.Expr, bound map[string]struct{}) {
		if e == nil {
			return
		}
		switch kind := e.GetExprKind().(type) {
		case *expr.Expr_SelectExpr:
			walk(kind.SelectExpr.GetOperand(), bound)
		case *expr.Expr_CallExpr:
			call := kind.CallExpr
			if args := call.GetArgs(); len(args) == 2 {
				switch call.GetFunction() {
				case operators.Equals, operators.NotEquals:
					checkTriggerKindComparand(args[0], args[1], bound, report)
					checkTriggerKindComparand(args[1], args[0], bound, report)
				}
			}
			walk(call.GetTarget(), bound)
			for _, arg := range call.GetArgs() {
				walk(arg, bound)
			}
		case *expr.Expr_ListExpr:
			for _, el := range kind.ListExpr.GetElements() {
				walk(el, bound)
			}
		case *expr.Expr_StructExpr:
			for _, entry := range kind.StructExpr.GetEntries() {
				walk(entry.GetMapKey(), bound)
				walk(entry.GetValue(), bound)
			}
		case *expr.Expr_ComprehensionExpr:
			c := kind.ComprehensionExpr

			walk(c.GetIterRange(), bound)
			walk(c.GetAccuInit(), bound)

			inner := make(map[string]struct{}, len(bound)+3)
			for name := range bound {
				inner[name] = struct{}{}
			}
			for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
				if name != "" {
					inner[name] = struct{}{}
				}
			}
			walk(c.GetLoopCondition(), inner)
			walk(c.GetLoopStep(), inner)
			walk(c.GetResult(), inner)
		}
	}

	walk(e, bound)

	return literals
}

// checkTriggerKindComparand reports the literal other is compared against
// when field is a reference to `trigger.kind` and other is a string
// constant — one side of one `==`/`!=` call, checked by
// [unknownTriggerKindLiterals] in both orders so the literal may sit on
// either side of the operator.
func checkTriggerKindComparand(field, other *expr.Expr, bound map[string]struct{}, report func(string)) {
	sel := field.GetSelectExpr()
	if sel == nil {
		return
	}
	root, name, _, ok := rootedName(sel, bound)
	if !ok || root != v1.TriggerRoot || name != "kind" {
		return
	}
	sv, ok := other.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return
	}
	report(sv.StringValue)
}

// unknownTriggerKindLiteral reports a `trigger.kind` comparison against a
// string literal that names no kind this build can start a run with.
//
// The wording echoes [v1.CheckTriggerContext]'s runtime refusal on purpose:
// the same fact — this string is not a trigger kind — is worth saying
// identically whether it is caught while authoring or, absent this check,
// discovered in a run's history after both drivers evaluated the comparison
// as false and skipped the branch it guarded.
func unknownTriggerKindLiteral(stepID, inputName, literal string) Diagnostic {
	kinds := v1.TriggerKinds()

	message := fmt.Sprintf("compares `%s.kind` to %q, which is not a kind Flowstate starts runs with",
		v1.TriggerRoot, literal)
	if suggestion, ok := nearest.Name(literal, kinds); ok {
		message += fmt.Sprintf("; did you mean %q?", suggestion)
	} else {
		message += fmt.Sprintf("; the kinds are %s", strings.Join(kinds, ", "))
	}

	return Diagnostic{
		Step: stepID, Field: inputName, Value: v1.TriggerRoot + ".kind", Message: message,
		Code: v1.DiagnosticCodeUnresolvedReference,
	}
}

// unknownStepOutput reports a reference to an output a step does not produce.
//
// Silent unless it is sure, which is most of the time and deliberately so. Certainty
// comes from one of two places, and both are covered:
//
//   - a step running a *task* whose outputs are declared as a message, where the set
//     is known in full from the task descriptor, or a wait whose `outputs:` shapes
//     them the same way;
//   - a step whose *top-level* name set is fixed by the grammar rather than by a task,
//     a sender, or an author's own shaping — a `value:`, a `switch:`, a `for_each`, a
//     `call`, a `parallel`, and a loop's own `results`/`state` — which [v1.OutputNames]
//     answers with the identical certainty, for the identical reason: none of those
//     consult a registry an author's editor may not have loaded, or a scope only the
//     task itself evaluates. "Top-level" matters: what a `for_each`'s `results`
//     *contains* is not knowable from the file, but `ref.Output` never asks — it names
//     one segment past `steps.<id>.` (see [stepRef]), and that segment is exactly what
//     these kinds fix.
//
// Every other shape produces names this cannot enumerate:
//
//   - `http` with an `outputs:` input replaces its declared outputs with names the
//     author chose, and the whole point of that input is that they are not fixed;
//   - an unshaped `wait_for_signal:` carries whatever a sender sent — and although its
//     top-level names (`timed_out`, `payload`, `sender`) are just as fixed by the
//     grammar as a loop's, checking them here was tried and reverted: it correctly
//     reports `${steps.approval.approved}` as unknown, and `TestWaitOutputsAreReferenceable`
//     and `TestValidateAcceptsWaits` pin that exact shape as legal. Reconciling the two
//     is a separate investigation, so this one stays silent, inheriting the boundary
//     rather than re-deciding it;
//   - a plugin may decline to describe its outputs at all, or shape with a computed
//     expression this cannot read statically.
//
// A false diagnostic is worse than a missing one, so every one of those stays silence —
// and [v1.OutputNames] agrees: it answers those with an entry whose Name is empty
// rather than a name, which [certainNames] reads as "nothing to check" for exactly the
// same reason this function would otherwise have to. The case this does cover is the
// one that is never right: a step whose kind declares a fixed set, referenced by a name
// outside it — which `log`, declaring none, makes total rather than occasional.
func unknownStepOutput(stepID, inputName string, ref stepRef, node *v1.Node) (Diagnostic, bool) {
	if ref.Output == "" {
		// The whole outputs mapping. Legal for any step, including one with nothing
		// in it.
		return Diagnostic{}, false
	}

	if node == nil {
		// Nothing recorded under that id, so there is nothing certain to say. A
		// reference that reaches here has already been reported as unresolved.
		return Diagnostic{}, false
	}

	// A loop's outputs are `results` and, when it carries state, `state` — fixed the
	// moment the loop is written (`state` appears exactly when `state:` is set), the
	// same certainty [v1.OutputNames] gives every other grammar-fixed kind below. Its
	// `as:` name binds *inside* the loop only — the body, `until:` and `update:` — so
	// from outside, a reference to `steps.<loop>.<the as: name>` is naming the bound
	// value by a name it does not have out here, the single most likely loop-output
	// mistake, and it earns its own message pointed at `state` rather than the generic
	// "did you mean" the fallthrough below would give it. What this does *not* reach is
	// one level deeper: a body step's own outputs, surfaced through `results`, are not
	// knowable in full (a body step may produce anything) — the same latitude a
	// `for_each`'s `results` gets below, and neither loop kind's certainty reaches past
	// its own top-level name.
	if loop := node.GetLoop(); loop != nil {
		// A loop whose carried state happens to be named `error` collides, by
		// spelling alone, with the tolerated-error output a `continue_on_error:`
		// loop also carries. Where the policy is set, `error` is not a mistaken
		// reach for the loop's `as:` name — it is the real output the policy
		// grants — so that reading has to win before the state-name message
		// below claims the whole name for itself.
		if state := loop.GetState(); state != "" && ref.Output == state &&
			!(ref.Output == toleratedErrorOutput && node.GetPolicy().GetContinueOnError()) {
			return Diagnostic{
				Step: stepID, Field: inputName, Value: ref.Output,
				Message: fmt.Sprintf(
					"step %q has no output %q; `%s` is the name the loop binds *inside* itself (its `as:`), which does not exist out here; the carried value is read as `%s.%s.state`",
					ref.ID, ref.Output, ref.Output, v1.StepsRoot, ref.ID),
				Code: v1.DiagnosticCodeUnresolvedReference,
			}, true
		}

		return certainStepOutput(stepID, inputName, "loop", ref, certainNames(node),
			fmt.Sprintf("a loop step's own id exposes nothing — its outputs are not read under `%s.%s.`", v1.StepsRoot, ref.ID))
	}

	// A switch, a for_each, a call, and a parallel all answer [v1.OutputNames] with the
	// same certainty a `value:` step's output has below: none of them consult a
	// registry, a sender, or an authored expression to decide what they expose, so the
	// set is fixed the instant the step is written.
	//
	// Two of the four can be certain and *empty* at once, for two different reasons a
	// reader should not have to guess between, which is why each names its own case
	// rather than sharing [certainStepOutput]'s generic wording:
	//
	//   - a parallel's branches merge into the *enclosing* scope, so nothing is ever
	//     reachable under the parallel step's own id — structural, true of every
	//     parallel there is or will be;
	//   - a call whose callee declares no `outputs:` produces nothing this run, the
	//     same standing `log`'s empty descriptor has below — true of *this* callee,
	//     and would read differently for one that declared even one.
	switch node.GetKind().(type) {
	case *v1.Node_Switch:
		return certainStepOutput(stepID, inputName, "switch", ref, certainNames(node),
			"a switch step's own id exposes nothing")
	case *v1.Node_ForEach:
		return certainStepOutput(stepID, inputName, "for_each", ref, certainNames(node),
			"a for_each step's own id exposes nothing")
	case *v1.Node_Call:
		return certainStepOutput(stepID, inputName, "call", ref, certainNames(node),
			"the called workflow declares no outputs")
	case *v1.Node_Parallel:
		return certainStepOutput(stepID, inputName, "parallel", ref, certainNames(node),
			fmt.Sprintf("a parallel step's own id exposes nothing — its branches merge into the enclosing scope, not under `%s.%s.`", v1.StepsRoot, ref.ID))
	}

	// A `value:` answers for its outputs exactly, and is the only kind that can
	// answer with certainty without consulting anything: the set is one name, fixed
	// by the grammar rather than by a task descriptor, a sender, or an author's
	// shaping. `${steps.decided.velue}` is therefore never right, and never a false
	// diagnostic.
	//
	// It is the kind where silence would cost most, too. A value exists to be read
	// from several places, so a name nothing produces is a reference that resolves
	// to nothing in every branch built on it at once, and a boolean that reads as
	// nothing takes the other arm of each.
	if _, isValue := node.GetKind().(*v1.Node_Value); isValue {
		produced := []string{v1.ValueOutput}

		// A value can fail at run time even though it cannot fail differently on
		// a second attempt: an expression divides by zero, or names something
		// absent. `retry:` is refused on this kind because a deterministic
		// expression has nothing to gain from another attempt, but
		// `continue_on_error:` is *not* refused and is not meaningless, so a
		// tolerated value produces `error` in place of `value` on both drivers
		// exactly as any other tolerated step does. Listing it is what keeps this
		// from reporting the documented pattern as a mistake.
		if node.GetPolicy().GetContinueOnError() {
			produced = append(produced, toleratedErrorOutput)
		}
		if slices.Contains(produced, ref.Output) {
			return Diagnostic{}, false
		}

		message := fmt.Sprintf(
			"step %q has no output %q; a `value:` step produces exactly one output, `%s`, so the whole of it is read as `%s.%s.%s`",
			ref.ID, ref.Output, v1.ValueOutput, v1.StepsRoot, ref.ID, v1.ValueOutput)
		if !node.GetPolicy().GetContinueOnError() && ref.Output == toleratedErrorOutput {
			message += "; `" + toleratedErrorOutput + "` exists only on a step that carries `continue_on_error:`, which this one does not"
		}

		return Diagnostic{
			Step: stepID, Field: inputName, Value: ref.Output,
			Message: message,
			Code:    v1.DiagnosticCodeUnresolvedReference,
		}, true
	}

	// A wait that shapes its own outputs answers for them exactly, which is the
	// one thing that makes replace semantics safe to ship: `outputs:` *drops* the
	// wait's defaults, so `${steps.gate.payload.approved}` after a shaping that
	// did not re-expose `payload` reads nothing at all, and every branch built on
	// it quietly takes the other arm. The shaped set is written in this file and
	// knowable in full, so the diagnostic cannot be false.
	//
	// A wait that does *not* shape is left unchecked, as it always has been. This
	// was tried during this change: [v1.OutputNames] answers an unshaped wait with
	// the same top-level certainty a loop's `results`/`state` gets below
	// (`timed_out`, `payload`, `sender`, fixed by the grammar), and reading it here
	// reports `${steps.approval.approved}` as unknown, which is correct under
	// [PayloadOutput]'s current rooting — a bare name outside those three never
	// resolves. But `TestWaitOutputsAreReferenceable` and `TestValidateAcceptsWaits`
	// pin the opposite: that exact reference, and others like it, must stay silent.
	// Reconciling that is a separate change with its own investigation (do those
	// tests encode a real exception, or are they simply stale against
	// [PayloadOutput]'s rooting?), so this stays exactly as narrow as it was before
	// this file started reading [v1.OutputNames] — silence here is inherited, not
	// re-decided.
	shapedWait := node.GetWait().GetSignal().GetOutputs()
	if len(shapedWait) == 0 {
		// The batch spelling shapes under exactly the same rule — replace, not
		// extend — so a reference to a name its `outputs:` dropped has to be
		// reported here too. Reading only the single-wait arm would leave the
		// newer spelling silently accepting every unresolved reference, which is
		// the diagnostic-rot shape `hasTimeout` above already records.
		shapedWait = node.GetWait().GetSignalBatch().GetOutputs()
	}
	if shaped := shapedWait; len(shaped) > 0 {
		if _, produced := shaped[ref.Output]; produced {
			return Diagnostic{}, false
		}

		names := slices.Sorted(maps.Keys(shaped))
		message := fmt.Sprintf("step %q has no output %q; its `outputs:` replaces what the wait produces, and it produces %s",
			ref.ID, ref.Output, strings.Join(names, ", "))
		if suggestion, ok := nearest.Name(ref.Output, names); ok {
			message = fmt.Sprintf("step %q has no output %q; its `outputs:` replaces what the wait produces; did you mean %q?",
				ref.ID, ref.Output, suggestion)
		} else if waitOwnOutput(node, ref.Output) {
			message += fmt.Sprintf("; `%s` is one of the wait's own outputs, which shaping dropped; re-expose it with `%s: ${%s}`",
				ref.Output, ref.Output, ref.Output)
		}

		return Diagnostic{
			Step: stepID, Field: inputName, Value: ref.Output,
			Message: message,
			Code:    v1.DiagnosticCodeUnresolvedReference,
		}, true
	}

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
	if !known {
		return Diagnostic{}, false
	}

	// A task that shapes its outputs answers for them, and answers exactly when
	// the names it shaped are knowable — which is the same standing a shaping wait
	// has, reached by the same route, and the reason the mapping form is the
	// documented spelling.
	//
	// Whether this task shapes at all is *declared* (see [v1.TaskDef.ShapesOutputs]).
	// It used to be decided by the presence of an input called `outputs`, which
	// stood this check down for any plugin that happened to name an input that —
	// while its executor returned exactly the outputs its descriptor declared. The
	// validator, the editor and the runtime disagreed, and nothing could flag it
	// because the two that agreed were the two doing the reporting (#324).
	if shaping, replaced := shapedTaskOutputs(task); replaced {
		if d, report := unknownShapedOutput(stepID, inputName, ref, shaping, def); report {
			return d, true
		}
		return Diagnostic{}, false
	}

	if def.Outputs == nil {
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
		if suggestion, ok := nearest.Name(ref.Output, produced); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			message += fmt.Sprintf("; it produces %s", strings.Join(produced, ", "))
		}
	}

	return Diagnostic{
		Step: stepID, Field: inputName, Value: ref.Output, Message: message,
		Code: v1.DiagnosticCodeUnresolvedReference,
	}, true
}

// certainNames narrows [v1.OutputNames] to the names it is sure of, for a node kind
// whose whole answer is fixed by the grammar.
//
// [v1.NamedOutput.Name] is empty in two cases OutputNames does not distinguish by
// return value alone: it is not certain at all (an unregistered task, a shaping
// expression it cannot read statically), or it is certain and the answer is *zero*
// names (a call whose callee declares no `outputs:`, the same standing `log`'s empty
// descriptor has). Filtering on Name here treats both alike — an empty result — which
// is safe for every caller in this file: none of the five kinds below ever has an
// uncertain arm in [OutputNames]'s switch, so an empty Name reaching here is always
// the second case, never the first. A node kind added to that switch without also
// being added here is the one way this stops being true, which is exactly what
// [TestACertainKindsAreExactlyWhatOutputNamesAnswersWithCertainty] pins.
func certainNames(node *v1.Node) []string {
	entries, _ := v1.OutputNames(node, nil)
	names := make([]string, 0, len(entries)+1)
	for _, e := range entries {
		if e.Name != "" {
			names = append(names, e.Name)
		}
	}
	// This output belongs to the step policy rather than its kind. Include it in
	// every fixed set so the early compound-step checks agree with the generic
	// task path below.
	if node.GetPolicy().GetContinueOnError() {
		names = append(names, toleratedErrorOutput)
	}
	return names
}

// certainStepOutput reports a reference outside a fixed output set — the shared
// message for every node kind whose answer [v1.OutputNames] gives with certainty and
// this file has no kind-specific wording for. kind names the step's flavor for the
// message ("switch", "for_each", "call", "parallel", "loop"); produced is that kind's
// exact name set. emptyMessage is used verbatim when produced is empty, because "why"
// differs by kind (a parallel's branches merge elsewhere; a call's callee simply
// declared none) and a caller who wrote the specific reason should not have it
// discarded for a generic one.
func certainStepOutput(stepID, inputName, kind string, ref stepRef, produced []string, emptyMessage string) (Diagnostic, bool) {
	if slices.Contains(produced, ref.Output) {
		return Diagnostic{}, false
	}

	message := fmt.Sprintf("step %q has no output %q", ref.ID, ref.Output)
	switch {
	case len(produced) == 0:
		message += "; " + emptyMessage
	default:
		if suggestion, ok := nearest.Name(ref.Output, produced); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		} else {
			message += fmt.Sprintf("; a %s step produces %s", kind, strings.Join(produced, ", "))
		}
	}

	return Diagnostic{
		Step: stepID, Field: inputName, Value: ref.Output,
		Message: message,
		Code:    v1.DiagnosticCodeUnresolvedReference,
	}, true
}

// shapedTaskOutputs returns a task step's shaping value, and reports whether the
// step shapes its outputs at all.
//
// Two questions, both of which have to be yes: the task declares that it reads
// [v1.ShapingInput] as a replacement, and this invocation writes one. A task that
// shapes and a step that did not ask it to is an ordinary step producing what the
// task declares.
func shapedTaskOutputs(task *v1.Task) (*v1.Value, bool) {
	if !v1.TaskShapesOutputs(task.GetName()) {
		return nil, false
	}
	value, written := task.GetInputs()[v1.ShapingInput]
	return value, written
}

// unknownShapedOutput reports a reference to a name a shaping step does not
// produce.
//
// The wait's diagnostic, word for word where the words are still true, because it
// is the same mistake: `outputs:` *replaces*, so a reference to a name that was
// dropped reads nothing at all and every branch built on it quietly takes the
// other arm. What makes it safe to report is that the shaped set is written in
// the file — so this is silent, and has to be, wherever it is not.
//
// The one sentence that is not the wait's is the re-exposure suggestion. A wait
// can offer the exact line to write (`payload: ${payload}`) because it binds its
// own result bare and the validator knows those three names. A shaping task's
// expressions are evaluated by the task, in a scope no validator has, so the
// honest offer is the name and the place to put it rather than an expression that
// might not resolve there.
func unknownShapedOutput(stepID, inputName string, ref stepRef, shaping *v1.Value, def v1.TaskDef) (Diagnostic, bool) {
	names, knowable := v1.ShapedOutputNames(shaping)
	if !knowable {
		// A map built by an expression has no keys until it has run. Deliberately
		// unchecked rather than guessed at: this is what the string-fenced spelling
		// costs, and it is the trade the docs state.
		return Diagnostic{}, false
	}
	if slices.Contains(names, ref.Output) {
		return Diagnostic{}, false
	}

	message := fmt.Sprintf("step %q has no output %q; its `outputs:` replaces what the %s task produces, and it produces %s",
		ref.ID, ref.Output, def.Name, strings.Join(names, ", "))

	// A name the task itself declares is answered first, and not by a
	// did-you-mean. It is the one case where what happened is *known* rather than
	// guessed — the author is reading a real output of this task that shaping
	// dropped — and the nearest-name search will happily offer a shaped name in
	// its place, which sends them to rewrite the reference instead of the
	// shaping. `body` against a shaping that produced `code` is exactly that
	// mistake, and it is close enough to match.
	switch {
	case def.Outputs != nil && def.Outputs.Fields().ByName(protoreflect.Name(ref.Output)) != nil:
		message += fmt.Sprintf("; `%s` is one of the %s task's own outputs, which shaping dropped; re-expose it by naming it in `outputs:`",
			ref.Output, def.Name)
	default:
		if suggestion, ok := nearest.Name(ref.Output, names); ok {
			message = fmt.Sprintf("step %q has no output %q; its `outputs:` replaces what the %s task produces; did you mean %q?",
				ref.ID, ref.Output, def.Name, suggestion)
		}
	}

	return Diagnostic{
		Step: stepID, Field: inputName, Value: ref.Output,
		Message: message,
		Code:    v1.DiagnosticCodeUnresolvedReference,
	}, true
}

// nodeWithID returns the node an id names, anywhere in the workflow, or nil.
//
// Scope-blind, and therefore not what a check on a *reference* may use: an id is
// unique within a visibility domain and not within a workflow, so two sibling
// blocks may each declare a body step of the same id and this answers with
// whichever comes first in the file. Every validation path takes the step from
// [refScope.steps] instead, which holds what was put in scope where it was put
// there. What remains here is the test helper that reads a fixture's node back
// by name, where the file is known to declare exactly one.
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
			case *v1.Node_Loop:
				if found := walk(kind.Loop.GetBody()); found != nil {
					return found
				}
			case *v1.Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					if found := walk(branch.GetSteps()); found != nil {
						return found
					}
				}
			case *v1.Node_Switch:
				for _, body := range v1.SwitchBodies(kind.Switch) {
					if found := walk(body); found != nil {
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
	code := d.Code
	if code == "" {
		code = v1.DiagnosticCodeGeneral
	}

	return &v1.Diagnostic{
		Line:    uint32(max(d.Line, 0)),
		Column:  uint32(max(d.Column, 0)),
		Message: d.Message,
		Step:    d.Step,
		Field:   d.Field,
		Kind:    d.Kind,
		Value:   d.Value,
		Code:    string(code),
		Edits:   d.Edits,
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

// waitOwnOutput reports whether name is one of the outputs the *waiting step
// itself* produces before shaping replaced them, so the diagnostic above can
// tell an author their reference was dropped rather than misspelled.
//
// Per arm rather than one union of both, and that is the point: `payload` and
// `sender` are a single wait's names and `deliveries` and `count` are a batch's,
// so a union would advise an author of a `wait_for_signals:` to "re-expose"
// `payload`, which that spelling never produced. Advice for a name the step
// could not have had is worse than no advice — it sends the reader looking for
// a value that never existed.
func waitOwnOutput(node *v1.Node, name string) bool {
	if name == v1.TimedOutOutput {
		// The one name both arms produce, which is why it is checked before
		// either of them.
		return node.GetWait() != nil
	}

	if node.GetWait().GetSignal() != nil {
		return name == v1.PayloadOutput || name == v1.SenderOutput
	}
	if node.GetWait().GetSignalBatch() != nil {
		return name == v1.DeliveriesOutput || name == v1.CountOutput
	}

	return false
}
