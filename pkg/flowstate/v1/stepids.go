package flowstatev1

import (
	"fmt"
	"slices"
	"strings"
)

// This file holds the scope rules a step id must satisfy, once, for every door a
// specification enters through.
//
// `flowfile.Validate` reports them against a position in a file; the submit
// boundary ([BindRunInputs], reached by `RunWorkflow`, `CreateSchedule`, the
// webhook bridge and the local driver alike) refuses them in a specification that
// never was a Flowfile. Both ask [StepIDIssues], so a rule has one spelling and a
// hand-built spec meets the rule the compiler's author did (invariant 2).
//
// Without it, a step `id: vars` beside a workflow `vars:` block, or two steps
// sharing an id, ran clean: the schema's pattern admits both, and the resolver
// answered a root-named step per selector rather than per name (#1430).

// DeclarationRoots are the rooted namespaces a bound name may not shadow.
//
// A step id, a loop's binding and a step's own `vars:` key are all names that
// would win over a root when an expression resolves, so a file taking one does
// not collide with the root: it hides it, silently, for every expression after
// the point it is bound. Written as the category rather than as the one root
// that needed it first.
//
// The table is unexported so an embedder cannot reassign it and switch a submit
// refusal off for the process; [DeclarationRoots] hands out a copy.
var declarationRoots = []string{StepsRoot, VarsRoot, InputsRoot, RunRoot, TriggerRoot}

// DeclarationRoots returns a copy of the rooted namespaces a bound name may not
// shadow.
func DeclarationRoots() []string { return slices.Clone(declarationRoots) }

// IsDeclarationRoot reports whether name is one of the [DeclarationRoots].
func IsDeclarationRoot(name string) bool {
	return slices.Contains(declarationRoots, name)
}

// CELUnusableStepIDs are the words no step may be named even under a root.
//
// CEL refuses a reserved word in identifier position and nowhere else, and
// `steps.<id>` is a field select, so most reserved words became legal ids when
// references were rooted. These four are refused a level lower, by the lexer,
// which no amount of qualifying reaches: `true`, `false` and `null` are literals
// and `in` is an operator, so `steps.in` is a syntax error in the grammar itself.
//
// Unexported for the reason [DeclarationRoots]' table is; [CELUnusableStepIDs]
// hands out a copy.
var celUnusableStepIDs = []string{"true", "false", "null", "in"}

// CELUnusableStepIDs returns a copy of the words no step may be named.
func CELUnusableStepIDs() []string { return slices.Clone(celUnusableStepIDs) }

// IsCELUnusableStepID reports whether name is one of the [CELUnusableStepIDs].
func IsCELUnusableStepID(name string) bool {
	return slices.Contains(celUnusableStepIDs, name)
}

// IsCELIdentifier reports whether s is a legal CEL identifier: a letter or
// underscore followed by letters, digits and underscores, ASCII only.
func IsCELIdentifier(s string) bool {
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

// A StepIDIssue is one step id that breaks a scope rule.
type StepIDIssue struct {
	// ID is the offending id; empty when the step has none.
	ID string

	// Field locates a step that has no id to name, as `steps[<index>]`. Set only
	// for a top-level step: a nested one has neither an id nor a stable index.
	Field string

	// Message states the problem and how to fix it.
	Message string
}

// maxStepIDWalkNodes bounds how many steps [StepIDIssues] visits in one
// workflow. The walk is iterative and a specification is already bounded by
// [CheckSpecSize] at every submit door, so this is the backstop for a direct
// caller, in the same spirit as [maxStructureWalkNodes].
const maxStepIDWalkNodes = 100_000

// stepIDFrame is one unit of work still owed by [StepIDIssues]: a node, a list
// of sibling steps, or the point where a `for_each` or `loop:` body ends and the
// ids it introduced stop being visible.
type stepIDFrame struct {
	node  *Node
	group []*Node
	exit  bool
	mark  int

	// top marks the workflow's own list of steps, whose members are located by
	// index; field is that location for a node taken from it.
	top   bool
	field string
}

// StepIDIssues reports every step id in wf's own steps, at any nesting, that
// breaks a scope rule. A `call:`'s callee is a separate namespace and is not
// entered; [CheckStepIDs] visits each workflow a specification embeds.
//
// The rules are the ones `flowfile.Validate` holds on source:
//
//   - an id is present;
//   - it is not a [DeclarationRoots], which it would hide;
//   - it is not a [CELUnusableStepIDs] word;
//   - it is a [IsCELIdentifier], so `steps.<id>` parses without index syntax;
//   - it is not already visible: used by an earlier step in the same namespace,
//     or by a step it is nested inside. Outputs of `parallel` branches and
//     `switch` bodies merge into the enclosing namespace, so they share it; a
//     `for_each` or `loop:` body's outputs do not escape, so two such bodies may
//     each use the same id.
//
// Issues come back in document order. The walk is explicit-stack rather than
// recursive, because how deeply steps nest is chosen by whoever built the
// specification.
func StepIDIssues(wf *Workflow) []StepIDIssue {
	var issues []StepIDIssue

	visible := make(map[string]struct{}, len(wf.GetSteps()))
	var log []string
	budget := maxStepIDWalkNodes
	queued, truncated := 0, false

	stack := []stepIDFrame{{group: wf.GetSteps(), top: true}}
	for len(stack) > 0 {
		last := len(stack) - 1
		f := stack[last]
		stack = stack[:last]

		switch {
		case f.exit:
			for _, id := range log[f.mark:] {
				delete(visible, id)
			}
			log = log[:f.mark]

		case f.node == nil:
			// Never queue more nodes than the walk will visit: a group of
			// millions of entries would otherwise be copied onto the stack before
			// the budget saw one of them.
			limit := len(f.group)
			if room := maxStepIDWalkNodes - queued; limit > room {
				limit, truncated = max(room, 0), true
			}
			for i := limit - 1; i >= 0; i-- {
				if f.group[i] != nil {
					child := stepIDFrame{node: f.group[i]}
					if f.top {
						child.field = fmt.Sprintf("steps[%d]", i)
					}
					stack = append(stack, child)
				}
			}
			queued += limit

		default:
			if budget--; budget < 0 {
				return append(issues, stepIDBudgetIssue())
			}

			node := f.node
			id := node.GetId()
			if issue, ok := stepIDIssue(id); ok {
				issue.Field = f.field
				issues = append(issues, issue)
			}
			if id != "" {
				if _, dup := visible[id]; dup {
					issues = append(issues, StepIDIssue{ID: id, Message: fmt.Sprintf(
						"duplicate id %q: another step already uses it in this namespace, or this step is "+
							"nested inside it; a step's outputs would silently replace the other's. "+
							"Ids are unique across a workflow's steps and the branches of a `parallel` or "+
							"`switch`; only separate `for_each` and `loop:` bodies may reuse one",
						id)})
				} else {
					visible[id] = struct{}{}
					log = append(log, id)
				}
			}

			switch kind := node.GetKind().(type) {
			case *Node_ForEach:
				stack = append(stack, stepIDFrame{exit: true, mark: len(log)},
					stepIDFrame{group: kind.ForEach.GetBody()})
			case *Node_Loop:
				stack = append(stack, stepIDFrame{exit: true, mark: len(log)},
					stepIDFrame{group: kind.Loop.GetBody()})
			case *Node_Parallel:
				branches := kind.Parallel.GetBranches()
				for i := len(branches) - 1; i >= 0; i-- {
					stack = append(stack, stepIDFrame{group: branches[i].GetSteps()})
				}
			case *Node_Switch:
				bodies := SwitchBodies(kind.Switch)
				for i := len(bodies) - 1; i >= 0; i-- {
					stack = append(stack, stepIDFrame{group: bodies[i]})
				}
			}
		}
	}

	if truncated {
		issues = append(issues, stepIDBudgetIssue())
	}

	return issues
}

// stepIDBudgetIssue is the refusal for a workflow holding more steps than the
// walk will visit.
func stepIDBudgetIssue() StepIDIssue {
	return StepIDIssue{Message: fmt.Sprintf(
		"the workflow holds more than %d steps, which is more than can be checked for "+
			"colliding ids; nothing further was checked", maxStepIDWalkNodes)}
}

// stepIDIssue is the per-id half of [StepIDIssues]: the rules that need only the
// id itself.
func stepIDIssue(id string) (StepIDIssue, bool) {
	switch {
	case id == "":
		return StepIDIssue{Message: "step has no id; every step needs an id so later steps can reference its outputs"}, true
	case IsDeclarationRoot(id):
		return StepIDIssue{ID: id, Message: "id " + ShadowsRootMessage("step", id)}, true
	case IsCELUnusableStepID(id):
		return StepIDIssue{ID: id, Message: fmt.Sprintf(
			"id %q is punctuation in CEL rather than a name, so ${%s.%s} cannot be parsed at all; choose another id",
			id, StepsRoot, id)}, true
	case !IsCELIdentifier(id):
		return StepIDIssue{ID: id, Message: fmt.Sprintf(
			"id %q is not a valid identifier, so ${%s.…} cannot be parsed; use letters, digits, and underscores, starting with a letter or underscore",
			id, id)}, true
	}
	return StepIDIssue{}, false
}

// ShadowsRootMessage renders the refusal for a name that would hide a root.
func ShadowsRootMessage(what, name string) string {
	return fmt.Sprintf(
		"%q is the root %s are named under, so a %s of that name would hide all of them; choose another %s name",
		name, rootHolds(name), what, what)
}

// rootHolds says what a root answers with, for [ShadowsRootMessage].
func rootHolds(root string) string {
	switch root {
	case StepsRoot:
		return "every step's outputs"
	case VarsRoot:
		return "the workflow's vars"
	case InputsRoot:
		return "the run's inputs"
	case RunRoot:
		return "the run's own address and starter identity"
	case TriggerRoot:
		return "how the run started"
	default:
		return "those values"
	}
}

// StepIDError is the refusal [CheckStepIDs] returns: the specification submitted
// breaks the step-id scope rules. Servers map it to an invalid-argument error.
type StepIDError struct {
	// Workflow is the name of the workflow the first issue was found in, which
	// differs from the submitted one when a `call:`'s callee holds it.
	Workflow string

	// Issues lists every violation found in that workflow, in document order.
	Issues []StepIDIssue
}

// Error names the first offending step id and how many more there are.
func (e *StepIDError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "workflow %q has an invalid step id", e.Workflow)
	if len(e.Issues) > 1 {
		fmt.Fprintf(&b, " (%d problems, first shown)", len(e.Issues))
	}
	first := e.Issues[0]
	if first.ID != "" {
		fmt.Fprintf(&b, ": step %q", first.ID)
	}
	b.WriteString(": " + first.Message)
	return b.String()
}

// CheckStepIDs refuses a specification, or any workflow it embeds through a
// `call:`, whose step ids break the scope rules in [StepIDIssues].
//
// The spec-side half of what `flowfile.Validate` holds on source, called from
// [BindRunInputs] so every submit path — the server's `RunWorkflow`, schedule
// creation, the webhook bridge, and `flow run local` — refuses the same
// specification in the same words (invariant 3). It runs at submit and never at
// resume: a run already started replays from its stored history.
func CheckStepIDs(wf *Workflow) error {
	for current, err := range specWorkflows(wf) {
		if err != nil {
			return err
		}
		if issues := StepIDIssues(current); len(issues) > 0 {
			return &StepIDError{Workflow: current.GetName(), Issues: issues}
		}
	}
	return nil
}
