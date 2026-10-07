package conformance

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A step id is a name an expression writes as `steps.<id>`, and a specification
// that never was a Flowfile can carry one the compiler would have refused: a root's
// own name, a duplicate, something that is not a CEL identifier. These are the
// submissions both drivers must refuse before any step runs, in the same words,
// because both reach [v1.CheckStepIDs] through [v1.BindRunInputs] (#1430).

// stepIDWorkflow wraps steps in a workflow named for the case.
func stepIDWorkflow(name string, steps ...*v1.Node) *v1.Workflow {
	return &v1.Workflow{Name: name, Profile: v1.CurrentProfile, Steps: steps}
}

// forEachOver returns a `for_each` step holding body.
func forEachOver(id string, body ...*v1.Node) *v1.Node {
	return &v1.Node{
		Id:   id,
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Items: v1.NewExpr("[]"), Body: body}},
	}
}

// StepIDRefusalCases returns specifications whose step ids break the scope rules,
// which both drivers refuse at submit.
//
// Every root is covered by name, because the argument is about what a name hides
// and not about which root was written first: a step called `vars` beside a
// workflow `vars:` block resolved `${vars.value}` to the step and `${vars.region}`
// to the root, one spelling in two namespaces depending on whether the step
// happened to have an output of that name.
func StepIDRefusalCases() []Refusal {
	var cases []Refusal

	for _, root := range v1.DeclarationRoots() {
		cases = append(cases, Refusal{
			Name:     fmt.Sprintf("a step named %s would hide the root", root),
			Workflow: stepIDWorkflow("step-id-root-"+root, says(root, "hi")),
			Contains: fmt.Sprintf("step %q: id %q is the root", root, root),
		})
	}

	return append(cases,
		Refusal{
			Name: "a step named for a root inside a for_each body is refused too",
			Workflow: stepIDWorkflow("step-id-nested-root",
				forEachOver("each", says("vars", "hi"))),
			Contains: `step "vars": id "vars" is the root`,
		},
		Refusal{
			Name: "two steps sharing an id would overwrite each other's outputs",
			Workflow: stepIDWorkflow("step-id-duplicate",
				says("x", "first"), says("x", "second")),
			Contains: `duplicate id "x"`,
		},
		Refusal{
			Name: "a body step may not reuse the id of the step it is nested inside",
			Workflow: stepIDWorkflow("step-id-nested-duplicate",
				forEachOver("each", says("each", "hi"))),
			Contains: `duplicate id "each"`,
		},
		Refusal{
			Name: "a step may not reuse an id an earlier sibling already holds",
			Workflow: stepIDWorkflow("step-id-nested-shadows-earlier",
				says("x", "first"), forEachOver("each", says("x", "hi"))),
			Contains: `duplicate id "x"`,
		},
		Refusal{
			Name: "parallel branches share one output namespace",
			Workflow: stepIDWorkflow("step-id-parallel-duplicate", &v1.Node{
				Id: "fan",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{says("x", "left")}},
					{Steps: []*v1.Node{says("x", "right")}},
				}}},
			}),
			Contains: `duplicate id "x"`,
		},
		Refusal{
			Name: "an id that starts with a digit cannot be written as steps.<id>",
			Workflow: stepIDWorkflow("step-id-leading-digit",
				says("1a", "hi")),
			Contains: `id "1a" is not a valid identifier`,
		},
		Refusal{
			Name: "an id with a dash would need index syntax",
			Workflow: stepIDWorkflow("step-id-dash",
				says("a-b", "hi")),
			Contains: `id "a-b" is not a valid identifier`,
		},
		Refusal{
			Name: "an id the lexer reads as an operator cannot be parsed at all",
			Workflow: stepIDWorkflow("step-id-lexer-word",
				says("in", "hi")),
			Contains: `id "in" is punctuation in CEL`,
		},
		Refusal{
			Name: "a callee is refused for its own ids, naming the callee",
			Workflow: stepIDWorkflow("step-id-caller",
				callNode("provision", stepIDWorkflow("step-id-callee", says("vars", "hi")), nil)),
			Contains: `workflow "step-id-callee" has an invalid step id: step "vars"`,
		},
	)
}
