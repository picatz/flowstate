package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FunctionMustExpansion is the rule the Flowfile compiler stores for a `must:` that
// calls a declared function, written out here so both drivers are held to the
// plain CEL a run evaluates and not to the call.
//
// The file it comes from declares `isCode(s: string): bool` with the body
// `s.matches("^[a-z]{3}-[0-9]{2}$")` and writes `must: isCode(this)`. It is a
// constant for the reason [InterpolationSource] is, and
// `TestAFunctionIsCallableInEveryMust` in the flowfile package compiles that file
// and asserts the compiler emits exactly this.
const FunctionMustExpansion = `cel.bind(s, this, s.matches("^[a-z]{3}-[0-9]{2}$"))`

// functionMustSource is the call form, carried beside the expansion and read by
// nothing that runs.
const functionMustSource = "isCode(this)"

// functionMustWorkflow declares an input and a record field whose `must:` called a
// function, as the compiler stores them: the expansion in `must`, the call in
// `must_source`, and no declared function in the rule a run evaluates.
func functionMustWorkflow(name string) *v1.Workflow {
	wf := declares(name,
		[]*v1.InputDeclaration{
			{
				Name: "id", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
				Must: new(FunctionMustExpansion), MustSource: new(functionMustSource),
			},
			{
				Name: "tag", Type: v1.InputDeclaration_TYPE_STRUCT,
				ValueType: &v1.Type{Kind: &v1.Type_Message{Message: "Tag"}},
			},
		},
		nil,
		pins("show", `inputs.id == "abc-12"`)...,
	)
	wf.DeclaredTypes = []*v1.TypeDeclaration{{
		Name: "Tag",
		Fields: []*v1.InputDeclaration{{
			Name: "code", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
			Must: new(FunctionMustExpansion), MustSource: new(functionMustSource),
		}},
	}}

	return wf
}
