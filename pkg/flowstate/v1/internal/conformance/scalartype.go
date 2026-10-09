package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ScalarTypeOwnMust is the rule a use of the scalar type adds to the type's own.
const ScalarTypeOwnMust = `this != "abc-00"`

// ScalarTypeLoweredMust is the rule the Flowfile compiler stores for an input that
// is `type: Code` with `must: this != "abc-00"`, where `Code` is a string type whose
// rule is `isCode(this)` (see [FunctionMustExpansion]): the type's rule expanded,
// conjoined with the use's own. Written out so both drivers are held to the plain
// CEL a run evaluates and not to the type. `TestAScalarTypeLowersAtEachUse` in the
// flowfile package compiles that file and asserts the compiler emits exactly this.
const ScalarTypeLoweredMust = `(` + FunctionMustExpansion + `) && (` + ScalarTypeOwnMust + `)`

// scalarTypeWorkflow declares what the compiler leaves of a constrained scalar: no
// trace of the type in anything a run reads, only a string input whose rule is the
// type's and the use's together, a string input whose rule is the type's alone, and
// a record field that names the type. `type_source` is carried for `flow fmt` and
// is read by nothing that runs.
func scalarTypeWorkflow(name string) *v1.Workflow {
	wf := declares(name,
		[]*v1.InputDeclaration{
			{
				Name: "id", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
				Must: new(ScalarTypeLoweredMust), MustSource: new(ScalarTypeOwnMust), TypeSource: new("Code"),
			},
			{
				Name: "alias", Type: v1.InputDeclaration_TYPE_STRING,
				Must: new(FunctionMustExpansion), TypeSource: new("Code"),
			},
			{
				Name: "tag", Type: v1.InputDeclaration_TYPE_STRUCT,
				ValueType: &v1.Type{Kind: &v1.Type_Message{Message: "Tag"}},
			},
		},
		nil,
		pins("show", `inputs.id == "abc-12"`)...,
	)
	wf.DeclaredTypes = []*v1.TypeDeclaration{
		{
			Name: "Code", Base: v1.InputDeclaration_TYPE_STRING.Enum(),
			Must: new(FunctionMustExpansion), MustSource: new(functionMustSource),
		},
		{
			Name: "Tag",
			Fields: []*v1.InputDeclaration{{
				Name: "code", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
				Must: new(FunctionMustExpansion), TypeSource: new("Code"),
			}},
		},
	}

	return wf
}
