package conformance

import (
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// UseModuleDigest is the digest the specification records for its module. It is
// provenance: nothing a run reads depends on it.
var UseModuleDigest = v1.ContentDigestPrefix + strings.Repeat("ab", v1.ContentDigestHexLen/2)

// useModuleWorkflow declares what the compiler leaves of a file that writes
// `use: {ids: {path: ./lib/ids.yaml}}` and then `type: ids.Code` and a record with
// a field of that type: the module's declarations under their qualified names, the
// module recorded as provenance, and each use lowered to the plain rule
// ([FunctionMustExpansion]) exactly as a declaration written in the file itself is.
// `TestAModuleLowersToWhatBothDriversRun` in the flowfile package compiles the file
// and asserts the compiler emits this shape.
func useModuleWorkflow(name string) *v1.Workflow {
	wf := declares(name,
		[]*v1.InputDeclaration{
			{
				Name: "id", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
				Must: new(FunctionMustExpansion), TypeSource: new("ids.Code"),
			},
			{
				Name: "tag", Type: v1.InputDeclaration_TYPE_STRUCT,
				ValueType: &v1.Type{Kind: &v1.Type_Message{Message: "ids.Tag"}},
			},
		},
		nil,
		pins("show", `inputs.id == "abc-12"`)...,
	)
	wf.Modules = []*v1.Module{{Alias: "ids", Source: "./lib/ids.yaml", SourceDigest: UseModuleDigest}}
	wf.DeclaredTypes = []*v1.TypeDeclaration{
		{
			Name: "ids.Code", Base: v1.InputDeclaration_TYPE_STRING.Enum(),
			Must: new(FunctionMustExpansion), MustSource: new(functionMustSource),
		},
		{
			Name: "ids.Tag",
			Fields: []*v1.InputDeclaration{{
				Name: "code", Type: v1.InputDeclaration_TYPE_STRING, Required: true,
				Must: new(FunctionMustExpansion), TypeSource: new("ids.Code"),
			}},
		},
	}

	return wf
}
