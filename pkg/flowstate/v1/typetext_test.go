package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestDeclarationTypeTextSpellsWhatTheAuthorWrote pins that the structural type
// wins over the legacy enum it was written beside, so a hover or diagnostic
// names `list(string)` rather than `list`.
func TestDeclarationTypeTextSpellsWhatTheAuthorWrote(t *testing.T) {
	t.Parallel()

	list := &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}}}

	for name, test := range map[string]struct {
		input *v1.InputDeclaration
		want  string
	}{
		"typed list":     {&v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_LIST, ValueType: list}, "list(string)"},
		"legacy list":    {&v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_LIST}, "list(dyn)"},
		"legacy struct":  {&v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_STRUCT}, "map(string, dyn)"},
		"legacy float":   {&v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_FLOAT}, "double"},
		"scalar":         {&v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_STRING}, "string"},
		"no type at all": {&v1.InputDeclaration{}, v1.DeclaredTypeName(v1.InputDeclaration_TYPE_UNSPECIFIED)},
		"enum":           {&v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_ENUM}, "enum"},
	} {
		require.Equal(t, test.want, test.input.TypeText(), name)
	}

	out := &v1.OutputDeclaration{Type: v1.InputDeclaration_TYPE_LIST, ValueType: list}
	require.Equal(t, "list(string)", out.TypeText())
}
