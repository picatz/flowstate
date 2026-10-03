package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestTypeAssignable(t *testing.T) {
	t.Parallel()

	str := &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}
	integer := &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_INT}}
	dyn := &v1.Type{Kind: &v1.Type_Dyn{Dyn: true}}
	enum := &v1.Type{Kind: &v1.Type_Enum{Enum: true}}

	for name, tc := range map[string]struct {
		from, to *v1.Type
		want     bool
	}{
		"same scalar":             {str, str, true},
		"different scalar":        {str, integer, false},
		"anything to dyn":         {listOf(str), dyn, true},
		"nil to dyn":              {nil, dyn, true},
		"dyn to a scalar":         {dyn, str, false},
		"list(string) to dyn":     {listOf(str), listOf(dyn), true},
		"list(dyn) to string":     {listOf(dyn), listOf(str), false},
		"nested":                  {listOf(mapOf(str)), listOf(mapOf(dyn)), true},
		"nested the other way":    {listOf(mapOf(dyn)), listOf(mapOf(str)), false},
		"list to map":             {listOf(str), mapOf(str), false},
		"enum to string":          {enum, str, true},
		"string to enum":          {str, enum, false},
		"enum to enum":            {enum, enum, true},
		"message by name equals":  {&v1.Type{Kind: &v1.Type_Message{Message: "a.B"}}, &v1.Type{Kind: &v1.Type_Message{Message: "a.B"}}, true},
		"message by name differs": {&v1.Type{Kind: &v1.Type_Message{Message: "a.B"}}, &v1.Type{Kind: &v1.Type_Message{Message: "a.C"}}, false},
	} {
		require.Equal(t, tc.want, v1.TypeAssignable(tc.from, tc.to), name)
	}

	cyclic := &v1.Type{}
	cyclic.Kind = &v1.Type_List{List: cyclic}
	require.False(t, v1.TypeAssignable(cyclic, cyclic), "a type with no end is never proved compatible")
}
