package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func strLit(s string) *expr.Value { return &expr.Value{Kind: &expr.Value_StringValue{StringValue: s}} }
func intLit(i int64) *expr.Value  { return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: i}} }
func listLit(vs ...*expr.Value) *expr.Value {
	return &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: vs}}}
}
func mapLit(kv ...*expr.Value) *expr.Value {
	m := &expr.MapValue{}
	for i := 0; i < len(kv); i += 2 {
		m.Entries = append(m.Entries, &expr.MapValue_Entry{Key: kv[i], Value: kv[i+1]})
	}
	return &expr.Value{Kind: &expr.Value_MapValue{MapValue: m}}
}

func typedDeclaration(legacy v1.InputDeclaration_Type, t *v1.Type) *v1.InputDeclaration {
	return &v1.InputDeclaration{Name: "xs", Type: legacy, ValueType: t}
}

func listOf(elem *v1.Type) *v1.Type { return &v1.Type{Kind: &v1.Type_List{List: elem}} }
func mapOf(value *v1.Type) *v1.Type {
	return &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: value}}}
}

// TestAStructuralTypeJudgesWhatAContainerHolds holds the container half of the
// declared-type rule: the legacy enum accepted any list for a list, and a
// structural type must not.
func TestAStructuralTypeJudgesWhatAContainerHolds(t *testing.T) {
	t.Parallel()

	str := &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}
	integer := &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_INT}}

	for name, tc := range map[string]struct {
		legacy  v1.InputDeclaration_Type
		typ     *v1.Type
		value   *expr.Value
		wantErr string
	}{
		"list of strings holds strings": {v1.InputDeclaration_TYPE_LIST, listOf(str), listLit(strLit("a")), ""},
		"list of strings, an int":       {v1.InputDeclaration_TYPE_LIST, listOf(str), listLit(strLit("a"), intLit(1)), "list(string) but was given an integer at [1]"},
		"nested list":                   {v1.InputDeclaration_TYPE_LIST, listOf(listOf(integer)), listLit(listLit(intLit(1)), listLit(strLit("x"))), "a string at [1][0]"},
		"map of ints":                   {v1.InputDeclaration_TYPE_STRUCT, mapOf(integer), mapLit(strLit("a"), intLit(1)), ""},
		"map value of the wrong type":   {v1.InputDeclaration_TYPE_STRUCT, mapOf(integer), mapLit(strLit("a"), strLit("x")), "a string at .a"},
		"map with an int key":           {v1.InputDeclaration_TYPE_STRUCT, mapOf(integer), mapLit(intLit(1), intLit(1)), "an integer key"},
		"list of dyn holds anything":    {v1.InputDeclaration_TYPE_LIST, listOf(&v1.Type{Kind: &v1.Type_Dyn{Dyn: true}}), listLit(intLit(1), strLit("a")), ""},
		"the legacy list is unchanged":  {v1.InputDeclaration_TYPE_LIST, nil, listLit(intLit(1), strLit("a")), ""},
	} {
		err := v1.CheckInputValue("xs", typedDeclaration(tc.legacy, tc.typ),
			&v1.Value{Kind: &v1.Value_Literal{Literal: tc.value}})
		if tc.wantErr == "" {
			require.NoError(t, err, name)
			continue
		}
		require.ErrorContains(t, err, tc.wantErr, name)
	}
}

// TestACyclicTypeEndsTheLiteralWalk is the bound: a hand-built type that points
// back at itself must not spend the stack.
func TestACyclicTypeEndsTheLiteralWalk(t *testing.T) {
	t.Parallel()

	cyclic := &v1.Type{}
	cyclic.Kind = &v1.Type_List{List: cyclic}
	deep := listLit()
	for range v1.MaxStructureDepth * 2 {
		deep = listLit(deep)
	}
	require.NotPanics(t, func() {
		_ = v1.CheckInputValue("xs", typedDeclaration(v1.InputDeclaration_TYPE_LIST, cyclic),
			&v1.Value{Kind: &v1.Value_Literal{Literal: deep}})
	})
}

// TestAWrongKindNamesTheDeclaredContainerType keeps a refusal's spelling aligned
// with the declaration: a `list(string)` input given a scalar is reported as
// `list(string)`, the text the author wrote, not as the legacy word `list`.
func TestAWrongKindNamesTheDeclaredContainerType(t *testing.T) {
	t.Parallel()

	str := &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}

	err := v1.CheckInputValue("xs", typedDeclaration(v1.InputDeclaration_TYPE_LIST, listOf(str)),
		&v1.Value{Kind: &v1.Value_Literal{Literal: intLit(3)}})
	require.ErrorContains(t, err, "declared list(string)")
}
