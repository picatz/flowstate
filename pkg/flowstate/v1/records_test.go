package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func recordTypeOf(name string) *v1.Type { return &v1.Type{Kind: &v1.Type_Message{Message: name}} }

func recordField(name string, legacy v1.InputDeclaration_Type, t *v1.Type, required bool) *v1.InputDeclaration {
	return &v1.InputDeclaration{Name: name, Type: legacy, ValueType: t, Required: required}
}

func recordStringField(name string, required bool) *v1.InputDeclaration {
	return recordField(name, v1.InputDeclaration_TYPE_STRING, nil, required)
}

// recordOrderWorkflow declares Order{id, status, lines: list(Line)} and Line{sku,
// quantity} and takes an Order.
func recordOrderWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "orders",
		DeclaredTypes: []*v1.TypeDeclaration{
			{Name: "Line", Fields: []*v1.InputDeclaration{
				recordStringField("sku", true),
				recordField("quantity", v1.InputDeclaration_TYPE_INT, nil, true),
			}},
			{Name: "Order", Fields: []*v1.InputDeclaration{
				recordStringField("id", true),
				{Name: "status", Type: v1.InputDeclaration_TYPE_ENUM, Values: []string{"open", "paid"}},
				recordField("lines", v1.InputDeclaration_TYPE_LIST, &v1.Type{Kind: &v1.Type_List{List: recordTypeOf("Line")}}, false),
			}},
		},
		DeclaredInputs: []*v1.InputDeclaration{
			recordField("order", v1.InputDeclaration_TYPE_STRUCT, recordTypeOf("Order"), true),
		},
		Steps: []*v1.Node{{Id: "a", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
	}
}

func recordStr(s string) *expr.Value { return strLit(s) }

func recordLine(sku string, quantity int64) *expr.Value {
	return mapLit(recordStr("sku"), recordStr(sku), recordStr("quantity"), intLit(quantity))
}

func bindOrder(t *testing.T, order *expr.Value) error {
	t.Helper()

	_, err := v1.BindRunInputs(recordOrderWorkflow(), map[string]*v1.Value{"order": {Kind: &v1.Value_Literal{Literal: order}}})

	return err
}

func TestARecordInputIsCheckedFieldByField(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		order *expr.Value
		want  string // empty accepts
	}{
		{"every field", mapLit(recordStr("id"), recordStr("o-1"), recordStr("status"), recordStr("paid"),
			recordStr("lines"), listLit(recordLine("kb", 2))), ""},
		{"an optional field left out", mapLit(recordStr("id"), recordStr("o-1")), ""},
		{"a required field missing", mapLit(recordStr("status"), recordStr("open")), `no required field "id" of Order`},
		{"an undeclared field", mapLit(recordStr("id"), recordStr("o-1"), recordStr("coupon"), recordStr("x")),
			`a field "coupon" that Order does not declare`},
		{"a wrong scalar", mapLit(recordStr("id"), intLit(1)), "an integer at .id"},
		{"a value outside the enum", mapLit(recordStr("id"), recordStr("o"), recordStr("status"), recordStr("refunded")),
			"status"},
		{"a nested record's wrong value, with the path", mapLit(recordStr("id"), recordStr("o"),
			recordStr("lines"), listLit(recordLine("kb", 1), mapLit(recordStr("sku"), recordStr("kb"), recordStr("quantity"), recordStr("two")))),
			"a string at .lines[1].quantity"},
		{"a nested record's missing field", mapLit(recordStr("id"), recordStr("o"),
			recordStr("lines"), listLit(mapLit(recordStr("sku"), recordStr("kb")))),
			`no required field "quantity" of Line at .lines[0]`},
		{"a nested record's undeclared field", mapLit(recordStr("id"), recordStr("o"),
			recordStr("lines"), listLit(mapLit(recordStr("sku"), recordStr("kb"), recordStr("quantity"), intLit(1), recordStr("x"), intLit(1)))),
			`a field "x" that Line does not declare at .lines[0]`},
		{"a list where a record is", listLit(), "was given list"},
		{"a scalar where a record is", recordStr("x"), "was given string"},
		{"a non-string key", mapLit(intLit(1), recordStr("x")), "a record with an integer key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := bindOrder(t, test.order)
			if test.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

// An unresolved name fails closed when there is a table to resolve it in, and a
// check with no table keeps accepting what it cannot judge.
func TestARecordNameNobodyDeclaredFailsClosed(t *testing.T) {
	t.Parallel()

	input := recordField("order", v1.InputDeclaration_TYPE_STRUCT, recordTypeOf("Ghost"), true)
	value := &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(recordStr("id"), recordStr("o"))}}

	require.NoError(t, v1.CheckInputValue("order", input, value), "no table: accepted, as a message type always was")

	err := v1.CheckInputValueIn(v1.TypeTable{"Other": {Name: "Other"}}, "order", input, value)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Ghost")
}

func TestACycleInAHandBuiltSpecificationIsRefusedAtSubmit(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	wf.DeclaredTypes[0].Fields = append(wf.DeclaredTypes[0].Fields,
		recordField("order", v1.InputDeclaration_TYPE_STRUCT, recordTypeOf("Order"), false))

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"order": {Kind: &v1.Value_Literal{Literal: mapLit(recordStr("id"), recordStr("o"))}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Line -> Order -> Line")
}

func TestAnUndeclaredTypeInAHandBuiltSpecificationIsRefusedAtSubmit(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	wf.DeclaredTypes = wf.DeclaredTypes[:1]

	_, err := v1.BindRunInputs(wf, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Order")
}

func TestRecordDeclarationsRefuseWhatAFieldDoesNotCarry(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*v1.InputDeclaration){
		"default":   func(f *v1.InputDeclaration) { f.Default = v1.NewLiteral("x") },
		"example":   func(f *v1.InputDeclaration) { f.Example = v1.NewLiteral("x") },
		"sensitive": func(f *v1.InputDeclaration) { f.Sensitive = true },
		"must":      func(f *v1.InputDeclaration) { f.Must = new("this != ''") },
		"min_len":   func(f *v1.InputDeclaration) { f.MinLen = new(uint64(1)) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := recordOrderWorkflow()
			mutate(wf.DeclaredTypes[0].Fields[0])

			err := v1.CheckRecordDeclarations(wf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "`"+name+"`")
		})
	}
}

// A declared output of a record type is judged on what the run computed, with the
// same function and the same table.
func TestARecordOutputIsCheckedWhenComputed(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	output := &v1.OutputDeclaration{
		Name:      "receipt",
		Type:      v1.InputDeclaration_TYPE_STRUCT,
		ValueType: recordTypeOf("Order"),
	}

	table := v1.TypesOf(wf)
	good := &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(recordStr("id"), recordStr("o"))}}
	bad := &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(recordStr("nope"), recordStr("o"))}}

	require.NoError(t, v1.CheckOutputValueIn(table, output, good))
	err := v1.CheckOutputValueIn(table, output, bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `a field "nope" that Order does not declare`)
}

func TestARecordIsAMapToAnOlderReader(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "Order", v1.TypeString(recordTypeOf("Order")))
	assert.True(t, v1.CELType(recordTypeOf("Order")).IsExactType(v1.CELType(&v1.Type{Kind: &v1.Type_Dyn{Dyn: true}})))
}

func TestTheRecordTypeCountIsBounded(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	for range v1.MaxRecordFields {
		wf.DeclaredTypes[0].Fields = append(wf.DeclaredTypes[0].Fields, recordStringField("extra", false))
	}
	err := v1.CheckRecordDeclarations(wf)
	require.Error(t, err)
}
