package flowstatev1_test

import (
	"errors"
	"fmt"
	"slices"
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

// A field carries the bounds an input does, judged by the same functions: the
// declaration is refused where no value could satisfy it, and a value that breaks one
// is refused with the path of the field.
func TestARecordFieldCarriesLengthAndItemBounds(t *testing.T) {
	t.Parallel()

	declare := func(mutate func(id, lines *v1.InputDeclaration)) *v1.Workflow {
		wf := recordOrderWorkflow()
		fields := wf.DeclaredTypes[1].Fields // Order
		named := func(name string) *v1.InputDeclaration {
			return fields[slices.IndexFunc(fields, func(f *v1.InputDeclaration) bool { return f.GetName() == name })]
		}
		mutate(named("id"), named("lines"))

		return wf
	}

	t.Run("a bound on a field that cannot hold it is refused", func(t *testing.T) {
		t.Parallel()

		err := v1.CheckRecordDeclarations(declare(func(_, lines *v1.InputDeclaration) { lines.MinLen = new(uint64(1)) }))
		require.Error(t, err)
		assert.Contains(t, err.Error(), `type "Order" field "lines" declares a string constraint`)
		assert.Contains(t, err.Error(), "those apply only to a string field")
	})

	t.Run("a bound no value could satisfy is refused", func(t *testing.T) {
		t.Parallel()

		err := v1.CheckRecordDeclarations(declare(func(id, _ *v1.InputDeclaration) {
			id.MinLen, id.MaxLen = new(uint64(5)), new(uint64(2))
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), `type "Order" field "id" min_len (5) is greater than max_len (2)`)
	})

	t.Run("a bound past what an input binds is not unsatisfiable on a field", func(t *testing.T) {
		t.Parallel()

		// An output carries no element ceiling, so only an input's own check refuses it.
		require.NoError(t, v1.CheckRecordDeclarations(declare(func(_, lines *v1.InputDeclaration) {
			lines.MinItems = new(uint64(20000))
		})))
	})

	wf := declare(func(id, lines *v1.InputDeclaration) {
		id.MinLen, id.MaxLen = new(uint64(2)), new(uint64(4))
		lines.MinItems, lines.MaxItems = new(uint64(1)), new(uint64(2))
	})
	require.NoError(t, v1.CheckRecordDeclarations(wf))
	table := v1.TypesOf(wf)

	order := func(id string, lines int) *v1.Value {
		items := make([]*expr.Value, lines)
		for i := range items {
			items[i] = recordLine("k", 1)
		}

		return &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(
			recordStr("id"), recordStr(id), recordStr("status"), recordStr("open"),
			recordStr("lines"), &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: items}}},
		)}}
	}
	input := &v1.InputDeclaration{Name: "order", Type: v1.InputDeclaration_TYPE_STRUCT, ValueType: recordTypeOf("Order")}

	for _, test := range []struct {
		name string
		id   string
		n    int
		want string // empty accepts
	}{
		{"within every bound", "abc", 2, ""},
		{"at the lower bound", "ab", 1, ""},
		{"a string under its minimum", "a", 1, "the field at .id must be at least 2 character(s) long; got 1"},
		{"a string over its maximum", "abcde", 1, "the field at .id must be at most 4 character(s) long; got 5"},
		{"a list under its minimum", "abc", 0, "the field at .lines must have at least 1 item(s); got 0"},
		{"a list over its maximum", "abc", 3, "the field at .lines must have at most 2 item(s); got 3"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := v1.CheckInputValueIn(table, "order", input, order(test.id, test.n))
			if test.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
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

// A sensitive output's refusal is the run's failure text, so what the sender put
// in a key or an enum value must not be echoed back in it.
func TestASensitiveRecordOutputWithholdsWhatItWasGiven(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	output := &v1.OutputDeclaration{
		Name: "receipt", Type: v1.InputDeclaration_TYPE_STRUCT,
		ValueType: recordTypeOf("Order"), Sensitive: true,
	}
	table := v1.TypesOf(wf)

	for name, literal := range map[string]*expr.Value{
		"an undeclared key": mapLit(recordStr("id"), recordStr("o"), recordStr("hunter2"), recordStr("x")),
		"an enum value":     mapLit(recordStr("id"), recordStr("o"), recordStr("status"), recordStr("hunter2")),
	} {
		err := v1.CheckOutputValueIn(table, output, &v1.Value{Kind: &v1.Value_Literal{Literal: literal}})
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "hunter2", name)
	}
}

func TestASensitiveOutputWithholdsAMapKeyInThePath(t *testing.T) {
	t.Parallel()

	output := &v1.OutputDeclaration{
		Name: "m", Type: v1.InputDeclaration_TYPE_STRUCT, Sensitive: true,
		ValueType: &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_INT}}}}},
	}
	literal := mapLit(recordStr("hunter2"), recordStr("not an int"))

	err := v1.CheckOutputValueIn(nil, output, &v1.Value{Kind: &v1.Value_Literal{Literal: literal}})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "hunter2")
}

// A chain of records longer than a value walk follows is refused where it is
// declared, and a walk that reaches the limit anyway refuses instead of
// accepting what it stopped judging.
func TestADeepRecordChainIsRefused(t *testing.T) {
	t.Parallel()

	chain := func(n int) *v1.Workflow {
		wf := &v1.Workflow{}
		for i := range n {
			d := &v1.TypeDeclaration{Name: fmt.Sprintf("T%d", i)}
			if i+1 < n {
				d.Fields = []*v1.InputDeclaration{recordField("x", v1.InputDeclaration_TYPE_STRUCT, recordTypeOf(fmt.Sprintf("T%d", i+1)), false)}
			} else {
				d.Fields = []*v1.InputDeclaration{recordStringField("leaf", false)}
			}
			wf.DeclaredTypes = append(wf.DeclaredTypes, d)
		}
		return wf
	}

	require.NoError(t, v1.CheckRecordDeclarations(chain(10)))

	err := v1.CheckRecordDeclarations(chain(40))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "levels deep")

	// A hand-built value walk past the limit is an error, not an acceptance.
	literal := mapLit(recordStr("leaf"), recordStr("ok"))
	for range 40 {
		literal = mapLit(recordStr("x"), literal)
	}
	deep := chain(40)
	err = v1.CheckInputValueIn(v1.TypesOf(deep), "t",
		recordField("t", v1.InputDeclaration_TYPE_STRUCT, recordTypeOf("T0"), true),
		&v1.Value{Kind: &v1.Value_Literal{Literal: literal}})
	require.Error(t, err)
}

func TestTheUnknownKeyAReportNamesIsTheFirstWritten(t *testing.T) {
	t.Parallel()

	for range 20 {
		err := bindOrder(t, mapLit(recordStr("id"), recordStr("o"), recordStr("zz"), recordStr("x"), recordStr("aa"), recordStr("x")))
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"zz"`)
	}
}

func TestTheDeclaredTypeCountIsBounded(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{}
	for i := range v1.MaxRecordTypes + 1 {
		wf.DeclaredTypes = append(wf.DeclaredTypes, &v1.TypeDeclaration{
			Name: fmt.Sprintf("T%d", i), Fields: []*v1.InputDeclaration{recordStringField("x", false)},
		})
	}

	err := v1.CheckRecordDeclarations(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "most a workflow declares")
}

func TestANestedRecordMismatchClaimsNoOutermostTypes(t *testing.T) {
	t.Parallel()

	err := bindOrder(t, mapLit(recordStr("id"), intLit(1)))
	require.Error(t, err)

	var invalid *v1.InputError
	require.True(t, errors.As(err, &invalid))
	assert.Empty(t, invalid.Declared, "a mismatch inside a record compared no outermost types")
	assert.Empty(t, invalid.Got)
}

// A rule on a field binds `this` to the field; a rule on the type binds it to the
// record. A value is held to every rule of every record it holds, nested in a list
// too, and the first one that does not hold names its path.
func TestRecordRulesAreEvaluatedOverAValue(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	line := wf.DeclaredTypes[0]
	order := wf.DeclaredTypes[1]
	named := func(fields []*v1.InputDeclaration, name string) *v1.InputDeclaration {
		return fields[slices.IndexFunc(fields, func(f *v1.InputDeclaration) bool { return f.GetName() == name })]
	}
	named(line.Fields, "quantity").Must = new("this > 0")
	order.Must = new("this.id != 'banned'")
	require.NoError(t, v1.CheckRecordDeclarations(wf))
	table := v1.TypesOf(wf)
	orderType := recordTypeOf("Order")

	build := func(id string, quantity int64) *v1.Value {
		return &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(
			recordStr("id"), recordStr(id), recordStr("status"), recordStr("open"),
			recordStr("lines"), &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{
				Values: []*expr.Value{recordLine("k", 1), recordLine("k", quantity)},
			}}},
		)}}
	}

	run := func(value *v1.Value) error {
		return v1.CheckRecordRules(table, "", "input", "order", false, orderType, value)
	}

	require.NoError(t, run(build("o-1", 3)))

	err := run(build("o-1", 0))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input \"order\": the field at .lines[1].quantity must satisfy `this > 0`; got 0")

	err = run(build("banned", 3))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the record Order must satisfy `this.id != 'banned'`")

	t.Run("the same refusal reaches submit", func(t *testing.T) {
		t.Parallel()

		_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"order": build("o-1", 0)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must satisfy `this > 0`")
	})

	t.Run("a sensitive declaration keeps the value out of the refusal", func(t *testing.T) {
		t.Parallel()

		err := v1.CheckRecordRules(table, "", "input", "order", true, orderType, build("o-1", 0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must satisfy `this > 0`")
		assert.NotContains(t, err.Error(), "got")
	})

	t.Run("a table with no rule, and no table, hold a value to nothing", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, v1.CheckRecordRules(nil, "", "input", "order", false, orderType, build("banned", 0)))
		require.NoError(t, v1.CheckRecordRules(table, "", "input", "order", false, &v1.Type{}, build("banned", 0)))
	})

	t.Run("a value that would take more evaluations than the bound is refused", func(t *testing.T) {
		t.Parallel()

		lines := make([]*expr.Value, v1.MaxRuleEvaluations+1)
		for i := range lines {
			lines[i] = recordLine("k", 1)
		}
		value := &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(
			recordStr("id"), recordStr("o"), recordStr("status"), recordStr("open"),
			recordStr("lines"), &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: lines}}},
		)}}

		err := run(value)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "`must:` rules apply to this value")
	})
}

// A rule that does not compile, or reads the clock, is a defect in the declaration.
func TestARecordRuleThatDoesNotCompileIsRefused(t *testing.T) {
	t.Parallel()

	for name, must := range map[string]string{
		"a syntax error":  "this >",
		"the clock":       "this != string(now)",
		"an unknown name": "other > 1",
		"not a predicate": "this + 1",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := recordOrderWorkflow()
			wf.DeclaredTypes[0].Fields[1].Must = new(must)
			require.Error(t, v1.CheckRecordDeclarations(wf))
		})
	}
}

func TestRecordRuleRefusalKeepsASensitiveValueAndKeyOut(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	line := wf.DeclaredTypes[0]
	line.Fields[slices.IndexFunc(line.Fields, func(f *v1.InputDeclaration) bool { return f.GetName() == "quantity" })].Must = new("this > 0")
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	byKey := &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: recordTypeOf("Line")}}}
	value := &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(recordStr("secret-key"), recordLine("k", 0))}}

	for _, sensitive := range []bool{false, true} {
		err := v1.CheckRecordRules(v1.TypesOf(wf), "", "input", "lines", sensitive, byKey, value)
		require.Error(t, err)
		if sensitive {
			assert.NotContains(t, err.Error(), "secret-key")
			assert.Contains(t, err.Error(), "[*].quantity")
		} else {
			assert.Contains(t, err.Error(), ".secret-key.quantity")
		}
	}
}

func TestRecordRuleEvaluationErrorKeepsASensitiveValueOut(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	order := wf.DeclaredTypes[1]
	order.Must = new("timestamp(this.id) > timestamp('2020-01-01T00:00:00Z')")
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	value := &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(recordStr("id"), recordStr("hunter2"), recordStr("status"), recordStr("open"),
		recordStr("lines"), &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{}}})}}

	for _, sensitive := range []bool{false, true} {
		err := v1.CheckRecordRules(v1.TypesOf(wf), "", "output", "o", sensitive, recordTypeOf("Order"), value)
		require.Error(t, err)
		if sensitive {
			assert.NotContains(t, err.Error(), "hunter2")
		}
	}
}

func TestALiteralOutputThatBreaksATypeRuleIsRefusedAtSubmit(t *testing.T) {
	t.Parallel()

	wf := recordOrderWorkflow()
	wf.DeclaredTypes[1].Must = new("this.id != 'banned'")
	wf.DeclaredInputs = nil
	wf.DeclaredOutputs = []*v1.OutputDeclaration{{
		Name: "answer", Type: v1.InputDeclaration_TYPE_STRUCT, ValueType: recordTypeOf("Order"),
		Value: &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(
			recordStr("id"), recordStr("banned"), recordStr("status"), recordStr("open"),
		)}},
	}}
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	_, err := v1.BindRunInputs(wf, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the record Order must satisfy")

	wf.DeclaredOutputs[0].Value = &v1.Value{Kind: &v1.Value_Literal{Literal: mapLit(
		recordStr("id"), recordStr("fine"), recordStr("status"), recordStr("open"),
	)}}
	_, err = v1.BindRunInputs(wf, nil)
	require.NoError(t, err)
}
