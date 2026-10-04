package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `types:` block, from the file's side: what it compiles to, that it writes
// back exactly, and what a document can be wrong about on its own. Whether a
// value is a record is [v1.CheckInputValueIn]'s claim, tested in package v1.

// typesSource wraps a `types:` block and one input of the given type in the
// smallest workflow that holds them.
func typesSource(types, inputType string) string {
	return `edition: v2026.4
name: t
` + types + `inputs:
  order:
    type: ` + inputType + `
steps:
  - id: a
    log:
      message: hi
`
}

const orderTypes = `types:
  Line:
    description: One thing on an order.
    fields:
      sku:
        type: string
        required: true
      quantity:
        type: int
  Order:
    fields:
      id:
        type: string
        required: true
      status:
        type: enum
        values:
          - open
          - paid
      lines:
        type: list(Line)
      by_sku:
        type: map(string, Line)
`

func TestTypesCompileToDeclarationsAndWriteBack(t *testing.T) {
	t.Parallel()

	source := typesSource(orderTypes, "Order")

	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))

	require.Len(t, wf.GetDeclaredTypes(), 2)
	line, order := wf.GetDeclaredTypes()[0], wf.GetDeclaredTypes()[1]
	assert.Equal(t, "Line", line.GetName())
	assert.Equal(t, "One thing on an order.", line.GetDescription())
	assert.Equal(t, "Order", order.GetName())
	require.Len(t, order.GetFields(), 4)

	// A field naming a record carries the structural type and, for a reader that
	// predates records, the legacy projection older workers enforce.
	lines := order.GetFields()[2]
	assert.Equal(t, v1.InputDeclaration_TYPE_LIST, lines.GetType())
	assert.Equal(t, "Line", lines.GetValueType().GetList().GetMessage())
	assert.Equal(t, "Line", order.GetFields()[3].GetValueType().GetMap().GetValue().GetMessage())

	input := wf.GetDeclaredInputs()[0]
	assert.Equal(t, v1.InputDeclaration_TYPE_STRUCT, input.GetType())
	assert.Equal(t, "Order", input.GetValueType().GetMessage())

	// The compiled message has to satisfy the schema, or a server refuses what the
	// compiler accepted.
	require.NoError(t, v1.Validate(wf))

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, source, string(written))
}

func TestATypeMayBeNamedBeforeItIsDeclared(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(typesSource(`types:
  Order:
    fields:
      line:
        type: Line
  Line:
    fields:
      sku:
        type: string
`, "Order")))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))
}

func TestTypesRefuseWhatCannotRun(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		types     string
		inputType string
		want      string
	}{
		{
			name:      "an undeclared type",
			types:     "",
			inputType: "Order",
			want:      "Order",
		},
		{
			name: "a type referring to itself",
			types: `types:
  Node:
    fields:
      next:
        type: Node
`,
			inputType: "Node",
			want:      "Node refers to itself: Node -> Node",
		},
		{
			name: "a cycle through another type",
			types: `types:
  A:
    fields:
      b:
        type: list(B)
  B:
    fields:
      a:
        type: map(string, A)
`,
			inputType: "A",
			want:      "A -> B -> A",
		},
		{
			name: "a type with no fields",
			types: `types:
  Empty:
    description: nothing
`,
			inputType: "string",
			want:      `type "Empty" declares no fields`,
		},
		{
			name: "a field naming a type nobody declared",
			types: `types:
  Order:
    fields:
      line:
        type: Line
`,
			inputType: "string",
			want:      "Line",
		},
		{
			name: "a rule over the whole record",
			types: `types:
  Order:
    must: this.id != ""
    fields:
      id:
        type: string
`,
			inputType: "string",
			want:      "a rule over the whole record is not carried yet",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(typesSource(test.types, test.inputType)))
			var message string
			if err != nil {
				message = err.Error()
			} else {
				message = flowfile.Validate(wf).Error()
			}
			assert.Contains(t, message, test.want)
		})
	}
}

// What a field does not carry yet is refused with the reason, not parsed and
// ignored: a `must:` that nothing enforces reads as a promise.
func TestAFieldRefusesWhatItDoesNotCarryYet(t *testing.T) {
	t.Parallel()

	for _, key := range []string{
		"default: x", "example: x", "sensitive: true", "must: this != ''",
	} {
		t.Run(strings.SplitN(key, ":", 2)[0], func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(typesSource(`types:
  Order:
    fields:
      id:
        type: string
        `+key+`
`, "Order")))
			require.NoError(t, err)

			ds := flowfile.Validate(wf)
			require.NotEmpty(t, ds)
			assert.Contains(t, ds.Error(), "does not carry yet")
			assert.Contains(t, ds.Error(), "`"+strings.SplitN(key, ":", 2)[0]+"`")
		})
	}
}

func TestADuplicateTypeFieldIsReported(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(typesSource(`types:
  Order:
    fields:
      id:
        type: string
`, "Order")))
	require.NoError(t, err)

	// A repeated key cannot be written in a mapping, so the duplicate is built
	// into the compiled message, as a specification that never was a Flowfile can.
	order := wf.GetDeclaredTypes()[0]
	order.Fields = append(order.Fields, order.GetFields()[0])
	assert.Contains(t, flowfile.Validate(wf).Error(), `declares field "id" twice`)
}

func TestACallArgumentIsCheckedAgainstTheCalleesRecord(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", typesSource(orderTypes, "Order"))

	caller := func(order string) string {
		return `edition: v2026.4
name: caller
steps:
  - id: place
    call: ./callee.yaml
    with:
      order:
` + order
	}

	good, err := flowfile.ValidateSourceAt([]byte(caller("        id: o-1\n        lines:\n          - sku: kb\n            quantity: 1\n")), dir+"/caller.yaml")
	require.NoError(t, err)
	assert.Empty(t, good)

	bad, err := flowfile.ValidateSourceAt([]byte(caller("        id: o-1\n        coupon: x\n")), dir+"/caller.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, bad)
	assert.Contains(t, bad.Error(), `a field "coupon" that Order does not declare`)
}

func TestARecordDefaultIsCheckedWhereItIsWritten(t *testing.T) {
	t.Parallel()

	source := func(def string) string {
		return `edition: v2026.4
name: t
` + orderTypes + `inputs:
  order:
    type: Order
    default:
` + def + `steps:
  - id: a
    log:
      message: hi
`
	}

	wf, _, err := flowfile.Parse([]byte(source("      id: o-1\n      status: open\n")))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(wf))

	wf, _, err = flowfile.Parse([]byte(source("      id: o-1\n      coupon: x\n")))
	require.NoError(t, err)
	ds := flowfile.Validate(wf)
	require.NotEmpty(t, ds)
	assert.Contains(t, ds.Error(), `a field "coupon" that Order does not declare`)
}

// The file is the sender's, so the number of names and fields is bounded before
// anything is built from them, not after the schema sees the result.
func TestTypesAreBoundedBeforeAnythingIsBuilt(t *testing.T) {
	t.Parallel()

	var types strings.Builder
	types.WriteString("types:\n")
	for i := range v1.MaxRecordTypes + 1 {
		types.WriteString("  T" + strings.Repeat("a", i%5) + string(rune('A'+i%26)) + string(rune('a'+i/26)) + ":\n    fields:\n      x:\n        type: \"list(string)\"\n")
	}
	_, _, err := flowfile.Parse([]byte(typesSource(types.String(), "string")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the most a workflow declares is 64")

	var fields strings.Builder
	fields.WriteString("types:\n  Big:\n    fields:\n")
	for i := range v1.MaxRecordFields + 1 {
		fields.WriteString("      f" + strings.Repeat("a", i/26) + string(rune('a'+i%26)) + ":\n        type: string\n")
	}
	_, _, err = flowfile.Parse([]byte(typesSource(fields.String(), "string")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the most a record holds is 64")
}

// The checker reads the fields a record declares: a path the file writes is typed
// by its field, and one the closed record does not declare is refused with the
// fields it does, before a run.
func recordExpressionSource(expression string) string {
	return `edition: ` + flowfile.CurrentEdition + `
name: t
` + orderTypes + `inputs:
  order:
    type: Order
    required: true
steps:
  - id: a
    if: ${` + expression + `}
    log:
      message: hi
`
}

func TestARecordFieldIsTypedWhereItIsRead(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		expression string
		want       string // empty accepts
	}{
		{"a string field used as one", `inputs.order.id.startsWith("o-")`, ""},
		{"a nested path through a list's element is cel's", `inputs.order.lines.size() > 0`, ""},
		{"a presence test of a declared field", `has(inputs.order.id)`, ""},
		{"a string field used as a bool", `inputs.order.id`, "`if:` is a condition"},
		{"an int method on a string field", `inputs.order.id + 1 > 0`, "no matching overload"},
		{"a misspelled field", `inputs.order.idd == "x"`, `the record Order has no field "idd"`},
		{"a suggestion", `inputs.order.statu == "open"`, `Did you mean "status"?`},
		{"a presence test of an undeclared field", `has(inputs.order.coupon)`, `has no field "coupon"`},
		{"an optional read of a declared field", `inputs.order.?id.orValue("") == "x"`, ""},
		{"an optional read of an undeclared field", `inputs.order.?idd.hasValue()`, `the record Order has no field "idd"`},
		{"an index by a declared field", `inputs.order["id"] == "x"`, ""},
		{"an index by an empty key", `inputs.order[""] == "x"`, `has no field ""`},
		{"an index by an undeclared field", `inputs.order["idd"] == "x"`, `the record Order has no field "idd"`},
		{"a comprehension variable named like the root", `[inputs].exists(inputs, has(inputs.order.coupon))`, ""},
		{"a range read outside the shadowing comprehension", `inputs.order.zzz.exists(inputs, inputs.a)`, `has no field "zzz"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(recordExpressionSource(test.expression)))
			require.NoError(t, err)

			ds := flowfile.Validate(wf)
			if test.want == "" {
				assert.Empty(t, ds)
				return
			}
			require.NotEmpty(t, ds)
			assert.Contains(t, ds.Error(), test.want)
		})
	}
}

// A path two records deep is typed at its leaf, and an undeclared field in the
// middle names the record that does not declare it.
func TestANestedRecordPathIsTypedAtTheLeaf(t *testing.T) {
	t.Parallel()

	source := func(expression string) string {
		return `edition: ` + flowfile.CurrentEdition + `
name: t
types:
  Money:
    fields:
      cents:
        type: int
  Order:
    fields:
      total:
        type: Money
inputs:
  order:
    type: Order
    required: true
steps:
  - id: a
    if: ${` + expression + `}
    log:
      message: hi
`
	}

	wf, _, err := flowfile.Parse([]byte(source(`inputs.order.total.cents > 0`)))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(wf))

	wf, _, err = flowfile.Parse([]byte(source(`inputs.order.total.cents.startsWith("x")`)))
	require.NoError(t, err)
	assert.NotEmpty(t, flowfile.Validate(wf), "an int field has no startsWith")

	wf, _, err = flowfile.Parse([]byte(source(`inputs.order.total.cent > 0`)))
	require.NoError(t, err)
	assert.Contains(t, flowfile.Validate(wf).Error(), `the record Money has no field "cent"`)
}

// What a file controls about a refusal is bounded: how many chains are reported,
// how long a name is echoed, and that a name that long is not matched against the
// record's fields.
func TestUnknownFieldDiagnosticsAreBounded(t *testing.T) {
	t.Parallel()

	var reads []string
	for i := range 40 {
		reads = append(reads, fmt.Sprintf("has(inputs.order.missing%d)", i))
	}
	long := strings.Repeat("x", 5000)
	wf, _, err := flowfile.Parse([]byte(recordExpressionSource(strings.Join(reads, " || ") + " || has(inputs.order." + long + ")")))
	require.NoError(t, err)

	ds := flowfile.Validate(wf)
	require.NotEmpty(t, ds)
	assert.LessOrEqual(t, len(ds), 8, "one expression reports a bounded number of unknown fields")
	assert.Less(t, len(ds.Error()), 8*1024, "and a bounded amount of text")

	wf, _, err = flowfile.Parse([]byte(recordExpressionSource("has(inputs.order." + long + ")")))
	require.NoError(t, err)
	ds = flowfile.Validate(wf)
	require.NotEmpty(t, ds)
	assert.NotContains(t, ds.Error(), long, "a name the file controls is cut before it is echoed")
	assert.Contains(t, ds.Error(), "…")
	assert.NotContains(t, ds.Error(), "Did you mean", "a name that long is not matched against the record")
}

// A field carries the bounds an input does. The declaration is refused where a
// bound cannot apply, and a literal in the file that breaks one is refused with the
// path of the field, before a run.
func TestAFieldCarriesLengthAndItemBounds(t *testing.T) {
	t.Parallel()

	const bounded = `types:
  Order:
    fields:
      id:
        type: string
        min_len: 3
      tags:
        type: list(string)
        max_items: 2
`

	check := func(t *testing.T, source string) string {
		t.Helper()

		wf, _, err := flowfile.Parse([]byte(source))
		if err != nil {
			return err.Error()
		}

		return flowfile.Validate(wf).Error()
	}

	t.Run("a bound on a type it does not apply to", func(t *testing.T) {
		t.Parallel()

		got := check(t, typesSource(`types:
  Order:
    fields:
      id:
        type: int
        min_len: 3
`, "Order"))
		assert.Contains(t, got, `type "Order" field "id" declares a string constraint`)
	})

	t.Run("an example that breaks a bound", func(t *testing.T) {
		t.Parallel()

		got := check(t, `edition: `+flowfile.CurrentEdition+`
name: t
`+bounded+`inputs:
  order:
    type: Order
    example: {id: ab}
steps:
  - id: a
    log:
      message: hi
`)
		assert.Contains(t, got, "the field at .id must be at least 3 character(s) long; got 2")
	})

	t.Run("an example within the bounds", func(t *testing.T) {
		t.Parallel()

		got := check(t, `edition: `+flowfile.CurrentEdition+`
name: t
`+bounded+`inputs:
  order:
    type: Order
    example: {id: abc, tags: [a, b]}
steps:
  - id: a
    log:
      message: hi
`)
		assert.Empty(t, got)
	})

	t.Run("a list over its bound", func(t *testing.T) {
		t.Parallel()

		got := check(t, `edition: `+flowfile.CurrentEdition+`
name: t
`+bounded+`inputs:
  order:
    type: Order
    example: {id: abc, tags: [a, b, c]}
steps:
  - id: a
    log:
      message: hi
`)
		assert.Contains(t, got, "the field at .tags must have at most 2 item(s); got 3")
	})
}
