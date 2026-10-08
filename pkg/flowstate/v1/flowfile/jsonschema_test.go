package flowfile_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// schemaOf parses source and returns the JSON the schema marshals to, so a test
// reads what a consumer reads rather than the Go maps that built it.
func schemaOf(t *testing.T, source string, render func(*v1.Workflow) map[string]any) map[string]any {
	t.Helper()

	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))

	encoded, err := json.Marshal(render(wf))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	return decoded
}

func at(t *testing.T, from any, path ...string) any {
	t.Helper()

	for _, key := range path {
		object, ok := from.(map[string]any)
		require.Truef(t, ok, "%v is not an object at %q", from, key)
		from, ok = object[key]
		require.Truef(t, ok, "no %q", key)
	}

	return from
}

const schemaSource = `edition: ` + flowfile.CurrentEdition + `
name: orders
types:
  Line:
    description: One thing on an order.
    fields:
      sku:
        type: string
        required: true
        min_len: 2
        max_len: 8
      quantity:
        type: int
        required: true
        must: this > 0
  Order:
    must: size(this.lines) > 0
    fields:
      status:
        type: enum
        values: [open, paid]
        default: open
      lines:
        type: list(Line)
        max_items: 20
      by_sku:
        type: map(string, Line)
      placed:
        type: timestamp
      ttl:
        type: duration
      blob:
        type: bytes
      token:
        type: string
        sensitive: true
inputs:
  order:
    type: Order
    required: true
    description: The order to fulfil.
  retries:
    type: int
    default: 3
    example: 5
steps:
  - id: done
    value: ${inputs.retries}
outputs:
  total:
    value: ${inputs.retries}
    type: int
`

func TestInputsJSONSchemaProjectsTheDeclarations(t *testing.T) {
	t.Parallel()

	schema := schemaOf(t, schemaSource, v1.InputsJSONSchema)

	assert.Equal(t, "https://json-schema.org/draft/2020-12/schema", schema["$schema"])
	assert.Equal(t, "orders inputs", schema["title"])
	assert.Equal(t, false, schema["additionalProperties"], "an undeclared input is refused at bind")
	assert.Equal(t, []any{"order"}, schema["required"])

	order := at(t, schema, "properties", "order")
	assert.Equal(t, "#/$defs/Order", at(t, order, "$ref"))
	assert.Equal(t, "The order to fulfil.", at(t, order, "description"), "a $ref may carry siblings in 2020-12")

	retries := at(t, schema, "properties", "retries")
	assert.Equal(t, "integer", at(t, retries, "type"))
	assert.EqualValues(t, 3, at(t, retries, "default"))
	assert.Equal(t, []any{float64(5)}, at(t, retries, "examples"))
	assert.NotContains(t, schema["required"], "retries")
}

func TestInputsJSONSchemaWritesEachRecordOnce(t *testing.T) {
	t.Parallel()

	schema := schemaOf(t, schemaSource, v1.InputsJSONSchema)
	defs := at(t, schema, "$defs").(map[string]any)
	assert.Len(t, defs, 2, "Order, and Line which it reaches through a list and a map")

	order := at(t, defs, "Order")
	assert.Equal(t, false, at(t, order, "additionalProperties"), "a record is closed")
	assert.Equal(t, "size(this.lines) > 0", at(t, order, "x-flowstate-must"))

	fields := at(t, order, "properties")
	assert.Equal(t, []any{"open", "paid"}, at(t, fields, "status", "enum"))
	assert.Equal(t, "open", at(t, fields, "status", "default"))
	assert.EqualValues(t, 20, at(t, fields, "lines", "maxItems"))
	assert.Equal(t, "array", at(t, fields, "lines", "type"))
	assert.Equal(t, "#/$defs/Line", at(t, fields, "lines", "items", "$ref"))
	assert.Equal(t, "#/$defs/Line", at(t, fields, "by_sku", "additionalProperties", "$ref"))
	assert.Equal(t, "date-time", at(t, fields, "placed", "format"))
	assert.Equal(t, "base64", at(t, fields, "blob", "contentEncoding"))
	assert.Contains(t, at(t, fields, "ttl", "description"), "90m")
	assert.Equal(t, true, at(t, fields, "token", "x-flowstate-sensitive"))

	line := at(t, defs, "Line")
	assert.ElementsMatch(t, []any{"sku", "quantity"}, at(t, line, "required").([]any))
	assert.EqualValues(t, 2, at(t, line, "properties", "sku", "minLength"))
	assert.EqualValues(t, 8, at(t, line, "properties", "sku", "maxLength"))
	assert.Equal(t, "this > 0", at(t, line, "properties", "quantity", "x-flowstate-must"))
	assert.Equal(t, "One thing on an order.", at(t, line, "description"))
}

func TestOutputsJSONSchemaRequiresEveryDeclaredOutput(t *testing.T) {
	t.Parallel()

	schema := schemaOf(t, schemaSource, v1.OutputsJSONSchema)

	assert.Equal(t, "orders outputs", schema["title"])
	assert.Equal(t, []any{"total"}, schema["required"])
	assert.Equal(t, "integer", at(t, schema, "properties", "total", "type"))
	assert.NotContains(t, schema, "$defs")
}

func TestAJSONSchemaOfAWorkflowWithNoInputsIsAnEmptyClosedObject(t *testing.T) {
	t.Parallel()

	schema := schemaOf(t, "edition: "+flowfile.CurrentEdition+"\nname: bare\nsteps:\n  - id: a\n    value: 1\n", v1.InputsJSONSchema)

	assert.Equal(t, map[string]any{}, schema["properties"])
	assert.Equal(t, false, schema["additionalProperties"])
	assert.NotContains(t, schema, "required")
}
