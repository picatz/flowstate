package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const recordHoverSource = `edition: ` + flowfile.CurrentEdition + `
name: record-hover
types:
  Money:
    fields:
      cents:
        type: int
        required: true
        description: Minor units.
  Order:
    description: What a customer bought.
    fields:
      id:
        type: string
        required: true
        description: The order's number.
      total:
        type: Money
        required: true
      note:
        type: string
inputs:
  order:
    type: Order
    required: true
steps:
  - id: show
    log:
      message: ${string(inputs.order.total.cents)} ${inputs.order.id}
`

// TestHoverOnARecordInputAndItsFields checks that each segment of an
// `inputs.<name>.<field>` chain answers for itself: the input with the record's
// fields, a field with its type and description, and a nested record's field with
// the record that holds it.
func TestHoverOnARecordInputAndItsFields(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///record-hover.yaml"
	require.Empty(t, messages(c.open(uri, recordHoverSource).Diagnostics), "premise: the file compiles")

	hover := func(needle string, offset int) (string, string) {
		pos := positionOf(t, recordHoverSource, needle, offset)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover at %q+%d", needle, offset)
		require.NotNil(t, got.Range)

		return hoverText(got), textInRange(recordHoverSource, *got.Range)
	}

	text, span := hover("inputs.order.id", len("inputs.o"))
	assert.Equal(t, "inputs.order", span, "the range stops at the segment under the cursor")
	assert.Contains(t, text, "`Order`")
	assert.Contains(t, text, "required")
	assert.Contains(t, text, "What a customer bought.")
	assert.Contains(t, text, "- `id`: `string`")
	assert.Contains(t, text, "- `total`: `Money`")
	assert.Contains(t, text, "- `note`: `string` (optional)")

	text, span = hover("inputs.order.id", len("inputs.order.i"))
	assert.Equal(t, "inputs.order.id", span)
	assert.Contains(t, text, "`order.id`** · `string` · required")
	assert.Contains(t, text, "A field of the record `Order`.")
	assert.Contains(t, text, "The order's number.")

	text, _ = hover("inputs.order.total.cents", len("inputs.order.total.c"))
	assert.Contains(t, text, "`order.total.cents`** · `int`")
	assert.Contains(t, text, "A field of the record `Money`.", "the record that holds the field, not the input's")
	assert.Contains(t, text, "Minor units.")

	text, _ = hover("inputs.order.total.cents", len("inputs.order.t"))
	assert.Contains(t, text, "- `cents`: `int`", "a record-typed field lists the fields it holds")
}

// TestHoverOnAFieldTheRecordDoesNotDeclareIsQuiet checks that a misspelled field
// gets no description: the diagnostic names what the record does declare, and a
// popup describing a field that is not there would contradict it.
func TestHoverOnAFieldTheRecordDoesNotDeclareIsQuiet(t *testing.T) {
	t.Parallel()

	src := recordHoverSourceWith("${inputs.order.idd}")
	c := newClient(t)
	c.initialize()
	const uri = "file:///record-hover-typo.yaml"
	c.open(uri, src)

	pos := positionOf(t, src, "inputs.order.idd", len("inputs.order.i"))
	assert.Nil(t, c.hover(uri, pos.Line, pos.Character))
}

func recordHoverSourceWith(message string) string {
	const marker = "${string(inputs.order.total.cents)} ${inputs.order.id}"
	return strings.Replace(recordHoverSource, marker, message, 1)
}

const scalarHoverSource = `edition: ` + flowfile.CurrentEdition + `
name: scalar-hover
types:
  Code:
    description: A three-letter, two-digit code.
    type: string
    must: size(this) == 6
    example: abc-12
inputs:
  id:
    type: Code
    required: true
    must: this != "abc-00"
steps:
  - id: show
    log:
      message: ${inputs.id}
`

// TestHoverOnAnInputOfAScalarTypeNamesTheTypeItsBaseAndItsRule checks that a use of a
// constrained scalar reads as the type the author wrote, not the lowered base alone:
// the name with the base it is held to, the use's own rule and not the conjoined
// expansion, and the type's description, rule and example.
func TestHoverOnAnInputOfAScalarTypeNamesTheTypeItsBaseAndItsRule(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///scalar-hover.yaml"
	require.Empty(t, messages(c.open(uri, scalarHoverSource).Diagnostics), "premise: the file compiles")

	pos := positionOf(t, scalarHoverSource, "inputs.id", len("inputs.i"))
	got := c.hover(uri, pos.Line, pos.Character)
	require.NotNil(t, got)
	text := hoverText(got)

	assert.Contains(t, text, "`id`** · `Code` (`string`) · required")
	assert.Contains(t, text, "Must satisfy `this != \"abc-00\"`.", "the use's own rule as written")
	assert.NotContains(t, text, "&&", "the conjoined rule the run evaluates is not what the author wrote")
	assert.Contains(t, text, "The scalar type `Code`: A three-letter, two-digit code.")
	assert.Contains(t, text, "A `string` that must satisfy `size(this) == 6`.")
	assert.Contains(t, text, "Example: `\"abc-12\"`.")
}
