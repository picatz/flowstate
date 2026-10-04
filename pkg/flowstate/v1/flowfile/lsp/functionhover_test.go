package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const functionHoverSource = `edition: ` + flowfile.CurrentEdition + `
name: function-hover
functions:
  slug:
    description: A title as it appears in a URL.
    params:
      title: string
      limit: int
    returns: string
    body: ${title.trim().lowerAscii().substring(0, limit)}
  plain:
    returns: int
    body: ${1}
inputs:
  title:
    type: string
    required: true
steps:
  - id: show
    log:
      message: ${slug(inputs.title, 10)} ${'slug(1)'} ${plain()}
`

// TestHoverOnACallToADeclaredFunction checks that a call to one of the file's
// own functions answers with the signature the definition declares and its
// description, and that a name inside a string literal does not.
func TestHoverOnACallToADeclaredFunction(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///function-hover.yaml"
	require.Empty(t, messages(c.open(uri, functionHoverSource).Diagnostics), "premise: the file compiles")

	hover := func(needle string, offset int) (string, string, bool) {
		pos := positionOf(t, functionHoverSource, needle, offset)
		got := c.hover(uri, pos.Line, pos.Character)
		if got == nil {
			return "", "", false
		}
		var span string
		if got.Range != nil {
			span = textInRange(functionHoverSource, *got.Range)
		}

		return hoverText(got), span, true
	}

	text, span, ok := hover("${slug(inputs", len("${sl"))
	require.True(t, ok, "no hover on the call")
	assert.Equal(t, "slug", span, "the range is the name under the cursor")
	assert.Contains(t, text, "`slug(title: string, limit: int) -> string`")
	assert.Contains(t, text, "A title as it appears in a URL.")
	assert.Contains(t, text, "replaced by the body when the file compiles")

	text, _, ok = hover("${plain()}", len("${pl"))
	require.True(t, ok)
	assert.Contains(t, text, "`plain() -> int`")
	assert.NotContains(t, text, "A title as it appears")

	// The same letters inside a string are text, not a call.
	text, _, ok = hover("${'slug(1)'}", len("${'sl"))
	if ok {
		assert.NotContains(t, text, "declared in this file")
	}
}
