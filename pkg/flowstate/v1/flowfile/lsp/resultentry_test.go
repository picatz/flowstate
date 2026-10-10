package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The variable of a macro over a loop's `results` is one entry of it: a map of the
// body's step ids to their outputs. The editor says the keys before the compiler
// refuses a misspelling of one.
func resultEntrySource(expression string) string {
	return `edition: ` + flowfile.CurrentEdition + `
name: result-entry
inputs:
  names:
    type: list(string)
    required: true
steps:
  - id: fan
    for_each:
      items: ${inputs.names}
      as: n
      steps:
        - id: greet
          value: ${n}
        - id: shout
          value: ${n + "!"}
  - id: show
    log:
      message: ` + expression + `
`
}

func TestAMisspelledResultsKeyIsADiagnosticInTheEditor(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const uri = "file:///result-entry-typo.yaml"
	clean := c.open(uri, resultEntrySource(`${string(steps.fan.results.filter(r, r.greet.value == "a").size())}`))
	assert.Empty(t, messages(clean.Diagnostics))

	typo := c.change(uri, resultEntrySource(`${string(steps.fan.results.filter(r, r.greeet.value == "a").size())}`), 2)
	require.NotEmpty(t, typo.Diagnostics)
	assert.Contains(t, strings.Join(messages(typo.Diagnostics), "\n"), `the body of "fan" has no step "greeet"; it has "greet", "shout". Did you mean "greet"?`)
}

func TestHoverInsideAMacroOverResultsDescribesTheEntry(t *testing.T) {
	t.Parallel()

	src := resultEntrySource(`${string(steps.fan.results.filter(r, r.greet.value == "a").size())}`)
	c := newClient(t)
	c.initialize()

	const uri = "file:///result-entry-hover.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics), "premise: the file compiles")

	hover := func(needle string, offset int) (string, string) {
		pos := positionOf(t, src, needle, offset)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover at %q+%d", needle, offset)
		require.NotNil(t, got.Range)

		return hoverText(got), textInRange(src, *got.Range)
	}

	text, span := hover("r.greet.value ==", 0)
	assert.Equal(t, "r", span)
	assert.Contains(t, text, "One iteration of the loop `fan`")
	assert.Contains(t, text, "- `greet`: `value`", "the body's step ids, with the outputs they have")
	assert.Contains(t, text, "- `shout`: `value`")

	text, span = hover("r.greet.value ==", len("r.g"))
	assert.Equal(t, "r.greet", span)
	assert.Contains(t, text, "the outputs of the body step `greet`")

	text, span = hover("r.greet.value ==", len("r.greet.v"))
	assert.Equal(t, "r.greet.value", span)
	assert.Contains(t, text, "`r.greet.value`")
}

func TestCompletionInsideAMacroOverResultsOffersTheBodysSteps(t *testing.T) {
	t.Parallel()

	complete := func(t *testing.T, expression string) []string {
		t.Helper()

		clean, pos := splitCursor(t, resultEntrySource(expression))
		c := newClient(t)
		c.initialize()
		const uri = "file:///result-entry-complete.yaml"
		c.open(uri, clean)

		var labels []string
		for _, item := range c.complete(uri, pos.Line, pos.Character).Items {
			labels = append(labels, item.Label)
		}

		return labels
	}

	t.Run("the step ids after the variable", func(t *testing.T) {
		t.Parallel()

		labels := complete(t, `${steps.fan.results.filter(r, r.|`)
		assert.ElementsMatch(t, []string{"greet", "shout"}, labels)
	})

	t.Run("the outputs of one step", func(t *testing.T) {
		t.Parallel()

		labels := complete(t, `${steps.fan.results.filter(r, r.greet.|`)
		assert.Contains(t, labels, "value")
		assert.NotContains(t, labels, "greet")
	})

	t.Run("the variable of a macro chained after a filter", func(t *testing.T) {
		t.Parallel()

		labels := complete(t, `${steps.fan.results.filter(r, true).map(q, q.|`)
		assert.ElementsMatch(t, []string{"greet", "shout"}, labels)
	})

	t.Run("the variable of a macro over anything else offers nothing after the dot", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, complete(t, `${inputs.names.map(r, r.|`))
	})
}
