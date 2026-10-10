package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAMacroVariableShadowsARootForCompletionAndHover(t *testing.T) {
	t.Parallel()

	clean, pos := splitCursor(t, resultEntrySource(`${steps.fan.results.map(steps, steps.|`))
	c := newClient(t)
	c.initialize()
	const uri = "file:///result-entry-shadow.yaml"
	c.open(uri, clean)

	var labels []string
	for _, item := range c.complete(uri, pos.Line, pos.Character).Items {
		labels = append(labels, item.Label)
	}
	assert.ElementsMatch(t, []string{"greet", "shout"}, labels, "the entry's keys, not the steps root's")

	const hoverURI = "file:///result-entry-shadow-hover.yaml"
	src := resultEntrySource(`${string(steps.fan.results.map(inputs, inputs.greet.value).size())}`)
	c.open(hoverURI, src)
	pos = positionOf(t, src, "inputs.greet.value", len("inputs.g"))
	got := c.hover(hoverURI, pos.Line, pos.Character)
	require.NotNil(t, got)
	assert.Contains(t, hoverText(got), "the outputs of the body step `greet`")
}

func TestCompletionAfterAStringHoldingBracketsInTheMacroArguments(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`${steps.fan.results.filter(r, r.greet.value == ")").map(q, q.|`,
		`${steps.fan.results.filter(r, r.greet.value == r")]").map(q, q.|`,
		`${steps.fan.results.filter(r, r.greet.value == "(").map(q, q.|`,
	} {
		clean, pos := splitCursor(t, resultEntrySource(expression))
		c := newClient(t)
		c.initialize()
		const uri = "file:///result-entry-strings.yaml"
		c.open(uri, clean)

		var labels []string
		for _, item := range c.complete(uri, pos.Line, pos.Character).Items {
			labels = append(labels, item.Label)
		}
		assert.ElementsMatch(t, []string{"greet", "shout"}, labels, expression)
	}
}
