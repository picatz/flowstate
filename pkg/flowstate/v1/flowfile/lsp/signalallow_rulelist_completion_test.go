package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// An expression inside a rule of the rule list keeps the rule's own scope: only
// a scalar `allow:` is the predicate.
func TestARuleListExpressionKeepsItsOwnScope(t *testing.T) {
	const src = `name: c
steps:
  - id: gate
    wait_for_signal:
      name: approved
signals:
  approved:
    allow:
      - subject: ${PLACEHOLDER
        claims:
          team: x
edition: v2026.4
`

	c := newClient(t)
	c.initialize()

	for _, typed := range []string{"", "inputs."} {
		text, pos := splitCursor(t, strings.Replace(src, "PLACEHOLDER", typed+"|", 1))
		uri := "file:///signal-allow-rule-list-" + strings.ReplaceAll(typed, ".", "dot") + ".yaml"
		c.open(uri, text)

		got := labels(c.complete(uri, pos.Line, pos.Character).Items)
		assert.NotContains(t, got, "sender", typed)
		// `run` itself is a root of the general scope; what must not leak in is the
		// allow predicate's own `run`, which is the starter's identity alone.
		for _, item := range c.complete(uri, pos.Line, pos.Character).Items {
			if item.Label == "run" {
				assert.NotEqual(t, "the run's starter", item.Detail, typed)
			}
		}
		if typed == "" {
			assert.Contains(t, got, "steps")
		}
	}
}
