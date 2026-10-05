package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// `debug: allow: ${` completes the same closed scope as `signals:`, and
// `triggers: - manual: allow: ${` the same scope without `run`: a manual start
// has no run, and a name the compiler refuses is never offered.
func TestDebugAndManualAllowPredicatesCompleteTheirClosedScopes(t *testing.T) {
	const debugSrc = `name: c
steps:
  - id: web
    sleep: 1s
debug:
  allow: ${PLACEHOLDER
edition: v2026.4
`
	const manualSrc = `name: c
steps:
  - id: web
    sleep: 1s
triggers:
  - manual:
      allow: ${PLACEHOLDER
edition: v2026.4
`

	c := newClient(t)
	c.initialize()

	for _, test := range []struct {
		name, src, typed string
		want, notWant    []string
	}{
		{"debug offers the three roots", debugSrc, "", []string{"sender", "run", "inputs"}, []string{"steps", "vars"}},
		{"debug identity fields", debugSrc, "run.identity.", []string{"principal", "claims"}, []string{"workflow_id"}},
		{"manual offers sender and inputs and not run", manualSrc, "", []string{"sender", "inputs"}, []string{"run", "steps", "vars"}},
		{"manual identity fields", manualSrc, "sender.identity.", []string{"principal", "subject", "issuer", "namespace", "claims"}, []string{"deployment"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			text, pos := splitCursor(t, strings.Replace(test.src, "PLACEHOLDER", test.typed+"|", 1))
			uri := "file:///allow-stanza-" + strings.ReplaceAll(test.name, " ", "-") + ".yaml"
			c.open(uri, text)

			got := labels(c.complete(uri, pos.Line, pos.Character).Items)
			for _, want := range test.want {
				assert.Contains(t, got, want)
			}
			for _, notWant := range test.notWant {
				assert.NotContains(t, got, notWant)
			}
		})
	}
}
