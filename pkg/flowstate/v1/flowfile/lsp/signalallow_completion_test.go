package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A `signals:` policy's `allow: ${...}` is evaluated over a closed scope, so
// the editor offers that scope and not the step scope: a name the compiler
// refuses is the one thing completion must not suggest.
func TestASignalAllowPredicateCompletesItsClosedScope(t *testing.T) {
	const src = `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: gate
    wait_for_signal:
      name: approved
signals:
  approved:
    allow: ${PLACEHOLDER
edition: v2026.4
`

	c := newClient(t)
	c.initialize()

	for _, test := range []struct {
		name    string
		typed   string
		want    []string
		notWant []string
	}{
		{
			name:    "bare offers the three roots and not the step scope",
			typed:   "",
			want:    []string{"sender", "run", "inputs"},
			notWant: []string{"steps", "vars"},
		},
		{
			name:    "the sender offers its identity",
			typed:   "sender.",
			want:    []string{"identity"},
			notWant: []string{"local", "steps"},
		},
		{
			name:    "the identity offers exactly the closed fields",
			typed:   "sender.identity.",
			want:    []string{"principal", "subject", "issuer", "namespace", "claims"},
			notWant: []string{"deployment", "web"},
		},
		{
			name:    "the starter has the same shape",
			typed:   "run.identity.",
			want:    []string{"principal", "claims"},
			notWant: []string{"workflow_id", "started_at"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			text, pos := splitCursor(t, strings.Replace(src, "PLACEHOLDER", test.typed+"|", 1))
			uri := "file:///signal-allow-" + strings.ReplaceAll(test.name, " ", "-") + ".yaml"
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
