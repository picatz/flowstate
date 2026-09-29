package flowdebug

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestOverlappingSecretsAreNotCutByEachOther: a session's own redactor and a
// held workflow's sensitive set each match the text as it was. Applied in
// turn, `abc` withheld first leaves `abcdef` unfound and `def` showing, and
// the reverse order fails the same way; where both would withhold something,
// the rendering is withheld whole (Copilot, #2209).
func TestOverlappingSecretsAreNotCutByEachOther(t *testing.T) {
	t.Parallel()

	session := strings.NewReplacer("secret", "[redacted]").Replace
	for name, test := range map[string]struct {
		held, rendered string
		want           string
	}{
		"held longer":  {held: "secret-and-more", rendered: "key=secret-and-more!", want: "[withheld]"},
		"held shorter": {held: "cret", rendered: "key=secret!", want: "[withheld]"},
		"held only":    {held: "hunter2", rendered: "key=hunter2", want: "key=[redacted]"},
		"session only": {held: "hunter2", rendered: "key=secret", want: "key=[redacted]"},
		"neither":      {held: "hunter2", rendered: "key=plain", want: "key=plain"},
		"both, apart":  {held: "hunter2", rendered: "secret hunter2", want: "[withheld]"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sensitive := v1.SensitiveInputValues(map[string]*v1.Value{"k": v1.NewLiteral(test.held)}, map[string]bool{"k": true})
			text, _ := withholdingAt(session, nil, sensitive)
			got := text(test.rendered)
			assert.Equal(t, test.want, got)
			assert.NotContains(t, got, "def")
		})
	}
}
