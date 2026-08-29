package ui_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// TestEscapeControlKeepsTextToItsOwnLine covers the three shapes that matter,
// each of them a way for text this process did not write to break a promise the
// surrounding line makes.
func TestEscapeControlKeepsTextToItsOwnLine(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name    string
		in      string
		want    string
		because string
	}{
		{
			name:    "a newline cannot fabricate a row",
			in:      "connection refused\n10:14:02  done     `deploy`",
			want:    `connection refused\n10:14:02  done     ` + "`deploy`",
			because: "a failure message that spans lines invents output the command never produced",
		},
		{
			name:    "a tab cannot move a column",
			in:      "timed out\tafter 30s",
			want:    `timed out\tafter 30s`,
			because: "a tab reaches the tabwriter as a column break and shifts every row after it",
		},
		{
			name:    "an escape cannot restyle the terminal",
			in:      "\x1b[31mfailed\x1b[0m",
			want:    `\x1b[31mfailed\x1b[0m`,
			because: "text from a workload must not choose how the reader's terminal looks",
		},
		{
			name:    "ordinary text is untouched",
			in:      "connection refused: dial tcp 10.0.0.1:443",
			want:    "connection refused: dial tcp 10.0.0.1:443",
			because: "escaping what needs no escaping would make every real diagnosis harder to read",
		},
		{
			name: "text outside ASCII is not a control character",
			in:   "no se pudo conectar — é 日本語",
			want: "no se pudo conectar — é 日本語",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := ui.EscapeControl(c.in)
			assert.Equal(t, c.want, got, c.because)
			assert.False(t, strings.ContainsAny(got, "\n\t\r\x1b"),
				"the escaped form still carries a character the terminal acts on")
		})
	}
}
