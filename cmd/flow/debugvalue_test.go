package main

import (
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

var sgr = regexp.MustCompile("\x1b\\[[0-9;]*m")

// TestAnInspectedValueIsColouredAndNeverChanged: the colour is the only thing a
// terminal adds, so the text under it is the bytes a pipe receives, and what is
// withheld is the one thing that stands out from data.
func TestAnInspectedValueIsColouredAndNeverChanged(t *testing.T) {
	t.Parallel()

	const value = "auth:\n  token: \"[redacted]\"\n  retries: 3\n  note: \"true 4\"\n  … 2 more keys\n"

	painted := ui.NewTheme(true, ui.Capabilities{Profile: colorprofile.TrueColor})
	plain := ui.Plain(io.Discard, io.Discard).Theme

	var colour, bare strings.Builder
	debugEmitter(&colour, painted)(value, flowdebug.ToneValue)
	debugEmitter(&bare, plain)(value, flowdebug.ToneValue)

	assert.Equal(t, value, bare.String(), "a theme with no colour changed a value's bytes")
	assert.Equal(t, value, sgr.ReplaceAllString(colour.String(), ""),
		"colour changed a value's bytes")
	assert.Contains(t, colour.String(), painted.Warning.Render(`"[redacted]"`), "the withheld marker is not marked")
	assert.Contains(t, colour.String(), painted.Accent.Render("3"), "a number is not marked")
	assert.Contains(t, colour.String(), `"true 4"`+"\n", "the content of a string was coloured as if it were a literal")
	assert.False(t, strings.HasSuffix(colour.String(), "m\n\n"), "a line break was wrapped in or doubled by styling")
}
