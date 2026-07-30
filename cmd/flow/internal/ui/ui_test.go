package ui

import (
	"bytes"
	"strings"
	"testing"

	"charm.land/lipgloss/v2"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/require"
)

// The whole value of this package is that it behaves correctly on terminals
// nobody here can see, so it is tested by feeding it the environments those
// terminals produce rather than by looking at one.
//
// The assertions are deliberately about *bytes*, not about styles. A style object
// that claims to be plain proves nothing; what a pipe receives is the only thing
// that matters, and the only way to know is to write through the writer and look
// at what came out.

// escape is the byte every one of these tests is really asking about.
const escape = "\x1b"

// forceTTY makes colorprofile treat a writer as a terminal, which is the only way
// to exercise the styled paths without one. It is colorprofile's own hook.
const forceTTY = "TTY_FORCE=1"

func TestProfileFollowsTheEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  []string
		want colorprofile.Profile
	}{
		{
			// The case that matters most: a pipe. Not "fewer colours" — none, and
			// the writer strips any escape that reaches it.
			name: "a pipe carries nothing",
			env:  []string{"TERM=xterm-256color"},
			want: colorprofile.NoTTY,
		},
		{
			name: "a 256-colour terminal",
			env:  []string{forceTTY, "TERM=xterm-256color"},
			want: colorprofile.ANSI256,
		},
		{
			name: "a true-colour terminal",
			env:  []string{forceTTY, "TERM=xterm-256color", "COLORTERM=truecolor"},
			want: colorprofile.TrueColor,
		},
		{
			// Honoured, and honoured as more than a downgrade: NO_COLOR clamps to
			// Ascii, which keeps bold and faint while removing every hue. Somebody
			// who set it did not ask to lose emphasis.
			name: "NO_COLOR is honoured",
			env:  []string{forceTTY, "TERM=xterm-256color", "NO_COLOR=1"},
			want: colorprofile.Ascii,
		},
		{
			name: "a dumb terminal is dumb",
			env:  []string{forceTTY, "TERM=dumb"},
			want: colorprofile.NoTTY,
		},
		{
			// The other direction, and it has to work: somebody piping through a
			// pager they know handles colour asked for this on purpose.
			name: "CLICOLOR_FORCE asks for colour through a pipe",
			env:  []string{"TERM=xterm-256color", "CLICOLOR_FORCE=1"},
			want: colorprofile.ANSI256,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, colorprofile.Detect(&bytes.Buffer{}, test.env),
				"the environment was read differently from how the CLI documents it")
		})
	}
}

// TestAPipeReceivesNoEscapes is the assertion the rest of the package exists to
// keep true.
func TestAPipeReceivesNoEscapes(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	// A buffer is not a terminal, which is exactly the shape of `flow list | awk`.
	caps := Capabilities{Profile: colorprofile.Detect(&out, []string{"TERM=xterm-256color"})}
	theme := NewTheme(true, caps)

	writer := colorprofile.NewWriter(&out, []string{"TERM=xterm-256color"})

	// Written through the writer the way the CLI writes, including a style that
	// would certainly emit escapes if anything let it.
	_, err := writer.WriteString(theme.Strong.Render("run-1") + "\t" + theme.Pill(ToneSuccess, "completed") + "\n")
	require.NoError(t, err)

	require.NotContains(t, out.String(), escape,
		"a piped stream received an escape sequence, which corrupts whatever is parsing it")

	// And the words survived, which is the other half: stripping colour must not
	// strip meaning.
	require.Contains(t, out.String(), "run-1")
	require.Contains(t, out.String(), "COMPLETED")

	require.Empty(t, errOut.String())
}

// TestATerminalReceivesStyle is the same assertion from the other side, so that a
// theme which simply rendered nothing could not pass the test above.
func TestATerminalReceivesStyle(t *testing.T) {
	t.Parallel()

	env := []string{forceTTY, "TERM=xterm-256color", "COLORTERM=truecolor"}

	var out bytes.Buffer
	caps := Capabilities{Profile: colorprofile.Detect(&out, env), TTY: true}
	require.Equal(t, colorprofile.TrueColor, caps.Profile, "the fixture is not exercising a colour terminal")

	theme := NewTheme(true, caps)
	require.False(t, theme.Plain(), "a true-colour terminal got a plain theme")

	writer := colorprofile.NewWriter(&out, env)
	_, err := writer.WriteString(theme.Pill(ToneDanger, "failed") + "\n")
	require.NoError(t, err)

	require.Contains(t, out.String(), escape, "a colour terminal received no styling at all")
	require.Contains(t, out.String(), "FAILED", "the label did not survive being styled")
}

// TestNoColorKeepsTheWords checks the case somebody explicitly asked for.
func TestNoColorKeepsTheWords(t *testing.T) {
	t.Parallel()

	env := []string{forceTTY, "TERM=xterm-256color", "NO_COLOR=1"}

	var out bytes.Buffer
	caps := Capabilities{Profile: colorprofile.Detect(&out, env), TTY: true}
	theme := NewTheme(true, caps)

	writer := colorprofile.NewWriter(&out, env)
	_, err := writer.WriteString(theme.Pill(ToneWarning, "timed out") + " " + theme.Muted.Render("(no answer)") + "\n")
	require.NoError(t, err)

	require.NotContains(t, out.String(), "38;2", "NO_COLOR was set and a colour was still emitted")
	require.Contains(t, out.String(), "TIMED OUT")
	require.Contains(t, out.String(), "(no answer)")
}

// TestEveryRoleSurvivesLosingItsColour is the accessibility assertion, and the one
// that keeps colour from becoming the channel rather than an accelerator.
func TestEveryRoleSurvivesLosingItsColour(t *testing.T) {
	t.Parallel()

	styled := NewTheme(true, Capabilities{Profile: colorprofile.TrueColor})
	plain := NewTheme(true, Capabilities{Profile: colorprofile.NoTTY})

	const label = "deploy"

	for _, tone := range []Tone{ToneNeutral, ToneSuccess, ToneWarning, ToneDanger, ToneInfo} {
		// The pill's own text is the same either way, so a reader who cannot see
		// the fill reads the same word as a reader who can.
		require.Equal(t, strings.ToUpper(label), plain.Pill(tone, label))
		require.Contains(t, styled.Pill(tone, label), strings.ToUpper(label))
	}

	require.Equal(t, label, plain.Strong.Render(label))
	require.Equal(t, label, plain.Muted.Render(label))
	require.True(t, plain.Plain())
}

func TestBothBackgroundsGetDistinctColours(t *testing.T) {
	t.Parallel()

	caps := Capabilities{Profile: colorprofile.TrueColor}
	light := NewTheme(false, caps)
	dark := NewTheme(true, caps)

	// A palette that resolved to one set of values regardless of background would
	// be half unreadable, and would pass every other test in this file.
	require.NotEqual(t,
		light.Success.Render("ok"), dark.Success.Render("ok"),
		"the palette renders identically on light and dark backgrounds, so one of them was not designed for")

	require.NotEqual(t, light.Muted.Render("hint"), dark.Muted.Render("hint"))
}

func TestSymbolsDegradeButKeepTheirWidth(t *testing.T) {
	t.Parallel()

	terminal := Capabilities{TTY: true, Profile: colorprofile.ANSI256, Unicode: true}
	pipe := Capabilities{Profile: colorprofile.NoTTY}

	require.Equal(t, unicodeSymbols, terminal.Symbols())
	require.Equal(t, asciiSymbols, pipe.Symbols())

	// A table rendered with one set and read with the other still lines up, which
	// is what makes golden output comparable against a real terminal — and it is
	// the property that rules emoji out, since a double-width glyph in a column
	// shifts every column after it.
	//
	// Measured with lipgloss.Width rather than by counting runes, because that is
	// the same measurement the renderer aligns with. Ellipsis is the deliberate
	// exception: three dots is what ASCII has to offer.
	for _, pair := range []struct {
		name           string
		unicode, ascii string
	}{
		{"success", unicodeSymbols.Success, asciiSymbols.Success},
		{"failure", unicodeSymbols.Failure, asciiSymbols.Failure},
		{"warning", unicodeSymbols.Warning, asciiSymbols.Warning},
		{"running", unicodeSymbols.Running, asciiSymbols.Running},
		{"waiting", unicodeSymbols.Waiting, asciiSymbols.Waiting},
		{"skipped", unicodeSymbols.Skipped, asciiSymbols.Skipped},
		{"bullet", unicodeSymbols.Bullet, asciiSymbols.Bullet},
		{"arrow", unicodeSymbols.Arrow, asciiSymbols.Arrow},
		{"divider", unicodeSymbols.Divider, asciiSymbols.Divider},
	} {
		require.Equal(t, 1, lipgloss.Width(pair.unicode),
			"%s is not one cell wide, so a column holding it will not line up", pair.name)
		require.Equal(t, lipgloss.Width(pair.unicode), lipgloss.Width(pair.ascii),
			"%s renders at a different width from its fallback, so the two sets do not align", pair.name)
	}
}

func TestNoSymbolIsAnEmoji(t *testing.T) {
	t.Parallel()

	// Emoji are excluded on purpose and the reasons are practical: they render at
	// inconsistent widths, support varies enough that a fallback glyph in a status
	// line is a real outcome, and screen readers announce them unpredictably.
	//
	// The check is two-sided because neither half is sufficient. Width alone would
	// admit a narrow pictograph; a block check alone would reject ✓ and ✗, which
	// are dingbats with text presentation and exactly the marks wanted here. So
	// this refuses the pictographic planes outright, refuses the variation
	// selector that forces emoji presentation on an otherwise textual character,
	// and — the assertion that actually protects the layout — requires every mark
	// to measure one cell.
	for _, symbol := range []string{
		unicodeSymbols.Success, unicodeSymbols.Failure, unicodeSymbols.Warning,
		unicodeSymbols.Waiting, unicodeSymbols.Running, unicodeSymbols.Skipped,
		unicodeSymbols.Bullet, unicodeSymbols.Arrow, unicodeSymbols.Divider,
	} {
		for _, r := range symbol {
			require.False(t, isPictographic(r),
				"%q is a pictographic character; the CLI uses typographic marks", symbol)
			require.NotEqual(t, rune(0xFE0F), r,
				"%q carries the emoji variation selector, which makes it render double-width", symbol)
		}

		require.Equal(t, 1, lipgloss.Width(symbol),
			"%q does not measure one cell, which is how an emoji gets in", symbol)
	}
}

// isPictographic reports whether a rune is in one of the planes that hold only
// emoji. Deliberately narrow: the blocks below contain nothing a status line would
// legitimately want, whereas the general symbol blocks contain the marks it does.
func isPictographic(r rune) bool {
	switch {
	case r >= 0x1F300 && r <= 0x1FAFF, // pictographs, transport, symbols, extensions
		r >= 0x1F000 && r <= 0x1F0FF: // tiles and cards
		return true
	}
	return false
}

func TestSymbolsEnvOverridesDetection(t *testing.T) {
	t.Parallel()

	// Detection can be wrong about somebody's terminal, and they can see it and we
	// cannot, so the override wins in both directions.
	pipe := Capabilities{Profile: colorprofile.NoTTY}
	require.True(t, wantsUnicode(pipe, []string{SymbolsEnv + "=unicode"}))

	terminal := Capabilities{TTY: true, Profile: colorprofile.TrueColor}
	require.False(t, wantsUnicode(terminal, []string{SymbolsEnv + "=ascii"}))
}

func TestPlainSurfaceWritesWhatItIsGiven(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	surface := Plain(&out, &errOut)

	_, err := surface.Out.Write([]byte("rows\n"))
	require.NoError(t, err)
	_, err = surface.Err.Write([]byte("account\n"))
	require.NoError(t, err)

	require.Equal(t, "rows\n", out.String())
	require.Equal(t, "account\n", errOut.String())
	require.True(t, surface.Theme.Plain())
	require.True(t, surface.ErrTheme.Plain())
	require.False(t, surface.Caps.Unicode, "a surface with no terminal chose typographic marks")
}

// TestEachStreamGetsItsOwnTheme is the negative direction of the per-stream rule.
//
// One theme for two streams has to be resolved against one of them, and either choice
// is wrong for `flow get x | jq`: resolved against the piped stdout it collapses to
// plain, so the status on a terminal stderr loses its colour to a pipe that was never
// going to receive it; resolved against stderr it sends escape sequences into jq.
//
// So the assertion is that they *differ* where the streams differ. A test checking
// only that a terminal gets style passes with one shared theme.
func TestEachStreamGetsItsOwnTheme(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	piped := Capabilities{Profile: colorprofile.NoTTY, Width: fallbackWidth}
	terminal := Capabilities{Profile: colorprofile.TrueColor, TTY: true, Dark: true, Width: 80, Height: 24}

	surface := ForCapabilities(&out, &errOut, piped, terminal)

	require.True(t, surface.Theme.Plain(),
		"the piped answer stream would receive escape sequences")
	require.False(t, surface.ErrTheme.Plain(),
		"the terminal account stream lost its styling to a pipe it does not share")

	// And the other way round, because a surface is not symmetric by construction:
	// `flow list > runs.txt` on a terminal has the piped stream on the other side.
	flipped := ForCapabilities(&out, &errOut, terminal, piped)
	require.False(t, flipped.Theme.Plain())
	require.True(t, flipped.ErrTheme.Plain())
}

// TestHeightIsZeroWhereThereAreNoRows checks the deliberate asymmetry with Width.
//
// Nothing needs a guessed number of rows, and a fallback would let a full-screen view
// believe it had 24 rows of a pipe to fill.
func TestHeightIsZeroWhereThereAreNoRows(t *testing.T) {
	t.Parallel()

	require.Zero(t, Detect(nil, nil, nil).Height)
}

// TestAMeasuredTerminalIsClampedInWidthAndNotInHeight is the positive direction, which
// Detect itself cannot be asked for.
//
// Reaching Detect's measuring branch needs a pty, which a Go test does not have — so a
// test written against Detect can only ever assert the *unmeasurable* case, and passes
// just as happily against a version that discards every size it is given. applySize
// holds the rules, so the rules are what this asserts.
func TestAMeasuredTerminalIsClampedInWidthAndNotInHeight(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		columns, rows         int
		wantWidth, wantHeight int
	}{
		// An ordinary terminal is used as it is.
		{columns: 72, rows: 24, wantWidth: 72, wantHeight: 24},
		// A very wide one is clamped, because long measures are harder to read.
		{columns: 300, rows: 90, wantWidth: maxWidth, wantHeight: 90},
		// A very tall one is not, because there is no equivalent reason to refuse
		// rows — and a clamp here would cap a full-screen view for no purpose.
		{columns: 80, rows: 500, wantWidth: 80, wantHeight: 500},
		// A size that could not be measured leaves both at what they were: the
		// fallback width, and no height at all.
		{columns: 0, rows: 0, wantWidth: fallbackWidth, wantHeight: 0},
	} {
		caps := Capabilities{Width: fallbackWidth}
		caps.applySize(tc.columns, tc.rows)

		require.Equal(t, tc.wantWidth, caps.Width, "%dx%d", tc.columns, tc.rows)
		require.Equal(t, tc.wantHeight, caps.Height, "%dx%d", tc.columns, tc.rows)
	}
}

// TestClampWidthIsTheOneAnswerToHowWide checks the bound a repainting view shares with
// every surface that prints.
func TestClampWidthIsTheOneAnswerToHowWide(t *testing.T) {
	t.Parallel()

	require.Equal(t, maxWidth, ClampWidth(300))
	require.Equal(t, 64, ClampWidth(64))

	// Zero and below are a stream that could not be measured, not a zero-width one —
	// so a caller that divides by the width never divides by nothing.
	require.Equal(t, fallbackWidth, ClampWidth(0))
	require.Equal(t, fallbackWidth, ClampWidth(-1))
}

func TestWidthIsBoundedAndNeverZero(t *testing.T) {
	t.Parallel()

	// A pipe cannot be measured, and a caller that divides by the width must not
	// have to check for nothing.
	require.Equal(t, fallbackWidth, Detect(nil, nil, nil).Width)
}
