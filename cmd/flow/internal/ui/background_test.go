package ui

import (
	"os"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Asking a terminal for its background colour is the only thing this CLI does that
// can make it look hung.
//
// It is an OSC 11 sequence written to the terminal and a reply read back, and
// lipgloss waits two seconds for one — against both the input and the output file,
// so a terminal that answers nothing at all costs four. On those, `flow` printed
// nothing for four seconds and then behaved normally, which reads as a hang in the
// network or the server: the two places somebody would look, and neither of them
// it.
//
// Narrower than it first appears, and measured rather than assumed. The query asks
// for the background colour *and* the primary device attributes in one write, so a
// terminal that simply does not implement background reporting still answers the
// second and ends the wait — 0.02s against a pty doing exactly that. The four
// seconds belong to a pty that answers neither, which is automation holding a tty
// rather than a person's terminal.
//
// So the rules around the question matter more than the question does. These are
// about the three answers that come first.

// TestABackgroundNobodyCanUseIsNotAskedFor is the case that costs the most and buys
// the least.
//
// Below ANSI there is no colour to resolve, so both halves of every pair collapse to
// the same styles and the answer cannot change a byte. That is the NO_COLOR reader,
// the TERM=dumb terminal and the CI log — and those are precisely the terminals
// least likely to answer, so the query that cannot help is the one most likely to
// stall.
//
// Written against settledBackground rather than Detect because Detect needs a
// terminal to reach this at all, and a Go test does not have one. What is under test
// is the rule, not the measurement — and the rule is checkable in the one way that
// distinguishes "did not ask" from "asked and got an error", which timing cannot.
func TestABackgroundNobodyCanUseIsNotAskedFor(t *testing.T) {
	t.Parallel()

	// A real file, so nothing here can be settled by the nil-input rule instead.
	in, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { _ = in.Close() })

	for _, profile := range []colorprofile.Profile{colorprofile.NoTTY, colorprofile.Ascii} {
		dark, settled := settledBackground(in, nil, profile)

		assert.True(t, settled,
			"a stream rendering no colour still pays for a terminal query whose answer it cannot use")
		assert.True(t, dark,
			"a background nothing reads was answered with something other than the safe default")
	}

	// The other direction, so the rule above cannot pass by settling everything: a
	// terminal that does carry colour has a real question to ask.
	_, settled := settledBackground(in, nil, colorprofile.ANSI)
	assert.False(t, settled,
		"a terminal that renders colour was given a background without being asked")
}

// TestTheEnvironmentSettlesTheBackgroundWithoutAsking is the escape hatch, and the
// only one there is.
//
// lipgloss's timeout is not configurable and must not be routed around by abandoning
// the call — the query puts the terminal into raw mode and restores it on the way
// out, so walking away from a slow one leaves the terminal raw for whatever runs
// next. Somebody on a terminal that does not answer therefore needs a way to say so.
func TestTheEnvironmentSettlesTheBackgroundWithoutAsking(t *testing.T) {
	t.Parallel()

	in, err := os.Open(os.DevNull)
	require.NoError(t, err)
	t.Cleanup(func() { _ = in.Close() })

	// TrueColor, so the profile rule cannot be what answers: this has to be the
	// environment winning over a query that would otherwise happen. `light` is the
	// case that proves it, since it is the one answer no other rule here produces.
	for _, test := range []struct {
		value string
		want  bool
	}{
		{value: "dark", want: true},
		{value: "light", want: false},
		{value: "DARK", want: true},
		{value: "Light", want: false},
	} {
		dark, settled := settledBackground(in, []string{BackgroundEnv + "=" + test.value}, colorprofile.TrueColor)

		assert.True(t, settled,
			"%s=%s was honoured only after asking the terminal anyway", BackgroundEnv, test.value)
		assert.Equal(t, test.want, dark,
			"%s=%s did not settle the background", BackgroundEnv, test.value)
	}
}

// TestAnUnsetOrMeaninglessBackgroundIsNotAnAssertion keeps the override from
// swallowing the detection.
//
// The empty string is the one that matters. A variable exported and left blank — by
// a shell profile, by a container image, by `env FLOWSTATE_BACKGROUND= flow ...` —
// is not somebody telling us their terminal is light, and reading it as one would
// silence the detection for a whole session in the direction that renders pale text
// on a pale background.
func TestAnUnsetOrMeaninglessBackgroundIsNotAnAssertion(t *testing.T) {
	t.Parallel()

	for _, environ := range [][]string{
		nil,
		{BackgroundEnv + "="},
		{BackgroundEnv + "=true"},
		{BackgroundEnv + "=1"},
		{BackgroundEnv + "=black"},
		{"FLOWSTATE_BACKGROUND_COLOUR=dark"},
	} {
		_, settled := backgroundFromEnv(environ)
		assert.False(t, settled,
			"%v was read as an assertion about somebody's terminal", environ)
	}
}

// TestTheTerminalIsAskedAtMostOnce is the property the memo exists for, stated as a
// count rather than as a duration.
//
// A `flow` invocation calls Detect twice — stdout and stderr are separate streams
// with separate colour depths — and [ForCapabilities] then merges the two answers
// into one. So the second query's result had nowhere to go but an OR with the
// first's. The help and error surfaces were a third asker until this CLI took
// them over from fang, which resolved its palette through a query no option reached.
func TestTheTerminalIsAskedAtMostOnce(t *testing.T) {
	t.Parallel()

	var asked int
	ask := func() bool {
		asked++

		return true
	}

	var m memo
	for range 3 {
		assert.True(t, m.get(ask))
	}

	assert.Equal(t, 1, asked,
		"the terminal was asked for its background once per stream instead of once per process")
}

// TestEverybodyGetsTheFirstAnswer is the other half, and the half that would go
// wrong silently.
//
// A memo that asked once and returned the *asker's* answer each time would pass the
// count above while handing later callers whatever their own closure returned.
// Detect runs twice with different files, so this is not hypothetical: the second
// call's question is about the same terminal and must not be able to disagree.
func TestEverybodyGetsTheFirstAnswer(t *testing.T) {
	t.Parallel()

	var m memo

	require.False(t, m.get(func() bool { return false }))
	assert.False(t, m.get(func() bool { return true }),
		"a later stream's answer replaced the one already settled for this terminal")
}
