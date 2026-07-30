package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// The error report is the last thing a failed command says, and it arrives after
// however much output the command already produced.
//
// Everything here is about not making it worse than the error already is. A report
// that summarises loses the reason; one carried only by colour is invisible in a CI
// log; and advice offered where none is knowable sends somebody to the wrong place.

// reportOf renders an error the way a failed command does, unstyled.
//
// The plain surface deliberately, because that is what a pipe and a log file
// receive, and because a test asserting on styled bytes would be a test of lipgloss.
func reportOf(t *testing.T, err error) string {
	t.Helper()

	var out strings.Builder
	surface := ui.Plain(&out, &out)
	renderError(&out, surface.ErrTheme, 80, err)

	return out.String()
}

// TestTheReportCarriesTheErrorVerbatim is the one that matters most.
//
// A report that rewrote, truncated or summarised the error would be the only place
// the reason existed, and it would be a worse one. Whatever the command said comes
// through unchanged.
func TestTheReportCarriesTheErrorVerbatim(t *testing.T) {
	t.Parallel()

	const reason = `step "web": http: dial tcp 10.0.0.1:443: i/o timeout`

	assert.Contains(t, reportOf(t, errors.New(reason)), reason,
		"the error's own text did not survive being reported")
}

// TestTheReportSurvivesLosingItsColour keeps it findable where colour is gone.
//
// The label is a filled pill on a terminal and the word ERROR everywhere else. A
// report whose only marker was a background colour would be, in a CI log, an
// unannounced sentence in the middle of whatever came before it.
func TestTheReportSurvivesLosingItsColour(t *testing.T) {
	t.Parallel()

	report := reportOf(t, errors.New("something went wrong"))

	assert.Contains(t, report, "ERROR",
		"the report is marked only by colour, so it vanishes in a log")
	assert.NotContains(t, report, "\x1b",
		"an escape sequence reached a stream that cannot render one")
}

// TestAdviceIsOfferedOnlyWhereItIsKnowable is the negative direction, and the one
// that would be tempting to get wrong.
//
// A mistake about the command line has a next step: read the help. A workload that
// failed, a server that refused, a file that does not parse — none of those are
// helped by being told to read the help, and saying it anyway trains people to
// ignore the line for the one case where it is true.
func TestAdviceIsOfferedOnlyWhereItIsKnowable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "an unknown flag", err: errors.New(`unknown flag: --nope`), want: true},
		{name: "an unknown command", err: errors.New(`unknown command "nope" for "flow"`), want: true},
		{name: "too many arguments", err: errors.New(`accepts 1 arg(s), received 3`), want: true},
		{name: "a flag missing its value", err: errors.New(`flag needs an argument: --address`), want: true},

		{name: "a run that failed", err: errors.New(`run "x" failed: step "web": 500`), want: false},
		{name: "a server that refused", err: errors.New(`unauthenticated: no token`), want: false},
		{name: "a file that does not parse", err: errors.New(`workflow.yaml:3:1: unknown key`), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			report := reportOf(t, test.err)

			if test.want {
				assert.Contains(t, report, "flow --help",
					"a mistake about the command line was reported with no way forward")

				return
			}

			assert.NotContains(t, report, "flow --help",
				"a failure that reading the help cannot fix was reported as though it could")
		})
	}
}

// TestTheReportDoesNotTouchTheFirstLetter is the rule that replaced a heuristic.
//
// An earlier version capitalized it, since Go errors are lower case because they are
// usually wrapped and this is the end of that chain. That is true of prose and wrong
// of everything else an error begins with: `step "web": …` became `Step "web": …`,
// and a position became `Workflow.yaml:3:1`, which is not a file anybody can search
// for. Nothing separates the two reliably, so nothing tries.
func TestTheReportDoesNotTouchTheFirstLetter(t *testing.T) {
	t.Parallel()

	for _, text := range []string{
		`step "web": http: 500`,
		`workflow.yaml:3:1: unknown key`,
		`unknown flag: --nope`,
		`Cannot reach the server`,
	} {
		assert.Contains(t, reportOf(t, errors.New(text)), text,
			"the report altered the error it was given")
	}
}

// TestNoErrorReportsNothing keeps the report out of the success path.
//
// renderError is reached from one place today, but a nil error printing a bare
// ERROR is the kind of thing that only shows up in front of somebody whose command
// worked.
func TestNoErrorReportsNothing(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	surface := ui.Plain(&out, &out)
	renderError(&out, surface.ErrTheme, 80, nil)

	require.Empty(t, out.String(), "a command that succeeded printed an error report")
}

// TestALongPathIsNotBrokenInHalf is about the one thing somebody does with an error.
//
// A file path, a URL and a workflow id are what gets copied out of a report, and a
// wrap that splits one makes it uncopyable. Both obvious implementations do exactly
// that: lipgloss's `Width` and `ansi.Wordwrap` treat a hyphen as a breakpoint, so a
// path under `/tmp/claude-0/-home-user-flowstate/1d2dc997-47cf-…` came out cut at
// `aae3-`. Overflowing the measure is the right answer — the terminal soft-wraps,
// which looks the same and leaves it selectable.
func TestALongPathIsNotBrokenInHalf(t *testing.T) {
	t.Parallel()

	const path = "/tmp/claude-0/-home-user-flowstate/1d2dc997-47cf-5bdc-aae3-b8b1e12a824f/bad.yaml:6:15"

	report := reportOf(t, errors.New(path+": step \"web\": bad method"))

	assert.Contains(t, report, path,
		"the report broke a path across lines, so it cannot be copied or searched for")
}

// TestWrappingStillHappensBetweenWords keeps the rule above from becoming "never
// wrap".
//
// Overflowing is for a single word with nowhere to break. Ordinary prose still has
// to fit the measure, or a long error runs off the side of every terminal.
func TestWrappingStillHappensBetweenWords(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("word ", 40)

	for _, line := range strings.Split(wrap(long, 40), "\n") {
		assert.LessOrEqual(t, len(line), 40,
			"a line of ordinary words was not wrapped to the measure")
	}
}
