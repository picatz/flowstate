package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// What a `log:` step looks like to the person who ran the workflow.
//
// The engine's tests assert the message reaches a logger; these assert what that logger
// puts on the screen, which is a separate claim and fails separately — a correct record
// rendered as `time=... level=WARN msg=...` is slog's default answer to a question
// nobody watching a run is asking.

// renderLog runs one record through the handler and returns the line, unstyled.
func renderLog(t *testing.T, level slog.Level, message string, attrs ...slog.Attr) string {
	t.Helper()

	var out bytes.Buffer
	logger := slog.New(newRunLogHandler(&out, ui.NewTheme(true, ui.Capabilities{})))
	logger.LogAttrs(t.Context(), level, message, attrs...)

	return strings.TrimRight(out.String(), "\n")
}

// TestARunLogLineLeadsWithItsLevel checks the shape a reader scans.
//
// The level first and as a word, because someone watching a rollout is looking for the
// one line that is not INFO — and a level buried mid-line as `level=WARN` is found by
// reading rather than by glancing.
func TestARunLogLineLeadsWithItsLevel(t *testing.T) {
	t.Parallel()

	for level, want := range map[slog.Level]string{
		slog.LevelInfo:  "INFO",
		slog.LevelWarn:  "WARN",
		slog.LevelError: "ERROR",

		// Below info still renders rather than vanishing: a workflow author wrote the
		// step, and a level this handler does not have a word for is still a message
		// somebody asked to see.
		slog.LevelDebug: "INFO",
	} {
		t.Run(want, func(t *testing.T) {
			t.Parallel()

			line := renderLog(t, level, "something happened")
			require.True(t, strings.HasPrefix(line, want),
				"a log line did not lead with its level:\n%s", line)
			require.Contains(t, line, "something happened")
		})
	}
}

// TestARunLogLineCarriesItsFields checks the structured half is rendered and not
// dropped.
func TestARunLogLineCarriesItsFields(t *testing.T) {
	t.Parallel()

	line := renderLog(t, slog.LevelInfo, "rolled out",
		slog.String("region", "eu-west-1"), slog.String("stage", "canary"))

	require.Contains(t, line, "region=eu-west-1")
	require.Contains(t, line, "stage=canary")

	// After the message, because the message is what decides whether a reader cares
	// about the fields at all.
	require.Less(t, strings.Index(line, "rolled out"), strings.Index(line, "region="),
		"a log line put its fields before its message")
}

// TestARunLogLineIsNotTruncated is the regression this handler was written wrong once
// and fixed.
//
// Every other surface in this program trims to the terminal, because they repaint a
// fixed screen where an overlong line corrupts the frame. This one is a stream: the
// terminal wraps it. Truncating would silently drop the end of the message an author
// wrote a step in order to emit, at a width that depends on the reader's window — so
// the same run would say different things to two people.
func TestARunLogLineIsNotTruncated(t *testing.T) {
	t.Parallel()

	message := strings.Repeat("wide ", 60)
	line := renderLog(t, slog.LevelWarn, message)

	require.Contains(t, line, message, "a log message was cut to the terminal width")
	require.Greater(t, len(line), 200, "the line under test is not long enough to prove anything")
}

// TestARunLogLineIsPlainWithoutAColourProfile checks the ASCII floor.
//
// A run piped to a file or read by an agent must not carry escape sequences, and this
// handler builds its line from theme styles — so the check belongs here rather than
// being assumed from the theme's own tests.
func TestARunLogLineIsPlainWithoutAColourProfile(t *testing.T) {
	t.Parallel()

	line := renderLog(t, slog.LevelError, "broke", slog.String("code", "500"))

	require.NotContains(t, line, "\x1b", "a log line carried escape sequences with no colour profile")
	require.Contains(t, line, "ERROR")
	require.Contains(t, line, "code=500")
}
