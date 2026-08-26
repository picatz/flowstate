package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/charmbracelet/colorprofile"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTheTimelineDetailColumnSaysWhichTry pins the two rules the rightmost
// column follows, because they differ and mixing them up is how a column
// becomes noise or a fact goes missing.
//
// A failure row always names its attempt: a step failing on attempt 1 and a
// step failing on attempt 5 are different situations, and the sentence is
// usually identical in both. A row that succeeded names its attempt only past
// the first, because every scheduling carries attempt 1 and a column of
// "attempt 1" says nothing.
func TestTheTimelineDetailColumnSaysWhichTry(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name    string
		entry   *v1.TimelineEntry
		want    string
		because string
	}{
		{
			name:    "a first-attempt failure still names the attempt",
			entry:   &v1.TimelineEntry{Attempt: 1, Failure: "connection refused"},
			want:    "attempt 1: connection refused",
			because: "the whole reason to read a failure row is which try it was",
		},
		{
			name:  "a later failure names its own attempt, not the next one",
			entry: &v1.TimelineEntry{Attempt: 3, Failure: "connection refused"},
			want:  "attempt 3: connection refused",
		},
		{
			name:    "a run-level failure has no attempt to name",
			entry:   &v1.TimelineEntry{Kind: v1.TimelineEntry_KIND_RUN_ENDED, Failure: "the run failed"},
			want:    "the run failed",
			because: "a run is not an attempt at anything, so `attempt 0` would be a fact it invented",
		},
		{
			name:    "an ordinary first attempt says nothing",
			entry:   &v1.TimelineEntry{Attempt: 1},
			want:    "",
			because: "every scheduling carries attempt 1, so printing it fills the column with noise",
		},
		{
			name:  "a step that succeeded after retrying says which try worked",
			entry: &v1.TimelineEntry{Attempt: 3},
			want:  "attempt 3",
		},
		{
			name:  "a row about neither says neither",
			entry: &v1.TimelineEntry{Kind: v1.TimelineEntry_KIND_SIGNAL_RECEIVED},
			want:  "",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, c.want, timelineDetail(c.entry), c.because)
		})
	}
}

// TestTheRenderedTimelineKeepsEachEventToItsOwnRow is the claim end to end: a
// workload's own text reaches a terminal through this command, and a row
// promises one event.
//
// The two columns that carry text this process did not write are the step — a
// signal row's is the name whoever sent it chose — and the detail, which holds
// the workload's failure sentence. Either can hold a newline, a tab, or an
// escape, and the table is a tabwriter, so a tab is a column break and a
// newline is a row (Codex, #1119).
func TestTheRenderedTimelineKeepsEachEventToItsOwnRow(t *testing.T) {
	t.Parallel()

	var out, errs bytes.Buffer
	surface := &ui.UI{
		Out:     &out,
		Err:     &errs,
		Caps:    ui.Capabilities{Profile: colorprofile.NoTTY},
		ErrCaps: ui.Capabilities{Profile: colorprofile.NoTTY},
	}

	msg := &v1.GetTimelineResponse{Entries: []*v1.TimelineEntry{
		{
			EventId: 5,
			Kind:    v1.TimelineEntry_KIND_STEP_FAILED,
			Step:    "`deploy`",
			Attempt: 1,
			Failure: "boom\n09:00:00  done     `deploy`\x1b[31m\tnope",
		},
		{
			EventId: 6,
			Kind:    v1.TimelineEntry_KIND_SIGNAL_RECEIVED,
			Step:    "approved\nfake-row",
		},
	}}

	renderTimeline(surface, false, msg)

	printed := out.String()

	// Two entries, so two rows plus the header. A newline that reached the
	// terminal would make more.
	assert.Equal(t, 3, len(strings.Split(strings.TrimRight(printed, "\n"), "\n")),
		"a failure message with a newline in it invented rows that read as this "+
			"command's own output:\n%s", printed)

	assert.NotContains(t, printed, "\x1b[31m",
		"a workload's failure text chose how the reader's terminal looks")
	assert.Contains(t, printed, `\n`, "the newline was dropped rather than shown")
	assert.Contains(t, printed, `\t`, "the tab was dropped rather than shown")
	assert.Contains(t, printed, `\x1b`, "the escape was dropped rather than shown")

	// The sentence itself survives — escaping is not redaction, and a
	// diagnosis a reader cannot read is no better than one they cannot trust.
	assert.Contains(t, printed, "boom")
	assert.Contains(t, printed, "approved")
}

// TestTheJSONTimelineKeepsTheValueAsItIs is the other half: escaping is for a
// terminal interpreting bytes, and a consumer parsing JSON is not one.
//
// Handing back an escaped message there would return something that is not what
// the run produced — a diagnosis rewritten on the way out, which is worse than
// one rendered awkwardly.
func TestTheJSONTimelineKeepsTheValueAsItIs(t *testing.T) {
	t.Parallel()

	raw := "boom\nsecond line\there"

	var out, errs bytes.Buffer
	surface := &ui.UI{
		Out:     &out,
		Err:     &errs,
		Caps:    ui.Capabilities{Profile: colorprofile.NoTTY},
		ErrCaps: ui.Capabilities{Profile: colorprofile.NoTTY},
	}

	require.NoError(t, writeJSON(surface, FormatJSON, &v1.GetTimelineResponse{
		Entries: []*v1.TimelineEntry{{
			EventId: 5,
			Kind:    v1.TimelineEntry_KIND_STEP_FAILED,
			Failure: raw,
		}},
	}))

	var back v1.GetTimelineResponse
	require.NoError(t, protojson.Unmarshal(out.Bytes(), &back))

	require.Len(t, back.GetEntries(), 1)
	assert.Equal(t, raw, back.GetEntries()[0].GetFailure(),
		"the machine-readable answer no longer round-trips what the run produced")
}

// TestAnExhaustedContinuationDoesNotClaimTheRunDidNothing keeps the command
// from contradicting what it has already printed.
//
// An answer that exactly fills its entry ceiling is reported as truncated
// whether or not anything follows it, so a caller who resumes can legitimately
// get an empty, complete page — after having read every entry the run has. The
// two empty answers are different facts and only the caller knows which is
// which (Codex, #1119).
func TestAnExhaustedContinuationDoesNotClaimTheRunDidNothing(t *testing.T) {
	t.Parallel()

	empty := &v1.GetTimelineResponse{}

	first := renderedTimeline(t, false, empty)
	assert.Contains(t, first, "recorded nothing yet",
		"a first answer with no entries is a run that has not done anything")

	continued := renderedTimeline(t, true, empty)
	assert.NotContains(t, continued, "recorded nothing yet",
		"a caller who just read four pages was told the run recorded nothing")
	assert.Contains(t, continued, "end of this run's account")
}

// renderedTimeline is what a person sees, both streams joined, since the
// account goes to one and the notes about it to the other.
func renderedTimeline(t *testing.T, resumed bool, msg *v1.GetTimelineResponse) string {
	t.Helper()

	var out, errs bytes.Buffer
	renderTimeline(&ui.UI{
		Out:     &out,
		Err:     &errs,
		Caps:    ui.Capabilities{Profile: colorprofile.NoTTY},
		ErrCaps: ui.Capabilities{Profile: colorprofile.NoTTY},
	}, resumed, msg)

	return out.String() + errs.String()
}
