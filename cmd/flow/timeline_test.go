package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
