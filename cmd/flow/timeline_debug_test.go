package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func debugTimelineFixture() *v1.GetTimelineResponse {
	return &v1.GetTimelineResponse{Entries: []*v1.TimelineEntry{
		{EventId: 5, Kind: v1.TimelineEntry_KIND_STEP_SCHEDULED, Step: "`deploy`", Attempt: 1, Occurrence: 1},
		{EventId: 6, Kind: v1.TimelineEntry_KIND_DEBUG_PAUSED, Step: "debug lease s1 held by sre expires", SessionId: "s1", Actor: "sre"},
		{EventId: 7, Kind: v1.TimelineEntry_KIND_DEBUG_RESUMED, Step: "debug lease s1 held by sre expires", SessionId: "s1", Actor: "sre", EndReason: "released"},
		{EventId: 8, Kind: v1.TimelineEntry_KIND_DEBUG_RESUMED, Step: "debug lease s2 held by sre expires", SessionId: "s2", Actor: "sre", EndReason: "lapsed"},
		{EventId: 9, Kind: v1.TimelineEntry_KIND_STEP_SCHEDULED, Step: "`deploy`", Attempt: 1, Occurrence: 2},
		{EventId: 10, Kind: v1.TimelineEntry_KIND_STEP_COMPLETED, Step: "`deploy`", Attempt: 1, Occurrence: 2},
	}}
}

// TestTheTimelineTableNamesDebugPausesAndRepeatedSteps pins the labels, that
// the occurrence shows only from the second execution on, and that the table
// keeps to one row per entry.
func TestTheTimelineTableNamesDebugPausesAndRepeatedSteps(t *testing.T) {
	t.Parallel()

	printed := renderedTimeline(t, false, debugTimelineFixture())
	lines := strings.Split(strings.TrimRight(printed, "\n"), "\n")
	require.Len(t, lines, 7, printed)

	assert.Contains(t, lines[1], "`deploy`")
	assert.NotContains(t, lines[1], "#", "the first execution says nothing about its ordinal")
	assert.Contains(t, lines[2], "debug paused")
	assert.Contains(t, lines[3], "debug resumed (released)")
	assert.Contains(t, lines[4], "debug resumed (lapsed)")
	assert.Contains(t, lines[5], "`deploy` #2")
	assert.Contains(t, lines[6], "`deploy` #2")
}

// TestTheJSONTimelineCarriesTheDebugFields is the machine-readable half: the
// answer is the proto message itself, so the new fields round-trip by name.
func TestTheJSONTimelineCarriesTheDebugFields(t *testing.T) {
	t.Parallel()

	var out, errs bytes.Buffer
	surface := &ui.UI{
		Out:     &out,
		Err:     &errs,
		Caps:    ui.Capabilities{Profile: colorprofile.NoTTY},
		ErrCaps: ui.Capabilities{Profile: colorprofile.NoTTY},
	}
	require.NoError(t, writeJSON(surface, FormatJSON, debugTimelineFixture()))

	for _, want := range []string{
		`"KIND_DEBUG_PAUSED"`, `"KIND_DEBUG_RESUMED"`, `"sessionId": "s1"`, `"actor": "sre"`,
		`"endReason": "released"`, `"endReason": "lapsed"`, `"occurrence": 2`,
	} {
		assert.Contains(t, out.String(), want)
	}

	var back v1.GetTimelineResponse
	require.NoError(t, protojson.Unmarshal(out.Bytes(), &back))
	assert.EqualValues(t, 2, back.GetEntries()[4].GetOccurrence())
	assert.Equal(t, "lapsed", back.GetEntries()[3].GetEndReason())
}
