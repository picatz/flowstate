package flowfile

import (
	"fmt"
	"strings"
	"testing"

	"github.com/goccy/go-yaml/parser"
	"github.com/stretchr/testify/require"
)

// #2106's independent review, round two: right-to-left removal (round one's
// own fix) was correct, but every anchor sharing a line still cost a full,
// independent rescan of that line — [byteOffsetOfColumn] restarting from
// column one, and the whole line rebuilt from scratch — so k anchors on one
// line of length L cost O(k·L), none of it charged to any budget. Before
// round one's fix, left-to-right processing refused at the second anchor on
// a shared line before this cost was ever reached; round one's own fix
// reaches it for the first time. Measured directly: 5,000 anchors on one
// flow-sequence line cost 0.58s, 20,000 cost 5.6s, 40,000 cost 17.2s — the
// square-law growth an O(k·L) scan produces, not the roughly-flat cost an
// O(L+k) one would.
//
// [byteOffsetsOfColumns] and [dropMarkersFromLine] are round two's fix: find
// every marker's column in one forward pass over the line, then rewrite it
// in one further pass, rather than one restart-from-scratch pass per
// marker. This file's own test checks that on a counter
// ([aliasInliner.markerScanSteps]) rather than on wall time, per the
// review's own preference: a counter is deterministic and fails a
// regression by a number rather than by how loaded the machine happens to
// be when the test runs.

// manyAnchorsOneLineFlowfile builds a Flowfile whose `vars:` block holds one
// long flow-style sequence, all on one line, of n bare anchors — none of
// them aliased, so nothing here is about the splice path, only about
// dropping n markers that all share one line close to [maxBytes], the shape
// the finding measured.
func manyAnchorsOneLineFlowfile(n int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  x: [")
	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "&a%d %d", i, i)
	}
	b.WriteString("]\nsteps:\n  - id: a\n    log:\n      message: hi\n")

	return b.String()
}

// TestDropMarkersScanIsLinearInLineLength is #2106 round two's own
// acceptance check: dropping every marker from a line close to [maxBytes]
// and holding tens of thousands of anchors has to scan that line a small,
// fixed number of times — bounded by a constant factor of the line's own
// length — never by the anchor count multiplied in. An O(k·L) regression
// fails this by orders of magnitude, the same way the finding's own
// measured times grew from 0.58s to 17.2s across a mere 8x increase in k;
// an O(L+k) implementation stays close to the line's own length regardless
// of how many anchors it holds.
func TestDropMarkersScanIsLinearInLineLength(t *testing.T) {
	t.Parallel()

	const anchors = 42_000
	src := manyAnchorsOneLineFlowfile(anchors)
	require.Less(t, len(src), maxBytes,
		"the fixture has to fit under the byte budget to reach dropMarkers at all")
	require.Greater(t, len(src), maxBytes/2,
		"the fixture is supposed to sit close to maxBytes, the shape the finding measured")

	data := []byte(src)
	file, err := parser.ParseBytes(data, parser.ParseComments)
	require.NoError(t, err)

	in, _, ok := runAliasInliner(data, file)
	require.True(t, ok, "expected this document to be accepted; refusals: %v", in.refusals)

	// k is small and fixed, not tuned to this fixture's own anchor count —
	// the property under test is that [aliasInliner.markerScanSteps] tracks
	// the document's own length, not the anchor count multiplied by it. An
	// O(k·L) regression fails this by orders of magnitude on 42,000 anchors,
	// not by a margin this bound would need tuning to catch.
	const k = 4
	bound := k * len(src)
	require.LessOrEqual(t, in.markerScanSteps, bound,
		"dropMarkers scanned %d steps against a %d-byte document — expected within %dx of the "+
			"document's own length, not disproportionately more, which is #2106's second round",
		in.markerScanSteps, len(src), k)
}

// TestDropMarkersFromLineMatchesTheOldSequentialAnswer calls
// [dropMarkersFromLine] directly, at the level [aliasInliner.dropMarkersOnLine]
// calls it, against shapes that pin the exact bytes it produces — the level
// this batched rewrite operates at, one line at a time, rather than a whole
// Flowfile.
//
// The trailing-whitespace case is the one round two's own single-pass build
// got wrong on the way to this fix: the first draft skipped only the one
// space [aliasInliner.dropMarker]'s original rule names, then appended
// whatever was left of the tail *after* trimming the prefix — so a second
// or third trailing space, never reached by the one-space skip, survived
// past the trim that was supposed to remove it. `http: &request` (nothing
// after it at all) already passed, because there was nothing left to
// survive; `http: &request   ` (the same anchor, three trailing spaces) is
// what actually exercises the bug, and is exactly the block-anchor shape
// [TestFixInlinesWholeValueAliases]'s "a mapping, copied with its comments"
// case already relies on this rule for — just without the extra spaces an
// author's editor is unlikely to leave but nothing here should depend on
// them not being there.
//
// The two-marker case is [dropMarkersFromLine]'s own general contract
// checked directly, at the byte level, independent of whether a shape that
// reaches it is realistic YAML: two sibling anchors are only ever collected
// as such — as distinct [ast.AnchorNode] values [aliasInliner.anchorNodes]
// holds and this function is handed columns and names for — when each one
// anchors a value of its own, which in valid, parsed YAML always leaves at
// least the value or a separator between them; this function's own
// contract does not depend on that being true, and a defect in how a blank
// tail composes across more than one marker would not necessarily show up
// with only one.
func TestDropMarkersFromLineMatchesTheOldSequentialAnswer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		text    string
		columns []int
		names   []string
		want    string
	}{
		{
			name:    "a lone marker with nothing after it at all",
			text:    "http: &request",
			columns: []int{7},
			names:   []string{"request"},
			want:    "http:",
		},
		{
			name:    "a lone marker followed by several trailing spaces",
			text:    "http: &request   ",
			columns: []int{7},
			names:   []string{"request"},
			want:    "http:",
		},
		{
			name:    "a value between the marker and the end still survives",
			text:    "message: &greeting hello",
			columns: []int{10},
			names:   []string{"greeting"},
			want:    "message: hello",
		},
		{
			// Not YAML this package would ever hand these two functions from a
			// real document (see the doc comment above), but exactly the shape
			// that would expose a blank tail composed incorrectly across two
			// markers: `&x`'s own gap to `&y` is blank, and so is `&y`'s own
			// tail to the end of the line — both trimmed away, along with the
			// space before `&x` itself, the same way one right-to-left pass
			// removing `&y` and then `&x` in turn would leave it.
			name:    "two markers whose gap and tail are both blank",
			text:    "a: &x &y  ",
			columns: []int{4, 7},
			names:   []string{"x", "y"},
			want:    "a:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, badIndex, notLocated, _, ok := dropMarkersFromLine(tt.text, tt.columns, tt.names)
			require.True(t, ok, "refused at index %d (notLocated=%v)", badIndex, notLocated)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestDropMarkersFromLineRefusesOverlappingMarkers is independent review's
// own finding on this rewrite: [byteOffsetsOfColumns] trusts the columns it
// is given and [dropMarkersFromLine]'s own validation loop only checked
// each marker against the text at its own offset, never against the marker
// before it — so two positions that each independently matched their own
// "&name", but claimed overlapping bytes, reached the build loop's slicing
// with markers that are not actually disjoint and in order, which is what
// every slice there assumes. `text[markers[i].at+markers[i].w : next]`
// panics with a negative-length slice bounds error instead of refusing,
// for a caller whose positions do not agree with the text the way the
// parser's own always should — goccy itself has no way to produce this,
// but "should never" is not "cannot", and main's own single-anchor
// dropMarker refuses the same shape rather than trusting it. Both cases
// are two markers whose *names* differ but whose *text* overlaps: the
// first is a byte a shorter name's own match consumes twice ("&ab" read as
// "&a" plus a one-byte overlap with "&ab" starting on the same "&"), the
// second is two names that together spell out one contiguous run of bytes
// ("&a&b" read as "&a&b" and, overlapping it, "&b").
func TestDropMarkersFromLineRefusesOverlappingMarkers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		text    string
		columns []int
		names   []string
	}{
		{
			name:    "two names matching the same leading byte",
			text:    "&ab x",
			columns: []int{1, 1},
			names:   []string{"a", "ab"},
		},
		{
			name:    "two names spelling out one overlapping run",
			text:    "&a&b x",
			columns: []int{1, 3},
			names:   []string{"a&b", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, badIndex, notLocated, _, ok := dropMarkersFromLine(tt.text, tt.columns, tt.names)
			require.False(t, ok, "expected overlapping markers to be refused rather than accepted")
			require.False(t, notLocated, "expected the same refusal a text mismatch gets, not a missing column")
			require.Equal(t, tt.text, got, "a refusal must not rewrite anything")
			require.Equal(t, 1, badIndex, "expected the second (overlapping) marker to be the one named")
		})
	}
}
