package flowdebug_test

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestStepStartLinesAgreesWithSiteAtLine: resolving a range in one pass
// offers exactly the lines where the per-line answer is a step that starts
// there, nested spans and a range at the largest line included.
func TestStepStartLinesAgreesWithSiteAtLine(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(root, []byte(journeyFlowfile), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.yaml"), []byte(childFlowfile), 0o600))
	workflow, positions, err := flowfile.ParseFile(root)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(root, []byte(journeyFlowfile), workflow, positions)

	const to = 80
	var want []uint32
	for line := uint32(1); line <= to; line++ {
		_, location, why := flowdebug.SiteAtLine(sourceMap, &v1.DebugSourceLine{Uri: root, Line: line})
		if why == "" && location.GetRange().GetStartLine() == line {
			want = append(want, line)
		}
	}
	require.NotEmpty(t, want)

	var got []uint32
	for _, location := range flowdebug.StepStartLines(sourceMap, "file://"+root, 1, to) {
		got = append(got, location.GetRange().GetStartLine())
	}
	assert.Equal(t, want, got)

	assert.Empty(t, flowdebug.StepStartLines(sourceMap, filepath.Join(dir, "other.yaml"), 1, to), "a source the map was not compiled from has no steps")
	assert.Empty(t, flowdebug.StepStartLines(sourceMap, root, to, 1), "an empty range has no steps")
	assert.Empty(t, flowdebug.StepStartLines(sourceMap, root, math.MaxUint32, math.MaxUint32), "the largest line wraps nothing")
	assert.Empty(t, flowdebug.StepStartLines(nil, root, 1, to))
}

// TestStepStartLinesBreaksTiesAsSiteAtLineDoes pins the winner rule on a map
// built to need it: the narrowest span wins, the later of equal spans wins, a
// reversed span covers only its start line, and the ends of the line range
// resolve without wrapping.
func TestStepStartLinesBreaksTiesAsSiteAtLineDoes(t *testing.T) {
	t.Parallel()

	const uri = "/synthetic.yaml"
	entry := func(id string, start, end uint32) *v1.DebugSourceEntry {
		return &v1.DebugSourceEntry{
			Site:     &v1.DebugSite{Workflow: "w", Path: []string{id}, Kind: "step"},
			Location: &v1.DebugSourceLocation{NodeId: id, Range: &v1.SourceRange{StartLine: start, EndLine: end}},
		}
	}
	sourceMap := &v1.DebugSourceMap{
		Documents: []*v1.DebugSourceDocument{{Uri: uri}},
		Entries: []*v1.DebugSourceEntry{
			entry("inner", 5, 6), // starts on a line the outer covers
			entry("first", 8, 8), // identical spans: the later one wins
			entry("second", 8, 8),
			entry("outer", 2, 10),     // listed after what it contains, so only width ranks it below them
			entry("reversed", 12, 11), // End < Start covers its start line only
			entry("early", 14, 15),    // equal widths overlapping on 15: the later wins
			entry("late", 15, 16),
			entry("zero", 0, 0),
			entry("top", math.MaxUint32, math.MaxUint32),
		},
	}

	resolve := func(from, to uint32) map[uint32]string {
		got := map[uint32]string{}
		for _, location := range flowdebug.StepStartLines(sourceMap, uri, from, to) {
			got[location.GetRange().GetStartLine()] = location.GetNodeId()
		}

		return got
	}
	want := func(from, to uint32) map[uint32]string {
		expected := map[uint32]string{}
		for line := from; ; line++ {
			_, location, why := flowdebug.SiteAtLine(sourceMap, &v1.DebugSourceLine{Uri: uri, Line: line})
			if why == "" && location.GetRange().GetStartLine() == line {
				expected[line] = location.GetNodeId()
			}
			if line == to {
				break
			}
		}

		return expected
	}

	for _, span := range [][2]uint32{{0, 20}, {math.MaxUint32 - 2, math.MaxUint32}} {
		assert.Equal(t, want(span[0], span[1]), resolve(span[0], span[1]), "range %v", span)
	}
	// Not vacuous: the tie-breaks decide these lines.
	all := resolve(0, 20)
	assert.Equal(t, "second", all[8])
	assert.Equal(t, "late", all[15])
	assert.Equal(t, "inner", all[5])
	assert.Equal(t, "reversed", all[12])
	assert.Equal(t, "zero", all[0])
	assert.Equal(t, "top", resolve(math.MaxUint32, math.MaxUint32)[math.MaxUint32])
}
