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
