package flowdebug_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestADriverArmsAndDeletesALineBreakpoint: the screen's gutter click is a
// driver call, resolved by the session through its source map, and the id it
// reports is the one `delete` takes.
func TestADriverArmsAndDeletesALineBreakpoint(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(root, []byte(journeyFlowfile), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.yaml"), []byte(childFlowfile), 0o600))
	workflow, positions, err := flowfile.ParseFile(root)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(root, []byte(journeyFlowfile), workflow, positions)

	run := startDebugWorkflow(t, workflow, func(opts *flowdebug.Options) { opts.SourceMap = sourceMap })
	waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)

	id := flowdebug.LineBreakpointID(root, 15)
	assert.NotContains(t, id, " ", "an id `delete` cannot take on a command line")
	assert.LessOrEqual(t, len(id), 128, "the contract bounds a breakpoint id")

	armed, err := driver.BreakLine(t.Context(), root, 15)
	require.NoError(t, err)
	require.Len(t, armed.Breakpoints, 1)
	assert.True(t, armed.Breakpoints[0].GetVerified(), armed.Breakpoints[0].GetMessage())
	assert.Equal(t, id, armed.Breakpoints[0].GetId())
	assert.Equal(t, []string{"each", "touch"}, armed.Breakpoints[0].GetSites()[0].GetPath())
	assert.Equal(t, "breakpoint at main.yaml:15\n", armed.Text, "the echo names the line, not an empty step")
	assert.Nil(t, armed.Unarmed)

	// A line no step is written on is taken and reported not armed, with why.
	blank, err := driver.BreakLine(t.Context(), root, 2)
	require.NoError(t, err)
	require.NotNil(t, blank.Unarmed)
	assert.Contains(t, blank.Unarmed.GetMessage(), "no step is written on line 2")
	assert.Contains(t, blank.Text, "not armed")

	// `delete` takes the id it printed, and the other breakpoint stays.
	deleted, err := driver.Do(t.Context(), "delete "+id)
	require.NoError(t, err)
	for _, state := range deleted.Breakpoints {
		assert.NotEqual(t, id, state.GetId())
	}
}

func TestSiteAtLineIsTheTargetsOwnAnswer(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(root, []byte(journeyFlowfile), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.yaml"), []byte(childFlowfile), 0o600))
	workflow, positions, err := flowfile.ParseFile(root)
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap(root, []byte(journeyFlowfile), workflow, positions)

	site, location, why := flowdebug.SiteAtLine(sourceMap, &v1.DebugSourceLine{Uri: "file://" + root, Line: 15})
	require.Empty(t, why)
	assert.Equal(t, []string{"each", "touch"}, site.GetPath())
	assert.NotNil(t, location.GetRange())

	site, _, why = flowdebug.SiteAtLine(sourceMap, &v1.DebugSourceLine{Uri: root, Line: 2})
	assert.Nil(t, site)
	assert.Contains(t, why, "no step is written on line 2")

	_, _, why = flowdebug.SiteAtLine(nil, &v1.DebugSourceLine{Uri: root, Line: 2})
	assert.True(t, strings.Contains(why, "no source map"))
}

func TestTwoFilesOfOneNameGetDifferentLineBreakpointIDs(t *testing.T) {
	t.Parallel()

	a, b := flowdebug.LineBreakpointID("/one/steps.yaml", 3), flowdebug.LineBreakpointID("/two/steps.yaml", 3)
	assert.NotEqual(t, a, b)
	assert.Equal(t, a, flowdebug.LineBreakpointID("file:///one/steps.yaml", 3), "two spellings of one file are one id")
	assert.NotEqual(t, a, flowdebug.LineBreakpointID("/one/steps.yaml", 4))
	assert.LessOrEqual(t, len(flowdebug.LineBreakpointID("/"+strings.Repeat("d", 4000)+"/"+strings.Repeat("n", 300)+".yaml", 99999)), 128)
}
