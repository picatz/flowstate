package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExploreRefusesWhatGraphRefusesBeforeAnythingIsOpened(t *testing.T) {
	res := runFlow(t, "explore")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "name a Flowfile or a directory of them")

	res = runFlow(t, "explore", filepath.Join("..", "..", "examples"), "--filter", `status == "FAILED"`)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--filter narrows the runs that --live reads")

	res = runFlow(t, "explore", "--live", "--filter", "status ==")
	require.Error(t, res.Err, "a filter that does not compile is refused before the server is asked")
}

func TestExploreNeedsATerminalAndSaysWhereElseTheGraphIs(t *testing.T) {
	res := runFlow(t, "explore", filepath.Join("..", "..", "examples", "approval-gate"))

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "needs a terminal")
	assert.Contains(t, res.Err.Error(), "flow graph")
	assert.Empty(t, res.Stdout, "nothing is written to a stream that is not a screen")
}

func TestExploreDescribesItsSourcesForTheHeader(t *testing.T) {
	assert.Equal(t, "examples live", graphSources{paths: []string{"../../examples/"}, live: true}.describe())
	assert.Equal(t, "a b", graphSources{paths: []string{"x/a", "b"}}.describe())
	assert.Equal(t, "live", graphSources{live: true}.describe())
}
