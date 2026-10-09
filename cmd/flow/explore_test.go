package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

func TestExploreReadsAWorkflowsRunsByName(t *testing.T) {
	fake := &fakeWorkflowService{listResponses: []*v1.ListResponse{{
		Runs:          []*v1.RunSummary{liveRun("billing", v1.RunResponse_STATUS_FAILED)},
		NextPageToken: "more",
	}}}
	serveFake(t, fake)

	cmd := newExploreCommand()
	require.NoError(t, cmd.ParseFlags(nil))

	runs, more, err := graphSources{live: true}.runsOf(cmd)(t.Context(), `bill"ing\`)
	require.NoError(t, err)
	assert.Len(t, runs, 1)
	assert.True(t, more, "a next page token means there are runs this page leaves out")
	assert.Equal(t, `name == "bill\"ing\\"`, fake.lastListFilter, "a name is quoted, never spliced into the expression")

	_, _, err = graphSources{live: true, filter: `status == "FAILED"`}.runsOf(cmd)(t.Context(), "billing")
	require.NoError(t, err)
	assert.Equal(t, `(status == "FAILED") && name == "billing"`, fake.lastListFilter, "--filter narrows the runs as it narrows the counts")
}

func TestExploreSaysWhatTheServerSaidWhenRunsCannotBeRead(t *testing.T) {
	for name, tc := range map[string]struct {
		response *v1.ListResponse
		want     string
	}{
		"a filter the server could not evaluate": {&v1.ListResponse{FilterDiagnostic: "no such field"}, "could not evaluate --filter: no such field"},
		"runs the filter errored on":             {&v1.ListResponse{ExcludedByError: 3}, "3 runs were left out"},
	} {
		t.Run(name, func(t *testing.T) {
			serveFake(t, &fakeWorkflowService{listResponses: []*v1.ListResponse{tc.response}})
			cmd := newExploreCommand()
			require.NoError(t, cmd.ParseFlags(nil))

			_, _, err := graphSources{live: true}.runsOf(cmd)(t.Context(), "billing")
			require.ErrorContains(t, err, tc.want)
		})
	}
}
