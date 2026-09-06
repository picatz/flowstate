package server_test

import (
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestListPagesThroughEveryRunExactlyOnce walks a real namespace with the
// signed token and checks the set, which is what #1772 asks of the token: the
// pages partition the runs, every one reached and none twice.
//
// The mock-backed walks in list_scan_test.go prove the loop; this proves the
// token round-trips through Temporal's own position, which the mocks stand in
// for. A hundred and fifty runs against a page of forty is four pages, the last
// of them partial, and every position handed back is one Temporal produced —
// the thing a signed cursor carries and must not disturb.
//
// No worker is started: the runs sit unclaimed, which is all a listing needs
// of them and considerably cheaper than a hundred and fifty runs progressing.
func TestListPagesThroughEveryRunExactlyOnce(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	server := mustNew(t, temporal)

	const total, pageSize = 150, 40

	started := make(map[string]bool, total)
	for range total {
		response, err := server.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
			Workflow: gatedWorkflow(),
		}))
		require.NoError(t, err)
		started[response.Msg.GetWorkflowId()] = true
	}
	require.Len(t, started, total)

	// walk pages the whole listing and reports what it saw. It reports rather
	// than asserts because it runs under Eventually, for the reason listRunIDs
	// gives: visibility is written asynchronously, so a walk taken too early is
	// legitimately short, and the walk is repeated until it is not.
	walk := func() (seen map[string]int, pages int, err error) {
		seen = map[string]int{}
		token := ""

		for {
			response, err := server.List(t.Context(), connect.NewRequest(&v1.ListRequest{
				PageSize:  pageSize,
				PageToken: token,
			}))
			if err != nil {
				return nil, pages, err
			}

			pages++
			if pages > 2*total/pageSize+2 {
				return nil, pages, fmt.Errorf("the listing did not terminate after %d pages", pages)
			}

			for _, run := range response.Msg.GetRuns() {
				seen[run.GetWorkflowId()]++
			}

			token = response.Msg.GetNextPageToken()
			if token == "" {
				return seen, pages, nil
			}
		}
	}

	var seen map[string]int
	var pages int
	var walkErr error
	require.Eventually(t, func() bool {
		seen, pages, walkErr = walk()
		return walkErr == nil && len(seen) == total
	}, 90*time.Second, 250*time.Millisecond,
		"the listing never reached all %d runs; last walk saw %d over %d pages (%v)", total, len(seen), pages, walkErr)

	require.Greater(t, pages, 1, "one page held every run, so no token was ever exchanged")

	for id := range started {
		require.Equal(t, 1, seen[id], "run %q was skipped or returned twice", id)
	}
	for id := range seen {
		require.True(t, started[id], "the listing returned a run %q this test did not start", id)
	}
}
