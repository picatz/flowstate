package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestSchedulesAreListedByTheirTenantsSearchAttribute is #1785 through the
// real visibility store: a schedule created by a deployment with search
// attributes registered carries its tenant as an attribute, the listing asks
// Temporal for that tenant's entries rather than walking everyone's, and a
// schedule created before registration — carrying no attribute — is still the
// tenant's and still listed, so the upgrade that adds the attribute hides
// nothing.
//
// Three servers on one cluster: two tenants with registration confirmed, and
// the first tenant again without it, which is what every server before the
// attribute was and what a deployment whose registration failed still is.
func TestSchedulesAreListedByTheirTenantsSearchAttribute(t *testing.T) {
	t.Parallel()

	temporal, namespace := newTemporalNamespace(t)
	require.NoError(t, server.EnsureSearchAttributesRegistered(t.Context(), temporal, namespace))

	teamA := mustNew(t, temporal, server.WithNamespace(teamANamespace), server.WithSearchAttributesRegistered())
	teamB := mustNew(t, temporal, server.WithNamespace(teamBNamespace), server.WithSearchAttributesRegistered())
	teamABefore := mustNew(t, temporal, server.WithNamespace(teamANamespace))

	create := func(s *server.FlowstateServer, name string) {
		t.Helper()
		_, err := s.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
			Workflow: scheduledWorkflow(name),
			Inputs:   map[string]*v1.Value{"attempts": v1.NewLiteral(int64(1))},
			Paused:   true,
		}))
		require.NoError(t, err)
	}
	create(teamA, "tagged-one")
	create(teamA, "tagged-two")
	create(teamB, "theirs")
	create(teamABefore, "from-before")

	names := func(s *server.FlowstateServer) map[string]bool {
		listed, err := s.ListSchedules(t.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
		if err != nil {
			return nil
		}
		out := map[string]bool{}
		for _, schedule := range listed.Msg.GetSchedules() {
			out[schedule.GetName()] = true
		}
		return out
	}

	// Visibility follows a create by a moment, so the assertion waits for the
	// store rather than for the first answer.
	require.Eventually(t, func() bool {
		return len(names(teamA)) == 3
	}, 30*time.Second, 200*time.Millisecond, "team-a never saw its three schedules: %v", names(teamA))

	require.Equal(t, map[string]bool{"tagged-one": true, "tagged-two": true, "from-before": true}, names(teamA),
		"the listing by attribute dropped a schedule created before the attribute, or admitted another tenant's")
	require.Equal(t, map[string]bool{"theirs": true}, names(teamB))

	// And the attribute is really on the schedule, not only on the run it
	// fires: what Temporal indexed is what the query filters on.
	tagged := 0
	iterator, err := temporal.ScheduleClient().List(t.Context(), client.ScheduleListOptions{
		Query: "FlowstateNamespace = '" + teamANamespace + "'",
	})
	require.NoError(t, err)
	for iterator.HasNext() {
		entry, err := iterator.Next()
		require.NoError(t, err)
		require.NotNil(t, entry.SearchAttributes, "a schedule Temporal returned for the attribute carries none")
		tagged++
	}
	require.Equal(t, 2, tagged, "team-a's schedules created with registration confirmed are the tagged ones")
}
