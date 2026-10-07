package server

import (
	"context"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

func listGatesRequest(pageSize int32, token string, answerableOnly bool) *connect.Request[v1.ListGatesRequest] {
	return connect.NewRequest(&v1.ListGatesRequest{
		WorkflowId: "orders-1", PageSize: pageSize, PageToken: token, AnswerableOnly: answerableOnly,
	})
}

// walkGates reads a whole listing a page at a time, as a client does, and
// returns the signal names in the order they came back and how many pages it took.
func walkGates(t *testing.T, s *FlowstateServer, ctx context.Context, pageSize int32, answerableOnly bool) (names []string, pages int, last *v1.ListGatesResponse) {
	t.Helper()

	token := ""
	for {
		resp, err := s.ListGates(ctx, listGatesRequest(pageSize, token, answerableOnly))
		require.NoError(t, err)

		pages++
		last = resp.Msg
		for _, gate := range resp.Msg.GetGates() {
			names = append(names, gate.GetSignalName())
		}

		if resp.Msg.GetNextPageToken() == "" {
			return names, pages, last
		}
		token = resp.Msg.GetNextPageToken()
		require.Less(t, pages, 1000, "the listing never ended")
	}
}

func gateCaller(subject string, actions ...string) context.Context {
	return auth.ContextWithPrincipal(context.Background(), auth.Principal{
		Issuer: "https://issuer.example", Subject: subject, Actions: actions,
	})
}

// TestListGatesReadsEveryHeldGateInPages: a run holding more gates than its
// summary lists is listed in full, oldest first, across pages that each respect
// the page size, with every gate reported as GetGate reports it, and the
// listing ends on an empty token.
func TestListGatesReadsEveryHeldGateInPages(t *testing.T) {
	t.Parallel()

	const gates, size = v1.MaxPendingWaits + 6, 25

	fake := crowdedRun(t, gates, nil)
	s := mustNew(t, fake)

	names, pages, last := walkGates(t, s, gateCaller("approver", "workload.signal"), size, false)

	want := make([]string, 0, gates)
	for i := range gates {
		want = append(want, fmt.Sprintf("n%d", i))
	}
	require.Equal(t, want, names, "a gate was skipped, repeated or listed out of order, including those past the summary bound")
	require.Equal(t, 3, pages)
	require.False(t, last.GetTruncated())
	require.Zero(t, fake.signals, "listing gates delivers nothing")

	// The listing and the single lookup agree about a gate.
	resp, err := s.ListGates(gateCaller("approver", "workload.signal"), listGatesRequest(size, "", false))
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetGates(), size, "a page held more than the size asked for")
	one, err := s.GetGate(gateCaller("approver", "workload.signal"), gateRequest("n3"))
	require.NoError(t, err)
	require.Equal(t, one.Msg.GetStepId(), resp.Msg.GetGates()[3].GetStepId())
	require.Equal(t, one.Msg.GetPrompt(), resp.Msg.GetGates()[3].GetPrompt())
	require.Equal(t, "r-1", resp.Msg.GetGates()[0].GetRunId())
}

// TestListGatesFiltersToTheGatesTheCallerMayAnswer: with a `signals:` policy
// that admits the approver on the even gates only, answerable_only lists those
// and no others, across pages, while the unfiltered list shows the caller the
// rest read-only without the question, and a caller the policy refuses everywhere
// gets none.
func TestListGatesFiltersToTheGatesTheCallerMayAnswer(t *testing.T) {
	t.Parallel()

	const gates, size = v1.MaxPendingWaits + 6, 10

	policy := map[string]*v1.SignalPolicy{}
	var even []string
	for i := range gates {
		who := "somebody-else"
		if i%2 == 0 {
			who = "approver"
			even = append(even, fmt.Sprintf("n%d", i))
		}
		policy[fmt.Sprintf("n%d", i)] = &v1.SignalPolicy{Allow: fmt.Sprintf(`sender.identity.principal == "https://issuer.example#%s"`, who)}
	}

	fake := crowdedRun(t, gates, policy)
	s := mustNew(t, fake)
	approver := gateCaller("approver", "workload.signal")

	names, _, _ := walkGates(t, s, approver, size, true)
	require.Equal(t, even, names, "the filtered list included a gate the policy refuses, or lost one it admits")

	all, _, _ := walkGates(t, s, approver, size, false)
	require.Len(t, all, gates)

	resp, err := s.ListGates(approver, listGatesRequest(maxListGatesPageSize+1, "", false))
	require.Error(t, err, "a page size over the schema's bound was accepted")
	require.Nil(t, resp)

	resp, err = s.ListGates(approver, listGatesRequest(maxListGatesPageSize, "", false))
	require.NoError(t, err)
	for i, gate := range resp.Msg.GetGates() {
		require.Equal(t, i%2 == 0, gate.GetMayAnswer(), gate.GetSignalName())
		require.Equal(t, i%2 == 0, gate.GetPrompt() != "", "%s: the question is for the people the policy admits", gate.GetSignalName())
		require.Equal(t, i%2 == 0, gate.GetStarter() != "", gate.GetSignalName())
	}

	bystander, _, last := walkGates(t, s, gateCaller("bystander", "workload.signal"), size, true)
	require.Empty(t, bystander)
	require.False(t, last.GetTruncated())
}

// TestListGatesIsBoundToTheSignalAction: a caller without `workload.signal`,
// including one holding only `workload.read`, is refused before the run is
// addressed or asked, and the refusal is recorded under the RPC's own name.
func TestListGatesIsBoundToTheSignalAction(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		actions []string
		allowed bool
	}{
		"signal only": {actions: []string{"workload.signal"}, allowed: true},
		"read only":   {actions: []string{"workload.read"}},
		"neither":     {actions: []string{"workload.cancel"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := crowdedRun(t, 3, nil)
			sink := &recordingEmitter{}
			s := mustNew(t, fake, WithAudit(recorderFor(t, sink)))

			resp, err := s.ListGates(revealer(t.Context(), tc.actions...), listGatesRequest(0, "", false))
			require.Len(t, sink.records, 1, "one decision, one record")
			require.Equal(t, "ListGates", sink.records[0].GetRpc())

			if !tc.allowed {
				require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
				require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, sink.records[0].GetDecision())
				require.Zero(t, fake.describes, "the run was addressed for a caller who may not read gates")
				require.Zero(t, fake.pageQueries, "the run was asked for its gates for a caller who may not read them")

				return
			}

			require.NoError(t, err)
			require.Len(t, resp.Msg.GetGates(), 3)
			require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, sink.records[0].GetDecision())
		})
	}
}

// TestListGatesNeverSaysNoGatesWhenItCouldNotAsk: a run that cannot answer the
// listing is Unavailable, not an empty list; a run that is not running is
// NotFound; a run that retains fewer gates than it holds says the listing is
// truncated, on every page.
func TestListGatesNeverSaysNoGatesWhenItCouldNotAsk(t *testing.T) {
	t.Parallel()

	approver := gateCaller("approver", "workload.signal")

	t.Run("no worker answering", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, 3, nil)
		fake.held = nil
		s := mustNew(t, fake)

		resp, err := s.ListGates(approver, listGatesRequest(0, "", false))
		require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
		require.Nil(t, resp)
	})

	t.Run("not running", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, 3, nil)
		fake.describe.WorkflowExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s := mustNew(t, fake)

		_, err := s.ListGates(approver, listGatesRequest(0, "", false))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		require.Zero(t, fake.pageQueries)
	})

	t.Run("running with no open gate", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, 0, nil)
		s := mustNew(t, fake)

		resp, err := s.ListGates(approver, listGatesRequest(0, "", false))
		require.NoError(t, err)
		require.Empty(t, resp.Msg.GetGates())
		require.Empty(t, resp.Msg.GetNextPageToken())
		require.False(t, resp.Msg.GetTruncated())
	})

	t.Run("truncated", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, 3, nil)
		fake.held.incomplete = true
		s := mustNew(t, fake)

		resp, err := s.ListGates(approver, listGatesRequest(0, "", false))
		require.NoError(t, err)
		require.Len(t, resp.Msg.GetGates(), 3)
		require.True(t, resp.Msg.GetTruncated(), "a run parked on gates it did not keep said its list was complete")
	})
}

// TestListGatesTokensAreBoundToTheListingTheyContinue: a token continues only
// the listing it was issued for, by the caller's tenant, on the run it was
// counted on; a forged one, one for a different filter or page size, and one from
// an earlier run of the workload are each refused, never read as a position.
func TestListGatesTokensAreBoundToTheListingTheyContinue(t *testing.T) {
	t.Parallel()

	approver := gateCaller("approver", "workload.signal")

	fake := crowdedRun(t, 10, nil)
	s := mustNew(t, fake)

	first, err := s.ListGates(approver, listGatesRequest(4, "", false))
	require.NoError(t, err)
	token := first.Msg.GetNextPageToken()
	require.NotEmpty(t, token)

	second, err := s.ListGates(approver, listGatesRequest(4, token, false))
	require.NoError(t, err)
	require.Equal(t, "n4", second.Msg.GetGates()[0].GetSignalName())

	for name, tc := range map[string]struct {
		size   int32
		token  string
		answer bool
	}{
		"forged":             {size: 4, token: token[:len(token)-2] + "AA"},
		"another filter":     {size: 4, token: token, answer: true},
		"another page size":  {size: 5, token: token},
		"not a token at all": {size: 4, token: "n4"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.ListGates(approver, listGatesRequest(tc.size, tc.token, tc.answer))
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}

	// The workload continued on to a later run: arrival numbers restart there,
	// so the old position names nothing.
	fake.describe.WorkflowExecutionInfo.Execution.RunId = "r-2"
	_, err = s.ListGates(approver, listGatesRequest(4, token, false))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}
