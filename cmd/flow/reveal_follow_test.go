package main

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// withholdingServer answers every Get as a server that withheld, and records
// whether each request asked to reveal.
type withholdingServer struct {
	flowstatev1connect.WorkflowServiceClient
	asked []bool
}

func (s *withholdingServer) Get(_ context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	s.asked = append(s.asked, req.Msg.GetRevealSensitive())
	return connect.NewResponse(&v1.GetResponse{
		Status:              v1.RunResponse_STATUS_RUNNING,
		SensitiveDisclosure: v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD,
	}), nil
}

// TestAFollowSaysOnceWhenTheServerWithheldItsReveal: `flow run` and `flow
// watch` announce "revealing" before the first poll, so a server that
// withholds must be reported, once, rather than left contradicting the
// notice for the whole follow.
func TestAFollowSaysOnceWhenTheServerWithheldItsReveal(t *testing.T) {
	t.Parallel()

	surface, _, stderr := plainSurface()
	server := &withholdingServer{}

	p := clientPoller{workflowID: "orders-1", client: server, reveal: true, withheld: noteWithheldOnce(surface)}
	for range 3 {
		_, err := p.Poll(t.Context())
		require.NoError(t, err)
	}
	require.Equal(t, []bool{true, true, true}, server.asked, "the follow did not ask the server to reveal")
	require.Equal(t, 1, strings.Count(stderr.String(), "the server withheld"), stderr.String())

	// Not asking is not a reveal the server refused.
	stderr.Reset()
	p.reveal = false
	_, err := p.Poll(t.Context())
	require.NoError(t, err)
	require.Empty(t, stderr.String())
}

// TestTheTimelineFooterAsksForWhatTheTimelineAsked: the retry footer's Get
// carries the timeline's own reveal request, so an authorized operator sees
// the same failure text in both.
func TestTheTimelineFooterAsksForWhatTheTimelineAsked(t *testing.T) {
	t.Parallel()

	for _, reveal := range []bool{false, true} {
		server := &withholdingServer{}
		surface, _, _ := plainSurface()
		noteRetryingSteps(t.Context(), surface, server, "orders-1", reveal, &v1.GetTimelineResponse{RunId: "r-1"})
		require.Equal(t, []bool{reveal}, server.asked)
	}
}
