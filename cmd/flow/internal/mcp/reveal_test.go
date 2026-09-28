package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestAnAgentReadsNoTimelineFailureAServerDidNotDecide: an older server
// answers UNSPECIFIED and has redacted nothing, and a REVEALED answer the
// operator did not ask for is not the agent's to read.
func TestAnAgentReadsNoTimelineFailureAServerDidNotDecide(t *testing.T) {
	t.Parallel()

	const quoted = `GET https://api.example/synthetic-token-3c9d failed`
	for _, tc := range []struct {
		disclosure v1.SensitiveDisclosure
		operator   bool
		shown      bool
	}{
		{v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_UNSPECIFIED, false, false},
		{v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_UNSPECIFIED, true, false},
		{v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED, false, false},
		{v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED, true, true},
		{v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, false, true},
		{v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED, false, true},
	} {
		response := &v1.GetTimelineResponse{
			SensitiveDisclosure: tc.disclosure,
			Entries:             []*v1.TimelineEntry{{Failure: quoted}},
		}
		v1.WithholdUndecidedTimelineFailures(response, tc.operator)
		require.Equal(t, tc.shown, response.GetEntries()[0].GetFailure() == quoted, "%v operator=%v", tc.disclosure, tc.operator)
	}
}

// TestAnAgentCannotAskForWhatItsOperatorDidNot: the reveal_sensitive switch
// the server sees is the operator's posture, whatever the agent wrote. An
// agent cannot turn it on, and cannot leave it off when the operator started
// the surface with --reveal-sensitive.
func TestAnAgentCannotAskForWhatItsOperatorDidNot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		operator bool
		args     string
	}{
		{false, `{"workflowId": "orders-1", "revealSensitive": true}`},
		{true, `{"workflowId": "orders-1", "revealSensitive": true}`},
		{true, `{"workflowId": "orders-1"}`},
		{true, `{"workflowId": "orders-1", "revealSensitive": false}`},
	} {
		operator := tc.operator
		var sent *v1.GetRequest
		handler := dispatch(ServiceMethod{
			Name:  "Get",
			Input: (&v1.GetRequest{}).ProtoReflect().Descriptor(),
			Call: func(_ context.Context, _ *server.FlowstateServer,
				_ func() flowstatev1connect.WorkflowServiceClient, in proto.Message,
			) (proto.Message, error) {
				sent = proto.Clone(in).(*v1.GetRequest)
				return &v1.GetResponse{}, nil
			},
		}, nil, nil, Deps{
			RevealSensitive: operator,
			Redact:          func(r *v1.GetResponse) *v1.GetResponse { return r },
		})

		_, err := handler(t.Context(), &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
			Arguments: json.RawMessage(tc.args),
		}})
		require.NoError(t, err)
		require.NotNil(t, sent, "the call never reached the server, so nothing was asserted")
		require.Equal(t, "orders-1", sent.GetWorkflowId())
		require.Equal(t, operator, sent.GetRevealSensitive(), "operator posture %v, arguments %s", operator, tc.args)
	}
}
