package server

import (
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestGetBoundsTheWorkflowIDItself holds Get to the schema's workflow_id bound
// without the CLI's protovalidate interceptor in front of it: an id one byte
// past [v1.MaxWorkflowIDLen] is refused before Temporal is addressed, and one at
// the bound reaches it.
func TestGetBoundsTheWorkflowIDItself(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		length    int
		code      connect.Code
		describes int
	}{
		{name: "past the bound", length: v1.MaxWorkflowIDLen + 1, code: connect.CodeInvalidArgument},
		{name: "at the bound", length: v1.MaxWorkflowIDLen, describes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			workflowID := strings.Repeat("w", test.length)
			temporal := &fakeRunClient{describe: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8"},
					Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					Memo:      mineMemo(t),
				},
			}}
			s := mustNew(t, temporal)

			_, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
			if test.code == 0 {
				require.NoError(t, err)
			} else {
				require.Equal(t, test.code, connect.CodeOf(err), "%v", err)
			}
			require.Equal(t, test.describes, temporal.describes, "lookups Temporal was asked for")
		})
	}
}
