package server

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSubmitPathsRefuseAModuleSpec is the server's half of the module rule. A
// module is a spec with no steps, and a spec built by hand over the API can say
// anything the compiler would not, so the refusal has to hold in the handlers
// themselves: Run and CreateSchedule both turn it away at the schema, before a
// tenant lookup, an admission decision or a Temporal call. The zero-value server
// proves it: nothing past the validation could have run.
func TestSubmitPathsRefuseAModuleSpec(t *testing.T) {
	t.Parallel()

	module := &v1.Workflow{
		Name:           "ids",
		DeclaredErrors: []*v1.ErrorDeclaration{{Name: "NotFound"}},
	}
	s := &FlowstateServer{}

	_, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: module}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "Run")
	assert.Contains(t, err.Error(), "steps", "refused for having no steps, not for some other reason")

	_, err = s.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{Workflow: module}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "CreateSchedule")
}
