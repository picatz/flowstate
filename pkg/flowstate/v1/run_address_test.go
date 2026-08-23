package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunAddressShapeLocal checks the local half of the shared assertion in
// [conformance.AssertRunAddressShape]: a local run has no address at all — no server
// in front of it, no Temporal behind it, nothing that could reach it by any
// name — so it answers the sentinel rather than an empty string. The durable
// half is [engine_test.TestRunAddressShapeDurable].
func TestRunAddressShapeLocal(t *testing.T) {
	outputs, err := v1.Run(context.Background(), conformance.RunAddressWorkflow())
	require.NoError(t, err)

	conformance.AssertRunAddressShape(t, outputs, v1.LocalRunAddress, v1.LocalRunAddress)
}

// TestLocalRunAddressIsNotBlank pins the rule the sentinel exists for, at the
// constant rather than through a run: whatever the local driver answers, it is
// never the empty string. A future change that "simplified" this to a blank
// would pass every assertion written in terms of the constant itself, so this
// asserts the property and not the spelling.
func TestLocalRunAddressIsNotBlank(t *testing.T) {
	require.NotEmpty(t, v1.LocalRunAddress)

	address := v1.NewLocalRunAddress()
	require.NotEmpty(t, address.GetWorkflowId())
	require.NotEmpty(t, address.GetRunId())
}
