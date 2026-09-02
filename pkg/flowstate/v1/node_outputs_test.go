package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestEmptyNodeOutputsIsValidAndRoundTripsInSignalRequest(t *testing.T) {
	request := &v1.SignalRequest{
		WorkflowId: "run",
		Name:       "approved",
		Payload:    &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}},
	}
	require.NoError(t, v1.Validate(request))

	wire, err := proto.Marshal(request)
	require.NoError(t, err)

	var decoded v1.SignalRequest
	require.NoError(t, proto.Unmarshal(wire, &decoded))
	require.NotNil(t, decoded.GetPayload(), "an explicitly empty payload became absent on the wire")
	require.Empty(t, decoded.GetPayload().GetNamedValues())
}

func TestNodeOutputsStillRejectsAnEmptyName(t *testing.T) {
	err := v1.Validate(&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"": v1.NewLiteral(true)}})
	require.ErrorContains(t, err, "named_values")
}
