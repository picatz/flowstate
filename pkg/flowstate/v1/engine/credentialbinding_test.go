package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestCredentialBindingDurable is the second caller of
// [conformance.CredentialBindingCases]: the identical cases the local driver
// runs in credentialbinding_local_test.go, through the expansion the server's
// admission performs and a worker registered with the catalog the run is pinned
// to.
//
// The pairing is the point. A binding is expanded before either driver sees the
// specification, so the two agree only if the durable path is handed the
// expanded specification and passes the references through unresolved: a step
// that received the wrong reference, or a reference turned into a value before
// the task, would still run, and the second is a secret in workflow history.
func TestCredentialBindingDurable(t *testing.T) {
	require.NoError(t, v1.DefaultRegistry().Register(conformance.BoundCredentialTaskDef()))
	require.NoError(t, v1.DefaultRegistry().Register(conformance.FederatedCredentialTaskDef()))
	t.Cleanup(func() {
		v1.DefaultRegistry().Unregister(conformance.BoundCredentialTaskName)
		v1.DefaultRegistry().Unregister(conformance.FederatedCredentialTaskName)
	})

	for _, test := range conformance.CredentialBindingCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
			conformance.RequireNoExchange(t, test)
		})
	}
}
