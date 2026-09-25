//go:build linux

package temporaltest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// TestAttachUsesARunningServerAndLeavesItRunning pins the other half: a
// reachable server is used through the address alone, and closing what
// Attach returned leaves the server exactly as it was found. The server here
// is started by this test because it has one to hand; RunPackage would find
// it through AddressEnv.
func TestAttachUsesARunningServerAndLeavesItRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: boots a Temporal dev server")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	started, err := Start(ctx, &client.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = started.Stop() })

	running, err := Attach(ctx, started.FrontendHostPort(), &client.Options{})
	require.NoError(t, err)
	require.Equal(t, started.FrontendHostPort(), running.FrontendHostPort())

	_, err = running.Client().CheckHealth(ctx, &client.CheckHealthRequest{})
	require.NoError(t, err, "the attached client does not reach the server")

	// What RunPackage does at the end of the package: release the client, and
	// nothing else.
	running.close()

	_, err = started.Client().CheckHealth(ctx, &client.CheckHealthRequest{})
	require.NoError(t, err, "the server this process did not start stopped answering after the attached client closed")

	var asServer Server = running
	require.NotNil(t, asServer, "an attached server is what the package's tests read")
}
