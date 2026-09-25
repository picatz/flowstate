package temporaltest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// TestAttachRefusesAnAddressNobodyAnswersAndNamesIt pins the fail-closed half
// of attaching: a server that is not there is one error naming the address
// and the variable it came from, not a refused connection in every test.
func TestAttachRefusesAnAddressNobodyAnswersAndNamesIt(t *testing.T) {
	t.Parallel()

	// A port that was free a moment ago and has nothing listening on it now.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	server, err := Attach(ctx, address, &client.Options{})
	require.Error(t, err, "attached to %s, where nothing listens", address)
	require.Nil(t, server)
	require.ErrorContains(t, err, address)
	require.ErrorContains(t, err, AddressEnv)
}

func TestAttachRefusesAnEmptyAddress(t *testing.T) {
	t.Parallel()

	server, err := Attach(t.Context(), "", nil)
	require.Error(t, err)
	require.Nil(t, server)
}
