package temporaltest

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

// AddressEnv names a Temporal server that is already running, as host:port.
// When it is set, [RunPackage] attaches to that server instead of starting one,
// and never stops it (#1738).
//
// What it buys is the inner loop: every package sharing a dev server pays
// about eleven seconds to download-check, boot, and tear one down before its
// first test runs, and an agent iterating on one engine test pays that per
// iteration. `make dev-temporal` starts a server that stays up and prints the
// export line; with it set, the same package starts its tests in about a
// second. Unset, nothing here changes, so CI and `make test` are exactly what
// they were.
//
// The namespaces a test registers carry the process id (see
// testkit.NamespaceNameFor), so two packages, or two runs of one, sharing a
// server cannot register the same name.
const AddressEnv = "FLOWSTATE_TEST_TEMPORAL_ADDRESS"

// Server is what a package's tests read of the dev server they share: a client
// to register namespaces through, and the address to dial their own clients
// at. A *testsuite.DevServer is one; so is a server attached to by address.
type Server interface {
	// Client is the server's own client, in its default namespace.
	Client() client.Client

	// FrontendHostPort is the address a test's client dials.
	FrontendHostPort() string
}

// attached is a running server this process did not start and will not stop.
type attached struct {
	hostPort string
	client   client.Client
}

func (a *attached) Client() client.Client    { return a.client }
func (a *attached) FrontendHostPort() string { return a.hostPort }

// close releases the client. The server is left exactly as it was found.
func (a *attached) close() { a.client.Close() }

// Attach connects to the server at address and returns it as a [Server] the
// package's tests can read. It fails closed on a server that does not answer,
// naming the address and where it came from, rather than letting every test
// discover the same refused connection on its own.
func Attach(ctx context.Context, address string, clientOptions *client.Options) (*attached, error) {
	if address == "" {
		return nil, fmt.Errorf("attaching to a running Temporal server: no address given")
	}

	options := client.Options{}
	if clientOptions != nil {
		options = *clientOptions
	}
	options.HostPort = address

	c, err := client.DialContext(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("attaching to the Temporal server at %s named by %s: %w", address, AddressEnv, err)
	}

	return &attached{hostPort: address, client: c}, nil
}
