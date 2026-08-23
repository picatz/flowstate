package main

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCheckInternalListenAddressAllowsEmpty(t *testing.T) {
	t.Parallel()

	require.NoError(t, checkInternalListenAddress(""), "empty disables the listener and must not be refused")
}

func TestCheckInternalListenAddressAllowsLoopback(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"127.0.0.1:9090", "localhost:9090", "[::1]:9090"} {
		require.NoErrorf(t, checkInternalListenAddress(addr), "loopback address %s must be allowed", addr)
	}
}

// TestCheckInternalListenAddressRefusesNonLoopback is the negative direction
// that matters: this listener carries pprof and no TLS configuration of its
// own, so anything but loopback must be a start-up failure.
func TestCheckInternalListenAddressRefusesNonLoopback(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{"0.0.0.0:9090", ":9090", "10.0.0.5:9090", "example.com:9090"} {
		require.Errorf(t, checkInternalListenAddress(addr),
			"the internal listener must refuse to bind %s: it serves pprof with no TLS option", addr)
	}
}

// TestInternalListenerIsOffByDefault is the negative direction CLAUDE.md's
// "fail closed" asks for: not that the listener works once configured, which
// TestInternalListenerServesHealthAndPprofButNotTheRPCSurface below already
// covers, but that an operator who never touches --internal-listen at all —
// no flag, no FLOWSTATE_INTERNAL_ADDRESS — ends up with no socket bound, and
// specifically not the old default of 127.0.0.1:9090. A test that only
// exercises an explicitly-configured address cannot see a default that
// silently opened a port.
func TestInternalListenerIsOffByDefault(t *testing.T) {
	// Not t.Parallel(): t.Setenv forbids it.
	t.Setenv("FLOWSTATE_INTERNAL_ADDRESS", "")

	cmd := &cobra.Command{}
	addInternalListenerFlags(cmd)

	flags := internalListenerFlagsOf(cmd)
	require.Empty(t, flags.address,
		"the internal listener's address must default to empty; a deployment that never "+
			"read --internal-listen's help must not end up with an extra port bound")

	require.NoError(t, checkInternalListenAddress(flags.address))

	server, listener, err := startInternalListener(discardLogger(), flags.address)
	require.NoError(t, err)
	require.Nil(t, server, "no --internal-listen means no internal HTTP server built")
	require.Nil(t, listener, "no --internal-listen means nothing bound, not even loopback")
}

func TestStartInternalListenerDisabledWhenEmpty(t *testing.T) {
	t.Parallel()

	server, listener, err := startInternalListener(discardLogger(), "")
	require.NoError(t, err)
	require.Nil(t, server)
	require.Nil(t, listener)
}

func TestStartInternalListenerRefusesNonLoopback(t *testing.T) {
	t.Parallel()

	server, listener, err := startInternalListener(discardLogger(), "0.0.0.0:0")
	require.Error(t, err)
	require.Nil(t, server)
	require.Nil(t, listener)
}

// TestInternalListenerServesHealthAndPprofButNotTheRPCSurface pins the shape
// an operator expects on the private port, and its complement: nothing here
// is the Connect RPC handler, because this socket carries no authentication
// in front of it.
func TestInternalListenerServesHealthAndPprofButNotTheRPCSurface(t *testing.T) {
	t.Parallel()

	server, listener, err := startInternalListener(discardLogger(), "127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, server)
	defer listener.Close()

	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	base := "http://" + listener.Addr().String()

	resp, err := http.Get(base + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "the internal listener must answer /healthz")

	resp, err = http.Get(base + "/debug/pprof/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "the internal listener must serve pprof")

	// The RPC surface is not mounted here at all: a request for it hits
	// ServeMux's own "not found" rather than anything resembling a Connect
	// handler.
	resp, err = http.Post(base+"/flowstate.v1.WorkflowService/Run", "application/json", nil)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"the internal listener must not serve the RPC surface")
}

// TestInternalListenerBoundsHeaderCountBelowTheByteBound is the count bound
// asserted where it differs from the byte bound, which is the only place
// asserting it proves anything.
//
// Both probes below are a few tens of kilobytes — around two percent of
// MaxHeaderBytes — so neither is refused for its size. What separates them is
// only how many header lines those bytes are spent on. The under-bound probe
// must be answered and the over-bound one must not, which together say the
// listener bounds a resource the sender chooses the ratio to, rather than
// bounding the bytes and calling it done.
//
// Bracketing rather than testing the exact boundary: net/http counts the
// headers the client library adds for itself (Host, User-Agent,
// Accept-Encoding) alongside these, so the precise cut-off is a fact about the
// client, and pinning it here would test that instead of this.
func TestInternalListenerBoundsHeaderCountBelowTheByteBound(t *testing.T) {
	t.Parallel()

	server, listener, err := startInternalListener(discardLogger(), "127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, server)
	defer listener.Close()

	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	base := "http://" + listener.Addr().String()

	probe := func(t *testing.T, count int) (*http.Response, error) {
		t.Helper()

		req, err := http.NewRequest(http.MethodGet, base+"/healthz", nil)
		require.NoError(t, err)
		for i := range count {
			req.Header.Add(fmt.Sprintf("X-Flowstate-Probe-%d", i), "v")
		}

		// One connection per probe, not the shared default transport: a
		// refusal here closes the connection, and a pooled one carrying that
		// state into another test is a flake nobody would enjoy finding.
		transport := &http.Transport{}
		defer transport.CloseIdleConnections()

		return transport.RoundTrip(req)
	}

	resp, err := probe(t, maxHeaderValueCount-100)
	require.NoError(t, err, "a request comfortably under the header-count bound must be answered")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"%d header lines is under the bound and must be served", maxHeaderValueCount-100)

	resp, err = probe(t, maxHeaderValueCount+100)
	if err == nil {
		defer resp.Body.Close()
		require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, resp.StatusCode,
			"%d header lines is over the bound and must be refused, not served",
			maxHeaderValueCount+100)
		return
	}

	// A refusal the server declines to write a response for is equally a
	// refusal: what must not happen is the request being *served*.
	require.Error(t, err)
}
