package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerBaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
		want    string
	}{
		{
			name:    "a bare address keeps defaulting to http",
			address: "localhost:9233",
			want:    "http://localhost:9233",
		},
		{
			// The reason this change exists: saying https should mean https,
			// rather than being prefixed into http://https://…
			name:    "an explicit https scheme is honored",
			address: "https://flowstate.example.com",
			want:    "https://flowstate.example.com",
		},
		{
			name:    "an explicit http scheme is left alone",
			address: "http://localhost:9233",
			want:    "http://localhost:9233",
		},
		{
			// The reason this default changed: refusePlaintextListener (tls.go)
			// now refuses to let `flow server` listen plaintext on anything but
			// loopback, so FLOWSTATE_ADDRESS pointed at a remote host names an
			// address that is guaranteed to be TLS-terminated. A client that
			// still guessed http here would open a handshake the server cannot
			// answer with anything but a certificate — see
			// TestServerBaseURLAgreesWithRefusePlaintextListener below, which
			// pins the two functions to the same address for the same reason.
			name:    "a remote bare address defaults to https, not http",
			address: "flowstate.example.com:9233",
			want:    "https://flowstate.example.com:9233",
		},
		{
			name:    "a remote bare IP defaults to https too",
			address: "10.0.0.5:9233",
			want:    "https://10.0.0.5:9233",
		},
		{
			name:    "an explicit http scheme overrides the https default for a remote address",
			address: "http://flowstate.example.com:9233",
			want:    "http://flowstate.example.com:9233",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, serverBaseURL(test.address))
		})
	}
}

// TestServerBaseURLAgreesWithRefusePlaintextListener is the join CLAUDE.md asks
// for: not that the client guesses a scheme, and separately that the server
// refuses to listen plaintext, but that the two answers about the *same address*
// cannot leave a deployment unable to talk to itself.
//
// refusePlaintextListener(addr, nil, false) erroring means "flow server refuses to bind
// addr without a certificate" — i.e. any server actually listening there over
// plain HTTP is either not-this-server or already broken. A client defaulting to
// http against such an address is exactly the shape of the P2 finding: it fixes
// nothing to point --address at 0.0.0.0 or a hostname and have the client dial
// plaintext, because the one server that could legitimately be listening there
// speaks TLS or does not exist.
func TestServerBaseURLAgreesWithRefusePlaintextListener(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{
		"0.0.0.0:9233",
		"example.com:9233",
		"10.0.0.5:9233",
		"flowstate.example.com:9233",
	} {
		refused := refusePlaintextListener(addr, nil, false) != nil
		require.Truef(t, refused, "test address %q must be one refusePlaintextListener refuses "+
			"plaintext on, or this test is not exercising the join", addr)

		base := serverBaseURL(addr)
		require.Truef(t, strings.HasPrefix(base, "https://"),
			"serverBaseURL(%q) = %q, but refusePlaintextListener refuses plaintext on this "+
				"address — a client defaulting to http here can never reach the server that is "+
				"actually allowed to be listening", addr, base)
	}

	// And the converse: every address refusePlaintextListener allows plaintext on
	// (loopback) is one serverBaseURL still defaults to http for, so local
	// development is not collaterally forced onto a scheme nothing served.
	for _, addr := range []string{"127.0.0.1:9233", "localhost:9233"} {
		require.NoErrorf(t, refusePlaintextListener(addr, nil, false), "test address %q must be one "+
			"refusePlaintextListener allows plaintext on, or this test is not exercising the join", addr)

		base := serverBaseURL(addr)
		require.Truef(t, strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://"),
			"serverBaseURL(%q) = %q, expected a plain http default for a loopback address", addr, base)
	}
}

func TestIsLoopbackAddress(t *testing.T) {
	t.Parallel()

	for address, want := range map[string]bool{
		"localhost:9233":             true,
		"localhost":                  true,
		"127.0.0.1:9233":             true,
		"[::1]:9233":                 true,
		"flowstate.example.com:9233": false,
		"10.0.0.5:9233":              false,
		"not a host at all":          false,
	} {
		require.Equal(t, want, isLoopbackAddress(address), "address %q", address)
	}
}

// TestBoundedTransportCapsErrorBodies is the property Connect's own option does not
// give us: a non-200 body is bounded too.
//
// connect.WithReadMaxBytes builds a separate unmarshaler for an error body without
// carrying the limit over, so a server answering 500 with an enormous body would
// otherwise be read in full.
func TestBoundedTransportCapsErrorBodies(t *testing.T) {
	t.Parallel()

	const limit = 1 << 10

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		// An order of magnitude past the limit, and on the error path.
		_, _ = w.Write([]byte(strings.Repeat("x", limit*10)))
	}))
	defer server.Close()

	client := &http.Client{
		Transport: &boundedTransport{
			base: http.DefaultTransport.(*http.Transport).Clone(),
			max:  limit,
		},
	}

	response, err := client.Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()

	require.Equal(t, http.StatusInternalServerError, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.LessOrEqual(t, len(body), limit+1,
		"an error body must be bounded; Connect's own read limit does not reach this path")
}
