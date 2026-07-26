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
			name:    "a remote bare address still works, with a warning",
			address: "flowstate.example.com:9233",
			want:    "http://flowstate.example.com:9233",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, serverBaseURL(test.address))
		})
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
