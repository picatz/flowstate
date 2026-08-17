package plugin

// boundedTransport exists for one attack the surrounding comment names by hand:
// connect.WithReadMaxBytes bounds only a *successful* response, so a hostile
// plugin answering HTTP 500 with an arbitrarily large body would otherwise be
// buffered whole into the worker's memory. The transport wraps every response
// body — whatever its status — in io.LimitReader(body, max+1), so the read is
// capped one byte past the limit: a body at the limit still passes, and one over
// it is caught rather than silently truncated into something that might parse.
//
// Nothing tested this. These tests drive a non-200 response with an oversized
// body through boundedTransport and assert the cap is *reached* — the body the
// caller can read is bounded to max+1, not the full oversized payload.

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bodyOfSize is a deterministic body of exactly n bytes.
func bodyOfSize(n int) string { return strings.Repeat("A", n) }

// roundTripThroughBound stands up a server that answers `status` with a body of
// `bodySize` bytes, drives one request through a boundedTransport capped at
// `max`, and returns what the caller can read from the response body.
//
// The base is a real *http.Transport (the field's concrete type) reaching a
// loopback httptest server, so the whole path a response takes on the way back —
// the exact place connect's own limit is not present on an error body — is
// exercised, not a stub in place of it.
func roundTripThroughBound(t *testing.T, status, bodySize int, max int64) (*http.Response, []byte) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, bodyOfSize(bodySize))
	}))
	t.Cleanup(srv.Close)

	base := &http.Transport{Proxy: nil} // loopback: no proxy, dial the server directly
	t.Cleanup(base.CloseIdleConnections)

	bt := &boundedTransport{base: base, max: max}

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := bt.RoundTrip(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, got
}

// TestBoundedTransportCapsNon200OversizedBody is the security-critical path: a
// hostile plugin answers HTTP 500 with a body far larger than the cap. The read
// the caller can perform is bounded to max+1 — the extra byte is exactly how
// "over the limit" is detectable — rather than the full oversized payload
// reaching memory.
func TestBoundedTransportCapsNon200OversizedBody(t *testing.T) {
	const max = 1024

	resp, got := roundTripThroughBound(t, http.StatusInternalServerError, 1<<20 /* 1 MiB */, max)

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"the test must exercise the non-200 path connect's own limit misses")
	assert.Len(t, got, max+1,
		"an oversized non-200 body must be capped at max+1, not read whole")
	assert.Less(t, len(got), 1<<20,
		"the full oversized payload must never reach the caller")
}

// TestBoundedTransportCapReachedAtBoundary pins the max+1 contract at its exact
// edge, on a non-200 response:
//
//   - a body of exactly max bytes passes through whole (the limit is inclusive);
//   - a body one byte over max is where the cap first bites, yielding max+1 —
//     the sentinel byte that lets a reader tell "at the limit" from "over it".
func TestBoundedTransportCapReachedAtBoundary(t *testing.T) {
	const max = 4096

	t.Run("body at the limit passes whole", func(t *testing.T) {
		_, got := roundTripThroughBound(t, http.StatusInternalServerError, max, max)
		assert.Len(t, got, max, "a body at exactly the limit must not be truncated")
	})

	t.Run("body one over the limit is capped at max+1", func(t *testing.T) {
		_, got := roundTripThroughBound(t, http.StatusInternalServerError, max+100, max)
		assert.Len(t, got, max+1,
			"a body over the limit is read to max+1, the sentinel that flags 'over'")
	})
}

// theLeakedToken is the per-launch bearer token standing in for a real one in
// TestTokenClientInterceptorNeverReflectsTheToken — distinctive enough that a
// substring match cannot be an accident.
const theLeakedToken = "s3cr3t-per-launch-bearer-token-must-never-print"

// TestTokenClientInterceptorNeverReflectsTheToken is CLAUDE.md's "secrets
// never enter workflow history" containment test, applied to
// [tokenClientInterceptor]: a round-2 fix (6c888eb) added streaming coverage
// by storing the per-launch token directly as a struct field, which reverses
// the guarantee [authInterceptor]'s own doc comment states two paragraphs
// above it — fmt reflects into an unexported field it cannot call a method
// on, and a credential in a field is a credential that prints. Rendering the
// interceptor itself, a struct wrapping one, and a slice of several — the
// three containment shapes CLAUDE.md's own testing standard names — must
// never surface the token under any of %v, %+v, %#v, or %s.
func TestTokenClientInterceptorNeverReflectsTheToken(t *testing.T) {
	interceptor := authInterceptor(theLeakedToken)

	type wrapper struct {
		one   connect.Interceptor
		batch []connect.Interceptor
	}
	w := wrapper{one: interceptor, batch: []connect.Interceptor{interceptor, authInterceptor(theLeakedToken + "-2")}}

	rendered := []string{
		fmt.Sprintf("%v", interceptor),
		fmt.Sprintf("%+v", interceptor),
		fmt.Sprintf("%#v", interceptor),
		fmt.Sprintf("%s", interceptor),
		fmt.Sprintf("%v", w),
		fmt.Sprintf("%+v", w),
		fmt.Sprintf("%#v", w),
		fmt.Sprintf("%v", w.batch),
		fmt.Sprintf("%+v", w.batch),
		fmt.Sprintf("%#v", w.batch),
	}

	for _, r := range rendered {
		assert.NotContains(t, r, theLeakedToken,
			"the per-launch token reached a formatted rendering of the client interceptor")
	}
}

// TestBoundedTransportCapsAllStatuses shows the cap is a property of the
// transport, not of a status: a 200 with an oversized body is bounded the same
// way, so the limit covers the successful path connect also guards and the error
// path it does not, uniformly.
func TestBoundedTransportCapsAllStatuses(t *testing.T) {
	const max = 512

	for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusInternalServerError, http.StatusBadGateway} {
		_, got := roundTripThroughBound(t, status, 64*1024, max)
		assert.Lenf(t, got, max+1, "status %d: oversized body must be capped at max+1", status)
	}
}
