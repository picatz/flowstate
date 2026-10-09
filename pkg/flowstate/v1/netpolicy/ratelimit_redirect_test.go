package netpolicy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRateLimitMarksRefusalsOnRedirectHopsOnly pins AfterRedirect where it is
// produced. false permits replaying a non-idempotent request, so the mark must
// be true exactly when an earlier hop in the chain already reached its peer, and
// must not depend on a consumer in another package to notice if it stops being.
func TestRateLimitMarksRefusalsOnRedirectHopsOnly(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/next", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	// One token and a frozen clock: the first hop spends it, so the redirect hop
	// is the refused one.
	policy := rateLimitedPolicy(t, serverURL.Hostname(), 1, newFakeClock())
	client := policy.Client()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/start", nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}

	var limited *RateLimitedError
	require.ErrorAs(t, err, &limited, "the redirect hop is the one the empty bucket refuses")
	require.True(t, limited.AfterRedirect,
		"the first hop already reached the peer, so a replay would duplicate it")

	// The bucket is still empty: a fresh request is refused on its first hop,
	// where nothing was sent and replay is safe.
	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/start", nil)
	require.NoError(t, err)

	resp, err = client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}

	require.ErrorAs(t, err, &limited)
	require.False(t, limited.AfterRedirect, "a first-hop refusal never sent anything")
}
