package vault

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Everything in these tests is offline. A secret provider that reached the network
// to be tested would be a provider nobody could test, and a fake Vault is cheap:
// the surface used here is two JSON endpoints.

// clock is a manual clock, so token expiry is exercised without waiting for it.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock {
	return &clock{now: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// kvBody renders a KV v2 read response, with the nesting the engine actually
// produces: the value of the current version at data.data, its metadata beside it.
func kvBody(version int64, fields map[string]any) string {
	body, err := json.Marshal(map[string]any{
		"data": map[string]any{
			"data": fields,
			"metadata": map[string]any{
				"version":   version,
				"destroyed": false,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	return string(body)
}

// newTestProvider serves handler as a vault and returns a provider authenticated
// with a static token, for the cases that care about one response rather than
// about the authentication flow.
func newTestProvider(t *testing.T, handler http.Handler, opts ...Option) (*Provider, *httptest.Server) {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	provider, err := NewProvider(server.URL, append([]Option{WithToken("static-token")}, opts...)...)
	require.NoError(t, err)

	return provider, server
}

// jsonHandler answers every request with one status and body.
func jsonHandler(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}
}

// readRecord is one read the fake vault served.
type readRecord struct {
	path      string
	token     string
	vaultNS   string
	userAgent string
	accept    string
	request   string
}

// fakeVault is an offline stand-in for the parts of Vault's API this provider
// uses: the Kubernetes login endpoint and a KV v2 read.
//
// It is safe for concurrent use, because the tests that matter most run several
// resolutions at once.
type fakeVault struct {
	server *httptest.Server

	mu sync.Mutex

	// stored maps an API path, as it appears after /v1/, to the fields of the
	// secret there.
	stored map[string]map[string]any

	// role is the only role the login endpoint accepts, and lease is the token
	// lifetime it reports.
	role  string
	lease time.Duration

	// accepted holds the client tokens reads may present.
	accepted map[string]bool

	// singleUse drops a token after one successful read, which is how a token
	// revoked or expired between two resolutions is reproduced.
	singleUse bool

	// rejectAll refuses every token, which is how a policy denial — the 403 that
	// re-authentication cannot fix — is reproduced.
	rejectAll bool

	// denied refuses one path whatever token is presented, which is the policy
	// denial that says nothing about the token.
	denied map[string]bool

	// loginDelay slows the login endpoint, so a burst of concurrent resolutions
	// genuinely contends for it.
	loginDelay time.Duration

	issued int
	logins []string // the JWT each login presented
	reads  []readRecord
}

func newFakeVault(t *testing.T) *fakeVault {
	t.Helper()

	vault := &fakeVault{
		stored:   make(map[string]map[string]any),
		accepted: make(map[string]bool),
		denied:   make(map[string]bool),
		role:     "flowstate-worker",
		lease:    time.Hour,
	}

	vault.server = httptest.NewServer(vault)
	t.Cleanup(vault.server.Close)

	return vault
}

// put stores a secret at an API path, such as "secret/data/team-a/apps/api".
func (f *fakeVault) put(apiPath string, fields map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stored[apiPath] = fields
}

// accept makes a token usable, for a provider configured with a static one.
func (f *fakeVault) accept(token string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.accepted[token] = true
}

func (f *fakeVault) loginCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.logins)
}

func (f *fakeVault) presentedJWTs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.logins...)
}

func (f *fakeVault) readsServed() []readRecord {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]readRecord(nil), f.reads...)
}

func (f *fakeVault) readPaths() []string {
	paths := []string{}
	for _, read := range f.readsServed() {
		paths = append(paths, read.path)
	}

	return paths
}

func (f *fakeVault) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/login"):
		f.serveLogin(w, r, path)
	case r.Method == http.MethodGet:
		f.serveRead(w, r, path)
	default:
		writeErrors(w, http.StatusMethodNotAllowed, "unsupported operation")
	}
}

func (f *fakeVault) serveLogin(w http.ResponseWriter, r *http.Request, path string) {
	var body struct {
		Role string `json:"role"`
		JWT  string `json:"jwt"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErrors(w, http.StatusBadRequest, "malformed login request")
		return
	}

	f.mu.Lock()
	delay := f.loginDelay
	role := f.role
	f.logins = append(f.logins, body.JWT)
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if body.Role != role || body.JWT == "" {
		writeErrors(w, http.StatusBadRequest, "invalid role or JWT")
		return
	}

	if path != "auth/kubernetes/login" && !strings.HasPrefix(path, "auth/") {
		writeErrors(w, http.StatusNotFound, "no handler for route")
		return
	}

	f.mu.Lock()
	f.issued++
	token := fmt.Sprintf("issued-token-%d", f.issued)
	f.accepted[token] = true
	lease := f.lease
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w,
		`{"auth":{"client_token":%q,"lease_duration":%d,"renewable":true,"policies":["default"]}}`,
		token, int64(lease.Seconds()),
	)
}

func (f *fakeVault) serveRead(w http.ResponseWriter, r *http.Request, path string) {
	token := r.Header.Get("X-Vault-Token")

	f.mu.Lock()
	f.reads = append(f.reads, readRecord{
		path:      path,
		token:     token,
		vaultNS:   r.Header.Get("X-Vault-Namespace"),
		userAgent: r.Header.Get("User-Agent"),
		accept:    r.Header.Get("Accept"),
		request:   r.Header.Get("X-Vault-Request"),
	})

	valid := f.accepted[token] && !f.rejectAll && !f.denied[path]
	if valid && f.singleUse {
		delete(f.accepted, token)
	}

	fields, found := f.stored[path]
	f.mu.Unlock()

	if !valid {
		writeErrors(w, http.StatusForbidden, "permission denied")
		return
	}

	if !found {
		writeErrors(w, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, kvBody(1, fields))
}

// writeErrors answers the way Vault does, with a JSON errors array. Its contents
// are what the provider must never echo.
func writeErrors(w http.ResponseWriter, status int, messages ...string) {
	body, err := json.Marshal(map[string]any{"errors": messages})
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

// staticProvider returns a provider authenticated with a token the fake accepts.
func (f *fakeVault) staticProvider(t *testing.T, opts ...Option) *Provider {
	t.Helper()

	f.accept("static-token")

	provider, err := NewProvider(f.server.URL, append([]Option{WithToken("static-token")}, opts...)...)
	require.NoError(t, err)

	return provider
}

// kubernetesProvider returns a provider that logs in with a projected service
// account token, and the path of the file holding it so a test can rotate it.
func (f *fakeVault) kubernetesProvider(t *testing.T, opts ...Option) (*Provider, string) {
	t.Helper()

	jwtPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(jwtPath, []byte("projected-jwt-1"), 0o600))

	options := []Option{
		WithKubernetesAuth(f.role),
		WithKubernetesJWTPath(jwtPath),
	}

	provider, err := NewProvider(f.server.URL, append(options, opts...)...)
	require.NoError(t, err)

	return provider, jwtPath
}
