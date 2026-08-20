package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// `flow mcp serve` over the wire, which is the only place several of its
// claims are true. The token adapter has its own tests next to it
// (pkg/flowstate/v1/auth/mcpverifier_test.go); what is exercised here is the
// assembly — that the refusals reach an actual HTTP response, that the
// challenge points a client at the metadata document, that the tool list a
// client sees is the reduced one, and that both bounds are *reached*, not
// merely configured.
//
// Everything below drives [mcpServeHandler] over an [httptest.Server] rather
// than the cobra command, for the reason CLAUDE.md gives about testing a
// capability rather than a constructor: this is the handler `flow mcp serve`
// builds, wired by the same function, with no listener of its own to leak.

// mcpServeTestResource is the resource identifier the test surface is.
// Deliberately https and deliberately not the httptest server's own URL: a
// resource identifier is a name a token is bound to (RFC 8707 section 2), not
// an address anything dials.
const mcpServeTestResource = "https://flowstate.example.test/mcp"

// mcpServeTestOtherResource is a second resource the same trust entry admits,
// so a cross-resource token can be minted that the *policy* accepts.
const mcpServeTestOtherResource = "https://flowstate.example.test/api"

// mcpServeFixture is one wired surface plus everything a test needs to talk
// to it.
type mcpServeFixture struct {
	server   *httptest.Server
	issuer   *authtest.Issuer
	resource *auth.ProtectedResource
	logs     *bytes.Buffer
}

// newMCPServeFixture stands up the surface under test.
//
// maxSessions and maxRequestBytes are parameters rather than the defaults
// because the two tests that reach those bounds have to reach them without
// allocating a gigabyte or opening thirty-two sessions: a bound is proved by
// crossing it, and a bound that can only be crossed expensively is a bound
// nothing crosses.
func newMCPServeFixture(t *testing.T, maxSessions int, maxRequestBytes int64) *mcpServeFixture {
	t.Helper()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "agent-idp",
		Issuer:    issuer.URL(),
		Audiences: []string{mcpServeTestResource, mcpServeTestOtherResource},
	}}}

	verifier, err := auth.NewOIDCVerifier(*policy)
	require.NoError(t, err)

	protectedResource, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             mcpServeTestResource,
		AuthorizationServers: []string{issuer.URL()},
	}, policy)
	require.NoError(t, err)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	handler, err := mcpServeHandler(logger, mcpServeTools(), verifier, protectedResource, mcpServeLimits{
		maxRequestBytes: maxRequestBytes,
		maxSessions:     maxSessions,
		sessionIdle:     mcpServeSessionIdleTimeout,
	})
	require.NoError(t, err)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return &mcpServeFixture{server: server, issuer: issuer, resource: protectedResource, logs: logs}
}

// endpoint is where the MCP surface answers: the resource's own path, which is
// what the advertised identifier promises a client it can reach.
func (f *mcpServeFixture) endpoint() string {
	return f.server.URL + f.resource.ResourcePath()
}

// goodToken mints a token this surface must accept.
func (f *mcpServeFixture) goodToken(subject string) string {
	return f.issuer.MintToken(nil,
		authtest.WithSubject(subject),
		authtest.WithAudience(mcpServeTestResource))
}

// initialize sends one raw MCP initialize POST, optionally on an existing
// session, and returns the response.
//
// Raw rather than through the SDK client because two of the properties under
// test are HTTP-level — the session id a response carries, and the status code
// a mismatched principal gets — and neither is reachable through a client that
// abstracts both away.
func (f *mcpServeFixture) initialize(t *testing.T, token, sessionID string) *http.Response {
	t.Helper()

	const body = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":` +
		`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.endpoint(), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if sessionID != "" {
		req.Header.Set(mcpSessionHeader, sessionID)
	}

	resp, err := f.server.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

// connect opens an MCP session through the SDK client, which is what an agent
// host actually does.
func (f *mcpServeFixture) connect(t *testing.T, token string) *mcp.ClientSession {
	t.Helper()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint:   f.endpoint(),
		HTTPClient: &http.Client{Transport: bearerTransport{token: token}},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

// bearerTransport presents one token on every request.
type bearerTransport struct {
	token string
}

// RoundTrip adds the Authorization header and dials.
func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+b.token)

	return http.DefaultTransport.RoundTrip(clone)
}

// TestMCPServeChallengesAnUnauthenticatedRequest is the 401-first bootstrap:
// a client that has never seen this server holds no token and learns where to
// get one from the refusal itself.
//
// Two assertions and the second is as load-bearing as the first. The
// challenge must name the RFC 9728 document (MCP's own MUST), and it must
// name *no scope*: #567's D1 is deferred by omission, so a `scope` parameter
// here would be this surface shipping a vocabulary nobody has decided.
func TestMCPServeChallengesAnUnauthenticatedRequest(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)

	resp := fixture.initialize(t, "", "")

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	challenge := resp.Header.Get("WWW-Authenticate")
	require.Contains(t, challenge, `resource_metadata="`+fixture.resource.MetadataURL()+`"`,
		"the 401 must point a client at the metadata document it can bootstrap from")
	require.NotContains(t, challenge, "scope=",
		"no scope vocabulary exists yet (#567 D1, deferred by omission); a challenge naming one "+
			"would ship a spelling that has to migrate")
}

// TestMCPServeServesTheMetadataDocumentUnauthenticated: the document the
// challenge points at has to be fetchable by a client that holds nothing, or
// the bootstrap the challenge promises cannot happen.
func TestMCPServeServesTheMetadataDocumentUnauthenticated(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fixture.server.URL+fixture.resource.Path(), nil)
	require.NoError(t, err)

	resp, err := fixture.server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var document map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&document))
	require.Equal(t, mcpServeTestResource, document["resource"])
	require.NotEmpty(t, document["authorization_servers"])
	require.NotContains(t, document, "scopes_supported",
		"#567 D1 again: the document names no scope vocabulary either")
}

// TestMCPServeRefusesEveryBadToken walks the negative cases end to end, so
// that each refusal is proved to reach an HTTP status a client sees rather
// than only an error inside a function.
//
// Each token has exactly one defect, which is authtest's own contract for
// these helpers: a token with two defects cannot say which one was caught.
func TestMCPServeRefusesEveryBadToken(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)

	foreignToken, foreign := authtest.WrongIssuerToken(nil, []authtest.TokenOption{
		authtest.WithSubject("agent"),
		authtest.WithAudience(mcpServeTestResource),
	})
	t.Cleanup(func() { _ = foreign.Close() })

	for name, token := range map[string]string{
		"a token addressed to another service": fixture.issuer.WrongAudienceToken(
			"https://elsewhere.example.test/api", nil, authtest.WithSubject("agent")),

		// The policy admits this one: the issuer is trusted and the audience
		// is on that entry's list. Only this surface's own RFC 8707 binding
		// refuses it, which is the whole point of checking the audience here
		// as well as there.
		"a token for another resource of this deployment": fixture.issuer.MintToken(nil,
			authtest.WithSubject("agent"), authtest.WithAudience(mcpServeTestOtherResource)),

		"a token from an issuer the policy does not name": foreignToken,

		"a token carrying an RFC 8693 act claim": fixture.issuer.MintToken(nil,
			authtest.WithSubject("alice@example.test"),
			authtest.WithAudience(mcpServeTestResource),
			authtest.WithDelegation(map[string]any{"sub": "agent:deploy-bot"})),

		"a token carrying an RFC 8693 may_act claim": fixture.issuer.MintToken(nil,
			authtest.WithSubject("alice@example.test"),
			authtest.WithAudience(mcpServeTestResource),
			authtest.WithMayAct(map[string]any{"sub": "agent:deploy-bot"})),

		"an expired token": fixture.issuer.MintToken(nil,
			authtest.WithSubject("agent"),
			authtest.WithAudience(mcpServeTestResource),
			authtest.Expired()),

		"a token that is not a token at all": "not-a-jwt",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			resp := fixture.initialize(t, token, "")

			require.Equal(t, http.StatusUnauthorized, resp.StatusCode,
				"%s must be refused with 401, not admitted and not answered 500", name)
			require.Contains(t, resp.Header.Get("WWW-Authenticate"), "resource_metadata=",
				"a refusal still has to say where a usable token comes from")

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NotContains(t, string(body), token,
				"the refusal body must never echo the token it refused")
		})
	}
}

// TestMCPServeSessionRefusesAnotherPrincipalsToken is the session pin: a
// session opened by principal A must refuse principal B's token, even though
// B's token is perfectly valid on its own.
//
// Both principals are minted by the same trusted issuer for the same
// resource, so nothing about B's token is wrong — the only thing that differs
// is who it names, which is exactly the property the pin exists to enforce.
func TestMCPServeSessionRefusesAnotherPrincipalsToken(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)

	alice := fixture.goodToken("alice")
	bob := fixture.goodToken("bob")

	opened := fixture.initialize(t, alice, "")
	require.Equal(t, http.StatusOK, opened.StatusCode)

	sessionID := opened.Header.Get(mcpSessionHeader)
	require.NotEmpty(t, sessionID, "a stateful session must carry an id, or there is no pin to test")

	// The premise: bob's token really is admissible on its own. Without this
	// the 403 below could be a bad token rather than a refused principal.
	fresh := fixture.initialize(t, bob, "")
	require.Equal(t, http.StatusOK, fresh.StatusCode)
	require.NotEqual(t, sessionID, fresh.Header.Get(mcpSessionHeader))

	hijacked := fixture.initialize(t, bob, sessionID)
	require.Equal(t, http.StatusForbidden, hijacked.StatusCode,
		"a session opened by one principal must refuse another's token")
}

// TestMCPServeServesAReducedToolList is the "absent, not disabled" claim,
// asserted against what a client actually sees.
//
// A golden set rather than a "does not contain run_local" check: a surface
// that quietly grows is one nobody reviews, and every tool added here is a
// capability reachable by whoever holds a token. Adding one should require
// editing this list on purpose.
func TestMCPServeServesAReducedToolList(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)
	session := fixture.connect(t, fixture.goodToken("agent"))

	tools, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)

	served := map[string]bool{}
	for _, tool := range tools.Tools {
		served[tool.Name] = true
	}

	require.Equal(t, map[string]bool{
		flowmcp.ToolName("Validate"):   true,
		flowmcp.ToolName("Compile"):    true,
		flowmcp.ToolName("GetCatalog"): true,
		flowmcp.TestToolName:           true,
	}, served)

	// Stated separately from the equality above, because these two are the
	// decisions rather than an incidental consequence of them, and a reader
	// of a failing test deserves to see which rule broke.
	require.False(t, served[flowmcp.RunLocalToolName],
		"flowstate_run_local executes submitted code in this process; over HTTP that is remote "+
			"code execution as a feature (#558 decision 3)")
	require.False(t, served[flowmcp.ToolName("Signal")],
		"a run-lifecycle tool would dispatch to a deployment under this process's own credential, "+
			"which is the confused deputy this surface refuses to be until it can authorize per principal")
	require.True(t, served[flowmcp.TestToolName],
		"flowstate_test is served (#558 Q3): a stubbed run reaches nothing by construction")
}

// TestMCPServeReachesTheRequestByteBound crosses the byte bound rather than
// staying under it — CLAUDE.md's rule that a bound nothing reaches is a bound
// nothing tests.
//
// The token is valid, so what is being proved is the bound and not the
// authentication in front of it: an over-large body from an authenticated
// caller is refused too.
func TestMCPServeReachesTheRequestByteBound(t *testing.T) {
	t.Parallel()

	const limit = 4 << 10

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, limit)
	token := fixture.goodToken("agent")

	// A syntactically real initialize whose clientInfo name is padded past
	// the limit: a body the surface would otherwise happily parse, refused
	// for its size alone.
	oversized := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":` +
		`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"` +
		strings.Repeat("x", limit) + `","version":"1"}}}`
	require.Greater(t, len(oversized), limit, "the test body must actually exceed the bound")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.endpoint(),
		strings.NewReader(oversized))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := fixture.server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode,
		"a body over --max-request-bytes must be refused rather than buffered")

	// And the bound is a bound, not a wall: the same request under the limit
	// still works, so a test that passed by breaking the surface would fail.
	under := fixture.initialize(t, token, "")
	require.Equal(t, http.StatusOK, under.StatusCode)
}

// TestMCPServeReachesTheSessionBound crosses the session bound, and then
// proves a released slot is reusable — an accounting that only ever grows
// refuses every caller eventually, which is the same outage with a longer
// fuse.
func TestMCPServeReachesTheSessionBound(t *testing.T) {
	t.Parallel()

	const maxSessions = 2

	fixture := newMCPServeFixture(t, maxSessions, mcpServeDefaultMaxRequestBytes)
	token := fixture.goodToken("agent")

	sessions := make([]string, 0, maxSessions)
	for range maxSessions {
		resp := fixture.initialize(t, token, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		id := resp.Header.Get(mcpSessionHeader)
		require.NotEmpty(t, id)
		sessions = append(sessions, id)
	}

	over := fixture.initialize(t, token, "")
	require.Equal(t, http.StatusServiceUnavailable, over.StatusCode,
		"the session bound must be reached, not merely configured")
	require.NotEmpty(t, over.Header.Get("Retry-After"))

	// An existing session still works while the surface is at capacity: the
	// bound is on opening sessions, not on using them.
	inUse := fixture.initialize(t, token, sessions[0])
	require.NotEqual(t, http.StatusServiceUnavailable, inUse.StatusCode)

	// Close one, and the slot comes back.
	deleteReq, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, fixture.endpoint(), nil)
	require.NoError(t, err)
	deleteReq.Header.Set("Authorization", "Bearer "+token)
	deleteReq.Header.Set(mcpSessionHeader, sessions[0])

	deleted, err := fixture.server.Client().Do(deleteReq)
	require.NoError(t, err)
	require.NoError(t, deleted.Body.Close())
	require.Less(t, deleted.StatusCode, 300)

	reopened := fixture.initialize(t, token, "")
	require.Equal(t, http.StatusOK, reopened.StatusCode,
		"a released slot must be reusable, or this bound is a slow outage")
}

// TestMCPSessionLimiterExpiresIdleSessions reaches the other way a slot is
// returned, on an injected clock so nothing sleeps. Without expiry, a client
// that opens sessions and walks away exhausts the bound permanently.
func TestMCPSessionLimiterExpiresIdleSessions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	limiter := newMCPSessionLimiter(1, time.Minute, func() time.Time { return now })

	require.True(t, limiter.reserve())
	limiter.settle("session-one")
	require.Equal(t, 1, limiter.open())

	require.False(t, limiter.reserve(), "the bound is one; a second reservation must be refused")

	now = now.Add(2 * time.Minute)
	require.True(t, limiter.reserve(), "an idle session's slot must come back")
	limiter.settle("session-two")
	require.Equal(t, 1, limiter.open())
}

// TestMCPSessionLimiterReturnsAReservationThatOpenedNothing: a POST that
// creates no session must not hold a slot, or a caller sending anything but
// an initialize would exhaust the bound without ever opening a session.
func TestMCPSessionLimiterReturnsAReservationThatOpenedNothing(t *testing.T) {
	t.Parallel()

	limiter := newMCPSessionLimiter(1, time.Minute, time.Now)

	require.True(t, limiter.reserve())
	limiter.settle("")
	require.Equal(t, 0, limiter.open())
	require.True(t, limiter.reserve(), "the slot must be available again")
}

// TestMCPSessionLimiterIgnoresAnUnknownSessionID: a caller that invents an id
// must not be able to add entries to the accounting, which would let it fill
// the bound with sessions that do not exist.
func TestMCPSessionLimiterIgnoresAnUnknownSessionID(t *testing.T) {
	t.Parallel()

	limiter := newMCPSessionLimiter(4, time.Minute, time.Now)
	limiter.touch("invented")

	require.Equal(t, 0, limiter.open())
}

// TestMCPServeLeaksNoTokenMaterial is invariant 7 for this surface: the token
// a caller presents must appear neither in what the server logs nor in what a
// tool answers, under every containment shape.
func TestMCPServeLeaksNoTokenMaterial(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)
	token := fixture.goodToken("agent")
	session := fixture.connect(t, token)

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("Validate"),
		Arguments: map[string]any{"source": "edition: v2026.3\nname: demo\nsteps:\n- id: hi\n  log:\n    message: hello\n"},
	})
	require.NoError(t, err)

	rendered := renderEveryShape(result)
	require.NotContains(t, rendered, token, "the token reached a tool result")

	// And a refused token must not reach the log either — the failure path is
	// the one that has a reason to mention what it refused.
	refused := fixture.issuer.WrongAudienceToken("https://elsewhere.example.test/api", nil,
		authtest.WithSubject("agent"))
	resp := fixture.initialize(t, refused, "")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	logged := fixture.logs.String()
	require.NotContains(t, logged, token)
	require.NotContains(t, logged, refused)
}

// renderEveryShape renders a value under each containment shape CLAUDE.md
// names, plus inside a struct and a slice, since a redacting String method
// protects a value printed directly and does nothing when reflection reaches
// it through an unexported field.
func renderEveryShape(value any) string {
	holder := struct{ Value any }{Value: value}

	return strings.Join([]string{
		sprint("%v", value), sprint("%+v", value), sprint("%#v", value), sprint("%s", value),
		sprint("%v", holder), sprint("%+v", holder), sprint("%#v", holder),
		sprint("%v", []any{value}), sprint("%+v", []any{value}), sprint("%#v", []any{value}),
	}, "\n")
}

// sprint renders one value under one verb.
func sprint(verb string, value any) string {
	return fmt.Sprintf(verb, value)
}

// TestCheckMCPServeFlagsFailsClosed covers every refusal this command makes
// before it binds anything. Each is a posture that, admitted, would make this
// something other than a protected resource.
func TestCheckMCPServeFlagsFailsClosed(t *testing.T) {
	t.Parallel()

	good := mcpServeFlags{
		listen:                 "127.0.0.1:0",
		policyPath:             "/etc/flowstate/policy.yaml",
		maxRequestBytes:        mcpServeDefaultMaxRequestBytes,
		maxSessions:            mcpServeDefaultMaxSessions,
		protectedResourceFlags: protectedResourceFlags{resource: mcpServeTestResource},
	}
	require.NoError(t, checkMCPServeFlags(good), "the baseline must be accepted, or nothing below proves anything")

	for name, mutate := range map[string]func(f *mcpServeFlags){
		"no protected resource":  func(f *mcpServeFlags) { f.protectedResourceFlags.resource = "" },
		"anonymous access":       func(f *mcpServeFlags) { f.insecure = true },
		"revealing sensitive":    func(f *mcpServeFlags) { f.revealSensitive = true },
		"no trust policy":        func(f *mcpServeFlags) { f.policyPath = "" },
		"unbounded request size": func(f *mcpServeFlags) { f.maxRequestBytes = 0 },
		"unbounded sessions":     func(f *mcpServeFlags) { f.maxSessions = 0 },
		"negative request size":  func(f *mcpServeFlags) { f.maxRequestBytes = -1 },
		"negative sessions":      func(f *mcpServeFlags) { f.maxSessions = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			flags := good
			mutate(&flags)

			require.Error(t, checkMCPServeFlags(flags), "%s must refuse to start", name)
		})
	}
}

// TestMCPServeHandlerRefusesToBeBuiltUnauthenticated is the last line of the
// same defence, one level down: even called directly, this helper must never
// assemble a surface with nothing verifying callers.
func TestMCPServeHandlerRefusesToBeBuiltUnauthenticated(t *testing.T) {
	t.Parallel()

	limits := mcpServeLimits{maxRequestBytes: 1 << 10, maxSessions: 1, sessionIdle: time.Minute}

	_, err := mcpServeHandler(slog.Default(), mcpServeTools(), nil, nil, limits)
	require.Error(t, err, "no protected resource and no verifier must not produce a servable handler")
}

// TestStdioMCPCommandDeclaresNoListener pins #558's decision 4 from the flag
// surface: `flow mcp` gained no way to bind a socket, so nothing an operator
// can type turns the stdio command into a network service. The HTTP surface
// is reached only through the subcommand this PR adds.
func TestStdioMCPCommandDeclaresNoListener(t *testing.T) {
	t.Parallel()

	root := newRootCommand()

	stdio, _, err := root.Find([]string{"mcp"})
	require.NoError(t, err)
	require.Equal(t, "mcp", stdio.Name())

	for _, flag := range []string{"listen", "protected-resource", "authorization-server", "max-sessions"} {
		require.Nil(t, stdio.Flags().Lookup(flag),
			"`flow mcp` must not declare --%s: the HTTP surface is its own verb, not a flag here", flag)
	}

	serve, _, err := root.Find([]string{"mcp", "serve"})
	require.NoError(t, err)
	require.Equal(t, "serve", serve.Name())

	for _, flag := range []string{"listen", "protected-resource", "authorization-server", "max-sessions"} {
		require.NotNil(t, serve.Flags().Lookup(flag), "`flow mcp serve` must declare --%s", flag)
	}

	// And the reverse: the posture flags that only make sense with one
	// trusted caller stay on the stdio command and are not inherited here.
	for _, flag := range []string{"egress-policy", "secret-env", "run-local-timeout"} {
		require.NotNil(t, stdio.Flags().Lookup(flag), "`flow mcp` must keep --%s", flag)
		require.Nil(t, serve.Flags().Lookup(flag),
			"`flow mcp serve` must not declare --%s: the tool it would govern is not served here", flag)
	}
}

// TestMCPSessionLimiterReleasesASlotTheSDKDoesNotKnow is Codex's finding on
// #807, as a test: a session slot must not outlive the session it counts.
//
// The mechanism it guards is a loop. The SDK closes an idle session on its own
// clock; a request naming that id afterwards is refused with 404, but the
// limiter's own idle timer had already been refreshed by the touch on the way
// in — so the slot survives another window, and another such request refreshes
// it again. Repeat and the bound fills with sessions that do not exist,
// refusing every real caller: the bound becoming the outage it exists to
// prevent.
//
// Driven against the wrapper directly, with an inner handler standing in for
// the SDK, because the loop is a property of what the limiter does with a
// status code and reproducing it end to end would mean waiting out an idle
// timeout for no extra confidence.
func TestMCPSessionLimiterReleasesASlotTheSDKDoesNotKnow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	limiter := newMCPSessionLimiter(1, time.Minute, func() time.Time { return now })

	// One session, opened the way a real initialize opens one: the inner
	// handler names it in the response header.
	opened := limiter.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(mcpSessionHeader, "session-one")
		w.WriteHeader(http.StatusOK)
	}))
	opened.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/mcp", nil))
	require.Equal(t, 1, limiter.open())

	// Time passes — less than the idle window, so nothing expires on its own
	// — and the SDK has meanwhile forgotten the session.
	now = now.Add(30 * time.Second)

	unknown := limiter.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "session not found", http.StatusNotFound)
	}))
	stale := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	stale.Header.Set(mcpSessionHeader, "session-one")
	unknown.ServeHTTP(httptest.NewRecorder(), stale)

	require.Equal(t, 0, limiter.open(),
		"a slot counting a session the SDK does not have must be released, not refreshed")
}

// TestMCPServeAtABareOriginServesOnlyTheRootPath is Codex's other finding on
// #807: a resource identifier naming a bare origin has the path "/", and
// [http.ServeMux] reads a pattern ending in "/" as a *subtree*, so registering
// it verbatim serves the MCP endpoint at every path on the listener rather
// than at the one URI the advertised identifier names.
//
// The assertion is about a path nobody configured answering as MCP, which is
// the surface being larger than the document it published.
func TestMCPServeAtABareOriginServesOnlyTheRootPath(t *testing.T) {
	t.Parallel()

	const bareOrigin = "https://flowstate-bare.example.test"

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "agent-idp", Issuer: issuer.URL(), Audiences: []string{bareOrigin},
	}}}

	verifier, err := auth.NewOIDCVerifier(*policy)
	require.NoError(t, err)

	protectedResource, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             bareOrigin,
		AuthorizationServers: []string{issuer.URL()},
	}, policy)
	require.NoError(t, err)
	require.Equal(t, "/", protectedResource.ResourcePath(),
		"a bare-origin resource is the shape this test exists for")

	handler, err := mcpServeHandler(slog.New(slog.NewTextHandler(io.Discard, nil)),
		mcpServeTools(), verifier, protectedResource, mcpServeLimits{
			maxRequestBytes: mcpServeDefaultMaxRequestBytes,
			maxSessions:     mcpServeDefaultMaxSessions,
			sessionIdle:     mcpServeSessionIdleTimeout,
		})
	require.NoError(t, err)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	token := issuer.MintToken(nil, authtest.WithSubject("agent"), authtest.WithAudience(bareOrigin))

	for _, path := range []string{"/healthz", "/anything", "/mcp", "/a/b/c"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+path, strings.NewReader("{}"))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		require.Equal(t, http.StatusNotFound, resp.StatusCode,
			"%s is not the resource this deployment advertises and must not answer as MCP", path)
	}

	// And the root itself still does, so the fix is a narrowing rather than a
	// surface that stopped working.
	root, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":`+
			`{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`))
	require.NoError(t, err)
	root.Header.Set("Content-Type", "application/json")
	root.Header.Set("Accept", "application/json, text/event-stream")
	root.Header.Set("Authorization", "Bearer "+token)

	resp, err := server.Client().Do(root)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
