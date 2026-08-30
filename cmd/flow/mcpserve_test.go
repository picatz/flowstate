package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
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
	audit    *mcpServeAuditEmitter

	// guard is the one the served surface was built with, kept so a test can
	// hold its lock from outside and prove a handler really is behind it.
	guard *mcpServeRegistryGuard
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

	return newMCPServeFixtureWith(t, maxSessions, maxRequestBytes, mcpServeDefaultTestTimeout)
}

// newMCPServeFixtureWithTestTimeout is [newMCPServeFixture] with a short bound
// on one flowstate_test call, for the test that has to cross it.
func newMCPServeFixtureWithTestTimeout(t *testing.T, testTimeout time.Duration) *mcpServeFixture {
	t.Helper()

	return newMCPServeFixtureWith(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes, testTimeout)
}

// newMCPServeFixtureWith is the one that actually wires it.
func newMCPServeFixtureWith(
	t *testing.T, maxSessions int, maxRequestBytes int64, testTimeout time.Duration,
) *mcpServeFixture {
	t.Helper()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	return newMCPServeFixtureForIssuer(t, issuer, maxSessions, maxRequestBytes, testTimeout)
}

// newMCPServeFixtureForIssuer builds one independently stateful handler that
// trusts issuer. Two calls with the same issuer model two replicas of one
// deployment: authentication agrees while process-local MCP sessions do not.
func newMCPServeFixtureForIssuer(
	t *testing.T,
	issuer *authtest.Issuer,
	maxSessions int,
	maxRequestBytes int64,
	testTimeout time.Duration,
) *mcpServeFixture {
	t.Helper()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:      "agent-idp",
		Issuer:    issuer.URL(),
		Audiences: []string{mcpServeTestResource, mcpServeTestOtherResource},
	}}}

	verifier, err := auth.NewOIDCVerifier(*policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	// Through the command's own resolution rather than around it: the scope
	// vocabulary this surface publishes is supplied there and nowhere else,
	// so a fixture calling auth.NewProtectedResource directly would be testing
	// a document no deployment serves.
	protectedResource, err := resolveProtectedResource(protectedResourceFlags{
		resource:             mcpServeTestResource,
		authorizationServers: []string{issuer.URL()},
	}, policy)
	require.NoError(t, err)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	guard := newMCPServeRegistryGuard()
	auditSink := &mcpServeAuditEmitter{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(auditSink))
	require.NoError(t, err)

	tools, err := mcpServeTools(guard, testTimeout, recorder, nil)
	require.NoError(t, err)

	handler, err := mcpServeHandler(logger, tools, verifier, protectedResource, mcpServeLimits{
		maxRequestBytes:    maxRequestBytes,
		maxSessions:        maxSessions,
		maxSessionRequests: mcpServeDefaultMaxSessionRequests,
		sessionIdle:        mcpServeSessionIdleTimeout,
	})
	require.NoError(t, err)

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return &mcpServeFixture{
		server: server, issuer: issuer, resource: protectedResource, logs: logs, audit: auditSink, guard: guard,
	}
}

type mcpServeAuditEmitter struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (e *mcpServeAuditEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, record)

	return nil
}

func (e *mcpServeAuditEmitter) Records() []*v1.AuditRecord {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]*v1.AuditRecord(nil), e.records...)
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
// name *no scope*: the vocabulary now exists, but no per-action enforcement
// point can truthfully say which scope this request requires.
func TestMCPServeChallengesAnUnauthenticatedRequest(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)

	resp := fixture.initialize(t, "", "")

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	challenge := resp.Header.Get("WWW-Authenticate")
	require.Contains(t, challenge, `resource_metadata="`+fixture.resource.MetadataURL()+`"`,
		"the 401 must point a client at the metadata document it can bootstrap from")
	require.NotContains(t, challenge, "scope=",
		"no per-action enforcement point can truthfully name a required scope")
	require.Contains(t, fixture.logs.String(), `reason="missing bearer token"`,
		"the SDK rejects an absent header before calling the token verifier, and that refusal must still be observed")

	// The document named by the challenge describes the exact audience this
	// MCP surface admits. A second audience on the same trusted issuer entry is
	// deliberately refused below, proving this is surface binding rather than
	// only issuer-level verification.
	metadataRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fixture.server.URL+fixture.resource.Path(), nil)
	require.NoError(t, err)
	metadataResponse, err := fixture.server.Client().Do(metadataRequest)
	require.NoError(t, err)
	defer metadataResponse.Body.Close()
	require.Equal(t, http.StatusOK, metadataResponse.StatusCode)

	var document struct {
		Resource string `json:"resource"`
	}
	require.NoError(t, json.NewDecoder(metadataResponse.Body).Decode(&document))
	require.Equal(t, mcpServeTestResource, document.Resource)

	accepted := fixture.initialize(t, fixture.goodToken("coherent-agent"), "")
	require.Equal(t, http.StatusOK, accepted.StatusCode,
		"a token naming the challenge document's resource must be admitted")

	const secretSubject = "mismatched-claim-that-must-not-leak"
	mismatchedToken := fixture.issuer.MintToken(nil,
		authtest.WithSubject(secretSubject), authtest.WithAudience(mcpServeTestOtherResource))
	refused := fixture.initialize(t, mismatchedToken, "")
	require.Equal(t, http.StatusUnauthorized, refused.StatusCode,
		"a policy-trusted token for a different resource must be refused by MCP's surface binding")
	refusedBody, err := io.ReadAll(refused.Body)
	require.NoError(t, err)
	refusedChallenge := refused.Header.Get("WWW-Authenticate")
	require.Contains(t, refusedChallenge, `resource_metadata="`+fixture.resource.MetadataURL()+`"`)
	for _, rendered := range []string{refusedChallenge, string(refusedBody), fixture.logs.String()} {
		require.NotContains(t, rendered, mismatchedToken)
		require.NotContains(t, rendered, secretSubject)
		require.NotContains(t, rendered, mcpServeTestOtherResource)
	}
}

// TestMCPServeObservesARefusalWithoutLoggingCredentialMaterial is the MCP half
// of the refusal observability contract. The adapter gives the command the
// internal cause, and the command records only its public classification.
func TestMCPServeObservesARefusalWithoutLoggingCredentialMaterial(t *testing.T) {
	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)
	const secretSubject = "subject-that-must-not-reach-mcp-logs"
	const secretAudience = "https://audience-that-must-not-reach-mcp-logs.example.test"
	token := fixture.issuer.WrongAudienceToken(secretAudience, nil, authtest.WithSubject(secretSubject))

	resp := fixture.initialize(t, token, "")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	logs := fixture.logs.String()
	require.Contains(t, logs, "rejected MCP request")
	require.Contains(t, logs, `path=`+fixture.resource.ResourcePath())
	require.Contains(t, logs, `status=401`)
	require.Contains(t, logs, `reason="token audience is not accepted"`)
	require.NotContains(t, logs, token)
	require.NotContains(t, logs, secretSubject)
	require.NotContains(t, logs, secretAudience)
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

	// #567's D1, answered: this surface's document publishes the schema's
	// action vocabulary. It is the same list `flow server` publishes, because
	// both go through resolveProtectedResource — the one place it is supplied.
	require.Equal(t, v1.AuthorizationActionScopes(), publishedScopes(t, document))
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
	logs := fixture.logs.String()
	require.Contains(t, logs, "rejected MCP request")
	require.Contains(t, logs, `status=403`)
	require.Contains(t, logs, `reason="authenticated caller is not permitted on this session"`)
	require.NotContains(t, logs, alice)
	require.NotContains(t, logs, bob)
}

// TestMCPServeSessionsRequireTheirOriginProcess pins the deployment contract,
// not merely the implementation detail behind it. Authentication is shared by
// all three handlers, but the SDK session map and Flowstate's limiter maps are
// newly allocated with each handler. That is both a second replica and a
// restarted process: neither knows an id minted by the first.
func TestMCPServeSessionsRequireTheirOriginProcess(t *testing.T) {
	t.Parallel()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	first := newMCPServeFixtureForIssuer(t, issuer,
		mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes, mcpServeDefaultTestTimeout)
	secondReplica := newMCPServeFixtureForIssuer(t, issuer,
		mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes, mcpServeDefaultTestTimeout)

	token := first.goodToken("agent")
	opened := first.initialize(t, token, "")
	require.Equal(t, http.StatusOK, opened.StatusCode)

	sessionID := opened.Header.Get(mcpSessionHeader)
	require.NotEmpty(t, sessionID)

	onOrigin := first.initialize(t, token, sessionID)
	require.NotEqual(t, http.StatusNotFound, onOrigin.StatusCode,
		"the process that minted the session id must still recognize it")

	onOtherReplica := secondReplica.initialize(t, token, sessionID)
	require.Equal(t, http.StatusNotFound, onOtherReplica.StatusCode,
		"a load balancer must route an existing session back to the process that minted it")

	// A fresh handler under the identical deployment identity is what a
	// restarted process is: tokens still verify, but active sessions are gone.
	restarted := newMCPServeFixtureForIssuer(t, issuer,
		mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes, mcpServeDefaultTestTimeout)
	afterRestart := restarted.initialize(t, token, sessionID)
	require.Equal(t, http.StatusNotFound, afterRestart.StatusCode,
		"restarting flow mcp serve must invalidate process-local sessions")
}

// TestMCPServePinsAdvertisedProtocolRevisions turns a go-sdk minor update into
// an explicit protocol review. The SDK supports 2026-07-28, but this handler is
// intentionally stateful, so server/discover must advertise the exact legacy
// set it can actually serve and must not claim the stateless target revision.
func TestMCPServePinsAdvertisedProtocolRevisions(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)

	body := `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{` +
		`"io.modelcontextprotocol/protocolVersion":"2026-07-28",` +
		`"io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},` +
		`"io.modelcontextprotocol/clientCapabilities":{}}}}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.endpoint(), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+fixture.goodToken("agent"))
	req.Header.Set("MCP-Protocol-Version", "2026-07-28")
	req.Header.Set("Mcp-Method", "server/discover")

	resp, err := fixture.server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	payload := raw
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		var data []string
		scanner := bufio.NewScanner(bytes.NewReader(raw))
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" && len(data) > 0 {
				break
			}
			field, value, found := strings.Cut(line, ":")
			if found && field == "data" {
				data = append(data, strings.TrimPrefix(value, " "))
			}
		}
		require.NoError(t, scanner.Err())
		require.NotEmpty(t, data, "server/discover SSE response: %s", raw)
		payload = []byte(strings.Join(data, "\n"))
	}

	var result struct {
		Result struct {
			SupportedVersions []string `json:"supportedVersions"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(payload, &result), "server/discover response: %s", raw)
	require.Equal(t, []string{"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"},
		result.Result.SupportedVersions)
	require.NotContains(t, result.Result.SupportedVersions, "2026-07-28",
		"2026-07-28 removes protocol sessions and is only valid on the SDK's stateless HTTP handler")
}

// TestMCPServeSessionTopologyContractStaysVisible couples the executable
// diagnostic, CLI help, and hand-written operator guide. A future change may
// make this surface stateless, but it must update all three claims together.
func TestMCPServeSessionTopologyContractStaysVisible(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logMCPServeSessionTopology(slog.New(slog.NewTextHandler(&logs, nil)))
	for _, claim := range []string{
		"level=WARN",
		"session_storage=" + mcpServeSessionStorage,
		"session_affinity_header=" + mcpSessionHeader,
		"horizontal_scaling=false",
	} {
		require.Contains(t, logs.String(), claim)
	}

	root := newRootCommand()
	serve, _, err := root.Find([]string{"mcp", "serve"})
	require.NoError(t, err)
	require.Contains(t, serve.Long, "run one replica")
	require.Contains(t, serve.Long, "restart to invalidate active sessions")

	doc, err := os.ReadFile("../../docs/MCP_AUTHORIZATION.md")
	require.NoError(t, err)
	for _, claim := range []string{
		"Session topology: one process, one replica",
		"`session_storage=process_memory`",
		"`session_affinity_header=Mcp-Session-Id`",
		"`horizontal_scaling=false`",
		"`2025-11-25`, `2025-06-18`, `2025-03-26`, and `2024-11-05`",
		"building a distributed session store",
	} {
		require.Contains(t, string(doc), claim)
	}
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
		flowmcp.DebugToolName:          true,
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
	require.True(t, served[flowmcp.DebugToolName],
		"flowstate_debug is served for the identical reason: it drives the same stubbed run, and "+
			"a finite script cannot hold it open — an exhausted script resumes the run, so the "+
			"bound that ends a flowstate_test call ends this one (#928 slice 3)")

	// Call every tool with an empty document. Some correctly return a tool
	// refusal because their required arguments are absent; authorization is
	// write-ahead of parsing, so each still owes exactly one decision record.
	for name := range served {
		_, err := session.CallTool(t.Context(), &mcp.CallToolParams{
			Name:      name,
			Arguments: map[string]any{},
		})
		require.NoError(t, err, "calling registered tool %q reached a transport failure", name)
	}

	recorded := map[string]int{}
	for _, record := range fixture.audit.Records() {
		recorded[record.GetMcpTool()]++
	}
	require.Len(t, recorded, len(served), "a served tool emitted no authorization decision")
	for name := range served {
		require.Equal(t, 1, recorded[name], "%q emitted other than one authorization decision", name)
	}
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
	limiter := newMCPSessionLimiter(1, mcpServeDefaultMaxSessionRequests, time.Minute, func() time.Time { return now })

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

	limiter := newMCPSessionLimiter(1, mcpServeDefaultMaxSessionRequests, time.Minute, time.Now)

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

	limiter := newMCPSessionLimiter(4, mcpServeDefaultMaxSessionRequests, time.Minute, time.Now)
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
	require.Len(t, fixture.audit.Records(), 1,
		"one admitted tools/call did not produce exactly one authorization record")
	record := fixture.audit.Records()[0]
	require.Equal(t, flowmcp.ToolName("Validate"), record.GetMcpTool())
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE, record.GetAction())
	require.Equal(t, "agent", record.GetIdentity().GetSubject())
	require.Equal(t, "agent-idp", record.GetIssuerName())
	auditRendering := renderEveryShape(record)
	require.NotContains(t, auditRendering, token, "the token reached the audit record")
	require.NotContains(t, auditRendering, "message: hello",
		"a Flowfile argument reached the audit record")

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
		maxSessionRequests:     mcpServeDefaultMaxSessionRequests,
		testTimeout:            mcpServeDefaultTestTimeout,
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

	limits := mcpServeLimits{maxRequestBytes: 1 << 10, maxSessions: 1, maxSessionRequests: 1, sessionIdle: time.Minute}

	tools, err := mcpServeTools(newMCPServeRegistryGuard(), mcpServeDefaultTestTimeout, nil, nil)
	require.NoError(t, err)

	_, err = mcpServeHandler(slog.Default(), tools, nil, nil, limits)
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
	limiter := newMCPSessionLimiter(1, mcpServeDefaultMaxSessionRequests, time.Minute, func() time.Time { return now })

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

	verifier, err := auth.NewOIDCVerifier(*policy, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	protectedResource, err := auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             bareOrigin,
		AuthorizationServers: []string{issuer.URL()},
	}, policy)
	require.NoError(t, err)
	require.Equal(t, "/", protectedResource.ResourcePath(),
		"a bare-origin resource is the shape this test exists for")

	tools, err := mcpServeTools(newMCPServeRegistryGuard(), mcpServeDefaultTestTimeout, nil, nil)
	require.NoError(t, err)

	handler, err := mcpServeHandler(slog.New(slog.NewTextHandler(io.Discard, nil)),
		tools, verifier, protectedResource, mcpServeLimits{
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

// TestMCPServeTestToolLeavesNoTaskInTheGlobalRegistry is Codex's P1 finding on
// #807: a caller's flowstate_test call must not write into state every other
// caller reads.
//
// The mechanism: a stub naming a task this build does not register makes
// flowtest register a synthetic definition into the process-wide
// v1.DefaultRegistry so the submitted workflow can be compiled, and leave it
// there. On stdio that is one trusted caller in a short-lived process; here
// the stub names are chosen by whoever holds a token, so the registry grows
// across calls and flowstate_get_catalog — reading that same registry — starts
// advertising one caller's invented tasks to everyone else, until the catalog
// no longer fits under the surface's byte bound and answers nobody.
//
// Asserted in both places it shows: the registry itself, and the catalog a
// second caller reads.
func TestMCPServeTestToolLeavesNoTaskInTheGlobalRegistry(t *testing.T) {
	// Not parallel: it asserts against the process-wide task registry, which
	// a parallel sibling registering a task of its own would make unreadable.

	const invented = "attacker_chosen_task_from_807"

	require.NotContains(t, v1.DefaultRegistry().Names(), invented,
		"the premise is that this build does not have this task")

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)
	session := fixture.connect(t, fixture.goodToken("agent"))

	_, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: flowmcp.TestToolName,
		Arguments: map[string]any{
			"workflow": "edition: v2026.3\nname: demo\nsteps:\n- id: reach\n  " + invented + ":\n    anything: 1\n",
			"tests":    "tests:\n  - name: it runs\n    stubs:\n      - task: " + invented + "\n        returns: {}\n    expect:\n      failed: false\n",
		},
	})
	require.NoError(t, err)

	require.NotContains(t, v1.DefaultRegistry().Names(), invented,
		"a caller's stub name must not outlive its own call in the process-wide registry")

	// And the surface a second caller reads: the catalog must not advertise
	// somebody else's invented task.
	catalog, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("GetCatalog"),
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.NotContains(t, renderEveryShape(catalog), invented,
		"flowstate_get_catalog must not advertise a task one caller invented")

	// And the same answer through the read-only half of the surface, which a
	// guard applied only to tool handlers would miss: flowstate://catalog/tasks
	// reads the identical registry, one request away. Codex's follow-up on
	// #807.
	resource, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: flowmcp.CatalogResourceURI})
	require.NoError(t, err)
	require.NotContains(t, renderEveryShape(resource), invented,
		"the catalog resource must not advertise a task one caller invented either")
}

// TestMCPServeRegistryGuardCoversTheServedResources is Codex's follow-up on
// #807: the guard has to cover the read-only half of the surface too.
//
// flowstate://catalog/tasks answers from v1.DefaultRegistry exactly as
// flowstate_get_catalog does, so a guard applied only to tool handlers leaves
// the identical read reachable one request away — and while one caller's
// flowstate_test has the registry swapped, that read hands their synthetic,
// caller-chosen task names to somebody else.
//
// Asserted as the exclusion rather than as a sighting of the leak, because the
// leak is a race: a sequential test reads the registry after the tool call has
// already repaired it and passes whether or not the guard is wired. What is
// checkable without a race is that a resources/read over the real served
// surface cannot proceed while the registry-mutating half holds the lock — so
// this holds that lock directly, through the guard the surface was built with.
func TestMCPServeRegistryGuardCoversTheServedResources(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)
	session := fixture.connect(t, fixture.goodToken("agent"))

	// The premise: it answers at all when nothing holds the lock.
	_, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: flowmcp.CatalogResourceURI})
	require.NoError(t, err)

	// Stand in for a flowstate_test call in flight, which is exactly what
	// holds this lock in production.
	require.NoError(t, fixture.guard.sem.Acquire(t.Context(), mcpServeRegistryReaders))

	read := make(chan struct{})
	go func() {
		defer close(read)
		_, _ = session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: flowmcp.CatalogResourceURI})
	}()

	select {
	case <-read:
		fixture.guard.sem.Release(mcpServeRegistryReaders)
		t.Fatal("the catalog resource answered while the registry-mutating half held the lock: " +
			"a resources/read can observe another caller's synthetic task names")
	case <-time.After(250 * time.Millisecond):
	}

	fixture.guard.sem.Release(mcpServeRegistryReaders)

	select {
	case <-read:
	case <-time.After(10 * time.Second):
		t.Fatal("the catalog resource never answered after the lock was released")
	}
}

// TestMCPServeBoundsAHangingTestCall is Codex's P1 on #807's third round: a
// submitted workflow can park forever on its own, and on this surface that
// would take the whole thing down rather than only the caller's own request.
//
// The mechanism is flowtest's virtual clock, which advances only when every
// participant is parked. A `wait_for_signal:` with no timeout and no scripted
// signal has no deadline to advance to, so the case never completes — and it
// is a legal Flowfile, so the refusal cannot live in validation. Unbounded,
// such a call holds this surface's exclusive registry lock and its goroutine
// forever, and every other tool and resource blocks behind it.
//
// A short timeout here rather than the default, for the reason every bound in
// this file is crossed with a small one: a bound proved by waiting two minutes
// is a bound nobody runs.
func TestMCPServeBoundsAHangingTestCall(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixtureWithTestTimeout(t, 2*time.Second)
	session := fixture.connect(t, fixture.goodToken("agent"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = session.CallTool(t.Context(), &mcp.CallToolParams{
			Name: flowmcp.TestToolName,
			Arguments: map[string]any{
				"workflow": "edition: v2026.3\nname: demo\nsteps:\n- id: gate\n  wait_for_signal:\n    name: approve\n",
				"tests":    "tests:\n  - name: waits forever\n    expect:\n      failed: false\n",
			},
		})
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a flowstate_test call on a workflow that parks forever never returned: " +
			"it holds this surface's registry lock and its goroutine for good")
	}

	// And the surface is still usable afterwards, which is the property the
	// bound is actually for: the lock came back.
	tools, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.NotEmpty(t, tools.Tools)
}

// TestMCPSessionLimiterBoundsRequestsWithinOneSession is Codex's P1 on the
// fourth round: `--max-sessions` bounds how many sessions exist and bounds
// nothing about what a caller does inside one.
//
// An established session id can be replayed over arbitrarily many parallel
// connections or HTTP/2 streams, each getting a goroutine and each either
// running under the registry guard or queued behind an exclusive
// flowstate_test. So the per-session in-flight count is a third resource the
// peer controls, and it gets its own bound.
//
// Crossed with a bound of one, and then checked to come back — a bound that
// never releases is the same outage the session bound's own test guards
// against, on a shorter timescale.
func TestMCPSessionLimiterBoundsRequestsWithinOneSession(t *testing.T) {
	t.Parallel()

	limiter := newMCPSessionLimiter(4, 1, time.Minute, time.Now)

	holding := make(chan struct{})
	release := make(chan struct{})

	handler := limiter.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Hold") == "yes" {
			close(holding)
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))

	// One request in flight for the session, parked inside the handler.
	held := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	held.Header.Set(mcpSessionHeader, "session-one")
	held.Header.Set("X-Hold", "yes")

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(httptest.NewRecorder(), held)
	}()
	<-holding

	// A second request naming the same session must be refused rather than
	// served alongside it.
	second := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	second.Header.Set(mcpSessionHeader, "session-one")

	refused := httptest.NewRecorder()
	handler.ServeHTTP(refused, second)

	require.Equal(t, http.StatusServiceUnavailable, refused.Code,
		"the per-session in-flight bound must be reached, not merely configured")
	require.NotEmpty(t, refused.Header().Get("Retry-After"))

	// Another session is unaffected: the bound is per session, so one caller
	// saturating their own cannot refuse anybody else's request.
	other := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	other.Header.Set(mcpSessionHeader, "session-two")

	unaffected := httptest.NewRecorder()
	handler.ServeHTTP(unaffected, other)
	require.Equal(t, http.StatusOK, unaffected.Code)

	close(release)
	<-done

	// And the slot comes back.
	after := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	after.Header.Set(mcpSessionHeader, "session-one")

	allowed := httptest.NewRecorder()
	handler.ServeHTTP(allowed, after)
	require.Equal(t, http.StatusOK, allowed.Code, "an in-flight slot must be returned when the request ends")
}

// TestMCPServeTimedOutTestCallIsNotAPassingVerdict is Codex's P2 on the same
// round, and it is the more interesting of the two: a serving deadline must
// never be readable as the submitted workflow's own failure.
//
// flowtest compares a case's `expect.failed` against whether the run returned
// an error, and a cancelled context produces one. So a case declaring
// `failed: true` on a workflow that never completes would be marked *passed*,
// every later case would run against an already expired context and pass the
// same way, and the tool would answer success about cases that were never
// really run.
func TestMCPServeTimedOutTestCallIsNotAPassingVerdict(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixtureWithTestTimeout(t, 2*time.Second)
	session := fixture.connect(t, fixture.goodToken("agent"))

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: flowmcp.TestToolName,
		Arguments: map[string]any{
			"workflow": "edition: v2026.3\nname: demo\nsteps:\n- id: gate\n  wait_for_signal:\n    name: approve\n",
			// The case that would otherwise be satisfied by the deadline.
			"tests": "tests:\n  - name: expects a failure\n    expect:\n      failed: true\n",
		},
	})
	require.NoError(t, err)

	require.True(t, result.IsError,
		"a call the serving deadline stopped must not be reported as a passing suite")
	require.Contains(t, renderEveryShape(result), "did not finish",
		"the answer has to say the tests were stopped, not report a verdict about them")
}

// TestMCPServeDescriptionsDescribeThisSurface is Codex's finding on the last
// round, and it is this repository's "diagnostics are a feature" rule pointed
// at a non-human reader: a tool description is what a model chooses a tool by
// and is the only account of the surface it ever gets, so one describing
// behavior this surface does not have sends it at something that is not there.
//
// Two were wrong, and both came from reusing stdio's metadata verbatim:
// flowstate_get_catalog's note explained dispatching to a deployment named by
// --address, a flag this command does not have; and every example resource
// told the reader to execute it with flowstate_run_local, which this surface
// deliberately does not serve.
func TestMCPServeDescriptionsDescribeThisSurface(t *testing.T) {
	t.Parallel()

	fixture := newMCPServeFixture(t, mcpServeDefaultMaxSessions, mcpServeDefaultMaxRequestBytes)
	session := fixture.connect(t, fixture.goodToken("agent"))

	tools, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)

	for _, tool := range tools.Tools {
		require.NotContains(t, tool.Description, "--address",
			"%s names a flag `flow mcp serve` does not have", tool.Name)
		require.NotContains(t, tool.Description, flowmcp.RunLocalToolName,
			"%s points at a tool this surface does not serve", tool.Name)
	}

	// And the same for the read-only half, where the example descriptions live.
	resources, err := session.ListResources(t.Context(), nil)
	require.NoError(t, err)
	require.NotEmpty(t, resources.Resources)

	for _, resource := range resources.Resources {
		require.NotContains(t, resource.Description, flowmcp.RunLocalToolName,
			"%s points at a tool this surface does not serve", resource.URI)
	}

	// The two tools whose stdio text was corrected still say something in
	// place of what was removed, so this is a correction rather than a
	// deletion — silence would leave a model looking for an answer that is
	// not there either.
	for _, tool := range tools.Tools {
		switch tool.Name {
		case flowmcp.ToolName("GetCatalog"):
			require.Contains(t, tool.Description, "never dispatches",
				"the catalog tool must still say where its answer comes from")
		case flowmcp.TestToolName:
			require.Contains(t, tool.Description, "nothing to reach for afterward",
				"the test tool must still answer the question its stdio text answered")
		}
	}

	// And the reduced description is a *derivation* of the full one rather
	// than a second copy: everything outside the paragraphs about
	// flowstate_run_local has to be word for word the same, or the two will
	// drift and only one of them will be corrected next time.
	require.NotEqual(t, flowmcp.TestToolDescription, flowmcp.ReducedTestToolDescription)
	require.Contains(t, flowmcp.ReducedTestToolDescription, "`tests` is a `*.test.yaml` document")
	require.Contains(t, flowmcp.TestToolDescription, "`tests` is a `*.test.yaml` document")
}

// TestMCPServeTestCallBudgetIncludesWaitingForTheLock is Codex's P1 on the
// last round, and it is the bounds-that-do-not-compose shape CLAUDE.md warns
// about: `--test-timeout` bounded how long a flowstate_test call *runs*, and
// the exclusive lock it waits for was acquired before that budget started.
//
// So a queued call began a fresh budget on reaching the front of the queue,
// and the surface's unavailability was the sum rather than the maximum:
// `--max-sessions` × `--max-session-requests` × `--test-timeout` from one
// burst, refillable indefinitely. Three bounds that each hold on their own and
// compose into none.
//
// Driven against the guard directly with the lock already held, which is what
// a queued call sees, and on a short budget so the wait is the whole test.
func TestMCPServeTestCallBudgetIncludesWaitingForTheLock(t *testing.T) {
	t.Parallel()

	const budget = 300 * time.Millisecond

	guard := newMCPServeRegistryGuard()

	// Somebody else is inside, and stays there for far longer than the budget
	// this call is allowed.
	require.NoError(t, guard.sem.Acquire(t.Context(), mcpServeRegistryReaders))
	defer guard.sem.Release(mcpServeRegistryReaders)

	var ran bool
	queued := guard.wrapTool(budget)(flowmcp.TestToolName,
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			ran = true

			return &mcp.CallToolResult{}, nil
		})

	started := time.Now()
	result, err := queued(t.Context(), &mcp.CallToolRequest{})
	elapsed := time.Since(started)

	require.NoError(t, err)
	require.False(t, ran, "the handler must not run: its whole budget was spent waiting for the lock")
	require.True(t, result.IsError, "a call that never started must say so rather than answering emptily")
	require.Less(t, elapsed, 5*time.Second,
		"waiting for the lock has to be inside the call's own budget, not before it")
	require.GreaterOrEqual(t, elapsed, budget, "and the call must actually have waited its budget out")
}

// TestMCPServeDebugToolTakesTheExclusiveRegistryLock is the direction that was
// missing when flowstate_debug landed (Codex, #1109): it drives the identical
// flowtest run through the identical door, so it registers a synthetic
// definition for any task a case stubs that this build does not have — and it
// was wrapped as a *reader*, which let a concurrent catalog read observe
// another caller's task names.
//
// The discriminator is *one* held unit, and getting that wrong is how this
// test was vacuous when first written: [mcpServeRegistryGuard.shared] takes a
// single unit and [mcpServeRegistryGuard.exclusive] takes all of them, so
// holding the whole semaphore blocks both and proves nothing about which path
// a tool took. Holding one blocks only a writer — which is exactly the claim.
func TestMCPServeDebugToolTakesTheExclusiveRegistryLock(t *testing.T) {
	t.Parallel()

	require.True(t, registryMutatingTools[flowmcp.DebugToolName],
		"flowstate_debug reaches flowtest.RunSourceWith, so it mutates the registry")

	fixture := newMCPServeFixtureWithTestTimeout(t, 2*time.Second)
	session := fixture.connect(t, fixture.goodToken("agent"))

	// One unit, as a concurrent reader holds: a writer cannot start beside it.
	require.NoError(t, fixture.guard.sem.Acquire(t.Context(), 1))

	called := make(chan struct{})
	go func() {
		defer close(called)
		_, _ = session.CallTool(t.Context(), &mcp.CallToolParams{
			Name: flowmcp.DebugToolName,
			Arguments: map[string]any{
				"workflow": "edition: v2026.3\nname: demo\nsteps:\n- id: hi\n  log:\n    message: hello\n",
				"tests":    "tests:\n  - name: it runs\n    stubs:\n      - task: log\n        returns: {}\n    expect:\n      ran: [hi]\n",
				"commands": []any{"continue"},
			},
		})
	}()

	select {
	case <-called:
		fixture.guard.sem.Release(1)
		t.Fatal("a flowstate_debug call ran beside a concurrent reader, so it is taking the shared " +
			"lock: its synthetic task names are visible to every catalog read, validate and compile " +
			"in flight")
	case <-time.After(250 * time.Millisecond):
	}

	fixture.guard.sem.Release(1)

	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("the debug call never ran after the lock was released")
	}
}
