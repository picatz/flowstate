package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// `flow mcp serve` is the MCP surface over streamable HTTP, as an OAuth 2.1
// protected resource. picatz/flowstate#558's slice two (S7a on #567), and its
// own command rather than a flag on `flow mcp` because #558's decision 2 turns
// on exactly that: every posture control `flow mcp` takes — --egress-policy,
// the secret flags, --reveal-sensitive, the result cap, the local-run timeout
// — is process-wide *because there is exactly one trusted caller*, and each
// becomes a per-principal question the moment there are several. A flag would
// have reinterpreted all of them silently. A verb cannot.
//
// Nothing in this file is reachable from `flow mcp`. The stdio path
// (cmd/flow/mcp.go's runMCP) is untouched, which is #558's decision 4: the MCP
// specification says implementations using stdio SHOULD NOT run this flow and
// SHOULD take credentials from the environment, so stdio is out of scope here
// rather than out of compliance.
//
// # What this surface serves, and what it deliberately does not
//
// Three refusals, each of them a capability the stdio surface has:
//
//   - flowstate_run_local is **absent**, not disabled. It executes a submitted
//     Flowfile in this process; over HTTP that is remote code execution as a
//     feature, and #558's decision 3 keeps it off this surface. Absent rather
//     than registered-and-refusing because a tool a model can see is a tool it
//     will try, and a tool list is the honest place to say no.
//   - Every RPC tool that dispatches to a *deployment* is absent too, which
//     goes beyond what #558 asked for and for a reason #558 itself names: this
//     process holds its own credential (cmd/flow/client.go's credential
//     source), so a tool that forwards an authenticated caller's request to
//     `--address` would spend *this process's* authority on their behalf. That
//     is the confused deputy the specification's "MUST NOT pass the client's
//     token through" rule is about, arrived at from the other direction. Until
//     this surface can authorize per principal (S7b), it serves only what
//     answers in this process and touches no run and no tenant:
//     [flowmcp.LocalTools] — validate, compile, get_catalog — plus
//     flowstate_test.
//   - flowstate_test **is** served, per #558's Q3: a stubbed run replaces
//     every task's implementation before a step executes, so it reaches
//     nothing whatever this process was started with. See cmd/flow/mcp.go's
//     comment above [testToolHandler] for the two independent proofs.
//
// And three postures, all fail-closed: no protected-resource configuration
// means no surface at all rather than an unauthenticated one; --insecure-no-auth
// is refused; --reveal-sensitive is refused.

// mcpServeDefaultListen is where this surface binds when an operator says
// nothing: loopback, so that the default of a command that serves a control
// plane is reachable from this machine and nowhere else. Any other address
// requires TLS here or --tls-terminated-upstream, exactly as `flow server`
// does — see [refusePlaintextListener], reused rather than restated.
const mcpServeDefaultListen = "127.0.0.1:8617"

// The two bounds this surface puts on what a peer may ask of it, both small
// on purpose and both overridable. CLAUDE.md's rule is to ask which resource
// the attacker controls and bound *that* one, and an authenticated MCP client
// controls two independently: how large one request is, and how many sessions
// it opens.
const (
	// mcpServeDefaultMaxRequestBytes bounds one request body. A tool call on
	// this surface carries a Flowfile and a test document — kilobytes — so a
	// megabyte is generous by two orders of magnitude while still refusing
	// the request whose only purpose is to be large. Deliberately far below
	// the SDK's own [mcp.DefaultMaxRequestBodyBytes] (4 MiB), which is a
	// general-purpose default rather than one chosen against this surface's
	// payloads.
	mcpServeDefaultMaxRequestBytes int64 = 1 << 20

	// mcpServeDefaultMaxSessions bounds how many streamable-HTTP sessions may
	// be open at once. A session holds a goroutine, a transport and an
	// initialized server for as long as it is alive, and how many exist is
	// the *client's* choice — one POST without an Mcp-Session-Id header opens
	// another. Bounding bytes does not bound this, and bounding this does not
	// bound bytes; they are two resources with two bounds, per CLAUDE.md.
	mcpServeDefaultMaxSessions = 32

	// mcpServeSessionIdleTimeout is how long a session may go without a
	// request before it is closed and its slot returned. Handed to the SDK as
	// [mcp.StreamableHTTPOptions.SessionTimeout] *and* used as this file's
	// own expiry, so the accounting below and the sessions it accounts for
	// expire on the same clock: a slot held by a session the SDK has already
	// closed would be a leak that eventually refuses every new caller.
	mcpServeSessionIdleTimeout = 5 * time.Minute
)

// mcpSessionHeader is the streamable-HTTP session header, spelled here
// because the SDK does not export it. A request carrying one addresses an
// existing session; a POST without one may create a new session, which is the
// only moment [mcpSessionLimiter] has anything to decide.
const mcpSessionHeader = "Mcp-Session-Id"

// addMCPServeFlags declares the serve surface's own flags.
//
// Its own set rather than the parent command's: cobra's local flags are not
// inherited, which is the property that makes this a genuinely separate
// surface rather than `flow mcp` wearing a hat. Nothing here widens what a
// call may reach, because there is nothing on this surface that a flag could
// widen — the tools that read a flag are the ones this command does not
// serve.
func addMCPServeFlags(cmd *cobra.Command) {
	cmd.Flags().String("listen", mcpServeDefaultListen,
		"address to serve the MCP surface on. Anything but a loopback address requires "+
			"--tls-cert-file/--tls-key-file, or --tls-terminated-upstream when a proxy in "+
			"front of this process already terminates TLS: a bearer token on a cleartext "+
			"connection that leaves this machine is a credential handed to whatever is in between")

	cmd.Flags().String("auth-policy", os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to the trust policy whose issuers may mint tokens for this surface "+
			"(overrides FLOWSTATE_AUTH_POLICY). Required: there is no anonymous variant of "+
			"this surface, and --insecure-no-auth is refused here")

	addProtectedResourceFlags(cmd)
	addTLSFlags(cmd)

	cmd.Flags().Int64("max-request-bytes", mcpServeDefaultMaxRequestBytes,
		"largest request body this surface will read, in bytes. A request over the limit is "+
			"refused with 413 rather than buffered")

	cmd.Flags().Int("max-sessions", mcpServeDefaultMaxSessions,
		"how many MCP sessions may be open at once. A request that would open one past the "+
			"limit is refused with 503; sessions idle for "+mcpServeSessionIdleTimeout.String()+
			" are closed and their slots returned")

	// Declared so that typing it gets a reason rather than "unknown flag".
	// See [checkMCPServeFlags] for why this surface refuses it
	// outright rather than honouring it.
	addRevealSensitiveFlag(cmd)

	// Same: declared to be refused. `flow server` accepts it for loopback
	// development; a protected resource that admits everyone is not a
	// protected resource, so it cannot mean anything here.
	cmd.Flags().Bool("insecure-no-auth", false,
		"refused on this surface: an OAuth 2.1 protected resource that authenticates nobody "+
			"is a contradiction. Use `flow mcp` over stdio for local development")
}

// mcpServeFlags is what an operator asked for, read once before anything is
// validated or bound.
type mcpServeFlags struct {
	listen                 string
	policyPath             string
	insecure               bool
	revealSensitive        bool
	maxRequestBytes        int64
	maxSessions            int
	protectedResourceFlags protectedResourceFlags
	tls                    tlsFlags
}

// mcpServeFlagsOf reads them off the command being run.
func mcpServeFlagsOf(cmd *cobra.Command) mcpServeFlags {
	listen, _ := cmd.Flags().GetString("listen")
	policyPath, _ := cmd.Flags().GetString("auth-policy")
	insecure, _ := cmd.Flags().GetBool("insecure-no-auth")
	maxRequestBytes, _ := cmd.Flags().GetInt64("max-request-bytes")
	maxSessions, _ := cmd.Flags().GetInt("max-sessions")

	return mcpServeFlags{
		listen:                 listen,
		policyPath:             policyPath,
		insecure:               insecure,
		revealSensitive:        revealSensitiveRequested(cmd),
		maxRequestBytes:        maxRequestBytes,
		maxSessions:            maxSessions,
		protectedResourceFlags: protectedResourceFlagsOf(cmd),
		tls:                    tlsFlagsOf(cmd),
	}
}

// checkMCPServeFlags is every refusal this surface makes before it binds
// anything, in one place so a test can reach each without a listener.
//
// Ordered by how fundamental the refusal is rather than by flag name: a
// surface with no protected resource does not exist at all, so that is first;
// the two flags that would each individually turn this into something other
// than a protected resource come next; and the two bounds, which are refusals
// about a value rather than about a posture, come last.
func checkMCPServeFlags(flags mcpServeFlags) error {
	if flags.protectedResourceFlags.resource == "" {
		return errors.New("`flow mcp serve` requires --protected-resource (and at least one " +
			"--authorization-server): this surface is an OAuth 2.1 protected resource, and a " +
			"resource identifier is what every accepted token's audience is checked against " +
			"(RFC 8707 section 2) and what the RFC 9728 document a client bootstraps from " +
			"advertises. There is no unauthenticated variant of it — for a surface with no " +
			"identity provider in front of it, `flow mcp` over stdio is the supported shape")
	}

	if flags.insecure {
		return errors.New("--insecure-no-auth is refused on `flow mcp serve`: every caller here is " +
			"a bearer token this process verifies, and a surface that admits an anonymous " +
			"caller can neither bind a token to this resource nor pin a session to whoever " +
			"opened it. Use `flow mcp` over stdio for local development")
	}

	if flags.revealSensitive {
		return errors.New("--reveal-sensitive is refused on `flow mcp serve`: over stdio it is one " +
			"deliberate decision by the person who started the process and is the only caller, " +
			"and over HTTP the same sentence reads \"show declared-sensitive values in the clear " +
			"to whoever authenticates\". Values declared `sensitive: true` are redacted on this " +
			"surface with no way to turn that off")
	}

	if flags.policyPath == "" {
		return errors.New("`flow mcp serve` requires --auth-policy (or FLOWSTATE_AUTH_POLICY): the " +
			"trust policy names the issuers whose tokens this surface accepts, and without one " +
			"there is nothing to verify a token against")
	}

	if flags.maxRequestBytes <= 0 {
		return fmt.Errorf("--max-request-bytes must be positive; got %d. There is no \"unlimited\" "+
			"spelling: a request body is chosen by the peer, and this surface bounds it",
			flags.maxRequestBytes)
	}

	if flags.maxSessions <= 0 {
		return fmt.Errorf("--max-sessions must be positive; got %d. There is no \"unlimited\" "+
			"spelling: how many sessions exist is chosen by the peer, and this surface bounds it",
			flags.maxSessions)
	}

	return nil
}

// mcpServeLimits is the two bounds, resolved.
type mcpServeLimits struct {
	maxRequestBytes int64
	maxSessions     int
	sessionIdle     time.Duration
}

// mcpServeHandler assembles the whole HTTP surface: the RFC 9728 document
// outside authentication, and the MCP endpoint behind it.
//
// Separated from [runMCPServe] so that every property this file claims —
// which tokens are refused, which tools are served, that both bounds are
// reached — is testable over an [httptest.Server] with no flags, no policy
// file, and no socket of this command's own. The wiring order below is the
// security-relevant part of it and is written outermost-first:
//
//  1. **The byte cap**, applied to every request body before anything else
//     reads one. Below the MCP library rather than only through its own
//     [mcp.StreamableHTTPOptions.MaxRequestBodyBytes] option, for the reason
//     CLAUDE.md gives about connect-go's non-200 path: a cap configured
//     through a library option covers the paths that library thought about.
//     Both are set — the SDK's so that its own 413 is what a client sees on
//     the ordinary path, this one so no path can miss it.
//  2. **Cross-origin protection**, because a browser on a developer's machine
//     is a caller this surface never intends to have, and the SDK's own
//     option for it is deprecated in favour of exactly this wrapping.
//  3. **The bearer token**, which is where an unauthenticated request stops.
//     The 401 it writes carries `WWW-Authenticate: Bearer resource_metadata=…`
//     naming the document mounted at step 5 — PR-1's mechanism
//     ([auth.ProtectedResource.MetadataURL]), reused rather than a second
//     challenge built here. No `scope` parameter: #567's D1 is deferred by
//     omission, so [mcpauth.RequireBearerTokenOptions.Scopes] stays empty and
//     the middleware emits no scope at all.
//  4. **The session bound**, inside authentication so that an unauthenticated
//     caller can never consume a session slot — a bound an anonymous peer can
//     exhaust is a denial of service with extra steps.
//  5. The metadata document, mounted outside all of the above, because a
//     client fetches it *before* it holds any credential. That is the same
//     reasoning cmd/flow/routing.go's serverHandler already gives for the
//     discovery document, and getting it wrong is the failure that looks like
//     a signing bug.
func mcpServeHandler(
	logger *slog.Logger,
	srv *mcp.Server,
	verifier auth.Verifier,
	protectedResource *auth.ProtectedResource,
	limits mcpServeLimits,
) (http.Handler, error) {
	if protectedResource == nil {
		// Unreachable from [runMCPServe], which refuses an unconfigured
		// surface in [checkMCPServeFlags] long before this. Restated here
		// because this function is the one a test calls directly, and a
		// helper that quietly serves an unauthenticated MCP endpoint when
		// handed a nil is the fail-open this whole file is about.
		return nil, errors.New("mcpServeHandler: no protected resource; this surface cannot be served unauthenticated")
	}
	if verifier == nil {
		return nil, errors.New("mcpServeHandler: no verifier; this surface cannot be served unauthenticated")
	}

	streamable := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{
			MaxRequestBodyBytes: limits.maxRequestBytes,
			SessionTimeout:      limits.sessionIdle,
			Logger:              logger,
		},
	)

	limiter := newMCPSessionLimiter(limits.maxSessions, limits.sessionIdle, time.Now)

	authenticated := mcpauth.RequireBearerToken(
		auth.MCPTokenVerifier(verifier, protectedResource.Resource()),
		&mcpauth.RequireBearerTokenOptions{
			ResourceMetadataURL: protectedResource.MetadataURL(),
			// Scopes deliberately empty: see this function's doc, step 3.
		},
	)(limiter.wrap(streamable))

	protection := http.NewCrossOriginProtection()

	mux := http.NewServeMux()
	mux.Handle(protectedResource.ResourcePath(), protection.Handler(authenticated))
	mux.Handle(protectedResource.Path(), protectedResource.Handler())

	return maxRequestBodyBytes(mux, limits.maxRequestBytes), nil
}

// maxRequestBodyBytes caps every request body below the MCP library, so that
// no path through it — including whatever it does with a request it refuses
// for its own reasons — can read more than this. See [mcpServeHandler]'s doc,
// step 1, and CLAUDE.md's account of the same lesson on plugin/transport.go.
func maxRequestBodyBytes(next http.Handler, limit int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}

// mcpSessionLimiter bounds how many streamable-HTTP sessions exist at once.
//
// The SDK has no such bound of its own and no callback when a session ends,
// so the accounting is here, keyed off the one thing that is observable from
// the outside: the Mcp-Session-Id header, which a response carries when a
// session is created and a request carries on every call afterwards.
//
// Three things it has to get right, and each is a way a bound can be
// ineffective rather than absent:
//
//   - A slot is reserved *before* the request that may create a session is
//     served, not after — otherwise many concurrent initializes all see room
//     and all get it.
//   - A slot is released when the session goes away, by an explicit DELETE
//     that succeeded or by idling out. The idle period is the same value the
//     SDK closes sessions at, so a slot cannot outlive the session holding
//     it; without that, this bound eventually refuses everyone.
//   - Requests that address an existing session are never counted again, and
//     never refused for lack of a slot: the session already has one.
type mcpSessionLimiter struct {
	max  int
	idle time.Duration
	now  func() time.Time

	mu sync.Mutex
	// active maps a live session id to when it was last seen.
	active map[string]time.Time
	// pending counts reservations held by in-flight requests that may yet
	// return a session id. Counted alongside active so that concurrent
	// initializes cannot collectively exceed max.
	pending int
}

// newMCPSessionLimiter builds one. now is injectable so a test can reach the
// idle-expiry path without sleeping.
func newMCPSessionLimiter(max int, idle time.Duration, now func() time.Time) *mcpSessionLimiter {
	return &mcpSessionLimiter{
		max:    max,
		idle:   idle,
		now:    now,
		active: make(map[string]time.Time),
	}
}

// wrap applies the bound to next.
func (l *mcpSessionLimiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := r.Header.Get(mcpSessionHeader); id != "" {
			l.touch(id)

			recorder := &mcpSessionRecorder{ResponseWriter: w}
			next.ServeHTTP(recorder, r)

			// A DELETE the SDK accepted is the client saying the session is
			// over; anything else leaves it alive to idle out on its own.
			if r.Method == http.MethodDelete && recorder.status >= 200 && recorder.status < 300 {
				l.release(id)
			}

			return
		}

		if !l.reserve() {
			// 503 rather than 429: the request is not being rate limited, the
			// server is at capacity for a resource it holds. Retry-After is a
			// plain hint, not a promise.
			w.Header().Set("Retry-After", "30")
			http.Error(w, "too many open sessions", http.StatusServiceUnavailable)

			return
		}

		recorder := &mcpSessionRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		// The reservation becomes a session when the response named one, and
		// is simply given back when it did not — a POST that opened nothing
		// must not hold a slot.
		l.settle(recorder.Header().Get(mcpSessionHeader))
	})
}

// reserve takes a slot for a request that may create a session, reporting
// whether one was available.
func (l *mcpSessionLimiter) reserve() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.expire()
	if len(l.active)+l.pending >= l.max {
		return false
	}
	l.pending++

	return true
}

// settle converts a reservation into a session, or gives it back when the
// request created none.
func (l *mcpSessionLimiter) settle(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pending--
	if id != "" {
		l.active[id] = l.now()
	}
}

// touch records that a live session was used, deferring its idle expiry. A
// session id this limiter has never seen is deliberately not added: it
// belongs to no reservation, and adding it would let a caller inflate the
// count with invented ids the SDK will answer 404 for anyway.
func (l *mcpSessionLimiter) touch(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, known := l.active[id]; known {
		l.active[id] = l.now()
	}
}

// release drops a session's slot.
func (l *mcpSessionLimiter) release(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.active, id)
}

// expire drops sessions idle past the timeout. Called with l.mu held.
func (l *mcpSessionLimiter) expire() {
	cutoff := l.now().Add(-l.idle)
	for id, seen := range l.active {
		if seen.Before(cutoff) {
			delete(l.active, id)
		}
	}
}

// open reports how many sessions are currently counted, for tests that assert
// a slot was returned rather than only that a request succeeded.
func (l *mcpSessionLimiter) open() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.active) + l.pending
}

// mcpSessionRecorder captures the status code and lets the session header be
// read back after the inner handler has written it.
type mcpSessionRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader records the status on its way through.
func (r *mcpSessionRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Write records the implicit 200 a handler that never called WriteHeader
// produces, so a DELETE answered with a bare body is still read as success.
func (r *mcpSessionRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}

	return r.ResponseWriter.Write(b)
}

// Flush passes through to the underlying writer when it supports flushing.
// Streamable HTTP answers over server-sent events, which are useless if they
// arrive only when the handler returns; a wrapper that swallows Flush turns a
// stream into a batch.
func (r *mcpSessionRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// mcpServeTools builds the server this surface serves: the RPC tools that
// answer in this process, plus flowstate_test, and nothing else. See this
// file's package comment for why the rest are absent rather than disabled.
func mcpServeTools() *mcp.Server {
	srv := flowmcp.NewServer(version)

	flowmcp.AddLocalCapabilities(
		srv,
		// The same nil-Temporal-client server `flow mcp` answers Validate,
		// Compile and GetCatalog from — see server/validate.go for why a nil
		// client is safe for exactly those.
		server.New(nil),
		flowmcp.Deps{
			// Nothing on this surface answers with a GetResponse — the tool
			// that would (flowstate_get) is not served — but Deps documents a
			// nil Redact as "nothing is ever withheld", and a field that means
			// that is not one to leave unset on a surface whose whole subject
			// is what a caller may see. Redacting unconditionally, with no
			// specification in reach, is the fail-closed answer cmd/flow's
			// sensitive.go already defines, and --reveal-sensitive cannot
			// reach it: this command refuses that flag outright.
			Redact: func(response *v1.GetResponse) *v1.GetResponse {
				return redactGetResponse(response, nil, false)
			},
		},
		flowmcp.ToolRegistration{Tool: flowmcp.TestTool(), Handler: testToolHandler()},
	)

	return srv
}

// runMCPServe implements the `mcp serve` sub-command.
func runMCPServe(cmd *cobra.Command, _ []string) error {
	logger := infraLogger()

	flags := mcpServeFlagsOf(cmd)
	if err := checkMCPServeFlags(flags); err != nil {
		return err
	}

	// The trust policy and the verifier built from it, before anything is
	// bound: an unreadable policy must stop this command rather than leave it
	// serving under one nobody configured.
	verifier, policy, err := authVerifier(authFlags{policyPath: flags.policyPath})
	if err != nil {
		return err
	}

	// Resolved against that same policy, so an advertised authorization
	// server whose tokens this deployment's own verifier would refuse for
	// this resource is a start-up failure here rather than a wall of 401s a
	// client meets after already trusting the document. See
	// cmd/flow/protectedresource.go.
	protectedResource, err := resolveProtectedResource(flags.protectedResourceFlags, policy)
	if err != nil {
		return err
	}
	if protectedResource == nil {
		// checkMCPServeFlags already refused an empty --protected-resource, so
		// this is unreachable; kept because the alternative to an explicit
		// refusal here is a nil reaching mcpServeHandler.
		return errors.New("`flow mcp serve`: no protected resource was resolved")
	}

	// The metadata document's path and the resource's own path are both
	// derived from the configured resource, and a resource whose path *is*
	// the well-known prefix would have the two collide on one mux. ServeMux
	// panics on a duplicate pattern, so this is a diagnosis rather than a
	// crash — the same check cmd/flow/protectedresource.go makes against the
	// server's fixed routes.
	if protectedResource.ResourcePath() == protectedResource.Path() {
		return fmt.Errorf("--protected-resource: the resource's own path %q is identical to the "+
			"RFC 9728 metadata path computed for it; choose a resource path that does not spell "+
			"the well-known location", protectedResource.ResourcePath())
	}

	tlsCfg, err := serverTLSConfig(flags.tls)
	if err != nil {
		return err
	}
	if err := refusePlaintextListener(flags.listen, tlsCfg, flags.tls.tlsTerminatedUpstream); err != nil {
		return err
	}

	// Fetch every trusted issuer's keys now, so a misconfigured or
	// unreachable issuer is reported at start-up rather than as a puzzling
	//401 on the first tool call. Log-and-continue, exactly as runServer does
	// and for the reason [auth.OIDCVerifier.Prime] gives: keys are fetched on
	// demand anyway.
	if oidc, ok := verifier.(*auth.OIDCVerifier); ok {
		if err := oidc.Prime(cmd.Context()); err != nil {
			logger.Warn("could not prefetch every trusted issuer's keys; verification will retry on demand",
				"error", err)
		}
	}

	handler, err := mcpServeHandler(logger, mcpServeTools(), verifier, protectedResource, mcpServeLimits{
		maxRequestBytes: flags.maxRequestBytes,
		maxSessions:     flags.maxSessions,
		sessionIdle:     mcpServeSessionIdleTimeout,
	})
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:      flags.listen,
		Handler:   handler,
		TLSConfig: tlsCfg,

		// The same explicit timeouts `flow server` sets, for the same reason:
		// Go's zero values mean no timeout at all, so a peer that opens a
		// connection and sends bytes slowly, or never, holds it forever.
		// WriteTimeout is deliberately absent rather than long: a streamable
		// HTTP session answers over server-sent events that stay open by
		// design, and a write deadline would cut every one of them at the
		// same age. ReadHeaderTimeout and IdleTimeout still bound the slow
		// peer this would otherwise be protecting against, and the session
		// idle timeout above bounds a stream nobody is using.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	listener, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", httpServer.Addr, err)
	}

	logger.Info("serving MCP over HTTP as a protected resource",
		"address", httpServer.Addr,
		"tls", tlsCfg != nil,
		"resource", protectedResource.Resource(),
		"metadata_url", protectedResource.MetadataURL(),
		"max_request_bytes", flags.maxRequestBytes,
		"max_sessions", flags.maxSessions)

	serveErr := make(chan error, 1)
	go func() {
		var err error
		if tlsCfg != nil {
			err = httpServer.ServeTLS(listener, "", "")
		} else {
			err = httpServer.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("serving on %s: %w", httpServer.Addr, err)

			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-cmd.Context().Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutting down: %w", err)
		}

		return <-serveErr
	}
}
