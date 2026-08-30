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
	"golang.org/x/sync/semaphore"

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

	// mcpServeDefaultMaxSessionRequests bounds how many requests one session
	// may have in flight at once.
	//
	// --max-sessions bounds how many sessions exist and bounds nothing about
	// what a caller does inside one: an established session id can be replayed
	// over arbitrarily many parallel connections or HTTP/2 streams, each
	// getting a goroutine and each either running under the registry guard or
	// queued behind an exclusive flowstate_test. That is a third resource the
	// peer controls the ratio to, so per CLAUDE.md it gets its own bound
	// rather than being assumed covered by the other two. Reported by Codex on
	// picatz/flowstate#807.
	//
	// Per session rather than global, because sessions are already bounded:
	// the product is what bounds the whole surface, and a per-session share
	// keeps one caller's parallelism from being spendable on another's
	// allowance. Eight leaves room for the standalone SSE stream a client
	// holds open plus ordinary request concurrency, and is far above what any
	// real MCP client does.
	mcpServeDefaultMaxSessionRequests = 8

	// mcpServeDefaultTestTimeout bounds one flowstate_test call, mirroring
	// `flow mcp`'s own --run-local-timeout default for the same reason that
	// flag gives: a tool call that never returns holds a model's turn open for
	// as long as the workflow asks, and the workflow is the untrusted input.
	// It matters more here, because a flowstate_test call also holds this
	// surface's exclusive registry lock while it runs — so the bound is what
	// keeps one caller from stopping the surface for everyone rather than only
	// for themselves.
	mcpServeDefaultTestTimeout = 2 * time.Minute
)

// mcpSessionHeader is the streamable-HTTP session header, spelled here
// because the SDK does not export it. A request carrying one addresses an
// existing session; a POST without one may create a new session, which is the
// only moment [mcpSessionLimiter] has anything to decide.
const mcpSessionHeader = "Mcp-Session-Id"

// mcpServeSessionStorage is the operator-facing spelling for the state model
// this command actually runs. It is emitted at startup and pinned in tests so
// a process-local handler cannot quietly be presented as a shared service.
const mcpServeSessionStorage = "process_memory"

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

	addProtectedResourceFlags(cmd, "Required on this command: this surface is the protected "+
		"resource, so without one there is nothing to bind a token's audience to and `flow mcp "+
		"serve` refuses to start rather than serving an unauthenticated MCP endpoint")
	addTLSFlags(cmd)

	cmd.Flags().Int64("max-request-bytes", mcpServeDefaultMaxRequestBytes,
		"largest request body this surface will read, in bytes. A request over the limit is "+
			"refused with 413 rather than buffered")

	cmd.Flags().Int("max-session-requests", mcpServeDefaultMaxSessionRequests,
		"how many requests one MCP session may have in flight in this process at once. A request past the limit "+
			"is refused with 503: --max-sessions bounds how many sessions exist and says nothing "+
			"about how many connections one of them is replayed over")

	cmd.Flags().Duration("test-timeout", mcpServeDefaultTestTimeout,
		"how long one flowstate_test call may run before it is stopped and reported as timed out. "+
			"A submitted workflow can park forever on its own — a `wait_for_signal:` with no timeout "+
			"and no scripted signal never completes — and while one runs, every other tool and "+
			"resource on this surface waits for it")

	cmd.Flags().Int("max-sessions", mcpServeDefaultMaxSessions,
		"how many MCP sessions may be open in this process at once. A request that would open one past the "+
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
	maxSessionRequests     int
	testTimeout            time.Duration
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
	maxSessionRequests, _ := cmd.Flags().GetInt("max-session-requests")
	testTimeout, _ := cmd.Flags().GetDuration("test-timeout")

	return mcpServeFlags{
		listen:                 listen,
		policyPath:             policyPath,
		insecure:               insecure,
		revealSensitive:        revealSensitiveRequested(cmd),
		maxRequestBytes:        maxRequestBytes,
		maxSessions:            maxSessions,
		maxSessionRequests:     maxSessionRequests,
		testTimeout:            testTimeout,
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

	if flags.maxSessionRequests <= 0 {
		return fmt.Errorf("--max-session-requests must be positive; got %d. There is no \"unlimited\" "+
			"spelling: how many requests one session has in flight is chosen by the peer, and this "+
			"surface bounds it", flags.maxSessionRequests)
	}

	if flags.testTimeout <= 0 {
		return fmt.Errorf("--test-timeout must be positive; got %s. There is no \"unlimited\" "+
			"spelling: a submitted workflow decides how long its own run takes, and a case the "+
			"virtual clock cannot advance past never ends on its own",
			flags.testTimeout)
	}

	return nil
}

// mcpServeLimits is the two bounds, resolved.
type mcpServeLimits struct {
	maxRequestBytes    int64
	maxSessions        int
	maxSessionRequests int
	sessionIdle        time.Duration
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
//     challenge built here. The protected-resource document advertises the
//     schema-owned scope vocabulary, but no request enforces a scope yet, so
//     [mcpauth.RequireBearerTokenOptions.Scopes] stays empty and the middleware
//     emits no `scope` challenge parameter.
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

	limiter := newMCPSessionLimiter(limits.maxSessions, limits.maxSessionRequests, limits.sessionIdle, time.Now)

	authenticated := mcpauth.RequireBearerToken(
		auth.MCPTokenVerifier(verifier, protectedResource.Resource(),
			auth.WithMCPFailureObserver(func(ctx context.Context, req *http.Request, err error) {
				if observation, ok := ctx.Value(mcpAuthenticationObservationKey{}).(*mcpAuthenticationObservation); ok {
					observation.reason = auth.PublicReason(err)
				}
			})),
		&mcpauth.RequireBearerTokenOptions{
			ResourceMetadataURL: protectedResource.MetadataURL(),
			// Scopes deliberately empty: see this function's doc, step 3.
		},
	)(limiter.wrap(streamable))
	authenticated = observeMCPAuthenticationFailures(logger, authenticated)

	protection := http.NewCrossOriginProtection()

	// Both paths are derived from the one configured resource, so they cannot
	// collide for any input [auth.NewProtectedResource] accepts — the
	// metadata path is the well-known prefix *plus* the resource's own path.
	// Checked anyway, because the failure if it ever became reachable is an
	// http.ServeMux panic at start-up rather than a diagnosis, and a
	// second registration of one pattern is exactly what ServeMux panics on.
	if protectedResource.ResourcePath() == protectedResource.Path() {
		return nil, fmt.Errorf("--protected-resource: the resource's own path %q is identical to the "+
			"RFC 9728 metadata path computed for it; the two would register one route and this "+
			"server would panic rather than serve either", protectedResource.ResourcePath())
	}

	mux := http.NewServeMux()
	mux.Handle(exactPattern(protectedResource.ResourcePath()), protection.Handler(authenticated))
	mux.Handle(protectedResource.Path(), protectedResource.Handler())

	return maxRequestBodyBytes(mux, limits.maxRequestBytes), nil
}

// logMCPServeSessionTopology makes the process-local deployment contract an
// executable startup diagnostic. It is a warning because a second replica or
// a restart does not merely change capacity: it makes an existing session id
// unknown unless every request remains on the process that minted it.
func logMCPServeSessionTopology(logger *slog.Logger) {
	logger.Warn("MCP sessions are process-local; run one replica and expect restarts to invalidate active sessions",
		"session_storage", mcpServeSessionStorage,
		"session_affinity_header", mcpSessionHeader,
		"horizontal_scaling", false)
}

type mcpAuthenticationObservationKey struct{}

type mcpAuthenticationObservation struct {
	reason string
}

// observeMCPAuthenticationFailures records every refusal from the SDK bearer
// middleware, including a missing Authorization header that it rejects before
// calling [auth.MCPTokenVerifier]. The verifier contributes [auth.PublicReason]
// when it ran; the SDK-only paths use fixed classifications rather than its
// response body, which is peer-visible text and not a logging boundary.
func observeMCPAuthenticationFailures(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		observation := &mcpAuthenticationObservation{}
		req = req.WithContext(context.WithValue(req.Context(), mcpAuthenticationObservationKey{}, observation))
		recorder := &mcpSessionRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, req)

		if recorder.status != http.StatusUnauthorized && recorder.status != http.StatusForbidden {
			return
		}
		reason := observation.reason
		if reason == "" {
			if recorder.status == http.StatusUnauthorized {
				reason = "missing bearer token"
			} else {
				reason = "authenticated caller is not permitted on this session"
			}
		}
		logger.WarnContext(req.Context(), "rejected MCP request",
			"path", req.URL.Path,
			"peer", req.RemoteAddr,
			"status", recorder.status,
			"reason", reason)
	})
}

// exactPattern renders a path as an [http.ServeMux] pattern that matches that
// path and nothing below it.
//
// Every pattern ServeMux registers is exact *except* one ending in "/", which
// is a subtree match. A resource identifier naming a bare origin has the path
// "/" (see [auth.ProtectedResource.ResourcePath]), so registering it verbatim
// would serve the MCP endpoint at every path on the listener — /healthz,
// /anything, a mistyped route — rather than at the one URI the advertised
// identifier names. "{$}" is ServeMux's own spelling for "end of path", which
// makes the root exact; [auth.validateResourceURI] refuses "{" and "}" in a
// resource path, so this can never collide with a path an operator wrote.
//
// Any other path is already exact, because the same validation refuses a
// trailing slash.
func exactPattern(path string) string {
	if path == "/" {
		return "/{$}"
	}

	return path
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
	max           int
	maxPerSession int
	idle          time.Duration
	now           func() time.Time

	mu sync.Mutex
	// active maps a live session id to when it was last seen.
	active map[string]time.Time
	// inflight counts requests currently being served for each session id.
	// Bounded per session because sessions are themselves bounded, so the
	// product bounds the surface — see [mcpServeDefaultMaxSessionRequests].
	inflight map[string]int
	// pending counts reservations held by in-flight requests that may yet
	// return a session id. Counted alongside active so that concurrent
	// initializes cannot collectively exceed max.
	pending int
}

// newMCPSessionLimiter builds one. now is injectable so a test can reach the
// idle-expiry path without sleeping.
func newMCPSessionLimiter(max, maxPerSession int, idle time.Duration, now func() time.Time) *mcpSessionLimiter {
	return &mcpSessionLimiter{
		max:           max,
		maxPerSession: maxPerSession,
		idle:          idle,
		now:           now,
		active:        make(map[string]time.Time),
		inflight:      make(map[string]int),
	}
}

// wrap applies the bound to next.
func (l *mcpSessionLimiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := r.Header.Get(mcpSessionHeader); id != "" {
			// Bounded before anything is served, because this branch is the
			// one an established session takes and --max-sessions does not
			// reach it: one id replayed over many connections is many
			// goroutines, each queued behind the registry guard.
			if !l.enter(id) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "too many requests in flight for this session", http.StatusServiceUnavailable)

				return
			}
			defer l.leave(id)

			l.touch(id)

			recorder := &mcpSessionRecorder{ResponseWriter: w}
			next.ServeHTTP(recorder, r)

			switch {
			// A DELETE the SDK accepted is the client saying the session is
			// over.
			case r.Method == http.MethodDelete && recorder.status >= 200 && recorder.status < 300:
				l.release(id)

			// The SDK does not know this session, so neither should this
			// accounting. Without it, a slot outlives the session it counted:
			// the SDK closes an idle session on its own clock, and a request
			// naming that id afterwards would be refreshed by the touch above
			// — held for another idle window, refreshable again, indefinitely.
			// Slots that count nothing eventually refuse every real caller,
			// which is this bound becoming the outage it exists to prevent.
			// Reported by Codex on #807.
			case recorder.status == http.StatusNotFound:
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
//
// It runs before the request is served rather than after, so that a session
// held open by a long-lived stream keeps its slot for the whole stream rather
// than expiring underneath it. The cost is small and worth naming: an
// authenticated caller who guesses another principal's session id can defer
// that session's idle expiry, while still being refused the session itself
// (the SDK answers 403 on a principal mismatch). Deferring somebody's expiry
// grants nothing and reaches nothing; losing a live stream's slot would.
func (l *mcpSessionLimiter) touch(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, known := l.active[id]; known {
		l.active[id] = l.now()
	}
}

// enter takes one of a session's in-flight request slots, reporting whether
// one was available.
func (l *mcpSessionLimiter) enter(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.inflight[id] >= l.maxPerSession {
		return false
	}
	l.inflight[id]++

	return true
}

// leave gives one back, and drops the entry entirely at zero so an id that is
// finished with leaves nothing behind.
func (l *mcpSessionLimiter) leave(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.inflight[id] <= 1 {
		delete(l.inflight, id)

		return
	}
	l.inflight[id]--
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

// Unwrap exposes the writer underneath, which is how [http.ResponseController]
// reaches capabilities this wrapper does not itself implement — a write
// deadline, most relevantly, which is what an SSE stream extends as it goes.
// Without it a wrapped writer silently loses every such capability, and the
// symptom is a stream that dies at a deadline nobody set.
func (r *mcpSessionRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// mcpServeTools builds the server this surface serves: the RPC tools that
// answer in this process, plus flowstate_test, and nothing else. See this
// file's package comment for why the rest are absent rather than disabled.
//
// Every tool and every resource it registers runs behind guard, so no caller
// ever reads the task registry while another caller's flowstate_test has it
// swapped — see [mcpServeRegistryGuard].
//
// testTimeout bounds one flowstate_test call. It is not a nicety: flowtest's
// virtual clock advances only when every participant is parked, so a
// `wait_for_signal:` with no timeout and no scripted signal has no deadline to
// advance to and the case never completes. That is a legal Flowfile, the
// submitted workflow is untrusted input, and without the bound such a call
// would hold this surface's exclusive registry lock and its goroutine
// forever. Reported by Codex on picatz/flowstate#807.
// guard is taken rather than made here so a test can hold its lock from the
// outside and prove that every served handler — tools and resources alike —
// really is behind it, which is a property of this wiring rather than of the
// guard itself.
func mcpServeTools(guard *mcpServeRegistryGuard, testTimeout time.Duration) (*mcp.Server, error) {
	srv := flowmcp.NewServer(version)

	// The same nil-Temporal-client server `flow mcp` answers Validate,
	// Compile and GetCatalog from — see server/validate.go for why a nil
	// client is safe for exactly those. No option is passed, so nothing here
	// can be misconfigured; the error is returned rather than dropped so that
	// stays true of whatever options this surface grows.
	local, err := server.New(nil)
	if err != nil {
		return nil, err
	}

	flowmcp.AddLocalCapabilities(
		srv,
		local,
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

			WrapHandler:         guard.wrapTool(testTimeout),
			WrapResourceHandler: guard.wrapResource,
		},
		// [flowmcp.ReducedTestTool] rather than TestTool: the stdio
		// description tells a model to reach for flowstate_run_local
		// afterward, and this surface does not serve it. Same schema, same
		// handler, one paragraph that is true here.
		flowmcp.ToolRegistration{Tool: flowmcp.ReducedTestTool(), Handler: testToolHandler(testTimeout)},

		// The debug tool is served here for the reason the test tool is: it
		// runs the identical stubbed run — no egress, no secret resolved, a
		// virtual clock — and adds only questions asked at its step
		// boundaries. It takes the same timeout and the same *exclusive*
		// registry guard ([registryMutatingTools]), because it reaches
		// [flowtest.RunSourceWith] through the same door and can stub a task
		// this build does not register exactly as a test case can. A finite script cannot hold the run open: when it runs
		// out the session resumes, so the bound that ends a test call ends
		// this one.
		flowmcp.ToolRegistration{Tool: flowmcp.DebugTool(), Handler: debugToolHandler(testTimeout)},
	)

	return srv, nil
}

// mcpServeRegistryGuard serializes this surface's tools against the one piece
// of process-wide state a caller can make them mutate: [v1.DefaultRegistry].
//
// The problem it solves, reported by Codex on picatz/flowstate#807. A
// flowstate_test case may stub a task this build does not register — a plugin
// task, typically — and [flowtest.RunSource] handles that by registering a
// synthetic definition into the *global* registry so the submitted workflow
// can be compiled at all, then putting the registry back (see swapRegistry in
// pkg/flowstate/v1/flowtest/run.go). Two consequences are harmless on stdio
// and are not here:
//
//   - **The synthetic names are left behind.** Over stdio that is one trusted
//     caller in a short-lived process. Over HTTP the stub names are chosen by
//     whoever holds a token, so the global registry grows without bound across
//     calls, and flowstate_get_catalog — which reads that same registry —
//     starts advertising one caller's invented tasks to every other caller,
//     until the catalog no longer fits under [flowmcp.MaxResultBytes] and
//     answers nobody. That is a caller writing into state every other
//     caller reads, which is the tenancy failure CLAUDE.md is about, arriving
//     through a test harness.
//   - **The swap is not concurrency-safe by design.** flowtest's own comment
//     says so: cases run in sequence and must not run concurrently with
//     anything else touching the same registry. On stdio nothing does. Here
//     two callers would.
//
// So this guard does two things. It takes the write lock for the duration of
// a flowstate_test call and the read lock for every other tool, so no caller
// ever observes the registry mid-swap; and it removes, after the call, every
// name that appeared during it — [v1.Registry.Unregister], the operation
// flowtest's own comment predates.
//
// Repair rather than a fix in flowtest: leaving a synthetic name registered
// is deliberate there ("fails loudly rather than silently resolving to a task
// that does not exist anywhere else") and is the right answer for a one-shot
// `flow test` process. What is wrong is not that behavior but this surface
// inheriting it, so the correction belongs to the surface.
//
// # Why the lock is a semaphore and not a sync.RWMutex
//
// A [sync.RWMutex] cannot be acquired with a deadline, and that turned the
// guard into an amplifier: a queued flowstate_test call waited for the lock
// *before* its own timeout began, so each queued writer started a fresh
// budget on reaching the front. With the defaults that is 32 sessions × 8
// in-flight requests × --test-timeout of surface unavailability from one
// burst, refillable indefinitely — bounds that each hold individually and
// compose into no bound at all, which is the shape CLAUDE.md names when it
// says bounding one resource does not bound another the peer controls the
// ratio to. Reported by Codex on picatz/flowstate#807.
//
// A weighted semaphore takes a context, so waiting for the lock is inside
// the same budget as holding it: a call's whole cost is one --test-timeout,
// whenever it arrives, and a caller that piles more on gets refusals rather
// than a longer queue. Readers take one unit and the writer takes all of
// them, which is an RWMutex with a deadline and nothing more.
type mcpServeRegistryGuard struct {
	sem *semaphore.Weighted
}

// mcpServeRegistryReaders is the semaphore's weight: how many tools and
// resources may read the task registry at once. Large enough not to be a
// bound in its own right — the real request bounds are --max-sessions and
// --max-session-requests — and it is only ever all-or-one, so its exact value
// decides nothing except that a writer excludes every reader.
const mcpServeRegistryReaders = 1 << 20

// newMCPServeRegistryGuard builds one.
func newMCPServeRegistryGuard() *mcpServeRegistryGuard {
	return &mcpServeRegistryGuard{sem: semaphore.NewWeighted(mcpServeRegistryReaders)}
}

// registryMutatingTools names every tool that reaches [flowtest]'s source
// door, which registers a synthetic definition into [v1.DefaultRegistry] for
// any task a case stubs that this build does not have.
//
// A set rather than a name, because there are two now and the second one
// arrived by being forgotten: flowstate_debug drives the identical run
// through the identical door, and it was wrapped as a *reader* while its own
// registration comment claimed it took this guard — so a debug session's
// synthetic task names were visible to a concurrent validate, compile, or
// catalog read (Codex, #1109). Anything routed through
// [flowtest.RunSourceWith] belongs here; a tool that only reads the registry
// does not.
var registryMutatingTools = map[string]bool{
	flowmcp.TestToolName:  true,
	flowmcp.DebugToolName: true,
}

// wrapTool is [flowmcp.Deps.WrapHandler]: exclusive for the tools that mutate
// the registry, shared for every tool that reads it.
//
// timeout is the whole budget an exclusive call gets, waiting for the lock
// included — see this type's doc.
func (g *mcpServeRegistryGuard) wrapTool(timeout time.Duration) func(string, mcp.ToolHandler) mcp.ToolHandler {
	return func(tool string, next mcp.ToolHandler) mcp.ToolHandler {
		if registryMutatingTools[tool] {
			return g.exclusive(next, timeout)
		}

		return g.shared(next)
	}
}

// wrapResource is [flowmcp.Deps.WrapResourceHandler]: the read side, for the
// half of the surface a guard applied only to tools would miss.
//
// flowstate://catalog/tasks answers from [v1.DefaultRegistry] exactly as
// flowstate_get_catalog does, so without this a caller could read one
// caller's synthetic task names through a resources/read while the tool form
// of the same answer was properly excluded — the same disclosure, one request
// away. Reported by Codex on picatz/flowstate#807.
//
// Applied to every resource rather than only that one: the others read
// embedded bytes and taking a read lock costs them nothing, and a guard that
// has to be remembered for each new resource is a guard that will be
// forgotten for one.
func (g *mcpServeRegistryGuard) wrapResource(_ string, next mcp.ResourceHandler) mcp.ResourceHandler {
	return func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		if err := g.sem.Acquire(ctx, 1); err != nil {
			return nil, fmt.Errorf("this surface was busy running another caller's tests and this "+
				"read was cancelled before it could be served: %w", err)
		}
		defer g.sem.Release(1)

		return next(ctx, req)
	}
}

// exclusive runs the registry-mutating tool alone, and repairs what it left.
//
// The deadline starts here, before the lock is waited for, so a call's whole
// cost — queueing plus running — is one timeout rather than one per call that
// reaches the front of the queue.
func (g *mcpServeRegistryGuard) exclusive(next mcp.ToolHandler, timeout time.Duration) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		if err := g.sem.Acquire(ctx, mcpServeRegistryReaders); err != nil {
			return flowmcp.ToolError(fmt.Errorf(
				"this surface was busy running another caller's tests for the whole %s this call is "+
					"allowed, so yours never started; try again", timeout)), nil
		}
		defer g.sem.Release(mcpServeRegistryReaders)

		registry := v1.DefaultRegistry()
		before := make(map[string]bool)
		for _, name := range registry.Names() {
			before[name] = true
		}

		// Deferred rather than run after the call returns, so a handler that
		// panics still leaves the registry as it found it: a panic mid-swap
		// is exactly when a leaked name would be least noticed.
		defer func() {
			for _, name := range registry.Names() {
				if !before[name] {
					registry.Unregister(name)
				}
			}
		}()

		return next(ctx, req)
	}
}

// shared runs a tool that only reads the registry, excluded from the window
// in which the tool above has it swapped.
func (g *mcpServeRegistryGuard) shared(next mcp.ToolHandler) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if err := g.sem.Acquire(ctx, 1); err != nil {
			return flowmcp.ToolError(errors.New(
				"this surface was busy running another caller's tests and this request was " +
					"cancelled before it could be served; try again")), nil
		}
		defer g.sem.Release(1)

		return next(ctx, req)
	}
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
	warnUnreachableIssuers(logger, policy)

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

	tools, err := mcpServeTools(newMCPServeRegistryGuard(), flags.testTimeout)
	if err != nil {
		return err
	}

	handler, err := mcpServeHandler(logger, tools, verifier, protectedResource, mcpServeLimits{
		maxRequestBytes:    flags.maxRequestBytes,
		maxSessions:        flags.maxSessions,
		maxSessionRequests: flags.maxSessionRequests,
		sessionIdle:        mcpServeSessionIdleTimeout,
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

		// Bounds how long a peer may take to deliver a request *body*, which
		// --max-request-bytes bounds the size of and not the pace of: a
		// megabyte delivered one byte a minute is within every byte bound
		// this surface has and holds a connection for a year. It does not cut
		// a server-sent-event response, which is a write; Go sets this
		// deadline when it begins reading a request and resets it for the
		// next one on the same connection.
		ReadTimeout:    1 * time.Minute,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 1 << 20,

		// The same header-count bound `flow server` sets; see
		// [maxHeaderValueCount] for why the byte bound above does not imply it.
		MaxHeaderValueCount: maxHeaderValueCount,
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
		"max_sessions", flags.maxSessions,
		"max_session_requests", flags.maxSessionRequests)
	logMCPServeSessionTopology(logger)

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
