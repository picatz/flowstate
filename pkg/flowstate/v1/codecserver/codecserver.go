// Package codecserver is Flowstate's remote payload codec: the HTTP endpoint
// Temporal's Web UI and CLI call to turn a namespace's encrypted history back
// into something a person can read, and to encrypt what a person types.
//
// # What it is, and what it is not
//
// Workers and clients encrypt and decrypt in process, with the keyring they
// were started with (see payloadcodec/envelope). Nothing on that path calls
// this server. This server exists for the humans and tools that inspect
// history from outside a Flowstate process, and it is therefore the one place
// in a deployment that releases plaintext over the network on request. It is
// built as that: deny by default, and every decision about an authenticated
// caller audited. A request the trust policy's verifier refuses never reaches
// this handler and is logged rather than audited, as it is for `flow server`:
// an unauthenticated caller must not be able to write the audit trail at will.
//
// # The protocol
//
// Temporal's remote codec protocol, as its SDK implements both ends
// (go.temporal.io/sdk@v1.48.0 converter/codec.go, NewPayloadCodecHTTPHandler
// and NewRemotePayloadCodec): a POST to a path ending "/encode" or "/decode",
// whose body is a JSON `temporal.api.common.v1.Payloads`, answered with the
// same shape and the same number of payloads. The Temporal UI and CLI name the
// namespace the payloads came from in an `X-Namespace` header and can forward
// the user's bearer token.
//
// # Who may decode what
//
// The header is a claim by the caller, not evidence, and the protocol carries
// nothing else about where a payload came from: no workflow, no run. So the
// finest scope this server can enforce is a Temporal namespace, and it
// enforces that one strictly:
//
//   - The caller is authenticated by the deployment's trust policy, the same
//     verifier `flow server` uses, before this handler runs.
//   - Their policy entry must list the `payload.decode` (or `payload.encode`)
//     action explicitly. Unlike the RPC actions, an entry that lists no
//     actions is not granted it: this surface releases every payload of a
//     namespace and has no legacy callers to preserve.
//   - The namespace named by the header must be the Temporal namespace the
//     caller's own tenant maps to, under the trust policy's tenancy. A caller
//     cannot choose another one.
//   - That namespace must be the caller's tenant's alone. When several tenants
//     share a Temporal namespace, a decode cannot tell whose payload it was
//     given, so it is refused unless the operator started the server with
//     [Options.AllowSharedNamespaces], accepting that any authorized tenant on
//     the namespace can read all of them.
//
// The keys are then the namespace's own, from the keyring, with the namespace
// authenticated into each payload: a payload from another namespace, submitted
// under a header the caller is authorized for, fails to decode.
//
// # What it never does
//
// It never echoes a payload, a key, or a token in an error, never tells the
// caller which of wrong key, wrong namespace, or tampering made a payload
// undecodable, and never lets a response be cached. It bounds the request body,
// the number of payloads, and each caller's request rate before any key is
// touched.
package codecserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
)

// Endpoint suffixes, fixed by Temporal's remote codec protocol and bound to
// authorization actions in flowstate.v1's bindings.
const (
	DecodeEndpoint = "/decode"
	EncodeEndpoint = "/encode"
)

// NamespaceHeader is the header Temporal's UI and CLI name a request's
// namespace in.
const NamespaceHeader = "X-Namespace"

// Defaults for [Options]. A decode request is a page of history's payloads,
// each at most Temporal's blob limit; four mebibytes and 256 payloads covers
// what the UI sends for one view with room to spare, and refuses a request
// that is trying to use the server as a bulk decryption service.
const (
	DefaultMaxBodyBytes      = 4 << 20
	DefaultMaxPayloads       = 256
	DefaultRequestsPerMinute = 600
	DefaultWorkTimeout       = 20 * time.Second
	DefaultMaxConcurrent     = 16
	defaultMaxTrackedCallers = 10_000
	// bodyReadTimeout bounds how long a request that holds an in-flight slot
	// may take to send its body: four mebibytes in ten seconds is a slow
	// link, and anything slower is holding a slot other tenants need.
	bodyReadTimeout            = 10 * time.Second
	rateWindow                 = time.Minute
	maxNamespaceHeaderBytes    = 255
	decodeFailureUserFacingMsg = "one or more payloads could not be decoded in this namespace"
)

// logSafe strips line breaks from a caller-supplied value before it is
// logged, so a header cannot forge a log line. net/http already refuses them
// in header values; this does not rely on that.
var logSafe = strings.NewReplacer("\n", "", "\r", "")

// Auditor records decisions. [*audit.Recorder] implements it. An Auditor
// that also has a Required() bool method says whether its trail may have gaps;
// one without it is treated as required.
type Auditor interface {
	Allow(ctx context.Context, subject audit.Subject) error
	Deny(ctx context.Context, subject audit.Subject, code v1.AuditDenyCode) error
}

// Options configures a [Handler].
type Options struct {
	// Codecs is the deployment's payload codec configuration, with a codec per
	// Temporal namespace (payloadcodec.Config.Namespaces). Required.
	Codecs payloadcodec.Config

	// Tenancy maps each Flowstate tenant to its Temporal namespace, from the
	// trust policy. Nil means every tenant shares the one namespace this
	// deployment dials, DefaultNamespace, which is shared by construction.
	Tenancy *auth.Tenancy

	// DefaultNamespace is the Temporal namespace an unmapped deployment uses.
	DefaultNamespace string

	// AllowSharedNamespaces permits decoding and encoding in a Temporal
	// namespace more than one tenant maps to. See the package doc.
	AllowSharedNamespaces bool

	// Insecure serves any caller, with no authentication or authorization, for
	// a loopback development server. The command that builds a handler with it
	// must refuse any other listener.
	Insecure bool

	// AllowedOrigins are the browser origins permitted to call this server,
	// compared exactly, such as "https://temporal.example.com". A request
	// carrying any other Origin is refused. Empty permits no browser at all.
	AllowedOrigins []string

	// Auditor records every decision. Nil records nothing.
	Auditor Auditor

	// Logger receives operational messages, never payloads. Nil uses
	// slog.Default.
	Logger *slog.Logger

	MaxBodyBytes      int64
	MaxPayloads       int
	RequestsPerMinute int

	// MaxConcurrent bounds the requests read and decoded at once, across
	// every caller: past it a request is refused with 503 before its body is
	// read. The per-caller rate bounds how many a caller sends in a minute,
	// not how many are in memory together, and callers are many; this is
	// what makes MaxConcurrent × MaxBodyBytes the most body this server
	// holds. One caller holds at most a quarter of them (at least one), so
	// one tenant cannot fill them. Zero is DefaultMaxConcurrent.
	MaxConcurrent int

	// WorkTimeout bounds the codec work one request may start: payloads are
	// processed one at a time, and none is started once it has passed or the
	// caller has gone. Zero is DefaultWorkTimeout. A server's response
	// deadline must allow it plus one payload's provider call, so the refusal
	// can still be written.
	WorkTimeout time.Duration

	// now is the clock, for tests.
	now func() time.Time
}

// Handler serves the remote codec protocol.
type Handler struct {
	opts    Options
	limiter *limiter
	// inFlight holds one token per request being read or decoded, and held
	// counts them per caller.
	inFlight chan struct{}
	held     callerSlots

	// refusals holds one token per refusal being recorded. A refusal is
	// answered before a request is admitted (rate limit, authorization), so
	// the in-flight bound does not reach it, and each one is an audit record
	// the sink writes synchronously: without this, one caller over its limit
	// starts as many exports as it sends requests (Codex, #2167).
	refusals chan struct{}
	// unrecorded counts refusals answered without a record because refusals
	// was full, for a best-effort trail; loggedAt is when that was last said.
	unrecorded atomic.Uint64
	loggedAt   atomic.Int64
}

// callerSlots counts the in-flight slots each caller holds.
type callerSlots struct {
	mu sync.Mutex
	n  map[string]int
}

// take reserves one of caller's slots if it holds fewer than limit.
func (c *callerSlots) take(caller string, limit int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n[caller] >= limit {
		return false
	}
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[caller]++
	return true
}

// give returns one of caller's slots.
func (c *callerSlots) give(caller string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n[caller]--; c.n[caller] <= 0 {
		delete(c.n, caller)
	}
}

// New returns a handler, or an error naming what is wrong with opts.
func New(opts Options) (*Handler, error) {
	if len(opts.Codecs.Namespaces) == 0 {
		return nil, errors.New("codec server: no payload keyring is configured, so there is nothing to decode " +
			"with; set --payload-keyring")
	}
	if err := opts.Codecs.Validate(); err != nil {
		return nil, fmt.Errorf("codec server: %w", err)
	}
	if opts.Tenancy != nil {
		if err := opts.Tenancy.Validate(); err != nil {
			return nil, fmt.Errorf("codec server: %w", err)
		}
	}
	for _, origin := range opts.AllowedOrigins {
		if !isSerializedOrigin(origin) {
			return nil, fmt.Errorf("codec server: allowed origin %q must be an exact lowercase http(s)://host[:port] "+
				"as a browser sends it: no path, trailing slash, query, fragment, or userinfo, never a wildcard, "+
				"and never the opaque origin null", origin)
		}
	}
	if opts.MaxBodyBytes < 0 || opts.MaxPayloads < 0 || opts.RequestsPerMinute < 0 ||
		opts.WorkTimeout < 0 || opts.MaxConcurrent < 0 {
		return nil, errors.New("codec server: a limit is negative; each is a positive bound, or zero for its default")
	}
	opts.MaxBodyBytes = cmp.Or(opts.MaxBodyBytes, DefaultMaxBodyBytes)
	opts.MaxPayloads = cmp.Or(opts.MaxPayloads, DefaultMaxPayloads)
	opts.RequestsPerMinute = cmp.Or(opts.RequestsPerMinute, DefaultRequestsPerMinute)
	opts.WorkTimeout = cmp.Or(opts.WorkTimeout, DefaultWorkTimeout)
	opts.MaxConcurrent = cmp.Or(opts.MaxConcurrent, DefaultMaxConcurrent)
	opts.Logger = cmp.Or(opts.Logger, slog.Default())
	if opts.now == nil {
		opts.now = time.Now
	}

	return &Handler{
		opts:     opts,
		limiter:  &limiter{limit: opts.RequestsPerMinute, windows: map[string]*window{}, now: opts.now},
		inFlight: make(chan struct{}, opts.MaxConcurrent),
		refusals: make(chan struct{}, opts.MaxConcurrent),
	}, nil
}

// isSerializedOrigin reports whether origin is a tuple origin exactly as a
// browser serializes it in an Origin header (RFC 6454 section 6.1): an http
// or https scheme, a lowercase host and optional port, and nothing else. The
// opaque origin "null" is refused: browsers send it for sandboxed frames and
// local documents alike, so granting it grants all of them.
func isSerializedOrigin(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return false
	}
	return origin == u.Scheme+"://"+strings.ToLower(u.Host)
}

// Headers sets the response headers every answer carries (no-store, and the
// exact-origin CORS grant) and reports whether the request may proceed; when
// it may not, the refusal is already written.
//
// ServeHTTP calls it first. A server that authenticates in front of this
// handler calls it too, before authenticating, so a browser at an allowed
// origin can read an authentication refusal instead of seeing it as an opaque
// CORS failure.
func (h *Handler) Headers(w http.ResponseWriter, r *http.Request) bool {
	header := w.Header()
	// Nothing this server answers may be kept by anything between it and the
	// caller: a decoded payload is plaintext.
	header.Set("Cache-Control", "no-store")
	header.Set("Pragma", "no-cache")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Vary", "Origin")

	if origin := r.Header.Get("Origin"); origin != "" {
		if !slices.Contains(h.opts.AllowedOrigins, origin) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return false
		}
		header.Set("Access-Control-Allow-Origin", origin)
		header.Set("Access-Control-Allow-Credentials", "true")
	}
	return true
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	if !h.Headers(w, r) {
		return
	}

	if r.Method == http.MethodOptions {
		header.Set("Access-Control-Allow-Methods", "POST")
		header.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, "+NamespaceHeader)
		header.Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		header.Set("Allow", "POST, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var endpoint string
	switch {
	case strings.HasSuffix(r.URL.Path, DecodeEndpoint):
		endpoint = DecodeEndpoint
	case strings.HasSuffix(r.URL.Path, EncodeEndpoint):
		endpoint = EncodeEndpoint
	default:
		http.NotFound(w, r)
		return
	}

	namespace := r.Header.Get(NamespaceHeader)
	if namespace == "" || len(namespace) > maxNamespaceHeaderBytes {
		http.Error(w, "the "+NamespaceHeader+" header must name the Temporal namespace the payloads belong to",
			http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	principal, _ := auth.PrincipalFromContext(ctx)
	subject := audit.Subject{
		HTTPEndpoint: endpoint,
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_NAMESPACE,
		ResourceKey:  namespace,
		IssuerName:   principal.IssuerName,
		Role:         principal.Role,
	}
	if !principal.IsZero() {
		// The same coordinates an RPC decision records, from the same
		// derivation, so one caller's codec and RPC records correlate.
		derived := auth.IdentityFromPrincipal(principal, "", "")
		subject.Identity = &v1.WorkloadIdentity{
			Subject:   derived.Subject,
			Issuer:    derived.Issuer,
			Namespace: derived.Namespace,
		}
	}

	if status, msg, code := h.authorize(principal, endpoint, namespace); status != 0 {
		// Only a missing action is a scope problem a new token can fix; a
		// namespace or shared-namespace refusal is not, and a client told
		// otherwise would keep asking for a scope it already holds.
		if scope := v1.AuthorizationActionScope(mustAction(endpoint)); status == http.StatusForbidden &&
			!slices.Contains(principal.Actions, scope) {
			header.Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope=%q`, scope))
		}
		h.refuse(ctx, w, subject, code, status, msg)
		return
	}

	if !h.limiter.allow(cmp.Or(principal.ID(), "anonymous")) {
		header.Set("Retry-After", "60")
		h.refuse(ctx, w, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_RATE_LIMITED, http.StatusTooManyRequests, "too many requests")
		return
	}

	// Taken before the body is read, and without waiting: a server at its
	// bound refuses at once rather than queueing bodies it would then hold.
	// A caller holds at most a share of the slots, so one tenant cannot fill
	// them all, and a body must arrive promptly once a slot is held, so a
	// slow one gives its slot back rather than keeping it until the server's
	// read timeout.
	caller := cmp.Or(principal.ID(), "anonymous")
	if !h.held.take(caller, max(1, h.opts.MaxConcurrent/4)) {
		header.Set("Retry-After", "1")
		h.refuse(ctx, w, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_RATE_LIMITED, http.StatusServiceUnavailable, "server busy")
		return
	}
	defer h.held.give(caller)
	select {
	case h.inFlight <- struct{}{}:
		defer func() { <-h.inFlight }()
	default:
		header.Set("Retry-After", "1")
		h.refuse(ctx, w, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_RATE_LIMITED, http.StatusServiceUnavailable, "server busy")
		return
	}
	// Not every ResponseWriter supports a deadline (an in-process recorder
	// does not); the server's own read timeout still bounds those.
	_ = http.NewResponseController(w).SetReadDeadline(h.opts.now().Add(bodyReadTimeout))

	payloads, tooLarge, err := h.readPayloads(w, r)
	if err != nil {
		// Only a size refusal is a decision worth a record: a body that does
		// not parse is a malformed request from a caller already authorized,
		// and the log says so without the body.
		if tooLarge {
			h.refuse(ctx, w, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_PAYLOAD_TOO_LARGE, http.StatusBadRequest, err.Error())
			return
		}
		h.opts.Logger.WarnContext(ctx, "codec server: malformed request body",
			"endpoint", endpoint, "caller", principal.ID())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	codec, err := h.opts.Codecs.ForNamespace(namespace)
	if err != nil {
		// Authorized, and the keyring does not cover it: an operator gap, not
		// the caller's. Said as not found, without naming the keyring.
		h.refuse(ctx, w, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED, http.StatusNotFound,
			"this server holds no keys for that namespace")
		return
	}

	out, err := h.process(ctx, codec.Codec, endpoint, payloads.GetPayloads())
	if errors.Is(err, errWorkSpent) {
		// Authorized, and stopped for time rather than refused: the data keys
		// already unwrapped are cached, so the retry goes further.
		if err := h.allow(ctx, subject); err != nil {
			http.Error(w, auditUnavailableMsg, http.StatusServiceUnavailable)
			return
		}
		header.Set("Retry-After", "1")
		http.Error(w, "this request needs more key provider calls than one response allows; retry", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		// Recorded as an allowed request that failed, since authorization
		// succeeded; the reason class stays in the server's log, not the
		// response, so the endpoint is not an oracle for which edit got
		// further.
		h.opts.Logger.WarnContext(ctx, "codec server: a payload could not be processed",
			"endpoint", endpoint, "namespace", logSafe.Replace(namespace), "caller", principal.ID(), "class", errorClass(err))
		if err := h.allow(ctx, subject); err != nil {
			http.Error(w, auditUnavailableMsg, http.StatusServiceUnavailable)
			return
		}
		// A key provider that did not answer is an outage, not a bad
		// request: the same request may succeed once it is back.
		if errors.Is(err, envelope.ErrProviderUnavailable) {
			header.Set("Retry-After", "5")
			http.Error(w, "the key provider is unavailable; retry shortly", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, decodeFailureUserFacingMsg, http.StatusBadRequest)
		return
	}

	if err := h.allow(ctx, subject); err != nil {
		// A required recorder that could not record: nothing is released.
		http.Error(w, auditUnavailableMsg, http.StatusServiceUnavailable)
		return
	}

	body, err := protojson.Marshal(&commonpb.Payloads{Payloads: out})
	if err != nil {
		http.Error(w, "encoding the response failed", http.StatusInternalServerError)
		return
	}
	header.Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// errWorkSpent is process stopping at the request's work budget.
var errWorkSpent = errors.New("codec server: the request's work budget is spent")

// process runs the codec over payloads one at a time, so the work one request
// starts is bounded: none is started once the budget has passed or the caller
// has gone. Each payload's own provider call is bounded by the codec.
func (h *Handler) process(ctx context.Context, codec payloadcodec.Codec, endpoint string, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	run := codec.Encode
	if endpoint == DecodeEndpoint {
		run = codec.Decode
	}
	deadline := h.opts.now().Add(h.opts.WorkTimeout)
	out := make([]*commonpb.Payload, 0, len(payloads))
	for _, p := range payloads {
		if ctx.Err() != nil || !h.opts.now().Before(deadline) {
			return nil, errWorkSpent
		}
		one, err := run([]*commonpb.Payload{p})
		if err != nil {
			return nil, err
		}
		out = append(out, one...)
	}
	return out, nil
}

// authorize answers 0 to proceed, or the status, message, and audit code of a
// refusal. The messages say what is missing and never whether a namespace
// exists.
func (h *Handler) authorize(principal auth.Principal, endpoint, namespace string) (int, string, v1.AuditDenyCode) {
	if h.opts.Insecure {
		return 0, "", 0
	}
	if principal.IsZero() {
		return http.StatusUnauthorized, "authentication required", v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED
	}

	scope := v1.AuthorizationActionScope(mustAction(endpoint))
	if !slices.Contains(principal.Actions, scope) {
		return http.StatusForbidden,
			fmt.Sprintf("the caller's policy entry does not grant %q, which this endpoint requires explicitly", scope),
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED
	}

	own, shared, err := h.temporalNamespaceOf(principal.Namespace)
	if err != nil || own != namespace {
		return http.StatusForbidden, "the caller is not authorized for that namespace",
			v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH
	}
	if shared && !h.opts.AllowSharedNamespaces {
		return http.StatusForbidden,
			"that Temporal namespace is shared with other tenants, and a payload does not say whose it is; " +
				"this server refuses shared namespaces unless started with --allow-shared-namespaces",
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED
	}
	return 0, "", 0
}

// temporalNamespaceOf is the Temporal namespace a tenant's runs execute in,
// and whether any other tenant's runs can land there too.
func (h *Handler) temporalNamespaceOf(tenant string) (string, bool, error) {
	if h.opts.Tenancy == nil {
		return h.opts.DefaultNamespace, true, nil
	}
	ns, mapped, err := h.opts.Tenancy.TemporalNamespace(tenant)
	if err != nil {
		return "", false, err
	}
	if !mapped {
		return h.opts.DefaultNamespace, true, nil
	}
	others := slices.DeleteFunc(h.opts.Tenancy.FlowstateNamespaces(ns), func(t string) bool { return t == tenant })
	shared := len(others) > 0 || h.opts.Tenancy.Default == ns
	return ns, shared, nil
}

// readPayloads reads and parses the body, reporting whether a refusal was
// for size (the body or the payload count) rather than for shape.
func (h *Handler) readPayloads(w http.ResponseWriter, r *http.Request) (*commonpb.Payloads, bool, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.opts.MaxBodyBytes))
	if tooBig := (*http.MaxBytesError)(nil); errors.As(err, &tooBig) {
		return nil, true, fmt.Errorf("the request body is over the %d byte limit", h.opts.MaxBodyBytes)
	}
	if err != nil {
		// A read that failed for another reason (a caller that went away) is
		// not a size decision, and is not recorded as one.
		return nil, false, errors.New("the request body could not be read")
	}
	var payloads commonpb.Payloads
	if err := protojson.Unmarshal(body, &payloads); err != nil {
		return nil, false, errors.New("the request body is not a JSON Payloads document")
	}
	if n := len(payloads.GetPayloads()); n > h.opts.MaxPayloads {
		return nil, true, fmt.Errorf("the request holds %d payloads, over the %d limit", n, h.opts.MaxPayloads)
	}
	for _, p := range payloads.GetPayloads() {
		if p == nil {
			return nil, false, errors.New("the request holds an empty payload")
		}
	}
	return &payloads, false, nil
}

func (h *Handler) allow(ctx context.Context, subject audit.Subject) error {
	if h.opts.Auditor == nil {
		return nil
	}
	return h.opts.Auditor.Allow(ctx, subject)
}

func (h *Handler) deny(ctx context.Context, subject audit.Subject, code v1.AuditDenyCode) error {
	if h.opts.Auditor == nil {
		return nil
	}
	return h.opts.Auditor.Deny(ctx, subject, code)
}

// auditUnavailableMsg is the answer when a required audit recorder could not
// record a decision: whatever the decision was, it is not acted on.
const auditUnavailableMsg = "the decision could not be recorded"

// refuse records a refusal and answers it. When the recorder is required and
// could not record, the answer is 503 instead of the refusal, so a gap in a
// required trail is an outage the operator sees rather than a quiet 4xx.
//
// Recording is bounded like the work it refuses: at most MaxConcurrent
// refusals are recorded at once. Past that, a required trail answers 503, the
// same outage an unrecordable decision is, and a best-effort one answers the
// refusal unrecorded and says, at most once a second, how many it could not
// record.
func (h *Handler) refuse(ctx context.Context, w http.ResponseWriter, subject audit.Subject, code v1.AuditDenyCode, status int, msg string) {
	if h.opts.Auditor != nil {
		select {
		case h.refusals <- struct{}{}:
			defer func() { <-h.refusals }()
		default:
			if required, ok := h.opts.Auditor.(interface{ Required() bool }); !ok || required.Required() {
				w.Header().Set("Retry-After", "1")
				http.Error(w, auditUnavailableMsg, http.StatusServiceUnavailable)
				return
			}
			h.noteUnrecorded(ctx)
			http.Error(w, msg, status)
			return
		}
	}
	if err := h.deny(ctx, subject, code); err != nil {
		http.Error(w, auditUnavailableMsg, http.StatusServiceUnavailable)
		return
	}
	http.Error(w, msg, status)
}

// noteUnrecorded counts a refusal a best-effort trail could not record, and
// logs the count at most once a second, so a flood is visible without a line
// per request.
func (h *Handler) noteUnrecorded(ctx context.Context) {
	h.unrecorded.Add(1)
	now := h.opts.now().UnixNano()
	last := h.loggedAt.Load()
	if now-last < int64(time.Second) || !h.loggedAt.CompareAndSwap(last, now) {
		return
	}
	h.opts.Logger.WarnContext(ctx, "codec server: refusals answered without an audit record: too many being recorded at once",
		"count", h.unrecorded.Swap(0))
}

func mustAction(endpoint string) v1.AuthorizationAction {
	action, err := v1.AuthorizationActionForHTTPEndpoint(endpoint)
	if err != nil {
		// Unreachable: ServeHTTP only produces the two bound endpoints, and a
		// test holds the bindings to them.
		panic(err)
	}
	return action
}

// errorClass names a codec error's kind for the server's own log, without its
// text, which may quote a key id.
func errorClass(err error) string {
	for _, c := range []struct {
		name string
		err  error
	}{
		{"unencrypted", envelope.ErrUnencrypted},
		{"unknown-version", envelope.ErrUnknownVersion},
		{"unknown-key", envelope.ErrUnknownKey},
		{"malformed", envelope.ErrMalformed},
		{"authentication", envelope.ErrAuthentication},
		{"suite-refused", envelope.ErrSuiteRefused},
		{"key-denied", envelope.ErrKeyDenied},
		{"provider-unavailable", envelope.ErrProviderUnavailable},
		{"reader-cannot-encode", envelope.ErrReaderCannotEncode},
	} {
		if errors.Is(err, c.err) {
			return c.name
		}
	}
	return "other"
}

// limiter is a fixed-window request counter per caller, bounded in the number
// of callers it tracks: when full, it starts over rather than growing, which
// at worst forgives some callers one window early.
type limiter struct {
	mu      sync.Mutex
	limit   int
	windows map[string]*window
	now     func() time.Time
}

type window struct {
	start time.Time
	count int
}

func (l *limiter) allow(caller string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	w, ok := l.windows[caller]
	if !ok || now.Sub(w.start) >= rateWindow {
		if !ok && len(l.windows) >= defaultMaxTrackedCallers {
			clear(l.windows)
		}
		w = &window{start: now}
		l.windows[caller] = w
	}
	if w.count >= l.limit {
		return false
	}
	w.count++
	return true
}
