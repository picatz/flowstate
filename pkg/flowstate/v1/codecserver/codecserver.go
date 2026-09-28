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
// built as that: deny by default, and every decision audited.
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
	"slices"
	"strings"
	"sync"
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
	DefaultMaxBodyBytes        = 4 << 20
	DefaultMaxPayloads         = 256
	DefaultRequestsPerMinute   = 600
	defaultMaxTrackedCallers   = 10_000
	rateWindow                 = time.Minute
	maxNamespaceHeaderBytes    = 255
	decodeFailureUserFacingMsg = "one or more payloads could not be decoded in this namespace"
)

// Auditor records decisions. [*audit.Recorder] implements it.
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

	// now is the clock, for tests.
	now func() time.Time
}

// Handler serves the remote codec protocol.
type Handler struct {
	opts    Options
	limiter *limiter
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
		if origin == "*" || origin == "" || strings.TrimRight(origin, "/") != origin {
			return nil, fmt.Errorf("codec server: allowed origin %q must be an exact scheme://host[:port] with no "+
				"trailing slash, and never a wildcard", origin)
		}
	}
	opts.MaxBodyBytes = cmp.Or(opts.MaxBodyBytes, DefaultMaxBodyBytes)
	opts.MaxPayloads = cmp.Or(opts.MaxPayloads, DefaultMaxPayloads)
	opts.RequestsPerMinute = cmp.Or(opts.RequestsPerMinute, DefaultRequestsPerMinute)
	opts.Logger = cmp.Or(opts.Logger, slog.Default())
	if opts.now == nil {
		opts.now = time.Now
	}

	return &Handler{
		opts:    opts,
		limiter: &limiter{limit: opts.RequestsPerMinute, windows: map[string]*window{}, now: opts.now},
	}, nil
}

// ServeHTTP implements [http.Handler].
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	// Nothing this server answers may be kept by anything between it and the
	// caller: a decoded payload is plaintext.
	header.Set("Cache-Control", "no-store")
	header.Set("Pragma", "no-cache")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Add("Vary", "Origin")

	if origin := r.Header.Get("Origin"); origin != "" {
		if !slices.Contains(h.opts.AllowedOrigins, origin) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		header.Set("Access-Control-Allow-Origin", origin)
		header.Set("Access-Control-Allow-Credentials", "true")
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
		ResourceKey:  principal.Namespace,
		IssuerName:   principal.IssuerName,
		Role:         principal.Role,
	}
	if !principal.IsZero() {
		subject.Identity = &v1.WorkloadIdentity{Subject: principal.ID(), Namespace: principal.Namespace}
	}

	if status, msg, code := h.authorize(principal, endpoint, namespace); status != 0 {
		h.deny(ctx, subject, code)
		if status == http.StatusForbidden && principal.Actions != nil {
			header.Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope=%q`,
				v1.AuthorizationActionScope(mustAction(endpoint))))
		}
		http.Error(w, msg, status)
		return
	}

	if !h.limiter.allow(cmp.Or(principal.ID(), "anonymous")) {
		h.deny(ctx, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_RATE_LIMITED)
		header.Set("Retry-After", "60")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}

	payloads, err := h.readPayloads(w, r)
	if err != nil {
		h.deny(ctx, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_PAYLOAD_TOO_LARGE)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	codec, err := h.opts.Codecs.ForNamespace(namespace)
	if err != nil {
		// Authorized, and the keyring does not cover it: an operator gap, not
		// the caller's. Said as not found, without naming the keyring.
		h.deny(ctx, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED)
		http.Error(w, "this server holds no keys for that namespace", http.StatusNotFound)
		return
	}

	var out []*commonpb.Payload
	if endpoint == DecodeEndpoint {
		out, err = codec.Codec.Decode(payloads.GetPayloads())
	} else {
		out, err = codec.Codec.Encode(payloads.GetPayloads())
	}
	if err != nil {
		// Recorded as an allowed request that failed, since authorization
		// succeeded; the reason class stays in the server's log, not the
		// response, so the endpoint is not an oracle for which edit got
		// further.
		h.opts.Logger.WarnContext(ctx, "codec server: a payload could not be processed",
			"endpoint", endpoint, "namespace", namespace, "caller", principal.ID(), "class", errorClass(err))
		_ = h.allow(ctx, subject)
		http.Error(w, decodeFailureUserFacingMsg, http.StatusBadRequest)
		return
	}

	if err := h.allow(ctx, subject); err != nil {
		// A required recorder that could not record: nothing is released.
		http.Error(w, "the decision could not be recorded", http.StatusServiceUnavailable)
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

func (h *Handler) readPayloads(w http.ResponseWriter, r *http.Request) (*commonpb.Payloads, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.opts.MaxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("the request body is over the %d byte limit or could not be read", h.opts.MaxBodyBytes)
	}
	var payloads commonpb.Payloads
	if err := protojson.Unmarshal(body, &payloads); err != nil {
		return nil, errors.New("the request body is not a JSON Payloads document")
	}
	if n := len(payloads.GetPayloads()); n > h.opts.MaxPayloads {
		return nil, fmt.Errorf("the request holds %d payloads, over the %d limit", n, h.opts.MaxPayloads)
	}
	for _, p := range payloads.GetPayloads() {
		if p == nil {
			return nil, errors.New("the request holds an empty payload")
		}
	}
	return &payloads, nil
}

func (h *Handler) allow(ctx context.Context, subject audit.Subject) error {
	if h.opts.Auditor == nil {
		return nil
	}
	return h.opts.Auditor.Allow(ctx, subject)
}

func (h *Handler) deny(ctx context.Context, subject audit.Subject, code v1.AuditDenyCode) {
	if h.opts.Auditor != nil {
		_ = h.opts.Auditor.Deny(ctx, subject, code)
	}
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
