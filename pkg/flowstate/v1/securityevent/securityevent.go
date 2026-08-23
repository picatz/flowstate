package securityevent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const (
	DefaultMaxBodyBytes        = 64 << 10
	DefaultMaxClaims           = 64
	DefaultMaxClaimBytes       = 32 << 10
	DefaultMaxBatch            = 32
	DefaultClockSkew           = 30 * time.Second
	DefaultMaxAge              = 10 * time.Minute
	DefaultReplayEntries       = 10000
	DefaultStateEntries        = 100000
	DefaultDeliveriesPerMinute = 600
)

var (
	ErrRefused     = errors.New("security event refused")
	ErrDuplicate   = errors.New("duplicate security event")
	ErrUnavailable = errors.New("security state unavailable")
)

// Claims is the bounded decoded payload of a cryptographically verified SET.
type Claims map[string]any

// Verifier must verify signature and algorithm against issuer-owned keys and
// require the configured issuer and audience before returning claims.
type Verifier interface {
	VerifySET(context.Context, string) (Claims, error)
}

// Adapter normalizes an event profile. Implementations must reject unknown
// event URIs and malformed subjects rather than guessing.
type Adapter interface {
	Normalize(Claims) ([]*v1.SecurityEvent, error)
}

// Observer receives privacy-safe lifecycle facts. Detail must be a bounded
// reason code, never a token, subject, issuer, audience, or event identifier.
type Observer interface {
	Observe(context.Context, Observation)
}
type Observation struct{ Stage, EventType, Detail string }

// Store is the cross-node consistency boundary. Apply must be linearizable and
// durable before returning. Snapshot may be stale only when strong is false.
type Store interface {
	Apply(context.Context, Entry) error
	Lookup(context.Context, Key, bool) (Entry, bool, error)
	Snapshot(context.Context, bool, int) ([]Entry, error)
	Compact(context.Context, time.Time, int) (int, error)
}

type Key struct {
	Issuer         string
	SubjectType    v1.SecuritySubjectType
	Identifier     string
	PolicyRevision uint64
}
type Entry struct {
	Key                 Key
	EventType           v1.SecurityEventType
	Enforcement         v1.EnforcementAction
	IssuedAt, ExpiresAt time.Time
	Emergency           bool
}

// Boundary names every place state must be checked. Callers choose strong=true
// for authentication, refresh, and immediate-enforcement paths.
type Boundary string

const (
	BoundaryAuthentication    Boundary = "authentication"
	BoundaryMCPRequest        Boundary = "mcp_request"
	BoundaryAuthorization     Boundary = "authorization"
	BoundaryCredentialRefresh Boundary = "credential_refresh"
	BoundarySignalDelivery    Boundary = "signal_delivery"
	BoundaryExternalCall      Boundary = "external_call"
)

type Limits struct {
	MaxBodyBytes, MaxClaims, MaxClaimBytes, MaxBatch int
	ClockSkew, MaxAge                                time.Duration
	ReplayEntries, StateEntries, DeliveriesPerMinute int
}

func DefaultLimits() Limits {
	return Limits{DefaultMaxBodyBytes, DefaultMaxClaims, DefaultMaxClaimBytes, DefaultMaxBatch, DefaultClockSkew, DefaultMaxAge, DefaultReplayEntries, DefaultStateEntries, DefaultDeliveriesPerMinute}
}

type Ingestor struct {
	verifier   Verifier
	adapters   []Adapter
	store      Store
	limits     Limits
	observer   Observer
	now        func() time.Time
	mu         sync.Mutex
	replay     map[string]time.Time
	deliveries map[string]rateWindow
}
type rateWindow struct {
	minute int64
	count  int
}

func New(verifier Verifier, adapters []Adapter, store Store, limits Limits, observer Observer) (*Ingestor, error) {
	if verifier == nil || store == nil || len(adapters) == 0 {
		return nil, fmt.Errorf("%w: verifier, adapter, and store are required", ErrRefused)
	}
	if limits == (Limits{}) {
		limits = DefaultLimits()
	}
	if limits.MaxBodyBytes <= 0 || limits.MaxClaims <= 0 || limits.MaxClaimBytes <= 0 || limits.MaxBatch <= 0 || limits.ClockSkew < 0 || limits.ClockSkew > 5*time.Minute || limits.MaxAge <= 0 || limits.ReplayEntries <= 0 || limits.StateEntries <= 0 || limits.DeliveriesPerMinute <= 0 {
		return nil, fmt.Errorf("%w: invalid limits", ErrRefused)
	}
	return &Ingestor{verifier: verifier, adapters: adapters, store: store, limits: limits, observer: observer, now: time.Now, replay: map[string]time.Time{}, deliveries: map[string]rateWindow{}}, nil
}

// Deliver reads exactly one bounded JSON request containing either {"set":...}
// or {"sets":[...]}; batches are atomic with respect to verification: no event
// is applied until every token has verified and normalized successfully.
func (i *Ingestor) Deliver(ctx context.Context, issuerHint string, body io.Reader) ([]*v1.SecurityEvent, error) {
	i.observe(ctx, "receipt", "", "received")
	var envelope struct {
		SET  string   `json:"set"`
		SETs []string `json:"sets"`
	}
	dec := json.NewDecoder(io.LimitReader(body, int64(i.limits.MaxBodyBytes)+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&envelope); err != nil {
		return nil, i.refuse(ctx, "body")
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return nil, i.refuse(ctx, "body")
	}
	sets := envelope.SETs
	if envelope.SET != "" {
		if len(sets) != 0 {
			return nil, i.refuse(ctx, "batch")
		}
		sets = []string{envelope.SET}
	}
	if len(sets) == 0 || len(sets) > i.limits.MaxBatch {
		return nil, i.refuse(ctx, "batch")
	}
	// issuerHint is untrusted routing metadata. It is intentionally not a rate
	// limit key: otherwise a sender evades the bound by inventing issuer names.
	if !i.allowDelivery() {
		return nil, i.refuse(ctx, "rate")
	}
	all := make([]*v1.SecurityEvent, 0, len(sets))
	for _, raw := range sets {
		if len(raw) > i.limits.MaxBodyBytes {
			return nil, i.refuse(ctx, "token_size")
		}
		claims, err := i.verifier.VerifySET(ctx, raw)
		if err != nil {
			return nil, i.refuse(ctx, "verification")
		}
		if err := boundedClaims(claims, i.limits); err != nil {
			return nil, i.refuse(ctx, "claims")
		}
		events, err := i.normalize(claims)
		if err != nil || len(events) == 0 || len(events) > i.limits.MaxBatch-len(all) {
			return nil, i.refuse(ctx, "event_type")
		}
		for _, event := range events {
			if err := i.validate(event, claims); err != nil {
				return nil, i.refuse(ctx, "claims")
			}
			all = append(all, event)
		}
	}
	for _, event := range all {
		if !i.reserveReplay(event.GetIssuer(), event.GetEventId(), event.GetExpiresAt().AsTime()) {
			i.observe(ctx, "duplicate", event.GetType().String(), "replay")
			return nil, ErrDuplicate
		}
		entry := entryFrom(event, false)
		if err := i.store.Apply(ctx, entry); err != nil {
			i.releaseReplay(event.GetIssuer(), event.GetEventId())
			return nil, fmt.Errorf("%w: apply: %v", ErrUnavailable, err)
		}
		i.observe(ctx, "application", event.GetType().String(), "applied")
	}
	return all, nil
}

func (i *Ingestor) normalize(c Claims) ([]*v1.SecurityEvent, error) {
	for _, a := range i.adapters {
		e, err := a.Normalize(c)
		if err == nil {
			return e, nil
		}
	}
	return nil, ErrRefused
}
func (i *Ingestor) validate(e *v1.SecurityEvent, c Claims) error {
	if e == nil || e.Type == v1.SecurityEventType_SECURITY_EVENT_TYPE_UNSPECIFIED || e.Subject == nil || e.Subject.Type == v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_UNSPECIFIED || e.Subject.Identifier == "" || len(e.Subject.Identifier) > 512 || e.Issuer == "" || e.Audience == "" || e.EventId == "" || len(e.EventId) > 256 || e.IssuedAt == nil || e.ExpiresAt == nil || e.Enforcement == v1.EnforcementAction_ENFORCEMENT_ACTION_UNSPECIFIED {
		return ErrRefused
	}
	iss, _ := c["iss"].(string)
	jti, _ := c["jti"].(string)
	if iss != e.Issuer || jti != e.EventId || !audienceContains(c["aud"], e.Audience) {
		return ErrRefused
	}
	now := i.now()
	issued, expires := e.IssuedAt.AsTime(), e.ExpiresAt.AsTime()
	if issued.After(now.Add(i.limits.ClockSkew)) || now.Sub(issued) > i.limits.MaxAge+i.limits.ClockSkew || !expires.After(now.Add(-i.limits.ClockSkew)) || expires.Sub(issued) > i.limits.MaxAge {
		return ErrRefused
	}
	return validPair(e.Type, e.Subject.Type)
}

func audienceContains(v any, want string) bool {
	switch x := v.(type) {
	case string:
		return x == want
	case []any:
		for _, a := range x {
			if s, ok := a.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}
func validPair(t v1.SecurityEventType, s v1.SecuritySubjectType) error {
	want := map[v1.SecurityEventType]v1.SecuritySubjectType{v1.SecurityEventType_SECURITY_EVENT_TYPE_PRINCIPAL_DISABLED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_PRINCIPAL, v1.SecurityEventType_SECURITY_EVENT_TYPE_SESSION_REVOKED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_SESSION, v1.SecurityEventType_SECURITY_EVENT_TYPE_CREDENTIAL_COMPROMISED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TOKEN, v1.SecurityEventType_SECURITY_EVENT_TYPE_DEVICE_POSTURE_CHANGED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DEVICE, v1.SecurityEventType_SECURITY_EVENT_TYPE_GROUP_MEMBERSHIP_CHANGED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_GROUP, v1.SecurityEventType_SECURITY_EVENT_TYPE_APPLICATION_ACCESS_WITHDRAWN: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_APPLICATION, v1.SecurityEventType_SECURITY_EVENT_TYPE_DELEGATION_REVOKED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DELEGATION, v1.SecurityEventType_SECURITY_EVENT_TYPE_ISSUER_KEY_COMPROMISED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_ISSUER_KEY, v1.SecurityEventType_SECURITY_EVENT_TYPE_TENANT_RELATIONSHIP_REMOVED: v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TENANT_RELATIONSHIP}
	if want[t] != s {
		return ErrRefused
	}
	return nil
}

func boundedClaims(c Claims, l Limits) error {
	b, err := json.Marshal(c)
	if err != nil || len(c) > l.MaxClaims || len(b) > l.MaxClaimBytes {
		return ErrRefused
	}
	type node struct {
		value any
		depth int
	}
	stack := []node{{value: map[string]any(c)}}
	nodes := 0
	for len(stack) != 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes++
		if current.depth > 8 || nodes > l.MaxClaims*32 {
			return ErrRefused
		}
		switch value := current.value.(type) {
		case map[string]any:
			for _, child := range value {
				stack = append(stack, node{child, current.depth + 1})
			}
		case []any:
			for _, child := range value {
				stack = append(stack, node{child, current.depth + 1})
			}
		}
	}
	return nil
}
func entryFrom(e *v1.SecurityEvent, emergency bool) Entry {
	return Entry{Key: Key{e.Issuer, e.Subject.Type, e.Subject.Identifier, e.PolicyRevision}, EventType: e.Type, Enforcement: e.Enforcement, IssuedAt: e.IssuedAt.AsTime(), ExpiresAt: e.ExpiresAt.AsTime(), Emergency: emergency}
}
func (i *Ingestor) reserveReplay(iss, id string, exp time.Time) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	now := i.now()
	for k, v := range i.replay {
		if !v.After(now) {
			delete(i.replay, k)
		}
	}
	key := iss + "\x00" + id
	if _, ok := i.replay[key]; ok {
		return false
	}
	if len(i.replay) >= i.limits.ReplayEntries {
		return false
	}
	i.replay[key] = exp
	return true
}
func (i *Ingestor) releaseReplay(iss, id string) {
	i.mu.Lock()
	delete(i.replay, iss+"\x00"+id)
	i.mu.Unlock()
}
func (i *Ingestor) allowDelivery() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	minute := i.now().Unix() / 60
	const deliveryKey = "all"
	w := i.deliveries[deliveryKey]
	if w.minute != minute {
		w = rateWindow{minute: minute}
	}
	w.count++
	i.deliveries[deliveryKey] = w
	return w.count <= i.limits.DeliveriesPerMinute
}
func (i *Ingestor) observe(ctx context.Context, stage, eventType, detail string) {
	if i.observer != nil {
		i.observer.Observe(ctx, Observation{stage, eventType, detail})
	}
}
func (i *Ingestor) refuse(ctx context.Context, detail string) error {
	i.observe(ctx, "refusal", "", detail)
	return fmt.Errorf("%w: %s", ErrRefused, detail)
}

// Check consults security state at a meaningful use boundary. Expired entries
// are ignored; backend failure is always a refusal rather than "not revoked".
func (i *Ingestor) Check(ctx context.Context, boundary Boundary, key Key, strong bool) (Entry, bool, error) {
	if boundary == "" {
		return Entry{}, false, ErrRefused
	}
	e, ok, err := i.store.Lookup(ctx, key, strong)
	if err != nil {
		return Entry{}, false, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if ok && !e.ExpiresAt.After(i.now()) {
		i.observe(ctx, "expiry", e.EventType.String(), "expired")
		return Entry{}, false, nil
	}
	return e, ok, nil
}

// EmergencyRevoke is the administrative fail-closed path. It requires an
// expiry and explicit durable action and uses the same linearizable Store.
func (i *Ingestor) EmergencyRevoke(ctx context.Context, e Entry) error {
	if e.Key.Issuer == "" || e.Key.Identifier == "" || e.ExpiresAt.IsZero() || e.Enforcement == v1.EnforcementAction_ENFORCEMENT_ACTION_UNSPECIFIED {
		return ErrRefused
	}
	e.Emergency = true
	if err := i.store.Apply(ctx, e); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	i.observe(ctx, "application", e.EventType.String(), "emergency")
	return nil
}
func (i *Ingestor) Inspect(ctx context.Context, strong bool, limit int) ([]Entry, error) {
	if limit <= 0 || limit > i.limits.StateEntries {
		return nil, ErrRefused
	}
	return i.store.Snapshot(ctx, strong, limit)
}
func (i *Ingestor) Compact(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > i.limits.StateEntries {
		return 0, ErrRefused
	}
	n, err := i.store.Compact(ctx, i.now(), limit)
	if err == nil && n > 0 {
		i.observe(ctx, "expiry", "", "compacted")
	}
	return n, err
}

// Handler adapts Deliver to Shared Signals push delivery. Authentication of
// the transport is additional to, never a substitute for, SET verification.
func (i *Ingestor) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, err := i.Deliver(r.Context(), r.Header.Get("X-Flowstate-Issuer"), r.Body)
		if err != nil {
			http.Error(w, "security event refused", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// String is intentionally privacy-safe.
func (k Key) String() string {
	return fmt.Sprintf("%s/%s/revision-%d", strings.ToLower(k.SubjectType.String()), "[redacted]", k.PolicyRevision)
}
