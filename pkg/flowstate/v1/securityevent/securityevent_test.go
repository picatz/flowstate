package securityevent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

type verifierFunc func(context.Context, string) (Claims, error)

func (f verifierFunc) VerifySET(c context.Context, s string) (Claims, error) { return f(c, s) }

func TestDeliverVerifiesNormalizesReplaysAndChecksBoundaries(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	claims := Claims{"iss": "https://issuer.example", "aud": "flowstate", "jti": "delivery-1", "iat": float64(now.Unix()), "exp": float64(now.Add(time.Minute).Unix()), "events": map[string]any{CAEPSessionRevoked: map[string]any{"subject": map[string]any{"subject_type": "session", "id": "session-secret"}, "policy_revision": float64(7)}}}
	store, err := NewMemoryStore(8)
	require.NoError(t, err)
	ing, err := New(verifierFunc(func(context.Context, string) (Claims, error) { return claims, nil }), []Adapter{SharedSignalsAdapter{Audience: "flowstate"}}, store, DefaultLimits(), nil)
	require.NoError(t, err)
	ing.now = func() time.Time { return now }
	events, err := ing.Deliver(t.Context(), "https://issuer.example", strings.NewReader(`{"set":"signed"}`))
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN, events[0].Enforcement)
	key := Key{"https://issuer.example", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_SESSION, "session-secret", 7}
	entry, blocked, err := ing.Check(t.Context(), BoundaryAuthentication, key, true)
	require.NoError(t, err)
	require.True(t, blocked)
	require.Equal(t, v1.SecurityEventType_SECURITY_EVENT_TYPE_SESSION_REVOKED, entry.EventType)
	_, err = ing.Deliver(t.Context(), "https://issuer.example", strings.NewReader(`{"set":"signed"}`))
	require.ErrorIs(t, err, ErrDuplicate)
}

func TestKnownSubjectDoesNotBypassVerification(t *testing.T) {
	store, _ := NewMemoryStore(1)
	ing, err := New(verifierFunc(func(context.Context, string) (Claims, error) { return nil, errors.New("bad signature") }), []Adapter{SharedSignalsAdapter{Audience: "flowstate"}}, store, DefaultLimits(), nil)
	require.NoError(t, err)
	_, err = ing.Deliver(t.Context(), "known-issuer", strings.NewReader(`{"set":"names-a-known-subject"}`))
	require.ErrorIs(t, err, ErrRefused)
	entries, err := store.Snapshot(t.Context(), true, 1)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestUnknownEventAndWrongSubjectFormatFailClosed(t *testing.T) {
	base := Claims{"iss": "issuer", "aud": "flowstate", "jti": "id", "iat": float64(time.Now().Unix())}
	a := SharedSignalsAdapter{Audience: "flowstate"}
	base["events"] = map[string]any{"https://vendor.invalid/invented": map[string]any{"subject": map[string]any{"subject_type": "session", "id": "x"}}}
	_, err := a.Normalize(base)
	require.ErrorIs(t, err, ErrRefused)
	base["events"] = map[string]any{CAEPSessionRevoked: map[string]any{"subject": map[string]any{"subject_type": "principal", "id": "known"}}}
	_, err = a.Normalize(base)
	require.ErrorIs(t, err, ErrRefused)
}

func TestStoreIsBoundedAndCompactsExpiredEntries(t *testing.T) {
	s, _ := NewMemoryStore(1)
	now := time.Now()
	e := Entry{Key: Key{Issuer: "i", Identifier: "a"}, IssuedAt: now, ExpiresAt: now.Add(-time.Second)}
	require.NoError(t, s.Apply(t.Context(), e))
	require.ErrorIs(t, s.Apply(t.Context(), Entry{Key: Key{Issuer: "i", Identifier: "b"}}), ErrUnavailable)
	n, err := s.Compact(t.Context(), now, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, s.Apply(t.Context(), Entry{Key: Key{Issuer: "i", Identifier: "b"}}))
}

func TestBodyBatchAndRateBounds(t *testing.T) {
	store, _ := NewMemoryStore(2)
	limits := DefaultLimits()
	limits.MaxBodyBytes = 16
	limits.DeliveriesPerMinute = 1
	ing, err := New(verifierFunc(func(context.Context, string) (Claims, error) {
		t.Fatal("verifier reached for refused body")
		return nil, nil
	}), []Adapter{SharedSignalsAdapter{Audience: "a"}}, store, limits, nil)
	require.NoError(t, err)
	_, err = ing.Deliver(t.Context(), "issuer", strings.NewReader(strings.Repeat("x", 17)))
	require.ErrorIs(t, err, ErrRefused)
}
