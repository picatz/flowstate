package stepup

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const secret = "pkce-verifier-authorization-code-token-dpop-browser-state"

type memoryStore struct {
	mu      sync.Mutex
	records map[string]Record
}

func (s *memoryStore) Create(_ context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = map[string]Record{}
	}
	s.records[r.Reference.TransactionID] = r
	return nil
}
func (s *memoryStore) Get(_ context.Context, id string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	if !ok {
		return Record{}, ErrNotFound
	}
	return r, nil
}
func (s *memoryStore) Mutate(_ context.Context, id string, f func(*Record) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	if !ok {
		return ErrNotFound
	}
	if err := f(&r); err != nil {
		return err
	}
	s.records[id] = r
	return nil
}

type provider struct {
	validated int
	device    bool
}

func (p *provider) Begin(_ context.Context, _ string, _ Binding, device bool) (Presentation, error) {
	p.device = device
	return Presentation{VerificationURL: "https://authorize.example/verify", UserCode: "ABCD", DeviceFlow: device}, nil
}
func (*provider) Complete(_ context.Context, _ string, values url.Values) (Status, error) {
	if values.Get("deny") != "" {
		return StatusDenied, nil
	}
	return StatusComplete, nil
}
func (p *provider) Validate(context.Context, string, Binding, string) error {
	p.validated++
	return nil
}
func (*provider) Cancel(context.Context, string) error { return nil }

type authorizer struct{ calls int }

func (a *authorizer) Reauthorize(context.Context, Binding, string, string) error {
	a.calls++
	return nil
}

func request() Request {
	return Request{Binding: Binding{Principal: "issuer#alice", ActorChain: []string{"agent", "alice"}, OAuthClient: "flow", ProtectedResource: "https://api.example", RequestedAction: "deploy", Resource: "prod/widget", Tenant: "acme", RedirectURI: "https://flow.example/callback", PolicyRevision: "policy-7", ProofKey: "jkt:public-thumbprint"}, RequiredAssurance: "aal2", PlanDigest: "sha256:plan", TTL: time.Minute}
}
func coordinator(now time.Time) (*Coordinator, *memoryStore, *provider, *authorizer) {
	s := &memoryStore{}
	p := &provider{}
	a := &authorizer{}
	return &Coordinator{Store: s, Provider: p, Authorizer: a, Now: func() time.Time { return now }, NewID: func() (string, error) { return "opaque-transaction", nil }}, s, p, a
}

func TestReferenceIsTheOnlyWorkflowSafeValue(t *testing.T) {
	c, store, _, _ := coordinator(time.Unix(100, 0))
	ref, display, err := c.Start(t.Context(), request())
	if err != nil {
		t.Fatal(err)
	}
	history, _ := json.Marshal(ref)
	for _, artifact := range []string{secret, "ABCD", "authorize.example", "issuer#alice", "flow", "prod/widget", "jkt:public-thumbprint"} {
		if strings.Contains(string(history), artifact) || strings.Contains(ref.StatusReference, artifact) {
			t.Fatalf("workflow-safe state contains %q: %s", artifact, history)
		}
	}
	if display.VerificationURL == "" {
		t.Fatal("browser instruction omitted")
	}
	record, _ := store.Get(t.Context(), ref.TransactionID)
	encoded, _ := json.Marshal(record)
	if string(encoded) == "" || strings.Contains(string(encoded), secret) {
		t.Fatal("protocol secret reached runtime metadata")
	}
}

func TestCompleteReauthorizeConsumeAndDuplicateCallback(t *testing.T) {
	c, _, p, a := coordinator(time.Unix(100, 0))
	ref, _, _ := c.Start(t.Context(), request())
	values := url.Values{"code": {secret}, "state": {secret}}
	if err := c.Callback(t.Context(), ref.TransactionID, values); err != nil {
		t.Fatal(err)
	}
	if err := c.Callback(t.Context(), ref.TransactionID, values); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if err := c.Resume(t.Context(), ref, ref.PlanDigest, "policy-7"); err != nil {
		t.Fatal(err)
	}
	if p.validated != 1 || a.calls != 1 {
		t.Fatalf("validate=%d authorize=%d", p.validated, a.calls)
	}
	if err := c.Resume(t.Context(), ref, ref.PlanDigest, "policy-7"); !errors.Is(err, ErrCanceled) {
		t.Fatalf("reusable grant: %v", err)
	}
}

func TestRefusalsAndDeviceFlowDoNotLeakArtifacts(t *testing.T) {
	now := time.Unix(100, 0)
	c, _, p, _ := coordinator(now)
	req := request()
	req.PreferDeviceFlow = true
	ref, display, err := c.Start(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !p.device || !display.DeviceFlow {
		t.Fatal("device path not selected")
	}
	if err := c.Callback(t.Context(), ref.TransactionID, url.Values{"code": {secret}, "state": {secret}}); err != nil {
		t.Fatal(err)
	}
	checks := []error{c.Resume(t.Context(), ref, "sha256:different", "policy-7"), c.Resume(t.Context(), ref, ref.PlanDigest, "policy-8")}
	for _, err := range checks {
		if err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("unsafe error: %v", err)
		}
	}
	c.Now = func() time.Time { return now.Add(2 * time.Minute) }
	if err := c.Callback(t.Context(), ref.TransactionID, url.Values{"code": {secret}}); !errors.Is(err, ErrExpired) || strings.Contains(err.Error(), secret) {
		t.Fatalf("expiry: %v", err)
	}
}
