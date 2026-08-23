package oauthclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type agentFunc struct {
	acquire func(context.Context, Profile, Request) (Credential, error)
	refresh func(context.Context, Profile, Request, Credential) (Credential, error)
}

func (a agentFunc) Acquire(c context.Context, p Profile, r Request) (Credential, error) {
	return a.acquire(c, p, r)
}
func (a agentFunc) Refresh(c context.Context, p Profile, r Request, o Credential) (Credential, error) {
	if a.refresh != nil {
		return a.refresh(c, p, r, o)
	}
	return a.acquire(c, p, r)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testProfile() Profile {
	return Profile{Name: "interactive", Issuer: "https://issuer.example", ClientID: "flow", Flows: []Flow{AuthorizationCodePKCE}, DPoP: false, ResourceIndicators: true, AuthorizationDetails: true, RefreshRotation: true}
}
func testRequest(subject string) Request {
	return Request{Profile: "interactive", Subject: subject, ActorChain: "agent:a", Resource: "https://api.example", Flow: AuthorizationCodePKCE, Scopes: []string{"write", "read"}, PolicyRevision: "42", SecurityProfile: "high"}
}

func TestTransportBrokersAndIsolatesCredentials(t *testing.T) {
	var acquisitions atomic.Int32
	agent := agentFunc{acquire: func(_ context.Context, _ Profile, r Request) (Credential, error) {
		acquisitions.Add(1)
		return NewCredential("token-"+r.Subject, "", "Bearer", time.Now().Add(time.Hour), "")
	}}
	seen := make(chan string, 2)
	c, err := New([]Profile{testProfile()}, agent, WithBaseTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen <- r.Header.Get("Authorization")
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header), Request: r}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"tenant-a", "tenant-b"} {
		tr, err := c.Transport(testRequest(subject))
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest("GET", "https://api.example/v1", nil)
		if _, err = tr.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
	}
	if acquisitions.Load() != 2 {
		t.Fatalf("acquisitions=%d, want 2", acquisitions.Load())
	}
	if a, b := <-seen, <-seen; a != "Bearer token-tenant-a" || b != "Bearer token-tenant-b" {
		t.Fatalf("credentials crossed isolation boundary: %q %q", a, b)
	}
}

func TestRefreshIsSingleFlight(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	agent := agentFunc{acquire: func(context.Context, Profile, Request) (Credential, error) {
		calls.Add(1)
		close(started)
		<-release
		return NewCredential("shared", "", "Bearer", time.Now().Add(time.Hour), "")
	}}
	c, err := New([]Profile{testProfile()}, agent, WithBaseTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := c.Transport(testRequest("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _ := http.NewRequest("GET", "https://api.example", nil)
			if _, e := tr.RoundTrip(r); e != nil {
				t.Error(e)
			}
		}()
	}
	<-started
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("acquisitions=%d, want one", calls.Load())
	}
}

func TestProfileFailsClosed(t *testing.T) {
	p := testProfile()
	p.ResourceIndicators = false
	c, err := New([]Profile{p}, agentFunc{acquire: func(context.Context, Profile, Request) (Credential, error) { panic("must not acquire") }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Transport(testRequest("tenant")); err != ErrDenied {
		t.Fatalf("error=%v, want ErrDenied", err)
	}
}
