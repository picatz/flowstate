package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

const testMessageKey = "018f0e6c-7b42-7cc1-8a31-65c0f8758f4a"

func TestSendPostUsesTheGovernedClientAndBoundedSlackShape(t *testing.T) {
	const token = "xoxb-test-value-never-a-real-credential"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/chat.postMessage" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q", got)
		}
		var body postRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decoding request: %v", err)
		}
		if body.Channel != "C123APPROVAL" || body.Text != "Approve deploy 42?" || body.ClientMsgID != testMessageKey {
			t.Errorf("request body = %#v", body)
		}
		if body.UnfurlLinks || body.UnfurlMedia {
			t.Error("slack.post allowed Slack to unfurl links from notification text")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C123APPROVAL","ts":"1503435956.000247"}`))
	}))
	t.Cleanup(server.Close)

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRedirects(),
		netpolicy.WithMaxResponseBytes(64<<10),
		netpolicy.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}

	got, err := sendPost(context.Background(), policy.Client(), server.URL+"/api/chat.postMessage", token, &slackv1.PostInputs{
		Channel: "C123APPROVAL", Text: "Approve deploy 42?", MessageKey: testMessageKey,
	})
	if err != nil {
		t.Fatalf("sendPost: %v", err)
	}
	if got.Channel != "C123APPROVAL" || got.TS != "1503435956.000247" {
		t.Errorf("response = %#v", got)
	}
}

func TestDeniedDestinationIsNotDialed(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	t.Cleanup(server.Close)

	policy, err := netpolicy.New(netpolicy.WithMaxResponseBytes(64 << 10))
	if err != nil {
		t.Fatalf("building deny-by-default test policy: %v", err)
	}
	_, err = sendPost(context.Background(), policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "egress policy denied") {
		t.Fatalf("sendPost error = %v, want egress-policy refusal", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("denied listener received %d request(s), want none", got)
	}
}

func TestSlackPostRefusesARehearsalBeforeAnyWrite(t *testing.T) {
	old := egressPolicy
	var err error
	egressPolicy, err = netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	t.Cleanup(func() { egressPolicy = old })

	// A direct call has no host-attested production identity. It must fail at
	// the first gate, before decoding (and therefore before resolving or using)
	// even an otherwise valid credential. Local dispatch carries REHEARSAL and
	// reaches the same branch.
	_, err = slackPost(context.Background(), map[string]*flowstatev1.Value{
		"token": flowstatev1.NewValue("not-a-real-token"),
	}, &flowstatev1.Scope{})
	if err == nil || !strings.Contains(err.Error(), "production execution identity") {
		t.Fatalf("slackPost error = %v, want rehearsal-write refusal", err)
	}
}

func TestRateLimitCarriesBoundedRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"error":"ratelimited"}`))
	}))
	t.Cleanup(server.Close)
	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithMaxResponseBytes(64<<10))
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	_, err = sendPost(context.Background(), policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "retry after 17s") {
		t.Fatalf("sendPost error = %v, want 17s rate-limit hint", err)
	}
	if got := retryAfter("999999"); got != 5*time.Minute {
		t.Errorf("oversized Retry-After = %s, want 5m bound", got)
	}
}

func TestServerErrorAndLostResponseAreUnknownOutcomes(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"documented ambiguous server error": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"ok":false,"error":"internal_error"}`))
		},
		"malformed acknowledgement": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`not-json`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithMaxResponseBytes(64<<10))
			if err != nil {
				t.Fatalf("building test egress policy: %v", err)
			}
			_, err = sendPost(context.Background(), policy.Client(), server.URL, "not-real", validPostInputs())
			if err == nil || !strings.Contains(err.Error(), "may") || !strings.Contains(err.Error(), "not retried automatically") {
				t.Fatalf("sendPost error = %v, want unknown-outcome refusal", err)
			}
		})
	}
}

func TestOversizedResponseIsAnUnknownOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
	}))
	t.Cleanup(server.Close)
	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	_, err = sendPost(context.Background(), policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "exceeded the 65536-byte limit") || !strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("sendPost error = %v, want bounded unknown outcome", err)
	}
}

func TestPostInputBounds(t *testing.T) {
	for name, mutate := range map[string]func(*slackv1.PostInputs){
		"channel name":     func(in *slackv1.PostInputs) { in.Channel = "#approvals" },
		"empty text":       func(in *slackv1.PostInputs) { in.Text = "" },
		"oversized text":   func(in *slackv1.PostInputs) { in.Text = strings.Repeat("界", maxTextCharacters+1) },
		"unstable key":     func(in *slackv1.PostInputs) { in.MessageKey = "deploy-42" },
		"malformed thread": func(in *slackv1.PostInputs) { in.ThreadTs = "latest" },
	} {
		t.Run(name, func(t *testing.T) {
			in := validPostInputs()
			mutate(in)
			if err := validatePost(in); err == nil {
				t.Fatal("validatePost accepted invalid input")
			}
		})
	}
	if _, err := tokenFromValue(flowstatev1.NewValue(strings.Repeat("x", maxTokenBytes+1))); err == nil {
		t.Fatal("tokenFromValue accepted an oversized resolved credential")
	}
}

func validPostInputs() *slackv1.PostInputs {
	return &slackv1.PostInputs{
		Channel: "C123APPROVAL", Text: "Approve deploy 42?", MessageKey: testMessageKey,
	}
}
