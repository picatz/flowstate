package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

const testIdempotencyKey = "018f0e6c-7b42-7cc1-8a31-65c0f8758f4a"

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
	_, err = sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "egress policy denied") {
		t.Fatalf("sendPost error = %v, want egress-policy refusal", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("denied listener received %d request(s), want none", got)
	}
}

func TestSlackPostOnlyAcceptsAnEstablishedProductionMode(t *testing.T) {
	for _, test := range []struct {
		name   string
		caller sdk.Caller
		found  bool
		wantOK bool
	}{
		{name: "production", caller: sdk.Caller{Identity: &flowstatev1.WorkloadIdentity{Mode: flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION}}, found: true, wantOK: true},
		{name: "rehearsal", caller: sdk.Caller{Identity: &flowstatev1.WorkloadIdentity{Mode: flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_REHEARSAL}}, found: true},
		{name: "unspecified", caller: sdk.Caller{Identity: &flowstatev1.WorkloadIdentity{}}, found: true},
		{name: "unknown future mode", caller: sdk.Caller{Identity: &flowstatev1.WorkloadIdentity{Mode: flowstatev1.WorkloadIdentityMode(99)}}, found: true},
		{name: "missing caller"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireProductionMode("slack.post", test.caller, test.found)
			if test.wantOK && err != nil {
				t.Fatalf("requireProductionMode: %v", err)
			}
			if !test.wantOK && (err == nil || !strings.Contains(err.Error(), "production execution identity")) {
				t.Fatalf("requireProductionMode error = %v, want fail-closed production-mode refusal", err)
			}
		})
	}
}

func TestSlackPostChecksModeBeforeInputsAndEgress(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })

	// Invalid inputs and absent egress would each fail if reached. The mode
	// refusal must remain the task boundary's first decision, before either one
	// and before any credential can be decoded or used.
	_, err := slackPost(context.Background(), map[string]*flowstatev1.Value{
		"token": flowstatev1.NewValue("inert-test-token"),
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "production execution identity") {
		t.Fatalf("slackPost error = %v, want production-mode refusal before inputs and egress", err)
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
	_, err = sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "retry after 17s") {
		t.Fatalf("sendPost error = %v, want 17s rate-limit hint", err)
	}
	if got := retryAfter("999999"); got != 5*time.Minute {
		t.Errorf("oversized Retry-After = %s, want 5m bound", got)
	}
}

func TestOperatorRateLimitRefusesBeforeASecondWrite(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C123APPROVAL","ts":"1503435956.000247"}`))
	}))
	t.Cleanup(server.Close)
	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxRequestsPerSecondPerProcess("127.0.0.1", 1),
	)
	if err != nil {
		t.Fatalf("building rate-limited test policy: %v", err)
	}
	if _, err := sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs()); err != nil {
		t.Fatalf("first sendPost: %v", err)
	}
	_, err = sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "before it was sent") || !strings.Contains(err.Error(), "retry after") {
		t.Fatalf("second sendPost error = %v, want retryable pre-send rate refusal", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("listener received %d requests, want only the first write", got)
	}
}

func TestRateLimitAfterRedirectIsAnUnknownOutcome(t *testing.T) {
	err := classifyTransportError(opPost, &netpolicy.RateLimitedError{
		Host: "slack.com", RetryAfter: time.Second, AfterRedirect: true,
	})
	if !strings.Contains(err.Error(), "original request may already have taken effect") || !strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("classifyTransportError = %v, want redirect unknown outcome", err)
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
			_, err = sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs())
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
	_, err = sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "exceeded the 65536-byte limit") || !strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("sendPost error = %v, want bounded unknown outcome", err)
	}
}

func TestOperatorResponseLimitIsNamedInTheUnknownOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 65)))
	}))
	t.Cleanup(server.Close)
	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithMaxResponseBytes(64))
	if err != nil {
		t.Fatalf("building response-limited test policy: %v", err)
	}
	_, err = sendTest(t, policy.Client(), server.URL, "not-real", validPostInputs())
	if err == nil || !strings.Contains(err.Error(), "operator egress policy's 64-byte limit") || !strings.Contains(err.Error(), "not retried automatically") {
		t.Fatalf("sendPost error = %v, want operator-bound unknown outcome", err)
	}
}

func TestPostInputBounds(t *testing.T) {
	for name, mutate := range map[string]func(*slackv1.PostInputs){
		"channel name":             func(in *slackv1.PostInputs) { in.Channel = "#approvals" },
		"nothing to send":          func(in *slackv1.PostInputs) { in.Text = "" },
		"oversized text":           func(in *slackv1.PostInputs) { in.Text = strings.Repeat("界", render.MaxMessageText+1) },
		"unstable key":             func(in *slackv1.PostInputs) { in.IdempotencyKey = "deploy-42" },
		"malformed thread":         func(in *slackv1.PostInputs) { in.ThreadTs = "latest" },
		"broadcast without thread": func(in *slackv1.PostInputs) { in.ReplyBroadcast = true },
		"ephemeral and broadcast": func(in *slackv1.PostInputs) {
			in.ThreadTs, in.ReplyBroadcast, in.ToUser = "1503435956.000247", true, "U0ALICE"
		},
		"ephemeral and metadata": func(in *slackv1.PostInputs) {
			in.ToUser, in.Metadata = "U0ALICE", &slackv1.Metadata{EventType: "x"}
		},
		"user name":        func(in *slackv1.PostInputs) { in.ToUser = "alice" },
		"card and blocks":  func(in *slackv1.PostInputs) { in.Card, in.Blocks = testCard(), testBlocks() },
		"metadata no type": func(in *slackv1.PostInputs) { in.Metadata = &slackv1.Metadata{} },
		"metadata too big": func(in *slackv1.PostInputs) {
			in.Metadata = &slackv1.Metadata{EventType: "x", EventPayload: map[string]string{"k": strings.Repeat("v", 1025)}}
		},
		"metadata too many": func(in *slackv1.PostInputs) {
			m := map[string]string{}
			for i := range 17 {
				m[fmt.Sprint("k", i)] = "v"
			}
			in.Metadata = &slackv1.Metadata{EventType: "x", EventPayload: m}
		},
	} {
		t.Run(name, func(t *testing.T) {
			in := validPostInputs()
			mutate(in)
			if _, err := planPost(in); err == nil || !sdk.IsInvalidInput(err) {
				t.Fatalf("planPost error = %v, want an InvalidInput refusal", err)
			}
		})
	}
	if _, err := tokenFromValue(flowstatev1.NewValue(strings.Repeat("x", maxTokenBytes+1))); err == nil {
		t.Fatal("tokenFromValue accepted an oversized resolved credential")
	}
}

func validPostInputs() *slackv1.PostInputs {
	return &slackv1.PostInputs{
		Channel: "C123APPROVAL", Text: "Approve deploy 42?", IdempotencyKey: testIdempotencyKey,
	}
}

func testCard() *chatv1.Card {
	return &chatv1.Card{Kind: &chatv1.Card_Notice{Notice: &chatv1.Notice{
		Title: &chatv1.Text{Kind: &chatv1.Text_Plain{Plain: "Heads up"}},
	}}}
}

func testBlocks() []*slackv1.Block {
	return []*slackv1.Block{{Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}}}
}

// sendTest plans in and sends it to a test server standing in for slack.com.
func sendTest(t *testing.T, client *http.Client, serverURL, token string, in *slackv1.PostInputs) (*slackResponse, error) {
	t.Helper()
	p, err := planPost(in)
	if err != nil {
		t.Fatalf("planPost: %v", err)
	}
	return send(context.Background(), client, serverURL+"/api/", token, p)
}
