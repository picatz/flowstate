package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestIssueCommentWritesWithoutAProductionCaller records this task's
// compatibility posture: execution mode is an attested fact, not the
// authorization to post. A caller with no production attestation can still
// reach the mutation when task policy, a credential, and egress controls
// separately permit it. The local server and inert token keep that contract
// credential-free.
func TestIssueCommentWritesWithoutAProductionCaller(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v3/repos/acme/widgets/issues/7/comments" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42,"html_url":"https://example.invalid/comment/42","created_at":"2026-09-01T12:00:00Z"}`))
	}))
	t.Cleanup(server.Close)

	// A governed client standing in for the one this plugin takes from a grant
	// permitting the fixture server, built here because a test binary is not
	// launched by a worker. It is netpolicy's own client, so it carries the
	// destination checks and the response bound but not the SDK's credential
	// marking — that transport comes from sdk.HTTPClientWithBounds, and what it
	// marks is covered where it lives (pkg/flowstate/v1/plugin/sdk) and end to
	// end in reachable/egress_test.go, rather than here, where the subject is
	// the comment task.
	old := egressClientOnce
	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRedirects(),
		netpolicy.WithMaxResponseBytes(maxResponseBytes),
		netpolicy.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	egressClientOnce = policy.Client()
	t.Cleanup(func() { egressClientOnce = old })
	t.Setenv(envAPIBaseURL, server.URL)

	outputs, err := issueComment(context.Background(), map[string]*flowstatev1.Value{
		"owner":  flowstatev1.NewValue("acme"),
		"repo":   flowstatev1.NewValue("widgets"),
		"number": flowstatev1.NewValue(int64(7)),
		"body":   flowstatev1.NewValue("rehearsal comment"),
		"token":  flowstatev1.NewValue("inert-test-token"),
	}, nil)
	if err != nil {
		t.Fatalf("issueComment without a production caller: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("comment requests = %d, want 1 real local mutation", got)
	}
	if got := outputs.GetNamedValues()["comment_id"].GetLiteral().GetInt64Value(); got != 42 {
		t.Fatalf("comment_id = %d, want 42", got)
	}
}
