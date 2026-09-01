package main

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != "/api/v3/repos/acme/widgets/issues/7/comments" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42,"html_url":"https://example.invalid/comment/42","created_at":"2026-09-01T12:00:00Z"}`))
	}))
	t.Cleanup(server.Close)

	old := egressPolicy
	var err error
	egressPolicy, err = netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithDenyRedirects(),
		netpolicy.WithMaxResponseBytes(maxResponseBytes),
		netpolicy.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	t.Cleanup(func() { egressPolicy = old })
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
	if requests != 1 {
		t.Fatalf("comment requests = %d, want 1 real local mutation", requests)
	}
	if got := outputs.GetNamedValues()["comment_id"].GetLiteral().GetInt64Value(); got != 42 {
		t.Fatalf("comment_id = %d, want 42", got)
	}
}
