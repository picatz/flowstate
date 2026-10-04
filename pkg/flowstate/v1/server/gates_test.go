package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/gates"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// auditLog collects the decision records a server emits.
type auditLog struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (a *auditLog) Emit(_ context.Context, record *v1.AuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)

	return nil
}

// signalRecords are the decisions recorded for the Signal verb, in order.
func (a *auditLog) signalRecords() []*v1.AuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()

	var out []*v1.AuditRecord
	for _, record := range a.records {
		if record.GetRpc() == "Signal" {
			out = append(out, record)
		}
	}

	return out
}

// TestAnApproverAnswersAGateInTheBrowser is #1748's conformance case, through the
// real server: alice, whom the gate's `signals:` policy allows, opens the page and
// approves, and the run proceeds; carol, whom it does not, is refused by the
// server's own decision, and the gate stays open for someone who may answer it.
// Both decisions are the audit records `flow signal` produces, because the page
// calls the same Signal verb through the same handler.
func TestAnApproverAnswersAGateInTheBrowser(t *testing.T) {
	t.Parallel()

	const issuer = "https://issuer.example.com"

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	trail := &auditLog{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(trail))
	require.NoError(t, err)
	flow := mustNew(t, temporal, server.WithNamespace(teamANamespace), server.WithAudit(recorder))

	started, err := flow.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflowRequiring(issuer, "alice@example.com"),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, temporal, workflowID)

	// The API as a deployment serves it, with the authenticator reduced to the
	// one thing the test needs from it: a bearer token that names a verified
	// subject. The page is mounted on this handler exactly as `flow server
	// --gates-ui` mounts it on the authenticated one.
	rpc := http.NewServeMux()
	rpc.Handle(flowstatev1connect.NewWorkflowServiceHandler(flow))
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		ctx := auth.ContextWithPrincipal(r.Context(), auth.Principal{Issuer: issuer, Subject: subject})
		rpc.ServeHTTP(w, r.WithContext(ctx))
	})
	page := gates.New(api)

	visit := func(method, subject string, form url.Values) *httptest.ResponseRecorder {
		var req *http.Request
		if method == http.MethodPost {
			req = httptest.NewRequest(method, gates.Path(workflowID, "deploy-approved"), strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Sec-Fetch-Site", "same-origin")
		} else {
			req = httptest.NewRequest(method, gates.Path(workflowID, "deploy-approved"), nil)
		}
		if subject != "" {
			req.Header.Set("Authorization", "Bearer "+subject)
		}
		rec := httptest.NewRecorder()
		page.ServeHTTP(rec, req)

		return rec
	}

	// A visitor with no credential is refused by the API, not by the page.
	require.Equal(t, http.StatusUnauthorized, visit(http.MethodGet, "", nil).Code)

	// Carol can open the gate; the question is not secret from the tenant. What
	// she cannot do is answer it.
	require.Equal(t, http.StatusOK, visit(http.MethodGet, "carol@example.com", nil).Code)

	refused := visit(http.MethodPost, "carol@example.com", url.Values{"decision": {"approve"}})
	require.Equal(t, http.StatusForbidden, refused.Code, refused.Body.String())

	deniedRecords := trail.signalRecords()
	require.Len(t, deniedRecords, 1)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, deniedRecords[0].GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, deniedRecords[0].GetDenyCode())
	require.Equal(t, "carol@example.com", deniedRecords[0].GetIdentity().GetSubject())

	// Refused before Temporal saw it: the gate is still open to answer.
	resp, err := flow.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, resp.Msg.GetStatus())
	require.Len(t, resp.Msg.GetProgress().GetPendingWaits(), 1)

	approved := visit(http.MethodPost, "alice@example.com", url.Values{"decision": {"approve"}, "comment": {"ship it"}})
	require.Equal(t, http.StatusOK, approved.Code, approved.Body.String())
	require.Contains(t, approved.Body.String(), "Approved")

	var final *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		got, err := flow.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil || got.Msg.GetStatus() != v1.RunResponse_STATUS_COMPLETED {
			return false
		}
		final = got

		return true
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after the browser approval")
	require.NotNil(t, final.Msg.GetOutputs().GetStepValues()["deploy"], "the gated step did not run after approval")

	allRecords := trail.signalRecords()
	require.Len(t, allRecords, 2)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allRecords[1].GetDecision())
	require.Equal(t, "alice@example.com", allRecords[1].GetIdentity().GetSubject())

	// The gate is answered; a second approver sees that, and nothing is
	// buffered for whatever might next wait on the same name.
	late := visit(http.MethodPost, "alice@example.com", url.Values{"decision": {"approve"}})
	require.Equal(t, http.StatusNotFound, late.Code)
	require.Len(t, trail.signalRecords(), 2, "an answer to a closed gate reached the Signal verb")
}
