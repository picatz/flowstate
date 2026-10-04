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
func (a *auditLog) signalRecords() []*v1.AuditRecord { return a.recordsFor("Signal") }

// recordsFor are the decisions recorded for one RPC, in order.
func (a *auditLog) recordsFor(rpc string) []*v1.AuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()

	var out []*v1.AuditRecord
	for _, record := range a.records {
		if record.GetRpc() == rpc {
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
//
// Neither approver holds `workload.read`, only `workload.signal` (#2290): the
// page reads the gate with GetGate, which that action admits, and Get stays
// closed to them. carol, whom the policy refuses, still sees the question, with
// may_answer false and no buttons, and the page does not send her answer; the
// server's own Signal decision, reached by a client that skips the page, is the
// audited denial.
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
		ctx := auth.ContextWithPrincipal(r.Context(), signalOnly(issuer, subject))
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

	// The read the page needs is the one the signal action admits; the run read
	// that needs `workload.read` is refused to the same callers, before the run
	// is addressed.
	carol := signalOnly(issuer, "carol@example.com")
	ctxCarol := auth.ContextWithPrincipal(t.Context(), carol)
	_, err = flow.Get(ctxCarol, connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "a signal-only caller must not read the run")

	readOnly := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: issuer, Subject: "reader@example.com",
		Actions: []string{v1.AuthorizationActionScope(v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ)},
	})
	_, err = flow.GetGate(readOnly, connect.NewRequest(&v1.GetGateRequest{WorkflowId: workflowID, SignalName: "deploy-approved"}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "workload.read does not open a gate: the read is scoped to answering")

	// What the policy decides is reported, not delivered: alice may answer, carol
	// may not, and neither read delivered anything.
	asked := connect.NewRequest(&v1.GetGateRequest{WorkflowId: workflowID, SignalName: "deploy-approved"})
	gateForCarol, err := flow.GetGate(ctxCarol, asked)
	require.NoError(t, err)
	require.False(t, gateForCarol.Msg.GetMayAnswer())
	require.Equal(t, "approval", gateForCarol.Msg.GetStepId())
	require.NotEmpty(t, gateForCarol.Msg.GetRunId())
	gateForAlice, err := flow.GetGate(auth.ContextWithPrincipal(t.Context(), signalOnly(issuer, "alice@example.com")), asked)
	require.NoError(t, err)
	require.True(t, gateForAlice.Msg.GetMayAnswer())
	require.Empty(t, trail.signalRecords(), "reading a gate decided nothing about delivering a signal")

	// A name nothing waits on is the one answer for an absent gate.
	_, err = flow.GetGate(ctxCarol, connect.NewRequest(&v1.GetGateRequest{WorkflowId: workflowID, SignalName: "no-such-gate"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Carol can open the page; the question is not secret from the tenant. What
	// she cannot do is answer it: it shows read-only and sends nothing.
	shown := visit(http.MethodGet, "carol@example.com", nil)
	require.Equal(t, http.StatusOK, shown.Code)
	require.NotContains(t, shown.Body.String(), `value="approve"`)

	refused := visit(http.MethodPost, "carol@example.com", url.Values{"decision": {"approve"}})
	require.Equal(t, http.StatusForbidden, refused.Code, refused.Body.String())
	require.Empty(t, trail.signalRecords(), "the page sent an answer the read said the policy refuses")

	// A client that skips the page still meets the server's own decision, and it
	// is audited.
	_, err = flow.Signal(ctxCarol, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload:    &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

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

	// Every gate read was decided and recorded under its own RPC name.
	gateRecords := trail.recordsFor("GetGate")
	require.NotEmpty(t, gateRecords)
	for _, record := range gateRecords {
		if record.GetDecision() == v1.AuditDecision_AUDIT_DECISION_DENY {
			require.Equal(t, "reader@example.com", record.GetIdentity().GetSubject())
		}
	}

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

// signalOnly is a caller granted `workload.signal` and nothing else: the
// approver #2290 describes, who may answer a gate and was refused the read of it.
func signalOnly(issuer, subject string) auth.Principal {
	return auth.Principal{
		Issuer:  issuer,
		Subject: subject,
		Actions: []string{v1.AuthorizationActionScope(v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL)},
	}
}
