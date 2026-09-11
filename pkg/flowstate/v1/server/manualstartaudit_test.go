package server_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// recordingEmitter mirrors the internal package's test helper of the same
// name (audit_internal_test.go), duplicated here because it is unexported and
// this file needs it from server_test to reach a real Temporal execution
// through the exported API.
type recordingEmitter struct {
	records []*v1.AuditRecord
}

func (e *recordingEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	e.records = append(e.records, record)
	return nil
}

// TestSignalWithStartAuditsAPolicyRefusalOnAnExistingEntity covers #1883: the
// already-running arm of SignalWithStart re-authorizes delivery under the
// entity's own `signals:` policy after the admission ALLOW is already
// written. A sender that policy refuses used to leave a trail reading as
// allowed; this pins that the refusal now leaves a DENY naming the run.
func TestSignalWithStartAuditsAPolicyRefusalOnAnExistingEntity(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	sink := &recordingEmitter{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)

	s := mustNew(t, temporal, server.WithNamespace("acme"), server.WithAudit(recorder))

	restricted := entityWorkflow(map[string]*v1.SignalPolicy{
		"update": {
			Allow: []*v1.SignalPolicyRule{
				{Subject: v1.QualifiedSubject("https://issuer.example.com", "owner@example.com")},
			},
		},
	})

	owner := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "owner@example.com",
	})
	created, err := s.SignalWithStart(owner, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "restricted-order",
		Workflow:  restricted,
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err)
	require.True(t, created.Msg.GetCreated())

	stranger := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})
	_, err = s.SignalWithStart(stranger, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "restricted-order",
		Workflow:  restricted,
		Name:      "update",
		Payload:   updatePayload(1, true),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Three records, not two: the owner's create leaves one admission ALLOW,
	// and the stranger's request writes its own admission ALLOW too — "may
	// this caller start work under this entity key in their own namespace"
	// is asked and answered before the already-running branch discovers there
	// is nothing to start, exactly as SignalWithStart's own comment on that
	// admission record says. What must not happen is the stranger's refused
	// *delivery* also reading as an allow — that is the second decision this
	// test pins, and it must be the last record: a DENY, not a second ALLOW
	// the stranger could be read as having earned.
	require.Len(t, sink.records, 3)
	ownerAllow, strangerAllow, strangerDeny := sink.records[0], sink.records[1], sink.records[2]

	for _, allow := range []*v1.AuditRecord{ownerAllow, strangerAllow} {
		require.Equal(t, "SignalWithStart", allow.GetRpc())
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allow.GetDecision())
	}

	require.Equal(t, "SignalWithStart", strangerDeny.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, strangerDeny.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, strangerDeny.GetDenyCode())
	require.Equal(t, strangerAllow.GetResourceKey(), strangerDeny.GetResourceKey(), "both of the stranger's decisions name the same run")
}

// TestSignalWithStartAuditsAPolicyRefusalOnTheConcurrencyCompatibilityArm
// covers the other of #1883's two authorizeSignal refusal sites: the arm
// reached when the request's own workflow copy declares `concurrency:`,
// which — per TestConcurrencySignalWithStartStillSignalsAnExistingEntity —
// is creation input ignored once the entity already exists, and is
// authorized through a different code path than the already-started arm
// TestSignalWithStartAuditsAPolicyRefusalOnAnExistingEntity covers. Both
// call the same authorizeSignal and the same new auditDeny, but only one
// of the two had a regression test before this.
func TestSignalWithStartAuditsAPolicyRefusalOnTheConcurrencyCompatibilityArm(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	sink := &recordingEmitter{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)

	s := mustNew(t, temporal, server.WithNamespace("acme"), server.WithAudit(recorder))

	restricted := entityWorkflow(map[string]*v1.SignalPolicy{
		"update": {
			Allow: []*v1.SignalPolicyRule{
				{Subject: v1.QualifiedSubject("https://issuer.example.com", "owner@example.com")},
			},
		},
	})

	owner := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "owner@example.com",
	})
	created, err := s.SignalWithStart(owner, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "concurrent-order",
		Workflow:  restricted,
		Name:      "update",
		Payload:   updatePayload(1, false),
	}))
	require.NoError(t, err)
	require.True(t, created.Msg.GetCreated())

	// Declaring concurrency on the *next* request's workflow copy is what
	// routes SignalWithStart through the compatibility arm instead of the
	// already-started arm — trustedWorkflow returns the request's own copy
	// when nothing is registered under the name, so setting it here is
	// enough, exactly as the existing success-path test for this arm does.
	restricted.Concurrency = &v1.Concurrency{Key: v1.NewLiteral("prod-eu")}

	stranger := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})
	_, err = s.SignalWithStart(stranger, connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "concurrent-order",
		Workflow:  restricted,
		Name:      "update",
		Payload:   updatePayload(1, true),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	require.Len(t, sink.records, 3)
	_, strangerAllow, strangerDeny := sink.records[0], sink.records[1], sink.records[2]

	require.Equal(t, "SignalWithStart", strangerAllow.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, strangerAllow.GetDecision())

	require.Equal(t, "SignalWithStart", strangerDeny.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, strangerDeny.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, strangerDeny.GetDenyCode())
	require.Equal(t, strangerAllow.GetResourceKey(), strangerDeny.GetResourceKey(), "both of the stranger's decisions name the same run")
}
