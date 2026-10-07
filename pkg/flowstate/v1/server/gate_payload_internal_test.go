package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestGetGateReportsDependsOnPayloadRatherThanTrue: a gate listing carries no
// delivery, so a predicate over `payload` cannot be decided for the caller
// alone. It is reported as depending on the payload, never as answerable, even
// for the approver the predicate would admit once they send an approve.
func TestGetGateReportsDependsOnPayloadRatherThanTrue(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		allow   string
		depends bool
		may     bool
	}{
		"reads payload": {
			allow:   `payload.decision == "reject" || (payload.decision == "approve" && sender.identity.claims.team == "x")`,
			depends: true,
		},
		"does not read payload": {
			allow: `sender.identity.principal == "https://issuer.example#approver"`,
			may:   true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := runningFake(t)
			fake.describe.WorkflowExecutionInfo.Memo.Fields[signalPolicyMemoKey] =
				memoWithSignalPolicy(t, map[string]*v1.SignalPolicy{"go": {Allow: tc.allow}}).
					GetWorkflowExecutionInfo().GetMemo().GetFields()[signalPolicyMemoKey]
			fake.progress = &v1.RunProgress{PendingWaits: []*v1.PendingWait{
				{StepId: "approve", SignalName: "go", Prompt: "approve?"},
			}}
			s := mustNew(t, fake)

			ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
				Issuer: "https://issuer.example", Subject: "approver", Actions: []string{"workload.signal"},
			})
			resp, err := s.GetGate(ctx, gateRequest("go"))
			require.NoError(t, err)

			require.Equal(t, tc.depends, resp.Msg.GetDependsOnPayload())
			require.Equal(t, tc.may, resp.Msg.GetMayAnswer(), "undecided is never advertised as answerable")
		})
	}
}
