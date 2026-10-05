package server

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// heldGates stands in for what a run retains to answer [engine.GateQuery] from:
// more gates than its progress answer lists.
type heldGates struct {
	waits []*v1.PendingWait

	// incomplete is the run saying a miss is not proof.
	incomplete bool
}

// lookup answers the gate query as the engine's handler does.
func (h *heldGates) lookup(args []any) *v1.RunProgress {
	name, _ := args[0].(string)
	for _, wait := range h.waits {
		if wait.GetSignalName() == name {
			return &v1.RunProgress{PendingWaits: []*v1.PendingWait{proto.Clone(wait).(*v1.PendingWait)}}
		}
	}

	return &v1.RunProgress{PendingWaitsTruncated: h.incomplete}
}

// crowdedRun returns a fake run holding gates open gates, of which its progress
// answer lists only the first [v1.MaxPendingWaits], every one carrying a prompt.
func crowdedRun(t *testing.T, gates int, policy map[string]*v1.SignalPolicy) *fakeRunClient {
	t.Helper()

	fake := runningFake(t)
	if policy != nil {
		fake.describe.WorkflowExecutionInfo.Memo.Fields[signalPolicyMemoKey] =
			memoWithSignalPolicy(t, policy).GetWorkflowExecutionInfo().GetMemo().GetFields()[signalPolicyMemoKey]
		starter, err := converter.GetDefaultDataConverter().ToPayload("https://issuer.example#requester")
		require.NoError(t, err)
		fake.describe.WorkflowExecutionInfo.Memo.Fields[starterMemoKey] = starter
	}

	var all []*v1.PendingWait
	for i := range gates {
		all = append(all, &v1.PendingWait{
			StepId:     fmt.Sprintf("s%d", i),
			SignalName: fmt.Sprintf("n%d", i),
			Prompt:     fmt.Sprintf("approve %d?", i),
			Policed:    policy != nil,
		})
	}

	fake.progress = &v1.RunProgress{
		PendingWaits:          all[:min(len(all), v1.MaxPendingWaits)],
		PendingWaitsTruncated: len(all) > v1.MaxPendingWaits,
	}
	fake.held = &heldGates{waits: all}

	return fake
}

// TestGetGateAnswersForTheGatePastTheSummaryBound: a run holding more than
// [v1.MaxPendingWaits] gates answers for the 65th, for the principal the
// `signals:` policy admits, and refuses the one it does not without showing them
// the question; and a caller without `workload.signal` never reaches the run.
func TestGetGateAnswersForTheGatePastTheSummaryBound(t *testing.T) {
	t.Parallel()

	const gates = v1.MaxPendingWaits + 6

	policy := map[string]*v1.SignalPolicy{
		"n64": {Allow: `sender.identity.principal == "https://issuer.example#approver"`},
	}
	as := func(t *testing.T, subject string, actions ...string) context.Context {
		t.Helper()

		return auth.ContextWithPrincipal(t.Context(), auth.Principal{
			Issuer: "https://issuer.example", Subject: subject, Actions: actions,
		})
	}

	t.Run("admitted", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, gates, policy)
		s := mustNew(t, fake)

		resp, err := s.GetGate(as(t, "approver", "workload.signal"), gateRequest("n64"))
		require.NoError(t, err)
		require.Equal(t, "s64", resp.Msg.GetStepId())
		require.Equal(t, v1.PromptWithheldSensitive, resp.Msg.GetPrompt(), "shown to the admitted caller, redacted as Get redacts it (the fake run has no readable specification)")
		require.True(t, resp.Msg.GetMayAnswer())
		require.Equal(t, "https://issuer.example#requester", resp.Msg.GetStarter())
		require.Equal(t, 1, fake.gateQueries)
		require.Zero(t, fake.signals, "reading a gate delivers nothing")

		// The last one too, not only the first past the bound.
		resp, err = s.GetGate(as(t, "approver", "workload.signal"), gateRequest(fmt.Sprintf("n%d", gates-1)))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("s%d", gates-1), resp.Msg.GetStepId())
	})

	t.Run("refused by the policy", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, gates, policy)
		s := mustNew(t, fake)

		resp, err := s.GetGate(as(t, "bystander", "workload.signal"), gateRequest("n64"))
		require.NoError(t, err)
		require.Equal(t, "s64", resp.Msg.GetStepId(), "the gate is reported to a caller who holds workload.signal")
		require.False(t, resp.Msg.GetMayAnswer())
		require.Empty(t, resp.Msg.GetPrompt(), "the question is for the people the policy admits")
		require.Empty(t, resp.Msg.GetStarter())
		require.Zero(t, fake.signals)
	})

	t.Run("without workload.signal", func(t *testing.T) {
		t.Parallel()

		fake := crowdedRun(t, gates, policy)
		s := mustNew(t, fake)

		_, err := s.GetGate(as(t, "approver", "workload.read"), gateRequest("n64"))
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		require.Zero(t, fake.describes)
		require.Zero(t, fake.gateQueries, "the run was asked about a gate for a caller who may not read gates")
	})
}

// TestGetGateLookupSaysOnlyWhatItCannotProve: past the summary bound a name the
// run does not hold is NotFound, a name the run says it may not have retained is
// FailedPrecondition, and a run that cannot answer the lookup leaves the answer
// as it was before the lookup existed. Never "closed" on a guess.
func TestGetGateLookupSaysOnlyWhatItCannotProve(t *testing.T) {
	t.Parallel()

	const gates = v1.MaxPendingWaits + 6

	for name, tc := range map[string]struct {
		mutate func(*fakeRunClient)
		want   connect.Code
	}{
		"held and absent":     {want: connect.CodeNotFound},
		"held and incomplete": {mutate: func(f *fakeRunClient) { f.held.incomplete = true }, want: connect.CodeFailedPrecondition},
		"no lookup answered":  {mutate: func(f *fakeRunClient) { f.held = nil }, want: connect.CodeFailedPrecondition},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := crowdedRun(t, gates, nil)
			if tc.mutate != nil {
				tc.mutate(fake)
			}
			s := mustNew(t, fake)

			_, err := s.GetGate(revealer(t.Context(), "workload.signal"), gateRequest("absent"))
			require.Equal(t, tc.want, connect.CodeOf(err))
		})
	}
}

// TestGetGateLookupLeaksNothingBeyondTheGate: a gate found through the lookup is
// redacted exactly as one in the summary is, and the response carries only the
// gate's own fields, none of which can hold a step output or an input.
func TestGetGateLookupLeaksNothingBeyondTheGate(t *testing.T) {
	t.Parallel()

	const secret = "synthetic-token-4c21"

	// No readable specification, so every prompt must be withheld.
	fake := runningFake(t)
	var all []*v1.PendingWait
	for i := range v1.MaxPendingWaits + 6 {
		all = append(all, &v1.PendingWait{StepId: fmt.Sprintf("s%d", i), SignalName: fmt.Sprintf("n%d", i), Prompt: "approve " + secret + "?"})
	}
	fake.progress = &v1.RunProgress{PendingWaits: all[:v1.MaxPendingWaits], PendingWaitsTruncated: true}
	fake.held = &heldGates{waits: all}
	s := mustNew(t, fake)

	resp, err := s.GetGate(revealer(t.Context(), "workload.signal"), gateRequest("n64"))
	require.NoError(t, err)
	require.Equal(t, v1.PromptWithheldSensitive, resp.Msg.GetPrompt())

	wire, err := proto.Marshal(resp.Msg)
	require.NoError(t, err)
	require.NotContains(t, string(wire), secret)

	// The schema, not the run, decides what can be carried: every populated
	// field is one of the gate's own.
	allowed := []string{"workflow_id", "run_id", "step_id", "signal_name", "prompt", "prompt_truncated", "deadline", "starter", "may_answer", "approvals", "approvals_needed"}
	resp.Msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		require.True(t, slices.Contains(allowed, string(fd.Name())), "unexpected field %s", fd.Name())

		return true
	})
}
