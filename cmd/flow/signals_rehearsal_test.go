package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #349's flag half: `flow run local` asserting who a `--signal` delivery is
// from, so a gate whose `signals:` policy names an approver is reachable in a
// rehearsal at all.
//
// The driver agreement itself is pinned in pkg/flowstate/v1/internal/conformance - these are
// about the command: what the flags parse to, that a rehearsal is announced
// rather than slipped past, and that the two refusals an author is most likely
// to meet say what to do about them.

// policedGateWorkflow is a gate declaring the shape
// examples/approval-gate/workflow.yaml declares: one rule ANDing a qualified
// subject and a claim, plus separation of duties.
func policedGateWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Signals: map[string]*v1.SignalPolicy{
			"deploy-approved": {
				Allow: []*v1.SignalPolicyRule{{
					Subject: v1.QualifiedSubject("https://issuer.example.com", "sre-lead@example.com"),
					Claims:  map[string]string{"team": "release-managers"},
				}},
				DistinctFromStarter: true,
			},
		},
		Steps: []*v1.Node{
			{Id: "approval", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
			}}},
		},
	}
}

// rehearsingAs sets the --signal-as-* flags on a `run local` command.
func rehearsingAs(t *testing.T, subject, issuer string, claims ...string) *cobra.Command {
	t.Helper()

	cmd := localSignalsTestCommand(t)
	if subject != "" {
		require.NoError(t, cmd.Flags().Set("signal-as-subject", subject))
	}
	if issuer != "" {
		require.NoError(t, cmd.Flags().Set("signal-as-issuer", issuer))
	}
	for _, claim := range claims {
		require.NoError(t, cmd.Flags().Set("signal-as-claim", claim))
	}

	return cmd
}

// TestRehearsedSignalReachesAPolicedGate is the positive direction: the
// approver the policy names opens the gate, and the delivery still reports
// itself as a rehearsal.
func TestRehearsedSignalReachesAPolicedGate(t *testing.T) {
	t.Parallel()

	cmd := rehearsingAs(t, "sre-lead@example.com", "https://issuer.example.com", "team=release-managers")

	out := &strings.Builder{}
	cmd.SetErr(out)

	stdout := &strings.Builder{}
	cmd.SetOut(stdout)

	ctx, err := withLocalSignals(t.Context(), cmd, policedGateWorkflow(), nil, []string{
		`deploy-approved={"approved": true}`,
	})
	require.NoError(t, err,
		"the approver this workflow's own signals: policy names was refused, so the gate is "+
			"still unreachable locally")

	waiter, ok := v1.SignalWaiterFromContext(ctx)
	require.True(t, ok)

	payload, sender, err := waiter.WaitForSignal(t.Context(), "deploy-approved")
	require.NoError(t, err)
	require.True(t, payload.GetNamedValues()["approved"].GetLiteral().GetBoolValue())

	require.Equal(t, "sre-lead@example.com", sender.GetIdentity().GetSubject())
	require.True(t, sender.GetLocal(),
		"a rehearsed sender was not marked local, so a local run's gate output would read "+
			"exactly like an attested production one")

	require.Contains(t, out.String(), "rehearsing --signal deliveries as",
		"a run standing in for an approver said nothing about it; a rehearsal identity that is "+
			"not visible in the output is one an author can mistake for a real approval")

	require.Empty(t, stdout.String(),
		"the rehearsal notice reached stdout, which is the run's single result document; "+
			"`-o json | jq` would receive prose ahead of the JSON")
	require.Contains(t, out.String(), "https://issuer.example.com#sre-lead@example.com")
}

// TestRehearsedSignalFromTheWrongApproverIsRefused is the direction that makes
// the one above worth having, and the one CLAUDE.md's "test that A cannot reach
// B" rule is about: satisfying the claim is not satisfying the rule.
func TestRehearsedSignalFromTheWrongApproverIsRefused(t *testing.T) {
	t.Parallel()

	cmd := rehearsingAs(t, "someone-else@example.com", "https://issuer.example.com", "team=release-managers")

	_, err := withLocalSignals(t.Context(), cmd, policedGateWorkflow(), nil, []string{
		`deploy-approved={"approved": true}`,
	})
	require.Error(t, err, "a rehearsal admitted an approver production's own policy refuses")
	require.Contains(t, err.Error(), "does not match any rule")
	require.Contains(t, err.Error(), "PermissionDenied",
		"the refusal does not say that production refuses this the same way, which is the "+
			"whole thing a rehearsal is for")
}

// TestRehearsedSignalFromTheStarterIsRefused is #349's separation-of-duties
// requirement: the run's own starter comes from --as-subject/--as-issuer, and a
// rehearsal sender equal to it is refused exactly as production refuses an
// approver approving their own request.
func TestRehearsedSignalFromTheStarterIsRefused(t *testing.T) {
	t.Parallel()

	cmd := rehearsingAs(t, "sre-lead@example.com", "https://issuer.example.com", "team=release-managers")

	// This rehearsal's own starter, made the same person as its approver.
	require.NoError(t, cmd.Flags().Set("as-subject", "sre-lead@example.com"))
	require.NoError(t, cmd.Flags().Set("as-issuer", "https://issuer.example.com"))

	_, err := withLocalSignals(t.Context(), cmd, policedGateWorkflow(), nil, []string{
		`deploy-approved={"approved": true}`,
	})
	require.Error(t, err,
		"an approver approving their own run was admitted locally, so distinct_from_starter: "+
			"cannot be rehearsed at all")
	require.Contains(t, err.Error(), "distinct from the run's own starter")
}

// TestUnrehearsedSignalIsStillRefusedAndSaysWhy pins the pre-#349 behavior,
// which is still the default: a delivery naming nobody is refused by a policy
// naming somebody. What is new is that the refusal now names the way out.
func TestUnrehearsedSignalIsStillRefusedAndSaysWhy(t *testing.T) {
	t.Parallel()

	_, err := withLocalSignals(t.Context(), localSignalsTestCommand(t), policedGateWorkflow(), nil, []string{
		`deploy-approved={"approved": true}`,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--signal-as-subject",
		"the refusal does not tell an author how to rehearse the gate they just failed to reach")
}

// TestRehearsalSenderFlagsAreReadTogether covers the malformed spellings, each
// reported as itself rather than as a gate that mysteriously never opens.
func TestRehearsalSenderFlagsAreReadTogether(t *testing.T) {
	t.Parallel()

	t.Run("no flags at all stands in for nobody", func(t *testing.T) {
		t.Parallel()

		sender, err := rehearsalSignalSender(localSignalsTestCommand(t), 1)
		require.NoError(t, err)
		require.False(t, v1.IsRehearsalSignalSender(sender),
			"a run naming no approver asserted one anyway")
		require.True(t, sender.GetLocal())
	})

	t.Run("a subject without its issuer", func(t *testing.T) {
		t.Parallel()

		_, err := rehearsalSignalSender(rehearsingAs(t, "sre-lead@example.com", ""), 1)
		require.Error(t, err, "a bare subject was accepted, which no allow: rule can ever match")
		require.Contains(t, err.Error(), "given together or not at all")
	})

	t.Run("an issuer without its subject", func(t *testing.T) {
		t.Parallel()

		_, err := rehearsalSignalSender(rehearsingAs(t, "", "https://issuer.example.com"), 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "given together or not at all")
	})

	t.Run("a malformed claim", func(t *testing.T) {
		t.Parallel()

		_, err := rehearsalSignalSender(
			rehearsingAs(t, "sre-lead@example.com", "https://issuer.example.com", "team"), 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "want NAME=VALUE")
	})

	t.Run("a repeated claim", func(t *testing.T) {
		t.Parallel()

		_, err := rehearsalSignalSender(
			rehearsingAs(t, "sre-lead@example.com", "https://issuer.example.com",
				"team=release-managers", "team=everyone"), 1)
		require.Error(t, err, "one of two values for a claim was silently dropped")
		require.Contains(t, err.Error(), "duplicate")
	})

	t.Run("an approver for a run that delivers nothing", func(t *testing.T) {
		t.Parallel()

		_, err := rehearsalSignalSender(
			rehearsingAs(t, "sre-lead@example.com", "https://issuer.example.com"), 0)
		require.Error(t, err,
			"naming an approver with no --signal to deliver did nothing and said nothing")
		require.Contains(t, err.Error(), "no --signal")
	})
}
