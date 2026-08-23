package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRehearsalSignalCasesLocally is the local driver's half of #349's shared
// table - the durable half is `server`'s own
// TestRehearsalSignalCasesDurably, which runs the identical cases through
// `authorizeSignal`.
//
// The enforcement point here is [v1.LocalSignals.DeliverFrom], which checks a
// delivery before it is ever queued: a refused signal is never seen by the
// waiting step, exactly as a refused `flow signal` never reaches the workflow
// durably.
func TestRehearsalSignalCasesLocally(t *testing.T) {
	t.Parallel()

	conformance.AssertRehearsalSignalCases(t, func(t testing.TB, c conformance.RehearsalSignalCase) error {
		signals := v1.NewPolicedLocalSignals(
			map[string]*v1.SignalPolicy{c.SignalName: c.Policy}, c.Starter, true)

		// The two shapes `flow run local` delivers as, chosen the same way it
		// chooses: a case naming nobody is the plain unattested delivery every
		// local run made before --signal-as-subject existed.
		sender := v1.LocalSignalSender()
		if c.Sender != nil {
			sender = v1.RehearsalSignalSender(c.Sender)
		}

		return signals.DeliverFrom(c.SignalName, &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		}, sender)
	})
}

// TestRehearsedApprovalOpensAGateAndSaysItWasRehearsed is the whole feature in
// one run: a gate whose policy names an approver opens for a delivery standing
// in for that approver, the gated step runs, and the answer says plainly that
// nobody authenticated any of it.
//
// The delivery check alone is not this. A policy can be satisfied and the gate
// still be unreachable - which is exactly the state #349 found the local driver
// in - so this asserts what an author actually sees: the step that was gated
// ran, and `sender` names the rehearsed approver while `sender.local` still
// reads true.
func TestRehearsedApprovalOpensAGateAndSaysItWasRehearsed(t *testing.T) {
	t.Parallel()

	policy := map[string]*v1.SignalPolicy{
		"deploy-approved": {
			Allow: []*v1.SignalPolicyRule{{
				Subject: v1.QualifiedSubject("https://issuer.example.com", "sre-lead@example.com"),
				Claims:  map[string]string{"team": "release-managers"},
			}},
			DistinctFromStarter: true,
		},
	}

	// The starter `flow run local` records from --as-subject/--as-issuer,
	// which is what distinct_from_starter compares the sender against.
	starter := &v1.WorkloadIdentity{Subject: "local-user", Issuer: "flowstate:local"}

	signals := v1.NewPolicedLocalSignals(policy, starter, true)

	require.NoError(t, signals.DeliverFrom("deploy-approved",
		&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
		v1.RehearsalSignalSender(&v1.WorkloadIdentity{
			Subject: "sre-lead@example.com",
			Issuer:  "https://issuer.example.com",
			Claims:  map[string]string{"team": "release-managers"},
		})))

	ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

	outputs, err := v1.Run(ctx, gatedLocalWorkflow(time.Minute))
	require.NoError(t, err)

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval)
	require.True(t, payloadField(t, approval, "approved").GetBoolValue())
	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the policed gate did not open for the approver its own policy names, so a rehearsal "+
			"can still only reach the refusal")

	sender := senderFields(t, approval)

	require.Equal(t, "sre-lead@example.com", sender["subject"],
		"the gate reported no approver, so a workflow reading sender.identity for its audit "+
			"trail rehearses something production will not do")
	require.Equal(t, "true", sender["local"],
		"a rehearsed sender reported itself as attested; `!sender.local` is what a workflow "+
			"author trusts to mean \"the server accepted this\", and nothing accepted this")
}

// TestRehearsedApprovalIsRefusedWhenItIsTheStarter is requirement 4 of #349 at
// the driver level: `distinct_from_starter:` is checked against the local run's
// own starter, so rehearsing an approver who is that starter is refused here
// for the reason production refuses it.
//
// The gate then lapses rather than erroring, which is the other half of the
// agreement: a refused signal is not an error the workflow sees, on either
// driver - it is a signal the workflow never learns was sent.
func TestRehearsedApprovalIsRefusedWhenItIsTheStarter(t *testing.T) {
	t.Parallel()

	starter := &v1.WorkloadIdentity{Subject: "sre-lead@example.com", Issuer: "https://issuer.example.com"}

	signals := v1.NewPolicedLocalSignals(map[string]*v1.SignalPolicy{
		"deploy-approved": {
			Allow: []*v1.SignalPolicyRule{{
				Subject: v1.QualifiedSubject("https://issuer.example.com", "sre-lead@example.com"),
			}},
			DistinctFromStarter: true,
		},
	}, starter, true)

	err := signals.DeliverFrom("deploy-approved",
		&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
		v1.RehearsalSignalSender(&v1.WorkloadIdentity{
			Subject: "sre-lead@example.com",
			Issuer:  "https://issuer.example.com",
		}))
	require.Error(t, err,
		"an approver approving their own run was admitted locally, which production refuses")
	require.Contains(t, err.Error(), "distinct from the run's own starter")

	ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

	// The condition a gate that may lapse is written with - the outcome the
	// wait always reports, rather than a payload key that exists only if
	// somebody sent one (see [TestLocalSignalTimeoutLeavesPayloadKeysAbsent]).
	workflow := gatedLocalWorkflow(50 * time.Millisecond)
	workflow.Steps[2].Condition = v1.NewExpr("!approval.timed_out")

	outputs, err := v1.Run(ctx, workflow)
	require.NoError(t, err, "a refused signal must not fail the run; the gate simply lapses")

	approval := outputs.GetStepValues()["approval"]
	require.True(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
		"the refused delivery reached the waiting step anyway")
	require.Nil(t, outputs.GetStepValues()["deploy"])
}

// TestRehearsalSenderIsDistinguishableFromEveryOtherSender pins the three
// shapes apart, because the whole design rests on them being distinguishable
// by shape rather than by convention: nobody-attested-and-claims-nobody,
// nobody-attested-and-stands-in-for-somebody, and attested.
func TestRehearsalSenderIsDistinguishableFromEveryOtherSender(t *testing.T) {
	t.Parallel()

	rehearsal := v1.RehearsalSignalSender(&v1.WorkloadIdentity{Subject: "sre-lead@example.com"})
	require.True(t, rehearsal.GetLocal(),
		"a rehearsal sender that is not marked local is indistinguishable from an attested one")
	require.True(t, v1.IsRehearsalSignalSender(rehearsal))

	require.False(t, v1.IsRehearsalSignalSender(v1.LocalSignalSender()),
		"a delivery standing in for nobody was read as a rehearsal of somebody")

	attested := &v1.SignalSender{Identity: &v1.WorkloadIdentity{Subject: "sre-lead@example.com"}}
	require.False(t, v1.IsRehearsalSignalSender(attested),
		"an attested production sender was read as a local rehearsal")
}

// senderFields flattens a wait's rendered `sender` output, with the identity's
// own fields lifted alongside it - every value there is a scalar, and the two
// levels never collide on a key.
func senderFields(t *testing.T, outputs *v1.Node_Outputs) map[string]string {
	t.Helper()

	fields := map[string]string{}

	for _, entry := range outputs.GetNamedValues()[v1.SenderOutput].GetLiteral().GetMapValue().GetEntries() {
		key := entry.GetKey().GetStringValue()
		switch key {
		case "identity":
			for _, field := range entry.GetValue().GetMapValue().GetEntries() {
				fields[field.GetKey().GetStringValue()] = field.GetValue().GetStringValue()
			}
		case "local":
			// The one boolean in the mapping, and the one this file is about:
			// rendered as a word here so a failure prints what it read.
			fields[key] = "false"
			if entry.GetValue().GetBoolValue() {
				fields[key] = "true"
			}
		default:
			fields[key] = entry.GetValue().GetStringValue()
		}
	}

	return fields
}
