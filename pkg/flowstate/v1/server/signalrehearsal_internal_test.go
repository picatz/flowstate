package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The durable half of #349's shared table. The local half is
// `TestRehearsalSignalCasesLocally` in the v1 package, which runs these exact
// cases through [v1types.LocalSignals.DeliverFrom].
//
// Written against [FlowstateServer.authorizeSignal] rather than through a real
// Temporal server because that *is* the durable enforcement point: a signal is
// authorized here, before Temporal ever sees it, and the workflow side never
// decides it at all. Going through the SDK would exercise the same function
// with a run's worth of scaffolding in front of it and no more evidence.

// runWithPolicyAndStarter builds the Describe response a run governed by policy
// and started by starter carries - both memo entries this server writes at
// submit, in the wire shape [signalPolicyMemoEntry] and [starterMemoEntry] write
// them.
func runWithPolicyAndStarter(t *testing.T, c conformance.RehearsalSignalCase) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	// The policy half is the same builder every other authorizeSignal case
	// uses, so a case here and a case there disagree about nothing but the
	// starter this one adds.
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{c.SignalName: c.Policy})

	// Recorded exactly as [starterMemoEntry] writes it: one qualified
	// "issuer#subject" string, which is the only shape a memo ever held one as.
	starter, err := converter.GetDefaultDataConverter().ToPayload(
		v1types.QualifiedSubject(c.Starter.GetIssuer(), c.Starter.GetSubject()))
	require.NoError(t, err)

	resp.GetWorkflowExecutionInfo().GetMemo().GetFields()[starterMemoKey] = starter

	return resp
}

// TestRehearsalSignalCasesDurably runs the shared table through the durable
// enforcement point, so that the verdict a `flow run local` rehearsal reaches
// is the verdict `flow signal` reaches in production - the whole claim #349
// makes about rehearsing a policed gate.
func TestRehearsalSignalCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertRehearsalSignalCases(t, func(t testing.TB, c conformance.RehearsalSignalCase) error {
		// The sender the server attests: the case's identity, established by
		// authentication rather than asserted by a command, and never marked
		// local. An identity-less case is an authenticated caller a deployment
		// configured no identity provider for, which is the durable shape of
		// "stands in for nobody".
		identity := c.Sender
		if identity == nil {
			identity = &v1types.WorkloadIdentity{}
		}

		return mustNew(t, nil).authorizeSignal(
			runWithPolicyAndStarter(t.(*testing.T), c), c.SignalName,
			&v1types.SignalSender{Identity: identity})
	})
}

// TestRehearsalSenderIsNeverAuthorizedDurably is the negative direction, and
// the one that makes the rehearsal marker mean something: a sender carrying it
// is refused here whatever identity it holds, including the identity this run's
// own policy would otherwise admit.
//
// Nothing on this path constructs such a sender today - both are built from
// [FlowstateServer.identityFor] with `local` left false, and
// [v1types.SignalRequest] has no field a caller could set it through. That is
// why the refusal is asserted rather than assumed: "a local rehearsal never
// authorizes a durable run" is a rule this driver enforces, not a fact about
// which constructors happen to exist.
func TestRehearsalSenderIsNeverAuthorizedDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertRehearsalSenderIsNeverAuthorizedDurably(t,
		func(t testing.TB, c conformance.RehearsalSignalCase, sender *v1types.SignalSender) error {
			return mustNew(t, nil).authorizeSignal(
				runWithPolicyAndStarter(t.(*testing.T), c), c.SignalName, sender)
		})
}

// TestRehearsalSenderIsRefusedEvenWithNoPolicyDeclared covers the arm the
// shared table cannot reach, because every case there declares a policy: a
// signal name nobody wrote a policy for is the zero case, allowed for any
// authenticated caller - and a rehearsal identity is still refused, because the
// shape is wrong whatever the run declared.
func TestRehearsalSenderIsRefusedEvenWithNoPolicyDeclared(t *testing.T) {
	t.Parallel()

	err := mustNew(t, nil).authorizeSignal(memoWithNoSignalPolicy(), "deploy-approved",
		v1types.RehearsalSignalSender(&v1types.WorkloadIdentity{
			Subject: "sre-lead@example.com",
			Issuer:  "https://issuer.example.com",
		}))

	require.Error(t, err,
		"an unpoliced signal accepted a local rehearsal identity; the zero case widens which "+
			"senders a policy admits, never which kinds of sender this path accepts at all")
	require.Contains(t, err.Error(), "local rehearsal identity")
}
