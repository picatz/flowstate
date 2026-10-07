package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalQuorumCasesLocally is the local driver's half of the shared
// `quorum:` table; the durable half is `engine`'s TestSignalQuorumCasesDurably,
// which runs the identical cases through the Temporal test environment.
//
// A delivery with no `After` is queued before the run starts, which is the local
// spelling of "already buffered". One with a delay is sent from a timer while the
// gate is parked, which is what exercises the receive loop; the timers are
// stopped when the case ends so none outlives it.
func TestSignalQuorumCasesLocally(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalQuorumCases(t, func(t *testing.T, c conformance.SignalQuorumCase) (*v1.Workflow_StepOutputs, error) {
		signals := v1.NewLocalSignals()

		deliver := func(d conformance.QuorumDelivery) error {
			payload := &v1.Node_Outputs{NamedValues: d.Payload}
			if d.Sender == nil {
				return signals.Deliver(c.SignalName, payload)
			}

			return signals.DeliverFrom(c.SignalName, payload, d.Sender)
		}

		for _, d := range c.Deliveries {
			if d.After > 0 {
				timer := time.AfterFunc(d.After, func() { _ = deliver(d) })
				t.Cleanup(func() { timer.Stop() })

				continue
			}
			require.NoError(t, deliver(d))
		}

		return v1.Run(v1.NewContextWithSignalWaiter(t.Context(), signals), c.Workflow)
	})
}

// quorumGate is a two-approver gate over one signal, bounded so a run nobody
// answers still ends.
func quorumGate(timeout time.Duration) *v1.Workflow {
	return &v1.Workflow{
		Name: "policed-quorum",
		Signals: map[string]*v1.SignalPolicy{
			"release-approved": {Allow: `(sender.identity.principal == "https://idp.example#alice") || (sender.identity.principal == "https://idp.example#bob")`},
		},
		Steps: []*v1.Node{{
			Id: "gate",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
					Name:   "release-approved",
					Quorum: &v1.SignalQuorum{Approve: 2},
					Outputs: map[string]*v1.Value{
						"decision":  v1.NewExpr(v1.DecisionOutput),
						"approvers": v1.NewExpr("approvals.map(a, a.sender.identity.subject)"),
					},
				}},
				Timeout: durationpb.New(timeout),
			}},
		}},
	}
}

// TestAVetoFromAnUnadmittedSenderNeverReachesAQuorumLocally is the consequence a
// quorum cares about in the admission rule [conformance.RehearsalSignalCases]
// pins: a delivery the `signals:` policy refuses is never queued, so it cannot
// veto however it is worded. mallory's rejection is refused at the door and the
// two admitted approvers complete the quorum as though it had not been sent.
//
// The durable twin is in the server package, against [FlowstateServer]'s own
// admission, which is where a durable delivery is refused.
func TestAVetoFromAnUnadmittedSenderNeverReachesAQuorumLocally(t *testing.T) {
	t.Parallel()

	wf := quorumGate(time.Minute)
	signals := v1.NewPolicedLocalSignals(wf.GetSignals(), &v1.WorkloadIdentity{}, true, nil)

	sender := func(subject string) *v1.SignalSender {
		return &v1.SignalSender{Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Subject: subject, Issuer: "https://idp.example"}}}
	}
	payload := func(approved bool) *v1.Node_Outputs {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(approved)}}
	}

	err := signals.DeliverFrom("release-approved", payload(false), sender("mallory"))
	require.Error(t, err, "a veto from a sender the policy does not admit was delivered")

	require.NoError(t, signals.DeliverFrom("release-approved", payload(true), sender("alice")))
	require.NoError(t, signals.DeliverFrom("release-approved", payload(true), sender("bob")))

	outputs, err := v1.Run(v1.NewContextWithSignalWaiter(t.Context(), signals), wf)
	require.NoError(t, err)

	gate := outputs.GetStepValues()["gate"].GetNamedValues()
	assert.Equal(t, v1.QuorumApproved, gate["decision"].GetLiteral().GetStringValue())
	assert.Equal(t, []string{"alice", "bob"}, stringsOf(gate["approvers"]))
}

// TestAQuorumDecidedFromTheQueueNeverRegistersADeadline proves the quorum does
// not spend a clock it never needed: a gate that decides from what is already
// queued must not register a deadline, as the single wait's own arm guarantees,
// so a run under a virtual clock that is never advanced still finishes, and
// finishes at the moment it started.
func TestAQuorumDecidedFromTheQueueNeverRegistersADeadline(t *testing.T) {
	t.Parallel()

	wf := quorumGate(time.Hour)
	signals := v1.NewLocalSignals()

	for _, subject := range []string{"alice", "bob"} {
		require.NoError(t, signals.DeliverFrom("release-approved",
			&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
			&v1.SignalSender{Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Subject: subject, Issuer: "https://idp.example"}}}))
	}

	clock := v1.NewVirtualClock(time.Unix(0, 0))
	ctx := v1.NewContextWithSignalWaiter(v1.NewContextWithClock(t.Context(), clock), signals)

	outputs, err := v1.Run(ctx, wf)
	require.NoError(t, err)

	assert.Equal(t, v1.QuorumApproved,
		outputs.GetStepValues()["gate"].GetNamedValues()["decision"].GetLiteral().GetStringValue())
	assert.Equal(t, time.Unix(0, 0), clock.Now(),
		"a gate answered from what was already queued moved the run's clock to a deadline it never spent")
}

// stringsOf reads a literal list of strings.
func stringsOf(value *v1.Value) []string {
	var out []string
	for _, item := range value.GetLiteral().GetListValue().GetValues() {
		out = append(out, item.GetStringValue())
	}

	return out
}
