package conformance

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The question a `quorum:` asks of both drivers: given these deliveries, from
// these senders, in this order, how does a `wait_for_signals:` decide?
//
// # Why a table of its own beside [SignalBatchCases]
//
// A quorum replaces the drain's shape, so its expectations are about decisions
// (who counted, who vetoed, what was left on the channel) rather than about how
// many arrived, and every one of them depends on *who sent* each delivery, which
// [SignalBatchCase] has no way to say. It follows that table's shape otherwise:
// the case names what is sent, each driver's caller sends it the way that driver
// sends things, and the assertion about what the run produced is shared.
//
// The counting is one function ([v1.QuorumTally]) that both drivers call, so
// what this table can find is a driver handing it the wrong deliveries: taking
// more than the decision needed, taking them in another order, or answering a
// lapsed gate differently. Each case below is written against one of those.
//
// # What is deliberately not here
//
// Admission. A delivery from a sender a `signals:` policy refuses never reaches
// a wait on either driver (`SignalPolicyCheck` runs at the `Signal` RPC durably
// and at [v1.LocalSignals.DeliverFrom] locally), so it can neither approve nor
// veto, and that is [RehearsalSignalCases]' rule rather than this table's.
// `TestAVetoFromAnUnadmittedSenderNeverReachesAQuorumLocally` and its durable
// twin in the server package pin the consequence a quorum cares about.

// QuorumDelivery is one delivery to a quorum case's channel.
type QuorumDelivery struct {
	// Sender is who the engine attests sent it. Nil is an unattested delivery:
	// no identity, which a distinct quorum never counts.
	Sender *v1.SignalSender

	// Payload is what the sender sent.
	Payload map[string]*v1.Value

	// After is how long after the run starts the delivery is sent. Zero buffers
	// it before the gate is reached, which is the shape [SignalBatchCases]
	// documents; a positive value arrives while the gate is parked, which is the
	// shape a quorum is *for* and the one that exercises the receive loop.
	//
	// Virtual time durably and real time locally, so the cases that use it keep
	// every margin in the tens of milliseconds at least.
	After time.Duration
}

// SignalQuorumCase is one sequence of deliveries and what both drivers must
// produce from it.
type SignalQuorumCase struct {
	// Name says what the case is about, and becomes the subtest name.
	Name string

	// Why says what the case is pinning, and is what a failure reports.
	Why string

	// Workflow is what runs. Every case writes a `wait_for_signals:` with a
	// `quorum:` and shapes its outputs through [quorumStep], for the reason
	// [drainStep] shapes: a sender's rendering is a shape, and transcribing it
	// into every expectation would be a second definition of it.
	Workflow *v1.Workflow

	// SignalName is the channel every delivery is addressed to.
	SignalName string

	// Deliveries are sent in written order, at their own times.
	Deliveries []QuorumDelivery

	// ExpectedOutputs is what the run must produce, compared whole.
	ExpectedOutputs *v1.Workflow_StepOutputs
}

// quorumSender builds an attested sender the way both harnesses can deliver.
func quorumSender(subject string) *v1.SignalSender {
	return &v1.SignalSender{Identity: &v1.WorkloadIdentity{Subject: subject, Issuer: "https://idp.example"}}
}

// approval and rejection are the two payloads the browser gate page sends.
func approval(id string) map[string]*v1.Value {
	return map[string]*v1.Value{"approved": v1.NewLiteral(true), "id": v1.NewLiteral(id)}
}

func rejection(id string) map[string]*v1.Value {
	return map[string]*v1.Value{"approved": v1.NewLiteral(false), "id": v1.NewLiteral(id)}
}

// quorumStep builds a `wait_for_signals:` with quorum, shaping what every case
// asserts: how it ended, who counted, who vetoed, how many deliveries it took,
// and whether it lapsed.
func quorumStep(id, signal string, quorum *v1.SignalQuorum, timeout time.Duration) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
				Name:   signal,
				Quorum: quorum,
				Outputs: map[string]*v1.Value{
					"decision":        v1.NewExpr(v1.DecisionOutput),
					"approvers":       v1.NewExpr("approvals.map(a, a.sender.identity.subject)"),
					"vetoed_by":       v1.NewExpr(`decision == "vetoed" ? vetoed_by.identity.subject : ""`),
					"taken":           v1.NewExpr(v1.CountOutput),
					v1.TimedOutOutput: v1.NewExpr(v1.TimedOutOutput),
				},
			}},
			Timeout: durationpb.New(timeout),
		}},
	}
}

// quorumOutputs is the expected outputs of a [quorumStep].
func quorumOutputs(decision string, approvers []any, vetoedBy string, taken int64, timedOut bool) *v1.Node_Outputs {
	return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"decision":        v1.NewLiteral(decision),
		"approvers":       v1.NewLiteralList(approvers...),
		"vetoed_by":       v1.NewLiteral(vetoedBy),
		"taken":           v1.NewLiteral(taken),
		v1.TimedOutOutput: v1.NewLiteral(timedOut),
	}}
}

// SignalQuorumCases exercise a `wait_for_signals:` `quorum:` on both drivers.
func SignalQuorumCases() []SignalQuorumCase {
	const signal = "release-approved"

	var (
		alice = quorumSender("alice")
		bob   = quorumSender("bob")
		carol = quorumSender("carol")
	)

	// `rest` takes whatever the quorum left on the channel, so a case can say
	// what a decision did *not* consume.
	rest := func() *v1.Node { return drainStep("rest", signal, 0, 20*time.Millisecond) }

	two := func() *v1.SignalQuorum { return &v1.SignalQuorum{Approve: 2} }

	restOf := func(ids ...any) *v1.Node_Outputs {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"ids":             v1.NewLiteralList(ids...),
			"taken":           v1.NewLiteral(int64(len(ids))),
			v1.TimedOutOutput: v1.NewLiteral(false),
		}}
	}

	return []SignalQuorumCase{
		{
			Name:       "two approvers complete the quorum and a later approval is not consumed",
			SignalName: signal,
			Why: "the wait ends on the delivery that decides it: a driver that drained the " +
				"channel as `wait_for_signals:` does would take carol's approval too, and the " +
				"next wait on the name would never see it",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
				{Sender: bob, Payload: approval("b")},
				{Sender: carol, Payload: approval("c")},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-completes",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), time.Minute), rest()},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumApproved, []any{"alice", "bob"}, "", 2, false),
				"rest": restOf("c"),
			}},
		},
		{
			Name:       "approvals that arrive while the gate is parked are counted as they come",
			SignalName: signal,
			Why: "the receive loop, rather than a drain: the gate parks with nothing, takes " +
				"alice, stays parked on one of two, and ends on bob",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a"), After: 20 * time.Millisecond},
				{Sender: bob, Payload: approval("b"), After: 40 * time.Millisecond},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-parked",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumApproved, []any{"alice", "bob"}, "", 2, false),
			}},
		},
		{
			Name:       "a repeat approval from the same sender is ignored",
			SignalName: signal,
			Why: "distinct is the default: alice approving twice is one approval, so the " +
				"quorum of two completes on bob and names alice and bob, not alice twice",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
				{Sender: alice, Payload: approval("a2")},
				{Sender: bob, Payload: approval("b")},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-distinct",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				// All three deliveries were taken (the repeat was examined and
				// ignored), but only two counted.
				"gate": quorumOutputs(v1.QuorumApproved, []any{"alice", "bob"}, "", 3, false),
			}},
		},
		{
			Name:       "distinct false counts a repeat",
			SignalName: signal,
			Why:        "the opt-out is real: one sender approving twice meets a quorum of two",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
				{Sender: alice, Payload: approval("a2")},
			},
			Workflow: &v1.Workflow{
				Name: "quorum-not-distinct",
				Steps: []*v1.Node{quorumStep("gate", signal,
					&v1.SignalQuorum{Approve: 2, Distinct: new(false)}, time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumApproved, []any{"alice", "alice"}, "", 2, false),
			}},
		},
		{
			Name:       "an unattested delivery never counts toward a distinct quorum",
			SignalName: signal,
			Why: "a delivery with no verified identity has nothing to be distinct by, so it " +
				"fails closed: two anonymous approvals are not two approvers",
			Deliveries: []QuorumDelivery{
				{Payload: approval("x")},
				{Payload: approval("y")},
				{Sender: alice, Payload: approval("a")},
				{Sender: bob, Payload: approval("b")},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-unattested",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumApproved, []any{"alice", "bob"}, "", 4, false),
			}},
		},
		{
			Name:       "a rejection vetoes at once, whatever has been approved",
			SignalName: signal,
			Why: "a veto ends the wait on its own delivery, names who vetoed, and leaves " +
				"what came after it for a later wait",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
				{Sender: bob, Payload: rejection("b")},
				{Sender: carol, Payload: approval("c")},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-vetoed",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), time.Minute), rest()},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumVetoed, []any{"alice"}, "bob", 2, false),
				"rest": restOf("c"),
			}},
		},
		{
			Name:       "a sender excluded from counting cannot approve",
			SignalName: signal,
			Why: "four-eyes: the requester's own approval is ignored, whether the exclusion " +
				"is an expression or a literal",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
				{Sender: bob, Payload: approval("b")},
				{Sender: carol, Payload: approval("c")},
			},
			Workflow: &v1.Workflow{
				Name: "quorum-exclude",
				Vars: map[string]*v1.Value{"requester": v1.NewLiteral("alice")},
				Steps: []*v1.Node{quorumStep("gate", signal, &v1.SignalQuorum{
					Approve: 2,
					Exclude: []*v1.Value{v1.NewExpr("vars.requester"), v1.NewLiteral("dave")},
				}, time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumApproved, []any{"bob", "carol"}, "", 3, false),
			}},
		},
		{
			Name:       "an excluded sender's rejection still vetoes",
			SignalName: signal,
			Why:        "exclusion removes an approval, not a voice",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: rejection("a")},
			},
			Workflow: &v1.Workflow{
				Name: "quorum-excluded-veto",
				Vars: map[string]*v1.Value{"requester": v1.NewLiteral("alice")},
				Steps: []*v1.Node{quorumStep("gate", signal, &v1.SignalQuorum{
					Approve: 2,
					Exclude: []*v1.Value{v1.NewExpr("vars.requester")},
				}, time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumVetoed, []any{}, "alice", 1, false),
			}},
		},
		{
			Name:       "a veto expression replaces the default, and a delivery that is neither is ignored",
			SignalName: signal,
			Why: "with `veto:` written, `approved: false` no longer vetoes (bob is ignored); " +
				"only the author's expression does (carol), and a non-boolean `approved` is " +
				"neither an approval nor a veto",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
				{Sender: bob, Payload: rejection("b")},
				{Sender: alice, Payload: map[string]*v1.Value{"approved": v1.NewLiteral("yes"), "id": v1.NewLiteral("a2")}},
				{Sender: carol, Payload: map[string]*v1.Value{
					"approved": v1.NewLiteral(true), "block": v1.NewLiteral(true), "id": v1.NewLiteral("c"),
				}},
			},
			Workflow: &v1.Workflow{
				Name: "quorum-custom-veto",
				Steps: []*v1.Node{quorumStep("gate", signal, &v1.SignalQuorum{
					Approve: 2,
					Veto:    v1.NewExpr("has(payload.block) && payload.block"),
				}, time.Minute)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumVetoed, []any{"alice"}, "carol", 4, false),
			}},
		},
		{
			Name:       "a quorum that lapses reports the approvals counted so far",
			SignalName: signal,
			Why: "the timeout is an ordinary outcome: decision timed_out with the partial " +
				"approvals, and `timed_out` true even though a delivery was taken",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a")},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-lapses",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), 40*time.Millisecond)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumTimedOut, []any{"alice"}, "", 1, true),
			}},
		},
		{
			Name:       "deliveries that decide nothing do not extend the deadline",
			SignalName: signal,
			Why: "the deadline is armed once, when the gate parks. Re-armed per delivery, " +
				"the third alice at 330ms would be taken (150ms + 200ms); fixed, the gate " +
				"lapses at 200ms and she is not",
			Deliveries: []QuorumDelivery{
				{Sender: alice, Payload: approval("a"), After: 50 * time.Millisecond},
				{Sender: alice, Payload: approval("a2"), After: 150 * time.Millisecond},
				{Sender: alice, Payload: approval("a3"), After: 330 * time.Millisecond},
			},
			Workflow: &v1.Workflow{
				Name:  "quorum-deadline-fixed",
				Steps: []*v1.Node{quorumStep("gate", signal, two(), 200*time.Millisecond)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": quorumOutputs(v1.QuorumTimedOut, []any{"alice"}, "", 2, true),
			}},
		},
	}
}

// AssertSignalQuorumCases runs every case through one driver's own way of
// delivering and running a workflow, and holds the result to the shared
// expectations.
//
// run sends every [QuorumDelivery] of the case, at its own time, and executes
// the workflow. A run that fails is reported as a failure, because none of these
// cases is about a run failing.
func AssertSignalQuorumCases(t *testing.T, run func(t *testing.T, c SignalQuorumCase) (*v1.Workflow_StepOutputs, error)) {
	t.Helper()

	for _, c := range SignalQuorumCases() {
		t.Run(c.Name, func(t *testing.T) {
			outputs, err := run(t, c)
			require.NoErrorf(t, err, "the run failed, and this case is about a decision\n  %s", c.Why)

			require.Truef(t, proto.Equal(c.ExpectedOutputs, outputs),
				"%s\n  %s\n%s", c.Name, c.Why,
				cmp.Diff(c.ExpectedOutputs, outputs, protocmp.Transform()))
		})
	}
}
