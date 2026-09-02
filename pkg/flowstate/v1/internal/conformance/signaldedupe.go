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

// The one question a redelivery asks, asked of both drivers: this run has
// already taken this delivery at a gate — may it take it again?
//
// # Why this needs a table of its own
//
// [RehearsalSignalCases] answers who may deliver, and answers it before any
// delivery is buffered. This is the question after that one, and it is not a
// policy question at all: the sender is admitted, the payload is fine, and the
// delivery is genuine — it is simply the *same* delivery as one this run
// already consumed, which is the ordinary case for a transport that is
// at-least-once by nature.
//
// It is also the one place a wrong answer is invisible in production. A gate
// answered twice by one click looks exactly like a gate answered twice by two
// clicks, and the only party who could tell is the person who clicked once.
//
// # What the two cases pin, and why both are needed
//
// The claim is *one delivery answers one gate*, and it takes two shapes to
// state honestly:
//
//   - Two waits on one name, sequentially. The delivery id is what separates
//     them, so a driver that dropped by payload equality, or by "a gate already
//     answered", would pass one of the cases below and fail the other.
//   - A `loop:` around a single wait. This is the shape the bridge's own
//     redelivery hazard has (`examples/webhook-approval-bridge`): a replay
//     arriving after the gate resumed would answer the loop's next turn, which
//     is a stage approved by nobody. A dedupe that only worked for waits
//     written out separately would miss it.
//
// Each case ships a *distinct-ids* twin, and those twins are not padding: a
// driver that refused every second delivery would satisfy the deduplicating
// direction of both cases and nothing else. The pair is what makes "deduped by
// the id" a claim rather than "refused the second one".
//
// # Where each driver enforces it
//
// Durably, `executor.admitDelivery` over `RunState.consumed_delivery_ids`, at
// every point a delivery enters a wait — the carried signal, the early channel
// peek, the parked selector, and the batch's drain. Locally,
// `LocalSignals.admitLocked` over its own in-process set, at the matching
// points. Both call [v1.ConsumeDeliveryID], which is the whole of the sharing:
// one membership rule, two intakes.

// SignalDedupeDelivery is one delivery a case sends: what it carries, and the
// webhook delivery id it carries it under.
type SignalDedupeDelivery struct {
	// DeliveryID is [v1.SignalSender.delivery_id] — the digest the receiver
	// derives from a webhook's `idempotency_key:`. Two deliveries sharing one
	// are one delivery arriving twice.
	//
	// Empty is every sender that is not a webhook, and deduplicates nothing.
	DeliveryID string

	// Payload is what the delivery carries into the gate.
	Payload map[string]*v1.Value
}

// SignalDedupeCase is one sequence of deliveries and what both drivers must
// produce from it.
type SignalDedupeCase struct {
	// Name says what the case is about, and becomes the subtest name.
	Name string

	// Workflow is what runs.
	Workflow *v1.Workflow

	// SignalName is the channel every delivery is addressed to.
	SignalName string

	// Deliveries are sent, in order, before the run reaches its first gate —
	// the shape [SignalBatchCases] documents and for its reason: a case that
	// raced its deliveries against a wait would be testing the scheduler.
	Deliveries []SignalDedupeDelivery

	// ExpectedOutputs is what the run must produce, compared whole.
	ExpectedOutputs *v1.Workflow_StepOutputs

	// Why says what the case is pinning, and is what a failure reports.
	Why string
}

// answeredGate is a `wait_for_signal:` that reports only whether it was
// answered.
//
// Shaped rather than raw, for [SignalBatchCases]' reason: an unshaped wait
// renders its whole `sender` into the outputs, which would put a second
// definition of that shape into every expectation here.
// The bound is short on purpose. Every case here has its deliveries buffered
// before the run starts, so a gate that should be answered never waits at all
// and a gate that should lapse has nothing to wait for — the deadline is only
// how long the local driver spends discovering that, against a real clock.
func answeredGate(id, signal string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Timeout: durationpb.New(250 * time.Millisecond),
			Kind: &v1.Wait_Signal{Signal: &v1.Signal{
				Name: signal,
				Outputs: map[string]*v1.Value{
					"answered": v1.NewExpr("!timed_out"),
				},
			}},
		}},
	}
}

// SignalDedupeCases is the shared table.
func SignalDedupeCases() []SignalDedupeCase {
	const signal = "stage-approved"

	approval := map[string]*v1.Value{"approved": v1.NewLiteral(true)}

	// Two sequential gates, and what each of them reports.
	sequential := func(name string) *v1.Workflow {
		return &v1.Workflow{
			Name:  name,
			Steps: []*v1.Node{answeredGate("first", signal), answeredGate("second", signal)},
		}
	}
	sequentialOutputs := func(first, second bool) *v1.Workflow_StepOutputs {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"first":  {NamedValues: map[string]*v1.Value{"answered": v1.NewLiteral(first)}},
			"second": {NamedValues: map[string]*v1.Value{"answered": v1.NewLiteral(second)}},
		}}
	}

	// One gate inside a loop that keeps asking until a turn goes unanswered,
	// plus a step that counts the turns an answer actually reached. The count
	// is what the expectation reads: a loop's own `results` would restate every
	// iteration's shape in every case here.
	looped := func(name string) *v1.Workflow {
		return &v1.Workflow{
			Name: name,
			Steps: []*v1.Node{
				{
					Id: "stages",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						Until:         v1.NewExpr("!steps.stage.answered"),
						MaxIterations: 3,
						Body:          []*v1.Node{answeredGate("stage", signal)},
					}},
				},
				{
					Id: "answered",
					Kind: &v1.Node_Value{
						Value: v1.NewExpr("steps.stages.results.filter(r, r.stage.answered).size()"),
					},
				},
			},
		}
	}

	return []SignalDedupeCase{
		{
			Name:       "two deliveries with one id answer one gate",
			SignalName: signal,
			Workflow:   sequential("dedupe-answers-one-gate"),
			Deliveries: []SignalDedupeDelivery{
				{DeliveryID: "click-a", Payload: approval},
				{DeliveryID: "click-a", Payload: approval},
			},
			ExpectedOutputs: sequentialOutputs(true, false),
			Why: "the claim in one line: a delivery answers one gate however many times it is " +
				"delivered, so the second wait on the same name lapses rather than taking the " +
				"redelivery — and a driver that took it would approve something nobody sent twice",
		},
		{
			Name:       "two deliveries with distinct ids answer two gates",
			SignalName: signal,
			Workflow:   sequential("distinct-answers-two-gates"),
			Deliveries: []SignalDedupeDelivery{
				{DeliveryID: "click-a", Payload: approval},
				{DeliveryID: "click-b", Payload: approval},
			},
			ExpectedOutputs: sequentialOutputs(true, true),
			Why: "the case above with the one fact that decides it changed; without this, a driver " +
				"that simply refused every second delivery would pass it",
		},
		{
			Name:       "two deliveries carrying no id at all answer two gates",
			SignalName: signal,
			Workflow:   sequential("unattributed-answers-two-gates"),
			Deliveries: []SignalDedupeDelivery{
				{Payload: approval},
				{Payload: approval},
			},
			ExpectedOutputs: sequentialOutputs(true, true),
			Why: "every sender that is not a webhook carries no delivery id, and two `flow signal` " +
				"calls are two answers; a dedupe keyed on the empty string would make the first " +
				"signal any run ever received suppress every one after it",
		},
		{
			Name:       "a replay after the gate is not taken by a loop's next turn",
			SignalName: signal,
			Workflow:   looped("replay-not-taken-by-loop"),
			Deliveries: []SignalDedupeDelivery{
				{DeliveryID: "click-a", Payload: approval},
				{DeliveryID: "click-a", Payload: approval},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"answered": {NamedValues: map[string]*v1.Value{
					v1.ValueOutput: v1.NewLiteral(int64(1)),
				}},
			}},
			Why: "the shape the bridge's own hazard has: a gate that is reached again would " +
				"otherwise be answered by a replay of the click that answered it the first time, " +
				"which is a stage approved by nobody",
		},
		{
			Name:       "two distinct deliveries answer two of a loop's turns",
			SignalName: signal,
			Workflow:   looped("distinct-answers-two-turns"),
			Deliveries: []SignalDedupeDelivery{
				{DeliveryID: "click-a", Payload: approval},
				{DeliveryID: "click-b", Payload: approval},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"answered": {NamedValues: map[string]*v1.Value{
					v1.ValueOutput: v1.NewLiteral(int64(2)),
				}},
			}},
			Why: "the loop case's twin, for the reason the sequential one has one: two real " +
				"clicks approve two turns, so the drop above is about the id and not about the loop",
		},
	}
}

// AssertSignalDedupeCases runs every case through one driver's own way of
// delivering signals and running a workflow, and holds the result to the shared
// expectations.
//
// run sends every entry in [SignalDedupeCase.Deliveries], in order, with its
// delivery id on the sender, before the run reaches its first gate — a
// `SignalWorkflow` durably, a [v1.LocalSignals] queue locally — then executes
// the workflow and returns its step outputs.
//
// A case's expectation names only the steps whose outputs the claim is about;
// the loop cases read a counting step rather than the loop's own `results`, so
// the runner drops step outputs the case does not name. That is the one place
// this differs from [AssertSignalBatchCases]' whole-transcript comparison, and
// it is deliberate: the alternative is restating every iteration's shape in
// every case, which is the second definition this repository keeps deleting.
func AssertSignalDedupeCases(t *testing.T, run func(t *testing.T, c SignalDedupeCase) (*v1.Workflow_StepOutputs, error)) {
	t.Helper()

	// The bridge's own mapping case rides this carrier too — see
	// [WebhookSignalDeliveryCases] for why it cannot be a [Case] — so both
	// driver callers run it without either of them knowing about it.
	for _, c := range append(SignalDedupeCases(), WebhookSignalDeliveryCases()...) {
		t.Run(c.Name, func(t *testing.T) {
			outputs, err := run(t, c)
			require.NoErrorf(t, err, "the run failed, and every case here is about an ordinary outcome\n  %s", c.Why)

			got := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
			for id := range c.ExpectedOutputs.GetStepValues() {
				got.StepValues[id] = outputs.GetStepValues()[id]
			}

			require.Truef(t, proto.Equal(c.ExpectedOutputs, got),
				"%s\n  %s\n%s", c.Name, c.Why,
				cmp.Diff(c.ExpectedOutputs, got, protocmp.Transform()))
		})
	}
}
