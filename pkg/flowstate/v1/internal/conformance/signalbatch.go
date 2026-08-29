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

// The one question a drain asks, asked of both drivers: given these deliveries
// buffered on this channel, what does `wait_for_signals:` produce?
//
// # Why this is a table of its own and not more [WaitCases]
//
// [WaitCases] says plainly why signals are not in it: locally a signal arrives
// through a [v1.SignalWaiter], durably it arrives over the control plane, and a
// shared [Case] has no way to deliver one. That is still true. What is different
// here is that a *batch* is meaningless without deliveries — every interesting
// claim it makes is about how many arrived, in what order, and what happened at
// the bound — so a table that could not deliver any would be a table that could
// only test the lapsed case, which is precisely the half [WaitCases] already
// covers through `timeout:`.
//
// So this follows [RehearsalSignalCases]' shape instead: the case names what is
// sent, each driver's caller sends it the way that driver sends things, and the
// assertion about what the run produced is shared. Two callers, one set of
// expectations, and the delivery mechanism — the only genuinely driver-specific
// part — stays in the driver's own package.
//
// # What every case here assumes, and why that is the honest shape
//
// Deliveries are buffered *before the drain looks*. That is not a convenience to
// make the assertions deterministic — it is the shape the feature is for. A
// drain takes what has arrived, so a burst it is meant to take as a burst has to
// have arrived; a workload spells "how long arrived means" as an ordinary
// `sleep:` before the drain, which is what `examples/signal-batch-drain` writes
// out. A case that raced its deliveries against the wait would be testing the
// scheduler.
//
// # What is deliberately not here
//
// Two things, each because it belongs to a table that already exists:
//
//   - **Admission.** A delivery from a sender a `signals:` policy refuses never
//     reaches any channel — `SignalPolicyCheck` runs at the `Signal` RPC durably
//     and at [v1.LocalSignals.DeliverFrom] locally, both before buffering. That
//     is exactly what [RehearsalSignalCases] pins, for both drivers, and a batch
//     changes nothing about it: the policy is not at receive time, so a drain has
//     no path around it to test. Restating it here would be a second account of
//     one rule.
//   - **Carriage across Continue-As-New.** A batch takes its carried signals
//     before the channel, oldest first — but only the durable driver has a
//     Continue-As-New to carry them across, and a local run is a process that
//     does not suspend. That claim is `engine`'s own to make and it does, in
//     `wait_test.go`, beside the single wait's version of it.

// SignalBatchCase is one buffered burst and what both drivers must produce from
// it.
type SignalBatchCase struct {
	// Name says what the case is about, and becomes the subtest name.
	Name string

	// Workflow is what runs. Every case here writes at least one
	// `wait_for_signals:` step and shapes its outputs, because the raw
	// `deliveries` list carries a `sender` rendering that would be transcribed
	// into every expectation otherwise — the second definition of a shape this
	// repository keeps finding.
	Workflow *v1.Workflow

	// SignalName is the channel every delivery is addressed to.
	SignalName string

	// Deliveries are the payloads to buffer before the run reaches its first
	// drain, in the order they are sent. That order is the one `deliveries`
	// must report, which is half of what these cases are for.
	Deliveries []map[string]*v1.Value

	// ExpectedOutputs is what the run must produce, compared whole.
	ExpectedOutputs *v1.Workflow_StepOutputs

	// Why says what the case is pinning, and is what a failure reports — a
	// diff alone does not tell the next reader which rule they broke.
	Why string
}

// SignalBatchCases exercise `wait_for_signals:` on both drivers.
//
// Read the table's own doc above for what is here and what deliberately is not.
func SignalBatchCases() []SignalBatchCase {
	return []SignalBatchCase{
		{
			Name:       "a burst is drained in one step, oldest first",
			SignalName: "order-placed",
			Why: "the whole point of the arm: one step takes the whole burst, and " +
				"`deliveries` reports it in the order the senders sent it rather than in " +
				"whatever order a map or a scheduler happened to produce",
			Deliveries: []map[string]*v1.Value{
				{"id": v1.NewLiteral("a")},
				{"id": v1.NewLiteral("b")},
				{"id": v1.NewLiteral("c")},
			},
			Workflow: &v1.Workflow{
				Name: "batch-drains-a-burst",
				Steps: []*v1.Node{
					drainStep("batch", "order-placed", 0, time.Minute),
					says("after", "carried on"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"batch": {NamedValues: map[string]*v1.Value{
					"ids":             v1.NewLiteralList("a", "b", "c"),
					"taken":           v1.NewLiteral(int64(3)),
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				"after": {},
			}},
		},
		{
			// The bound REACHED, not merely not exceeded. CLAUDE.md's own rule:
			// `taken <= max_batch` is satisfied just as well by a drain that
			// gave up after one, so the assertion has to be that the bound was
			// hit exactly and that the remainder survived it.
			Name:       "a burst larger than max_batch fills one drain and leaves the rest",
			SignalName: "order-placed",
			Why: "the bound is reached rather than declared, and what did not fit is " +
				"neither dropped nor re-buffered — the next drain finds it exactly where " +
				"it was, which is what makes a `loop:` around this step the right way to " +
				"process more than a bound's worth",
			Deliveries: []map[string]*v1.Value{
				{"id": v1.NewLiteral("a")},
				{"id": v1.NewLiteral("b")},
				{"id": v1.NewLiteral("c")},
				{"id": v1.NewLiteral("d")},
				{"id": v1.NewLiteral("e")},
			},
			Workflow: &v1.Workflow{
				Name: "batch-bound-is-reached",
				Steps: []*v1.Node{
					drainStep("first", "order-placed", 2, time.Minute),
					drainStep("second", "order-placed", 2, time.Minute),
					drainStep("third", "order-placed", 2, time.Minute),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"first": {NamedValues: map[string]*v1.Value{
					"ids":             v1.NewLiteralList("a", "b"),
					"taken":           v1.NewLiteral(int64(2)),
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				"second": {NamedValues: map[string]*v1.Value{
					"ids":             v1.NewLiteralList("c", "d"),
					"taken":           v1.NewLiteral(int64(2)),
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				// The seam every paging bug lives at: the third drain must find
				// exactly the one delivery the first two left, not zero and not
				// a repeat. Walking to exhaustion and checking the whole set is
				// what a per-drain assertion cannot do.
				"third": {NamedValues: map[string]*v1.Value{
					"ids":             v1.NewLiteralList("e"),
					"taken":           v1.NewLiteral(int64(1)),
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
			}},
		},
		{
			// The interplay the proposal asks for: a drain with a bound and
			// nothing to take reports an *empty batch*, not a failure, and says
			// which of the two it was. `count: 0` and `timed_out: true` are one
			// outcome and have to arrive together — a drain reporting `count: 0`
			// with `timed_out: false` would be claiming a burst of nothing
			// arrived on time.
			Name:       "a drain nobody sends anything to lapses with an empty batch",
			SignalName: "order-placed",
			Why: "a lapsed batch is an ordinary outcome branchable with an `if:`, exactly " +
				"as a lapsed single gate is, and it is `count: 0` with `timed_out: true` " +
				"rather than an error or an absent `deliveries` list",
			Workflow: &v1.Workflow{
				Name: "batch-lapses-empty",
				Steps: []*v1.Node{
					drainStep("batch", "order-placed", 0, 15*time.Millisecond),
					says("after", "carried on"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"batch": {NamedValues: map[string]*v1.Value{
					// Empty rather than absent, which is [v1.SignalOutputs]'
					// always-present-payload rule at list scale: `size(...)` is
					// answerable either way.
					"ids":             v1.NewLiteralList(),
					"taken":           v1.NewLiteral(int64(0)),
					v1.TimedOutOutput: v1.NewLiteral(true),
				}},
				"after": {},
			}},
		},
		{
			// The other side of that interplay, and the one a driver is most
			// likely to get wrong: a bound that *exists* over a channel that
			// already holds something. The bound bounds the wait for the first
			// delivery, and one is already here — so nothing was waited for and
			// `timed_out` is false, however short the bound was.
			Name:       "a bounded drain that finds deliveries already buffered does not report a lapse",
			SignalName: "order-placed",
			Why: "`timed_out` is about whether anything arrived in time, and something " +
				"had already arrived; a driver that armed its timer before looking would " +
				"race an expired deadline against a delivery it already held",
			Deliveries: []map[string]*v1.Value{
				{"id": v1.NewLiteral("a")},
				{"id": v1.NewLiteral("b")},
			},
			Workflow: &v1.Workflow{
				Name: "batch-buffered-beats-bound",
				Steps: []*v1.Node{
					// A bound short enough that a driver waiting on it at all
					// would lapse before it saw anything.
					drainStep("batch", "order-placed", 0, time.Millisecond),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"batch": {NamedValues: map[string]*v1.Value{
					"ids":             v1.NewLiteralList("a", "b"),
					"taken":           v1.NewLiteral(int64(2)),
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
			}},
		},
		{
			// The two spellings over one channel, which is the join worth
			// testing rather than each half alone: a `wait_for_signal:` consumes
			// exactly one and a `wait_for_signals:` after it takes the rest, so
			// neither can be satisfied twice by one delivery and neither can
			// swallow what the other was waiting for.
			Name:       "a single wait takes one and a drain takes the rest of the same channel",
			SignalName: "order-placed",
			Why: "one channel, two spellings, no delivery counted twice and none lost " +
				"between them — the seam a table testing each arm alone cannot reach",
			Deliveries: []map[string]*v1.Value{
				{"id": v1.NewLiteral("a")},
				{"id": v1.NewLiteral("b")},
				{"id": v1.NewLiteral("c")},
			},
			Workflow: &v1.Workflow{
				Name: "batch-after-single",
				Steps: []*v1.Node{
					{
						Id: "one",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name:    "order-placed",
								Outputs: map[string]*v1.Value{"id": v1.NewExpr("payload.id")},
							}},
							Timeout: durationpb.New(time.Minute),
						}},
					},
					drainStep("rest", "order-placed", 0, time.Minute),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"one": {NamedValues: map[string]*v1.Value{"id": v1.NewLiteral("a")}},
				"rest": {NamedValues: map[string]*v1.Value{
					"ids":             v1.NewLiteralList("b", "c"),
					"taken":           v1.NewLiteral(int64(2)),
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
			}},
		},
		{
			// The sender travels per delivery, and it travels *rooted*: a batch
			// entry is `{payload, sender}` shaped exactly as a single wait's own
			// two outputs are. Asserted through the shaping rather than against
			// the raw list for the reason [WaitCases] gives about a lapsed
			// gate's sender: the rendering is a shape, not a value, and
			// transcribing it here would be a second definition of it.
			Name:       "each delivery carries its own sender, rooted as a single wait's is",
			SignalName: "order-placed",
			Why: "a payload is what somebody asserted and a sender is what the engine " +
				"established, per delivery — a batch must not collapse many senders into " +
				"one, and must not let a payload key named `sender` be mistaken for one",
			Deliveries: []map[string]*v1.Value{
				{"id": v1.NewLiteral("a"), "sender": v1.NewLiteral("forged")},
				{"id": v1.NewLiteral("b")},
			},
			Workflow: &v1.Workflow{
				Name: "batch-senders",
				Steps: []*v1.Node{
					{
						Id: "batch",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
								Name: "order-placed",
								Outputs: map[string]*v1.Value{
									"ids": v1.NewExpr("deliveries.map(d, d.payload.id)"),
									// What the payload *claimed*, still reachable
									// under `payload` where it belongs and
									// nowhere else.
									"claimed": v1.NewExpr(
										`deliveries.map(d, has(d.payload.sender) ? d.payload.sender : "")`),
									// What the engine says about each sender. A
									// local delivery is unattested on both
									// drivers when nothing attested it, which is
									// the fact both can agree on without a
									// server in the loop.
									"attested": v1.NewExpr("deliveries.map(d, !d.sender.local)"),
								},
							}},
							Timeout: durationpb.New(time.Minute),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"batch": {NamedValues: map[string]*v1.Value{
					"ids":     v1.NewLiteralList("a", "b"),
					"claimed": v1.NewLiteralList("forged", ""),
					// Both false: nothing attested either delivery, and the
					// payload's own "sender" key reached `payload` and stopped
					// there.
					"attested": v1.NewLiteralList(false, false),
				}},
			}},
		},
	}
}

// drainStep builds a `wait_for_signals:` step shaping the two outputs every case
// here asserts.
//
// Shaped rather than raw, and that is the load-bearing choice in this file. The
// unshaped `deliveries` list carries [v1.SignalSender]'s whole rendering per
// entry — an identity map, an `accepted_at`, a `local` flag — which every
// expectation in this table would otherwise have to transcribe. Transcribing a
// shape is how a second definition of it gets written down, and the drift that
// follows is the thing this repository keeps finding. `ids` and `taken` say
// everything these cases are about, and the sender case above reaches the third
// field through an expression rather than through a copy of its shape.
//
// maxBatch of zero leaves the field unset, which is how an author who does not
// write the key spells it.
func drainStep(id, signal string, maxBatch int32, timeout time.Duration) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
				Name:     signal,
				MaxBatch: maxBatch,
				Outputs: map[string]*v1.Value{
					"ids":             v1.NewExpr("deliveries.map(delivery, delivery.payload.id)"),
					"taken":           v1.NewExpr(v1.CountOutput),
					v1.TimedOutOutput: v1.NewExpr(v1.TimedOutOutput),
				},
			}},
			Timeout: durationpb.New(timeout),
		}},
	}
}

// AssertSignalBatchCases runs every case through one driver's own way of
// buffering deliveries and running a workflow, and holds the result to the
// shared expectations.
//
// run buffers every payload in [SignalBatchCase.Deliveries], in order, so that
// they are on the channel before the run's first drain looks — then executes the
// workflow and returns its step outputs. Everything driver-specific is inside
// it: a `RegisterDelayedCallback` and `SignalWorkflow` durably, a
// [v1.LocalSignals] queue locally. Everything that is a claim about the
// *language* is here, so a case added to the table above is answered by both
// drivers or by neither.
//
// A run that fails is reported as a failure rather than compared, because none
// of these cases is about a run failing: a lapsed batch is an ordinary outcome,
// which is the point of the lapsed case rather than an exception to it.
func AssertSignalBatchCases(t *testing.T, run func(t *testing.T, c SignalBatchCase) (*v1.Workflow_StepOutputs, error)) {
	t.Helper()

	for _, c := range SignalBatchCases() {
		t.Run(c.Name, func(t *testing.T) {
			outputs, err := run(t, c)
			require.NoErrorf(t, err, "the run failed, and this case is about an ordinary outcome\n  %s", c.Why)

			require.Truef(t, proto.Equal(c.ExpectedOutputs, outputs),
				"%s\n  %s\n%s", c.Name, c.Why,
				cmp.Diff(c.ExpectedOutputs, outputs, protocmp.Transform()))
		})
	}
}
