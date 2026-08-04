package tests

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// WaitCases exercise durable waiting.
//
// They live beside [Workflows] and [PolicyCases] so both drivers are held to the
// same expectations, and waiting is a place where the two are especially tempting
// to implement separately: durably a timer is server state, locally it is a
// sleep. What an author observes has to be identical anyway — the step blocks, it
// produces outputs, and the next step runs afterwards — because a local run that
// disagreed about a wait would be worse than no local run at all.
//
// The durations are deliberately short. The durable driver's test environment
// skips workflow time, so a week would cost nothing there, but the local driver
// actually sleeps — and a shared case has to be reasonable for both.
//
// Signals are not here, and that is a real gap rather than an oversight: locally
// a signal arrives through a [v1.SignalWaiter], durably it arrives over the
// control plane, and a shared case cannot deliver one without a hook neither
// driver has yet. Both are covered by driver-specific tests until then.
func WaitCases() []Case {
	return []Case{
		{
			Name: "a sleep blocks and then continues",
			Workflow: &v1.Workflow{
				Name: "sleep",
				Steps: []*v1.Node{
					says("before", "starting"),
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Duration{Duration: durationpb.New(10 * time.Millisecond)},
						}},
					},
					says("after", "resumed"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"before": {},
				// A wait reports how it ended, so an author can branch on it.
				"pause": {NamedValues: map[string]*v1.Value{
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				"after": {},
			}},
		},
		{
			Name: "a zero sleep is a step that does nothing",
			Workflow: &v1.Workflow{
				Name: "sleep-zero",
				Steps: []*v1.Node{
					{
						Id:   "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(0)}}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"pause": {NamedValues: map[string]*v1.Value{
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
			}},
		},
		{
			Name: "a wait_until whose moment has passed does not hold the run up",
			Workflow: &v1.Workflow{
				Name: "wait-until-past",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							// A workload resumed after an outage reaches a window
							// that has already opened, and must catch up rather
							// than fail for being late.
							Kind: &v1.Wait_Until{Until: v1.NewLiteral("2000-01-01T00:00:00Z")},
						}},
					},
					says("after", "caught up"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"pause": {NamedValues: map[string]*v1.Value{
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				"after": {},
			}},
		},
		{
			// A regression guard for the local driver's [v1.Clock] plumbing
			// (#155): `now`, read inside a wait_until expression, must still
			// resolve to the real current moment on the default, uninjected
			// path — not to a stale or zero value a virtual-clock mistake
			// could otherwise leave behind. Both drivers computing the same
			// near-future deadline from the same expression is exactly the
			// time-observability agreement CLAUDE.md's "both execution
			// drivers must agree" asks for; only the *existence* of a
			// virtual clock is local-driver-only, not what `now` means by
			// default.
			Name: "a wait_until in the near future blocks until then",
			Workflow: &v1.Workflow{
				Name: "wait-until-future",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Until{Until: v1.NewExpr(`now + duration("15ms")`)},
						}},
					},
					says("after", "caught up"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"pause": {NamedValues: map[string]*v1.Value{
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				"after": {},
			}},
		},
		{
			Name: "a skipped wait does not wait",
			Workflow: &v1.Workflow{
				Name: "wait-skipped",
				Steps: []*v1.Node{
					counter("gate", "no"),
					{
						Id:        "pause",
						Condition: v1.NewExpr("size(gate.results) == 99"),
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							// An hour, so that a driver which ignored the
							// condition is caught twice over: it would record
							// outputs for a step that should have been skipped,
							// and locally it would also take an hour to do it.
							Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Hour)},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(map[string]any{"gate_body": map[string]any{}}),
				}},
			}},
		},
	}
}
