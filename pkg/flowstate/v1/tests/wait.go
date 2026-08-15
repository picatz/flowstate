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
			// A computed sleep, in the shape an author writes: the length is a
			// property of the run rather than of the file. Here the branch is over a
			// declared input, which is the `sleep: ${inputs.plan == 'enterprise' ?
			// ... : ...}` spelling with the durations shrunk to what a local run can
			// reasonably sit through.
			Name: "a computed sleep blocks for the length its expression produces",
			Workflow: &v1.Workflow{
				Name: "sleep-computed",
				DeclaredInputs: []*v1.InputDeclaration{
					{Name: "plan", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
				},
				Steps: []*v1.Node{
					says("before", "starting"),
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{
								DurationExpr: v1.NewExpr(
									`inputs.plan == "enterprise" ? duration("30ms") : duration("10ms")`),
							},
						}},
					},
					says("after", "resumed"),
				},
			},
			Inputs: map[string]*v1.Value{"plan": v1.NewLiteral("growth")},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"before": {},
				"pause": {NamedValues: map[string]*v1.Value{
					v1.TimedOutOutput: v1.NewLiteral(false),
				}},
				"after": {},
			}},
		},
		{
			// A duration arriving as text, which is how one arrives from outside at
			// all: [v1.InputDeclaration_Type] has no duration member, so a caller
			// sends a string. `10ms` therefore has to mean here exactly what the
			// same characters mean in a literal `sleep:` — one parser
			// ([v1.ParseDuration]), read by both drivers.
			Name: "a computed sleep reads a duration written as a string",
			Workflow: &v1.Workflow{
				Name: "sleep-computed-string",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{DurationExpr: v1.NewExpr(`"10ms"`)},
						}},
					},
					says("after", "resumed"),
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
			// Zero is legal and releases at once, which is the boundary the negative
			// case below sits on the other side of. Stated as its own case because
			// "greater than zero" and "not negative" are one character apart in the
			// code and a fortnight apart in consequence: the literal `sleep: 0s` is
			// refused by the *compiler*, which can see it, and an expression's value
			// exists only at run time where the honest answer is that there is no
			// time left to wait.
			Name: "a computed sleep of zero is a step that does nothing",
			Workflow: &v1.Workflow{
				Name: "sleep-computed-zero",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{DurationExpr: v1.NewExpr(`duration("0s")`)},
						}},
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
			// The other side of it. A negative sleep fails the run on both drivers
			// rather than being clamped to zero, because the two readings of a
			// negative duration — "somebody meant zero" and "somebody's arithmetic
			// has its operands the wrong way round" — are indistinguishable here,
			// and only one of them is harmless.
			Name:          "a computed sleep that is negative fails the run",
			ExpectFailure: true,
			Workflow: &v1.Workflow{
				Name: "sleep-computed-negative",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{DurationExpr: v1.NewExpr(`duration("-1s")`)},
						}},
					},
				},
			},
		},
		{
			// A type mismatch, which has to be a failure and not a guess. An integer
			// is the one worth pinning: CEL would happily read it as nanoseconds, so
			// `sleep: ${inputs.minutes}` against a number would "work" and wait
			// nothing, which is the failure an author never finds.
			Name:          "a computed sleep that produces a number fails the run",
			ExpectFailure: true,
			Workflow: &v1.Workflow{
				Name: "sleep-computed-int",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{DurationExpr: v1.NewExpr(`1 + 2`)},
						}},
					},
				},
			},
		},
		{
			// A string that is not a duration. Distinct from the integer above
			// because it fails in [v1.ParseDuration] rather than in the type switch,
			// and a case covering only one of the two would leave the other's
			// agreement between drivers unproven.
			Name:          "a computed sleep that produces a string that is not a duration fails the run",
			ExpectFailure: true,
			Workflow: &v1.Workflow{
				Name: "sleep-computed-nonsense",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{DurationExpr: v1.NewExpr(`"a fortnight"`)},
						}},
					},
				},
			},
		},
		{
			// `now` inside a computed sleep, which is the binding this change
			// widened. Both drivers hold a replay-safe clock here — `workflow.Now`
			// durably, the context's [v1.Clock] locally — so the same expression has
			// to resolve on both, and this is the case that says so.
			//
			// Written as a round trip through the clock so the result is a fixed
			// 15ms whatever the clock reads, which is what makes it assertable at
			// all: the point is that the name resolves, not what time it is.
			Name: "a computed sleep may read the clock",
			Workflow: &v1.Workflow{
				Name: "sleep-computed-now",
				Steps: []*v1.Node{
					{
						Id: "pause",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_DurationExpr{
								DurationExpr: v1.NewExpr(`(now + duration("15ms")) - now`),
							},
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
			// A computed *timeout*, ending a gate nobody answers. This is the case
			// that would hang rather than fail if the computed bound fell through to
			// the encoding that means "no timeout" — which is exactly what a
			// negative or zero value used to do, and why [v1.EvalWaitTimeout]
			// reports whether a bound was written separately from what it is.
			//
			// A signal nothing sends is deliverable to both drivers precisely
			// because nothing has to deliver it: the timer is the whole assertion,
			// which is what lets this be a shared case where the answered gate above
			// still cannot be.
			Name: "a computed timeout lapses a gate nobody answers",
			Workflow: &v1.Workflow{
				Name: "timeout-computed",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind:        &v1.Wait_Signal{Signal: &v1.Signal{Name: "sign-off"}},
							TimeoutExpr: v1.NewExpr(`(now + duration("15ms")) - now`),
						}},
					},
					says("after", "carried on"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				// Built through the engine's own constructor rather than written
				// out, because a lapsed gate's `sender` is a shape
				// ([v1.SignalOutputs]'s nil-sender rendering) and not a value —
				// transcribing it here would be a second definition of it, which
				// is the drift this repository keeps finding.
				"gate":  v1.SignalOutputs(nil, nil, true),
				"after": {},
			}},
		},
		{
			// A computed timeout of zero, which means the gate has already lapsed —
			// not that it is unbounded. The distinction is the whole reason
			// [v1.EvalWaitTimeout] returns two values, and a case that only ever
			// computed a positive bound could not tell the two apart.
			Name: "a computed timeout of zero lapses the gate at once",
			Workflow: &v1.Workflow{
				Name: "timeout-computed-zero",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind:        &v1.Wait_Signal{Signal: &v1.Signal{Name: "sign-off"}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
					says("after", "carried on"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				// Built through the engine's own constructor rather than written
				// out, because a lapsed gate's `sender` is a shape
				// ([v1.SignalOutputs]'s nil-sender rendering) and not a value —
				// transcribing it here would be a second definition of it, which
				// is the drift this repository keeps finding.
				"gate":  v1.SignalOutputs(nil, nil, true),
				"after": {},
			}},
		},
		{
			// And a negative one fails, on both drivers, rather than becoming the
			// unbounded gate it would decay into. The sharpest case in this file:
			// the failure it prevents is not a wrong answer but a run that never
			// ends, which no assertion about outputs could ever catch.
			Name:          "a computed timeout that is negative fails the run",
			ExpectFailure: true,
			Workflow: &v1.Workflow{
				Name: "timeout-computed-negative",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind:        &v1.Wait_Signal{Signal: &v1.Signal{Name: "sign-off"}},
							TimeoutExpr: v1.NewExpr(`duration("0s") - duration("1s")`),
						}},
					},
				},
			},
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

		// Output shaping. Every case below lapses its gate rather than answering
		// it, for the reason the computed-timeout cases above give: delivering a
		// signal is still driver-specific, and timing one out is not — so shaping
		// is testable on both drivers today, on the arm where it matters most
		// (a gate that lapsed is exactly the case an author most often has to
		// distinguish from one that was refused).
		{
			// Replace, not extend: the wait's own `payload`, `sender` and
			// `timed_out` are gone, and only what was shaped is there. Asserted
			// as the *whole* outputs map rather than by presence, because
			// "extend" would pass every presence check this could make.
			Name: "a shaped wait produces only the names it shaped",
			Workflow: &v1.Workflow{
				Name: "wait-shaping-replaces",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name: "sign-off",
								Outputs: map[string]*v1.Value{
									"approved": v1.NewExpr("has(payload.approved) && payload.approved"),
									"answered": v1.NewExpr("!timed_out"),
								},
							}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
					says("after", "carried on"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{
					"approved": v1.NewLiteral(false),
					"answered": v1.NewLiteral(false),
				}},
				"after": {},
			}},
		},
		{
			// The re-exposure the replace decision costs, and the proof that it
			// costs exactly one line: `timed_out: ${timed_out}` puts the name
			// back, unchanged, beside the shaped one.
			Name: "a shaped wait can re-expose the wait's own outputs",
			Workflow: &v1.Workflow{
				Name: "wait-shaping-reexposes",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name: "sign-off",
								Outputs: map[string]*v1.Value{
									"approved":          v1.NewExpr("has(payload.approved) && payload.approved"),
									v1.TimedOutOutput:   v1.NewExpr(v1.TimedOutOutput),
									"attested":          v1.NewExpr("!sender.local"),
									"approver":          v1.NewExpr("sender.identity.subject"),
									"shaped_at_present": v1.NewExpr(`now > timestamp("2000-01-01T00:00:00Z")`),
								},
							}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{
					"approved": v1.NewLiteral(false),
					// A lapsed gate with nothing pending is unattested, which is
					// what `sender.local` reads as — the shaping sees the same
					// rendering a later step would have.
					"attested":          v1.NewLiteral(false),
					"approver":          v1.NewLiteral(""),
					v1.TimedOutOutput:   v1.NewLiteral(true),
					"shaped_at_present": v1.NewLiteral(true),
				}},
			}},
		},
		{
			// The enclosing scope is underneath the wait's own names, so a
			// shaping expression is an ordinary expression that happens to see
			// three more things. Without this a shaping block could only ever
			// restate the payload, which is half of what the gate this feature
			// exists for needs.
			Name: "a shaped wait reads the enclosing scope",
			Inputs: map[string]*v1.Value{
				"quorum": v1.NewLiteral(int64(2)),
			},
			Workflow: &v1.Workflow{
				Name: "wait-shaping-scope",
				DeclaredInputs: []*v1.InputDeclaration{
					{Name: "quorum", Type: v1.InputDeclaration_TYPE_INT, Required: true},
				},
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name: "sign-off",
								Outputs: map[string]*v1.Value{
									"reason": v1.NewExpr(
										`timed_out ? "release needs " + string(inputs.quorum) : "answered"`),
								},
							}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{
					"reason": v1.NewLiteral("release needs 2"),
				}},
			}},
		},
		{
			// A literal shaped output, which is what an author writes for a
			// constant beside the derived names. It goes through the same path
			// and is recorded unchanged, so `outputs:` is not a block that
			// silently demands a fence.
			Name: "a shaped output may be a literal",
			Workflow: &v1.Workflow{
				Name: "wait-shaping-literal",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name:    "sign-off",
								Outputs: map[string]*v1.Value{"kind": v1.NewLiteral("approval")},
							}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{"kind": v1.NewLiteral("approval")}},
			}},
		},
		{
			// The negative direction, and the one that decides whether this
			// feature is safe: an expression naming a field the payload does not
			// carry fails the run rather than recording an empty value.
			//
			// Silently empty is the dangerous answer, not the annoying one. These
			// values are what later steps branch on, so an unnamed field that
			// evaluated to nothing would take the *other* arm of every gate built
			// on it — the exact failure the four hand-copied predicates this
			// feature replaces used to produce.
			Name:          "a shaping expression naming a field the payload lacks fails the run",
			ExpectFailure: true,
			Workflow: &v1.Workflow{
				Name: "wait-shaping-unknown-field",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name:    "sign-off",
								Outputs: map[string]*v1.Value{"approved": v1.NewExpr("payload.approved")},
							}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
				},
			},
		},
		{
			// #534: a bare `${secret(...)}` in a wait's shaped `outputs:` is
			// refused by the compiler now, but a spec submitted straight to
			// the Run RPC never goes through the compiler — [v1.ValidateWait]
			// and the schema's own rules are what a hand-built spec meets
			// instead. This pins the runtime backstop directly: a shaped
			// output holding a [v1.SecretRef] must fail the run rather than
			// leaking the resolved value into the outputs it records, or
			// silently carrying the reference through as a dangling one.
			//
			// [v1.ShapeSignalOutputs] evaluates each shaped output through an
			// internal switch that has no case for `*Value_SecretRef` — it is
			// neither a literal nor an expression — so it falls to the
			// default arm and fails the step. That is the fact this case
			// establishes for both drivers: not a leak, and not a
			// silently-empty value either, but a failed run.
			Name:          "a shaped output holding a secret reference fails the run rather than leaking it",
			ExpectFailure: true,
			Workflow: &v1.Workflow{
				Name: "wait-shaping-secret-ref",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Signal{Signal: &v1.Signal{
								Name: "sign-off",
								Outputs: map[string]*v1.Value{
									"token": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
										Scheme: "env", Name: "API_TOKEN",
									}}},
								},
							}},
							TimeoutExpr: v1.NewExpr(`duration("0s")`),
						}},
					},
				},
			},
		},
		// There is deliberately no case here for shaping on a `sleep:` or a
		// `wait_until:`, and its absence is the decision rather than a gap.
		//
		// The field is on [v1.Signal], not on [v1.Wait], so neither arm can carry
		// one — the refusal is structural instead of reported, and there is no
		// state for a driver to disagree about. That placement is the whole point:
		// `timeout` sits on [v1.Wait] and is meaningless on two of its arms, which
		// is exactly why [v1.ValidateWait] has to exist and say so. Shaping does
		// not repeat the mistake.
		//
		// It is also the honest answer about what those arms produce. Two of the
		// three names a shaping expression reads — `payload` and `sender` — do not
		// exist for a timer, and the third, `timed_out`, is the constant `false`;
		// naming a constant is what `vars:` is for.
	}
}
