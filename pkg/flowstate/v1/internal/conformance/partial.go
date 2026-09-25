package conformance

import v1 "github.com/picatz/flowstate/pkg/flowstate/v1"

// PartialTranscriptCase is a workflow that fails, paired with the record its
// driver must hand back alongside the failure ([v1.PartialTranscript]).
//
// A separate type from [Case] because what it asserts is the *failed* run's
// outputs, and [Case.ExpectedOutputs] is explicitly ignored when
// [Case.ExpectFailure] is set, a case whose point is the failure had nothing to
// compare, which is exactly the gap issue #453 is about.
type PartialTranscriptCase struct {
	// Name of the case, used for test identification.
	Name string

	// Workflow is the definition to run. Every one of these fails on purpose.
	Workflow *v1.Workflow

	// Expected is the whole transcript the failed run must hand back: every step
	// it recorded outputs for, and the step it stopped on. Compared exactly rather
	// than as a subset, so a driver that carries *more* than the other one fails
	// here too, the direction a "reached at least these" assertion cannot see, and
	// the one that would let a rehearsal credit a branch production never records.
	Expected *v1.Workflow_StepOutputs
}

// PartialTranscriptCases cover what a run that fails hands back about what it
// did, which both drivers must answer identically (invariant 3).
//
// Neither driver used to answer at all: a failed run returned no step outputs, so
// `flow test`'s coverage counted a case whose whole point was `expect.failed:
// true` as reaching none of the steps it had just exercised, and an author had to
// record a `coverage.allow_unreached` reason for a branch that really ran (issue
// #453). The record itself was never missing, both drivers accumulate it as they
// walk, only unreturned.
//
// The shapes here are the questions a transcript can get wrong:
//
//   - what ran before the failure, and the failing step itself. The step that
//     ended the run is recorded through [v1.FailedStepOutputs], the same shape a
//     step tolerated by `continue_on_error:` is recorded in, because it is the
//     same fact: this step ran and failed. Recording it one step short of the
//     truth is the version of this bug that survives a naive fix.
//   - what never ran. A step skipped by its `if:` before the failure, and every
//     step after the failure, are absent, the transcript is a record of what
//     happened, and absence is how it says a step did not.
//   - what a nesting that did not finish contributes, which is nothing. A loop's
//     per-iteration outputs only reach the transcript when the loop node completes
//     and writes its own `results`, so a body step that ran inside the iteration
//     that failed is *not* in the record. Both drivers keep it that way, and a
//     driver that reached into the unfinished nesting would be inventing a record
//     the other one does not have. (Exhaustion is the deliberate exception,
//     covered by [LoopExhaustionTranscriptCases]: every iteration an exhausted
//     loop recorded ran to completion, so its entry carries them.)
//
// Most failures below are a step's own `vars:` indexing past the end of a list:
// they compile, fail when evaluated, carry no TaskError, and need no server, so
// their recorded sentence is the same one [ToleratedStepFailureCases] already
// pins for the tolerated version of the identical failure. The final case is a
// pre-dispatch atomic-bound refusal, so it additionally proves that failures
// before a selected switch body starts preserve the same account.
func PartialTranscriptCases() []PartialTranscriptCase {
	const oops = "['a'][5]"
	const recorded = `var "bad": evaluate expression: index out of bounds: 5`

	// boom is a step that always fails and is never tolerated, so it is always the
	// step that ends the run it appears in.
	boom := func(id string) *v1.Node {
		return withVars(says(id, "unreachable"), map[string]*v1.Value{
			"bad": v1.NewExpr(oops),
		})
	}
	return []PartialTranscriptCase{
		{
			Name: "the steps before an untolerated failure are recorded, and so is the step that failed",
			Workflow: &v1.Workflow{
				Name: "partial-before-failure",
				Steps: []*v1.Node{
					says("first", "ran"),
					says("second", "ran"),
					boom("boom"),
					// Never reached, so never recorded: the assertion that the
					// transcript stops where the run did.
					says("after", "unreachable"),
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"first":  {},
				"second": {},
				"boom":   v1.FailedStepOutputs(recorded),
			}},
		},
		{
			Name: "a step skipped by its condition before the failure stays absent",
			Workflow: &v1.Workflow{
				Name: "partial-skipped-branch",
				Steps: []*v1.Node{
					says("taken", "ran"),
					guarded("not_taken", "false", "unreachable"),
					boom("boom"),
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"taken": {},
				"boom":  v1.FailedStepOutputs(recorded),
			}},
		},
		{
			Name: "a loop that fails mid-body contributes only the loop step, never the body",
			Workflow: &v1.Workflow{
				Name: "partial-unfinished-loop",
				Steps: []*v1.Node{
					says("before", "ran"),
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewLiteralList("one"),
							Body: []*v1.Node{
								// This one genuinely runs, inside an iteration that
								// never finishes, so it is not in the record, and
								// that is the claim.
								says("inside", "ran"),
								boom("inside_boom"),
							},
						}},
					},
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"before": {},
				// The position the failure passed out through, prefixed by the loop
				// level exactly as a tolerated `for_each` body failure is.
				"each": v1.FailedStepOutputs(`iteration 0: step "inside_boom": ` + recorded),
			}},
		},
		{
			// The account a container keeps of itself, the other half of what
			// [v1.LoopExhaustedError] does for an exhausted loop. A switch
			// records its selection after its body returns, so a body that
			// failed used to leave the switch's entry holding the failure text
			// alone — the arm that ran erased from the only record of the run.
			// `flow test` reads that record to measure switch-arm coverage
			// (#801), so a case whose whole point is `expect.failed: true` on an
			// error arm was reported as never having taken it.
			//
			// Both drivers therefore attach the selection to the failed switch's
			// entry ([v1.SwitchBodyError]), and this is the case that says so
			// once for both of them.
			Name: "a switch whose body fails keeps the arm it selected on its own entry",
			Workflow: &v1.Workflow{
				Name: "partial-failed-switch",
				Steps: []*v1.Node{
					says("before", "ran"),
					{
						Id: "route",
						Kind: &v1.Node_Switch{Switch: &v1.Switch{
							Value: v1.NewLiteral("boom"),
							Cases: []*v1.Switch_Case{
								{
									Values: []*v1.Value{v1.NewLiteral("boom")},
									Steps:  []*v1.Node{boom("inside_boom")},
								},
								{
									Values: []*v1.Value{v1.NewLiteral("fine")},
									Steps:  []*v1.Node{says("not_taken", "unreachable")},
								},
							},
						}},
					},
					says("after", "unreachable"),
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"before": {},
				// A switch body merges into the enclosing scope, so the body step
				// that failed is on the record under its own id, exactly as a
				// `parallel` branch step would be.
				"inside_boom": v1.FailedStepOutputs(recorded),
				// And the switch's own entry: the position the failure passed out
				// through, *plus* the arm it had already selected.
				"route": switchFailure(`step "inside_boom": `+recorded, "boom", "boom"),
			}},
		},
		{
			// The static pre-dispatch check is still a failure of the selected
			// switch body. It must preserve the selection exactly like a task
			// failure in that body; otherwise coverage reports the chosen arm as
			// unreached solely because the bound correctly prevented dispatch.
			Name:     "an oversized switch body keeps the arm selected before its refusal",
			Workflow: siblingParallelForEachBlocks(),
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"choose": switchFailure(v1.AtomicBlockBodyActivitiesError(v1.MaxAtomicBlockActivities).Error(), "selected", "selected"),
			}},
		},
	}
}

// switchFailure is the entry a switch records when the body it selected failed:
// the failure text, plus the selection [v1.SelectSwitchCase] had already made.
//
// Spelled through [v1.FailedStepOutputs] and the same output names the drivers
// write, rather than as a literal map, so a case here cannot claim a shape no
// driver produces.
func switchFailure(text, observed, took string) *v1.Node_Outputs {
	out := v1.FailedStepOutputs(text)
	out.NamedValues[v1.SwitchValueOutput] = v1.NewLiteral(observed)
	out.NamedValues[v1.SwitchCaseOutput] = v1.NewLiteral(took)

	return out
}
